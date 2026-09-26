# build_shot_data.R — Extract recent shot data from Opta for shot chart feature.
# Run standalone (Rscript scripts/build_shot_data.R) or source()'d from
# build_blog_data.R. Must be run from the pannadata repo root.
# In GHA, build_shot_data.R runs as a separate workflow step.

library(arrow)
library(dplyr)

opta_path <- "source/opta_shot_events.parquet"
if (!file.exists(opta_path)) stop("opta_shot_events.parquet not found in source/")

opta_shots <- read_parquet(opta_path)

required_cols <- c("season", "competition", "player_name", "x", "y",
                   "is_goal", "type_id", "body_part", "situation")
missing <- setdiff(required_cols, names(opta_shots))
if (length(missing) > 0) {
  stop("opta_shot_events.parquet missing columns: ", paste(missing, collapse = ", "),
       ". Check the Opta scraper output schema.")
}

tracked_leagues <- c("EPL", "La_Liga", "Serie_A", "Bundesliga", "Ligue_1", "UCL", "UEL")
recent_seasons <- head(
  sort(unique(opta_shots$season[opta_shots$competition %in% tracked_leagues]),
       decreasing = TRUE), 5
)

panna_shots <- opta_shots |>
  filter(competition %in% tracked_leagues, season %in% recent_seasons) |>
  transmute(
    player_name,
    x = round(x, 1),
    y = round(y, 1),
    is_goal,
    type_id = as.integer(type_id),
    body_part,
    situation,
    big_chance = if ("big_chance" %in% names(opta_shots)) as.integer(big_chance) else 0L,
    season
  )

stopifnot(nrow(panna_shots) > 0, length(recent_seasons) > 0)

# ── xG: model-score EVERY shot, coalescing the canonical source column where ──
# present. The source `xg` only covers freshly-enriched shots (recent daily
# scrape), so the old "use source if the column exists, else predict" left ~99%
# NaN whenever the column was present-but-sparse (pannadata#87). Model-scoring
# every row makes `xg` ship non-null so the blog can read s.xg directly
# (pannadata#89). Mirrors enrich_shots_xg.R's own-goal + penalty guards.
source_xg <- if ("xg" %in% names(opta_shots)) {
  round(opta_shots$xg[opta_shots$competition %in% tracked_leagues &
    opta_shots$season %in% recent_seasons], 3)
} else rep(NA_real_, nrow(panna_shots))

xg_model_path <- "source/xg_model.rds"
if (file.exists(xg_model_path)) {
  library(xgboost)
  xg_model <- readRDS(xg_model_path)
  penalty_xg <- xg_model$panna_metadata$penalty_xg
  if (is.null(penalty_xg)) {
    stop("xg_model.rds panna_metadata lacks penalty_xg (pre-panna#91 artifact) -- ",
         "republish the model from panna::fit_xg_model()")
  }
  cat("Loaded xG model:", length(xg_model$panna_metadata$feature_cols),
      "features | penalty_xg:", penalty_xg, "\n")
  # One shared builder (scripts/xg_features.R), season_num included; stops on
  # any feature it cannot build rather than zero-filling it.
  source("scripts/xg_features.R")
  X <- xg_feature_matrix(panna_shots, xg_model)
  model_xg <- round(predict(xg_model$model, X), 3)
  # Own-goal guard: Opta logs an OG as a goal (type_id 16) at the scorer's own-half
  # location, which the model reads as ~0.97 — meaningless. Surface as NA.
  is_og <- panna_shots$type_id == 16L & !is.na(panna_shots$x) & panna_shots$x < 50
  model_xg[is_og] <- NA_real_
  # Penalty override: panna's xG model is penalty-free → fix to the artifact's
  # panna_metadata$penalty_xg (single-sourced from panna::PENALTY_XG, panna#91).
  is_pen <- !is.na(panna_shots$situation) & tolower(panna_shots$situation) == "penalty"
  model_xg[is_pen] <- penalty_xg
  # Prefer the canonical source xG where present (already OG/penalty-guarded by
  # enrich_shots_xg.R), model-fill the rest.
  panna_shots$xg <- coalesce(source_xg, model_xg)
  cat("xG:", round(sum(panna_shots$xg, na.rm = TRUE), 1), "total across", nrow(panna_shots),
      "shots (", sum(is.na(source_xg)), "model-filled,", sum(is_pen), paste0("pens@", penalty_xg, ","),
      sum(is_og), "OG->NA)\n")
} else if (!all(is.na(source_xg))) {
  panna_shots$xg <- source_xg
  warning("xg_model.rds not found — using source xG only (likely sparse; see pannadata#87)")
} else {
  warning("No xG in source and xg_model.rds not found — shots will not include xG")
}

# ── Goal-mouth placement (Opta q102/103): where the shot crosses the line ──
# Powers the blog's shot-placement maps ("which corners does this player
# find?"). Present once the updated scraper + backfill_goalmouth.py have
# shipped goalmouth_y/z to opta-latest; absent gracefully otherwise.
sel <- opta_shots$competition %in% tracked_leagues & opta_shots$season %in% recent_seasons
if (all(c("goalmouth_y", "goalmouth_z") %in% names(opta_shots))) {
  panna_shots$gm_y <- round(opta_shots$goalmouth_y[sel], 1)  # NA stays NA
  panna_shots$gm_z <- round(opta_shots$goalmouth_z[sel], 1)
  cat("goal-mouth coords:", sum(!is.na(panna_shots$gm_y)), "of", nrow(panna_shots),
      "shots have placement\n")
} else {
  cat("goal-mouth coords: not in source yet (run backfill_goalmouth.py + re-upload)\n")
}

# ── xGOT (post-shot xG): per-shot placement value, from enrich_shots_xgot.R ──
# 0 = off-target, NA = on-target without coords (surfaced, not imputed).
if ("xgot" %in% names(opta_shots)) {
  panna_shots$xgot <- round(opta_shots$xgot[sel], 3)
  cat("xGOT from source:", sum(!is.na(panna_shots$xgot)), "shots have xGOT\n")
}

# Keep big_chance (renamed is_big_chance) — it's the dominant xG feature, so the
# blog can score xG from this parquet as a worker-fallback (pannadata#89).
panna_shots <- panna_shots |> rename(is_big_chance = big_chance)

dir.create("blog", showWarnings = FALSE)
write_parquet(panna_shots, "blog/shots.parquet")
cat("shots:", nrow(panna_shots), "shots across", length(recent_seasons), "seasons\n")
