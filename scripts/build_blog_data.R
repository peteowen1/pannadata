library(arrow)
library(dplyr)

# Shared blog league config (BLOG_COMP_EXCLUDE — comps we drop from blog outputs).
source("scripts/league_config.R")

# Player ratings - latest season xRAPM + SPM
for (f in c("source/seasonal_xrapm.parquet", "source/seasonal_spm.parquet", "source/player_metadata.parquet")) {
  if (!file.exists(f)) stop("Required file not found: ", f, ". Check the 'Download source data' step.")
}
seasonal_xrapm <- read_parquet("source/seasonal_xrapm.parquet")
seasonal_spm <- read_parquet("source/seasonal_spm.parquet")
player_meta <- read_parquet("source/player_metadata.parquet")

# Validate required columns (explicit errors instead of cryptic stopifnot)
xrapm_required <- c("season_end_year", "player_name", "total_minutes", "xrapm", "offense", "defense")
xrapm_missing <- setdiff(xrapm_required, names(seasonal_xrapm))
if (length(xrapm_missing) > 0) stop("seasonal_xrapm missing columns: ", paste(xrapm_missing, collapse = ", "))

spm_required <- c("season_end_year", "total_minutes", "spm")
spm_missing <- setdiff(spm_required, names(seasonal_spm))
if (length(spm_missing) > 0) stop("seasonal_spm missing columns: ", paste(spm_missing, collapse = ", "))

meta_required <- c("player_name")
meta_missing <- setdiff(meta_required, names(player_meta))
if (length(meta_missing) > 0) stop("player_meta missing columns: ", paste(meta_missing, collapse = ", "))

# Determine join key: prefer player_id, fall back to player_name
has_player_id <- "player_id" %in% names(seasonal_xrapm) &&
  "player_id" %in% names(seasonal_spm) &&
  "player_id" %in% names(player_meta)
dedup_key <- if (has_player_id) "player_id" else "player_name"
cat("Join key:", dedup_key, "\n")

# Career-trait Panna (decay-weighted multi-season xRAPM): the HEADLINE rating, distinct
# from the per-season `xrapm` (this season's contribution). See panna/CLAUDE_TODO_CAREER_PANNA.md.
career_path <- "source/career_panna.parquet"
if (!file.exists(career_path)) stop("Required file not found: ", career_path,
  ". Add career_panna.parquet (ratings-data release) to the 'Download source data' step.")
career_panna <- read_parquet(career_path)
career_missing <- setdiff(c(dedup_key, "panna", "panna_offense", "panna_defense"), names(career_panna))
if (length(career_missing) > 0) stop("career_panna missing columns: ", paste(career_missing, collapse = ", "))
career <- career_panna |>
  group_by(.data[[dedup_key]]) |>
  slice_max(total_minutes, n = 1, with_ties = FALSE) |>
  ungroup() |>
  select(all_of(dedup_key), panna, panna_offense, panna_defense)

latest_season <- max(seasonal_xrapm$season_end_year)

xrapm <- seasonal_xrapm |>
  filter(season_end_year == latest_season) |>
  group_by(.data[[dedup_key]]) |>
  slice_max(total_minutes, n = 1, with_ties = FALSE) |>
  ungroup()

spm <- seasonal_spm |>
  filter(season_end_year == latest_season) |>
  group_by(.data[[dedup_key]]) |>
  slice_max(total_minutes, n = 1, with_ties = FALSE) |>
  ungroup() |>
  select(all_of(dedup_key), spm_overall = spm)

# Per-player EPR + PSR — latest weekly snapshot per player, from the opta-latest
# release (opta_epr_weekly.parquet / opta_psr_weekly.parquet). These feed the
# blog's "Piero" composite player rating (panna+EPR+PSR), the player-level
# analog of the team Tiento. OPTIONAL: join defensively so a missing/failed
# download degrades to NA (Piero renormalizes onto available components) rather
# than breaking the build. EPR weekly currently refreshes manually upstream, so
# it can lag — that's acceptable; PSR weekly refreshes on schedule.
load_latest_snapshot <- function(path, value_col) {
  if (!file.exists(path)) { cat("Optional source missing:", path, "-", value_col, "will be NA\n"); return(NULL) }
  df <- read_parquet(path)
  if (!all(c(dedup_key, value_col, "snapshot_date") %in% names(df))) {
    warning(path, " missing ", dedup_key, "/", value_col, "/snapshot_date - skipping"); return(NULL)
  }
  df |>
    filter(!is.na(.data[[dedup_key]])) |>
    group_by(.data[[dedup_key]]) |>
    slice_max(snapshot_date, n = 1, with_ties = FALSE) |>
    ungroup() |>
    select(all_of(dedup_key), all_of(value_col))
}
epr <- load_latest_snapshot("source/opta_epr_weekly.parquet", "epr")
psr <- load_latest_snapshot("source/opta_psr_weekly.parquet", "psr")

# Aggregate per-season EPV / WPA / PSV from per-match game-logs.parquet (if present).
# game-logs.parquet is downloaded into blog/ by the workflow before this script runs.
# We produce per-90 rates (to match the scale of panna/offense/defense) and join on dedup_key.
gl_path <- "blog/game-logs.parquet"
gl_extra <- NULL
if (file.exists(gl_path)) {
  game_logs <- read_parquet(gl_path)
  # Only pull columns that don't already exist in xrapm/spm (to avoid collisions).
  # panna/offense/defense/spm_overall/panna_percentile are excluded — those come from xrapm+spm.
  extra_cols <- intersect(
    c("epv_total",
      "epv_total_adj", "epv_offensive_adj", "epv_defensive_adj", "opp_adj",
      "epv_passing", "epv_shooting", "epv_dribbling",
      "epv_aerial", "epv_keeping", "epv_defending",
      "wpa_total", "wpa_as_actor", "wpa_as_receiver",
      "psv", "osv", "dsv", "panna_value_p90"),
    names(game_logs)
  )
  if (length(extra_cols) > 0 && dedup_key %in% names(game_logs) &&
      "total_minutes" %in% names(game_logs)) {
    gl_dt <- data.table::as.data.table(game_logs)
    # panna_value_p90 is already a per-90 rate — average weighted by minutes.
    # Everything else is per-match credit — sum then divide by season minutes * 90.
    rate_cols <- setdiff(extra_cols, "panna_value_p90")
    agg <- gl_dt[, c(
      list(gl_minutes = sum(total_minutes, na.rm = TRUE)),
      lapply(.SD, \(x) sum(x, na.rm = TRUE))
    ), by = dedup_key, .SDcols = rate_cols]
    if ("panna_value_p90" %in% extra_cols) {
      pvp_agg <- gl_dt[, .(
        panna_value_p90 = sum(panna_value_p90 * total_minutes, na.rm = TRUE) /
          pmax(sum(total_minutes, na.rm = TRUE), 1)
      ), by = dedup_key]
      agg <- merge(agg, pvp_agg, by = dedup_key, all.x = TRUE)
    }
    for (col in rate_cols) {
      data.table::set(
        agg, j = col,
        value = ifelse(agg$gl_minutes > 0,
                       round(agg[[col]] / agg$gl_minutes * 90, 4),
                       NA_real_)
      )
    }
    if ("panna_value_p90" %in% extra_cols) {
      data.table::set(agg, j = "panna_value_p90", value = round(agg$panna_value_p90, 4))
    }
    agg[, gl_minutes := NULL]
    gl_extra <- as.data.frame(agg)
    cat("game-logs enrichment:", nrow(gl_extra), "players,",
        length(extra_cols), "columns\n")
  } else {
    cat("game-logs found but missing required columns — skipping EPV/WPA/PSV enrichment\n")
  }
} else {
  cat("game-logs.parquet not present — ratings will lack EPV/WPA/PSV columns\n")
}

n_before <- nrow(xrapm)
n_excl <- 0L  # players dropped by BLOG_COMP_EXCLUDE (set below); keeps fan-out check honest
# Select only columns from player_meta that aren't already in xrapm (plus the join key)
meta_cols <- c(dedup_key, setdiff(names(player_meta), c(names(xrapm), "player_name")))
enriched <- xrapm |>
  left_join(spm, by = dedup_key) |>
  left_join(player_meta |> select(any_of(meta_cols)), by = dedup_key)
if (!is.null(gl_extra)) {
  enriched <- left_join(enriched, gl_extra, by = dedup_key)
}
# Join career-trait Panna onto this season's players (each current player carries
# their career rating). Keyed unique by dedup_key, so row count is unchanged.
enriched <- left_join(enriched, career, by = dedup_key)

# EPR / PSR (optional — present only if the opta-latest snapshots downloaded).
if (!is.null(epr)) enriched <- left_join(enriched, epr, by = dedup_key)
if (!is.null(psr)) enriched <- left_join(enriched, psr, by = dedup_key)

# Drop non-blog competitions (CAF_CL / Tunisian_Ligue_1) BEFORE ranking, so
# panna_rank + percentiles are computed over the blog pool only. `%in%` returns
# FALSE for NA league, so internationals / unmapped (NA league) are kept.
if ("league" %in% names(enriched)) {
  n_excl <- sum(enriched$league %in% BLOG_COMP_EXCLUDE, na.rm = TRUE)
  enriched <- enriched |> filter(!league %in% BLOG_COMP_EXCLUDE)
  cat("Excluded", n_excl, "players in non-blog comps:",
      paste(BLOG_COMP_EXCLUDE, collapse = ", "), "\n")
}

panna_ratings <- enriched |>
  # Season O/D split -> xrapm_offense/xrapm_defense; the career O/D (from the join)
  # becomes the headline offense/defense, matching `panna` = career.
  rename(xrapm_offense = offense, xrapm_defense = defense,
         offense = panna_offense, defense = panna_defense) |>
  mutate(
    # Ranks/percentiles are on the HEADLINE career Panna.
    panna_rank = as.integer(rank(-panna, ties.method = "min")),
    panna_percentile = round(100 * rank(panna, ties.method = "min") / n(), 1)
  ) |>
  # Position-stratified ranks/percentiles. Allows the blog UI to compare
  # players against peers in the same position bucket (e.g. "Salah is the
  # 99th-percentile striker" rather than just "99th overall"). Resolves the
  # confusion users get when a winger like L. Diaz appears among "top
  # defenders" — that's a team-effect artefact of additive RAPM that goes
  # away once you compare wingers vs wingers. Players with NA position
  # (no main starting position recorded) get NA percentiles.
  group_by(position) |>
  mutate(
    panna_rank_position = ifelse(is.na(position), NA_integer_,
                                  as.integer(rank(-panna, ties.method = "min"))),
    panna_percentile_position = ifelse(is.na(position), NA_real_,
                                        round(100 * rank(panna, ties.method = "min") / n(), 1)),
    offense_percentile_position = ifelse(is.na(position), NA_real_,
                                          round(100 * rank(offense, ties.method = "min") / n(), 1)),
    defense_percentile_position = ifelse(is.na(position), NA_real_,
                                          round(100 * rank(defense, ties.method = "min") / n(), 1))
  ) |>
  ungroup() |>
  select(
    panna_rank,
    # Opta player_id (UUID) — carried through so the blog can join players to
    # external registers (e.g. reep → Wikidata) by a stable ID instead of
    # fuzzy name matching, and route profiles by #id= rather than #name=.
    # any_of() so the build still works if a future source lacks it.
    any_of("player_id"),
    player_name, team, league, position,
    # Headline = career-trait Panna (career O/D); season xRAPM kept alongside.
    panna, offense, defense, spm_overall,
    # EPR/PSR trait ratings (latest weekly snapshot) — feed the blog's Piero
    # composite. any_of() so the build still works if the snapshots are absent.
    any_of(c("epr", "psr")),
    xrapm, xrapm_offense, xrapm_defense,
    total_minutes,
    panna_percentile,
    panna_rank_position, panna_percentile_position,
    offense_percentile_position, defense_percentile_position,
    any_of(c(
      "epv_total",
      "epv_total_adj", "epv_offensive_adj", "epv_defensive_adj", "opp_adj",
      "epv_passing", "epv_shooting", "epv_dribbling",
      "epv_aerial", "epv_keeping", "epv_defending",
      "wpa_total", "wpa_as_actor", "wpa_as_receiver",
      "psv", "osv", "dsv", "panna_value_p90"
    ))
  ) |>
  mutate(across(any_of(c("panna", "offense", "defense", "xrapm", "xrapm_offense",
                         "xrapm_defense", "spm_overall", "epr", "psr")),
                \(x) round(x, 4))) |>
  arrange(panna_rank)

na_spm <- sum(is.na(panna_ratings$spm_overall))
cat("SPM join:", nrow(panna_ratings) - na_spm, "/", nrow(panna_ratings),
    "matched (", round(100 * na_spm / nrow(panna_ratings), 1), "% missing)\n")
for (col in c("epr", "psr")) {
  if (col %in% names(panna_ratings)) {
    n_ok <- sum(!is.na(panna_ratings[[col]]))
    cat(toupper(col), "join:", n_ok, "/", nrow(panna_ratings),
        "matched (", round(100 * (1 - n_ok / nrow(panna_ratings)), 1), "% missing)\n")
  } else {
    cat(toupper(col), "absent — snapshot not downloaded; Piero will fall back to available components\n")
  }
}
# Minutes-gated drift companion: heavy-minutes players join SPM far more
# reliably than the tail, so a join-key drift spikes this rate toward 100%
# while structural growth moves it slowly. NOT a pure minutes gate — players
# in leagues without xmetrics features (the 2026-06 league additions) lack
# SPM at any minutes, hence the nonzero baseline.
heavy <- panna_ratings$total_minutes >= 900
na_spm_heavy <- sum(heavy & is.na(panna_ratings$spm_overall))
cat("SPM join (900+ min):", sum(heavy) - na_spm_heavy, "/", sum(heavy),
    "matched (", round(100 * na_spm_heavy / max(sum(heavy), 1), 1), "% missing)\n")
stopifnot(
  nrow(panna_ratings) == n_before - n_excl,  # joins didn't fan out (allowing the excluded comps)
  nrow(panna_ratings) > 0,
  # Join-drift gates, not coverage targets: real drift looks like ~100% NA.
  # Measured baselines on 2026-06-11 (the day MLS/Liga MX/Argentina/Saudi
  # shipped): global 25.1% (mid-season calendar-year leagues carry many
  # players with xRAPM evidence but no SPM row), 900+-minutes 15.5%.
  na_spm / nrow(panna_ratings) < 0.4,
  na_spm_heavy / max(sum(heavy), 1) < 0.3
)

# ── Piero: pool-independent composite player rating ──────────────────────────
# Faithful R port of inthegame-blog/football/player-rating.js
# `computePlayerRating(rows, { scaleTo: "panna" })`. Computed ONCE here over the
# canonical reference population (the full rated pool in ratings.parquet) and
# shipped as a `piero` column, so the blog reads it directly instead of
# recomputing pool-relative z-scores per page (which made the SAME player show
# different Piero on the player vs World Cup pages — see the rationale doc
# pannaverse/PIERO-POOL-INDEPENDENCE.md in the parent repo).
#
# Method (mirrors the JS exactly):
#   weights panna 0.5 / epr 0.3 / psr 0.2
#   1. per-metric POPULATION mean/sd over finite values (sd <- 1 if degenerate)
#   2. z_m = (value - mu_m) / sd_m  (NA where the metric is missing)
#   3. blend = sum(w_m * z_m) / sum(w_m) over the metrics PRESENT for that row
#      (weights renormalize on gaps; a panna-only player gets blend = z_panna)
#   4. blend POPULATION mean/sd over finite blends
#   5. piero = ((blend - mu_blend)/sd_blend) * sd_panna + mu_panna, rounded 4dp
# `panna` here is the CAREER trait (never the season xrapm) — guaranteed because
# panna_ratings$panna is the career column assembled above.
PIERO_WEIGHTS <- c(panna = 0.5, epr = 0.3, psr = 0.2)
piero_metrics <- names(PIERO_WEIGHTS)

# Population mean/sd over finite values; sd falls back to 1 (matches JS meanSd:
# empty -> {mu:0, sd:1}; sd uses /N, and `|| 1` makes a zero/degenerate sd -> 1).
.piero_mean_sd <- function(x) {
  v <- x[is.finite(x)]
  if (length(v) == 0L) return(c(mu = 0, sd = 1))
  mu <- mean(v)
  sd <- sqrt(mean((v - mu)^2))           # population sd (/N), as in the JS
  if (!is.finite(sd) || sd == 0) sd <- 1
  c(mu = mu, sd = sd)
}

# Only the rows that survive into the published parquet form the reference pool.
piero_present <- piero_metrics[piero_metrics %in% names(panna_ratings)]
piero_stat <- lapply(piero_present, function(m) .piero_mean_sd(panna_ratings[[m]]))
names(piero_stat) <- piero_present

# Per-metric z matrix (NA where the metric column is absent or value missing).
piero_z <- vapply(piero_metrics, function(m) {
  if (!m %in% piero_present) return(rep(NA_real_, nrow(panna_ratings)))
  (panna_ratings[[m]] - piero_stat[[m]]["mu"]) / piero_stat[[m]]["sd"]
}, numeric(nrow(panna_ratings)))
# vapply drops to a vector when nrow == 1; force a matrix so the rowSums work.
if (is.null(dim(piero_z))) piero_z <- matrix(piero_z, nrow = nrow(panna_ratings),
                                             dimnames = list(NULL, piero_metrics))

# Renormalized weighted z-blend per row.
w_vec  <- PIERO_WEIGHTS[piero_metrics]
w_mat  <- matrix(w_vec, nrow = nrow(piero_z), ncol = length(w_vec), byrow = TRUE)
have   <- is.finite(piero_z)
acc    <- rowSums(ifelse(have, w_mat * piero_z, 0))
sw     <- rowSums(ifelse(have, w_mat, 0))
piero_blend <- ifelse(sw > 0, acc / sw, NA_real_)

# Standardize the blend, then map onto panna's own mean/sd ("panna" scale).
blend_stat <- .piero_mean_sd(piero_blend)
panna_stat <- if ("panna" %in% names(piero_stat)) piero_stat[["panna"]] else c(mu = 0, sd = 1)
piero <- round(((piero_blend - blend_stat["mu"]) / blend_stat["sd"]) *
                 panna_stat["sd"] + panna_stat["mu"], 4)
panna_ratings$piero <- unname(piero)

cat("Piero:", sum(!is.na(panna_ratings$piero)), "/", nrow(panna_ratings),
    "players rated (coverage:",
    paste(sprintf("%s %d", piero_present,
                  vapply(piero_present, function(m) sum(is.finite(panna_ratings[[m]])), integer(1))),
          collapse = ", "), ")\n")

# Persist the reference constants so the WC squads build (panna step 12) can
# recompute Piero for the rare squad players absent from ratings.parquet using
# the SAME population stats (NEVER the squad pool's own stats — that reproduces
# the divergence bug). Squad players present in ratings.parquet get piero via a
# left join on player_id instead (identical number for free).
piero_reference <- list(
  weights = as.list(PIERO_WEIGHTS),
  metrics = lapply(piero_present, function(m) {
    list(mean = unname(piero_stat[[m]]["mu"]), sd = unname(piero_stat[[m]]["sd"]))
  }),
  blend = list(mean = unname(blend_stat["mu"]), sd = unname(blend_stat["sd"])),
  panna = list(mean = unname(panna_stat["mu"]), sd = unname(panna_stat["sd"])),
  n_reference = nrow(panna_ratings),
  built_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
)
names(piero_reference$metrics) <- piero_present

dir.create("blog", showWarnings = FALSE)
if (requireNamespace("jsonlite", quietly = TRUE)) {
  jsonlite::write_json(piero_reference, "blog/piero_reference.json",
                       auto_unbox = TRUE, pretty = TRUE, digits = 10)
  cat("Piero reference constants written to blog/piero_reference.json\n")
} else {
  # jsonlite absent: the `piero` column still ships (it needs no JSON); only the
  # WC-squad recompute-from-reference fallback loses its constants source.
  warning("jsonlite not installed — skipping blog/piero_reference.json (piero column still written)")
}

write_parquet(panna_ratings, "blog/ratings.parquet")
cat("ratings:", nrow(panna_ratings), "players (season", latest_season, ")\n")

# NOTE: Shot data, match-stats, match-shots, league-xg, and chains are built
# by dedicated workflow steps in build-blog-data.yml — not here.
# This script only builds ratings.parquet.
