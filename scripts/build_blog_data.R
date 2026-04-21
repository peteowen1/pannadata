library(arrow)
library(dplyr)

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
# Select only columns from player_meta that aren't already in xrapm (plus the join key)
meta_cols <- c(dedup_key, setdiff(names(player_meta), c(names(xrapm), "player_name")))
enriched <- xrapm |>
  left_join(spm, by = dedup_key) |>
  left_join(player_meta |> select(any_of(meta_cols)), by = dedup_key)
if (!is.null(gl_extra)) {
  enriched <- left_join(enriched, gl_extra, by = dedup_key)
}
panna_ratings <- enriched |>
  mutate(
    panna_rank = as.integer(rank(-xrapm, ties.method = "min")),
    panna_percentile = round(100 * rank(xrapm, ties.method = "min") / n(), 1)
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
                                  as.integer(rank(-xrapm, ties.method = "min"))),
    panna_percentile_position = ifelse(is.na(position), NA_real_,
                                        round(100 * rank(xrapm, ties.method = "min") / n(), 1)),
    offense_percentile_position = ifelse(is.na(position), NA_real_,
                                          round(100 * rank(offense, ties.method = "min") / n(), 1)),
    defense_percentile_position = ifelse(is.na(position), NA_real_,
                                          round(100 * rank(defense, ties.method = "min") / n(), 1))
  ) |>
  ungroup() |>
  select(
    panna_rank, player_name, team, league, position,
    panna = xrapm, offense, defense, spm_overall,
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
  mutate(across(c(panna, offense, defense, spm_overall), \(x) round(x, 4))) |>
  arrange(panna_rank)

na_spm <- sum(is.na(panna_ratings$spm_overall))
cat("SPM join:", nrow(panna_ratings) - na_spm, "/", nrow(panna_ratings),
    "matched (", round(100 * na_spm / nrow(panna_ratings), 1), "% missing)\n")
stopifnot(
  nrow(panna_ratings) == n_before,
  nrow(panna_ratings) > 0,
  na_spm / nrow(panna_ratings) < 0.2
)

dir.create("blog", showWarnings = FALSE)
write_parquet(panna_ratings, "blog/ratings.parquet")
cat("ratings:", nrow(panna_ratings), "players (season", latest_season, ")\n")

# NOTE: Shot data, match-stats, match-shots, league-xg, and chains are built
# by dedicated workflow steps in build-blog-data.yml — not here.
# This script only builds ratings.parquet.
