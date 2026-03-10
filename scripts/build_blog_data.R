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

n_before <- nrow(xrapm)
# Select only columns from player_meta that aren't already in xrapm (plus the join key)
meta_cols <- c(dedup_key, setdiff(names(player_meta), c(names(xrapm), "player_name")))
panna_ratings <- xrapm |>
  left_join(spm, by = dedup_key) |>
  left_join(player_meta |> select(any_of(meta_cols)), by = dedup_key) |>
  mutate(
    panna_rank = as.integer(rank(-xrapm, ties.method = "min")),
    panna_percentile = round(100 * rank(xrapm, ties.method = "min") / n(), 1)
  ) |>
  select(
    panna_rank, player_name, team, league, position,
    panna = xrapm, offense, defense, spm_overall,
    total_minutes, panna_percentile
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
write_parquet(panna_ratings, "blog/panna_ratings.parquet")
cat("panna_ratings:", nrow(panna_ratings), "players (season", latest_season, ")\n")

# Shot data from Opta — tryCatch ensures shot failures don't block ratings
# when run locally or via source(). In GHA, build_shot_data.R runs as a separate step.
tryCatch({
  source("scripts/build_shot_data.R")
}, error = function(e) {
  warning("Shot data extraction failed, skipping panna_shots.parquet: ",
          conditionMessage(e), call. = FALSE)
  cat("::warning::Shot data extraction failed:", conditionMessage(e), "\n")
})
