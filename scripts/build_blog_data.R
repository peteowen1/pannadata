library(arrow)
library(dplyr)

# Player ratings - latest season xRAPM + SPM
seasonal_xrapm <- read_parquet("source/seasonal_xrapm.parquet")
seasonal_spm <- read_parquet("source/seasonal_spm.parquet")

latest_season <- max(seasonal_xrapm$season_end_year)

xrapm <- seasonal_xrapm |>
  filter(season_end_year == latest_season) |>
  group_by(player_name) |>
  slice_max(total_minutes, n = 1, with_ties = FALSE) |>
  ungroup()

spm <- seasonal_spm |>
  filter(season_end_year == latest_season) |>
  group_by(player_name) |>
  slice_max(total_minutes, n = 1, with_ties = FALSE) |>
  ungroup() |>
  select(player_name, spm_overall = spm)

panna_ratings <- xrapm |>
  left_join(spm, by = "player_name") |>
  mutate(
    panna_rank = as.integer(rank(-xrapm, ties.method = "min")),
    panna_percentile = round(100 * rank(xrapm, ties.method = "min") / n(), 1)
  ) |>
  select(
    panna_rank, player_name,
    panna = xrapm, offense, defense, spm_overall,
    total_minutes, panna_percentile
  ) |>
  mutate(across(c(panna, offense, defense, spm_overall), \(x) round(x, 4))) |>
  arrange(panna_rank)

stopifnot(nrow(panna_ratings) > 0)

dir.create("blog", showWarnings = FALSE)
write_parquet(panna_ratings, "blog/panna_ratings.parquet")
cat("panna_ratings:", nrow(panna_ratings), "players (season", latest_season, ")\n")
