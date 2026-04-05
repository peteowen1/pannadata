library(arrow)
library(dplyr)

# Support both local (data/opta/) and CI (source/) paths
lu_path <- if (file.exists("source/opta_lineups.parquet")) {
  "source/opta_lineups.parquet"
} else {
  "data/opta/opta_lineups.parquet"
}
lu <- read_parquet(lu_path)

# Use the main league season format (e.g. "2024-2025"), not tournament seasons
league_seasons <- grep("^\\d{4}-\\d{4}$", unique(lu$season), value = TRUE)
latest_season <- max(league_seasons)
cat("Latest season:", latest_season, "\n")

current <- lu |>
  filter(season == latest_season, minutes_played > 0)

# Most common team & league per player (by appearances, not most recent)
main_team_league <- current |>
  count(player_id, team_name, competition, sort = TRUE) |>
  group_by(player_id) |>
  slice_max(n, n = 1, with_ties = FALSE) |>
  ungroup() |>
  select(player_id, team = team_name, league = competition)

# Most common starting position per player (ignore "Substitute" and blanks)
main_position <- current |>
  filter(is_starter, position != "", position != "Substitute") |>
  count(player_id, position, sort = TRUE) |>
  group_by(player_id) |>
  slice_max(n, n = 1, with_ties = FALSE) |>
  ungroup() |>
  select(player_id, position)

# Canonical player_name per player_id (lineups may have slight name variants)
player_names <- current |>
  distinct(player_id, player_name) |>
  group_by(player_id) |>
  slice(1) |>
  ungroup()

player_meta <- main_team_league |>
  left_join(main_position, by = "player_id") |>
  left_join(player_names, by = "player_id")

cat("Player metadata:", nrow(player_meta), "players\n")
cat("Position coverage:", round(100 * mean(!is.na(player_meta$position)), 1), "%\n")

dir.create("blog", showWarnings = FALSE)
write_parquet(player_meta, "blog/player-details.parquet")
cat("Saved blog/player-details.parquet\n")
