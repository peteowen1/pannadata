library(arrow)
library(dplyr)

# Shared blog league config (BLOG_COMP_EXCLUDE — comps we drop from blog outputs).
source("scripts/league_config.R")

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
start_year <- as.integer(substr(latest_season, 1, 4))
previous_season <- sprintf("%d-%d", start_year - 1L, start_year)
cat("Latest season:", latest_season, "| previous:", previous_season, "\n")

# R195: each player's OWN latest season, not one season for everyone. Early in
# a season most players have no row in it yet (2026-10-14: 12,282 players in
# 2026-2027 vs 46,101 in 2025-2026), and a league the scrape has not reached
# would vanish entirely. Players last seen in the previous season keep their
# team and league from it, flagged by is_previous_season.
played <- lu |>
  filter(season %in% c(latest_season, previous_season), minutes_played > 0)
player_season <- played |>
  distinct(player_id, season) |>
  group_by(player_id) |>
  summarise(season = max(season), .groups = "drop")
current <- played |>
  semi_join(player_season, by = c("player_id", "season"))

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
  left_join(player_names, by = "player_id") |>
  left_join(player_season, by = "player_id") |>
  mutate(is_previous_season = season != latest_season)

stopifnot(!anyDuplicated(player_meta$player_id), !anyNA(player_meta$season))

# Drop players whose MAIN competition is a non-blog comp (CAF_CL / Tunisian_Ligue_1).
# Players who also feature in a covered domestic league keep that league (main_team_league
# picks the most-frequent comp), so only pure CAF/Tunisian players are removed.
n_excl <- sum(player_meta$league %in% BLOG_COMP_EXCLUDE, na.rm = TRUE)
player_meta <- player_meta |> filter(!league %in% BLOG_COMP_EXCLUDE)
cat("Excluded", n_excl, "players whose main comp is non-blog:",
    paste(BLOG_COMP_EXCLUDE, collapse = ", "), "\n")

cat("Player metadata:", nrow(player_meta), "players (",
    sum(!player_meta$is_previous_season), latest_season, "+",
    sum(player_meta$is_previous_season), previous_season, "only )\n")
cat("Position coverage:", round(100 * mean(!is.na(player_meta$position)), 1), "%\n")

dir.create("blog", showWarnings = FALSE)
write_parquet(player_meta, "blog/player-details.parquet")
cat("Saved blog/player-details.parquet\n")
