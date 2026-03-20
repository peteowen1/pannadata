#!/usr/bin/env Rscript
# Build football chain parquets in CI — one league at a time to manage memory.
# Downloads event files from opta-latest release as needed.
# Usage: Rscript scripts/build_chains_ci.R
# Requires: source/opta_lineups.parquet (pre-downloaded)
# Outputs: blog/chains-{CODE}.parquet

library(arrow)
library(dplyr)

src_dir <- "source"
out_dir <- "blog"
dir.create(out_dir, showWarnings = FALSE)

cat("=== Chain Builder (CI) ===\n")
cat("Memory:", round(as.numeric(system("free -m | awk '/Mem:/{print $2}'", intern = TRUE)), 0), "MB total\n\n")

# Load lineups once (shared across all leagues)
lineup_file <- file.path(src_dir, "opta_lineups.parquet")
if (!file.exists(lineup_file)) stop("Missing: ", lineup_file)

cat("Loading lineups...\n")
lineups <- read_parquet(lineup_file,
  col_select = c("match_id", "team_id", "team_name", "team_position"))
match_teams <- lineups |>
  distinct(match_id, team_id, team_name, team_position) |>
  group_by(match_id) |>
  summarise(
    home_team = team_name[team_position == "home"][1],
    away_team = team_name[team_position == "away"][1],
    .groups = "drop"
  )
global_team_map <- lineups |> distinct(team_id, team_name) |>
  group_by(team_id) |> slice(1) |> ungroup()
rm(lineups); gc(verbose = FALSE)
cat("Lineups loaded:", nrow(match_teams), "matches\n\n")

blog_comps <- c("EPL", "Championship", "La_Liga", "Ligue_1", "Bundesliga",
                "Serie_A", "Eredivisie", "Primeira_Liga", "Scottish_Premiership",
                "Super_Lig")
comp_to_code <- c(
  EPL = "ENG", Championship = "ENG2", La_Liga = "ESP", Ligue_1 = "FRA",
  Bundesliga = "GER", Serie_A = "ITA", Eredivisie = "NED",
  Primeira_Liga = "POR", Scottish_Premiership = "SCO", Super_Lig = "TUR"
)
dead_ball_types <- c(2L, 4L, 5L, 6L, 17L, 55L, 56L, 57L, 70L, 80L, 81L)
non_play_types <- c(18L, 19L, 24L, 27L, 28L, 30L, 32L, 34L, 37L, 40L, 43L, 65L, 68L)
shot_types <- c(13L, 14L, 15L, 16L)

for (comp in blog_comps) {
  event_file <- file.path(src_dir, paste0("events_", comp, ".parquet"))

  # Download if not present (CI downloads one at a time to save disk)
  if (!file.exists(event_file)) {
    cat("Downloading events_", comp, ".parquet...\n", sep = "")
    dl <- system2("gh", c("release", "download", "opta-latest",
      "-p", paste0("events_", comp, ".parquet"), "-D", src_dir),
      stdout = TRUE, stderr = TRUE)
    if (!file.exists(event_file)) { cat("  SKIP (download failed)\n"); next }
  }

  cat(comp, ": ", sep = "")
  events <- read_parquet(event_file)
  seasons <- sort(unique(events$season), decreasing = TRUE)
  if (nrow(events) > 200000 && length(seasons) > 1) {
    events <- events |> filter(season == seasons[1])
  }
  cat(nrow(events), " events (", seasons[1], ") ... ", sep = "")

  events <- events |>
    filter(!type_id %in% non_play_types) |>
    arrange(match_id, period_id, minute, second, event_id)

  # Build chain numbers
  events$chain_number <- NA_integer_
  chain_n <- 0L
  prev_team <- ""
  prev_match <- ""
  for (i in seq_len(nrow(events))) {
    new_chain <- events$match_id[i] != prev_match ||
      (events$team_id[i] != prev_team && events$team_id[i] != "") ||
      events$type_id[i] %in% dead_ball_types
    if (new_chain) chain_n <- chain_n + 1L
    events$chain_number[i] <- chain_n
    prev_team <- events$team_id[i]
    prev_match <- events$match_id[i]
  }

  chain_states <- events |>
    group_by(chain_number) |>
    summarise(
      final_state = case_when(
        any(type_id == 16L) ~ "goal",
        any(type_id %in% c(13L, 14L, 15L)) ~ "shot_saved",
        any(type_id %in% c(5L, 6L)) ~ "out_of_play",
        any(type_id == 4L) ~ "foul",
        any(type_id %in% c(70L, 2L)) ~ "offside",
        TRUE ~ "lost_possession"
      ),
      .groups = "drop"
    )

  chains <- events |>
    left_join(chain_states, by = "chain_number") |>
    group_by(match_id, chain_number) |>
    mutate(display_order = row_number()) |>
    ungroup() |>
    left_join(match_teams, by = "match_id") |>
    left_join(global_team_map |> rename(team_name_lookup = team_name), by = "team_id") |>
    transmute(
      match_id, chain_number = as.integer(chain_number),
      display_order = as.integer(display_order),
      player_id, player_name, team_id,
      x = round(x, 1), y = round(y, 1),
      end_x = round(end_x, 1), end_y = round(end_y, 1),
      type_id = as.integer(type_id), outcome = as.integer(outcome),
      final_state, period_id = as.integer(period_id),
      minute = as.integer(minute), second = as.integer(second),
      is_shot = type_id %in% shot_types,
      competition, season,
      team_name = coalesce(team_name_lookup, team_id),
      home_team = coalesce(home_team, "Unknown"),
      away_team = coalesce(away_team, "Unknown")
    ) |>
    select(-any_of("team_name_lookup")) |>
    arrange(match_id, chain_number, display_order)

  league_code <- comp_to_code[comp]
  out_path <- file.path(out_dir, paste0("chains-", league_code, ".parquet"))
  write_parquet(chains, out_path)
  cat(nrow(chains), " chains -> ", out_path, "\n", sep = "")

  # Free memory before next league
  rm(events, chains, chain_states); gc(verbose = FALSE)

  # Delete event file to save disk space in CI
  unlink(event_file)
}

cat("\nDone!\n")
