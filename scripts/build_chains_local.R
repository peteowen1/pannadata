#!/usr/bin/env Rscript
# Build football chain parquets locally from Opta event files.
# Usage: Rscript scripts/build_chains_local.R
# Reads from data/opta/, writes to blog/

library(arrow)
library(dplyr)

opta_dir <- "data/opta"
out_dir  <- "blog"
dir.create(out_dir, showWarnings = FALSE)

cat("Loading lineups...\n")
lineups <- read_parquet(file.path(opta_dir, "opta_lineups.parquet"),
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
rm(lineups); gc()

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

# Failed duel/contest touches from the team that does NOT currently have the
# ball must not end the attacking team's possession. Take-On (3), Tackle (7),
# Good Skill / take-on class (42), Aerial (44), Challenge i.e. Opta's
# "dribbled past" marker (45), and Att One-On-One (83) are logged for BOTH
# sides of a contested-ball duel. When such a row's team_id differs from the
# current possessor (prev_team) AND its outcome is a FAILURE (outcome != 1),
# the non-possessing team's attempt to win the ball back failed, so
# possession never actually changed hands — it must not trigger the
# team-change split below. A row that WINS the duel (outcome == 1) is left to
# split normally: the winning team's own NEXT event still differs from
# prev_team and starts a new chain correctly, so genuine turnovers via a won
# tackle/take-on are unaffected — only the failed contest row itself is
# suppressed. Same fix as build_chains_ci.R, which additionally carries
# keeper-rebound transparency (pannadata#76) that this local script never
# had ported to it — that gap is pre-existing and out of scope here.
duel_contest_types <- c(3L, 7L, 42L, 44L, 45L, 83L)

# Vectorized chain numbering (pannadata#111 / FABLE-111 follow-up) -- this
# script had the exact same per-row loop build_chains_ci.R was fixed for
# (same shape, up to 200k rows/competition). Reuses the validated
# compute_chain_numbers() with keeper_rebound_types = integer(0): this
# script never implemented keeper-rebound transparency (see the comment
# above), and the empty vector preserves that EXACT pre-existing behavior
# (type_id %in% integer(0) is always FALSE) rather than silently also
# fixing that separate, pre-existing gap in the same change.
source("scripts/chain_numbering.R")

for (comp in blog_comps) {
  event_file <- file.path(opta_dir, paste0("events_", comp, ".parquet"))
  if (!file.exists(event_file)) { cat("SKIP:", comp, "(file not found)\n"); next }
  cat("Chains:", comp, "... ")

  events <- read_parquet(event_file)
  seasons <- sort(unique(events$season), decreasing = TRUE)
  if (nrow(events) > 200000 && length(seasons) > 1) {
    events <- events |> filter(season == seasons[1])
  }
  cat(nrow(events), "events (season", seasons[1], ") ... ")

  events <- events |>
    filter(!type_id %in% non_play_types) |>
    arrange(match_id, period_id, minute, second, event_id)

  # Build chain numbers (vectorized -- see the source() call above)
  events$chain_number <- compute_chain_numbers(
    events, dead_ball_types, integer(0), duel_contest_types)

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
  cat(nrow(chains), "chain actions ->", out_path, "\n")
  rm(events, chains, chain_states); gc()
}

cat("\nDone! Chain parquets written to", out_dir, "\n")
