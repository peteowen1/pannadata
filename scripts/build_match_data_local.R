# build_match_data_local.R — Build match-stats and match-shots parquets locally.
# Run from pannadata repo root: Rscript scripts/build_match_data_local.R
# Requires: source/opta_player_stats.parquet, source/opta_shot_events.parquet,
#           source/opta_lineups.parquet

library(arrow)
library(dplyr)

dir.create("blog", showWarnings = FALSE)

blog_comps <- c("EPL", "Championship", "La_Liga", "Ligue_1", "Bundesliga",
                "Serie_A", "Eredivisie", "Primeira_Liga", "Scottish_Premiership",
                "Super_Lig")
comp_to_code <- c(
  EPL = "ENG", Championship = "ENG2", La_Liga = "ESP", Ligue_1 = "FRA",
  Bundesliga = "GER", Serie_A = "ITA", Eredivisie = "NED",
  Primeira_Liga = "POR", Scottish_Premiership = "SCO", Super_Lig = "TUR"
)

# ── Match Stats ───────────────────────────────────────────────────────────
cat("Building match stats...\n")
stats_raw <- read_parquet("source/opta_player_stats.parquet")
stats_filtered <- stats_raw |> filter(competition %in% blog_comps)

# Include all seasons (historical match pages need stats too)

match_stats <- stats_filtered |>
  transmute(
    match_id, league = comp_to_code[competition], season, match_date,
    player_id, player_name, team_id, team_name, team_position, position,
    minsPlayed = as.integer(coalesce(minsPlayed, 0)),
    goals = as.integer(coalesce(goals, 0)),
    assists = as.integer(coalesce(goalAssist, 0)),
    shots = as.integer(coalesce(totalScoringAtt, 0)),
    shots_on_target = as.integer(coalesce(ontargetScoringAtt, 0)),
    passes = as.integer(coalesce(totalPass, 0)),
    passes_accurate = as.integer(coalesce(accuratePass, 0)),
    tackles = as.integer(coalesce(totalTackle, 0)),
    tackles_won = as.integer(coalesce(wonTackle, 0)),
    interceptions = as.integer(coalesce(interception, 0)),
    clearances = as.integer(coalesce(totalClearance, 0)),
    fouls = as.integer(coalesce(fouls, 0)),
    was_fouled = as.integer(coalesce(wasFouled, 0)),
    duels_won = as.integer(coalesce(duelWon, 0)),
    duels_lost = as.integer(coalesce(duelLost, 0)),
    aerials_won = as.integer(coalesce(aerialWon, 0)),
    aerials_lost = as.integer(coalesce(aerialLost, 0)),
    touches = as.integer(coalesce(touches, 0)),
    dispossessed = as.integer(coalesce(dispossessed, 0)),
    saves = as.integer(coalesce(saves, 0)),
    yellows = as.integer(coalesce(yellowCard, 0)),
    reds = as.integer(coalesce(redCard, 0)),
    big_chances_created = as.integer(coalesce(bigChanceCreated, 0)),
    key_passes = as.integer(coalesce(totalAttAssist, 0))
  ) |>
  filter(minsPlayed > 0) |>
  arrange(match_id, team_name, desc(minsPlayed))

for (comp in names(comp_to_code)) {
  code <- comp_to_code[comp]
  league_stats <- match_stats |> filter(league == code)
  if (nrow(league_stats) > 0) {
    write_parquet(league_stats, paste0("blog/match-stats-", code, ".parquet"))
    cat("  match-stats-", code, ": ", nrow(league_stats), " rows\n", sep = "")
  }
}
rm(stats_raw, stats_filtered, match_stats); gc()

# ── Match Shots ───────────────────────────────────────────────────────────
cat("Building match shots...\n")
shots <- read_parquet("source/opta_shot_events.parquet")
lineups <- read_parquet("source/opta_lineups.parquet",
  col_select = c("match_id", "player_id", "team_id", "team_name"))
player_teams <- lineups |>
  distinct(match_id, player_id, team_id, team_name) |>
  rename(lineup_team_id = team_id, lineup_team_name = team_name)

# period_id (1=H1, 2=H2, 3/4=ET, 5=shootout) disambiguates H1 stoppage-time
# minutes from H2 minutes of the same number, e.g. H1 46' vs H2 46' (pannadata#84).
# Not present on opta_shot_events.parquet itself, but IS on the raw match_events
# feed (events_consolidated) — carry it through via a (match_id, event_id) join,
# the same key backfill_goalmouth.py uses to backfill goalmouth placement.
events_dir <- if (dir.exists("source/events_consolidated")) {
  "source/events_consolidated"
} else if (dir.exists("data/opta/events_consolidated")) {
  "data/opta/events_consolidated"
} else {
  NA_character_
}
SHOT_TYPE_IDS <- c(13, 14, 15, 16)  # miss, post, saved, goal — same as backfill_goalmouth.py
if (!is.na(events_dir)) {
  # Filter pushdown to blog_comps + shot type_ids before collect() — scanning
  # the unfiltered dataset (all 100+ scraped competitions, ~80M+ event rows)
  # took 9+ minutes and was still running; scoping to what shots actually need
  # cuts it to ~1.2M rows in a few seconds.
  period_lookup <- open_dataset(events_dir) |>
    filter(competition %in% blog_comps, type_id %in% SHOT_TYPE_IDS, !is.na(period_id)) |>
    select(match_id, event_id, period_id) |>
    distinct(match_id, event_id, period_id) |>
    collect() |>
    mutate(match_id = as.character(match_id), event_id = as.numeric(event_id))
  cat("  period_id lookup:", nrow(period_lookup), "event rows from", events_dir, "\n")
} else {
  period_lookup <- tibble::tibble(match_id = character(), event_id = numeric(), period_id = numeric())
  cat("  WARNING: events_consolidated not found — match-shots will lack period_id\n")
}

shots_filtered <- shots |> filter(competition %in% blog_comps)
has_team_id <- "team_id" %in% names(shots_filtered)

# Compute xG using panna's pre-trained XGBoost model
xg_model_path <- "source/xg_model.rds"
if (file.exists(xg_model_path)) {
  library(xgboost)
  xg_model <- readRDS(xg_model_path)
  cat("  Loaded xG model:", length(xg_model$panna_metadata$feature_cols), "features\n")

  # One shared builder (scripts/xg_features.R), season_num included; stops on
  # any feature it cannot build rather than zero-filling it.
  source("scripts/xg_features.R")
  X <- xg_feature_matrix(shots_filtered, xg_model)

  shots_filtered$xg <- round(predict(xg_model$model, X), 3)
  cat("  xG predicted:", round(sum(shots_filtered$xg, na.rm = TRUE)), "total xG across",
      nrow(shots_filtered), "shots\n")
} else {
  shots_filtered$xg <- NA_real_
  cat("  WARNING: xg_model.rds not found — shots will have no xG\n")
}

shots_enriched <- shots_filtered |>
  mutate(match_id = as.character(match_id), event_id = as.numeric(event_id)) |>
  left_join(player_teams, by = c("match_id", "player_id")) |>
  left_join(period_lookup, by = c("match_id", "event_id"))

match_shots <- shots_enriched |>
  transmute(
    match_id,
    team_id = if (has_team_id) coalesce(team_id, lineup_team_id) else lineup_team_id,
    team_name = lineup_team_name,
    player_name,
    minute = as.integer(minute),
    second = as.integer(coalesce(second, 0)),
    x = round(x, 1),
    y = round(y, 1),
    type_id = as.integer(type_id),
    is_goal = type_id == 16L,
    period_id = as.integer(period_id),
    xg = round(xg, 3)
  ) |>
  filter(!is.na(match_id)) |>
  arrange(match_id, minute, second)

n_period <- sum(!is.na(match_shots$period_id))
cat("  period_id coverage:", n_period, "/", nrow(match_shots), "shots (",
    round(100 * n_period / nrow(match_shots), 1), "%)\n")

write_parquet(match_shots, "blog/match-shots.parquet")
cat("  match-shots:", nrow(match_shots), "shot events\n")

cat("Done! Files in blog/:\n")
system("ls -lh blog/*.parquet")
