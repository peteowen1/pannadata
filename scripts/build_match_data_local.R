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

shots_filtered <- shots |> filter(competition %in% blog_comps)
has_team_id <- "team_id" %in% names(shots_filtered)

# Compute xG using panna's pre-trained XGBoost model
xg_model_path <- "source/xg_model.rds"
if (file.exists(xg_model_path)) {
  library(xgboost)
  xg_model <- readRDS(xg_model_path)
  cat("  Loaded xG model:", length(xg_model$panna_metadata$feature_cols), "features\n")

  distance_to_goal <- sqrt((100 - shots_filtered$x)^2 + (50 - shots_filtered$y)^2)
  dist_to_goal_line <- pmax(100 - shots_filtered$x, 0.1)
  goal_half_w <- 6
  angle_left  <- atan2(50 - goal_half_w - shots_filtered$y, dist_to_goal_line)
  angle_right <- atan2(50 + goal_half_w - shots_filtered$y, dist_to_goal_line)
  angle_to_goal <- abs(angle_right - angle_left)

  bp_lower <- tolower(shots_filtered$body_part)
  sit_lower <- tolower(shots_filtered$situation)

  features <- data.frame(
    x = shots_filtered$x, y = shots_filtered$y,
    distance_to_goal = distance_to_goal, angle_to_goal = angle_to_goal,
    in_penalty_area = as.integer(shots_filtered$x > 83 & shots_filtered$y > 21 & shots_filtered$y < 79),
    in_six_yard_box = as.integer(shots_filtered$x > 94 & shots_filtered$y > 37 & shots_filtered$y < 63),
    is_header = as.integer(grepl("head", bp_lower)),
    is_right_foot = as.integer(grepl("right", bp_lower)),
    is_left_foot = as.integer(grepl("left", bp_lower)),
    is_open_play = as.integer(grepl("open", sit_lower)),
    is_set_piece = as.integer(grepl("set", sit_lower)),
    is_corner = as.integer(grepl("corner", sit_lower)),
    is_direct_freekick = as.integer(grepl("free", sit_lower)),
    is_big_chance = as.integer(coalesce(shots_filtered$big_chance, 0L))
  )

  feature_cols <- xg_model$panna_metadata$feature_cols
  for (col in setdiff(feature_cols, names(features))) features[[col]] <- 0
  X <- as.matrix(features[, feature_cols, drop = FALSE])
  X[is.na(X)] <- 0

  shots_filtered$xg <- round(predict(xg_model$model, X), 3)
  cat("  xG predicted:", round(sum(shots_filtered$xg, na.rm = TRUE)), "total xG across",
      nrow(shots_filtered), "shots\n")
} else {
  shots_filtered$xg <- NA_real_
  cat("  WARNING: xg_model.rds not found — shots will have no xG\n")
}

shots_enriched <- shots_filtered |>
  left_join(player_teams, by = c("match_id", "player_id"))

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
    xg = round(xg, 3)
  ) |>
  filter(!is.na(match_id)) |>
  arrange(match_id, minute, second)

write_parquet(match_shots, "blog/match-shots.parquet")
cat("  match-shots:", nrow(match_shots), "shot events\n")

cat("Done! Files in blog/:\n")
system("ls -lh blog/*.parquet")
