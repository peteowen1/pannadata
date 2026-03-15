# Quick rebuild of match-stats with all seasons
library(arrow); library(dplyr)
dir.create("blog", showWarnings = FALSE)

comp_to_code <- c(
  EPL = "ENG", Championship = "ENG2", La_Liga = "ESP", Ligue_1 = "FRA",
  Bundesliga = "GER", Serie_A = "ITA", Eredivisie = "NED",
  Primeira_Liga = "POR", Scottish_Premiership = "SCO", Super_Lig = "TUR"
)

stats_raw <- read_parquet("source/opta_player_stats.parquet")
stats_filtered <- stats_raw |> filter(competition %in% names(comp_to_code))

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

for (code in unique(match_stats$league)) {
  league_stats <- match_stats |> filter(league == code)
  write_parquet(league_stats, paste0("blog/match-stats-", code, ".parquet"))
  cat(code, ":", nrow(league_stats), "rows,", length(unique(league_stats$season)), "seasons\n")
}
