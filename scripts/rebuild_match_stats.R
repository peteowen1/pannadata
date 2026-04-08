# Rebuild match-stats per league — memory-efficient (one league at a time)
library(arrow); library(dplyr)
dir.create("blog", showWarnings = FALSE)

source("scripts/league_config.R")
comp_to_code <- BLOG_COMP_TO_CODE

# Use arrow dataset for lazy evaluation — avoids loading 247MB into R memory at once
ds <- open_dataset("source/opta_player_stats.parquet", format = "parquet")

for (comp in names(comp_to_code)) {
  code <- comp_to_code[comp]

  league_stats <- ds |>
    filter(competition == comp) |>
    select(match_id, season, match_date, player_id, player_name, team_id,
           team_name, team_position, position, minsPlayed, goals,
           goalAssist, totalScoringAtt, ontargetScoringAtt, totalPass,
           accuratePass, totalTackle, wonTackle, interception, totalClearance,
           fouls, wasFouled, duelWon, duelLost, aerialWon, aerialLost,
           touches, dispossessed, saves, yellowCard, redCard,
           bigChanceCreated, totalAttAssist) |>
    collect()

  if (nrow(league_stats) == 0) next

  match_stats <- league_stats |>
    transmute(
      match_id, league = code, season, match_date,
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

  write_parquet(match_stats, paste0("blog/match-stats-", code, ".parquet"))
  cat(code, ":", nrow(match_stats), "rows,", length(unique(match_stats$season)), "seasons\n")
  rm(league_stats, match_stats); gc(verbose = FALSE)
}
