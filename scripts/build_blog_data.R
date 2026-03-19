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
write_parquet(panna_ratings, "blog/ratings.parquet")
cat("ratings:", nrow(panna_ratings), "players (season", latest_season, ")\n")

# Shot data from Opta — tryCatch ensures shot failures don't block ratings
# when run locally or via source(). In GHA, build_shot_data.R runs as a separate step.
tryCatch({
  source("scripts/build_shot_data.R")
}, error = function(e) {
  warning("Shot data extraction failed, skipping panna_shots.parquet: ",
          conditionMessage(e), call. = FALSE)
  cat("::warning::Shot data extraction failed:", conditionMessage(e), "\n")
})

# ── Match stats from Opta player stats ─────────────────────────────────────
# Per-match player stats (goals, passes, tackles, etc.) for the football match page.
# Source: opta_player_stats.parquet from opta-latest release.
tryCatch({
  stats_file <- list.files("source", pattern = "^opta_player_stats\\.parquet$", full.names = TRUE)
  if (length(stats_file) == 0) {
    message("INFO: No opta_player_stats.parquet in source/ — skipping match-stats")
  } else {
    stats_raw <- read_parquet(stats_file[1])

    blog_comps <- c("EPL", "Championship", "La_Liga", "Ligue_1", "Bundesliga",
                    "Serie_A", "Eredivisie", "Primeira_Liga", "Scottish_Premiership",
                    "Super_Lig")
    comp_to_code <- c(
      EPL = "ENG", Championship = "ENG2", La_Liga = "ESP", Ligue_1 = "FRA",
      Bundesliga = "GER", Serie_A = "ITA", Eredivisie = "NED",
      Primeira_Liga = "POR", Scottish_Premiership = "SCO", Super_Lig = "TUR"
    )

    stats_filtered <- stats_raw |> filter(competition %in% blog_comps)

    # Keep only latest season for large leagues
    for (comp in blog_comps) {
      comp_rows <- stats_filtered |> filter(competition == comp)
      seasons <- sort(unique(comp_rows$season), decreasing = TRUE)
      if (nrow(comp_rows) > 50000 && length(seasons) > 1) {
        stats_filtered <- stats_filtered |> filter(!(competition == comp & season != seasons[1]))
      }
    }

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

    # Write per-league parquets (match page loads match-stats-{code}.parquet)
    for (comp in names(comp_to_code)) {
      code <- comp_to_code[comp]
      league_stats <- match_stats |> filter(league == code)
      if (nrow(league_stats) > 0) {
        write_parquet(league_stats, paste0("blog/match-stats-", code, ".parquet"))
        cat("match-stats-", code, ":", nrow(league_stats), "rows\n", sep = "")
      }
    }
  }
}, error = function(e) {
  message("::warning::Match stats processing failed: ", conditionMessage(e))
})

# ── Match shots parquet for xG timeline + shot map ────────────────────────
# Combines shot events with team info from lineups for the football match page.
tryCatch({
  shot_file <- list.files("source", pattern = "^opta_shot_events\\.parquet$", full.names = TRUE)
  lineup_file <- list.files("source", pattern = "^opta_lineups\\.parquet$", full.names = TRUE)
  if (length(shot_file) == 0 || length(lineup_file) == 0) {
    message("INFO: Missing shot events or lineups — skipping match-shots")
  } else {
    shots <- read_parquet(shot_file[1])
    lineups <- read_parquet(lineup_file[1],
      col_select = c("match_id", "player_id", "team_id", "team_name"))
    player_teams <- lineups |>
      distinct(match_id, player_id, team_id, team_name) |>
      rename(lineup_team_id = team_id, lineup_team_name = team_name)

    blog_comps <- c("EPL", "Championship", "La_Liga", "Ligue_1", "Bundesliga",
                    "Serie_A", "Eredivisie", "Primeira_Liga", "Scottish_Premiership",
                    "Super_Lig")

    shots_filtered <- shots |> filter(competition %in% blog_comps)
    has_team_id <- "team_id" %in% names(shots_filtered)
    has_team_name <- "team_name" %in% names(shots_filtered)

    shots_enriched <- shots_filtered |>
      left_join(player_teams, by = c("match_id", "player_id"))

    match_shots <- shots_enriched |>
      transmute(
        match_id,
        team_id = if (has_team_id) coalesce(team_id, lineup_team_id) else lineup_team_id,
        team_name = coalesce(lineup_team_name,
                             if (has_team_name) team_name else NA_character_),
        player_name,
        minute = as.integer(minute),
        second = as.integer(coalesce(second, 0)),
        x = round(x, 1),
        y = round(y, 1),
        type_id = as.integer(type_id),
        is_goal = type_id == 16L,
        xg = if ("xg" %in% names(shots_filtered)) round(xg, 3) else NA_real_
      ) |>
      filter(!is.na(match_id)) |>
      arrange(match_id, minute, second)

    write_parquet(match_shots, "blog/match-shots.parquet")
    cat("match-shots:", nrow(match_shots), "shot events\n")
  }
}, error = function(e) {
  message("::warning::Match shots processing failed: ", conditionMessage(e))
})

# ── League-level xG aggregation for standings page ─────────────────────────
# Sums shot-level xG per team per league per season from match-shots data.
# Also computes xGA (opponent xG). Output: blog/league-xg.parquet (~2KB).
tryCatch({
  if (exists("match_shots") && exists("match_stats")) {
    # Get match_id → league/season mapping from match_stats
    match_meta <- match_stats |>
      distinct(match_id, league, season)

    # Sum xG per team per match
    team_match_xg <- match_shots |>
      filter(!is.na(xg)) |>
      inner_join(match_meta, by = "match_id") |>
      group_by(match_id, league, season, team_name) |>
      summarise(xgf = sum(xg, na.rm = TRUE), .groups = "drop")

    # Compute xGA: for each match, each team's xGA = opponent's xGF
    match_teams <- team_match_xg |>
      group_by(match_id) |>
      mutate(xga = sum(xgf) - xgf) |>
      ungroup()

    # Aggregate to season level
    league_xg <- match_teams |>
      group_by(league, season, team_name) |>
      summarise(
        matches = n(),
        xgf = round(sum(xgf), 1),
        xga = round(sum(xga), 1),
        xgd = round(sum(xgf) - sum(xga), 1),
        .groups = "drop"
      ) |>
      arrange(league, desc(xgd))

    write_parquet(league_xg, "blog/league-xg.parquet")
    cat("league-xg:", nrow(league_xg), "team-seasons across",
        length(unique(league_xg$league)), "leagues\n")
  } else {
    message("INFO: match_shots or match_stats not available — skipping league-xg")
  }
}, error = function(e) {
  message("::warning::League xG aggregation failed: ", conditionMessage(e))
})

# ── Football chain parquets from Opta events ───────────────────────────────
# Per-league possession chain data for the football chain visualizer page.
# Source: events_{Competition}.parquet + opta_lineups.parquet from opta-latest release.
tryCatch({
  lineup_file <- list.files("source", pattern = "^opta_lineups\\.parquet$", full.names = TRUE)
  if (length(lineup_file) == 0) {
    message("INFO: No opta_lineups.parquet in source/ — skipping chains")
  } else {
    lineups <- read_parquet(lineup_file[1], col_select = c("match_id", "team_id", "team_name", "team_position"))
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
      event_file <- list.files("source", pattern = paste0("^events_", comp, "\\.parquet$"), full.names = TRUE)
      if (length(event_file) == 0) next
      cat("Chains:", comp, "...")

      events <- read_parquet(event_file[1])
      seasons <- sort(unique(events$season), decreasing = TRUE)
      if (nrow(events) > 200000 && length(seasons) > 1) {
        events <- events |> filter(season == seasons[1])
      }

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

      # Compute final_state per chain
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
      out_name <- paste0("chains-", league_code, ".parquet")
      write_parquet(chains, file.path("blog", out_name))
      cat(" ", nrow(chains), "chain actions\n")
    }
  }
}, error = function(e) {
  message("::warning::Football chain data processing failed: ", conditionMessage(e))
})
