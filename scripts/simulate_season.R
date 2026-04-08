# simulate_season.R — Monte Carlo season simulations for football leagues
# Generates football/simulations.parquet for the inthegame-blog leagues page.
#
# Requires:
#   - blog/predictions.parquet: remaining fixtures with Panna probabilities
#   - blog/season_standings.parquet: current standings (points, GD, games played)
#
# Usage: Rscript scripts/simulate_season.R
# Output: blog/simulations.parquet

library(arrow)
library(dplyr)

N_SIMS <- 10000L

cat("=== Football Season Simulator ===\n")
cat("Simulations:", N_SIMS, "\n\n")

# ── Load predictions (remaining fixtures with Panna probabilities) ──────
pred_file <- "blog/predictions.parquet"
if (!file.exists(pred_file)) stop("predictions.parquet not found. Run build_blog_data.R first.")

predictions <- read_parquet(pred_file)
cat("Loaded", nrow(predictions), "match predictions\n")

# ── Load current standings ────────────────────────────────────────────
standings_file <- "blog/season_standings.parquet"
if (file.exists(standings_file)) {
  standings <- read_parquet(standings_file)
  cat("Loaded standings for", nrow(standings), "teams\n")
} else {
  warning("season_standings.parquet not found — probabilities will be based on remaining games only.")
  standings <- NULL
}

# ── Filter to domestic leagues only (cups have TBD matchups) ──────────
league_codes <- c("ENG", "ENG2", "ESP", "FRA", "GER", "ITA", "NED", "POR", "SCO", "TUR")
predictions <- predictions |>
  filter(league %in% league_codes, home_team != "", away_team != "")

leagues <- unique(predictions$league)
cat("Leagues:", paste(leagues, collapse = ", "), "\n")
cat("Matches after filtering:", nrow(predictions), "\n\n")

# ── Simulate one season for a league ───────────────────────────────────
simulate_league <- function(preds, league_standings = NULL, n_sims = N_SIMS) {
  teams <- sort(unique(c(preds$home_team, preds$away_team)))
  n_teams <- length(teams)

  # Build current standings lookup
  cur_pts <- setNames(rep(0L, n_teams), teams)
  cur_gd <- setNames(rep(0, n_teams), teams)
  cur_gp <- setNames(rep(0L, n_teams), teams)

  if (!is.null(league_standings)) {
    for (i in seq_len(nrow(league_standings))) {
      tm <- league_standings$team[i]
      if (tm %in% teams) {
        cur_pts[tm] <- league_standings$points[i]
        cur_gd[tm] <- league_standings$gd[i]
        cur_gp[tm] <- league_standings$games_played[i]
      }
    }
  }

  # Initialize result arrays (remaining games only — added to current later)
  sim_points <- matrix(0L, nrow = n_sims, ncol = n_teams)
  sim_gd <- matrix(0, nrow = n_sims, ncol = n_teams)
  colnames(sim_points) <- teams
  colnames(sim_gd) <- teams

  for (sim in seq_len(n_sims)) {
    for (i in seq_len(nrow(preds))) {
      m <- preds[i, ]
      r <- runif(1)
      if (r < m$prob_H) {
        # Home win
        sim_points[sim, m$home_team] <- sim_points[sim, m$home_team] + 3L
        sim_gd[sim, m$home_team] <- sim_gd[sim, m$home_team] + (m$pred_home_goals - m$pred_away_goals)
        sim_gd[sim, m$away_team] <- sim_gd[sim, m$away_team] - (m$pred_home_goals - m$pred_away_goals)
      } else if (r < m$prob_H + m$prob_D) {
        # Draw
        sim_points[sim, m$home_team] <- sim_points[sim, m$home_team] + 1L
        sim_points[sim, m$away_team] <- sim_points[sim, m$away_team] + 1L
      } else {
        # Away win
        sim_points[sim, m$away_team] <- sim_points[sim, m$away_team] + 3L
        sim_gd[sim, m$away_team] <- sim_gd[sim, m$away_team] + (m$pred_away_goals - m$pred_home_goals)
        sim_gd[sim, m$home_team] <- sim_gd[sim, m$home_team] - (m$pred_away_goals - m$pred_home_goals)
      }
    }
  }

  # Add current standings to each simulation for ranking
  total_points <- sweep(sim_points, 2, cur_pts, "+")
  total_gd <- sweep(sim_gd, 2, cur_gd, "+")

  # Compute positions per simulation (using full-season totals)
  positions <- matrix(0L, nrow = n_sims, ncol = n_teams)
  colnames(positions) <- teams
  for (sim in seq_len(n_sims)) {
    pts <- total_points[sim, ]
    gd <- total_gd[sim, ]
    ord <- order(-pts, -gd)
    positions[sim, ord] <- seq_along(ord)
  }

  # Aggregate results
  results <- tibble(
    team = teams,
    avg_points = round(colMeans(sim_points), 1),
    avg_gd = round(colMeans(sim_gd), 1),
    avg_position = round(colMeans(positions), 1),
    title_pct = round(colMeans(positions == 1), 4),
    top_4_pct = round(colMeans(positions <= 4), 4),
    top_half_pct = round(colMeans(positions <= ceiling(n_teams / 2)), 4),
    bottom_3_pct = round(colMeans(positions > n_teams - 3), 4),
    current_points = as.integer(cur_pts[teams]),
    current_gd = as.integer(cur_gd[teams]),
    games_played = as.integer(cur_gp[teams])
  )

  # Position distribution (pos_1_pct through pos_N_pct)
  for (pos in seq_len(n_teams)) {
    results[[paste0("pos_", pos, "_pct")]] <- round(colMeans(positions == pos), 4)
  }

  results
}

# ── Run simulations per league ─────────────────────────────────────────
all_results <- list()

for (league in leagues) {
  league_preds <- predictions |> filter(league == !!league)

  # Get standings for this league
  league_standings <- NULL
  if (!is.null(standings)) {
    league_standings <- standings |> filter(league == !!league)
    if (nrow(league_standings) == 0) league_standings <- NULL
  }

  # Determine season and current matchday
  season <- unique(league_preds$season)[1]
  matchdays <- length(unique(league_preds$match_date))

  standings_label <- if (!is.null(league_standings)) {
    sprintf("%d teams with standings", nrow(league_standings))
  } else {
    "no standings"
  }

  cat("Simulating", league, ":", nrow(league_preds), "remaining matches,",
      length(unique(c(league_preds$home_team, league_preds$away_team))), "teams,",
      standings_label, "\n")

  result <- simulate_league(league_preds, league_standings, N_SIMS)
  result$league <- league
  result$season <- season %||% ""
  result$matchday <- as.integer(matchdays)
  result$n_sims <- N_SIMS

  all_results[[league]] <- result
}

simulations <- bind_rows(all_results)

dir.create("blog", showWarnings = FALSE)
write_parquet(simulations, "blog/simulations.parquet")
cat("\nsimulations:", nrow(simulations), "team-leagues across",
    length(unique(simulations$league)), "leagues\n")
