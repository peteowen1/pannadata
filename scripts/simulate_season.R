# simulate_season.R — Monte Carlo season simulations for football leagues
# Generates football/simulations.parquet for the inthegame-blog ladder page.
#
# Requires: predictions.parquet (from build_blog_data.R) with remaining fixtures
#   and team-level strength estimates from Panna ratings.
#
# Usage: Rscript scripts/simulate_season.R
# Output: blog/simulations.parquet
#
# TODO: Hook into Panna model's team strength estimates.
#       Currently uses a placeholder that needs replacing with actual
#       panna::predict_match() or equivalent.

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

# ── Load fixture results for current standings ──────────────────────────
# The fixtures JSON has actual results for played matches.
# For now, we compute current standings from predictions that have actual results.
# TODO: Load fixtures.json or use a separate results source.

# ── Filter to domestic leagues only (cups have TBD matchups) ──────────
league_codes <- c("ENG", "ENG2", "ESP", "FRA", "GER", "ITA", "NED", "POR", "SCO", "TUR")
predictions <- predictions |>
  filter(league %in% league_codes, home_team != "", away_team != "")

leagues <- unique(predictions$league)
cat("Leagues:", paste(leagues, collapse = ", "), "\n")
cat("Matches after filtering:", nrow(predictions), "\n\n")

# ── Simulate one season for a league ───────────────────────────────────
simulate_league <- function(preds, n_sims = N_SIMS) {
  teams <- sort(unique(c(preds$home_team, preds$away_team)))
  n_teams <- length(teams)

  # Initialize result arrays
  total_points <- matrix(0L, nrow = n_sims, ncol = n_teams)
  total_gd <- matrix(0, nrow = n_sims, ncol = n_teams)
  colnames(total_points) <- teams
  colnames(total_gd) <- teams

  for (sim in seq_len(n_sims)) {
    for (i in seq_len(nrow(preds))) {
      m <- preds[i, ]
      # Sample match result from Panna probabilities
      r <- runif(1)
      if (r < m$prob_H) {
        # Home win
        total_points[sim, m$home_team] <- total_points[sim, m$home_team] + 3L
        total_gd[sim, m$home_team] <- total_gd[sim, m$home_team] + (m$pred_home_goals - m$pred_away_goals)
        total_gd[sim, m$away_team] <- total_gd[sim, m$away_team] - (m$pred_home_goals - m$pred_away_goals)
      } else if (r < m$prob_H + m$prob_D) {
        # Draw
        total_points[sim, m$home_team] <- total_points[sim, m$home_team] + 1L
        total_points[sim, m$away_team] <- total_points[sim, m$away_team] + 1L
      } else {
        # Away win
        total_points[sim, m$away_team] <- total_points[sim, m$away_team] + 3L
        total_gd[sim, m$away_team] <- total_gd[sim, m$away_team] + (m$pred_away_goals - m$pred_home_goals)
        total_gd[sim, m$home_team] <- total_gd[sim, m$home_team] - (m$pred_away_goals - m$pred_home_goals)
      }
    }
  }

  # Compute positions per simulation
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
    avg_points = round(colMeans(total_points), 1),
    avg_gd = round(colMeans(total_gd), 1),
    avg_position = round(colMeans(positions), 1),
    title_pct = round(colMeans(positions == 1), 4),
    top_4_pct = round(colMeans(positions <= 4), 4),
    top_half_pct = round(colMeans(positions <= ceiling(n_teams / 2)), 4),
    bottom_3_pct = round(colMeans(positions > n_teams - 3), 4)
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

  # Determine season and current matchday
  season <- unique(league_preds$season)[1]
  # Matchday approximation: count distinct dates
  matchdays <- length(unique(league_preds$match_date))

  cat("Simulating", league, ":", nrow(league_preds), "remaining matches,",
      length(unique(c(league_preds$home_team, league_preds$away_team))), "teams\n")

  result <- simulate_league(league_preds, N_SIMS)
  result$league <- league
  result$season <- season %||% ""
  result$matchday <- as.integer(matchdays)
  result$n_sims <- N_SIMS

  # TODO: Add current standings (games_played, current_points, current_gd)
  # These come from fixture results for already-played matches.
  # For now, set to NA — the front-end handles missing values gracefully.
  result$games_played <- NA_integer_
  result$current_points <- NA_integer_
  result$current_gd <- NA_integer_

  all_results[[league]] <- result
}

simulations <- bind_rows(all_results)

dir.create("blog", showWarnings = FALSE)
write_parquet(simulations, "blog/simulations.parquet")
cat("\nsimulations:", nrow(simulations), "team-leagues across",
    length(unique(simulations$league)), "leagues\n")
