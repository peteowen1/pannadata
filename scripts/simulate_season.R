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
# Read blog-latest snapshot. Stale standings are acceptable here: the sim
# treats its inputs as a self-consistent snapshot — current_points, avg_points,
# avg_position, title_pct etc. are all computed against the same baseline, so
# a stale snapshot just means older-but-internally-consistent output. The blog
# surfaces a "stale simulation" banner when live GP has advanced past sim GP.
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

# predictions.parquet carries every historical match plus upcoming fixtures
# (the blog's Results view shows past predictions alongside projections).
# We only want CURRENT SEASON REMAINING FIXTURES. See pannadata#28 / inthegame-blog#189.
#
# Season filter: take max(season) per league — handles mid-year when some
# leagues are mid-season and others haven't started.
if ("season" %in% names(predictions)) {
  predictions <- predictions |>
    group_by(league) |>
    filter(season == max(season)) |>
    ungroup()
  cat("Filtered to current season per league:", nrow(predictions), "matches\n")
}

# Status filter: status == "fixture" means unplayed. Older exports lack the
# column — fall back to "matches after today" as a best-effort guard.
# `match_date` can arrive as an ISO timestamp ("2026-04-19T15:30:00Z"); take
# the first 10 chars so as.Date parses deterministically across locales. Without
# this, full-string as.Date silently returns NA and the filter drops every row.
if ("status" %in% names(predictions)) {
  predictions <- predictions |> filter(status == "fixture")
  cat("Filtered to status == 'fixture':", nrow(predictions), "matches remaining\n")
} else {
  today <- Sys.Date()
  date_str <- substr(as.character(predictions$match_date), 1, 10)
  parsed <- suppressWarnings(as.Date(date_str))
  if (any(is.na(parsed) & !is.na(predictions$match_date))) {
    stop(sprintf("Failed to parse %d match_date values as Date — check predictions.parquet schema",
                 sum(is.na(parsed) & !is.na(predictions$match_date))))
  }
  predictions <- predictions[!is.na(parsed) & parsed >= today, ]
  cat("No 'status' column — filtered to match_date >=", as.character(today),
      ":", nrow(predictions), "matches\n")
}

# Team name normalization: Opta sometimes serves the same team under two names
# within a single season (e.g. "Ajax" → "AFC Ajax" mid-year). Standings are
# built from played matches so their names are canonical. Map every fixture
# variant back to its standings counterpart so the sim treats them as one team.
# Defensive workaround — the upstream fix is in panna step 01 (team_id-based
# name canonicalization at source); this layer exists to keep older blog-latest
# exports working before that fix propagates.
normalize_team_names <- function(fixture_teams, canonical_teams) {
  mapping <- setNames(fixture_teams, fixture_teams)  # identity default
  if (length(canonical_teams) == 0) return(mapping)

  canon_norm <- tolower(trimws(canonical_teams))
  for (ft in fixture_teams) {
    if (ft %in% canonical_teams) next
    ft_n <- tolower(trimws(ft))

    # 1. Word-bounded substring match (either direction); pick longest canonical
    subs <- character()
    for (i in seq_along(canonical_teams)) {
      ct_n <- canon_norm[i]
      pat <- paste0("(^|\\b)", ct_n, "($|\\b)")
      if (grepl(pat, ft_n, perl = TRUE)) subs <- c(subs, canonical_teams[i])
    }
    if (length(subs) > 0) {
      mapping[ft] <- subs[which.max(nchar(subs))]
      next
    }

    # 2. Acronym fallback — "Nijmegen Eendracht Combinatie" → "NEC".
    # Guard against 1-letter initials matching unrelated single-letter canonicals.
    initials <- paste(substr(strsplit(ft_n, "\\s+")[[1]], 1, 1), collapse = "")
    if (nchar(initials) >= 2) {
      idx <- which(canon_norm == initials)
      if (length(idx) == 1) mapping[ft] <- canonical_teams[idx]
    }
  }
  mapping
}

# Apply the mapping per league (standings are league-scoped).
if (!is.null(standings)) {
  predictions <- predictions |>
    group_by(league) |>
    group_modify(function(df, key) {
      canon <- standings |> filter(league == key$league) |> pull(team)
      fixture_names <- unique(c(df$home_team, df$away_team))
      m <- normalize_team_names(fixture_names, canon)
      unmapped <- fixture_names[!fixture_names %in% canon & m[fixture_names] == fixture_names]
      if (length(unmapped) > 0) {
        warning(sprintf("[%s] %d fixture team names did not match standings: %s",
                        key$league, length(unmapped),
                        paste(unmapped, collapse = ", ")), call. = FALSE)
      }
      df$home_team <- unname(m[df$home_team])
      df$away_team <- unname(m[df$away_team])
      df
    }) |>
    ungroup()
}

leagues <- unique(predictions$league)
cat("Leagues:", paste(leagues, collapse = ", "), "\n")
cat("Matches after filtering:", nrow(predictions), "\n\n")

# ── Simulate one season for a league (vectorized) ─────────────────────
simulate_league <- function(preds, league_standings = NULL, n_sims = N_SIMS) {
  teams <- sort(unique(c(preds$home_team, preds$away_team)))
  n_teams <- length(teams)
  n_matches <- nrow(preds)
  team_idx <- setNames(seq_along(teams), teams)

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

  # Pre-compute match indices and probabilities as vectors
  home_idx <- team_idx[preds$home_team]
  away_idx <- team_idx[preds$away_team]
  prob_H <- preds$prob_H
  prob_HD <- preds$prob_H + preds$prob_D
  gd_match <- preds$pred_home_goals - preds$pred_away_goals  # home perspective

  # Generate all random numbers at once: n_sims x n_matches
  rand <- matrix(runif(n_sims * n_matches), nrow = n_sims, ncol = n_matches)

  # Classify outcomes: 1=home win, 2=draw, 3=away win
  # Vectorized across all sims and matches simultaneously
  is_home_win <- rand < rep(prob_H, each = n_sims)
  is_draw <- !is_home_win & (rand < rep(prob_HD, each = n_sims))
  is_away_win <- !is_home_win & !is_draw

  # Initialize accumulators
  sim_points <- matrix(0L, nrow = n_sims, ncol = n_teams)
  sim_gd <- matrix(0, nrow = n_sims, ncol = n_teams)

  # Process each match vectorized across all sims
  for (m in seq_len(n_matches)) {
    hi <- home_idx[m]
    ai <- away_idx[m]
    gd_m <- gd_match[m]

    hw <- is_home_win[, m]
    dr <- is_draw[, m]
    aw <- is_away_win[, m]

    # Points
    sim_points[hw, hi] <- sim_points[hw, hi] + 3L
    sim_points[dr, hi] <- sim_points[dr, hi] + 1L
    sim_points[dr, ai] <- sim_points[dr, ai] + 1L
    sim_points[aw, ai] <- sim_points[aw, ai] + 3L

    # Goal difference
    sim_gd[hw, hi] <- sim_gd[hw, hi] + gd_m
    sim_gd[hw, ai] <- sim_gd[hw, ai] - gd_m
    sim_gd[aw, ai] <- sim_gd[aw, ai] - gd_m
    sim_gd[aw, hi] <- sim_gd[aw, hi] + gd_m
  }

  # Add current standings for ranking
  total_points <- sweep(sim_points, 2, cur_pts, "+")
  total_gd <- sweep(sim_gd, 2, cur_gd, "+")

  # Compute positions per simulation (vectorized ranking)
  positions <- matrix(0L, nrow = n_sims, ncol = n_teams)
  colnames(positions) <- teams
  for (sim in seq_len(n_sims)) {
    ord <- order(-total_points[sim, ], -total_gd[sim, ])
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
    current_gd = round(cur_gd[teams]),
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

  t0 <- proc.time()["elapsed"]
  cat("Simulating", league, ":", nrow(league_preds), "remaining matches,",
      length(unique(c(league_preds$home_team, league_preds$away_team))), "teams,",
      standings_label)

  result <- simulate_league(league_preds, league_standings, N_SIMS)
  result$league <- league
  result$season <- season %||% ""
  result$matchday <- as.integer(matchdays)
  result$n_sims <- N_SIMS

  elapsed <- round(proc.time()["elapsed"] - t0, 1)
  cat(sprintf(" [%.1fs]\n", elapsed))

  all_results[[league]] <- result
}

simulations <- bind_rows(all_results)

dir.create("blog", showWarnings = FALSE)
write_parquet(simulations, "blog/simulations.parquet")
cat("\nsimulations:", nrow(simulations), "team-leagues across",
    length(unique(simulations$league)), "leagues\n")
