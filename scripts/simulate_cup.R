# simulate_cup.R — Monte Carlo cup competition simulations (UCL, UEL, UECL)
# Generates cup rows in football/simulations.parquet for the inthegame-blog leagues page.
#
# Two-phase simulation:
#   1. League phase: 36 teams, points-based standings (same maths as domestic sim)
#   2. Knockout phase: bracket seeded by league phase positions, Poisson match model
#
# Team strengths for knockout matchups are derived from league phase predictions
# using a Poisson decomposition (attack/defense parameters per team).
#
# Requires:
#   - blog/predictions.parquet: remaining fixtures with Panna probabilities
#   - blog/season_standings.parquet (optional): current cup standings
#
# Usage: Rscript scripts/simulate_cup.R
# Output: blog/cup-simulations.parquet

library(arrow)
library(dplyr)

source("scripts/league_config.R")

N_SIMS <- 10000L
HOME_ADV <- 1.08  # ~8% home advantage in expected goals for knockout legs

cat("=== Cup Competition Simulator ===\n")
cat("Simulations:", N_SIMS, "\n\n")

# ── Cup format configuration ─────────────────────────────────────────
# All three UEFA cups use the same Swiss-model league phase + seeded knockout
CUP_CONFIG <- list(
  UCL  = list(code = "UCL",  name = "Champions League",   n_matches = 8L),
  UEL  = list(code = "UEL",  name = "Europa League",      n_matches = 8L),
  UECL = list(code = "UECL", name = "Conference League",  n_matches = 6L)
)
# All cups: top 8 → auto R16, 9-24 → playoffs, 25-36 → eliminated
AUTO_R16 <- 8L
PLAYOFF_END <- 24L

# Playoff seeding pairs (by league phase position):
# 9/10 vs 23/24, 11/12 vs 21/22, 13/14 vs 19/20, 15/16 vs 17/18
PLAYOFF_PAIRS <- list(
  c(9L, 10L, 23L, 24L),
  c(11L, 12L, 13L, 14L),  # paired with...
  c(21L, 22L, 19L, 20L),  # ...these
  c(15L, 16L, 17L, 18L)
)
# Flattened: high seeds (9-16) vs low seeds (17-24)
# Within each group of 4, draw is random (9 or 10 vs 23 or 24)

# R16 bracket paths (pre-determined):
# Top 8 team (by position) faces the winner from the corresponding playoff pair
# Position 1/2 face winner of 15/16 vs 17/18 path
# Position 3/4 face winner of 13/14 vs 19/20 path
# Position 5/6 face winner of 11/12 vs 21/22 path
# Position 7/8 face winner of 9/10 vs 23/24 path
R16_BRACKET <- list(
  high = c(1L, 2L),   playoff_high = c(15L, 16L), playoff_low = c(17L, 18L),
  high2 = c(3L, 4L),  playoff_high2 = c(13L, 14L), playoff_low2 = c(19L, 20L),
  high3 = c(5L, 6L),  playoff_high3 = c(11L, 12L), playoff_low3 = c(21L, 22L),
  high4 = c(7L, 8L),  playoff_high4 = c(9L, 10L),  playoff_low4 = c(23L, 24L)
)

# ── Load data ─────────────────────────────────────────────────────────
pred_file <- "blog/predictions.parquet"
if (!file.exists(pred_file)) stop("predictions.parquet not found")
predictions <- read_parquet(pred_file)

standings_file <- "blog/season_standings.parquet"
standings <- if (file.exists(standings_file)) read_parquet(standings_file) else NULL

cup_codes <- c("UCL", "UEL", "UECL")
cup_preds <- predictions |>
  filter(league %in% cup_codes, home_team != "", away_team != "")

if (nrow(cup_preds) == 0) {
  cat("No cup predictions found — skipping cup simulation\n")
  # Write empty parquet with correct schema
  empty <- tibble(
    team = character(), league = character(), season = character(),
    n_sims = integer(), current_points = integer(), current_gd = numeric(),
    games_played = integer(), avg_points = numeric(), avg_gd = numeric(),
    avg_position = numeric(), auto_r16_pct = numeric(), playoff_pct = numeric(),
    eliminated_league_pct = numeric(), r16_pct = numeric(), qf_pct = numeric(),
    sf_pct = numeric(), final_pct = numeric(), winner_pct = numeric()
  )
  dir.create("blog", showWarnings = FALSE)
  write_parquet(empty, "blog/cup-simulations.parquet")
  quit(save = "no")
}

cat("Cup predictions loaded:", nrow(cup_preds), "matches\n")
cat("Competitions:", paste(unique(cup_preds$league), collapse = ", "), "\n\n")

# ── Derive team strength parameters (Poisson decomposition) ──────────
# For each team, estimate attack and defense parameters from their predictions.
# log(pred_goals) ≈ log(league_avg) + attack_team + defense_opponent + home_factor
#
# Simplified approach: compute per-team averages, normalize to league mean.
derive_team_strengths <- function(preds) {
  league_avg_home <- mean(preds$pred_home_goals, na.rm = TRUE)
  league_avg_away <- mean(preds$pred_away_goals, na.rm = TRUE)

  teams <- sort(unique(c(preds$home_team, preds$away_team)))

  strengths <- lapply(teams, function(tm) {
    home_matches <- preds[preds$home_team == tm, ]
    away_matches <- preds[preds$away_team == tm, ]

    # Attack: goals team is predicted to score
    home_attack <- if (nrow(home_matches) > 0) mean(home_matches$pred_home_goals) else league_avg_home
    away_attack <- if (nrow(away_matches) > 0) mean(away_matches$pred_away_goals) else league_avg_away

    # Defense: goals team is predicted to concede
    home_defense <- if (nrow(home_matches) > 0) mean(home_matches$pred_away_goals) else league_avg_away
    away_defense <- if (nrow(away_matches) > 0) mean(away_matches$pred_home_goals) else league_avg_home

    tibble(
      team = tm,
      attack = (home_attack + away_attack) / 2,  # avg goals scored per match
      defense = (home_defense + away_defense) / 2, # avg goals conceded per match
      home_attack = home_attack,
      away_attack = away_attack,
      home_defense = home_defense,
      away_defense = away_defense
    )
  })

  bind_rows(strengths)
}

# ── Simulate league phase (identical logic to domestic sim) ───────────
simulate_league_phase <- function(preds, league_standings = NULL, n_sims = N_SIMS) {
  # Include teams from both predictions AND standings (some teams may have
  # all matches played and only appear in standings)
  teams_from_preds <- unique(c(preds$home_team, preds$away_team))
  teams_from_standings <- if (!is.null(league_standings) && nrow(league_standings) > 0) {
    league_standings$team
  } else character()
  teams <- sort(unique(c(teams_from_preds, teams_from_standings)))
  n_teams <- length(teams)
  n_matches <- nrow(preds)
  team_idx <- setNames(seq_along(teams), teams)

  cur_pts <- setNames(rep(0L, n_teams), teams)
  cur_gd <- setNames(rep(0, n_teams), teams)
  cur_gp <- setNames(rep(0L, n_teams), teams)

  if (!is.null(league_standings) && nrow(league_standings) > 0) {
    for (i in seq_len(nrow(league_standings))) {
      tm <- league_standings$team[i]
      if (tm %in% teams) {
        cur_pts[tm] <- league_standings$points[i]
        cur_gd[tm] <- league_standings$gd[i]
        cur_gp[tm] <- league_standings$games_played[i]
      }
    }
  }

  if (n_matches == 0) {
    # All matches played — just return current standings as positions
    ord <- order(-cur_pts, -cur_gd)
    positions_static <- rep(0L, n_teams)
    positions_static[ord] <- seq_along(ord)
    names(positions_static) <- teams

    return(list(
      positions = matrix(rep(positions_static, each = n_sims),
                         nrow = n_sims, ncol = n_teams,
                         dimnames = list(NULL, teams)),
      cur_pts = cur_pts,
      cur_gd = cur_gd,
      cur_gp = cur_gp,
      sim_points = matrix(0L, nrow = n_sims, ncol = n_teams,
                          dimnames = list(NULL, teams)),
      sim_gd = matrix(0, nrow = n_sims, ncol = n_teams,
                      dimnames = list(NULL, teams)),
      total_points = matrix(rep(cur_pts, each = n_sims),
                            nrow = n_sims, ncol = n_teams,
                            dimnames = list(NULL, teams))
    ))
  }

  home_idx <- team_idx[preds$home_team]
  away_idx <- team_idx[preds$away_team]
  prob_H <- preds$prob_H
  prob_HD <- preds$prob_H + preds$prob_D
  gd_match <- preds$pred_home_goals - preds$pred_away_goals

  rand <- matrix(runif(n_sims * n_matches), nrow = n_sims, ncol = n_matches)
  is_home_win <- rand < rep(prob_H, each = n_sims)
  is_draw <- !is_home_win & (rand < rep(prob_HD, each = n_sims))

  sim_points <- matrix(0L, nrow = n_sims, ncol = n_teams)
  sim_gd <- matrix(0, nrow = n_sims, ncol = n_teams)
  colnames(sim_points) <- teams
  colnames(sim_gd) <- teams

  for (m in seq_len(n_matches)) {
    hi <- home_idx[m]
    ai <- away_idx[m]
    gd_m <- gd_match[m]
    hw <- is_home_win[, m]
    dr <- is_draw[, m]
    aw <- !hw & !dr

    sim_points[hw, hi] <- sim_points[hw, hi] + 3L
    sim_points[dr, hi] <- sim_points[dr, hi] + 1L
    sim_points[dr, ai] <- sim_points[dr, ai] + 1L
    sim_points[aw, ai] <- sim_points[aw, ai] + 3L

    sim_gd[hw, hi] <- sim_gd[hw, hi] + gd_m
    sim_gd[hw, ai] <- sim_gd[hw, ai] - gd_m
    sim_gd[aw, ai] <- sim_gd[aw, ai] - gd_m
    sim_gd[aw, hi] <- sim_gd[aw, hi] + gd_m
  }

  total_points <- sweep(sim_points, 2, cur_pts, "+")
  total_gd <- sweep(sim_gd, 2, cur_gd, "+")

  positions <- matrix(0L, nrow = n_sims, ncol = n_teams)
  colnames(positions) <- teams
  for (sim in seq_len(n_sims)) {
    ord <- order(-total_points[sim, ], -total_gd[sim, ])
    positions[sim, ord] <- seq_along(ord)
  }

  list(
    positions = positions,
    cur_pts = cur_pts,
    cur_gd = cur_gd,
    cur_gp = cur_gp,
    sim_points = sim_points,
    sim_gd = sim_gd,
    total_points = total_points
  )
}

# ── Simulate knockout bracket ────────────────────────────────────────
# For each simulation, given final league phase positions:
# 1. Draw playoff matchups per seeding
# 2. Simulate playoffs (2 legs)
# 3. Draw R16 matchups (top 8 vs playoff winners, per bracket path)
# 4. Simulate R16 → QF → SF → Final
simulate_knockout <- function(positions, strengths, n_sims = N_SIMS) {
  teams <- colnames(positions)
  n_teams <- length(teams)

  # Compute league-average defense for Poisson lambda normalization
  avg_defense <- mean(strengths$defense, na.rm = TRUE)

  # Build strength lookup
  str_lookup <- setNames(
    lapply(seq_len(nrow(strengths)), function(i) {
      list(attack = strengths$attack[i], defense = strengths$defense[i])
    }),
    strengths$team
  )

  # Helper: compute expected goals for team A vs team B
  # lambda_A = A_attack * (B_defense / avg_defense) * home_factor
  # This scales A's attack by how leaky B's defense is relative to average
  ko_lambda <- function(attacker, defender, home = FALSE) {
    lam <- attacker$attack * (defender$defense / avg_defense)
    if (home) lam <- lam * HOME_ADV
    max(lam, 0.1)  # floor to avoid degenerate Poisson(~0)
  }

  # Track which round each team reaches per simulation
  # Rounds: 0=eliminated_league, 1=playoff, 2=r16, 3=qf, 4=sf, 5=final, 6=winner
  round_reached <- matrix(0L, nrow = n_sims, ncol = n_teams)
  colnames(round_reached) <- teams

  # ── Classify league phase outcomes ──
  for (sim in seq_len(n_sims)) {
    pos <- positions[sim, ]

    # Top 8: auto R16
    auto_teams <- names(pos[pos <= AUTO_R16])
    round_reached[sim, auto_teams] <- 2L  # start at R16

    # 9-24: playoffs
    playoff_teams <- names(pos[pos > AUTO_R16 & pos <= PLAYOFF_END])
    round_reached[sim, playoff_teams] <- 1L  # start at playoff

    # 25+: eliminated
    # round_reached stays 0
  }

  # ── Simulate playoffs ──
  # For each sim, pair teams 9/10 vs 23/24, etc.
  # This is vectorized per pairing group across all sims
  playoff_winners <- matrix(NA_character_, nrow = n_sims, ncol = 8L)

  for (sim in seq_len(n_sims)) {
    pos <- positions[sim, ]
    # Get teams sorted by position
    pos_to_team <- names(sort(pos))

    pw_idx <- 0L
    # Pair: positions 9,10 vs 23,24 — draw within pairs
    pair_groups <- list(
      list(high = c(9L, 10L), low = c(23L, 24L)),
      list(high = c(11L, 12L), low = c(21L, 22L)),
      list(high = c(13L, 14L), low = c(19L, 20L)),
      list(high = c(15L, 16L), low = c(17L, 18L))
    )

    for (pg in pair_groups) {
      high_teams <- pos_to_team[pg$high]
      low_teams <- pos_to_team[pg$low]
      # Random draw within group: shuffle and pair
      high_shuf <- sample(high_teams)
      low_shuf <- sample(low_teams)

      for (k in 1:2) {
        pw_idx <- pw_idx + 1L
        tm_a <- high_shuf[k]  # higher seed
        tm_b <- low_shuf[k]   # lower seed

        str_a <- str_lookup[[tm_a]]
        str_b <- str_lookup[[tm_b]]

        if (is.null(str_a) || is.null(str_b)) {
          # Fallback: higher seed advances
          playoff_winners[sim, pw_idx] <- tm_a
        } else {
          # Higher seed has 2nd leg at home (slight advantage)
          # Leg 1 at B's home, leg 2 at A's home
          g_a1 <- rpois(1, ko_lambda(str_a, str_b, home = FALSE))
          g_b1 <- rpois(1, ko_lambda(str_b, str_a, home = TRUE))
          g_a2 <- rpois(1, ko_lambda(str_a, str_b, home = TRUE))
          g_b2 <- rpois(1, ko_lambda(str_b, str_a, home = FALSE))

          agg_a <- g_a1 + g_a2
          agg_b <- g_b1 + g_b2

          if (agg_a > agg_b) {
            playoff_winners[sim, pw_idx] <- tm_a
          } else if (agg_b > agg_a) {
            playoff_winners[sim, pw_idx] <- tm_b
          } else {
            # Penalties: 50/50
            playoff_winners[sim, pw_idx] <- if (runif(1) < 0.5) tm_a else tm_b
          }
        }
      }
    }
  }

  # Mark playoff winners as reaching R16
  for (sim in seq_len(n_sims)) {
    pws <- playoff_winners[sim, ]
    pws <- pws[!is.na(pws)]
    round_reached[sim, pws] <- pmax(round_reached[sim, pws], 2L)
  }

  # ── Simulate R16 → QF → SF → Final ──
  # R16: 16 teams (8 auto + 8 playoff winners)
  # Bracket is pre-determined by league phase position
  for (sim in seq_len(n_sims)) {
    pos <- positions[sim, ]
    pos_to_team <- names(sort(pos))
    pws <- playoff_winners[sim, ]

    # Build R16 matchups (8 matches)
    # Top seed pairs: 1/2 vs PO winners from 15/16 vs 17/18 bracket
    # etc.
    # Simplified: pair auto teams with playoff winners in bracket order
    auto_teams <- pos_to_team[1:8]
    # Match auto teams with playoff winners based on bracket paths
    # Position 1/2 face PO winners 7/8 (from pair 15/16 vs 17/18)
    # Position 3/4 face PO winners 5/6 (from pair 13/14 vs 19/20)
    # Position 5/6 face PO winners 3/4 (from pair 11/12 vs 21/22)
    # Position 7/8 face PO winners 1/2 (from pair 9/10 vs 23/24)
    r16_matchups <- list()
    auto_shuf <- list(
      sample(auto_teams[1:2]),
      sample(auto_teams[3:4]),
      sample(auto_teams[5:6]),
      sample(auto_teams[7:8])
    )
    po_groups <- list(pws[7:8], pws[5:6], pws[3:4], pws[1:2])

    for (g in 1:4) {
      po_shuf <- sample(po_groups[[g]])
      r16_matchups[[length(r16_matchups) + 1]] <- c(auto_shuf[[g]][1], po_shuf[1])
      r16_matchups[[length(r16_matchups) + 1]] <- c(auto_shuf[[g]][2], po_shuf[2])
    }

    # Simulate R16 (2 legs, auto team is higher seed → 2nd leg at home)
    r16_winners <- character(8)
    for (m in seq_along(r16_matchups)) {
      tm_a <- r16_matchups[[m]][1]  # higher seed
      tm_b <- r16_matchups[[m]][2]

      str_a <- str_lookup[[tm_a]]
      str_b <- str_lookup[[tm_b]]

      if (is.null(str_a) || is.null(str_b)) {
        r16_winners[m] <- tm_a
        next
      }

      # Leg 1 at B's home, leg 2 at A's home
      g_a1 <- rpois(1, ko_lambda(str_a, str_b, home = FALSE))
      g_b1 <- rpois(1, ko_lambda(str_b, str_a, home = TRUE))
      g_a2 <- rpois(1, ko_lambda(str_a, str_b, home = TRUE))
      g_b2 <- rpois(1, ko_lambda(str_b, str_a, home = FALSE))

      agg_a <- g_a1 + g_a2
      agg_b <- g_b1 + g_b2

      if (agg_a > agg_b) r16_winners[m] <- tm_a
      else if (agg_b > agg_a) r16_winners[m] <- tm_b
      else r16_winners[m] <- if (runif(1) < 0.5) tm_a else tm_b
    }
    round_reached[sim, r16_winners] <- pmax(round_reached[sim, r16_winners], 3L)

    # QF: 8 → 4 (open draw within bracket halves)
    left <- sample(r16_winners[1:4])
    right <- sample(r16_winners[5:8])
    qf_matchups <- list(
      c(left[1], left[2]), c(left[3], left[4]),
      c(right[1], right[2]), c(right[3], right[4])
    )

    qf_winners <- character(4)
    for (m in 1:4) {
      tm_a <- qf_matchups[[m]][1]
      tm_b <- qf_matchups[[m]][2]
      str_a <- str_lookup[[tm_a]]
      str_b <- str_lookup[[tm_b]]
      if (is.null(str_a) || is.null(str_b)) { qf_winners[m] <- tm_a; next }

      g_a1 <- rpois(1, ko_lambda(str_a, str_b, home = TRUE))
      g_b1 <- rpois(1, ko_lambda(str_b, str_a, home = FALSE))
      g_b2 <- rpois(1, ko_lambda(str_b, str_a, home = TRUE))
      g_a2 <- rpois(1, ko_lambda(str_a, str_b, home = FALSE))
      agg_a <- g_a1 + g_a2; agg_b <- g_b1 + g_b2
      if (agg_a > agg_b) qf_winners[m] <- tm_a
      else if (agg_b > agg_a) qf_winners[m] <- tm_b
      else qf_winners[m] <- if (runif(1) < 0.5) tm_a else tm_b
    }
    round_reached[sim, qf_winners] <- pmax(round_reached[sim, qf_winners], 4L)

    # SF: 4 → 2 (left vs left, right vs right)
    sf_matchups <- list(c(qf_winners[1], qf_winners[2]), c(qf_winners[3], qf_winners[4]))
    sf_winners <- character(2)
    for (m in 1:2) {
      tm_a <- sf_matchups[[m]][1]
      tm_b <- sf_matchups[[m]][2]
      str_a <- str_lookup[[tm_a]]
      str_b <- str_lookup[[tm_b]]
      if (is.null(str_a) || is.null(str_b)) { sf_winners[m] <- tm_a; next }

      g_a1 <- rpois(1, ko_lambda(str_a, str_b, home = TRUE))
      g_b1 <- rpois(1, ko_lambda(str_b, str_a, home = FALSE))
      g_b2 <- rpois(1, ko_lambda(str_b, str_a, home = TRUE))
      g_a2 <- rpois(1, ko_lambda(str_a, str_b, home = FALSE))
      agg_a <- g_a1 + g_a2; agg_b <- g_b1 + g_b2
      if (agg_a > agg_b) sf_winners[m] <- tm_a
      else if (agg_b > agg_a) sf_winners[m] <- tm_b
      else sf_winners[m] <- if (runif(1) < 0.5) tm_a else tm_b
    }
    round_reached[sim, sf_winners] <- pmax(round_reached[sim, sf_winners], 5L)

    # Final: single match, neutral venue (no home advantage)
    tm_a <- sf_winners[1]
    tm_b <- sf_winners[2]
    str_a <- str_lookup[[tm_a]]
    str_b <- str_lookup[[tm_b]]

    if (is.null(str_a) || is.null(str_b)) {
      winner <- tm_a
    } else {
      g_a <- rpois(1, ko_lambda(str_a, str_b, home = FALSE))
      g_b <- rpois(1, ko_lambda(str_b, str_a, home = FALSE))
      if (g_a > g_b) winner <- tm_a
      else if (g_b > g_a) winner <- tm_b
      else winner <- if (runif(1) < 0.5) tm_a else tm_b
    }
    round_reached[sim, winner] <- 6L
  }

  round_reached
}

# ── Run cup simulations ──────────────────────────────────────────────
all_results <- list()

for (cup_name in names(CUP_CONFIG)) {
  cfg <- CUP_CONFIG[[cup_name]]
  cup_code <- cfg$code

  league_preds <- cup_preds |> filter(league == cup_code)
  if (nrow(league_preds) == 0) {
    cat("Skipping", cup_name, "— no predictions\n")
    next
  }

  teams <- sort(unique(c(league_preds$home_team, league_preds$away_team)))
  n_teams <- length(teams)
  season <- unique(league_preds$season)[1]

  cat("Simulating", cfg$name, "(", cup_code, "):",
      n_teams, "teams,", nrow(league_preds), "remaining matches\n")

  # Get current standings
  cup_standings <- NULL
  if (!is.null(standings)) {
    cup_standings <- standings |> filter(league == cup_code)
    if (nrow(cup_standings) == 0) cup_standings <- NULL
  }

  if (!is.null(cup_standings)) {
    cat("  standings:", nrow(cup_standings), "teams with results\n")
  } else {
    cat("  no standings — simulating all league phase matches\n")
  }

  # Derive team strengths from predictions
  strengths <- derive_team_strengths(league_preds)
  cat("  team strengths derived for", nrow(strengths), "teams\n")

  t0 <- proc.time()["elapsed"]

  # Phase 1: League phase simulation
  lp_result <- simulate_league_phase(league_preds, cup_standings, N_SIMS)

  # Phase 2: Knockout simulation
  if (n_teams >= PLAYOFF_END) {
    cat("  simulating knockout bracket...")
    round_reached <- simulate_knockout(lp_result$positions, strengths, N_SIMS)
  } else {
    cat("  fewer than", PLAYOFF_END, "teams — league phase only\n")
    round_reached <- matrix(0L, nrow = N_SIMS, ncol = n_teams)
    colnames(round_reached) <- teams
    for (sim in seq_len(N_SIMS)) {
      pos <- lp_result$positions[sim, ]
      round_reached[sim, names(pos[pos <= AUTO_R16])] <- 2L
      round_reached[sim, names(pos[pos > AUTO_R16 & pos <= PLAYOFF_END])] <- 1L
    }
  }

  elapsed <- round(proc.time()["elapsed"] - t0, 1)
  cat(sprintf(" [%.1fs]\n", elapsed))

  # Aggregate results
  positions <- lp_result$positions
  result <- tibble(
    team = teams,
    league = cup_code,
    season = season %||% "",
    n_sims = N_SIMS,
    current_points = as.integer(lp_result$cur_pts[teams]),
    current_gd = round(lp_result$cur_gd[teams]),
    games_played = as.integer(lp_result$cur_gp[teams]),
    avg_points = round(colMeans(lp_result$sim_points[, teams, drop = FALSE]), 1),
    avg_gd = round(colMeans(lp_result$sim_gd[, teams, drop = FALSE]), 1),
    avg_position = round(colMeans(positions[, teams, drop = FALSE]), 1),
    # League phase probabilities
    auto_r16_pct = round(colMeans(positions[, teams, drop = FALSE] <= AUTO_R16), 4),
    playoff_pct = round(colMeans(positions[, teams, drop = FALSE] > AUTO_R16 &
                                   positions[, teams, drop = FALSE] <= PLAYOFF_END), 4),
    eliminated_league_pct = round(colMeans(positions[, teams, drop = FALSE] > PLAYOFF_END), 4),
    # Knockout progression probabilities
    r16_pct = round(colMeans(round_reached[, teams, drop = FALSE] >= 2L), 4),
    qf_pct = round(colMeans(round_reached[, teams, drop = FALSE] >= 3L), 4),
    sf_pct = round(colMeans(round_reached[, teams, drop = FALSE] >= 4L), 4),
    final_pct = round(colMeans(round_reached[, teams, drop = FALSE] >= 5L), 4),
    winner_pct = round(colMeans(round_reached[, teams, drop = FALSE] >= 6L), 4)
  )

  all_results[[cup_code]] <- result
}

if (length(all_results) == 0) {
  cat("\nNo cup simulations produced\n")
} else {
  cup_simulations <- bind_rows(all_results)
  dir.create("blog", showWarnings = FALSE)
  write_parquet(cup_simulations, "blog/cup-simulations.parquet")
  cat("\ncup-simulations:", nrow(cup_simulations), "team-cups across",
      length(unique(cup_simulations$league)), "competitions\n")
}
