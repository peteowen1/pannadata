# simulate_cup.R — Monte Carlo cup competition simulations (UCL, UEL, UECL)
# Generates cup rows for the inthegame-blog leagues page (blog/cup-simulations.parquet).
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
source("scripts/sim_margin.R")   # sample_margin()
source("scripts/cup_knockout.R") # simulate_knockout(), extract_known_ties(), bracket constants (R090)

N_SIMS <- 10000L

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
# (AUTO_R16, PLAYOFF_END and HOME_ADV live in cup_knockout.R)

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

# Keep only current-season cup matches (predictions.parquet carries every
# historical match plus upcoming fixtures for the blog's Results view). Cup
# composition changes yearly, so stale seasons would contaminate team-strength
# derivation and league-phase sim. See pannadata#28 / inthegame-blog#189.
if ("season" %in% names(cup_preds) && nrow(cup_preds) > 0) {
  cup_preds <- cup_preds |>
    group_by(league) |>
    filter(season == max(season)) |>
    ungroup()
  cat("Filtered cups to current season:", nrow(cup_preds), "matches\n")
}

# Team-name normalization — apply before the
# played/fixture split so both sides see canonical names. Defensive workaround;
# the upstream fix is in panna step 01 via team_id-based canonicalization.
normalize_team_names <- function(fixture_teams, canonical_teams) {
  mapping <- setNames(fixture_teams, fixture_teams)
  if (length(canonical_teams) == 0) return(mapping)
  canon_norm <- tolower(trimws(canonical_teams))
  for (ft in fixture_teams) {
    if (ft %in% canonical_teams) next
    ft_n <- tolower(trimws(ft))
    subs <- character()
    for (i in seq_along(canonical_teams)) {
      pat <- paste0("(^|\\b)", canon_norm[i], "($|\\b)")
      if (grepl(pat, ft_n, perl = TRUE)) subs <- c(subs, canonical_teams[i])
    }
    if (length(subs) > 0) {
      mapping[ft] <- subs[which.max(nchar(subs))]; next
    }
    initials <- paste(substr(strsplit(ft_n, "\\s+")[[1]], 1, 1), collapse = "")
    # Guard 1-letter initials against unrelated canonicals
    if (nchar(initials) >= 2) {
      idx <- which(canon_norm == initials)
      if (length(idx) == 1) mapping[ft] <- canonical_teams[idx]
    }
  }
  mapping
}

if (!is.null(standings) && nrow(cup_preds) > 0) {
  cup_preds <- cup_preds |>
    group_by(league) |>
    group_modify(function(df, key) {
      canon <- standings |> filter(league == key$league) |> pull(team)
      fixture_names <- unique(c(df$home_team, df$away_team))
      m <- normalize_team_names(fixture_names, canon)
      # Warn on unmapped names so silent orphans surface
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
  lam_h <- preds$pred_home_goals  # expected goals; shape the winning-margin distribution
  lam_a <- preds$pred_away_goals

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
    hw <- is_home_win[, m]
    dr <- is_draw[, m]
    aw <- !hw & !dr

    sim_points[hw, hi] <- sim_points[hw, hi] + 3L
    sim_points[dr, hi] <- sim_points[dr, hi] + 1L
    sim_points[dr, ai] <- sim_points[dr, ai] + 1L
    sim_points[aw, ai] <- sim_points[aw, ai] + 3L

    # Goal difference (R024 / pannadata#155): sample a winning margin consistent
    # with the drawn outcome. The old code added the signed EXPECTED goal
    # difference (home - away) whatever the outcome, so an away win for a home
    # favourite LOWERED the winner's goal difference. Draws add 0.
    n_hw <- sum(hw)
    if (n_hw > 0) {
      marg <- sample_margin(lam_h[m], lam_a[m], +1L, n_hw)
      sim_gd[hw, hi] <- sim_gd[hw, hi] + marg
      sim_gd[hw, ai] <- sim_gd[hw, ai] - marg
    }
    n_aw <- sum(aw)
    if (n_aw > 0) {
      marg <- sample_margin(lam_h[m], lam_a[m], -1L, n_aw)
      sim_gd[aw, ai] <- sim_gd[aw, ai] + marg
      sim_gd[aw, hi] <- sim_gd[aw, hi] - marg
    }
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
# simulate_knockout() lives in scripts/cup_knockout.R so it can be tested.

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

  # Derive team strengths from ALL current-season matches (more data = better
  # attack/defense estimates), but simulate only UNPLAYED fixtures — otherwise
  # we'd double-count already-played matches whose points are in standings.
  strengths <- derive_team_strengths(league_preds)
  cat("  team strengths derived for", nrow(strengths), "teams\n")

  if ("status" %in% names(league_preds)) {
    remaining_preds <- league_preds |> filter(status == "fixture")
  } else {
    # match_date may be ISO with time/timezone; take first 10 chars so as.Date
    # parses deterministically instead of silently returning NA.
    date_str <- substr(as.character(league_preds$match_date), 1, 10)
    parsed <- suppressWarnings(as.Date(date_str))
    remaining_preds <- league_preds[!is.na(parsed) & parsed >= Sys.Date(), ]
  }
  # Knockout-phase rows (Feb-Jun) are not league-phase matches: counting them
  # would hand league-table points to playoff/R16 ties. They feed the bracket
  # below instead (R090).
  known_ties <- extract_known_ties(league_preds)
  n_ko <- sum(is_knockout_date(remaining_preds$match_date))
  if (n_ko > 0L) {
    remaining_preds <- remaining_preds[!is_knockout_date(remaining_preds$match_date), ]
    cat("  excluded", n_ko, "knockout-phase fixtures from the league-phase sim
")
  }
  if (length(known_ties) > 0L) {
    cat("  known knockout rounds from fixtures:",
        paste(names(known_ties), lengths(known_ties), sep = "=", collapse = ", "), "
")
  }
  cat("  remaining fixtures to simulate:", nrow(remaining_preds), "\n")

  t0 <- proc.time()["elapsed"]

  # Phase 1: League phase simulation
  lp_result <- simulate_league_phase(remaining_preds, cup_standings, N_SIMS)

  # simulate_league_phase builds its team universe from remaining_preds ∪ standings.
  # When league phase is already finished (remaining_preds empty, knockout fixtures
  # not in standings), that universe can differ from the outer `teams` list sourced
  # from all current-season cup matches. Re-align to the sim's canonical teams so
  # downstream indexing and the result tibble stay consistent.
  teams <- colnames(lp_result$sim_points)
  n_teams <- length(teams)

  # Guard against degenerate cup state: empty remaining_preds AND empty cup_standings
  # (e.g. a brand-new cup season where nothing has been played yet and predictions
  # aren't available). Skip cleanly rather than writing an empty tibble into the
  # cup-simulations parquet with no downstream signal.
  if (n_teams == 0) {
    cat(sprintf("  [%s] no teams in sim universe (empty fixtures + empty standings) — skipping\n",
                cup_code))
    next
  }

  # Phase 2: Knockout simulation
  if (n_teams >= PLAYOFF_END) {
    cat("  simulating knockout bracket...")
    round_reached <- simulate_knockout(lp_result$positions, strengths, N_SIMS,
                                      known = known_ties)
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
