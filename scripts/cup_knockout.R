# cup_knockout.R -- knockout-phase helpers for simulate_cup.R (R090).
#
# Split out of simulate_cup.R so tests can source it without running the
# simulator. Defines the bracket constants, the stage classifier / known-tie
# extractor, and simulate_knockout().
#
# Why this exists: simulate_knockout() used to rebuild every knockout round from
# league-phase positions and ignore the knockout fixtures already in
# predictions.parquet, so once the playoffs had been drawn or played, teams that
# were already out kept non-zero R16/QF/SF/final/winner probabilities.
#
# Limits (documented, not hidden): blog/predictions.parquet carries NO scores
# (columns: match_id, match_date, league, season, teams, pred goals, prob_H/D/A,
# status). So a played leg's result is unknown, and the winner of a tie can only
# be inferred from who appears in the NEXT round's fixture list. A round is used
# only when its fixture list is complete (the right number of distinct ties,
# each team in at most one tie); an incomplete round is ignored and the old
# seeded simulation runs from that point on. Consequence: a played playoff tie
# whose R16 draw is not yet published is still simulated, not fixed to its real
# winner. Adding home/away scores to match_predictions.parquet would close that.

AUTO_R16 <- 8L
PLAYOFF_END <- 24L
HOME_ADV <- 1.08  # ~8% home advantage in expected goals for knockout legs

# round_reached levels: 0 out in league phase, 1 reached playoff, 2 R16, 3 QF,
# 4 SF, 5 final, 6 winner
KO_STAGE_LEVEL <- c(playoff = 1L, r16 = 2L, qf = 3L, sf = 4L, final = 5L)
KO_STAGE_TIES  <- c(playoff = 8L, r16 = 8L, qf = 4L, sf = 2L, final = 1L)

# Knockout rows are dated Feb-Jun of the season's second year. Qualifiers are
# Jul/Aug and the league phase Sep-Jan, so month alone separates them. Within
# the knockouts the stage is read off the calendar (UEFA: playoffs in Feb, R16
# in Mar, QF in Apr, SF late Apr - mid May, final from the third week of May).
# A wrong guess is harmless: extract_known_ties() uses a stage only when its
# tie count is exactly right.
knockout_stage_of <- function(match_date) {
  d <- suppressWarnings(as.Date(substr(as.character(match_date), 1, 10)))
  m <- as.integer(format(d, "%m"))
  day <- as.integer(format(d, "%d"))
  out <- rep(NA_character_, length(d))
  out[!is.na(m) & m == 2L] <- "playoff"
  out[!is.na(m) & m == 3L] <- "r16"
  out[!is.na(m) & m == 4L & day < 25L] <- "qf"
  out[!is.na(m) & ((m == 4L & day >= 25L) | (m == 5L & day < 16L))] <- "sf"
  out[!is.na(m) & ((m == 5L & day >= 16L) | m == 6L)] <- "final"
  out
}

is_knockout_date <- function(match_date) !is.na(knockout_stage_of(match_date))

# preds: one cup's current-season rows (any status). Returns a named list with
# one element per COMPLETE stage: a list of c(team_a, team_b) ties.
extract_known_ties <- function(preds) {
  if (nrow(preds) == 0L) return(list())
  stage <- knockout_stage_of(preds$match_date)
  known <- list()
  for (st in names(KO_STAGE_LEVEL)) {
    rows <- preds[!is.na(stage) & stage == st, , drop = FALSE]
    if (nrow(rows) == 0L) next
    ties <- unique(lapply(seq_len(nrow(rows)), function(i) {
      sort(c(as.character(rows$home_team[i]), as.character(rows$away_team[i])))
    }))
    ok <- length(ties) == KO_STAGE_TIES[[st]] && !anyDuplicated(unlist(ties))
    if (ok) known[[st]] <- ties
  }
  known
}

# Order the known playoff ties by bracket group: group g (1..4) holds the ties
# whose higher seed finished 9/10, 11/12, 13/14, 15/16. Returns NULL when the
# ties do not fit that shape (so the caller falls back to the seeded draw).
order_known_playoff <- function(pos, ties) {
  hi <- lo <- character(length(ties))
  grp <- integer(length(ties))
  for (i in seq_along(ties)) {
    p <- pos[ties[[i]]]
    if (anyNA(p)) return(NULL)
    a <- names(p)[which.min(p)]
    b <- names(p)[which.max(p)]
    if (!(p[[a]] >= 9L && p[[a]] <= 16L && p[[b]] >= 17L && p[[b]] <= 24L)) return(NULL)
    hi[i] <- a
    lo[i] <- b
    grp[i] <- as.integer(ceiling((p[[a]] - 8L) / 2))
  }
  if (!identical(sort(grp), rep(1:4, each = 2L))) return(NULL)
  o <- order(grp, pos[hi])
  lapply(o, function(i) c(hi[i], lo[i]))
}

# Simulate the knockout rounds for each league-phase outcome.
# known: output of extract_known_ties() (NULL / empty = the old behaviour). Every
# complete round in `known` fixes who is in it; the simulation then starts from
# the latest such round, using the real ties.
simulate_knockout <- function(positions, strengths, n_sims = 10000L, known = NULL) {
  teams <- colnames(positions)
  n_teams <- length(teams)

  avg_defense <- mean(strengths$defense, na.rm = TRUE)
  str_lookup <- setNames(
    lapply(seq_len(nrow(strengths)), function(i) {
      list(attack = strengths$attack[i], defense = strengths$defense[i])
    }),
    strengths$team
  )

  # lambda_A = A_attack * (B_defense / avg_defense) * home_factor
  ko_lambda <- function(attacker, defender, home = FALSE) {
    lam <- attacker$attack * (defender$defense / avg_defense)
    if (home) lam <- lam * HOME_ADV
    max(lam, 0.1)  # floor to avoid degenerate Poisson(~0)
  }

  # Two-leg tie. tm_a is the higher seed: leg 2 at its ground when
  # a_home_first is FALSE (playoffs, R16), leg 1 at its ground otherwise.
  # Missing strengths: tm_a advances. Level aggregate: 50/50 penalties.
  two_leg_winner <- function(tm_a, tm_b, a_home_first = FALSE) {
    str_a <- str_lookup[[tm_a]]
    str_b <- str_lookup[[tm_b]]
    if (is.null(str_a) || is.null(str_b)) return(tm_a)
    g_a1 <- rpois(1, ko_lambda(str_a, str_b, home = a_home_first))
    g_b1 <- rpois(1, ko_lambda(str_b, str_a, home = !a_home_first))
    g_a2 <- rpois(1, ko_lambda(str_a, str_b, home = !a_home_first))
    g_b2 <- rpois(1, ko_lambda(str_b, str_a, home = a_home_first))
    agg_a <- g_a1 + g_a2
    agg_b <- g_b1 + g_b2
    if (agg_a > agg_b) return(tm_a)
    if (agg_b > agg_a) return(tm_b)
    if (runif(1) < 0.5) tm_a else tm_b
  }

  # A tie naming a team the simulator does not know means the fixture names did
  # not line up with the standings, so do not trust any of it.
  if (length(known) > 0L && !all(unlist(known) %in% teams)) {
    warning("simulate_knockout: known knockout ties name teams outside the league-phase ",
            "universe (", paste(setdiff(unlist(known), teams), collapse = ", "),
            ") -- ignoring them and simulating from league-phase positions",
            call. = FALSE)
    known <- list()
  }
  known_stages <- names(known)[vapply(known, length, 1L) > 0L]
  start_stage <- if (length(known_stages) > 0L) {
    unname(max(KO_STAGE_LEVEL[known_stages]))
  } else 0L

  round_reached <- matrix(0L, nrow = n_sims, ncol = n_teams)
  colnames(round_reached) <- teams

  for (sim in seq_len(n_sims)) {
    pos <- positions[sim, ]
    round_reached[sim, names(pos[pos <= AUTO_R16])] <- 2L
    round_reached[sim, names(pos[pos > AUTO_R16 & pos <= PLAYOFF_END])] <- 1L
  }

  # Real participants of every known round reached at least that round, and a
  # team missing from a completed round went no further than the round before
  # it. A team in the known R16 but not in a known QF set keeps level 2.
  for (st in known_stages) {
    lvl <- KO_STAGE_LEVEL[[st]]
    in_round <- unique(unlist(known[[st]]))
    round_reached[, in_round] <- pmax(round_reached[, in_round], lvl)
    out_round <- setdiff(teams, in_round)
    if (lvl >= 2L && length(out_round) > 0L) {
      round_reached[, out_round] <- pmin(round_reached[, out_round], lvl - 1L)
    }
  }

  for (sim in seq_len(n_sims)) {
    pos <- positions[sim, ]
    pos_to_team <- names(sort(pos))

    # ── Playoffs ──
    r16_pool <- NULL
    if (start_stage <= 1L) {
      ties <- NULL
      if (start_stage == 1L) ties <- order_known_playoff(pos, known$playoff)
      if (is.null(ties)) {
        ties <- list()
        pair_groups <- list(
          list(high = c(9L, 10L), low = c(23L, 24L)),
          list(high = c(11L, 12L), low = c(21L, 22L)),
          list(high = c(13L, 14L), low = c(19L, 20L)),
          list(high = c(15L, 16L), low = c(17L, 18L))
        )
        for (pg in pair_groups) {
          high_shuf <- sample(pos_to_team[pg$high])
          low_shuf <- sample(pos_to_team[pg$low])
          for (k in 1:2) ties[[length(ties) + 1L]] <- c(high_shuf[k], low_shuf[k])
        }
      }
      r16_pool <- vapply(ties, function(tt) two_leg_winner(tt[1], tt[2]), "")
      round_reached[sim, r16_pool] <- pmax(round_reached[sim, r16_pool], 2L)
    }

    # ── R16: top 8 vs playoff winners, bracket fixed by league position ──
    r16_winners <- NULL
    if (start_stage <= 2L) {
      if (start_stage == 2L) {
        r16_matchups <- lapply(known$r16, function(tt) tt[order(pos[tt])])
      } else {
        auto_teams <- pos_to_team[1:8]
        auto_shuf <- list(sample(auto_teams[1:2]), sample(auto_teams[3:4]),
                          sample(auto_teams[5:6]), sample(auto_teams[7:8]))
        po_groups <- list(r16_pool[7:8], r16_pool[5:6], r16_pool[3:4], r16_pool[1:2])
        r16_matchups <- list()
        for (g in 1:4) {
          po_shuf <- sample(po_groups[[g]])
          r16_matchups[[length(r16_matchups) + 1L]] <- c(auto_shuf[[g]][1], po_shuf[1])
          r16_matchups[[length(r16_matchups) + 1L]] <- c(auto_shuf[[g]][2], po_shuf[2])
        }
      }
      r16_winners <- vapply(r16_matchups, function(tt) two_leg_winner(tt[1], tt[2]), "")
      round_reached[sim, r16_winners] <- pmax(round_reached[sim, r16_winners], 3L)
    }

    # ── QF: open draw within bracket halves ──
    qf_winners <- NULL
    if (start_stage <= 3L) {
      if (start_stage == 3L) {
        qf_matchups <- known$qf
      } else {
        left <- sample(r16_winners[1:4])
        right <- sample(r16_winners[5:8])
        qf_matchups <- list(c(left[1], left[2]), c(left[3], left[4]),
                            c(right[1], right[2]), c(right[3], right[4]))
      }
      qf_winners <- vapply(qf_matchups, function(tt) two_leg_winner(tt[1], tt[2], TRUE), "")
      round_reached[sim, qf_winners] <- pmax(round_reached[sim, qf_winners], 4L)
    }

    # ── SF ──
    sf_winners <- NULL
    if (start_stage <= 4L) {
      sf_matchups <- if (start_stage == 4L) known$sf else
        list(c(qf_winners[1], qf_winners[2]), c(qf_winners[3], qf_winners[4]))
      sf_winners <- vapply(sf_matchups, function(tt) two_leg_winner(tt[1], tt[2], TRUE), "")
      round_reached[sim, sf_winners] <- pmax(round_reached[sim, sf_winners], 5L)
    }

    # ── Final: single match, neutral venue ──
    fin <- if (start_stage == 5L) known$final[[1]] else sf_winners
    str_a <- str_lookup[[fin[1]]]
    str_b <- str_lookup[[fin[2]]]
    winner <- fin[1]
    if (!is.null(str_a) && !is.null(str_b)) {
      g_a <- rpois(1, ko_lambda(str_a, str_b, home = FALSE))
      g_b <- rpois(1, ko_lambda(str_b, str_a, home = FALSE))
      if (g_b > g_a) winner <- fin[2]
      else if (g_a == g_b && runif(1) >= 0.5) winner <- fin[2]
    }
    round_reached[sim, winner] <- 6L
  }

  round_reached
}
