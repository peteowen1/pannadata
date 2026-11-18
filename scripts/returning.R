# Returning players for the team page (panna#497, pannadata#232).
#
# PORT of panna's returner_rust() / returner_adjusted_panna() (panna R/returner_adjust.R). pannadata does not load
# the panna package, so the rule lives here too: change both together.
#
# Why: players back from an absence deliver less than their Panna says for their first games back.
# Rule (Pete, 2026-11-17/18: no cliffs), chosen on hold-outs in pannaverse docs/reviews/panna-validity-2026-11/
# (shrink_fringe_holdout.R, shrink_bench_holdout.R, shrink_bench_w.R; fit before 2024-07-01, score after, eight groups
# including fringe players, regulars held still): test loss 0.00083 vs 0.00075 for the old flat rule (difference
# -0.00024 to +0.00041), 0.00160 with no adjustment.
#   * "rust" rises on an S-curve of club matches missed: s(m) = (m / 4)^6 / (1 + (m / 4)^6), m = matches his club
#     played between his last appearance and now; an unused-sub game counts 0.75, not in the squad counts 1.
#     Club matches, not days, so the summer break adds nothing.
#   * rust drains linearly with minutes played and is 0 after 1,800 minutes back.
#   * the page's Panna keeps (1 - 0.25 * rust) of the lead over replacement: 0.875 after 4 missed, 0.75 after 8+.
# One row per (match, player); minutes capped at 120 (the lineups hold a few corrupt values). All competitions.

returner_rust <- function(lineups, as_of, centre = 4, k = 6, drain_minutes = 1800, bench_weight = 0.75,
                          minutes_cap = 120) {
  need <- c("player_id", "team_id", "match_id", "match_date", "minutes_played")
  miss <- setdiff(need, names(lineups))
  if (length(miss)) stop("lineups is missing ", paste(miss, collapse = ", "))
  as_of <- as.Date(as_of)
  L <- data.table::as.data.table(lineups)[, need, with = FALSE]
  L[, md := as.Date(substr(as.character(match_date), 1, 10))]
  L <- L[!is.na(md) & md < as_of]
  if (!nrow(L)) return(data.table::data.table(player_id = character(), rust = numeric(), club_matches_missed = numeric(),
                                              days_out = numeric(), minutes_since_return = numeric()))
  L <- unique(L, by = c("match_id", "player_id"))
  L[, mins := pmin(pmax(data.table::fcoalesce(as.numeric(minutes_played), 0), 0), minutes_cap)]
  Tc <- unique(L[, .(team_id, match_id, md)])[, .(nm = .N), by = .(team_id, md)]
  data.table::setorder(Tc, team_id, md); Tc[, cum := cumsum(nm), by = team_id]
  cal <- function(tid, t) {
    r <- Tc[data.table::data.table(team_id = tid, md = t), on = .(team_id, md), roll = TRUE, x.cum]
    data.table::fifelse(is.na(r), 0, r)
  }
  sfun <- function(m) { z <- (pmax(m, 0) / centre)^k; z / (1 + z) }
  # unused-substitute games (0-minute lineup rows), cumulative per (player, club), for the bench weight
  Bn <- L[mins <= 0, .(nb = .N), by = .(player_id, team_id, md)]
  data.table::setorder(Bn, player_id, team_id, md); Bn[, cb := cumsum(nb), by = .(player_id, team_id)]
  bench <- function(pl, tid, t) {
    r <- Bn[data.table::data.table(player_id = pl, team_id = tid, md = t), on = .(player_id, team_id, md), roll = TRUE, x.cb]
    data.table::fifelse(is.na(r), 0, r)
  }
  eff <- function(cm, nb) pmax(cm - (1 - bench_weight) * pmin(pmax(nb, 0), pmax(cm, 0)), 0)
  A <- L[mins > 0, .(mins = sum(mins), team_id = team_id[which.max(mins)]), by = .(player_id, md)]
  data.table::setorder(A, player_id, md)
  A[, prev_md := data.table::shift(md), by = player_id]
  A[, prev_team := data.table::shift(team_id), by = player_id]
  A[, cm := data.table::fifelse(is.na(prev_md), 0, cal(prev_team, md - 1) - cal(prev_team, prev_md))]
  A[, nb := data.table::fifelse(is.na(prev_md), 0, bench(player_id, prev_team, md - 1) - bench(player_id, prev_team, prev_md))]
  sv <- sfun(eff(A$cm, A$nb)); pid <- match(A$player_id, unique(A$player_id)); mn <- A$mins; n <- nrow(A)
  ra_v <- numeric(n); jump <- logical(n); last <- 0L; ra <- 0
  for (i in seq_len(n)) {
    if (pid[i] != last) { ra <- 0; last <- pid[i]; rb <- 0 } else { jump[i] <- sv[i] > ra; rb <- max(sv[i], ra) }
    ra <- max(0, rb - mn[i] / drain_minutes); ra_v[i] <- ra
  }
  A[, `:=`(r_after = ra_v, jump = jump)]
  A[, episode := cumsum(jump), by = player_id]
  ep <- A[episode > 0, .(ep_cm = cm[1], ep_days = as.numeric(md[1] - prev_md[1]), ep_mins = sum(mins)),
          by = .(player_id, episode)][, .SD[.N], by = player_id]
  out <- A[, .SD[.N], by = player_id][, .(player_id, last_md = md, last_team = team_id, r_after)]
  out[, cm_now := cal(last_team, as_of - 1) - cal(last_team, last_md)]
  out[, nb_now := bench(player_id, last_team, as_of - 1) - bench(player_id, last_team, last_md)]
  out[, s_now := sfun(eff(cm_now, nb_now))]
  out[, rust := pmax(s_now, r_after)]
  out <- merge(out, ep[, .(player_id, ep_cm, ep_days, ep_mins)], by = "player_id", all.x = TRUE)
  out[, still_out := s_now >= r_after & s_now > 0.01]
  out[, club_matches_missed := data.table::fifelse(still_out, cm_now, ep_cm)]
  out[, days_out := data.table::fifelse(still_out, as.numeric(as_of - last_md), ep_days)]
  out[, minutes_since_return := data.table::fifelse(still_out, 0, ep_mins)]
  out[rust <= 0, `:=`(club_matches_missed = NA_real_, days_out = NA_real_, minutes_since_return = NA_real_)]
  out[, .(player_id, rust, club_matches_missed, days_out, minutes_since_return)]
}

returner_adjusted_panna <- function(panna, replacement, rust, max_discount = 0.25) {
  stopifnot(is.numeric(panna), is.numeric(replacement), is.numeric(rust),
            length(rust) == length(panna),
            length(replacement) %in% c(1L, length(panna)),
            is.numeric(max_discount), length(max_discount) == 1L, max_discount >= 0, max_discount <= 1)
  r <- pmin(pmax(data.table::fcoalesce(as.numeric(rust), 0), 0), 1)
  replacement + (panna - replacement) * (1 - max_discount * r)
}
