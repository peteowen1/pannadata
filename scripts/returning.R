# Returning players for the team page (panna#497, pannadata#232).
#
# PORT of panna's returner_status() and returner_adjusted_panna() (panna R/returner_adjust.R, 61d6d074).
# pannadata does not load the panna package, so the rule lives here too: change both together.
#
# Why: players back after 60+ days with no minutes deliver less than their Panna says for their first 900
# minutes. A flat discount, keep 85% of the lead over replacement, was chosen on a hold-out (test loss
# 0.0022 -> 0.0009, regulars unchanged; pannaverse docs/reviews/panna-validity-2026-11/shrink_shapes_holdout.R).
# Pete, 2026-11-17: show it on the team page only; career Panna itself is unchanged.
#
# Returning on `as_of`: still out (no minutes for >= 60 days while his last club played >= 8 matches), or back
# (his last such gap, at the club he returned to and had played for before, ended before as_of and he has
# fewer than 900 minutes since). One row per (match, player); minutes capped at 120.

returner_status <- function(lineups, as_of, gap_days = 60L, club_matches = 8L,
                            window_minutes = 900, minutes_cap = 120) {
  need <- c("player_id", "team_id", "match_id", "match_date", "minutes_played")
  miss <- setdiff(need, names(lineups))
  if (length(miss)) stop("lineups is missing ", paste(miss, collapse = ", "))
  as_of <- as.Date(as_of)
  L <- data.table::as.data.table(lineups)[, need, with = FALSE]
  L[, md := as.Date(substr(as.character(match_date), 1, 10))]
  L <- L[!is.na(md) & md < as_of]
  if (!nrow(L)) return(data.table::data.table(player_id = character(), returning = logical(),
                                              days_out = numeric(), minutes_since_return = numeric()))
  L <- unique(L, by = c("match_id", "player_id"))
  L[, mins := pmin(pmax(data.table::fcoalesce(as.numeric(minutes_played), 0), 0), minutes_cap)]
  Tc <- unique(L[, .(team_id, match_id, md)])[, .(nm = .N), by = .(team_id, md)]
  data.table::setorder(Tc, team_id, md); Tc[, cum := cumsum(nm), by = team_id]
  cal <- function(tid, t) {
    r <- Tc[data.table::data.table(team_id = tid, md = t), on = .(team_id, md), roll = TRUE, x.cum]
    data.table::fifelse(is.na(r), 0, r)
  }
  A <- L[mins > 0, .(mins = sum(mins), team_id = team_id[which.max(mins)]), by = .(player_id, md)]
  data.table::setorder(A, player_id, md)
  fpt <- A[, .(first_pt = min(md)), by = .(player_id, team_id)]
  A[, prev_md := data.table::shift(md), by = player_id]
  A[, gap := as.numeric(md - prev_md)]
  cand <- A[!is.na(gap) & gap >= gap_days]
  cand <- merge(cand, fpt, by = c("player_id", "team_id"))
  cand[, cm := cal(team_id, md - 1) - cal(team_id, prev_md)]
  ret <- cand[cm >= club_matches & first_pt <= prev_md, .(player_id, ret_md = md, days_out = gap)]
  last_ret <- ret[, .SD[which.max(ret_md)], by = player_id]
  A <- merge(A, last_ret, by = "player_id", all.x = TRUE)
  since <- A[!is.na(ret_md) & md >= ret_md, .(minutes_since_return = sum(mins)), by = player_id]
  last <- A[, .(last_md = max(md), last_team = team_id[which.max(md)]), by = player_id]
  last[, out_days := as.numeric(as_of - last_md)]
  last[, out_cm := cal(last_team, as_of - 1) - cal(last_team, last_md)]
  out <- merge(last[, .(player_id, out_days, out_cm)], last_ret[, .(player_id, days_out)], by = "player_id", all.x = TRUE)
  out <- merge(out, since, by = "player_id", all.x = TRUE)
  out[, still_out := out_days >= gap_days & out_cm >= club_matches]
  out[, back := !is.na(days_out) & data.table::fcoalesce(minutes_since_return, 0) < window_minutes]
  out[, returning := still_out | back]
  out[still_out == TRUE, `:=`(days_out = out_days, minutes_since_return = 0)]
  out[, .(player_id, returning, days_out, minutes_since_return)]
}

returner_adjusted_panna <- function(panna, replacement, returning, keep = 0.85) {
  stopifnot(is.numeric(panna), is.numeric(replacement), length(replacement) == 1L,
            is.logical(returning), length(returning) == length(panna))
  ifelse(returning %in% TRUE, replacement + (panna - replacement) * keep, panna)
}
