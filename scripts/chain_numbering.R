# Vectorized possession-chain numbering (pannadata#111 / FABLE-111).
#
# Replaces the original per-row loop (git history: build_chains_ci.R, lines
# ~213-234 pre-2026-07-23) with a single vectorized pass. World_Cup alone took
# ~40 minutes to number chains with the loop; the vectorized version runs in
# well under a second. See pannaverse/docs/plans/FABLE-111-CHAIN-VECTORIZATION-PLAN.md
# for the full proof and validation record.
#
# Proof sketch (the loop looks circular on first read but isn't): a
# (duel_contest_type, outcome != 1) row is a no-op on the `prev_team` state
# regardless of which way its team-vs-prev_team comparison resolves -- if
# team_id == prev_team, the update fires but sets prev_team to the value it
# already holds; if team_id != prev_team, the row is transparent and the
# update never fires. Either way prev_team's trajectory over the whole event
# stream doesn't depend on first knowing which duel rows are "failed",
# breaking the apparent chain_n -> prev_team -> is_failed_duel -> chain_n
# circularity and making the whole loop vectorizable.
#
# DEVIATION from the FABLE-111 plan's sketch: `prev_team`'s trajectory is
# carried GLOBALLY across the whole (sorted) events table, NOT reset per
# match_id. The original loop never resets `prev_team` at match boundaries --
# only the boundary row's own `new_chain` flag is forced TRUE by the
# match_id-change clause -- so a transparent row (keeper rebound / failed
# duel) landing as the first row of a new match can carry the PREVIOUS
# match's last team forward into the new match. A `by = match_id`-grouped
# LOCF (as sketched in the plan) diverges from the loop's actual output
# whenever the same team_id recurs across a match boundary right after such
# a transparent boundary row -- caught with a probe fixture during
# validation (2026-07-23), not by inspection alone. The GLOBAL (ungrouped)
# LOCF instead reproduces the loop bit-for-bit; verified `identical()`
# against the loop on events_World_Cup.parquet (1,526,649 rows / 900
# matches) plus a hand-built synthetic fixture covering every branch (see
# scripts/tests/test_chain_numbering.R).
compute_chain_numbers <- function(events, dead_ball_types, keeper_rebound_types, duel_contest_types) {
  non_advancing <- events$type_id %in% keeper_rebound_types |
    (events$type_id %in% duel_contest_types & events$outcome != 1)
  effective_team <- data.table::fifelse(non_advancing, NA_character_, events$team_id)

  # data.table::nafill(type = "locf") only supports numeric types; team_id is
  # character, so LOCF is done by hand via a running "last advancing index".
  shifted <- data.table::shift(effective_team, 1L)
  shifted[1] <- NA_character_   # row 1 of the whole table has no predecessor
  last_idx <- cummax(ifelse(!is.na(shifted), seq_along(shifted), 0L))
  # NB: shifted[last_idx] directly (without pmax) is WRONG when last_idx
  # contains 0 -- R silently drops zero-indexed elements
  # (shifted[0] == character(0)), which shifts/misaligns the whole recycled
  # result rather than erroring. Clamp to 1 for the lookup, then null out by
  # hand for positions with no advancing predecessor yet.
  prev_team_before <- shifted[pmax(last_idx, 1L)]
  prev_team_before[last_idx == 0L] <- ""

  is_failed_duel <- events$type_id %in% duel_contest_types & events$outcome != 1 &
    events$team_id != prev_team_before
  is_transparent <- events$type_id %in% keeper_rebound_types | is_failed_duel

  n <- nrow(events)
  new_match <- c(TRUE, events$match_id[-1] != events$match_id[-n])

  new_chain <- new_match |
    (!is_transparent & events$team_id != prev_team_before & events$team_id != "") |
    events$type_id %in% dead_ball_types

  cumsum(new_chain)
}
