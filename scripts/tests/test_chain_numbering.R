#!/usr/bin/env Rscript
# Unit test for scripts/chain_numbering.R (pannadata#111 / FABLE-111).
# Usage: Rscript scripts/tests/test_chain_numbering.R
#
# Hand-built ~25-row synthetic events table covering every branch of the
# original per-row loop (keeper rebound mid-chain, failed duel by the
# non-possessing team, won duel, dead-ball event, match boundary, first row
# of the whole table, plus the cross-match state-carrying edge case found
# during validation -- see chain_numbering.R's DEVIATION note). Expected
# chain_number is asserted both against a hand-derived sequence AND against
# the ORIGINAL per-row loop re-implemented here verbatim, so this test also
# catches any future drift between the two without needing real data.
#
# The real-data gate (identical() against production output on
# events_World_Cup.parquet, 1,526,649 rows / 900 matches) was run separately
# during development -- see pannadata#111 PR description / issue comment for
# that record; it is not re-run here to keep this test fast and offline.

# Invoked from the pannadata repo root, matching every other script here
# (e.g. `Rscript scripts/build_chains_ci.R`).
source("scripts/chain_numbering.R")

dead_ball_types <- c(2L, 4L, 5L, 6L, 17L, 55L, 56L, 57L, 70L, 80L, 81L)
keeper_rebound_types <- c(10L, 11L, 41L, 50L)
duel_contest_types <- c(3L, 7L, 42L, 44L, 45L, 83L)

stopifnot(
  "keeper_rebound_types and duel_contest_types must be disjoint (FABLE-111 proof assumption)" =
    length(intersect(keeper_rebound_types, duel_contest_types)) == 0
)

# Original per-row loop, reproduced verbatim for comparison (this is the
# ground truth the vectorized version must match bit-for-bit).
run_original_loop <- function(events) {
  chain_number <- integer(nrow(events))
  chain_n <- 0L
  prev_team <- ""
  prev_match <- ""
  for (i in seq_len(nrow(events))) {
    is_keeper_rebound <- events$type_id[i] %in% keeper_rebound_types
    is_failed_duel <- events$type_id[i] %in% duel_contest_types &&
      events$outcome[i] != 1 &&
      events$team_id[i] != prev_team
    is_transparent <- is_keeper_rebound || is_failed_duel
    new_chain <- events$match_id[i] != prev_match ||
      (!is_transparent && events$team_id[i] != prev_team && events$team_id[i] != "") ||
      events$type_id[i] %in% dead_ball_types
    if (new_chain) chain_n <- chain_n + 1L
    chain_number[i] <- chain_n
    if (!is_transparent) prev_team <- events$team_id[i]
    prev_match <- events$match_id[i]
  }
  chain_number
}

# type_id = 1 is a generic "Pass" placeholder (not in any special set) used
# for rows that should behave as ordinary, non-transparent events.
events <- data.frame(
  match_id  = c("M1","M1","M1","M1","M1","M1","M1","M1","M1","M1",
                "M1","M1","M1","M1","M1",
                "M2","M2","M2","M2",
                "M3","M3","M3","M3","M3","M3"),
  period_id = 1L,
  minute    = seq(0L, 120L, by = 5L),
  second    = 0L,
  event_id  = 1:25,
  type_id   = c(1L, 1L, 7L, 1L, 42L, 13L, 10L, 1L, 83L, 1L,
                4L, 1L, 44L, 45L, 6L,
                1L, 1L, 7L, 1L,
                7L, 1L, 1L, 41L, 1L, 5L),
  team_id   = c("A","A","B","A","A","A","B","A","B","B",
                "A","A","B","B","B",
                "C","C","B","C",
                "X","C","D","C","D","D"),
  outcome   = c(1L, 1L, 0L, 1L, 0L, 0L, 1L, 1L, 1L, 1L,
                1L, 1L, 0L, 1L, 1L,
                1L, 1L, 0L, 1L,
                0L, 1L, 1L, 1L, 1L, 1L),
  stringsAsFactors = FALSE
)

expected <- c(
  1, 1, 1, 1, 1, 1, 1, 1, 2, 2,
  3, 3, 3, 4, 5,
  6, 6, 6, 6,
  7, 7, 8, 8, 8, 9
)

reference <- run_original_loop(events)
stopifnot("hand-derived `expected` must match the original loop (sanity check on the fixture itself)" =
            identical(as.integer(expected), as.integer(reference)))

actual <- compute_chain_numbers(events, dead_ball_types, keeper_rebound_types, duel_contest_types)

if (!identical(as.integer(actual), as.integer(expected))) {
  cat("MISMATCH\n")
  print(data.frame(i = seq_len(nrow(events)), match_id = events$match_id,
                    type_id = events$type_id, team_id = events$team_id,
                    outcome = events$outcome,
                    expected = expected, actual = actual))
  stop("compute_chain_numbers() does not match the hand-derived / original-loop expected sequence")
}

cat("PASS: compute_chain_numbers() matches the original loop on all", nrow(events),
    "rows across", length(unique(events$match_id)), "synthetic matches",
    "(covers: first row of table, same-team continuation, failed duel by",
    "non-possessing team, duel row where team_id == prev_team, keeper",
    "rebound mid-chain, won duel by non-possessing team, dead-ball events,",
    "match boundary, and match boundary landing on a transparent row with",
    "cross-match team_id recurrence).\n")
