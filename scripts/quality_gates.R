# quality_gates.R
# Predicates for build_blog_data.R's data-quality gates.
#
# WHY THESE LIVE IN THEIR OWN FILE. build_blog_data.R is a long top-to-bottom
# script: a gate written inline there cannot be tested without running the whole
# build, so until now none of them were. On 2026-09-04 that cost three days of
# publishing. The gate that failed was arithmetically correct and still wrong,
# which is exactly the kind of thing a test says out loud and a code review can
# read past.
#
# Sourced by build_blog_data.R (repo root working directory, the same way it
# sources scripts/league_config.R) and by scripts/tests/test_quality_gates.R,
# so the tests exercise the shipped predicate rather than a copy of it.

#' Is the heavy-minutes SPM join rate acceptable?
#'
#' Watches for join-key drift between the ratings table and the SPM table.
#' Real drift looks like ~100% unmatched; ordinary structural growth moves the
#' rate slowly. The threshold was measured at 15.5% on a full season
#' (2026-06-11, the day MLS/Liga MX/Argentina/Saudi shipped).
#'
#' THE MINIMUM SAMPLE IS THE POINT. A rate needs a denominator big enough to be
#' a rate. Three matchweeks into 2026-2027 exactly ONE player had 900+ minutes,
#' that player had no SPM row, and 1/1 tripped a threshold set for a full
#' season. The build then failed every day for three days, and because this
#' script is also what passes game logs, ratings, predictions and skills
#' through to R2, a gate meant to stop bad data shipping stopped all data
#' shipping. `max(n_heavy, 1)` protects the division from zero but says nothing
#' about whether the ratio means anything.
#'
#' 50 sits far below any real mid-season count (thousands) and far above the
#' season-start trickle, so the gate is off exactly while it cannot work and on
#' everywhere it can.
#'
#' @param n_heavy Number of players at or above the minutes threshold.
#' @param n_missing How many of those have no SPM value.
#' @param min_n Minimum sample before the rate is judged at all.
#' @param max_rate Failing rate, exclusive.
#' @return TRUE if the gate passes (or is skipped for a small sample).
spm_heavy_join_ok <- function(n_heavy, n_missing, min_n = 50, max_rate = 0.3) {
  stopifnot(n_heavy >= 0, n_missing >= 0, n_missing <= n_heavy)
  if (n_heavy < min_n) return(TRUE)
  n_missing / n_heavy < max_rate
}
