#!/usr/bin/env Rscript
# Tests for scripts/quality_gates.R
#
#   Rscript scripts/tests/test_quality_gates.R      (from the repo root)
#
# These exist because the gate they cover failed the daily build for three days
# on a sample of one, and nothing in this repo would have caught it. They source
# the shipped predicate rather than restating it: a test that re-implements the
# rule can agree with itself while the build is wrong.

source("scripts/quality_gates.R")

failed <- 0L
chk <- function(label, got, want) {
  if (!identical(got, want)) {
    failed <<- failed + 1L
    cat("FAIL:", label, "— got", format(got), "want", format(want), "\n")
  }
}

# --- the incident: a sample too small to be a rate --------------------------
# 2026-09-04, three matchweeks into the season: one player at 900+ minutes,
# unmatched. The old expression read that as 100% drift and halted the build.
chk("1 heavy, all missing -> pass (skipped)", spm_heavy_join_ok(1, 1), TRUE)
chk("49 heavy, all missing -> pass (skipped)", spm_heavy_join_ok(49, 49), TRUE)
chk("0 heavy -> pass, and no divide by zero", spm_heavy_join_ok(0, 0), TRUE)

# --- the drift it exists to catch, which must still fail --------------------
chk("4000 heavy, all missing -> FAIL", spm_heavy_join_ok(4000, 4000), FALSE)
chk("4000 heavy, 40% missing -> FAIL", spm_heavy_join_ok(4000, 1600), FALSE)
chk("4000 heavy, 30% missing -> FAIL (threshold exclusive)", spm_heavy_join_ok(4000, 1200), FALSE)

# --- healthy seasons pass ---------------------------------------------------
chk("4000 heavy, 15.5% missing (the measured baseline) -> pass",
    spm_heavy_join_ok(4000, 620), TRUE)
chk("4000 heavy, none missing -> pass", spm_heavy_join_ok(4000, 0), TRUE)

# --- the boundary itself ----------------------------------------------------
# At exactly min_n the gate is ON. A boundary that silently drifted one either
# way would move when the gate applies without anyone noticing.
chk("exactly 50 heavy, all missing -> FAIL (gate is on at min_n)",
    spm_heavy_join_ok(50, 50), FALSE)
chk("49 heavy is the last skipped sample", spm_heavy_join_ok(49, 49), TRUE)

# --- nonsense input is an error, not a quiet pass ---------------------------
chk("more missing than heavy is refused",
    inherits(try(spm_heavy_join_ok(10, 11), silent = TRUE), "try-error"), TRUE)
chk("negative input is refused",
    inherits(try(spm_heavy_join_ok(-1, 0), silent = TRUE), "try-error"), TRUE)

# --- prove this harness can fail -------------------------------------------
# A gate that cannot fail reads as coverage and is worse than nothing.
local({
  before <- failed
  sink(tempfile()); chk("deliberately wrong expectation", spm_heavy_join_ok(1, 1), FALSE); sink()
  if (failed != before + 1L) {
    cat("FAIL: the assert helper did not report a known-bad expectation\n")
    quit(status = 1L)
  }
  failed <<- before
})

# --- piero_metric_coverage_ok: the "printed diagnostic is not a check" gate ---
# 2026-09-15: spmr covered 2,754 of 19,167 (14.4%) and the build stayed green
# because the check only cat()'d a warning. This predicate is what the fix
# actually calls to stop() the build -- test it directly against that incident.
chk("the 2026-09-15 incident (14.4%) -> FAIL", piero_metric_coverage_ok(2754 / 19167), FALSE)
chk("exactly 50% -> pass (threshold inclusive)", piero_metric_coverage_ok(0.5), TRUE)
chk("just under 50% -> FAIL", piero_metric_coverage_ok(0.499), FALSE)
chk("full coverage -> pass", piero_metric_coverage_ok(1), TRUE)
chk("zero coverage -> FAIL", piero_metric_coverage_ok(0), FALSE)
chk("out-of-range input is refused",
    inherits(try(piero_metric_coverage_ok(1.1), silent = TRUE), "try-error"), TRUE)

if (failed > 0L) {
  cat("\n", failed, "check(s) failed\n")
  quit(status = 1L)
}
cat("quality gates: all checks passed\n")
