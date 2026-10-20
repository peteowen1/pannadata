#!/usr/bin/env Rscript
# Tests for the rank/percentile helpers in scripts/build_blog_data.R
#
#   Rscript scripts/tests/test_blog_ranks.R      (from the repo root)
#
# 2026-10-20: rank()'s default na.last = TRUE gave the 11 players with no career
# Panna the 100th percentile, and the site showed "Pctile 100.0" on their pages.
# build_blog_data.R runs the whole pipeline when sourced, so this pulls the two
# helper definitions out of the shipped file and evaluates only those: a test
# that restated them could agree with itself while the build is wrong.

exprs <- parse("scripts/build_blog_data.R")
for (e in exprs) {
  # eval() runs only these two top-level definitions from our own repo file.
  if (is.call(e) && identical(e[[1]], as.name("<-")) && is.name(e[[2]]) &&
      as.character(e[[2]]) %in% c("desc_rank", "pct_rank")) eval(e)
}
stopifnot(exists("desc_rank"), exists("pct_rank"))

failed <- 0L
chk <- function(label, got, want) {
  if (!identical(got, want)) {
    failed <<- failed + 1L
    cat("FAIL:", label, "- got", format(got), "want", format(want), "\n")
  }
}

x <- c(0.3, NA, 0.1, 0.2, NA)
chk("missing stays missing (percentile)", is.na(pct_rank(x)), c(FALSE, TRUE, FALSE, FALSE, TRUE))
chk("missing stays missing (rank)", is.na(desc_rank(x)), c(FALSE, TRUE, FALSE, FALSE, TRUE))
chk("top rated is the 100th percentile", pct_rank(x)[1], 100)
chk("denominator counts the rated only", pct_rank(x)[3], round(100 / 3, 1))
chk("rank 1 is the highest value", desc_rank(x), c(1L, NA, 3L, 2L, NA))
chk("ties share the lower rank", desc_rank(c(2, 2, 1)), c(1L, 1L, 3L))
chk("no values at all -> all missing", pct_rank(c(NA_real_, NA_real_)), c(NA_real_, NA_real_))
chk("unchanged when nothing is missing", pct_rank(c(1, 2, 3, 4)), c(25, 50, 75, 100))

if (failed) { cat(failed, "failed\n"); quit(status = 1) }
cat("blog rank tests: all passed\n")
