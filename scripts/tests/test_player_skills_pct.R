#!/usr/bin/env Rscript
# Tests for the player-skills percentiles and the player-index position column
# in scripts/by_player.R (pannadata#182).
#
#   Rscript scripts/tests/test_player_skills_pct.R      (from the repo root)
#
# The reference below is the rule /football/player applies in the browser
# (player.qmd skillRows), written out the slow way (a loop over the pool and
# JavaScript's Math.round), so the vectorised code in by_player.R is checked
# against the rule itself rather than against a copy of its own approach.

suppressPackageStartupMessages(library(arrow))
source("scripts/by_player.R")

failed <- 0L
chk <- function(label, got, want) {
  if (!identical(got, want)) {
    failed <<- failed + 1L
    cat("FAIL:", label, "- got", format(got), "want", format(want), "\n")
  }
}

js_round <- function(x) floor(x + 0.5)
ref_pct <- function(val, pool) {
  pool <- pool[!is.na(pool)]
  n <- length(pool)
  if (n == 1L) return(50L)
  as.integer(js_round(100 * sum(pool <= val) / n))
}

set.seed(182)
n <- 60L
sk <- data.frame(
  player_id = sprintf("p%02d", sample(n)),
  player_name = sprintf("Player %d", seq_len(n)),
  primary_position = sample(c("GK", "DEF", "MID", "FWD"), n, replace = TRUE, prob = c(.1, .3, .3, .3)),
  total_minutes = runif(n, 500, 3000),
  goals_p90 = round(runif(n, 0, 1), 1),            # many ties
  pass_accuracy = runif(n),
  saves_p90 = ifelse(runif(n) < .8, NA, runif(n)),  # mostly null
  only_one = c(0.7, rep(NA, n - 1L)),               # a pool of exactly one
  is_gk = 0L,
  stringsAsFactors = FALSE
)
sk$primary_position[1] <- NA                         # no position: pct_pos is pct_all, as on the page
sk$primary_position[3] <- ""                         # empty is falsy on the page too
sk$pass_accuracy[2] <- NaN                           # NaN counts as null
# a stat where 100 * count / n lands on x.5: 8 values -> 1/8 = 12.5 (R's round gives 12, JS 13)
sk$half <- NA_real_; sk$half[1:8] <- 1:8

long <- skills_pct_long(sk)

# every (player, stat) with a non-null value has exactly one row, no more
stats <- c("goals_p90", "pass_accuracy", "saves_p90", "only_one", "half")
want_rows <- sum(vapply(stats, function(s) sum(!is.na(sk[[s]])), integer(1)))
chk("row count = non-null values", nrow(long), want_rows)
chk("only stat columns appear (skip list and non-numeric left out)", sort(unique(long$stat)), sort(stats))
chk("one row per (player, stat)", anyDuplicated(long[, c("player_id", "stat")]), 0L)

# each row against the slow reference
bad <- 0L
for (i in seq_len(nrow(long))) {
  r <- long[i, ]
  row <- sk[sk$player_id == r$player_id, ]
  col <- sk[[r$stat]]
  # the page's pool: everyone when the player has no position, else exact matches
  same_pos <- if (is.na(row$primary_position) || row$primary_position == "") rep(TRUE, nrow(sk)) else (!is.na(sk$primary_position) & sk$primary_position == row$primary_position)
  if (!identical(r$value, row[[r$stat]]) || !identical(r$player_name, row$player_name) ||
      r$pct_all != ref_pct(r$value, col) ||
      r$pct_pos != ref_pct(r$value, col[same_pos])) bad <- bad + 1L
}
chk("every pct_all / pct_pos equals the reference rule, and player_name is the skills file's", bad, 0L)

# the cases the rule is easy to get wrong
chk("n == 1 pool gives 50", long$pct_all[long$stat == "only_one"], 50L)
chk("x.5 rounds up like Math.round (12.5 -> 13)", long$pct_all[long$stat == "half" & long$player_id == sk$player_id[1]], 13L)
no_pos <- long$player_id %in% sk$player_id[c(1, 3)]
chk("no position (NA or \"\"): pct_pos equals pct_all", identical(long$pct_pos[no_pos], long$pct_all[no_pos]), TRUE)
chk("NaN gets no row", any(long$stat == "pass_accuracy" & long$player_id == sk$player_id[2]), FALSE)
chk("percentiles are whole numbers in 0..100", all(long$pct_all >= 0L & long$pct_all <= 100L & long$pct_pos >= 0L & long$pct_pos <= 100L), TRUE)
chk("percentiles are integer type", is.integer(long$pct_all) && is.integer(long$pct_pos), TRUE)

# a new column flows through with no list to edit
sk2 <- cbind(sk, brand_new_p90 = runif(n))
chk("a new numeric column is picked up", "brand_new_p90" %in% skills_pct_long(sk2)$stat, TRUE)

# bad input is an error, not a quiet pass
dup <- sk; dup$player_id[2] <- dup$player_id[1]
chk("duplicate player_id refused", inherits(try(skills_pct_long(dup), silent = TRUE), "try-error"), TRUE)

# written file: sorted by player_id, a player never split across row groups
td <- tempfile(); dir.create(td)
path <- file.path(td, "player-skills-pct.parquet")
sink(tempfile()); write_player_skills_pct(sk, path, rows = 40L); sink()
back <- as.data.frame(read_parquet(path))
chk("file has the long frame's rows", nrow(back), nrow(long))
chk("file columns", names(back), c("player_id", "player_name", "stat", "value", "pct_all", "pct_pos"))
nameless <- sk; nameless$player_name <- NULL
chk("skills with no player_name refused", inherits(try(skills_pct_long(nameless), silent = TRUE), "try-error"), TRUE)
chk("file sorted by player_id", identical(back$player_id, sort(back$player_id, method = "radix")), TRUE)
rdr <- ParquetFileReader$create(path)
chk("several row groups", rdr$num_row_groups > 1L, TRUE)
per_rg <- lapply(seq_len(rdr$num_row_groups) - 1L, function(g) unique(as.data.frame(rdr$ReadRowGroup(g))$player_id))
chk("no player in two row groups", anyDuplicated(unlist(per_rg)), 0L)

# player-index: position column joined by player_id, NA when no skills row
ratings <- data.frame(player_id = c("p01", "p02", "zzz"), player_name = c("A", "B", "C"), league = c("ENG", "ESP", "ITA"))
ipath <- file.path(td, "player-index.parquet")
sink(tempfile()); write_player_index(ratings, ipath, positions = sk[, c("player_id", "primary_position")]); sink()
idx <- as.data.frame(read_parquet(ipath))
chk("index columns with positions", names(idx), c("player_id", "player_name", "league", "primary_position"))
chk("index position matches skills", idx$primary_position[idx$player_id == "p02"], sk$primary_position[sk$player_id == "p02"])
chk("rated player without a skills row gets NA", is.na(idx$primary_position[idx$player_id == "zzz"]), TRUE)
# a second path: on Windows, Arrow keeps ipath memory-mapped, so rewriting it fails
ipath2 <- file.path(td, "player-index-nopos.parquet")
sink(tempfile()); write_player_index(ratings, ipath2); sink()
chk("no positions given: column absent", names(read_parquet(ipath2)), c("player_id", "player_name", "league"))

# prove this harness can fail
local({
  before <- failed
  sink(tempfile()); chk("deliberately wrong expectation", pool_pct(c(1, 2)), 0L); sink()
  if (failed != before + 1L) { cat("FAIL: the assert helper did not report a known-bad expectation\n"); quit(status = 1L) }
  failed <<- before
})

if (failed > 0L) { cat("\n", failed, "check(s) failed\n"); quit(status = 1L) }
cat("player-skills percentiles: all checks passed\n")
