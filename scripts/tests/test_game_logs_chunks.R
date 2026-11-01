#!/usr/bin/env Rscript
# Test for rechunk_game_logs_by_league() in scripts/by_player.R (pannadata#153):
# a game-logs-<season>.parquet comes back league-sorted, in several row groups,
# with the same rows and values, and min/max on `league` that let a reader skip
# groups.
#
#   Rscript scripts/tests/test_game_logs_chunks.R      (from the repo root)

suppressPackageStartupMessages(library(arrow))
source("scripts/by_player.R")

failed <- 0L
chk <- function(label, got, want) {
  if (!identical(got, want)) {
    failed <<- failed + 1L
    cat("FAIL:", label, "- got", format(got), "want", format(want), "\n")
  }
}

set.seed(153)
n <- 1000L
gl <- data.frame(
  player_id = sprintf("p%03d", seq_len(n)),
  league = sample(c("WC", "ENG", "ESP", "UCL"), n, replace = TRUE, prob = c(.1, .4, .3, .2)),
  epv = round(runif(n), 3),
  stringsAsFactors = FALSE
)
dir <- tempfile("gl"); dir.create(dir)
path <- file.path(dir, "game-logs-2025-2026.parquet")
write_parquet(gl, path)  # one row group, league unsorted

n_rg <- rechunk_game_logs_by_league(path, rows = 300L)
chk("row groups", n_rg, 4L)
rdr <- ParquetFileReader$create(path)
chk("file row groups", rdr$num_row_groups, 4L)
out <- as.data.frame(read_parquet(path))
chk("same row count", nrow(out), n)
chk("league sorted", is.unsorted(out$league, strictly = FALSE), FALSE)
want <- gl[order(gl$league, method = "radix"), ]; rownames(want) <- NULL
chk("same rows, stable within league", out$player_id, want$player_id)
chk("same values", out$epv, want$epv)

# Per-group statistics on league: contiguous, so WC sits in a minority of groups
# (read through the dataset layer: one group at a time).
grp_has_wc <- vapply(seq_len(rdr$num_row_groups) - 1L, function(i) {
  "WC" %in% as.data.frame(rdr$ReadRowGroup(i))$league
}, logical(1))
chk("WC confined to fewer groups than the file has", sum(grp_has_wc) < 4L, TRUE)

# build_game_logs_chunks() touches only game-logs-<season> files
other <- file.path(dir, "game-logs-2025-2026-by-league.parquet")
write_parquet(gl, other)
build_game_logs_chunks(dir)
chk("by-league copy untouched", ParquetFileReader$create(other)$num_row_groups, 1L)

if (failed > 0L) { cat(failed, "check(s) failed\n"); quit(status = 1) }
cat("all game-logs chunk checks passed\n")
