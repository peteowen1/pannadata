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
rm(rdr); invisible(gc())  # Windows: release the handle, or the rename below is refused

# build_game_logs_chunks() touches only game-logs-<season> files
other <- file.path(dir, "game-logs-2025-2026-by-league.parquet")
write_parquet(gl, other)
chk("build_game_logs_chunks: no file left un-chunked", length(build_game_logs_chunks(dir)), 0L)
chk("by-league copy untouched", ParquetFileReader$create(other)$num_row_groups, 1L)

# The current-season alias game-logs.parquet (pannadata#224): sorted by match_id,
# not league, in 20,000-row groups. Each group covers a contiguous match_id range,
# so a match_id read can skip the groups that cannot hold it.
set.seed(224)
n_m <- 25000L
al <- data.frame(
  match_id = sprintf("m%05d", sample.int(2000L, n_m, replace = TRUE)),
  player_id = sprintf("p%05d", seq_len(n_m)),
  league = sample(c("WC", "ENG", "ESP"), n_m, replace = TRUE),
  epv = round(runif(n_m), 3),
  stringsAsFactors = FALSE
)
alias_dir <- tempfile("alias"); dir.create(alias_dir)
alias <- file.path(alias_dir, "game-logs.parquet")
write_parquet(al, alias)  # one row group, match_id unsorted
chk("alias: no failures", length(build_game_logs_chunks(alias_dir)), 0L)
ra <- ParquetFileReader$create(alias)
chk("alias: 2 row groups at 20,000 rows", ra$num_row_groups, 2L)
out_a <- as.data.frame(read_parquet(alias))
chk("alias: same row count", nrow(out_a), n_m)
chk("alias: sorted by match_id", identical(order(out_a$match_id, method = "radix"), seq_len(n_m)), TRUE)
want_a <- al[order(al$match_id, method = "radix"), ]; rownames(want_a) <- NULL
chk("alias: same rows, stable within match_id", out_a$player_id, want_a$player_id)
chk("alias: same values", out_a$epv, want_a$epv)
rng <- lapply(seq_len(ra$num_row_groups) - 1L, function(i) range(as.data.frame(ra$ReadRowGroup(i))$match_id))
contiguous <- all(vapply(seq_len(length(rng) - 1L), function(k) rng[[k]][2] <= rng[[k + 1L]][1], logical(1)))
chk("alias: groups contiguous on match_id", contiguous, TRUE)
# a match whose rows sit in one group: a match_id read skips the other group
hits <- vapply(unique(out_a$match_id), function(m)
  sum(vapply(rng, function(r) m >= r[1] && m <= r[2], logical(1))), integer(1))
chk("alias: some match_id lives in one group", any(hits == 1L), TRUE)
rm(ra); invisible(gc())

if (failed > 0L) { cat(failed, "check(s) failed\n"); quit(status = 1) }
cat("all game-logs chunk checks passed\n")
