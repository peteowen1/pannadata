#!/usr/bin/env Rscript
# Tests for the all-leagues by-player files in scripts/by_player.R
# (pannadata#193): match-stats-by-player.parquet and game-logs-all-by-player.parquet.
#
#   Rscript scripts/tests/test_all_by_player.R      (from the repo root)

suppressPackageStartupMessages(library(arrow))
source("scripts/by_player.R")

failed <- 0L
chk <- function(label, got, want) {
  if (!identical(got, want)) {
    failed <<- failed + 1L
    cat("FAIL:", label, "- got", format(got), "want", format(want), "\n")
  }
}
quietly <- function(expr) { sink(tempfile()); on.exit(sink()); suppressMessages(expr) }

# --- bind_rows_fill: union of columns, typed NA fill, first-appearance order ---
a <- data.frame(player_id = c("p1", "p2"), x = c(1.5, 2.5), s = c("u", "v"))
b <- data.frame(player_id = "p3", y = 7L, x = 3.5)
u <- bind_rows_fill(list(a, b))
chk("union columns, first-appearance order", names(u), c("player_id", "x", "s", "y"))
chk("row count", nrow(u), 3L)
chk("missing string column is NA_character_", u$s, c("u", "v", NA_character_))
chk("missing integer column keeps its type", u$y, c(NA_integer_, NA_integer_, 7L))
chk("present values untouched", u$x, c(1.5, 2.5, 3.5))

# --- write_parquet_by_player_fit: retries with larger groups until the footer fits ---
set.seed(193)
n_players <- 400L
games <- sample(1:30, n_players, replace = TRUE)
df <- data.frame(
  player_id = rep(sprintf("p%04d", seq_len(n_players)), games),
  match_date = as.Date("2020-01-01") + sample(0:2000, sum(games), replace = TRUE),
  match_id = sprintf("m%06d", sample(1e6, sum(games))),
  v = runif(sum(games)), stringsAsFactors = FALSE
)
td <- tempfile("allbp"); dir.create(td)
p1 <- file.path(td, "fit.parquet")
quietly(r1 <- write_parquet_by_player_fit(df, p1, by = ALL_BY_PLAYER_SORT, rows = 500L, max_footer = 1e6))
chk("fits first time: rows unchanged", r1, 500L)
f1 <- parquet_footer_bytes(p1)
# A limit just under what 500-row groups gave forces at least one retry
p2 <- file.path(td, "fit2.parquet")
quietly(r2 <- write_parquet_by_player_fit(df, p2, by = ALL_BY_PLAYER_SORT, rows = 500L, max_footer = f1 - 1L))
chk("retry grows the groups", r2 > 500L, TRUE)
chk("retry result fits the limit", parquet_footer_bytes(p2) <= f1 - 1L, TRUE)
chk("retried file has fewer row groups", ParquetFileReader$create(p2)$num_row_groups < ParquetFileReader$create(p1)$num_row_groups, TRUE)
chk("retried file keeps every row", ParquetFileReader$create(p2)$num_rows, nrow(df))
# An impossible limit stops instead of looping
err <- tryCatch(quietly(write_parquet_by_player_fit(df, file.path(td, "fit3.parquet"), by = ALL_BY_PLAYER_SORT,
                                                    rows = 500L, max_footer = 10L, max_tries = 3L)),
                error = function(e) conditionMessage(e))
chk("impossible footer limit stops with an error", grepl("footer still over", err), TRUE)
err <- tryCatch(write_parquet_by_player_fit(df, file.path(td, "fit4.parquet"), by = c("match_id", "player_id")),
                error = function(e) conditionMessage(e))
chk("player_id must lead the sort", grepl("player_id as the first sort key", err), TRUE)

# --- build_all_by_player on a small blog dir ---
bd <- file.path(td, "blog"); dir.create(bd)
mk_ms <- function(league, ids, n_each) {
  d <- data.frame(
    player_id = rep(ids, each = n_each), league = league,
    match_date = as.Date("2024-08-01") + rep(seq_len(n_each) * 7L, times = length(ids)),
    match_id = sprintf("%s-%d", league, rep(seq_len(n_each), times = length(ids))),
    goals = rep(1:2, length.out = length(ids) * n_each), stringsAsFactors = FALSE)
  d[sample(nrow(d)), ]
}
# Player "mover" appears in both leagues
write_parquet(mk_ms("ENG", c("mover", "a", "b"), 5L), file.path(bd, "match-stats-ENG.parquet"))
write_parquet(mk_ms("WC",  c("mover", "c"),      3L), file.path(bd, "match-stats-WC.parquet"))
# Outputs of the other writers must not be read back in as sources
write_parquet(mk_ms("ENG", "zzz", 2L), file.path(bd, "match-stats-ENG-by-player.parquet"))
write_parquet(mk_ms("ENG", "zzz", 2L), file.path(bd, "match-stats-ENG-by-team.parquet"))

mk_gl <- function(season, ids, extra = NULL) {
  d <- data.frame(player_id = rep(ids, each = 2L), league = "ENG",
                  match_date = as.Date(paste0(substr(season, 1, 4), "-09-01")) + rep(1:2, times = length(ids)),
                  match_id = sprintf("%s-%d", season, seq_len(2L * length(ids))), epv = runif(2L * length(ids)),
                  stringsAsFactors = FALSE)
  if (!is.null(extra)) d[[extra]] <- 1
  d
}
write_parquet(mk_gl("2023-2024", c("mover", "a")),            file.path(bd, "game-logs-2023-2024.parquet"))
write_parquet(mk_gl("2024-2025", c("mover", "b"), "piero"),   file.path(bd, "game-logs-2024-2025.parquet"))
write_parquet(mk_gl("2024-2025", "zzz"),                      file.path(bd, "game-logs-2024-2025-by-league.parquet"))
write_parquet(mk_gl("2024-2025", "zzz"),                      file.path(bd, "game-logs.parquet"))  # alias: not a source

quietly(ok <- build_all_by_player(bd))
chk("both files written", ok, 2L)

ms <- as.data.frame(read_parquet(file.path(bd, MATCH_STATS_ALL_BY_PLAYER)))
chk("match-stats: rows = both shards only", nrow(ms), 3L * 5L + 2L * 3L)
chk("match-stats: league kept", sort(unique(ms$league)), c("ENG", "WC"))
chk("match-stats: columns as the shards", names(ms), c("player_id", "league", "match_date", "match_id", "goals"))
chk("match-stats: no by-player/by-team rows read back in", "zzz" %in% ms$player_id, FALSE)
chk("match-stats: sorted by player_id then match_date",
    order(ms$player_id, ms$match_date, ms$match_id, method = "radix"), seq_len(nrow(ms)))
chk("match-stats: mover has both leagues together", sort(unique(ms$league[ms$player_id == "mover"])), c("ENG", "WC"))

gl <- as.data.frame(read_parquet(file.path(bd, GAME_LOGS_ALL_BY_PLAYER)))
chk("game-logs: rows = the two season files only", nrow(gl), 4L + 4L)
chk("game-logs: union columns", names(gl), c("player_id", "league", "match_date", "match_id", "epv", "piero"))
chk("game-logs: season without piero gets NA", sum(is.na(gl$piero)), 4L)
chk("game-logs: alias/by-league/by-player not read", "zzz" %in% gl$player_id, FALSE)
chk("game-logs: sorted", order(gl$player_id, gl$match_date, gl$match_id, method = "radix"), seq_len(nrow(gl)))

# Row groups never split a player, and player_id carries min/max statistics in each
split_rows <- function(path) {
  ms_ <- arrow::read_parquet(path, col_select = "player_id")
  rdr <- ParquetFileReader$create(path)
  sizes <- vapply(seq_len(rdr$num_row_groups) - 1L, function(i) rdr$ReadRowGroup(i)$num_rows, numeric(1))
  grp <- rep(seq_along(sizes), sizes)
  any(tapply(grp, ms_$player_id, function(g) length(unique(g))) > 1L)
}
chk("match-stats: no player split across groups", split_rows(file.path(bd, MATCH_STATS_ALL_BY_PLAYER)), FALSE)
chk("footer helper reads a plausible size", parquet_footer_bytes(file.path(bd, MATCH_STATS_ALL_BY_PLAYER)) > 0L, TRUE)

# Re-running over the same directory must not pick the outputs up as sources
before <- nrow(ms)
quietly(build_all_by_player(bd))
chk("re-run reads no output as a source", nrow(as.data.frame(read_parquet(file.path(bd, MATCH_STATS_ALL_BY_PLAYER)))), before)

# A directory with game-logs only (sync-game-logs-r2.yml): match-stats file skipped, no failure
bd2 <- file.path(td, "blog2"); dir.create(bd2)
invisible(file.copy(file.path(bd, "game-logs-2023-2024.parquet"), bd2))
quietly(ok2 <- build_all_by_player(bd2))
chk("game-logs-only dir: one file written", ok2, 1L)
chk("game-logs-only dir: no match-stats file", file.exists(file.path(bd2, MATCH_STATS_ALL_BY_PLAYER)), FALSE)

# A shard missing `league` fails the whole match-stats file, and a stale copy is removed
bd3 <- file.path(td, "blog3"); dir.create(bd3)
write_parquet(mk_ms("ENG", c("a", "b"), 2L), file.path(bd3, "match-stats-ENG.parquet"))
write_parquet(mk_ms("WC", "c", 2L)[, c("player_id", "match_date", "match_id", "goals")], file.path(bd3, "match-stats-WC.parquet"))
invisible(file.create(file.path(bd3, MATCH_STATS_ALL_BY_PLAYER)))
quietly(ok3 <- build_all_by_player(bd3))
chk("shard without league: nothing written", ok3, 0L)
chk("shard without league: stale output removed", file.exists(file.path(bd3, MATCH_STATS_ALL_BY_PLAYER)), FALSE)

# prove this harness can fail
local({
  before <- failed
  sink(tempfile()); chk("deliberately wrong expectation", nrow(u), 99L); sink()
  if (failed != before + 1L) { cat("FAIL: the assert helper did not report a known-bad expectation\n"); quit(status = 1L) }
  failed <<- before
})

if (failed > 0L) { cat("\n", failed, "check(s) failed\n"); quit(status = 1L) }
cat("all-leagues by-player files: all checks passed\n")
