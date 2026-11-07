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
# Row-group count / row count with the reader released straight away
# (an open reader blocks rewriting the file on Windows)
pq_groups <- function(path) { r <- ParquetFileReader$create(path, mmap = FALSE); n <- r$num_row_groups; rm(r); invisible(gc()); n }
pq_rows   <- function(path) { r <- ParquetFileReader$create(path, mmap = FALSE); n <- r$num_rows; rm(r); invisible(gc()); n }
rd <- function(path) as.data.frame(read_parquet(path, mmap = FALSE))
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
# A column whose type differs between inputs is an error, not a coercion
err <- tryCatch(bind_rows_fill(list(data.frame(player_id = "p1", x = 1.5), data.frame(player_id = "p2", x = "a"))),
                error = function(e) conditionMessage(e))
chk("type mismatch (numeric vs character) stops, naming the column", grepl("column 'x' is numeric in input 1 but character in input 2", err), TRUE)
err <- tryCatch(bind_rows_fill(list(data.frame(player_id = "p1", n = 1L), data.frame(player_id = "p2", n = 2.5))),
                error = function(e) conditionMessage(e))
chk("type mismatch (integer vs numeric) stops", grepl("column 'n' is integer", err), TRUE)
err <- tryCatch(bind_rows_fill(list(data.frame(player_id = "p1", d = as.Date("2024-01-01")), data.frame(player_id = "p2", d = 19000))),
                error = function(e) conditionMessage(e))
chk("type mismatch (Date vs numeric) stops", grepl("column 'd'", err), TRUE)

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
chk("no temp file left after a write that fits", length(list.files(td, pattern = "\\.tmp$")), 0L)
f1 <- parquet_footer_bytes(p1)
# A limit just under what 500-row groups gave forces at least one retry
p2 <- file.path(td, "fit2.parquet")
quietly(r2 <- write_parquet_by_player_fit(df, p2, by = ALL_BY_PLAYER_SORT, rows = 500L, max_footer = f1 - 1L))
chk("retry grows the groups", r2 > 500L, TRUE)
chk("retry result fits the limit", parquet_footer_bytes(p2) <= f1 - 1L, TRUE)
chk("retried file has fewer row groups", pq_groups(p2) < pq_groups(p1), TRUE)
chk("retried file keeps every row", pq_rows(p2), nrow(df))
chk("no temp file left after retries", length(list.files(td, pattern = "\\.tmp$")), 0L)
# An impossible limit stops instead of looping
p3 <- file.path(td, "fit3.parquet")
invisible(file.create(p3))   # an older file at the final path must not survive a failed run
err <- tryCatch(quietly(write_parquet_by_player_fit(df, p3, by = ALL_BY_PLAYER_SORT,
                                                    rows = 500L, max_footer = 10L, max_tries = 3L)),
                error = function(e) conditionMessage(e))
chk("impossible footer limit stops with an error", grepl("footer still over", err), TRUE)
chk("failed run leaves nothing at the final path", file.exists(p3), FALSE)
chk("failed run leaves no temp file", length(list.files(td, pattern = "\\.tmp$")), 0L)
# Rewriting a path that was just read must work (the Windows 1224 case)
p5 <- file.path(td, "fit5.parquet")
for (i in 1:3) quietly(write_parquet_by_player_fit(df, p5, by = ALL_BY_PLAYER_SORT, rows = 500L))
chk("same path rewritten three times", pq_rows(p5), nrow(df))
err <- tryCatch(write_parquet_by_player_fit(df, file.path(td, "fit4.parquet"), by = c("match_id", "player_id")),
                error = function(e) conditionMessage(e))
chk("player_id must lead the sort", grepl("player_id as the first sort key", err), TRUE)

# --- build_all_by_player on a small blog dir ---
bd <- file.path(td, "blog"); dir.create(bd)
mk_ms <- function(league, ids, n_each, start = as.Date("2024-08-01"), season = "2024-2025") {
  d <- data.frame(
    player_id = rep(ids, each = n_each), league = league,
    match_date = start + rep(seq_len(n_each) * 7L, times = length(ids)),
    match_id = sprintf("%s-%s-%d", league, season, rep(seq_len(n_each), times = length(ids))),
    goals = rep(1:2, length.out = length(ids) * n_each),
    player_name = paste0("Name ", rep(ids, each = n_each)), team_name = paste0(league, " FC"), season = season,
    stringsAsFactors = FALSE)
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

ms <- rd(file.path(bd, MATCH_STATS_ALL_BY_PLAYER))
chk("match-stats: rows = both shards only", nrow(ms), 3L * 5L + 2L * 3L)
chk("match-stats: league kept", sort(unique(ms$league)), c("ENG", "WC"))
chk("match-stats: columns as the shards", names(ms), c("player_id", "league", "match_date", "match_id", "goals", "player_name", "team_name", "season"))
chk("match-stats: no by-player/by-team rows read back in", "zzz" %in% ms$player_id, FALSE)
chk("match-stats: sorted by player_id then match_date",
    order(ms$player_id, ms$match_date, ms$match_id, method = "radix"), seq_len(nrow(ms)))
chk("match-stats: mover has both leagues together", sort(unique(ms$league[ms$player_id == "mover"])), c("ENG", "WC"))

# The player list, written beside match-stats-by-player from the same rows
pl <- rd(file.path(bd, MATCH_STATS_PLAYERS))
chk("players: one row per player in match-stats", sort(pl$player_id), sort(unique(ms$player_id)))
chk("players: columns", names(pl), c("player_id", "player_name", "team_name", "league", "first_season", "last_season", "games"))
chk("players: games = their match-stats rows", pl$games[pl$player_id == "mover"], 5L + 3L)
chk("players: sorted by name", order(pl$player_name, pl$player_id, method = "radix"), seq_len(nrow(pl)))
chk("players: a WC-only player keeps the WC row", pl$league[pl$player_id == "c"], "WC")
chk("players: the shard list never reads the player list back in", nrow(rd(file.path(bd, MATCH_STATS_ALL_BY_PLAYER))), nrow(ms))

# player_list_from_match_stats: the label comes from the latest CLUB game, not a
# later tournament; seasons span every game, tournaments included
pls <- player_list_from_match_stats(rbind(
  mk_ms("ESP", "x", 2L, as.Date("2021-09-01"), "2021-2022"),
  mk_ms("ENG", "x", 2L, as.Date("2023-09-01"), "2023-2024"),
  mk_ms("WC",  "x", 2L, as.Date("2024-06-20"), "2024")))
pls_x <- pls[pls$player_id == "x", ]
chk("players: latest club over a later World Cup", c(pls_x$league, pls_x$team_name), c("ENG", "ENG FC"))
chk("players: first and last season over every game", c(pls_x$first_season, pls_x$last_season), c(2021L, 2024L))
# A tournament season string sorts below its own year's club season as text
# ("2026 C..." < "2026-2027"); as start years they tie
pls2 <- player_list_from_match_stats(rbind(
  mk_ms("ENG", "y", 1L, as.Date("2025-09-01"), "2025-2026"),
  mk_ms("WC",  "y", 1L, as.Date("2026-06-20"), "2026 Canada-Mexico-USA")))
chk("players: tournament season counts as its start year", pls2$last_season, 2026L)
chk("players: games over every league", pls_x$games, 6L)

part_files <- function(dir) sort(list.files(dir, pattern = "^game-logs-all-by-player-part-[0-9]+\\.parquet$", full.names = TRUE))
read_parts <- function(dir) do.call(rbind, lapply(part_files(dir), rd))
read_index <- function(dir) jsonlite::fromJSON(file.path(dir, GAME_LOGS_ALL_INDEX))$parts

chk("game-logs: small input is one part", length(part_files(bd)), 1L)
chk("game-logs: no unsplit game-logs-all-by-player.parquet", file.exists(file.path(bd, "game-logs-all-by-player.parquet")), FALSE)
idx <- read_index(bd)
chk("game-logs: index lists the part", idx$file, "game-logs-all-by-player-part-001.parquet")
gl <- read_parts(bd)
chk("game-logs: index first/last player_id", c(idx$first_player_id, idx$last_player_id), c(min(gl$player_id), max(gl$player_id)))
chk("game-logs: rows = the two season files only", nrow(gl), 4L + 4L)
chk("game-logs: union columns", names(gl), c("player_id", "league", "match_date", "match_id", "epv", "piero"))
chk("game-logs: season without piero gets NA", sum(is.na(gl$piero)), 4L)
chk("game-logs: alias/by-league/by-player not read", "zzz" %in% gl$player_id, FALSE)
chk("game-logs: sorted", order(gl$player_id, gl$match_date, gl$match_id, method = "radix"), seq_len(nrow(gl)))

# Row groups never split a player, and player_id carries min/max statistics in each
split_rows <- function(path) {
  ms_ <- arrow::read_parquet(path, col_select = "player_id", mmap = FALSE)
  rdr <- ParquetFileReader$create(path, mmap = FALSE)
  sizes <- vapply(seq_len(rdr$num_row_groups) - 1L, function(i) rdr$ReadRowGroup(i)$num_rows, numeric(1))
  rm(rdr); invisible(gc())
  grp <- rep(seq_along(sizes), sizes)
  any(tapply(grp, ms_$player_id, function(g) length(unique(g))) > 1L)
}
chk("match-stats: no player split across groups", split_rows(file.path(bd, MATCH_STATS_ALL_BY_PLAYER)), FALSE)
chk("footer helper reads a plausible size", parquet_footer_bytes(file.path(bd, MATCH_STATS_ALL_BY_PLAYER)) > 0L, TRUE)

# Re-running over the same directory must not pick the outputs up as sources
before <- nrow(ms)
quietly(build_all_by_player(bd))
chk("re-run reads no output as a source", nrow(rd(file.path(bd, MATCH_STATS_ALL_BY_PLAYER))), before)
chk("re-run reads no part as a source", nrow(read_parts(bd)), 8L)

# A directory with game-logs only (sync-game-logs-r2.yml): match-stats file skipped, no failure
bd2 <- file.path(td, "blog2"); dir.create(bd2)
invisible(file.copy(file.path(bd, "game-logs-2023-2024.parquet"), bd2))
quietly(ok2 <- build_all_by_player(bd2))
chk("game-logs-only dir: one file set written", ok2, 1L)
chk("game-logs-only dir: no match-stats file", file.exists(file.path(bd2, MATCH_STATS_ALL_BY_PLAYER)), FALSE)
chk("game-logs-only dir: index written", file.exists(file.path(bd2, GAME_LOGS_ALL_INDEX)), TRUE)

# --- splitting into parts: a size limit that one file cannot meet ---
bd4 <- file.path(td, "blog4"); dir.create(bd4)
n_pl <- 300L
mk_big <- function(season, offset) {
  ids <- sprintf("p%04d", seq_len(n_pl) + offset)
  d <- data.frame(player_id = rep(ids, each = 8L), league = "ENG",
                  match_date = as.Date("2023-08-01") + rep(seq_len(8L) * 5L, times = n_pl) + offset,
                  match_id = sprintf("%s-%05d", season, seq_len(8L * n_pl)), epv = rnorm(8L * n_pl), wpa = rnorm(8L * n_pl),
                  stringsAsFactors = FALSE)
  d[sample(nrow(d)), ]
}
write_parquet(mk_big("2022-2023", 0L),   file.path(bd4, "game-logs-2022-2023.parquet"))
write_parquet(mk_big("2023-2024", 150L), file.path(bd4, "game-logs-2023-2024.parquet"))   # 150 players in both seasons
quietly(ok4 <- build_all_by_player(bd4))
chk("big dir: written", ok4, 1L)
one_part <- file.size(part_files(bd4))
chk("big dir: one part under the real limit", length(one_part), 1L)
limit_real <- ALL_BY_PLAYER_MAX_BYTES
ALL_BY_PLAYER_MAX_BYTES <- one_part * 0.7
quietly(ok5 <- build_all_by_player(bd4))
pf <- part_files(bd4)
chk("limit below one file: split into several parts", length(pf) > 1L, TRUE)
chk("every part is under the limit", all(file.size(pf) < ALL_BY_PLAYER_MAX_BYTES), TRUE)
all_rows <- read_parts(bd4)
chk("parts keep every row", nrow(all_rows), 2L * 8L * n_pl)
chk("parts are in player_id order end to end", order(all_rows$player_id, all_rows$match_date, all_rows$match_id, method = "radix"), seq_len(nrow(all_rows)))
idx <- read_index(bd4)
chk("index has one entry per part", idx$file, basename(pf))
per_part <- lapply(pf, function(f) unique(rd(f)$player_id))
chk("no player in two parts", anyDuplicated(unlist(per_part)), 0L)
chk("index first_player_id is each part's min", idx$first_player_id, vapply(per_part, min, ""))
chk("index last_player_id is each part's max", idx$last_player_id, vapply(per_part, max, ""))
chk("index ranges ascend without overlap", all(idx$first_player_id[-1] > idx$last_player_id[-nrow(idx)]), TRUE)
chk("every part has row groups", all(vapply(pf, pq_groups, 1L) >= 1L), TRUE)
# A smaller limit leaves fewer stale parts behind: a re-run with the real limit replaces all parts
ALL_BY_PLAYER_MAX_BYTES <- limit_real
quietly(build_all_by_player(bd4))
chk("re-run with the real limit: stale parts gone", length(part_files(bd4)), 1L)
# A limit nothing can meet: build stops (warns), leaving no part and no index
ALL_BY_PLAYER_MAX_BYTES <- 1000
err <- tryCatch(quietly(build_all_by_player(bd4)), error = function(e) conditionMessage(e))
chk("impossible size limit: build stops with an error", grepl("MB or over after", err), TRUE)
chk("impossible size limit: no part left", length(part_files(bd4)), 0L)
chk("impossible size limit: no index left", file.exists(file.path(bd4, GAME_LOGS_ALL_INDEX)), FALSE)
ALL_BY_PLAYER_MAX_BYTES <- limit_real

# match-stats-by-player.parquet over the limit also stops the build and leaves no file
bd6 <- file.path(td, "blog6"); dir.create(bd6)
write_parquet(mk_ms("ENG", c("a", "b", "c"), 4L), file.path(bd6, "match-stats-ENG.parquet"))
ALL_BY_PLAYER_MAX_BYTES <- 1000
err <- tryCatch(quietly(build_all_by_player(bd6)), error = function(e) conditionMessage(e))
chk("match-stats over the limit: build stops with an error", grepl("not under the", err), TRUE)
chk("match-stats over the limit: no file left", file.exists(file.path(bd6, MATCH_STATS_ALL_BY_PLAYER)), FALSE)
ALL_BY_PLAYER_MAX_BYTES <- limit_real

# A type clash between seasons drops the game-logs file set instead of coercing
bd5 <- file.path(td, "blog5"); dir.create(bd5)
write_parquet(mk_gl("2023-2024", c("a", "b")), file.path(bd5, "game-logs-2023-2024.parquet"))
bad <- mk_gl("2024-2025", c("a", "b")); bad$epv <- as.character(bad$epv)
write_parquet(bad, file.path(bd5, "game-logs-2024-2025.parquet"))
quietly(ok7 <- build_all_by_player(bd5))
chk("type clash across seasons: nothing written", ok7, 0L)
chk("type clash across seasons: no parts", length(part_files(bd5)), 0L)

# A shard missing `league` fails the whole match-stats file, and a stale copy is removed
bd3 <- file.path(td, "blog3"); dir.create(bd3)
write_parquet(mk_ms("ENG", c("a", "b"), 2L), file.path(bd3, "match-stats-ENG.parquet"))
write_parquet(mk_ms("WC", "c", 2L)[, c("player_id", "match_date", "match_id", "goals")], file.path(bd3, "match-stats-WC.parquet"))
invisible(file.create(file.path(bd3, MATCH_STATS_ALL_BY_PLAYER)))
quietly(ok3 <- build_all_by_player(bd3))
chk("shard without league: nothing written", ok3, 0L)
chk("shard without league: stale output removed", file.exists(file.path(bd3, MATCH_STATS_ALL_BY_PLAYER)), FALSE)

# A missing expected shard or season writes nothing and removes stale output
bd7 <- file.path(td, "blog7"); dir.create(bd7)
write_parquet(mk_ms("ENG", c("a", "b"), 2L), file.path(bd7, "match-stats-ENG.parquet"))
write_parquet(mk_gl("2023-2024", c("a", "b")), file.path(bd7, "game-logs-2023-2024.parquet"))
invisible(file.create(file.path(bd7, MATCH_STATS_ALL_BY_PLAYER)))
invisible(file.create(file.path(bd7, GAME_LOGS_ALL_INDEX)))
quietly(ok8 <- build_all_by_player(bd7, expect_match_stats = c("ENG", "ESP"), expect_game_logs = c("2023-2024", "2024-2025")))
chk("missing expected sources: nothing written", ok8, 0L)
chk("missing expected shard: stale match-stats output removed", file.exists(file.path(bd7, MATCH_STATS_ALL_BY_PLAYER)), FALSE)
chk("missing expected shard: no player list", file.exists(file.path(bd7, MATCH_STATS_PLAYERS)), FALSE)
chk("missing expected season: stale index removed", file.exists(file.path(bd7, GAME_LOGS_ALL_INDEX)), FALSE)
# ...and with the expected lists met, both are written
quietly(ok9 <- build_all_by_player(bd7, expect_match_stats = "ENG", expect_game_logs = "2023-2024"))
chk("expected sources all present: both written", ok9, 2L)
# The lists come from space-separated env vars; unset means no check
Sys.unsetenv(c("ALL_BY_PLAYER_MATCH_STATS", "ALL_BY_PLAYER_GAME_LOGS"))
chk("unset env: no expected files", expected_from_env("ALL_BY_PLAYER_MATCH_STATS"), character(0))
Sys.setenv(ALL_BY_PLAYER_MATCH_STATS = " ENG  WC ")
chk("env list split on spaces", expected_from_env("ALL_BY_PLAYER_MATCH_STATS"), c("ENG", "WC"))
Sys.unsetenv("ALL_BY_PLAYER_MATCH_STATS")

# prove this harness can fail
local({
  before <- failed
  sink(tempfile()); chk("deliberately wrong expectation", nrow(u), 99L); sink()
  if (failed != before + 1L) { cat("FAIL: the assert helper did not report a known-bad expectation\n"); quit(status = 1L) }
  failed <<- before
})

if (failed > 0L) { cat("\n", failed, "check(s) failed\n"); quit(status = 1L) }
cat("all-leagues by-player files: all checks passed\n")
