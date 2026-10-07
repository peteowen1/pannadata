# Per-player copies of the football blog parquets, per-team copies of
# match-stats (sorted by club, each club in one row group, for /football/team), and a small player index
# (pannadata#164), for /football/player.
#
# The page filters match-stats-<CODE>.parquet and game-logs.parquet on one
# player_id, but those files are laid out for other pages (match-stats in
# 20,000-row groups that /football/match reads by match), so a player page
# read all 103,323 rows of match-stats-GER to keep one player's games. The
# copies written here hold the same rows and columns sorted by player_id,
# packed into row groups that never split a player, so a player_id filter
# reads one group. The originals are NOT re-sorted: /football/match and other
# pages depend on their order. Same pattern as torpdata#112
# (torpdata scripts/parquet_helpers.R write_parquet_by_player).
#
# Run from the repo root after the match-stats and game-logs files are in blog/:
#   Rscript scripts/by_player.R [blog_dir]
# build_blog_data.R sources this file for write_player_index() only.

suppressPackageStartupMessages(library(arrow))

# Same rows and columns as `df`, sorted by by[1] (the key a page filters on)
# then by[-1], in byte order (method = "radix", the order parquet min/max
# statistics compare in), packed into row groups of up to `rows` rows that
# never split a key value (a key with more than `rows` rows gets a group of its
# own; rows = 1 gives one group per key). SNAPPY, as every football blog
# parquet: the blog loads plain hyparquet with no other codecs. Logs the
# row-group count, rows per group and footer size; returns the group count.
#
# The footer is the hidden cost: a browser downloads all of it before any row,
# and it grows with row groups x columns (ENG2 match-stats in 1,500-row groups:
# 431 KB of footer to read a 63 KB group; one group per UEL club: 1.5 MB). So
# min/max statistics are written for the key column only (the one pages filter
# on; ~1/3 of the footer otherwise), groups default to 5,000 rows (footer plus
# one group was smallest there, measured on ENG/ENG2/UEL 2026-10-07), and a
# footer over `max_footer` stops the write: strip_parquet_r_metadata.R rewrites
# any file whose footer is over 1 MB, re-chunking it and losing the layout.
write_parquet_packed <- function(df, path, by, rows = 5000L, max_footer = 900000) {
  df <- as.data.frame(df)
  key <- by[1]
  if (!key %in% names(df)) stop(path, ": key column ", key, " missing")
  if (anyNA(df[[key]])) stop(sprintf("%s: %d rows with NA %s", path, sum(is.na(df[[key]])), key))
  if (nrow(df) == 0L) stop(path, ": no rows")
  by <- intersect(by, names(df))
  df <- df[do.call(order, c(unname(as.list(df[by])), method = "radix")), , drop = FALSE]
  rownames(df) <- NULL
  # Greedy packing of whole keys: start a new group when adding the next
  # key's rows would take the current one past `rows`.
  run <- rle(df[[key]])
  grp <- integer(length(run$lengths)); g <- 1L; filled <- 0L
  for (i in seq_along(run$lengths)) {
    if (filled > 0L && filled + run$lengths[i] > rows) { g <- g + 1L; filled <- 0L }
    grp[i] <- g; filled <- filled + run$lengths[i]
  }
  sizes <- as.integer(tapply(run$lengths, grp, sum))
  ends <- cumsum(sizes); starts <- c(1L, head(ends, -1L) + 1L)
  tbl <- arrow::arrow_table(df)
  sink <- arrow::FileOutputStream$create(path)
  props <- arrow::ParquetWriterProperties$create(names(df), compression = "snappy",
                                                 write_statistics = stats::setNames(names(df) == key, names(df)))
  writer <- arrow::ParquetFileWriter$create(tbl$schema, sink, properties = props)
  for (i in seq_along(starts)) {
    writer$WriteTable(tbl[starts[i]:ends[i], ], chunk_size = sizes[i])
  }
  writer$Close()
  sink$close()
  rdr <- arrow::ParquetFileReader$create(path)
  n_rg <- rdr$num_row_groups
  if (n_rg != length(sizes)) stop(sprintf("%s: wrote %d row groups, expected %d", path, n_rg, length(sizes)))
  if (rdr$num_rows != nrow(df)) stop(sprintf("%s: %d rows written, expected %d", path, rdr$num_rows, nrow(df)))
  con <- file(path, "rb"); seek(con, file.size(path) - 8L)
  footer <- readBin(con, "integer", 1L, size = 4L, endian = "little"); close(con)
  if (footer > max_footer) stop(sprintf("%s: footer is %d bytes (limit %d) with %d row groups; raise `rows`", path, footer, as.integer(max_footer), n_rg))
  cat(sprintf("%s: %d rows, %d distinct %s, %d row groups (rows per group: min %d, median %d, max %d; largest %s %d rows), footer %d KB, %.2f MB\n",
              path, nrow(df), length(run$lengths), key, n_rg, min(sizes), as.integer(stats::median(sizes)), max(sizes),
              key, max(run$lengths), footer %/% 1024L, file.info(path)$size / 1024^2))
  invisible(n_rg)
}

write_parquet_by_player <- function(df, path, by = "player_id", rows = 5000L) {
  if (by[1] != "player_id") stop(path, ": by-player file needs player_id as the first sort key")
  write_parquet_packed(df, path, by = by, rows = rows)
}

# match-stats-<CODE>-by-team.parquet (pannadata#164 comment 6028785255), for
# /football/team: every row of every match a club played (both teams' rows:
# the xG trend and goals against need the opponent's), with `for_team` = the
# club's team_name, sorted by for_team. Each match appears twice, so the file
# is about twice the original, but a for_team filter reads one row group
# instead of the whole shard (124,548 rows for ENG). Same idea as torpdata's
# write_chain_events_by_team, except that small clubs share a group (never
# splitting one): one group per club gave UEL 376 groups and a 1.5 MB footer,
# more than the page saved. A club with over 5,000 rows has a group to itself.
# /football/match keeps reading the original, which is grouped by match.
write_match_stats_by_team <- function(ms, path) {
  ms <- as.data.frame(ms)
  if (anyNA(ms$team_name)) stop(sprintf("%s: %d rows with NA team_name", path, sum(is.na(ms$team_name))))
  # Every match must have exactly two clubs, or "both teams' rows" (and the 2x check) is wrong
  n_teams <- tapply(ms$team_name, ms$match_id, function(x) length(unique(x)))
  if (any(n_teams != 2L)) stop(sprintf("%s: %d of %d matches do not have exactly 2 team_names", path, sum(n_teams != 2L), length(n_teams)))
  pairs <- unique(ms[, c("match_id", "team_name")])
  names(pairs)[2] <- "for_team"
  out <- merge(pairs, ms, by = "match_id", sort = FALSE)
  out <- out[, c("for_team", names(ms))]
  teams <- sort(unique(ms$team_name), method = "radix")
  if (nrow(out) != 2L * nrow(ms)) stop(sprintf("%s: %d rows, expected 2 x %d (each match under both clubs)", path, nrow(out), nrow(ms)))
  n_rg <- write_parquet_packed(out, path, by = c("for_team", "season", "match_date", "match_id", "team_name", "player_id"))
  cat(sprintf("%s: %d clubs in %d row groups, each club in exactly one; %d rows = 2 x %d\n", path, length(teams), n_rg, nrow(out), nrow(ms)))
  invisible(n_rg)
}

# player-index.parquet: one row per rated player, (player_id, player_name,
# league) exactly as ratings.parquet has them, so /football/player can learn a
# player's name and league without waiting for the full ratings file (0.79 MB
# even projected to these 3 columns, because the ids are long). Sorted by
# player_id in 500-row groups, so a player_id filter reads a few KB. Built from
# the same data frame ratings.parquet is written from, then checked against it.
write_player_index <- function(ratings, path, rows = 500L) {
  idx <- as.data.frame(ratings)[, c("player_id", "player_name", "league")]
  if (anyDuplicated(idx$player_id)) stop(path, ": ratings has duplicate player_id, so the index would be ambiguous")
  write_parquet_by_player(idx, path, by = "player_id", rows = rows)
  back <- as.data.frame(arrow::read_parquet(path))
  key <- function(x) paste(x$player_id, x$player_name, x$league, sep = "\r")
  if (nrow(back) != nrow(idx) || !setequal(key(back), key(idx))) {
    stop(sprintf("%s: %d rows read back, %d in ratings, or the (player_id, player_name, league) triples differ", path, nrow(back), nrow(idx)))
  }
  cat(sprintf("%s: %d rows, same (player_id, player_name, league) triples as ratings.parquet\n", path, nrow(back)))
  invisible(nrow(back))
}

# Sort keys after player_id, per file family. Absent columns are skipped.
BY_PLAYER_SORT <- c("player_id", "season", "match_date", "match_id")

build_by_player_copies <- function(blog_dir = "blog") {
  ms <- list.files(blog_dir, pattern = "^match-stats-.+\\.parquet$", full.names = TRUE)
  ms <- ms[!grepl("-by-(player|team)\\.parquet$", ms)]
  gl <- file.path(blog_dir, "game-logs.parquet")
  if (!file.exists(gl)) message("::warning::", gl, " not present, so no game-logs-by-player.parquet this run")
  # One job per output file: (source, output, writer)
  jobs <- c(
    lapply(c(ms, if (file.exists(gl)) gl), function(src) list(src = src, out = sub("\\.parquet$", "-by-player.parquet", src),
      write = function(df, out) write_parquet_by_player(df, out, by = BY_PLAYER_SORT))),
    lapply(ms, function(src) list(src = src, out = sub("\\.parquet$", "-by-team.parquet", src),
      write = write_match_stats_by_team))
  )
  failed <- character()
  for (j in jobs) {
    tryCatch(j$write(arrow::read_parquet(j$src), j$out),
             error = function(e) {
               unlink(j$out)
               failed <<- c(failed, j$out)
               message("::warning::", j$out, " NOT written (the page falls back to ", basename(j$src), "): ", conditionMessage(e))
             })
  }
  cat(sprintf("by-player / by-team copies: %d of %d written\n", length(jobs) - length(failed), length(jobs)))
  invisible(failed)
}

if (sys.nframe() == 0L) {
  args <- commandArgs(trailingOnly = TRUE)
  build_by_player_copies(if (length(args) > 0) args[1] else "blog")
}
