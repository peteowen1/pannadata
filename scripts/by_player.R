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
# player-skills-pct.parquet and primary_position in player-index.parquet are
# built here too, from blog/player-skills.parquet (pannadata#182).

suppressPackageStartupMessages(library(arrow))

# Size in bytes of a parquet file's footer: the 4-byte little-endian length
# just before the closing "PAR1" magic.
parquet_footer_bytes <- function(path) {
  con <- file(path, "rb"); on.exit(close(con))
  seek(con, file.size(path) - 8L)
  readBin(con, "integer", 1L, size = 4L, endian = "little")
}

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
  # Drop R's attribute metadata (footer key "r"), as strip_parquet_r_metadata.R
  # does: torpdata's game-stats carried 8.7 MB of it (2026-10-07), which would
  # trip max_footer below. Browsers never read it.
  tbl$metadata$r <- NULL
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
  footer <- parquet_footer_bytes(path)
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

# write_parquet_by_player for a file that has to fit under `max_footer` without
# the caller knowing how many columns it will have: writes with `rows` per group
# and, if the footer comes out over `max_footer`, rewrites with `grow` x larger
# groups until it fits (the footer is row groups x columns, so wider files need
# bigger groups: the all-seasons game-logs file has 57 columns, and 5,000-row
# groups gave a footer over 2 MB). Gives up after `max_tries` writes, leaving
# the last file in place for the caller to remove. Returns the `rows` used.
write_parquet_by_player_fit <- function(df, path, by = BY_PLAYER_SORT, rows = 5000L,
                                        max_footer = 900000, grow = 1.5, max_tries = 6L) {
  if (by[1] != "player_id") stop(path, ": by-player file needs player_id as the first sort key")
  for (i in seq_len(max_tries)) {
    write_parquet_packed(df, path, by = by, rows = rows, max_footer = Inf)
    footer <- parquet_footer_bytes(path)
    if (footer <= max_footer) return(invisible(rows))
    cat(sprintf("%s: footer %d KB over the %d KB limit with %d-row groups, retrying with larger groups\n",
                path, footer %/% 1024L, as.integer(max_footer) %/% 1024L, as.integer(rows)))
    rows <- as.integer(ceiling(rows * grow))
  }
  stop(sprintf("%s: footer still over %d bytes after %d writes (last try %d-row groups)",
               path, as.integer(max_footer), as.integer(max_tries), as.integer(rows)))
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
#
# `positions` (optional; pannadata#182): a data frame with player_id and
# primary_position (player-skills.parquet's), which adds a primary_position
# column to the index, NA for a rated player with no skills row. The page builds
# its player_name -> primary_position map from it instead of reading
# player-skills.parquet. Left out (column absent, not all-NA) when NULL.
write_player_index <- function(ratings, path, rows = 500L, positions = NULL) {
  idx <- as.data.frame(ratings)[, c("player_id", "player_name", "league")]
  if (anyDuplicated(idx$player_id)) stop(path, ": ratings has duplicate player_id, so the index would be ambiguous")
  cols <- c("player_id", "player_name", "league")
  if (!is.null(positions)) {
    positions <- as.data.frame(positions)
    if (anyDuplicated(positions$player_id)) stop(path, ": positions has duplicate player_id, so primary_position would be ambiguous")
    idx$primary_position <- as.character(positions$primary_position[match(idx$player_id, positions$player_id)])
    cols <- c(cols, "primary_position")
  }
  write_parquet_by_player(idx, path, by = "player_id", rows = rows)
  back <- as.data.frame(arrow::read_parquet(path))
  key <- function(x) do.call(paste, c(unname(as.list(x[cols])), sep = "\r"))
  if (nrow(back) != nrow(idx) || !setequal(key(back), key(idx))) {
    stop(sprintf("%s: %d rows read back, %d in ratings, or the (%s) tuples differ", path, nrow(back), nrow(idx), paste(cols, collapse = ", ")))
  }
  cat(sprintf("%s: %d rows, same (%s) tuples as ratings.parquet\n", path, nrow(back), paste(cols, collapse = ", ")))
  invisible(nrow(back))
}

# player-skills-pct.parquet (pannadata#182): /football/player ranks ONE player
# against everyone in every skills column, which needed all 154 columns of
# every player (player-skills.parquet, 6.5 MB). This is that ranking done here:
# long format, one row per (player, stat) with the player's value and its
# percentile among all players (pct_all) and among players of the same
# primary_position (pct_pos), sorted by player_id in row groups that never split
# a player, so a player_id filter reads one group. player_name is the skills
# file's own spelling, repeated on each of the player's rows: the page's heading
# shows it, and player-index.parquet carries ratings' spelling, which differs
# for ~195 players ("S. Mckenna" vs "S. McKenna").
#
# The rule is the page's (player.qmd skillRows), kept exactly: a column's pool is
# its non-null values; pct = round(100 * count(v <= x) / n), 50 when n == 1.
# round() is JavaScript's Math.round (half up), NOT R's round() (half to even),
# or x.5 cases would differ from today's page. Stats are discovered by type, not
# listed, so a new skills column flows through; only the id/descriptor columns
# below are skipped (the page's own skip list). A null (NA/NaN) value gets no
# row. A null or empty primary_position has no position pool: its pct_pos is
# its pct_all, because the page's Position toggle falls back to every player
# when the player has no position (`pctileMode === "Position" &&
# playerSkills.primary_position ? ... : skillsAll`).
SKILLS_PCT_SKIP <- c("player_id", "total_minutes", "n_matches", "player_name", "primary_position",
                     "date", "weighted_90s", "season_end_year", "is_gk", "is_df", "is_mf", "is_fw")

# Percentile of each element of `v` (no NA) within `v`. rank(ties.method = "max")
# is count(v <= x).
pool_pct <- function(v) {
  n <- length(v)
  if (n == 1L) return(50L)
  as.integer(floor(100 * rank(v, ties.method = "max") / n + 0.5))
}

skills_pct_long <- function(skills) {
  skills <- as.data.frame(skills)
  for (col in c("player_id", "player_name", "primary_position")) if (!col %in% names(skills)) stop("skills has no ", col, " column")
  if (anyNA(skills$player_id)) stop("skills has rows with NA player_id")
  if (anyDuplicated(skills$player_id)) stop("skills has duplicate player_id (expected one row per player)")
  stats <- setdiff(names(skills)[vapply(skills, is.numeric, logical(1))], SKILLS_PCT_SKIP)
  if (length(stats) == 0L) stop("skills has no numeric stat columns")
  pos <- as.character(skills$primary_position)
  has_pos <- !is.na(pos) & pos != ""
  pos_group <- split(which(has_pos), pos[has_pos])
  pieces <- lapply(stats, function(st) {
    v <- as.numeric(skills[[st]])
    ok <- which(!is.na(v))
    if (length(ok) == 0L) return(NULL)
    pct_all <- pool_pct(v[ok])
    pct_pos <- pct_all  # rows with no position keep this
    for (g in pos_group) {
      in_g <- ok %in% g
      if (any(in_g)) pct_pos[in_g] <- pool_pct(v[ok[in_g]])
    }
    data.frame(player_id = skills$player_id[ok], player_name = as.character(skills$player_name[ok]),
               stat = st, value = v[ok],
               pct_all = pct_all, pct_pos = pct_pos, stringsAsFactors = FALSE)
  })
  do.call(rbind, pieces)
}

write_player_skills_pct <- function(skills, path, rows = 5000L) {
  long <- skills_pct_long(skills)
  write_parquet_by_player(long, path, by = c("player_id", "stat"), rows = rows)
  back <- as.data.frame(arrow::read_parquet(path))
  if (nrow(back) != nrow(long) ||
      !isTRUE(all.equal(sum(back$value), sum(long$value))) ||
      sum(back$pct_all) != sum(long$pct_all) || sum(back$pct_pos) != sum(long$pct_pos)) {
    stop(sprintf("%s: read back %d rows, wrote %d, or value/pct sums differ", path, nrow(back), nrow(long)))
  }
  cat(sprintf("%s: %d stats x %d players -> %d rows\n", path, length(unique(long$stat)), length(unique(long$player_id)), nrow(long)))
  invisible(nrow(long))
}

# Runs after player-skills.parquet is in blog_dir (the workflow's "Download and
# filter player skills" step). Writes player-skills-pct.parquet and re-writes
# player-index.parquet with primary_position, from ratings.parquet plus the
# skills file. Both are optional and independent: a failure warns and leaves the
# page on player-skills.parquet / the index without primary_position (the index
# is replaced only once the new one has passed its checks).
build_player_skills_pct <- function(blog_dir = "blog") {
  skills_path <- file.path(blog_dir, "player-skills.parquet")
  if (!file.exists(skills_path)) {
    message("::warning::", skills_path, " not present, so no player-skills-pct.parquet and no primary_position in player-index.parquet this run")
    return(invisible(FALSE))
  }
  skills <- tryCatch(arrow::read_parquet(skills_path), error = function(e) {
    message("::warning::", skills_path, " unreadable, so no player-skills-pct.parquet and no primary_position in player-index.parquet this run: ", conditionMessage(e))
    NULL
  })
  if (is.null(skills)) return(invisible(FALSE))
  failed <- character()
  out <- file.path(blog_dir, "player-skills-pct.parquet")
  tryCatch(write_player_skills_pct(skills, out),
           error = function(e) {
             unlink(out); failed <<- c(failed, out)
             message("::warning::", out, " NOT written (the page falls back to player-skills.parquet): ", conditionMessage(e))
           })
  idx_path <- file.path(blog_dir, "player-index.parquet")
  ratings_path <- file.path(blog_dir, "ratings.parquet")
  tmp <- file.path(blog_dir, "player-index.tmp.parquet")
  tryCatch({
    if (!file.exists(ratings_path)) stop(ratings_path, " not present")
    ratings <- arrow::read_parquet(ratings_path, col_select = c("player_id", "player_name", "league"))
    write_player_index(ratings, tmp, positions = skills[, c("player_id", "primary_position")])
    if (!file.rename(tmp, idx_path)) stop("could not replace ", idx_path)
  }, error = function(e) {
    unlink(tmp); failed <<- c(failed, idx_path)
    message("::warning::", idx_path, " keeps no primary_position (the page falls back to player-skills.parquet): ", conditionMessage(e))
  })
  invisible(failed)
}

# game-logs-<season>-by-league.parquet (pannadata#175), for the
# /football/player-stats "All Seasons" Value tab: it reads every season's
# game-logs to total one league, and each original holds every league in one
# row group (12-16 MB per season, ~26 s for 17 seasons). This copy holds the
# same rows and columns sorted by league, ONE row group per league (rows = 1:
# no group mixes two leagues, a big league is never split), with min/max
# statistics on `league`, so a league filter reads only that league's group.
# ~22 groups per season, so the footer stays small. Originals untouched.
write_game_logs_by_league <- function(gl, path) {
  gl <- as.data.frame(gl)
  if (!is.character(gl$league)) stop(path, ": league is ", class(gl$league)[1], ", expected a plain string column")
  n_rg <- write_parquet_packed(gl, path, by = c("league", "player_id", "match_date", "match_id"), rows = 1L)
  n_lg <- length(unique(gl$league))
  if (n_rg != n_lg) stop(sprintf("%s: %d row groups for %d leagues", path, n_rg, n_lg))
  invisible(n_rg)
}

GAME_LOGS_SEASON_RE <- "^game-logs-[0-9]{4}-[0-9]{4}\\.parquet$"

# Rewrites each game-logs-<season>.parquet in place as league-sorted,
# 20,000-row groups (pannadata#153). The files come from panna step 10b as ONE
# row group, so /football/world-cup-player, which keeps `league == "WC"` (3,272
# of 194,971 rows for 2025-26), decoded all of it. With league leading, each
# group's min/max on `league` lets a reader skip every group that cannot hold
# the league: 10 groups for 2025-26, WC in the last one. Rows keep their
# existing order inside a league (stable radix sort; panna already writes them
# league-sorted, so this is normally a no-op reorder), and the columns and
# values are untouched. 20,000 rows over 10,000: measured on 2025-26 (pyarrow,
# snappy) the file is 21.8 MB as one group, 26.9 MB at 20,000 and 29.8 MB at
# 10,000, because the string columns' dictionaries are rebuilt per group; the
# footer is 72 KB against 135 KB. A season under `rows` rows stays one group.
# Failure-soft per file: the original stays in place.
rechunk_game_logs_by_league <- function(path, rows = 20000L) {
  df <- as.data.frame(arrow::read_parquet(path))
  if (!is.character(df$league)) stop(path, ": league is ", class(df$league)[1], ", expected a plain string column")
  if (anyNA(df$league)) stop(sprintf("%s: %d rows with NA league", path, sum(is.na(df$league))))
  df <- df[order(df$league, method = "radix"), , drop = FALSE]
  rownames(df) <- NULL
  tbl <- arrow::arrow_table(df)
  tbl$metadata$r <- NULL  # R attributes in the footer, as by_player.R's other writers
  tmp <- paste0(path, ".tmp")
  on.exit(unlink(tmp), add = TRUE)
  arrow::write_parquet(tbl, tmp, chunk_size = rows, compression = "snappy")
  rdr <- arrow::ParquetFileReader$create(tmp)
  if (rdr$num_rows != nrow(df)) stop(sprintf("%s: %d rows written, expected %d", path, rdr$num_rows, nrow(df)))
  expect_rg <- ceiling(nrow(df) / rows)
  if (rdr$num_row_groups != expect_rg) stop(sprintf("%s: wrote %d row groups, expected %d", path, rdr$num_row_groups, expect_rg))
  rm(rdr); invisible(gc())  # Windows: release the reader's handle before the rename
  if (!file.rename(tmp, path)) stop(path, ": could not replace the original with the re-chunked copy")
  cat(sprintf("%s: %d rows, %d row group(s), %.2f MB\n", path, nrow(df), as.integer(expect_rg), file.info(path)$size / 1024^2))
  invisible(as.integer(expect_rg))
}

build_game_logs_chunks <- function(blog_dir = "blog") {
  srcs <- list.files(blog_dir, pattern = GAME_LOGS_SEASON_RE, full.names = TRUE)
  failed <- character()
  for (src in srcs) {
    tryCatch(rechunk_game_logs_by_league(src),
             error = function(e) {
               failed <<- c(failed, src)
               message("::warning::", src, " left as panna wrote it (one row group): ", conditionMessage(e))
             })
  }
  cat(sprintf("game-logs league-sorted row groups: %d of %d written\n", length(srcs) - length(failed), length(srcs)))
  invisible(failed)
}

build_by_league_copies <- function(blog_dir = "blog") {
  srcs <- list.files(blog_dir, pattern = GAME_LOGS_SEASON_RE, full.names = TRUE)
  if (length(srcs) == 0L) message("::warning::no game-logs-<season>.parquet in ", blog_dir, ", so no -by-league copies this run")
  failed <- character()
  for (src in srcs) {
    out <- sub("\\.parquet$", "-by-league.parquet", src)
    tryCatch(write_game_logs_by_league(arrow::read_parquet(src), out),
             error = function(e) {
               unlink(out)
               failed <<- c(failed, out)
               message("::warning::", out, " NOT written (the page falls back to ", basename(src), "): ", conditionMessage(e))
             })
  }
  cat(sprintf("by-league game-logs copies: %d of %d written\n", length(srcs) - length(failed), length(srcs)))
  invisible(failed)
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

# match-stats-by-player.parquet and game-logs-all-by-player.parquet (pannadata#193):
# ONE file over every league and season, sorted by player_id, for a player
# search that has to cover every league a player has played in. The blog's
# game finder otherwise reads all 20 match-stats-<code> shards (1.67M rows) to
# keep a few hundred, plus one game-logs-<season>-by-league read per season.
# Same layout as the other by-player files (row groups that never split a
# player, player_id min/max statistics, footer under 900 KB), via
# write_parquet_by_player_fit. The per-league files are untouched.
#
# - match-stats: every match-stats-<code>.parquet shard (clubs and the
#   WC/EURO/Copa_America/AFCON shards), `league` kept, columns as the shards.
# - game-logs: every game-logs-<season>.parquet (not the game-logs.parquet
#   alias, which repeats the newest season). Seasons differ in columns (older
#   ones have no piero/epv_*, the newest no panna columns yet), so the file has
#   the union and a season without a column gets NA there.
# The name is game-logs-ALL-by-player because game-logs-by-player.parquet is
# already the current-season file /football/player reads.
MATCH_STATS_ALL_BY_PLAYER <- "match-stats-by-player.parquet"
GAME_LOGS_ALL_BY_PLAYER   <- "game-logs-all-by-player.parquet"
ALL_BY_PLAYER_SORT <- c("player_id", "match_date", "match_id")

# Stack data frames that share most columns: columns a frame lacks are added as
# NA of the type another frame gives that column; column order follows first
# appearance.
bind_rows_fill <- function(dfs) {
  cols <- unique(unlist(lapply(dfs, names), use.names = FALSE))
  ref <- list()
  for (d in dfs) for (nm in names(d)) if (is.null(ref[[nm]])) ref[[nm]] <- d[[nm]][0L]
  dfs <- lapply(dfs, function(d) {
    d <- as.data.frame(d)
    for (nm in setdiff(cols, names(d))) d[[nm]] <- ref[[nm]][rep(NA_integer_, nrow(d))]
    d[, cols, drop = FALSE]
  })
  out <- do.call(rbind, dfs)
  rownames(out) <- NULL
  out
}

# Reads `srcs`, stacks them, checks the row count against the files' footers
# and writes one by-player file. Returns TRUE if written; on any problem
# removes `out` and returns FALSE with a warning, so a half-read set of shards
# never ships as "every league".
write_all_by_player <- function(srcs, out, rows, require_cols = "player_id") {
  tryCatch({
    n_src <- sum(vapply(srcs, function(f) arrow::ParquetFileReader$create(f)$num_rows, numeric(1)))
    # Required columns are checked per file: stacking would fill a missing one with NA
    df <- bind_rows_fill(lapply(srcs, function(f) {
      d <- as.data.frame(arrow::read_parquet(f))
      miss <- setdiff(require_cols, names(d))
      if (length(miss)) stop(basename(f), " lacks column(s) ", paste(miss, collapse = ", "))
      d
    }))
    if (nrow(df) != n_src) stop(sprintf("%d rows after stacking %d files, the files hold %d", nrow(df), length(srcs), n_src))
    write_parquet_by_player_fit(df, out, by = ALL_BY_PLAYER_SORT, rows = rows)
    TRUE
  }, error = function(e) {
    unlink(out)
    message("::warning::", out, " NOT written: ", conditionMessage(e))
    FALSE
  })
}

build_all_by_player <- function(blog_dir = "blog") {
  ms <- list.files(blog_dir, pattern = "^match-stats-.+\\.parquet$", full.names = TRUE)
  ms <- ms[!grepl("-by-(player|team)\\.parquet$", ms)]
  gl <- list.files(blog_dir, pattern = GAME_LOGS_SEASON_RE, full.names = TRUE)
  jobs <- list(
    list(srcs = ms, out = file.path(blog_dir, MATCH_STATS_ALL_BY_PLAYER), rows = 6000L, req = c("player_id", "match_date", "match_id", "league")),
    list(srcs = gl, out = file.path(blog_dir, GAME_LOGS_ALL_BY_PLAYER),   rows = 15000L, req = c("player_id", "match_date", "match_id", "league"))
  )
  ok <- 0L
  for (j in jobs) {
    if (length(j$srcs) == 0L) {
      # Expected in sync-game-logs-r2.yml, which downloads no match-stats
      cat(basename(j$out), ": no source files in ", blog_dir, ", not written\n", sep = "")
      next
    }
    ok <- ok + write_all_by_player(j$srcs, j$out, j$rows, j$req)
  }
  cat(sprintf("all-leagues by-player files: %d written\n", ok))
  invisible(ok)
}

if (sys.nframe() == 0L) {
  args <- commandArgs(trailingOnly = TRUE)
  blog_dir <- if (length(args) > 0) args[1] else "blog"
  build_by_player_copies(blog_dir)
  build_game_logs_chunks(blog_dir)
  build_by_league_copies(blog_dir)
  build_player_skills_pct(blog_dir)
  build_all_by_player(blog_dir)
}
