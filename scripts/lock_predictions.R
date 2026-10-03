# lock_predictions.R — keep each football match's last PRE-MATCH forecast (pannadata#156)
#
# Why: scripts/07_predict_fixtures.R (panna) re-scores every row of
# match_predictions.parquet on every run, played games included, so prob_H/D/A
# for a finished match is today's view, not the forecast made before kickoff.
# This script keeps a separate append-only copy: football/predictions-locked.parquet,
# one row per match_id.
#
# Lock rule (match_date carries a DATE only, no kickoff time):
#   - A fixture is refreshed on every run while its match_date is STRICTLY AFTER
#     the run date (so the run is certainly before kickoff).
#   - On or after match_date the row is frozen and never touched again, even if
#     the model later re-scores it. locked_at is the run time of the last refresh,
#     so locked_at < kickoff always holds (a day-of kickoff cannot be earlier than
#     the previous day's run).
#   - A match first seen on its own match day, or already played, is NOT
#     inserted: no genuine pre-match forecast exists for it.
#
# Seeding (one-off): predictions-history holds dated daily snapshots since
# 2026-07-25. seed_from_history() turns them into locked rows (last snapshot
# dated strictly before match_date, status == "fixture"). Existing locked rows
# always win over seeds.
#
# Usage: Rscript scripts/lock_predictions.R <predictions.parquet> <prev-locked.parquet|NONE> \
#            <out.parquet> [history_dir]

suppressPackageStartupMessages({ library(arrow); library(dplyr) })

LOCK_COLS <- c("match_id", "match_date", "league", "season", "home_team", "away_team",
               "prob_H_pre", "prob_D_pre", "prob_A_pre",
               "pred_home_goals_pre", "pred_away_goals_pre", "locked_at", "lock_source")

as_lock_rows <- function(df, locked_at, source) {
  tibble(
    match_id = as.character(df$match_id),
    match_date = as.character(df$match_date),
    league = as.character(df$league),
    season = as.character(df$season),
    home_team = as.character(df$home_team),
    away_team = as.character(df$away_team),
    prob_H_pre = as.numeric(df$prob_H),
    prob_D_pre = as.numeric(df$prob_D),
    prob_A_pre = as.numeric(df$prob_A),
    pred_home_goals_pre = as.numeric(df$pred_home_goals),
    pred_away_goals_pre = as.numeric(df$pred_away_goals),
    locked_at = rep(as.character(locked_at), nrow(df)),
    lock_source = rep(source, nrow(df))
  )
}

date_of <- function(x) as.Date(substr(as.character(x), 1, 10))

# current: match_predictions.parquet. locked: previous locked table (or NULL).
lock_predictions <- function(current, locked = NULL, run_time = Sys.time()) {
  run_date <- as.Date(format(run_time, tz = "UTC"))
  stamp <- format(run_time, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
  md <- date_of(current$match_date)
  if (any(is.na(md))) stop("lock_predictions: unparseable match_date in predictions")
  cand <- current[current$status == "fixture" & md > run_date, , drop = FALSE]
  fresh <- as_lock_rows(cand, stamp, "daily")
  if (is.null(locked) || nrow(locked) == 0) return(fresh)
  # A previously locked row is overwritten only if its match is still a future
  # fixture in the current predictions; everything else is carried over verbatim.
  keep <- locked[!locked$match_id %in% fresh$match_id, LOCK_COLS]
  bind_rows(keep, fresh)
}

# history_dir holds predictions_<YYYY-MM-DD>.parquet snapshots.
seed_from_history <- function(history_dir) {
  files <- list.files(history_dir, pattern = "^predictions_[0-9]{4}-[0-9]{2}-[0-9]{2}[.]parquet$",
                      full.names = TRUE)
  out <- list()
  for (f in sort(files)) {          # ascending: later snapshots overwrite earlier
    snap <- as.Date(substr(basename(f), 13, 22))   # predictions_YYYY-MM-DD.parquet
    s <- read_parquet(f)
    s <- s[s$status == "fixture" & date_of(s$match_date) > snap, , drop = FALSE]
    if (nrow(s) == 0) next
    out[[length(out) + 1]] <- as_lock_rows(s, paste0(snap, "T00:00:00Z"), "history-seed")
  }
  if (length(out) == 0) return(NULL)
  all <- bind_rows(out)
  all[!duplicated(all$match_id, fromLast = TRUE), ]
}

main <- function(args) {
  stopifnot(length(args) >= 3)
  current <- read_parquet(args[1])
  cat("predictions:", nrow(current), "rows;", sum(current$status == "fixture"), "fixtures\n")
  prev <- if (args[2] != "NONE" && file.exists(args[2])) read_parquet(args[2]) else NULL
  cat("previous locked rows:", if (is.null(prev)) 0 else nrow(prev), "\n")
  if (length(args) >= 4 && dir.exists(args[4])) {
    seeds <- seed_from_history(args[4])
    if (!is.null(seeds)) {
      add <- seeds[!seeds$match_id %in% if (is.null(prev)) character(0) else prev$match_id, ]
      cat("history seeds added:", nrow(add), "of", nrow(seeds), "\n")
      prev <- bind_rows(if (!is.null(prev)) prev[, LOCK_COLS], add)
    }
  }
  out <- lock_predictions(current, prev)
  # Tripwires: never silently ship an empty or shrunken lock table.
  if (!is.null(prev) && nrow(out) < nrow(prev)) stop("locked table shrank: ", nrow(prev), " -> ", nrow(out))
  if (anyDuplicated(out$match_id)) stop("duplicate match_id in locked table")
  if (nrow(out) == 0) stop("locked table is empty")
  write_parquet(out, args[3])
  cat("locked rows written:", nrow(out), "| played with a lock:",
      sum(out$match_id %in% current$match_id[current$status == "played"]),
      "| of", sum(current$status == "played" & current$season %in% unique(out$season)),
      "played matches in locked seasons\n")
}

if (sys.nframe() == 0 && !interactive()) main(commandArgs(trailingOnly = TRUE))
