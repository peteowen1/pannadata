#!/usr/bin/env Rscript
# Weak-foot lookup for live xG v5 / xGOT v3 scoring (the worker's foot_share
# input): each player's foot shots so far and how many were right-footed.
#
# Mirrors panna .shot_foot_history(): foot shots only (RightFoot / LeftFoot),
# own goals excluded, and only matches dated BEFORE today (UTC), so a match
# played today scores with the history it had at kick-off, as in training.
# Built daily, so a match re-scored on a later day sees its own shots in the
# count too -- a small leak for post-match views, recorded as a decision point
# (pannaverse docs/reference/NET-GOALS-DECISION-POINTS.md, D7).
#
# Only players with at least SHOT_FOOT_MIN (10) earlier foot shots are written:
# below that the model reads the input as missing anyway (panna .foot_share()),
# so leaving them out gives the same score in a much smaller file.
#
# Usage: Rscript scripts/build_shot_foot_history.R [shots.parquet] [fixtures.parquet] [out.json]
suppressMessages({ library(arrow); library(data.table); library(jsonlite) })

args <- commandArgs(trailingOnly = TRUE)
shots_path <- if (length(args) >= 1) args[1] else "source/opta_shot_events.parquet"
fx_path    <- if (length(args) >= 2) args[2] else "source/opta_fixtures.parquet"
out_path   <- if (length(args) >= 3) args[3] else "blog/shot-foot-history.json"
SHOT_FOOT_MIN <- 10L   # panna R/shot_context.R

s <- as.data.table(read_parquet(shots_path, col_select = c("player_id", "match_id", "body_part", "is_own_goal")))
fx <- unique(as.data.table(read_parquet(fx_path, col_select = c("match_id", "match_date"))), by = "match_id")
cat("shots:", nrow(s), "| fixtures:", nrow(fx), "\n")
s <- merge(s, fx, by = "match_id")
today <- as.Date(format(Sys.time(), tz = "UTC", "%Y-%m-%d"))
s <- s[!(is_own_goal %in% TRUE) & !is.na(player_id) & body_part %in% c("RightFoot", "LeftFoot") &
         !is.na(match_date) & as.Date(match_date) < today]
agg <- s[, .(r = sum(body_part == "RightFoot"), n = .N), by = player_id][n >= SHOT_FOOT_MIN]
cat("foot shots before", format(today), ":", nrow(s), "| players with >=", SHOT_FOOT_MIN, ":", nrow(agg), "\n")
if (nrow(agg) < 1000) stop("only ", nrow(agg), " players with foot history -- shot or fixture file looks short; refusing to publish")

players <- setNames(lapply(seq_len(nrow(agg)), function(i) c(agg$r[i], agg$n[i])), agg$player_id)
dir.create(dirname(out_path), showWarnings = FALSE, recursive = TRUE)
write_json(list(built_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
                through_date = format(today - 1), min_shots = SHOT_FOOT_MIN,
                n_players = nrow(agg), players = players),
           out_path, auto_unbox = TRUE)
cat("wrote", out_path, round(file.size(out_path) / 1e6, 2), "MB\n")
