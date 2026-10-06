#!/usr/bin/env Rscript
# Score shots with a CONTEXT xG / xGOT model (xG v5, xGOT v3) through panna's own
# code, so the blog's stored xG equals what panna's game logs price shots at.
#
# The context models read pre-shot context from the full event stream (assist,
# possession, rebound, score) and each shooter's earlier foot shots. Rather than
# rebuild that here (xg_features.R is already one hand-kept copy of panna's
# features), this script loads panna's own R files -- the six below hold nothing
# at top level but constants -- and calls add_xg_to_spadl() / add_xgot_to_spadl()
# on the shot table laid out as SPADL shot rows. pannadata's workflows do not
# install panna, so the files come from GitHub at PANNA_REF, or from a local
# checkout when PANNA_R_DIR is set.
#
# Rows scored: those with no stored value (mode "missing", the daily default; D5 in
# pannaverse docs/reference/NET-GOALS-DECISION-POINTS.md), or every row (mode
# "all", the one-time rescore). Shots in a match with a thin or goals-only event
# feed get no value and are flagged thin_feed = TRUE (D11). Scored one
# competition-season at a time, reading that competition's events file only for
# the matches with a shot to score.
#
# Usage:
#   Rscript scripts/score_shots_context.R <shots.parquet> <fixtures.parquet> <events_dir> \
#           <xg_model.rds|-> <xgot_model.rds|-> [missing|all]
#   PANNA_REF (default main) or PANNA_R_DIR=../panna/R
suppressPackageStartupMessages({ library(arrow); library(data.table); library(cli); library(xgboost) })

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 5) stop("usage: score_shots_context.R shots fixtures events_dir xg_model|- xgot_model|- [missing|all]")
shot_path <- args[1]; fx_path <- args[2]; ev_dir <- args[3]
xg_path <- if (args[4] == "-") NULL else args[4]
xgot_path <- if (args[5] == "-") NULL else args[5]
mode <- if (length(args) >= 6) args[6] else "missing"
stopifnot(mode %in% c("missing", "all"), file.exists(shot_path), file.exists(fx_path), dir.exists(ev_dir))

# ---- panna's code --------------------------------------------------------------
PANNA_FILES <- c("constants.R", "utils.R", "spadl_conversion.R", "xg_model.R", "xgot_model.R", "shot_context.R")
panna_dir <- Sys.getenv("PANNA_R_DIR")
if (!nzchar(panna_dir)) {
  ref <- Sys.getenv("PANNA_REF", "main")
  panna_dir <- file.path(tempdir(), "panna-R"); dir.create(panna_dir, showWarnings = FALSE)
  for (f in PANNA_FILES) {
    out <- file.path(panna_dir, f)
    st <- system2("gh", c("api", "-H", shQuote("Accept: application/vnd.github.raw"),
                          sprintf("repos/peteowen1/panna/contents/R/%s?ref=%s", f, ref)),
                  stdout = out, stderr = FALSE)
    if (st != 0 || !file.exists(out) || file.size(out) < 100) stop("could not fetch panna R/", f, " at ", ref)
  }
  cat("panna code: R/{", paste(PANNA_FILES, collapse = ", "), "} at", ref, "\n")
} else cat("panna code: local", panna_dir, "\n")
for (f in PANNA_FILES) sys.source(file.path(panna_dir, f), envir = globalenv())

load_model <- function(p) if (is.null(p)) NULL else readRDS(p)
xg_model <- load_model(xg_path); xgot_model <- load_model(xgot_path)
for (m in Filter(Negate(is.null), list(xg_model, xgot_model)))
  if (!.needs_shot_context(m)) stop("this script is for context models; score older models with enrich_shots_xg.R / enrich_shots_xgot.R")

# ---- shots -------------------------------------------------------------------------
shots <- as.data.table(read_parquet(shot_path))
cat("shots:", nrow(shots), "rows\n")
if (!"xg" %in% names(shots)) shots[, xg := NA_real_]
if (!"xgot" %in% names(shots)) shots[, xgot := NA_real_]
# thin_feed: TRUE = the shot's match has a goals-only or thin event feed (below), so it
# carries no xG and no xGOT on target (off target stays 0); FALSE = a full feed;
# NA = not yet looked at.
if (!"thin_feed" %in% names(shots)) shots[, thin_feed := NA]
on_target <- shots$type_id %in% c(15L, 16L)
# Own goals keep no xG or xGOT by design (enrich_shots_xg.R's own-goal guard), so
# they are never "to score": otherwise every daily run would retry them, and the
# failure guard below would count their blanks as failures.
og <- shots$is_own_goal %in% TRUE
# A shot already found to be in a thin feed stays blank, so the daily run does not
# re-read that match's events every day to find it out again. "all" looks again.
known_thin <- mode == "missing" & shots$thin_feed %in% TRUE
todo_xg   <- if (is.null(xg_model)) rep(FALSE, nrow(shots)) else !og & !known_thin & (mode == "all" | is.na(shots$xg))
# xGOT is only ever set on on-target shots with goal-mouth coordinates; off-target is 0
has_gm    <- !is.na(shots$goalmouth_y) & !is.na(shots$goalmouth_z)
todo_xgot <- if (is.null(xgot_model)) rep(FALSE, nrow(shots)) else !og & !known_thin & on_target & has_gm & (mode == "all" | is.na(shots$xgot))
# Direct corners (panna#277) scored before the fixed values existed still carry the
# model's ~0.85. Their value is a constant, so set it on the stored rows directly
# rather than re-scoring them: no model or events needed, and a row that holds the
# constant is never picked again. Box rule only, since the shot table has no
# qualifiers; new shots get panna's full rule (q263 too) when they are scored.
# Blocked shots are left alone: panna gives them no on-target xGOT.
write_shots <- function(shots) {
  tmp <- paste0(shot_path, ".tmp"); write_parquet(shots, tmp)
  if (file.exists(shot_path)) invisible(file.remove(shot_path))
  invisible(file.rename(tmp, shot_path))
  cat("Written:", shot_path, "(", round(file.size(shot_path) / 1e6, 1), "MB)\n")
}
dc <- !og & !known_thin & .is_direct_corner(shots$x, shots$y, shots$situation)
fix_xg   <- if (is.null(xg_model)) FALSE else dc & !is.na(shots$xg) & round(shots$xg, 3) != DIRECT_CORNER_XG
fix_xgot <- if (is.null(xgot_model)) FALSE else dc & on_target & has_gm & !(if ("is_blocked" %in% names(shots)) shots$is_blocked %in% TRUE else FALSE) &
  !is.na(shots$xgot) & round(shots$xgot, 3) != DIRECT_CORNER_XGOT
shots[fix_xg, xg := DIRECT_CORNER_XG]
shots[fix_xgot, xgot := DIRECT_CORNER_XGOT]
n_fixed <- sum(fix_xg) + sum(fix_xgot)
cat("direct corners set to the fixed value: xG", sum(fix_xg), "| xGOT", sum(fix_xgot), "\n")
shots[, `:=`(.row = .I, .todo = todo_xg | todo_xgot)]
cat("to score: xG", sum(todo_xg), "| xGOT", sum(todo_xgot), "\n")
if (!any(shots$.todo)) {
  cat("nothing to score\n")
  if (n_fixed > 0) { shots[, c(".row", ".todo") := NULL]; write_shots(shots) }
  quit(status = 0)
}

# weak-foot history: every shooter's earlier foot shots, from the whole shot file
fx <- unique(as.data.table(read_parquet(fx_path, col_select = c("match_id", "match_date"))), by = "match_id")
fh <- .shot_foot_history(merge(shots[, .(player_id, match_id, body_part, is_own_goal)], fx, by = "match_id"))

# ---- score, one competition-season at a time -----------------------------------------
MIN_PASSES <- 200L   # a full event feed: goals-only feeds have 0-9 passes, full matches 680+ (1st percentile)
EV_COLS <- c("match_id", "event_id", "type_id", "team_id", "period_id", "minute", "second",
             "outcome", "x", "y", "qualifier_json")
groups <- unique(shots[.todo == TRUE, .(competition, season)])
new_xg <- rep(NA_real_, nrow(shots)); new_xgot <- rep(NA_real_, nrow(shots))
failed <- character(0); no_ctx_rows <- integer(0); thin_rows <- integer(0); full_rows <- integer(0)
for (i in seq_len(nrow(groups))) {
  comp <- groups$competition[i]; ssn <- groups$season[i]
  # only matches with a row to score: a daily run reads and scores the new
  # matches, not the whole season (each match's context needs only its own events)
  todo_mids <- unique(shots[competition == comp & season == ssn & .todo == TRUE, match_id])
  g <- shots[competition == comp & season == ssn & match_id %in% todo_mids]
  label <- paste(comp, ssn)
  res <- tryCatch({
    # A context model never scores a shot without its context, nor a shot from a
    # goals-only or thin feed (below). v5 priced such shots near 1 (69,599 with no
    # context in its training features: 6,682 goals, 65,931 v5 xG); v5.1 never
    # trains on them. Shots without context keep what is stored (or stay blank);
    # thin-feed shots are blanked. Neither counts as a failure, nor does a
    # competition with no events file.
    ev_path <- file.path(ev_dir, paste0("events_", comp, ".parquet"))
    has_ctx <- rep(FALSE, nrow(g))
    if (file.exists(ev_path)) {
      mids <- unique(g$match_id)
      ev <- as.data.table(open_dataset(ev_path) |> dplyr::filter(match_id %in% mids) |>
                            dplyr::select(dplyr::all_of(EV_COLS)) |> dplyr::collect())
      cx <- .shot_context(ev)
      has_ctx <- paste(g$match_id, as.character(g$event_id)) %in% paste(cx$match_id, cx$event_id)
      # A goals-only or thin feed (fewer than MIN_PASSES passes in the match) is
      # never scored: its "shots" are nearly all goals, and the models train
      # only on full feeds (panna xgv_12_feed_passes.R, Pete 2026-09-28).
      # Their stored values are blanked, not kept (below): a goals-only feed's
      # "shots" are its goals, so any xG there only inflates team xG.
      feed <- ev[, .(passes = sum(type_id == 1L)), by = match_id]
      thin <- feed[passes < MIN_PASSES, match_id]
      has_ctx <- has_ctx & !(g$match_id %in% thin)
      thin_rows <- c(thin_rows, g$.row[g$match_id %in% thin])
      full_rows <- c(full_rows, g$.row[g$match_id %in% feed[passes >= MIN_PASSES, match_id]])
    }
    no_ctx_rows <- c(no_ctx_rows, g$.row[!has_ctx])
    g <- g[has_ctx]
    if (!nrow(g)) list(xg = NULL, xgot = NULL, rows = integer(0)) else {
      # SPADL shot rows as panna's scorers read them: coordinates are the shot
      # table's own (Opta, attacking right), keyed by the Opta event id
      sp <- data.frame(match_id = g$match_id, original_event_id = as.numeric(g$event_id),
                       action_type = "shot", start_x = g$x, start_y = g$y, player_id = g$player_id,
                       is_big_chance = g$big_chance %in% TRUE,
                       is_penalty = tolower(g$situation) %in% "penalty",
                       is_own_goal = g$is_own_goal, stringsAsFactors = FALSE)
      lk <- as.data.frame(g[, intersect(c("match_id", "event_id", "type_id", "body_part", "situation",
                                          "goalmouth_y", "goalmouth_z", "is_blocked"), names(g)), with = FALSE])
      lk$event_id <- as.numeric(lk$event_id)   # integer64 in the shot file; SPADL's key is plain numeric
      out <- list(xg = NULL, xgot = NULL, rows = g$.row)
      if (any(todo_xg[g$.row]))
        out$xg <- suppressMessages(add_xg_to_spadl(sp, xg_model, season = ssn, shot_lookup = lk,
                                                   events = ev, foot_history = fh))$xg
      if (any(todo_xgot[g$.row]))
        out$xgot <- suppressMessages(add_xgot_to_spadl(sp, xgot_model, lk, season = ssn,
                                                       events = ev, foot_history = fh))$xgot
      out
    }
  }, error = function(e) { message("  FAILED ", label, ": ", conditionMessage(e)); NULL })
  if (is.null(res)) { failed <- c(failed, label); next }
  if (!is.null(res$xg)) new_xg[res$rows] <- res$xg
  if (!is.null(res$xgot)) new_xgot[res$rows] <- res$xgot
  if (i %% 25 == 0) cat("  scored", i, "of", nrow(groups), "competition-seasons\n")
}

# ---- write back, only the rows this run was asked to score ---------------------------
if (mode == "all") shots[og, `:=`(xg = NA_real_, xgot = NA_real_)]   # own goals: no xG / xGOT
w_xg <- todo_xg & !is.na(new_xg); w_xgot <- todo_xgot & !is.na(new_xgot)
shots[w_xg, xg := round(new_xg[w_xg], 3)]
shots[w_xgot, xgot := round(new_xgot[w_xgot], 3)]
# Thin feeds: no xG, and no xGOT on target (off target stays 0 below, which is true
# whatever the feed). A match is either thin or full, so the two sets never overlap.
shots[full_rows, thin_feed := FALSE]
shots[thin_rows, `:=`(xg = NA_real_, thin_feed = TRUE)]
shots[intersect(thin_rows, which(on_target)), xgot := NA_real_]
if (!is.null(xgot_model)) shots[!on_target & (mode == "all" | is.na(xgot)), xgot := 0]   # off target: cannot score
cat("wrote xG on", sum(w_xg), "of", sum(todo_xg), "| xGOT on", sum(w_xgot), "of", sum(todo_xgot), "\n")
cat("thin or goals-only feeds (fewer than", MIN_PASSES, "passes): blanked", length(unique(thin_rows)), "rows in",
    uniqueN(shots$match_id[thin_rows]), "matches\n")
if (length(failed)) cat("::warning::", length(failed), "competition-season(s) not scored:", paste(head(failed, 20), collapse = "; "), "\n")
# Shots with no pre-shot context are reported, not failures: their match has no
# event feed (older seasons, no events file), and they keep their stored value.
# Thin-feed shots are in this set too (blanked above), so they are left out of the count.
skip <- seq_len(nrow(shots)) %in% no_ctx_rows
thin <- seq_len(nrow(shots)) %in% thin_rows
cat("no pre-shot context (left as stored):", sum(skip & !thin & todo_xg), "xG rows,", sum(skip & !thin & todo_xgot), "xGOT rows\n")
n_scorable <- sum(todo_xg & !skip) + sum(todo_xgot & !skip)
share_failed <- if (n_scorable == 0) 0 else 1 - (sum(w_xg) + sum(w_xgot)) / n_scorable   # all skipped is not a failure
if (share_failed > 0.05) stop(sprintf("%.1f%% of the shots to score got no value: refusing to write", 100 * share_failed))

shots[, c(".row", ".todo") := NULL]
write_shots(shots)
