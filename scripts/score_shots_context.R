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
# "all", the one-time rescore). Scored one competition-season at a time, reading
# that competition's events file only for the matches that need them.
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
on_target <- shots$type_id %in% c(15L, 16L)
# Own goals keep no xG or xGOT by design (enrich_shots_xg.R's own-goal guard), so
# they are never "to score": otherwise every daily run would retry them, and the
# failure guard below would count their blanks as failures.
og <- shots$is_own_goal %in% TRUE
todo_xg   <- if (is.null(xg_model)) rep(FALSE, nrow(shots)) else !og & (mode == "all" | is.na(shots$xg))
# xGOT is only ever set on on-target shots with goal-mouth coordinates; off-target is 0
has_gm    <- !is.na(shots$goalmouth_y) & !is.na(shots$goalmouth_z)
todo_xgot <- if (is.null(xgot_model)) rep(FALSE, nrow(shots)) else !og & on_target & has_gm & (mode == "all" | is.na(shots$xgot))
shots[, `:=`(.row = .I, .todo = todo_xg | todo_xgot)]
cat("to score: xG", sum(todo_xg), "| xGOT", sum(todo_xgot), "\n")
if (!any(shots$.todo)) { cat("nothing to score\n"); quit(status = 0) }

# weak-foot history: every shooter's earlier foot shots, from the whole shot file
fx <- unique(as.data.table(read_parquet(fx_path, col_select = c("match_id", "match_date"))), by = "match_id")
fh <- .shot_foot_history(merge(shots[, .(player_id, match_id, body_part, is_own_goal)], fx, by = "match_id"))

# ---- score, one competition-season at a time -----------------------------------------
EV_COLS <- c("match_id", "event_id", "type_id", "team_id", "period_id", "minute", "second",
             "outcome", "x", "y", "qualifier_json")
groups <- unique(shots[.todo == TRUE, .(competition, season)])
new_xg <- rep(NA_real_, nrow(shots)); new_xgot <- rep(NA_real_, nrow(shots))
failed <- character(0)
for (i in seq_len(nrow(groups))) {
  comp <- groups$competition[i]; ssn <- groups$season[i]
  g <- shots[competition == comp & season == ssn]
  label <- paste(comp, ssn)
  res <- tryCatch({
    ev_path <- file.path(ev_dir, paste0("events_", comp, ".parquet"))
    if (!file.exists(ev_path)) stop("no events file ", basename(ev_path))
    mids <- unique(g$match_id)
    ev <- as.data.table(open_dataset(ev_path) |> dplyr::filter(match_id %in% mids) |>
                          dplyr::select(dplyr::all_of(EV_COLS)) |> dplyr::collect())
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
    out <- list(xg = NULL, xgot = NULL)
    if (any(todo_xg[g$.row]))
      out$xg <- suppressMessages(add_xg_to_spadl(sp, xg_model, season = ssn, shot_lookup = lk,
                                                 events = ev, foot_history = fh))$xg
    if (any(todo_xgot[g$.row]))
      out$xgot <- suppressMessages(add_xgot_to_spadl(sp, xgot_model, lk, season = ssn,
                                                     events = ev, foot_history = fh))$xgot
    out
  }, error = function(e) { message("  FAILED ", label, ": ", conditionMessage(e)); NULL })
  if (is.null(res)) { failed <- c(failed, label); next }
  if (!is.null(res$xg)) new_xg[g$.row] <- res$xg
  if (!is.null(res$xgot)) new_xgot[g$.row] <- res$xgot
  if (i %% 25 == 0) cat("  scored", i, "of", nrow(groups), "competition-seasons\n")
}

# ---- write back, only the rows this run was asked to score ---------------------------
if (mode == "all") shots[og, `:=`(xg = NA_real_, xgot = NA_real_)]   # own goals: no xG / xGOT
w_xg <- todo_xg & !is.na(new_xg); w_xgot <- todo_xgot & !is.na(new_xgot)
shots[w_xg, xg := round(new_xg[w_xg], 3)]
shots[w_xgot, xgot := round(new_xgot[w_xgot], 3)]
if (!is.null(xgot_model)) shots[!on_target & (mode == "all" | is.na(xgot)), xgot := 0]   # off target: cannot score
cat("wrote xG on", sum(w_xg), "of", sum(todo_xg), "| xGOT on", sum(w_xgot), "of", sum(todo_xgot), "\n")
if (length(failed)) cat("::warning::", length(failed), "competition-season(s) not scored:", paste(head(failed, 20), collapse = "; "), "\n")
share_failed <- 1 - (sum(w_xg) + sum(w_xgot)) / max(1, sum(todo_xg) + sum(todo_xgot))
if (share_failed > 0.05) stop(sprintf("%.1f%% of the shots to score got no value: refusing to write", 100 * share_failed))

shots[, c(".row", ".todo") := NULL]
tmp <- paste0(shot_path, ".tmp"); write_parquet(shots, tmp)
if (file.exists(shot_path)) invisible(file.remove(shot_path))
invisible(file.rename(tmp, shot_path))
cat("Written:", shot_path, "(", round(file.size(shot_path) / 1e6, 1), "MB)\n")
