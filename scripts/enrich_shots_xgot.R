#!/usr/bin/env Rscript
# Enrich opta_shot_events.parquet with xGOT (post-shot xG) for on-target shots.
# Run AFTER enrich_shots_xg.R and AFTER goalmouth_y/z are present (scraper +
# backfill_goalmouth.py). Companion to enrich_shots_xg.R.
# Usage: Rscript scripts/enrich_shots_xgot.R [shot_file] [model_file]
# Defaults: data/opta/opta_shot_events.parquet, source/xgot_model.rds
#
# xGOT is defined only for ON-TARGET shots (type 15 saved, 16 goal):
#   on-target + goalmouth coords -> model prediction
#   off-target (13 miss, 14 post) -> 0  (cannot score)
#   on-target without coords      -> NA (surfaced, never imputed to 0)
# Skips gracefully (exit 0) if the model or goalmouth columns aren't present
# yet, so it never blocks the daily build.

library(arrow)
library(dplyr)
library(xgboost)

args <- commandArgs(trailingOnly = TRUE)
shot_path  <- if (length(args) >= 1) args[1] else "data/opta/opta_shot_events.parquet"
model_path <- if (length(args) >= 2) args[2] else "source/xgot_model.rds"

if (!file.exists(shot_path)) stop("Shot file not found: ", shot_path)
if (!file.exists(model_path)) {
  cat("::notice:: xGOT model not found (", model_path, ") -- skipping xGOT enrichment\n")
  quit(status = 0)
}

cat("=== Enrich shots with xGOT ===\n")
shots <- read_parquet(shot_path)
cat("Shots:", nrow(shots), "rows\n")

if (!all(c("goalmouth_y", "goalmouth_z") %in% names(shots))) {
  cat("::notice:: goalmouth_y/z not in shot file -- run backfill_goalmouth.py first; skipping\n")
  quit(status = 0)
}

xgot_model <- readRDS(model_path)
feature_cols <- xgot_model$panna_metadata$feature_cols
cat("Model features:", length(feature_cols), "(",
    length(xgot_model$panna_metadata$placement_cols), "placement )\n")

# Base (pre-shot) features -- identical to enrich_shots_xg.R / .create_shot_features
distance_to_goal <- sqrt((100 - shots$x)^2 + (50 - shots$y)^2)
dist_to_goal_line <- pmax(100 - shots$x, 0.1)
angle_left  <- atan2(50 - 6 - shots$y, dist_to_goal_line)
angle_right <- atan2(50 + 6 - shots$y, dist_to_goal_line)
bp <- tolower(shots$body_part)
si <- tolower(shots$situation)

# Placement features -- keep in sync with panna::.create_placement_features
POST_L <- 45.2; POST_R <- 54.8; BAR <- 38
dist_to_near_post  <- pmin(abs(shots$goalmouth_y - POST_L), abs(shots$goalmouth_y - POST_R))
dist_to_top_corner <- sqrt(dist_to_near_post^2 + (BAR - shots$goalmouth_z)^2)

features <- data.frame(
  x                  = shots$x,
  y                  = shots$y,
  distance_to_goal   = distance_to_goal,
  angle_to_goal      = abs(angle_right - angle_left),
  in_penalty_area    = as.integer(shots$x > 83 & shots$y > 21 & shots$y < 79),
  in_six_yard_box    = as.integer(shots$x > 94 & shots$y > 37 & shots$y < 63),
  is_header          = as.integer(grepl("head", bp)),
  is_right_foot      = as.integer(grepl("right", bp)),
  is_left_foot       = as.integer(grepl("left", bp)),
  is_open_play       = as.integer(grepl("open", si)),
  is_set_piece       = as.integer(grepl("set", si)),
  is_corner          = as.integer(grepl("corner", si)),
  is_direct_freekick = as.integer(grepl("free", si)),
  is_big_chance      = as.integer(coalesce(if ("big_chance" %in% names(shots)) shots$big_chance else 0L, 0L)),
  gm_y               = shots$goalmouth_y,
  gm_z               = shots$goalmouth_z,
  dist_to_near_post  = dist_to_near_post,
  dist_to_top_corner = dist_to_top_corner
)

missing_feat <- setdiff(feature_cols, names(features))
if (length(missing_feat) > 0) {
  cat("::warning:: model expects features not built here:", paste(missing_feat, collapse=", "), "\n")
  for (col in missing_feat) features[[col]] <- 0
}

on_target <- shots$type_id %in% c(15L, 16L)
has_gm    <- !is.na(shots$goalmouth_y) & !is.na(shots$goalmouth_z)
predable  <- on_target & has_gm

xgot <- rep(NA_real_, nrow(shots))
xgot[!on_target] <- 0                       # off-target: cannot score
if (any(predable)) {
  X <- as.matrix(features[predable, feature_cols, drop = FALSE])
  xgot[predable] <- round(predict(xgot_model$model, X), 3)
}
shots$xgot <- xgot
cat("xGOT: predicted", sum(predable), "on-target shots;",
    sum(on_target & !has_gm), "on-target missing coords -> NA;",
    sum(!on_target), "off-target -> 0\n")

# Own-goal guard (mirror enrich_shots_xg.R): an own goal is type 16 at x < 50;
# its placement is meaningless for xGOT. NA, never a fabricated value.
if ("type_id" %in% names(shots)) {
  is_own_goal <- shots$type_id == 16L & !is.na(shots$x) & shots$x < 50
  n_og <- sum(is_own_goal, na.rm = TRUE)
  if (n_og > 0L) {
    shots$xgot[is_own_goal] <- NA_real_
    cat("Own-goal guard: set xGOT = NA on", n_og, "own-goal shot(s)\n")
  }
}

cat("xGOT: mean (on-target) =",
    round(mean(shots$xgot[on_target], na.rm = TRUE), 4),
    ", sum =", round(sum(shots$xgot, na.rm = TRUE), 1), "\n")

write_parquet(shots, shot_path)
cat("Written:", shot_path, "(", round(file.size(shot_path) / 1e6, 1), "MB)\n")
