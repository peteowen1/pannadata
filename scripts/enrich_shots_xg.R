#!/usr/bin/env Rscript
# Enrich opta_shot_events.parquet with xG predictions from pre-trained model.
# Run after Opta scrape, before uploading to opta-latest release.
# Usage: Rscript scripts/enrich_shots_xg.R [shot_file] [model_file]
# Defaults: data/opta/opta_shot_events.parquet, source/xg_model.rds
# Overwrites the input file with xG column added.

library(arrow)
library(dplyr)
library(xgboost)

args <- commandArgs(trailingOnly = TRUE)
shot_path  <- if (length(args) >= 1) args[1] else "data/opta/opta_shot_events.parquet"
model_path <- if (length(args) >= 2) args[2] else "source/xg_model.rds"

if (!file.exists(shot_path)) stop("Shot file not found: ", shot_path)
if (!file.exists(model_path)) stop("xG model not found: ", model_path)

cat("=== Enrich shots with xG ===\n")
shots <- read_parquet(shot_path)
cat("Shots:", nrow(shots), "rows\n")

xg_model <- readRDS(model_path)
feature_cols <- xg_model$panna_metadata$feature_cols
cat("Model features:", length(feature_cols), "\n")

# Build features (replicates panna::.create_shot_features)
distance_to_goal <- sqrt((100 - shots$x)^2 + (50 - shots$y)^2)
dist_to_goal_line <- pmax(100 - shots$x, 0.1)
angle_left  <- atan2(50 - 6 - shots$y, dist_to_goal_line)
angle_right <- atan2(50 + 6 - shots$y, dist_to_goal_line)

bp <- tolower(shots$body_part)
si <- tolower(shots$situation)

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
  is_big_chance      = as.integer(coalesce(if ("big_chance" %in% names(shots)) shots$big_chance else 0L, 0L))
)

# Fill any missing model features with 0
for (col in setdiff(feature_cols, names(features))) features[[col]] <- 0

X <- as.matrix(features[, feature_cols, drop = FALSE])
X[is.na(X)] <- 0

pred_xg <- round(predict(xg_model$model, X), 3)

# Preserve existing non-NA xG values; only fill NaN/NA rows with model
# predictions. Today the model is the only xG source, so this is a no-op —
# but if upstream ever provides real Opta xG we don't want to overwrite it
# with a weaker signal. Also makes re-runs idempotent on already-enriched
# rows, since build-blog-data.yml now invokes this script defensively.
if ("xg" %in% names(shots)) {
  n_filled <- sum(is.na(shots$xg))
  shots$xg <- ifelse(is.na(shots$xg), pred_xg, shots$xg)
  cat("Filled xG on", n_filled, "NaN rows (preserved", nrow(shots) - n_filled, "existing)\n")
} else {
  shots$xg <- pred_xg
  cat("Added xG to all", nrow(shots), "rows\n")
}

# Own-goal guard. Opta logs an own goal as a goal (type_id == 16) at the
# scoring player's location -- which is near their OWN goal, so the
# attacking-right x-coordinate lands in their own half (x < 50). The xG model
# reads that as a point-blank chance and returns ~0.97, which is meaningless:
# an own goal has no shot xG by construction. Left in, these ~0.97 values are
# the single largest xG entries (confirmed: every top-20 xG shot is an OG at
# x = 0.6-7.4) and they pollute the blog shot map + inflate team/league xG
# totals. Set them to NA (surface, never impute a fabricated number) so
# downstream sum(xg, na.rm = TRUE) ignores them and the shot map can show
# "OG -- no xG" rather than a fake 0.97. Mirrors panna's "own goals use EPV
# not xG" convention (panna/CLAUDE.md).
if ("type_id" %in% names(shots)) {
  is_own_goal <- shots$type_id == 16L & !is.na(shots$x) & shots$x < 50
  n_og <- sum(is_own_goal, na.rm = TRUE)
  if (n_og > 0L) {
    shots$xg[is_own_goal] <- NA_real_
    cat("Own-goal guard: set xG = NA on", n_og,
        "own-goal shot(s) (type_id == 16, x < 50)\n")
  }
} else {
  cat("::warning:: no type_id column -- skipping own-goal xG guard\n")
}

# Penalty override. panna's xG model is deliberately penalty-free (penalties
# carry no shot-geometry signal), so a spot kick scores ~0.33 geometric xG --
# meaningless. The EPV pipeline overrides penalties to a fixed conversion rate;
# replicate that here so the blog shot map and team/league xG totals use the
# right value. 0.80 == panna::PENALTY_XG (panna constants.R:349 / xg_model.R:475).
if ("situation" %in% names(shots)) {
  is_pen <- !is.na(shots$situation) & tolower(shots$situation) == "penalty"
  n_pen <- sum(is_pen)
  shots$xg[is_pen] <- 0.80  # == panna::PENALTY_XG
  cat("Penalty override: set xG = 0.80 on", n_pen, "penalty shot(s)\n")
} else {
  cat("::warning:: no situation column -- skipping penalty xG override\n")
}

cat("xG: mean =", round(mean(shots$xg, na.rm = TRUE), 4),
    ", total =", round(sum(shots$xg, na.rm = TRUE), 1),
    ", goals =", sum(shots$is_goal, na.rm = TRUE), "\n")

write_parquet(shots, shot_path)
cat("Written:", shot_path, "(", round(file.size(shot_path) / 1e6, 1), "MB)\n")
