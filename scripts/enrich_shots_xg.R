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

shots$xg <- round(predict(xg_model$model, X), 3)

cat("xG: mean =", round(mean(shots$xg), 4),
    ", total =", round(sum(shots$xg), 1),
    ", goals =", sum(shots$is_goal, na.rm = TRUE), "\n")

write_parquet(shots, shot_path)
cat("Written:", shot_path, "(", round(file.size(shot_path) / 1e6, 1), "MB)\n")
