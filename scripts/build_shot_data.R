# build_shot_data.R — Extract recent shot data from Opta for shot chart feature.
# Run standalone (Rscript scripts/build_shot_data.R) or source()'d from
# build_blog_data.R. Must be run from the pannadata repo root.
# In GHA, build_shot_data.R runs as a separate workflow step.

library(arrow)
library(dplyr)

opta_path <- "source/opta_shot_events.parquet"
if (!file.exists(opta_path)) stop("opta_shot_events.parquet not found in source/")

opta_shots <- read_parquet(opta_path)

required_cols <- c("season", "competition", "player_name", "x", "y",
                   "is_goal", "type_id", "body_part", "situation")
missing <- setdiff(required_cols, names(opta_shots))
if (length(missing) > 0) {
  stop("opta_shot_events.parquet missing columns: ", paste(missing, collapse = ", "),
       ". Check the Opta scraper output schema.")
}

tracked_leagues <- c("EPL", "La_Liga", "Serie_A", "Bundesliga", "Ligue_1", "UCL", "UEL")
recent_seasons <- head(
  sort(unique(opta_shots$season[opta_shots$competition %in% tracked_leagues]),
       decreasing = TRUE), 5
)

panna_shots <- opta_shots |>
  filter(competition %in% tracked_leagues, season %in% recent_seasons) |>
  transmute(
    player_name,
    x = round(x, 1),
    y = round(y, 1),
    is_goal,
    type_id = as.integer(type_id),
    body_part,
    situation,
    big_chance = if ("big_chance" %in% names(opta_shots)) as.integer(big_chance) else 0L,
    season
  )

stopifnot(nrow(panna_shots) > 0, length(recent_seasons) > 0)

# ── xG prediction using pre-trained XGBoost model ──
xg_model_path <- "source/xg_model.rds"
if (file.exists(xg_model_path)) {
  library(xgboost)

  xg_model <- readRDS(xg_model_path)
  cat("Loaded xG model:", length(xg_model$panna_metadata$feature_cols), "features\n")

  # Replicate panna::.create_shot_features() inline
  distance_to_goal <- sqrt((100 - panna_shots$x)^2 + (50 - panna_shots$y)^2)
  dist_to_goal_line <- pmax(100 - panna_shots$x, 0.1)
  goal_half_w <- 6  # goal_width=12 / 2
  angle_left  <- atan2(50 - goal_half_w - panna_shots$y, dist_to_goal_line)
  angle_right <- atan2(50 + goal_half_w - panna_shots$y, dist_to_goal_line)
  angle_to_goal <- abs(angle_right - angle_left)

  bp_lower <- tolower(panna_shots$body_part)
  sit_lower <- tolower(panna_shots$situation)

  features <- data.frame(
    x                 = panna_shots$x,
    y                 = panna_shots$y,
    distance_to_goal  = distance_to_goal,
    angle_to_goal     = angle_to_goal,
    in_penalty_area   = as.integer(panna_shots$x > 83 & panna_shots$y > 21 & panna_shots$y < 79),
    in_six_yard_box   = as.integer(panna_shots$x > 94 & panna_shots$y > 37 & panna_shots$y < 63),
    is_header         = as.integer(grepl("head", bp_lower)),
    is_right_foot     = as.integer(grepl("right", bp_lower)),
    is_left_foot      = as.integer(grepl("left", bp_lower)),
    is_open_play      = as.integer(grepl("open", sit_lower)),
    is_set_piece      = as.integer(grepl("set", sit_lower)),
    is_corner         = as.integer(grepl("corner", sit_lower)),
    is_direct_freekick = as.integer(grepl("free", sit_lower)),
    is_big_chance     = panna_shots$big_chance
  )

  # Fill any missing model features with 0
  feature_cols <- xg_model$panna_metadata$feature_cols
  for (col in setdiff(feature_cols, names(features))) {
    features[[col]] <- 0
  }

  X <- as.matrix(features[, feature_cols, drop = FALSE])
  X[is.na(X)] <- 0

  panna_shots$xg <- round(predict(xg_model$model, X), 3)
  cat("xG predicted:", sum(panna_shots$xg, na.rm = TRUE), "total xG across",
      nrow(panna_shots), "shots\n")
} else {
  warning("xg_model.rds not found in source/ — shots will not include xG")
}

# Drop big_chance before writing (internal feature, not needed in blog)
panna_shots <- panna_shots |> select(-big_chance)

dir.create("blog", showWarnings = FALSE)
write_parquet(panna_shots, "blog/shots.parquet")
cat("shots:", nrow(panna_shots), "shots across", length(recent_seasons), "seasons\n")
