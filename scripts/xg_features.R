# xG model features for pannadata's shot scripts, in ONE place.
#
# enrich_shots_xg.R, build_shot_data.R and build_match_data_local.R score shots
# with panna's published xg_model.rds, but pannadata's workflows do not install
# panna, so each script rebuilt panna::.create_shot_features() by hand. The three
# copies drifted: only one had the missing-situation fix (pannadata#101), and
# none built `season_num`, which the published model reads. They zero-filled it
# instead, which panna itself calls indefensible (R/xg_model.R, "Ensure all
# required columns exist"): 0 is not a season, so every shot went down the
# model's earliest-era branch. Measured 2026-09-26 on 3,254,124 non-penalty,
# non-own-goal shots: goals/xG 1.135 with season 0 against 0.990 with the real
# season.
#
# Mirrors panna::.create_shot_features() (R/xg_model.R) and
# panna::extract_season_end_year() (R/utils.R). If either changes, change this.
#
# Usage:  source("scripts/xg_features.R")
#         X <- xg_feature_matrix(shots, xg_model)   # stops on any feature it can't build

# Season label -> end year: "2023-2024" -> 2024, "2018 Russia" -> 2018,
# "Intl_Friendlies_2024" -> 2024. Same three formats, same order, as panna.
xg_season_end_year <- function(season) {
  season <- as.character(season)
  out <- rep(NA_real_, length(season))
  usable <- !is.na(season) & nzchar(season)
  fmt1 <- usable & grepl("^[0-9]{4}-[0-9]{4}$", season)
  out[fmt1] <- as.numeric(substr(season[fmt1], 6, 9))
  fmt2 <- usable & !fmt1
  out[fmt2] <- suppressWarnings(as.numeric(sub("^([0-9]{4}).*", "\\1", season[fmt2])))
  fmt3 <- usable & is.na(out)
  out[fmt3] <- vapply(regmatches(season[fmt3], gregexpr("[0-9]{4}", season[fmt3])),
                      function(m) if (length(m)) as.numeric(m[length(m)]) else NA_real_,
                      numeric(1))
  out
}

# Every feature the published xG models can read from a shot row. `shots` needs
# x, y, body_part, situation and season; big_chance (or is_big_chance) is optional.
xg_shot_features <- function(shots) {
  # Clamped to the pitch first, as panna does: Opta sometimes sends x/y outside
  # 0-100, and the model was trained on clamped values (review finding).
  x <- pmin(pmax(shots$x, 0), 100)
  y <- pmin(pmax(shots$y, 0), 100)
  dist_line   <- pmax(100 - x, 0.1)
  angle_left  <- atan2(50 - 6 - y, dist_line)
  angle_right <- atan2(50 + 6 - y, dist_line)
  bp <- tolower(shots$body_part)
  si <- tolower(shots$situation)
  bc <- if ("big_chance" %in% names(shots)) shots$big_chance else
        if ("is_big_chance" %in% names(shots)) shots$is_big_chance else 0L
  bc <- as.integer(bc); bc[is.na(bc)] <- 0L
  data.frame(
    x                  = x,
    y                  = y,
    distance_to_goal   = sqrt((100 - x)^2 + (50 - y)^2),
    angle_to_goal      = abs(angle_right - angle_left),
    in_penalty_area    = as.integer(x > 83 & y > 21 & y < 79),
    in_six_yard_box    = as.integer(x > 94 & y > 37 & y < 63),
    is_header          = as.integer(grepl("head", bp)),
    is_right_foot      = as.integer(grepl("right", bp)),
    is_left_foot       = as.integer(grepl("left", bp)),
    # panna's NULL-situation branch defaults to open play (pannadata#101).
    is_open_play       = as.integer(is.na(si) | si == "" | grepl("open", si)),
    is_set_piece       = as.integer(grepl("set", si)),
    is_corner          = as.integer(grepl("corner", si)),
    is_direct_freekick = as.integer(grepl("free", si)),
    is_big_chance      = bc,
    season_num         = if ("season" %in% names(shots)) xg_season_end_year(shots$season) else NA_real_
  )
}

# The model's input matrix. Stops, never zero-fills, when the model reads a
# feature built here only from the event stream (xG v5's assist, possession and
# weak-foot inputs come from panna's .shot_context()) or when season_num could
# not be parsed for any row.
xg_feature_matrix <- function(shots, xg_model) {
  fc <- xg_model$panna_metadata$feature_cols
  f <- xg_shot_features(shots)
  missing <- setdiff(fc, names(f))
  if (length(missing)) {
    stop("xg_model expects features pannadata cannot build from the shot table: ",
         paste(missing, collapse = ", "),
         " -- score these shots through panna (add_xg_to_spadl() with events) instead.")
  }
  if ("season_num" %in% fc && anyNA(f$season_num)) {
    bad <- unique(shots$season[is.na(f$season_num)])
    stop("season_num could not be parsed for ", sum(is.na(f$season_num)), " shot(s), seasons: ",
         paste(head(bad, 5), collapse = ", "), " -- refusing to score them as season 0.")
  }
  X <- as.matrix(f[, fc, drop = FALSE])
  X[is.na(X)] <- 0   # as the scripts always did; season_num is checked above
  X
}
