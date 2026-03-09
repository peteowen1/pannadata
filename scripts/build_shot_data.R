# build_shot_data.R — Extract recent shot data from Opta for shot chart feature.
# Can be run standalone (Rscript scripts/build_shot_data.R) or source()'d from
# build_blog_data.R (where tryCatch prevents shot failures from blocking ratings).

library(arrow)
library(dplyr)

opta_path <- "source/opta_shot_events.parquet"
if (!file.exists(opta_path)) stop("opta_shot_events.parquet not found in source/")

opta_shots <- read_parquet(opta_path)

tracked_leagues <- c("EPL", "La_Liga", "Serie_A", "Bundesliga", "Ligue_1", "UCL", "UEL")
recent_seasons <- sort(unique(opta_shots$season[opta_shots$competition %in% tracked_leagues]),
                       decreasing = TRUE)[1:5]

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
    season
  )

stopifnot(nrow(panna_shots) > 0, length(recent_seasons) > 0)

dir.create("blog", showWarnings = FALSE)
write_parquet(panna_shots, "blog/panna_shots.parquet")
cat("panna_shots:", nrow(panna_shots), "shots across", length(recent_seasons), "seasons\n")
