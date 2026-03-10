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
    season
  )

stopifnot(nrow(panna_shots) > 0, length(recent_seasons) > 0)

dir.create("blog", showWarnings = FALSE)
write_parquet(panna_shots, "blog/panna_shots.parquet")
cat("panna_shots:", nrow(panna_shots), "shots across", length(recent_seasons), "seasons\n")
