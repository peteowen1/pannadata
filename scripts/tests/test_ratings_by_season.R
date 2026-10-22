#!/usr/bin/env Rscript
# Tests for scripts/ratings_by_season.R (pannadata#135)
#
#   Rscript scripts/tests/test_ratings_by_season.R      (from the repo root)
#
# Sources the shipped function rather than restating it.

suppressMessages({ library(dplyr) })
source("scripts/ratings_by_season.R")

failed <- 0L
chk <- function(label, got, want) {
  if (!identical(got, want)) {
    failed <<- failed + 1L
    cat("FAIL:", label, "— got", format(got), "want", format(want), "\n")
  }
}

xr <- data.frame(
  season_end_year = c(2024, 2024, 2025, 2025, 2025, 2025),
  player_id = c("a", "b", "a", "b", "c", "c"),
  player_name = c("A", "B", "A", "B", "C", "C"),
  total_minutes = c(900, 500, 1000, 800, 100, 300),
  xrapm = c(0.123456, -0.2, 0.3, 0.1, 9, 0.5),
  offense = c(0.1, 0, 0.2, 0.1, 9, 0.3),
  defense = c(0.02, -0.2, 0.1, 0, 0, 0.2)
)
spm <- data.frame(
  season_end_year = c(2024, 2025, 2025),
  player_id = c("a", "a", "b"),
  total_minutes = c(900, 1000, 800),
  spm = c(0.5, 0.6, 0.7)
)
meta <- data.frame(player_id = c("a", "b", "c"), league = c("EPL", "TUN", "EPL"))

out <- build_ratings_by_season(xr, spm, meta, "player_id", exclude_leagues = "TUN")

# every season is present, with the season column
chk("seasons", sort(unique(out$season_end_year)), c(2024, 2025))
# player b is in an excluded league -> dropped in every season, like ratings.parquet
chk("excluded league dropped", "b" %in% out$player_id, FALSE)
# one row per player per season; duplicate c row keeps the highest-minutes one
chk("one row per player-season", nrow(out), 3L)
chk("duplicate keeps max minutes", out$xrapm[out$player_id == "c"], 0.5)
# SPM joins on (player, season), not player alone
chk("spm season-matched 2024", out$spm_overall[out$season_end_year == 2024], 0.5)
chk("spm season-matched 2025", out$spm_overall[out$season_end_year == 2025 & out$player_id == "a"], 0.6)
chk("spm missing -> NA", is.na(out$spm_overall[out$player_id == "c"]), TRUE)
# same 4dp rounding as ratings.parquet; minutes untouched
chk("rounded to 4dp", out$xrapm[out$season_end_year == 2024], 0.1235)
chk("minutes not rounded", out$total_minutes[out$season_end_year == 2024], 900)
# no career / unrelated columns
chk("columns", names(out),
    c("season_end_year", "player_id", "player_name", "xrapm", "xrapm_offense",
      "xrapm_defense", "spm_overall", "total_minutes"))
chk("offense renamed", out$xrapm_offense[out$season_end_year == 2024], 0.1)

# player_name as the key (no player_id anywhere) still works
xr2 <- xr[, setdiff(names(xr), "player_id")]
spm2 <- spm |> rename(player_name = player_id) |> mutate(player_name = toupper(player_name))
meta2 <- data.frame(player_name = c("A", "B", "C"), league = c("EPL", "TUN", "EPL"))
out2 <- build_ratings_by_season(xr2, spm2, meta2, "player_name", "TUN")
chk("name key rows", nrow(out2), 3L)
chk("name key has no player_id", "player_id" %in% names(out2), FALSE)

if (failed > 0L) {
  cat(failed, "test(s) FAILED\n")
  quit(status = 1)
}
cat("test_ratings_by_season: all checks passed\n")
