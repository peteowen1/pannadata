# ratings_by_season.R
# Per-season companion to football/ratings.parquet (pannadata#135).
#
# ratings.parquet carries ONE season of xrapm / spm_overall / total_minutes (the
# latest fitted one), although both seasonal tables are fitted per season
# upstream. The blog's team page has a season selector, so a historical squad
# was shown next to today's ratings. This builds the full history as a sibling
# file, `ratings-by-season.parquet`, so ratings.parquet keeps its shape and size.
#
# Lives in its own file for the reason quality_gates.R does: build_blog_data.R
# is a top-to-bottom script, so logic inline there cannot be tested without
# running the whole build. Sourced by build_blog_data.R and by
# scripts/tests/test_ratings_by_season.R (repo root working directory).
#
# The per-season rule is the one build_blog_data.R applies to the latest season,
# repeated per season: one row per player per season (the highest-minutes row if
# a player appears twice), SPM joined on (player, season), blog-excluded
# competitions dropped, same 4dp rounding. So filtering the result to
# max(season_end_year) gives back the xrapm / xrapm_offense / xrapm_defense /
# spm_overall / total_minutes of every active row in ratings.parquet.
#
# `panna` is a career trait with no per-season version and is deliberately NOT
# here; the file carries no career, EPR/PSR or game-log columns.

#' @param seasonal_xrapm Per-season xRAPM table (season_end_year, key, player_name,
#'   total_minutes, xrapm, offense, defense).
#' @param seasonal_spm Per-season SPM table (season_end_year, key, total_minutes, spm).
#' @param player_meta Player metadata; only its `league` is used, to apply the
#'   same exclusion ratings.parquet applies.
#' @param dedup_key Join key column name ("player_id" or "player_name").
#' @param exclude_leagues Leagues dropped from blog outputs (BLOG_COMP_EXCLUDE).
#' @return Data frame, one row per (player, season), ordered by season then key.
build_ratings_by_season <- function(seasonal_xrapm, seasonal_spm, player_meta,
                                    dedup_key, exclude_leagues = character(0)) {
  one_per_player_season <- function(df) {
    df |>
      dplyr::group_by(season_end_year, .data[[dedup_key]]) |>
      dplyr::slice_max(total_minutes, n = 1, with_ties = FALSE) |>
      dplyr::ungroup()
  }

  xr <- one_per_player_season(seasonal_xrapm)
  spm <- one_per_player_season(seasonal_spm) |>
    dplyr::select(season_end_year, dplyr::all_of(dedup_key), spm_overall = spm)

  out <- xr |>
    dplyr::left_join(spm, by = c("season_end_year", dedup_key))

  # ratings.parquet takes `league` from the xRAPM table if it has one, else from
  # player_meta; mirror that so the exclusion drops the same players.
  if (!"league" %in% names(out) && "league" %in% names(player_meta)) {
    out <- out |>
      dplyr::left_join(player_meta |> dplyr::select(dplyr::all_of(c(dedup_key, "league"))),
                       by = dedup_key)
  }
  if ("league" %in% names(out)) {
    out <- out |> dplyr::filter(!league %in% exclude_leagues)
  }

  out |>
    dplyr::rename(xrapm_offense = offense, xrapm_defense = defense) |>
    dplyr::select(
      season_end_year, dplyr::any_of("player_id"), player_name,
      xrapm, xrapm_offense, xrapm_defense, spm_overall, total_minutes
    ) |>
    dplyr::mutate(dplyr::across(
      c(xrapm, xrapm_offense, xrapm_defense, spm_overall), \(x) round(x, 4)
    )) |>
    dplyr::arrange(season_end_year, .data[[dedup_key]])
}
