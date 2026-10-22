# Shared league configuration — single source of truth for blog data pipeline.
# Opta competition name → blog league code mapping.
# Source this file from any script that needs the league list.

BLOG_COMP_TO_CODE <- c(
  EPL = "ENG", Championship = "ENG2", La_Liga = "ESP", Ligue_1 = "FRA",
  Bundesliga = "GER", Serie_A = "ITA", Eredivisie = "NED",
  Primeira_Liga = "POR", Scottish_Premiership = "SCO", Super_Lig = "TUR",
  UCL = "UCL", UEL = "UEL", Conference_League = "UECL"
)

BLOG_COMPS <- names(BLOG_COMP_TO_CODE)
BLOG_CODES <- unname(BLOG_COMP_TO_CODE)

# Competitions whose SHOTS we want in match-shots.parquet / predictions xG, even
# though they are NOT first-class blog leagues (no BLOG_CODE, no match-stats
# file, no league badge). The World Cup is keyed by tournament YEAR not a club
# season, and its `league` in match_predictions.parquet is "WC" while its
# `competition` in opta_shot_events.parquet is "World_Cup" (see panna
# R/opta_loaders.R league map). Adding it here lets finished WC matches get
# xg_home/xg_away from the same panna XGBoost shot-xG model as club games,
# joined onto predictions by match_id — without pulling WC into BLOG_COMPS
# (which drives match-stats / league-xg and would have wider side effects).
BLOG_SHOT_COMPS <- c(BLOG_COMPS, "World_Cup")

# Competitions rated upstream but deliberately EXCLUDED from the blog outputs.
# CAF_CL and Tunisian_Ligue_1 are no longer treated as tier-2 for the blog, and
# they carry ~0% box-score skills coverage (the skills loader never picks them up),
# so they would only ever surface as blank Top Trumps cards / skill-less leaderboard
# rows. Dropped at build time from ratings.parquet + player-details.parquet via
# BLOG_COMP_EXCLUDE; they remain in the upstream ratings for other uses.
# Opta competition names (match the `competition`/`league` column verbatim).
BLOG_COMP_EXCLUDE <- c("CAF_CL", "Tunisian_Ligue_1")

# Seasons too thin to be a season (pannadata#136). Opta's early coverage is a
# sample: EPL 2012-13 holds 47 of 380 matches, Ligue_1 2014-15 102 of 380, so a
# shard that advertises those seasons lets a consumer build a "final table" out
# of a few games. A season is kept when it holds at least THIN_SEASON_FRAC of
# the competition's median season (match counts are matches with a player who
# played, so COVID-cut seasons -- Ligue_1 2019-20 at 279 of 380 -- stay in). The
# newest season is always kept: it is in progress, so it is short on purpose.
THIN_SEASON_FRAC <- 0.5
# Only seasons starting before this year may be cut: that is Opta's sampled era.
# A thin season from here on is a broken source, not a sample, so it is kept and
# the log gets a ::warning:: instead of the season disappearing from the shard.
THIN_SEASON_BEFORE <- 2015L

# `df` needs `season` and `match_id`. Returns df without the thin seasons and
# prints which ones went, so a build log shows the cut.
drop_thin_seasons <- function(df, label, frac = THIN_SEASON_FRAC) {
  counts <- tapply(df$match_id, df$season, function(x) length(unique(x)))
  seasons <- names(counts)
  if (length(seasons) < 2L) return(df)
  floor_n <- frac * stats::median(counts)
  thin <- setdiff(seasons[counts < floor_n], max(seasons))
  recent <- thin[as.integer(substr(thin, 1L, 4L)) >= THIN_SEASON_BEFORE]
  if (length(recent)) {
    cat("::warning::", label, ": thin seasons kept, check the source (fewer than ",
        floor_n, " matches): ", paste0(recent, "=", counts[recent], collapse = ", "),
        "\n", sep = "")
  }
  thin <- setdiff(thin, recent)
  if (length(thin) == 0L) return(df)
  cat(label, ": dropping thin seasons (fewer than ", floor_n, " matches): ",
      paste0(thin, "=", counts[thin], collapse = ", "), "\n", sep = "")
  df[!df$season %in% thin, , drop = FALSE]
}
