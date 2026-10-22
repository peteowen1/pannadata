#!/usr/bin/env Rscript
# Unit test for drop_thin_seasons() in scripts/league_config.R (pannadata#136).
# Usage: Rscript scripts/tests/test_thin_seasons.R
source("scripts/league_config.R")
season_rows <- function(season, n) data.frame(season = season, match_id = rep(paste0(season, "-", seq_len(n)), each = 2L))
seasons_of <- function(df) sort(unique(df$season))

# 1. Thin early seasons go; complete ones stay; the short newest season stays.
df <- rbind(season_rows("2012-2013", 3), season_rows("2013-2014", 3),
            season_rows("2014-2015", 12), season_rows("2015-2016", 38),
            season_rows("2016-2017", 38), season_rows("2017-2018", 37),
            season_rows("2018-2019", 38), season_rows("2019-2020", 36),
            season_rows("2020-2021", 38), season_rows("2021-2022", 5))
out <- drop_thin_seasons(df, "T")
stopifnot(identical(seasons_of(out), c("2015-2016", "2016-2017", "2017-2018", "2018-2019",
                                       "2019-2020", "2020-2021", "2021-2022")),
          nrow(out) == nrow(df) - 2L * (3L + 3L + 12L))

# 2. A COVID-cut season (~73% of normal) is a real season, not a sample.
df <- rbind(season_rows("2018-2019", 38), season_rows("2019-2020", 28), season_rows("2020-2021", 38))
stopifnot(identical(seasons_of(drop_thin_seasons(df, "T")), seasons_of(df)))

# 3. Nothing thin: input comes back untouched. One season: nothing to compare to.
df <- rbind(season_rows("2019-2020", 38), season_rows("2020-2021", 38))
stopifnot(identical(drop_thin_seasons(df, "T"), df))
one <- season_rows("2020-2021", 3)
stopifnot(identical(drop_thin_seasons(one, "T"), one))

# 4. A thin season from 2015-16 on is a broken source, not Opta's sample era:
# it is kept (with a ::warning:: in the log), while an early thin one still goes.
df <- rbind(season_rows("2013-2014", 3), season_rows("2021-2022", 38),
            season_rows("2022-2023", 38), season_rows("2023-2024", 10),
            season_rows("2024-2025", 38), season_rows("2025-2026", 20))
log <- capture.output(out <- drop_thin_seasons(df, "T"))
stopifnot("2023-2024" %in% seasons_of(out), !"2013-2014" %in% seasons_of(out),
          any(grepl("::warning::.*2023-2024=10", log)))

cat("test_thin_seasons: all passed\n")
