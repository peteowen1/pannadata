# Build a slim per-tournament event-TIMING dataset for the inthegame-blog
# hydration-break / cooling-break cross-tournament analysis.
#
# Output: tournament_events_timing.parquet — ONE row per in-play event, columns:
#   tournament, season, match_id, home_team, away_team, team_id, x, y,
#   minute, second, period_id, type_id
#
# team_id + coordinates added for pannadata#82's sibling ask, pannadata#85:
# without a per-event ACTOR the blog can only compute undirected counts
# (shot-rate), not directional momentum (home-away), and without x/y it cannot
# weight by territory. Both were already present in the consolidated event
# files at 100% non-NA — this only stopped whitelisting them away.
#
# end_x/end_y are deliberately NOT carried: they cost 8MB of a 24MB file (a
# third of it) and #85 asks for x/y. They are one line away if the blog later
# wants pass direction for progressive-play measures.
#
# Still no qualifiers / player / EPV. EPV is the expensive half of #85 (it needs
# SPADL conversion + scoring over historical tournaments) and is deliberately
# not attempted here; team + x/y alone unlocks the territorial-momentum
# comparison, which is the part that was cheap.
#
# Covers every season in each consolidated file → the 6 hot-climate targets AND
# the older/cool CONTROLS in one shot.
#
# (A previous version of this comment said WC 2022/2026 were absent from the
# consolidated file and needed a separate fetch. Stale as of 2026-08-22: the
# file now runs 1966 through 2026, both included, verified on 1,635,882 rows.)

suppressMessages({library(arrow); library(data.table)})

comps <- c("World_Cup", "UEFA_Euros", "AFCON", "Copa_America",
           "AFC_Asian_Cup", "CONCACAF_Gold_Cup", "Club_World_Cup")
ev_dir  <- "data/opta/events_consolidated"
fx_path <- "data/opta/opta_fixtures.parquet"

fixtures <- if (file.exists(fx_path)) {
  fx <- as.data.table(read_parquet(fx_path))
  unique(fx[, .(match_id, home_team, away_team)])
} else NULL

out <- list()
for (cp in comps) {
  f <- file.path(ev_dir, sprintf("events_%s.parquet", cp))
  if (!file.exists(f)) { message(sprintf("  skip %s (no file)", cp)); next }
  dt <- as.data.table(read_parquet(f))
  miss <- setdiff(c("match_id","minute","second","period_id","type_id"), names(dt))
  if (length(miss)) { message(sprintf("  skip %s (missing %s)", cp, paste(miss, collapse=","))); next }
  # In-play periods only (1=1H,2=2H,3/4=ET) — drops pre-match/warmup markers,
  # keeps everything the break + shot-rate analysis needs.
  dt <- dt[period_id %in% 1:4]
  dt[, tournament := if ("competition" %in% names(dt)) competition else cp]
  if (!"season" %in% names(dt)) dt[, season := NA_character_]
  # Actor + position. Guarded per competition rather than assumed: these are
  # 100% non-NA in the World Cup file today, but a comp whose consolidation
  # predates them must degrade to NA rather than abort the whole build.
  for (col in c("team_id", "x", "y")) {
    if (!col %in% names(dt)) dt[, (col) := NA]
  }
  slim <- dt[, .(tournament, season = as.character(season), match_id,
                 team_id = as.character(team_id),
                 x = as.numeric(x), y = as.numeric(y),
                 minute = as.integer(minute), second = as.integer(second),
                 period_id = as.integer(period_id), type_id = as.integer(type_id))]
  if (!is.null(fixtures)) {
    slim <- merge(slim, fixtures, by = "match_id", all.x = TRUE, sort = FALSE)
  } else { slim[, home_team := NA_character_]; slim[, away_team := NA_character_] }
  setcolorder(slim, c("tournament","season","match_id","home_team","away_team",
                      "team_id","x","y",
                      "minute","second","period_id","type_id"))
  out[[cp]] <- slim
  message(sprintf(paste("  %-20s %7d events | %4d matches | %2d seasons |",
                        "home/away %.0f%% | team_id %.0f%% | x/y %.0f%%"),
                  cp, nrow(slim), uniqueN(slim$match_id), uniqueN(slim$season),
                  100 * mean(!is.na(slim$home_team)),
                  100 * mean(!is.na(slim$team_id)),
                  100 * mean(!is.na(slim$x))))
}

combined <- rbindlist(out, use.names = TRUE, fill = TRUE)
setorder(combined, tournament, season, match_id, period_id, minute, second)

out_path <- "data/opta/tournament_events_timing.parquet"
write_parquet(combined, out_path)
message(sprintf("\nWritten %s: %d rows, %d matches, %.1f MB",
                out_path, nrow(combined), uniqueN(combined$match_id),
                file.size(out_path) / 1048576))
message("Tournaments x seasons:")
print(combined[, .(matches = uniqueN(match_id), events = .N),
               by = .(tournament, season)][order(tournament, season)])
