# Build a slim per-tournament event-TIMING dataset for the inthegame-blog
# hydration-break / cooling-break cross-tournament analysis.
#
# Output: tournament_events_timing.parquet — ONE row per in-play event, columns:
#   tournament, season, match_id, home_team, away_team, minute, second,
#   period_id, type_id
# No coords / qualifiers / player / EPV — just enough for the gap-based break
# detector + shot-rate flow analysis. The blog runs the (calibrated) detector;
# we ship raw timing only.
#
# Covers every season in each consolidated file → the 6 hot-climate targets AND
# the older/cool CONTROLS in one shot. WC 2022/2026 are NOT in the consolidated
# WC file (2002-2018); flagged for a separate fetch.

suppressMessages({library(arrow); library(data.table)})

comps <- c("World_Cup", "UEFA_Euros", "AFCON", "Copa_America",
           "AFC_Asian_Cup", "CONCACAF_Gold_Cup", "Club_World_Cup")
ev_dir  <- "data/opta/events_consolidated"
fx_path <- "data/opta/opta_fixtures.parquet"

fixtures <- if (file.exists(fx_path)) {
  fx <- as.data.table(read_parquet(fx_path))
  unique(fx[, .(match_id, home_team, away_team)])
} else NULL

keep <- c("match_id", "minute", "second", "period_id", "type_id",
          "competition", "season")

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
  slim <- dt[, .(tournament, season = as.character(season), match_id,
                 minute = as.integer(minute), second = as.integer(second),
                 period_id = as.integer(period_id), type_id = as.integer(type_id))]
  if (!is.null(fixtures)) {
    slim <- merge(slim, fixtures, by = "match_id", all.x = TRUE, sort = FALSE)
  } else { slim[, home_team := NA_character_]; slim[, away_team := NA_character_] }
  setcolorder(slim, c("tournament","season","match_id","home_team","away_team",
                      "minute","second","period_id","type_id"))
  out[[cp]] <- slim
  message(sprintf("  %-20s %7d events | %4d matches | %2d seasons | home/away %.0f%%",
                  cp, nrow(slim), uniqueN(slim$match_id), uniqueN(slim$season),
                  100 * mean(!is.na(slim$home_team))))
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
