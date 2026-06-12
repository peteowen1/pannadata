#!/usr/bin/env Rscript
# Build football chain parquets in CI — one league at a time to manage memory.
# Downloads event files from opta-latest release as needed.
# Usage: Rscript scripts/build_chains_ci.R
# Requires: source/opta_lineups.parquet (pre-downloaded)
# Outputs: blog/chains-{CODE}.parquet

library(arrow)
library(dplyr)

src_dir <- "source"
out_dir <- "blog"
dir.create(out_dir, showWarnings = FALSE)

cat("=== Chain Builder (CI) ===\n")
cat("Memory:", round(as.numeric(system("free -m | awk '/Mem:/{print $2}'", intern = TRUE)), 0), "MB total\n\n")

# Join keys drift between sources: newer scraper consolidations write event_id
# as INT64 (arrow reads it as bit64::integer64) while action_equity / action_wpa
# carry doubles — dplyr hard-errors on an integer64<->double join, which killed
# the whole loop at Championship (2026-06-12 run) and left every later comp's
# chains stale on R2 since March. Opta event ids are < 2^53, so double is exact.
normalize_keys <- function(df) {
  for (col in intersect(c("event_id"), names(df))) {
    df[[col]] <- as.numeric(df[[col]])
  }
  if ("match_id" %in% names(df)) df$match_id <- as.character(df$match_id)
  df
}

# Load equity lookup (optional — from panna predictions pipeline)
equity_file <- file.path(src_dir, "action_equity.parquet")
has_equity <- file.exists(equity_file)
if (has_equity) {
  equity_lookup <- normalize_keys(read_parquet(equity_file))
  # Transition: action_equity.parquet carries `epv_credit` (panna 10c, from
  # 2026-06-03) or the legacy `equity` name. Normalise to epv_credit so the
  # join and the chains output column are name-stable regardless of which
  # snapshot we downloaded. (The blog reads `epv_credit ?? equity`.)
  if (!"epv_credit" %in% names(equity_lookup) && "equity" %in% names(equity_lookup)) {
    equity_lookup <- equity_lookup |> rename(epv_credit = equity)
  }
  equity_match_ids <- unique(equity_lookup$match_id)
  cat("Equity loaded:", nrow(equity_lookup), "actions across",
      length(equity_match_ids), "matches\n")
} else {
  cat("No equity file — chains will not include EPV credit\n")
}

# Load wpa lookup (optional — from panna's 06_calculate_wpa.R action_wpa
# shards, one parquet per league-season). When present, we join per-event
# wp/wpa/wpa_actor/wpa_receiver onto chains so the blog match page can
# render per-event WPA without a worker call for finished matches. See
# inthegame-blog memory followup_panna_chains_per_event_wp.md.
wpa_files <- list.files(src_dir,
                         pattern = "^action_wpa_.*\\.parquet$",
                         full.names = TRUE)
has_wpa <- length(wpa_files) > 0
if (has_wpa) {
  cat("Loading WPA from", length(wpa_files), "shard(s)...\n")
  wpa_lookup <- normalize_keys(
    data.table::rbindlist(lapply(wpa_files, read_parquet),
                          use.names = TRUE, fill = TRUE))
  wpa_match_ids <- unique(wpa_lookup$match_id)
  cat("WPA loaded:", nrow(wpa_lookup), "actions across",
      length(wpa_match_ids), "matches\n")
} else {
  cat("No action_wpa_*.parquet files — chains will not include per-action WPA\n")
}

# Load lineups once (shared across all leagues)
lineup_file <- file.path(src_dir, "opta_lineups.parquet")
if (!file.exists(lineup_file)) stop("Missing: ", lineup_file)

cat("Loading lineups...\n")
lineups <- read_parquet(lineup_file,
  col_select = c("match_id", "team_id", "team_name", "team_position"))
match_teams <- lineups |>
  distinct(match_id, team_id, team_name, team_position) |>
  group_by(match_id) |>
  summarise(
    home_team = team_name[team_position == "home"][1],
    away_team = team_name[team_position == "away"][1],
    .groups = "drop"
  )
global_team_map <- lineups |> distinct(team_id, team_name) |>
  group_by(team_id) |> slice(1) |> ungroup()
rm(lineups); gc(verbose = FALSE)
cat("Lineups loaded:", nrow(match_teams), "matches\n\n")

source("scripts/league_config.R")
# World Cup shard for the blog's WC match pages (Territorial/Pass Map; #64).
# Deliberately NOT added to BLOG_COMP_TO_CODE — that map also drives
# ratings/player-details/standings steps where a tournament doesn't belong.
# Same local-extension precedent as rebuild_match_stats.R's build_shard call.
blog_comps <- c(BLOG_COMPS, "World_Cup")
comp_to_code <- c(BLOG_COMP_TO_CODE, World_Cup = "WC")
dead_ball_types <- c(2L, 4L, 5L, 6L, 17L, 55L, 56L, 57L, 70L, 80L, 81L)
non_play_types <- c(18L, 19L, 24L, 27L, 28L, 30L, 32L, 34L, 37L, 40L, 43L, 65L, 68L)
shot_types <- c(13L, 14L, 15L, 16L)

# Equity/WPA joins land on chain events by (match_id, event_id). Only chain
# events that survive panna's SPADL conversion carry equity/wpa — SPADL drops
# non-gameplay events and merges duel pairs, so a *healthy* join matches ~84-86%
# of chain actions (see pannadata/CLAUDE.md "Equity join in chains"). A sharp
# drop below this means action_equity / action_wpa drifted out of sync with the
# events snapshot used here (different scrape, different event_id scheme), which
# would silently ship a misaligned column. A breach skips that comp (its prior
# parquet is left intact, not overwritten with a bad one) and is recorded in
# join_failures; the build fails after all healthy comps are built, so one
# drifted comp doesn't block delivery of the others while CI still goes red.
MIN_JOIN_MATCH_FRAC <- 0.80

# Comps whose equity/wpa join breached a guard. Healthy comps still build and
# upload; this list is raised as a hard error after the loop.
join_failures <- character(0)

for (comp in blog_comps) {
  event_file <- file.path(src_dir, paste0("events_", comp, ".parquet"))

  # Download if not present (CI downloads one at a time to save disk)
  if (!file.exists(event_file)) {
    cat("Downloading events_", comp, ".parquet...\n", sep = "")
    dl <- system2("gh", c("release", "download", "opta-latest",
      "-p", paste0("events_", comp, ".parquet"), "-D", src_dir),
      stdout = TRUE, stderr = TRUE)
    if (!file.exists(event_file)) { cat("  SKIP (download failed)\n"); next }
  }

  cat(comp, ": ", sep = "")
  # One comp's failure must not starve every comp after it in the loop — that
  # failure mode left ESP..TUR chains stale on R2 for 3 months and UCL/UEL/UECL
  # never built (the loop died at Championship on a join type error while the
  # workflow step is continue-on-error). Errors are recorded in join_failures
  # and raised after all comps have had their turn.
  tryCatch({
  events <- normalize_keys(read_parquet(event_file))
  seasons <- sort(unique(events$season), decreasing = TRUE)
  if (nrow(events) > 200000 && length(seasons) > 1) {
    events <- events |> filter(season == seasons[1])
  }
  cat(nrow(events), " events (", seasons[1], ") ... ", sep = "")

  events <- events |>
    filter(!type_id %in% non_play_types) |>
    arrange(match_id, period_id, minute, second, event_id)

  # Build chain numbers
  events$chain_number <- NA_integer_
  chain_n <- 0L
  prev_team <- ""
  prev_match <- ""
  for (i in seq_len(nrow(events))) {
    new_chain <- events$match_id[i] != prev_match ||
      (events$team_id[i] != prev_team && events$team_id[i] != "") ||
      events$type_id[i] %in% dead_ball_types
    if (new_chain) chain_n <- chain_n + 1L
    events$chain_number[i] <- chain_n
    prev_team <- events$team_id[i]
    prev_match <- events$match_id[i]
  }

  chain_states <- events |>
    group_by(chain_number) |>
    summarise(
      final_state = case_when(
        any(type_id == 16L) ~ "goal",
        any(type_id %in% c(13L, 14L, 15L)) ~ "shot_saved",
        any(type_id %in% c(5L, 6L)) ~ "out_of_play",
        any(type_id == 4L) ~ "foul",
        any(type_id %in% c(70L, 2L)) ~ "offside",
        TRUE ~ "lost_possession"
      ),
      .groups = "drop"
    )

  chains <- events |>
    left_join(chain_states, by = "chain_number") |>
    group_by(match_id, chain_number) |>
    mutate(display_order = row_number()) |>
    ungroup() |>
    left_join(match_teams, by = "match_id") |>
    left_join(global_team_map |> rename(team_name_lookup = team_name), by = "team_id") |>
    transmute(
      match_id, chain_number = as.integer(chain_number),
      display_order = as.integer(display_order),
      event_id,
      player_id, player_name, team_id,
      x = round(x, 1), y = round(y, 1),
      end_x = round(end_x, 1), end_y = round(end_y, 1),
      type_id = as.integer(type_id), outcome = as.integer(outcome),
      final_state, period_id = as.integer(period_id),
      minute = as.integer(minute), second = as.integer(second),
      is_shot = type_id %in% shot_types,
      competition, season,
      team_name = coalesce(team_name_lookup, team_id),
      home_team = coalesce(home_team, "Unknown"),
      away_team = coalesce(away_team, "Unknown")
    ) |>
    select(-any_of("team_name_lookup")) |>
    arrange(match_id, chain_number, display_order)

  # Join per-action EPV credit if available. Output column is `epv_credit`
  # (per-action player credit — sum it, never diff it). Renamed from `equity`
  # 2026-06-03 to end the collision with the worker's `equity` = EPV state; the
  # blog reads `epv_credit ?? equity` so the rename is transparent.
  if (has_equity) {
    n_before <- nrow(chains)
    chains <- chains |>
      left_join(equity_lookup |> select(match_id, event_id, epv_credit),
                by = c("match_id", "event_id"))
    # (match_id, event_id) is unique within a match, so the left join must not
    # add rows. If it ever does, action_equity has duplicate keys and the join
    # is scattering credit across rows (and corrupting display_order) — skip
    # this comp rather than write a corrupted parquet.
    if (nrow(chains) != n_before) {
      join_failures <- c(join_failures, sprintf(
        "%s: epv_credit join inflated chains %d -> %d rows (duplicate (match_id, event_id) in action_equity)",
        comp, n_before, nrow(chains)))
      cat("  SKIP ", comp, " — epv_credit join inflated rows (recorded, continuing)\n", sep = "")
      next
    }
    # Coverage floor, scoped to matches present in action_equity (the
    # `match_id %in% equity_match_ids` filter). Uncovered matches legitimately
    # have no credit — e.g. older seasons the current-season equity alias omits
    # (low-volume comps keep all seasons; see the 200k season filter above) — so
    # measure the SPADL match rate only over covered matches. Healthy ~85%; a
    # sharp drop there means the event_id scheme drifted between action_equity
    # and events_<comp>.parquet.
    n_eq <- sum(!is.na(chains$epv_credit))
    covered <- chains$match_id %in% equity_match_ids
    if (any(covered)) {
      frac <- mean(!is.na(chains$epv_credit[covered]))
      cat("  epv_credit: ", n_eq, "/", nrow(chains), " actions (",
          sprintf("%.1f%%", 100 * frac), " of ", sum(covered),
          " covered)\n", sep = "")
      if (frac < MIN_JOIN_MATCH_FRAC) {
        join_failures <- c(join_failures, sprintf(
          "%s: epv_credit matched only %.1f%% of actions in covered matches (floor %.0f%%) — action_equity.parquet is likely stale or misaligned with events_%s.parquet",
          comp, 100 * frac, 100 * MIN_JOIN_MATCH_FRAC, comp))
        cat("  SKIP ", comp, " — epv_credit coverage ", sprintf("%.1f%%", 100 * frac),
            " below floor (recorded, continuing)\n", sep = "")
        next
      }
    } else {
      # No overlap at all. Legitimate for a comp out of the alias's season/comp
      # set, but also the signature of a match_id scheme drift — warn loudly
      # (not just cat) since we are shipping an all-NA epv_credit column.
      warning(sprintf(
        "%s: epv_credit join matched 0 of %d chain actions — no overlap with action_equity; shipping an all-NA epv_credit column (season/comp mismatch or match_id drift?)",
        comp, nrow(chains)), call. = FALSE, immediate. = TRUE)
    }
  }

  # Join per-action WPA if available (panna 06_calculate_wpa.R output).
  # Same join key as equity (match_id, event_id) and the same fail-fast
  # guards. When wpa is present, the blog match page can skip the worker
  # call for finished matches.
  if (has_wpa) {
    n_before <- nrow(chains)
    chains <- chains |>
      left_join(wpa_lookup |> select(match_id, event_id,
                                      wp, wpa, wpa_actor, wpa_receiver),
                by = c("match_id", "event_id"))
    if (nrow(chains) != n_before) {
      join_failures <- c(join_failures, sprintf(
        "%s: wpa join inflated chains %d -> %d rows (duplicate (match_id, event_id) in action_wpa)",
        comp, n_before, nrow(chains)))
      cat("  SKIP ", comp, " — wpa join inflated rows (recorded, continuing)\n", sep = "")
      next
    }
    n_wpa <- sum(!is.na(chains$wpa))
    covered <- chains$match_id %in% wpa_match_ids
    if (any(covered)) {
      frac <- mean(!is.na(chains$wpa[covered]))
      cat("  wpa: ", n_wpa, "/", nrow(chains), " actions (",
          sprintf("%.1f%%", 100 * frac), " of ", sum(covered),
          " covered)\n", sep = "")
      if (frac < MIN_JOIN_MATCH_FRAC) {
        join_failures <- c(join_failures, sprintf(
          "%s: wpa matched only %.1f%% of actions in covered matches (floor %.0f%%) — action_wpa_*.parquet is likely stale or misaligned with events_%s.parquet",
          comp, 100 * frac, 100 * MIN_JOIN_MATCH_FRAC, comp))
        cat("  SKIP ", comp, " — wpa coverage ", sprintf("%.1f%%", 100 * frac),
            " below floor (recorded, continuing)\n", sep = "")
        next
      }
    } else {
      warning(sprintf(
        "%s: wpa join matched 0 of %d chain actions — no overlap with action_wpa; shipping an all-NA wpa column (season/comp mismatch or match_id drift?)",
        comp, nrow(chains)), call. = FALSE, immediate. = TRUE)
    }
  }

  league_code <- comp_to_code[comp]
  out_path <- file.path(out_dir, paste0("chains-", league_code, ".parquet"))
  write_parquet(chains, out_path)
  cat(nrow(chains), " chains -> ", out_path, "\n", sep = "")

  # Free memory before next league
  rm(events, chains, chain_states); gc(verbose = FALSE)
  }, error = function(e) {
    join_failures <<- c(join_failures,
                        sprintf("%s: %s", comp, conditionMessage(e)))
    cat("  FAIL ", comp, " — ", conditionMessage(e),
        " (recorded, continuing)\n", sep = "")
    gc(verbose = FALSE)
  })

  # Delete event file to save disk space in CI
  unlink(event_file)
}

# Healthy comps are built and uploaded above; now fail the run if any comp's
# equity/wpa join breached a guard, so CI goes red with the full offender list.
# The workflow's chains step runs with continue-on-error (so the R2 upload
# still ships the healthy comps), which demotes this stop() to a step-level
# warning and leaves the RUN green — so also write a marker file that a final
# non-continue-on-error workflow step checks AFTER the upload; that step is
# what actually turns the run red. The ::error:: annotation surfaces the
# offender list on the run page either way.
if (length(join_failures) > 0) {
  writeLines(join_failures, "chains_failures.txt")
  cat(sprintf("::error::Chain build failed for %d comp(s): %s\n",
              length(join_failures),
              paste(join_failures, collapse = " | ")))
  stop(sprintf("Chain build failed for %d comp(s):\n%s",
               length(join_failures),
               paste0("  - ", join_failures, collapse = "\n")))
}

cat("\nDone!\n")
