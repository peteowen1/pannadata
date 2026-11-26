# Helpers for build_chains_ci.R: Opta event names and per-season chains files
# (pannadata#229, the blog's Football Play Finder).
#
# - OPTA_EVENT_TYPE_NAMES / add_type_name(): a `type_name` label next to
#   `type_id`, so a play log can filter and label by event name without a
#   lookup table in the browser.
# - chain_season_label() / chain_season_file(): per-season file naming.
# - chain_newest_seasons(): which seasons get their own file.
# - chain_coverage_check(): the 80% equity/WPA coverage floor, factored out so
#   the main file and each past-season file are held to the same rule.

# Opta F24 event names for the type_ids that appear in events_<comp>.parquet.
# Written from Opta's F24 event list, NOT read from a file in this repo (the
# docstring in scripts/opta/opta_scraper.py uses a different numbering and is
# not used here). Only ids whose name is well known are listed; any other id
# gets "Type <id>" from add_type_name() so a row is never unlabelled and never
# wrongly labelled. Check a few against panna's OPTA_REFERENCE.md.
OPTA_EVENT_TYPE_NAMES <- c(
  `1` = "Pass", `2` = "Offside Pass", `3` = "Take On", `4` = "Foul",
  `5` = "Out", `6` = "Corner Awarded", `7` = "Tackle", `8` = "Interception",
  `10` = "Save", `11` = "Claim", `12` = "Clearance", `13` = "Miss",
  `14` = "Post", `15` = "Attempt Saved", `16` = "Goal", `17` = "Card",
  `18` = "Player Off", `19` = "Player On", `24` = "Condition Change",
  `27` = "Start Delay", `28` = "End Delay", `30` = "End", `32` = "Start",
  `34` = "Team Set Up", `41` = "Punch", `42` = "Good Skill",
  `43` = "Deleted Event", `44` = "Aerial", `45` = "Challenge",
  `49` = "Ball Recovery", `50` = "Dispossessed", `51` = "Error",
  `52` = "Keeper Pick-up", `53` = "Cross Not Claimed", `54` = "Smother",
  `55` = "Offside Provoked", `56` = "Shield Ball Opp", `57` = "Foul Throw-in",
  `58` = "Penalty Faced", `59` = "Keeper Sweeper", `60` = "Chance Missed",
  `61` = "Ball Touch", `72` = "Caught Offside", `73` = "Other Ball Contact",
  `74` = "Blocked Pass", `83` = "Attempted Tackle"
)

# Name for each type_id; ids not in the table read "Type <id>"; NA stays NA.
add_type_name <- function(type_id, names_map = OPTA_EVENT_TYPE_NAMES) {
  key <- as.character(as.integer(type_id))
  out <- unname(names_map[key])
  unknown <- is.na(out) & !is.na(key)
  out[unknown] <- paste0("Type ", key[unknown])
  out
}

# "2025-2026" -> "2025-26"; the label is filename/URL-safe. Anything that is not
# a two-year club season (e.g. a tournament label) gets NA, so it gets no file.
chain_season_label <- function(season) {
  m <- regmatches(season, regexec("^(\\d{4})-(\\d{4})$", season))
  vapply(m, function(p) {
    if (length(p) != 3L) NA_character_ else paste0(p[2], "-", substr(p[3], 3, 4))
  }, character(1))
}

chain_season_file <- function(league_code, season, out_dir = "blog") {
  file.path(out_dir, paste0("chains-", league_code, "-", chain_season_label(season), ".parquet"))
}

# The newest `n` seasons that have a filename-safe label, newest first.
chain_newest_seasons <- function(seasons, n) {
  s <- sort(unique(as.character(seasons)), decreasing = TRUE)
  s <- s[!is.na(chain_season_label(s))]
  head(s, n)
}

# Coverage check for one equity/WPA column over the rows whose match is in
# `covered_ids`. Returns list(ok, frac, n_covered); ok is TRUE when nothing is
# covered (nothing to judge) or when frac >= floor.
chain_coverage_check <- function(values, match_id, covered_ids, floor = 0.80) {
  covered <- match_id %in% covered_ids
  if (!any(covered)) return(list(ok = TRUE, frac = NA_real_, n_covered = 0L))
  frac <- mean(!is.na(values[covered]))
  list(ok = frac >= floor, frac = frac, n_covered = sum(covered))
}
