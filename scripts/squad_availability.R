# Squad availability for ratings.parquet (pannadata#232)
#
# The blog's team page lists a club's players by rating. Rating order is kept,
# but the reader needs to know who is actually around: Mitoma has been in 0 of
# Brighton's last 10 match-day squads, and Veltman / March / Milner are no longer
# in Brighton's squad feed at all. Two columns answer that, both about the
# player's CURRENT club:
#
#   in_club_squad  TRUE  = in the club's current-season Opta squads feed
#                  FALSE = not in it, though the player's (stale) league is one
#                          the squads feed covers -- i.e. he has left / is not
#                          registered. The blog should leave these out or grey them.
#                  NA    = unknown: the player's league is not covered by the
#                          squads feed, so absence from it means nothing.
#   squads_last3   how many of the club's last 3 matches (all competitions) had
#                  the player in the match-day squad (started or on the bench),
#                  0-3. NA when the player has no current club here
#                  (in_club_squad is not TRUE), or the club has played fewer than
#                  3 matches in the squads feed's season (a "0 of 1" tag would
#                  mislead early in a campaign, and last season's matches must
#                  not count against a summer signing).
#
# Match-day squads cannot tell injured from dropped, and the squads feed has no
# injury field: squads_last3 == 0 means "not in recent squads", nothing more.
#
# Clubs are matched on Opta team_id, NEVER team_name: one team_id can carry two
# names and two clubs can share a name.

# Number of club matches the window covers.
SQUAD_WINDOW <- 3L

# The club's last `window` matches, and which players were in each match-day squad.
#
# lineups: match_id, match_date (string, "YYYY-MM-DD" with optional suffix such
#          as "Z"), player_id, team_id, season. One row per player per match.
# Returns list(
#   clubs  = data.frame(team_id, season): clubs with a full window; `season` is
#            the season string of the club's most recent match,
#   counts = data.frame(team_id, player_id, squads_last3): one row per player
#            in at least one of those matches.
# )
# A club's matches are those of the season of its most recent match. The season
# string is shared by a club's league and cup competitions for the leagues that
# matter; where it differs the club ends up with fewer than `window` matches and
# gets no row (NA downstream) rather than a wrong count.
club_last_matches_counts <- function(lineups, window = SQUAD_WINDOW) {
  lu <- data.frame(
    match_id   = as.character(lineups$match_id),
    match_date = substr(as.character(lineups$match_date), 1, 10),
    player_id  = as.character(lineups$player_id),
    team_id    = as.character(lineups$team_id),
    season     = as.character(lineups$season),
    stringsAsFactors = FALSE
  )
  lu <- lu[!is.na(lu$match_id) & !is.na(lu$match_date) & !is.na(lu$team_id) &
             !is.na(lu$player_id) & !is.na(lu$season), , drop = FALSE]
  empty <- list(
    clubs  = data.frame(team_id = character(0), season = character(0),
                        stringsAsFactors = FALSE),
    counts = data.frame(team_id = character(0), player_id = character(0),
                        squads_last3 = integer(0), stringsAsFactors = FALSE))
  if (nrow(lu) == 0L) return(empty)

  # One row per club match. Two clubs in the same match have different team_id,
  # so (team_id, match_id) is the unit.
  m <- unique(lu[c("team_id", "match_id", "match_date", "season")])
  m <- m[order(m$team_id, m$match_date, m$match_id, decreasing = TRUE), , drop = FALSE]
  cur <- m[!duplicated(m$team_id), c("team_id", "season")]   # newest match per club
  m <- m[m$season == cur$season[match(m$team_id, cur$team_id)], , drop = FALSE]
  m$rank <- stats::ave(seq_len(nrow(m)), m$team_id, FUN = seq_along)
  m <- m[m$rank <= window, c("team_id", "match_id"), drop = FALSE]
  full <- names(which(table(m$team_id) >= window))
  m <- m[m$team_id %in% full, , drop = FALSE]
  if (nrow(m) == 0L) return(empty)

  hit <- unique(merge(lu[c("team_id", "match_id", "player_id")], m,
                      by = c("team_id", "match_id")))
  counts <- stats::aggregate(match_id ~ team_id + player_id, data = hit, FUN = length)
  names(counts)[3] <- "squads_last3"
  counts$squads_last3 <- as.integer(counts$squads_last3)
  list(clubs = cur[cur$team_id %in% full, , drop = FALSE], counts = counts)
}

# Add in_club_squad / squads_last3 (and the squads-feed team / league) to the
# ratings frame, keeping its row order and columns.
#
# ratings: has player_id, team, league (any extra columns pass through).
# squads:  player_id, team, team_id, league, season (current-season squads feed,
#          already filtered to the leagues the blog keeps).
# lineups: see club_last_matches_counts().
#
# `team` / `league` come from the squads feed when the player is in it (the
# existing pannadata#123 override); a player not in the feed keeps his old value,
# which in_club_squad then flags.
add_squad_availability <- function(ratings, squads, lineups, window = SQUAD_WINDOW) {
  if (anyDuplicated(squads$player_id))
    stop("squads has duplicate player_id rows (", sum(duplicated(squads$player_id)),
         "); a player can only be at one current club")
  pid <- as.character(ratings$player_id)
  i <- match(pid, as.character(squads$player_id))   # row in squads, NA = not in feed
  in_squad <- !is.na(i)
  sq_team_id <- as.character(squads$team_id)[i]
  sq_season  <- as.character(squads$season)[i]

  covered_leagues <- unique(squads$league)
  ratings$in_club_squad <- ifelse(in_squad, TRUE,
                                  ifelse(ratings$league %in% covered_leagues, FALSE, NA))
  ratings$in_club_squad <- as.logical(ratings$in_club_squad)
  ratings$team   <- ifelse(in_squad, squads$team[i],   ratings$team)
  ratings$league <- ifelse(in_squad, squads$league[i], ratings$league)

  lm <- club_last_matches_counts(lineups, window)
  # The club's matches must be from the season the squads feed describes, or an
  # old season's last 3 matches would be counted against this season's squad.
  club_ok <- paste(lm$clubs$team_id, lm$clubs$season, sep = "\r")
  ok <- in_squad & paste(sq_team_id, sq_season, sep = "\r") %in% club_ok
  hits <- lm$counts$squads_last3[match(paste(sq_team_id, pid, sep = "\r"),
                                       paste(lm$counts$team_id, lm$counts$player_id, sep = "\r"))]
  # Club has a full window but the player is in none of its matches: a real 0.
  hits[ok & is.na(hits)] <- 0L
  hits[!ok] <- NA_integer_
  ratings$squads_last3 <- as.integer(hits)
  ratings
}
