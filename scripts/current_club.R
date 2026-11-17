# Current club for players the squads feed does not list (pannadata#234, parts 3-4).
#
# build_blog_data.R takes team/league from squads.parquet when a player is in it.
# Everyone else used to keep player_meta's team/league: wherever he was last RATED,
# i.e. end of last season. That left relegated clubs' players in "EPL" (West Ham,
# Wolves in the Championship) and departed players under an old club name ("Hearts"
# next to the current "Heart of Midlothian").
#
# Two fixes, both from opta_lineups:
#  1. apply_last_league_club(): a player not in squads whose latest DOMESTIC league
#     appearance (not UCL/UEL/Conference_League, which last_rated_league counts) is RECENT (within max_age_days of the newest lineup) takes that appearance's club
#     and competition. Recent only: an old league appearance must not beat a newer cup
#     appearance for his current club (checked 2026-11-17: without the limit a Bastia
#     player became "Vitesse", his league club of years ago).
#  2. canonical_club_names(): every club name is replaced by the NEWEST name Opta uses
#     for that club, matched on (name, competition) -> team_id where that pair points
#     at exactly one team_id, so one club has one name.

# Newest team_name per team_id (lu: team_id, team_name, match_date).
newest_name_by_id <- function(lu) {
  x <- lu[!is.na(lu$team_id) & !is.na(lu$team_name), c("team_id", "team_name", "match_date")]
  x <- x[order(x$team_id, -as.numeric(x$match_date)), ]
  x <- x[!duplicated(x$team_id), ]
  stats::setNames(x$team_name, x$team_id)
}

# ratings: data.frame with team, league, in_squads (logical), last_dom_league,
# last_dom_team_id, last_dom_date (latest domestic league appearance). ref_date: newest lineup date.
apply_last_league_club <- function(ratings, names_by_id, ref_date, max_age_days = 180L,
                                   exclude_leagues = character(0)) {
  use <- !ratings$in_squads & !is.na(ratings$last_dom_team_id) &
    !is.na(ratings$last_dom_league) & !ratings$last_dom_league %in% exclude_leagues &
    !is.na(ratings$last_dom_date) & as.numeric(ref_date - ratings$last_dom_date) <= max_age_days
  new_team <- unname(names_by_id[ratings$last_dom_team_id[use]])
  ok <- !is.na(new_team)
  idx <- which(use)[ok]
  changed <- sum(is.na(ratings$team[idx]) | is.na(ratings$league[idx]) |
                   ratings$team[idx] != new_team[ok] | ratings$league[idx] != ratings$last_dom_league[idx])
  ratings$team[idx] <- new_team[ok]
  ratings$league[idx] <- ratings$last_dom_league[idx]
  attr(ratings, "club_fallback") <- c(eligible = sum(use), applied = length(idx), changed = changed)
  ratings
}

# Rename each (team, league) to the newest name of the one team_id that name has in that
# competition. Pairs matching 0 or 2+ team_ids are left alone (Opta reuses names such as
# "Liverpool" across countries, so the match is always within a competition).
canonical_club_names <- function(ratings, lu, names_by_id) {
  p <- unique(lu[!is.na(lu$team_id) & !is.na(lu$team_name), c("team_name", "competition", "team_id")])
  k <- paste(p$team_name, p$competition, sep = "\r")
  one <- !k %in% k[duplicated(k)]
  map <- stats::setNames(unname(names_by_id[p$team_id[one]]), k[one])
  rk <- paste(ratings$team, ratings$league, sep = "\r")
  new <- unname(map[rk])
  hit <- !is.na(ratings$team) & !is.na(new) & new != ratings$team
  ratings$team[hit] <- new[hit]
  attr(ratings, "club_renamed") <- sum(hit)
  ratings
}
