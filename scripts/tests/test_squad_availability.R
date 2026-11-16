#!/usr/bin/env Rscript
# Tests for scripts/squad_availability.R (pannadata#232)
#
#   Rscript scripts/tests/test_squad_availability.R      (from the repo root)
#
# Sources the shipped functions rather than restating them.

source("scripts/squad_availability.R")

failed <- 0L
chk <- function(label, got, want) {
  if (!identical(got, want)) {
    failed <<- failed + 1L
    cat("FAIL:", label, "- got", format(got), "want", format(want), "\n")
  }
}

# Club "T1": 4 matches in season S1 (m1 oldest .. m4 newest); last 3 = m2,m3,m4.
# Club "T2": only 2 matches this season. Club "T3": newest match is last season.
# "T1" also has an older match (m0) from the previous season.
mk <- function(match_id, date, team, season, players)
  data.frame(match_id = match_id, match_date = date, team_id = team,
             season = season, player_id = players, stringsAsFactors = FALSE)
lineups <- rbind(
  mk("m0", "2026-05-01Z", "T1", "2025-2026", c("a", "b", "c")),
  mk("m1", "2026-10-01Z", "T1", "2026-2027", c("a", "b", "c")),   # outside window
  mk("m2", "2026-10-08Z", "T1", "2026-2027", c("a", "b")),
  mk("m3", "2026-10-15Z", "T1", "2026-2027", c("a", "b")),
  mk("m4", "2026-10-22Z", "T1", "2026-2027", c("a", "d")),
  mk("n1", "2026-10-10Z", "T2", "2026-2027", c("e", "f")),
  mk("n2", "2026-10-17Z", "T2", "2026-2027", c("e", "f")),
  mk("o1", "2026-05-01Z", "T3", "2025-2026", c("g")),
  mk("o2", "2026-05-08Z", "T3", "2025-2026", c("g")),
  mk("o3", "2026-05-15Z", "T3", "2025-2026", c("g"))
)
# same match seen twice (duplicate lineup row) must not double count
lineups <- rbind(lineups, lineups[lineups$match_id == "m4" & lineups$player_id == "d", ])

lm <- club_last_matches_counts(lineups)
cnt <- function(t, p) lm$counts$squads_last3[lm$counts$team_id == t & lm$counts$player_id == p]
chk("a in all 3", cnt("T1", "a"), 3L)
chk("b in 2 of 3", cnt("T1", "b"), 2L)
chk("c only in an older match: absent from counts", length(cnt("T1", "c")), 0L)
chk("d in 1 (duplicate row not double counted)", cnt("T1", "d"), 1L)
chk("T2 has no full window", "T2" %in% lm$clubs$team_id, FALSE)
chk("T1 season is the newest match's", lm$clubs$season[lm$clubs$team_id == "T1"], "2026-2027")
chk("T3 full window, old season", lm$clubs$season[lm$clubs$team_id == "T3"], "2025-2026")
# the same player_id at another club is counted for that club only
lu2 <- rbind(lineups, mk("p1", "2026-10-22Z", "T9", "2026-2027", "a"))
chk("clubs separate on team_id", length(club_last_matches_counts(lu2)$counts$team_id[
  club_last_matches_counts(lu2)$counts$team_id == "T9"]), 0L)  # T9 has 1 match only

squads <- data.frame(
  player_id = c("a", "b", "c", "d", "e", "g"),
  team      = c("Club One", "Club One", "Club One", "Club One", "Club Two", "Club Three"),
  team_id   = c("T1", "T1", "T1", "T1", "T2", "T3"),
  league    = "EPL",
  season    = c("2026-2027", "2026-2027", "2026-2027", "2026-2027", "2026-2027", "2026-2027"),
  stringsAsFactors = FALSE)
ratings <- data.frame(
  player_id = c("x", "a", "b", "c", "e", "g", "veltman", "abroad", "d"),
  player_name = c("X","A","B","C","E","G","V","W","D"),
  team   = c("Old", "Old", "Old", "Old", "Old", "Old", "Club One", "Elsewhere", "Old"),
  league = c("EPL", "La_Liga", "EPL", "EPL", "EPL", "EPL", "EPL", "Qatar_League", "EPL"),
  panna = 1:9, stringsAsFactors = FALSE)
res <- add_squad_availability(ratings, squads, lineups)

chk("row count kept", nrow(res), nrow(ratings))
chk("row order kept", res$player_id, ratings$player_id)
chk("columns kept, new ones appended", names(res), c(names(ratings), "in_club_squad", "squads_last3"))
chk("panna untouched", res$panna, ratings$panna)
chk("in_club_squad", res$in_club_squad,
    c(FALSE, TRUE, TRUE, TRUE, TRUE, TRUE, FALSE, NA, TRUE))
chk("team from squads when in feed", res$team,
    c("Old", "Club One", "Club One", "Club One", "Club Two", "Club Three", "Club One", "Elsewhere", "Club One"))
chk("league from squads when in feed", res$league[2], "EPL")
chk("league kept when not in feed", res$league[8], "Qatar_League")
# a: 3, b: 2, c: in feed but in none of the last 3 -> real 0, e: club has 2 matches -> NA,
# g: club's newest match is last season but feed says 2026-2027 -> NA, not-in-feed -> NA
chk("squads_last3", res$squads_last3,
    c(NA, 3L, 2L, 0L, NA, NA, NA, NA, 1L))
chk("squads_last3 integer", is.integer(res$squads_last3), TRUE)
chk("in_club_squad logical", is.logical(res$in_club_squad), TRUE)

# team_name collision: two clubs share a name, only team_id tells them apart
lu3 <- rbind(
  mk("q1", "2026-10-01Z", "A1", "2026", c("p", "q")), mk("q2", "2026-10-08Z", "A1", "2026", c("p")),
  mk("q3", "2026-10-15Z", "A1", "2026", c("p")),
  mk("r1", "2026-10-01Z", "B1", "2026", c("q")), mk("r2", "2026-10-08Z", "B1", "2026", c("q")),
  mk("r3", "2026-10-15Z", "B1", "2026", c("q")))
sq3 <- data.frame(player_id = c("p", "q"), team = c("Same Name", "Same Name"),
                  team_id = c("A1", "B1"), league = "MLS", season = "2026",
                  stringsAsFactors = FALSE)
r3 <- add_squad_availability(data.frame(player_id = c("p", "q"), team = "x", league = "MLS",
                                        stringsAsFactors = FALSE), sq3, lu3)
chk("same team_name, different team_id", r3$squads_last3, c(3L, 3L))
lu4 <- lu3[!(lu3$team_id == "B1" & lu3$match_id == "r3"), ]
chk("q only counted at its own club", add_squad_availability(
  data.frame(player_id = "q", team = "x", league = "MLS", stringsAsFactors = FALSE),
  sq3, lu4)$squads_last3, NA_integer_)

# duplicate squads rows are an error, not a silent pick
dup <- tryCatch(add_squad_availability(ratings, rbind(squads, squads[1, ]), lineups),
                error = function(e) "err")
chk("duplicate squads player_id stops", dup, "err")

# empty lineups: everything NA, nothing breaks
res0 <- add_squad_availability(ratings, squads, lineups[0, ])
chk("empty lineups -> all NA squads_last3", all(is.na(res0$squads_last3)), TRUE)
chk("empty lineups keeps in_club_squad", res0$in_club_squad, res$in_club_squad)

if (failed > 0L) { cat(failed, "test(s) FAILED\n"); quit(status = 1) }
cat("test_squad_availability: all passed\n")
