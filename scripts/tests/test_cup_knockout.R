#!/usr/bin/env Rscript
# Unit test for scripts/cup_knockout.R (R090). Usage: Rscript scripts/tests/test_cup_knockout.R
source("scripts/cup_knockout.R")
set.seed(1)
N <- 400L
teams <- sprintf("T%02d", 1:36)                 # T01 finished 1st ... T36 36th
positions <- matrix(rep(1:36, each = N), nrow = N, dimnames = list(NULL, teams))
strengths <- data.frame(team = teams, attack = 1.4, defense = 1.2)
pct <- function(rr, lvl) colMeans(rr >= lvl)

# 1. No known ties: old behaviour -- 16 teams reach R16, exactly one winner.
rr <- simulate_knockout(positions, strengths, N)
stopifnot(all(rowSums(rr >= 2L) == 16L), all(rowSums(rr >= 6L) == 1L),
          all(pct(rr, 2L)[25:36] == 0))

# 2. Known R16: T01-T08 v T09-T15 and T24 (T24 got through its playoff, T16 did
#    not). Teams outside it get nothing past the playoff; its 16 teams reach R16
#    with certainty.
r16_teams <- c(teams[1:8], teams[c(9:15, 24)])   # T24 beat its way in, T16 out
ties <- lapply(1:8, function(i) c(teams[i], teams[c(9:15, 24)][i]))
rr <- simulate_knockout(positions, strengths, N, known = list(r16 = ties))
stopifnot(all(pct(rr, 2L)[r16_teams] == 1),
          all(pct(rr, 3L)[setdiff(teams, r16_teams)] == 0),
          all(pct(rr, 6L)[setdiff(teams, r16_teams)] == 0),
          pct(rr, 2L)[teams[16]] == 0,              # T16 lost its playoff
          abs(sum(pct(rr, 6L)) - 1) < 1e-9, abs(sum(pct(rr, 3L)) - 8) < 1e-9)
# each R16 tie sends exactly one team through
for (tt in ties) stopifnot(abs(sum(pct(rr, 3L)[tt]) - 1) < 1e-9)

# 3. Known QF set (4 ties): losers of R16 keep level 2 only, 8 teams at QF.
qf <- list(c("T01", "T02"), c("T03", "T04"), c("T05", "T06"), c("T07", "T08"))
rr <- simulate_knockout(positions, strengths, N, known = list(qf = qf))
stopifnot(all(pct(rr, 3L)[teams[1:8]] == 1), all(pct(rr, 3L)[teams[9:36]] == 0),
          all(pct(rr, 6L)[teams[9:36]] == 0), abs(sum(pct(rr, 6L)) - 1) < 1e-9,
          abs(sum(pct(rr, 4L)) - 4) < 1e-9)

# 4. Known final: only the two finalists can win.
rr <- simulate_knockout(positions, strengths, N, known = list(final = list(c("T03", "T20"))))
stopifnot(all(pct(rr, 6L)[setdiff(teams, c("T03", "T20"))] == 0),
          abs(sum(pct(rr, 6L)[c("T03", "T20")]) - 1) < 1e-9,
          all(pct(rr, 5L)[c("T03", "T20")] == 1), pct(rr, 5L)["T01"] == 0)

# 5. Known playoff ties follow the real draw: 8 ties, one winner each.
po <- list(c("T10", "T24"), c("T09", "T23"), c("T11", "T21"), c("T12", "T22"),
           c("T13", "T20"), c("T14", "T19"), c("T15", "T18"), c("T16", "T17"))
rr <- simulate_knockout(positions, strengths, N, known = list(playoff = po))
for (tt in po) stopifnot(abs(sum(pct(rr, 2L)[tt]) - 1) < 1e-9)
stopifnot(all(pct(rr, 2L)[teams[25:36]] == 0), all(rowSums(rr >= 2L) == 16L))
# a playoff pairing that does not fit the seeding is ignored, not trusted
bad <- po; bad[[1]] <- c("T10", "T11"); bad[[2]] <- c("T09", "T12")
rr <- simulate_knockout(positions, strengths, N, known = list(playoff = bad))
stopifnot(all(rowSums(rr >= 2L) == 16L))

# 6. Unknown team names: warn, fall back to the seeded simulation.
w <- tryCatch(simulate_knockout(positions, strengths, 20L,
                                known = list(final = list(c("T01", "Nobody FC")))),
              warning = function(w) conditionMessage(w))
stopifnot(is.character(w), grepl("outside the league-phase universe", w))

# 7. extract_known_ties: complete rounds only, legs collapsed into ties.
mk <- function(h, a, d) data.frame(home_team = h, away_team = a,
                                    match_date = paste0(d, "T20:00:00Z"), stringsAsFactors = FALSE)
r16_rows <- do.call(rbind, lapply(1:8, function(i)
  rbind(mk(sprintf("A%d", i), sprintf("B%d", i), "2027-03-10"),
        mk(sprintf("B%d", i), sprintf("A%d", i), "2027-03-17"))))
kt <- extract_known_ties(r16_rows)
stopifnot(identical(names(kt), "r16"), length(kt$r16) == 8L)
stopifnot(length(extract_known_ties(r16_rows[r16_rows$home_team != "A1" &
                                               r16_rows$away_team != "A1", ])) == 0L)  # 7 ties: ignored
league_rows <- mk("A1", "B1", "2026-11-04")
stopifnot(length(extract_known_ties(league_rows)) == 0L, !is_knockout_date(league_rows$match_date),
          is_knockout_date("2027-02-17T20:00:00Z"), !is_knockout_date("2026-08-05T20:00:00Z"),
          knockout_stage_of("2027-04-08") == "qf", knockout_stage_of("2027-05-05") == "sf",
          knockout_stage_of("2027-05-30") == "final", is.na(knockout_stage_of("2027-01-28")))
cat("test_cup_knockout: all passed\n")
