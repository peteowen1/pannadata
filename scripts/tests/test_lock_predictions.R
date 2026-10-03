#!/usr/bin/env Rscript
# Unit test for scripts/lock_predictions.R (pannadata#156). Usage: Rscript scripts/tests/test_lock_predictions.R
source("scripts/lock_predictions.R")
mk <- function(id, date, status, pH) data.frame(match_id = id, match_date = paste0(date, "Z"),
  league = "ENG", season = "2026-2027", home_team = "A", away_team = "B", prob_H = pH, prob_D = .3,
  prob_A = 1 - pH - .3, pred_home_goals = 1.5, pred_away_goals = 1, status = status)
t1 <- as.POSIXct("2026-10-01 05:00:00", tz = "UTC")
t2 <- as.POSIXct("2026-10-03 05:00:00", tz = "UTC")
# run 1: m1 plays 10-03, m2 plays 10-10, m3 plays 10-01 (match day: not lockable)
cur1 <- rbind(mk("m1", "2026-10-03", "fixture", .5), mk("m2", "2026-10-10", "fixture", .5), mk("m3", "2026-10-01", "fixture", .5))
l1 <- lock_predictions(cur1, NULL, t1)
stopifnot(setequal(l1$match_id, c("m1", "m2")))
# run 2: m1 played (and re-scored to .9), m2 still future and re-scored .6
cur2 <- rbind(mk("m1", "2026-10-03", "played", .9), mk("m2", "2026-10-10", "fixture", .6))
l2 <- lock_predictions(cur2, l1, t2)
stopifnot(l2$prob_H_pre[l2$match_id == "m1"] == .5)      # frozen, not .9
stopifnot(l2$prob_H_pre[l2$match_id == "m2"] == .6)      # refreshed
stopifnot(l2$locked_at[l2$match_id == "m1"] == "2026-10-01T05:00:00Z")
stopifnot(!anyDuplicated(l2$match_id), nrow(l2) == 2)
# a match first seen already played gets no lock
l3 <- lock_predictions(rbind(cur2, mk("m9", "2026-10-02", "played", .4)), l2, t2)
stopifnot(!"m9" %in% l3$match_id)
cat("test_lock_predictions: all passed\n")
