#!/usr/bin/env Rscript
# Unit test for scripts/chain_seasons.R (pannadata#229).
# Usage (from the repo root): Rscript scripts/tests/test_chain_seasons.R
source("scripts/chain_seasons.R")

# --- add_type_name -----------------------------------------------------------
nm <- add_type_name(c(1L, 16L, 74L, 50L, 90L, NA))
stopifnot(identical(nm, c("Pass", "Goal", "Blocked Pass", "Dispossessed", "Type 90", NA)))
stopifnot(identical(add_type_name(1), "Pass"))              # double ids work
stopifnot(identical(add_type_name(integer(0)), character(0)))
stopifnot(!anyNA(add_type_name(1:100)))                     # never unlabelled
stopifnot(!anyDuplicated(names(OPTA_EVENT_TYPE_NAMES)))     # one name per id
stopifnot(!anyDuplicated(unname(OPTA_EVENT_TYPE_NAMES)))    # one id per name
# Every type id the chain builder reads by number must be named.
for (id in c(2L, 4L, 5L, 6L, 10L, 11L, 13L, 14L, 15L, 16L, 41L)) {
  stopifnot(!startsWith(add_type_name(id), "Type "))
}

# --- chain_season_label / chain_season_file ----------------------------------
stopifnot(identical(chain_season_label(c("2025-2026", "1999-2000", "2026 Canada-Mexico-USA", "2025", NA)),
                    c("2025-26", "1999-00", NA, NA, NA)))
stopifnot(identical(chain_season_file("ENG", "2025-2026"), file.path("blog", "chains-ENG-2025-26.parquet")))

# --- chain_newest_seasons ----------------------------------------------------
ss <- c("2023-2024", "2025-2026", "2024-2025", "2025-2026", "2022-2023")
stopifnot(identical(chain_newest_seasons(ss, 3L), c("2025-2026", "2024-2025", "2023-2024")))
stopifnot(identical(chain_newest_seasons(ss, 1L), "2025-2026"))
stopifnot(identical(chain_newest_seasons(ss, 0L), character(0)))
stopifnot(identical(chain_newest_seasons("2025-2026", 3L), "2025-2026"))   # fewer than n
stopifnot(identical(chain_newest_seasons(c("2026 Canada-Mexico-USA", "2022 Qatar"), 3L), character(0)))

# --- chain_coverage_check ----------------------------------------------------
ids <- rep(c("a", "b"), each = 10)
v <- c(rep(1, 9), NA, rep(NA_real_, 10))                   # a: 90% matched; b: uncovered
r <- chain_coverage_check(v, ids, "a", 0.80)
stopifnot(r$ok, isTRUE(all.equal(r$frac, 0.9)), r$n_covered == 10L)
r <- chain_coverage_check(v, ids, c("a", "b"), 0.80)       # b now covered, all NA: 45%
stopifnot(!r$ok, isTRUE(all.equal(r$frac, 0.45)), r$n_covered == 20L)
r <- chain_coverage_check(v, ids, "zzz", 0.80)             # nothing covered: nothing to judge
stopifnot(r$ok, is.na(r$frac), r$n_covered == 0L)
r <- chain_coverage_check(c(rep(1, 8), NA, NA), rep("a", 10), "a", 0.80)   # exactly at the floor
stopifnot(r$ok)

# --- shot_xg_lookup / join_shot_xg (inthegame-blog#336) ------------------------
sh <- data.frame(match_id = c("m1", "m1", "m2", "m2"), event_id = c(10, 3e9, 5, 6),
                 xg = c(0.1, 0.25, NA, 0.4), xgot = c(0.2, NA, NA, 0.5))
lk <- shot_xg_lookup(sh)
ch <- data.frame(match_id = c("m1", "m1", "m1", "m2", "m2", "m9"), event_id = c(10, 11, 3e9, 5, 6, 1),
                 is_shot = c(TRUE, FALSE, TRUE, TRUE, TRUE, TRUE))
r <- join_shot_xg(ch, lk, "T")
stopifnot(is.null(r$failure), nrow(r$chains) == nrow(ch), identical(r$chains$event_id, ch$event_id))
stopifnot(isTRUE(all.equal(r$chains$xg, c(0.1, NA, 0.25, NA, 0.4, NA))))    # 3e9 id keyed exactly; own goal NA kept
stopifnot(isTRUE(all.equal(r$chains$xgot, c(0.2, NA, NA, NA, 0.5, NA))))
stopifnot(r$n_shots == 5L, r$n_covered == 4L, r$n_found == 4L, r$n_xg == 3L) # m9 not in the shot table: not judged
stopifnot(is.na(r$chains$xg[2]))                                             # non-shot row stays NA
lk_pen <- shot_xg_lookup(transform(sh, xg = c(0.76943, 0.25, NA, 0.4)))     # unrounded penalty value
stopifnot(identical(join_shot_xg(ch, lk_pen, "T")$chains$xg[1], 0.769))     # 3 dp, as match-shots
ch2 <- ch; ch2$event_id[c(1, 3)] <- c(99, 98)                                # 2 of 4 covered shots lost
stopifnot(grepl("matched only 50.0%", join_shot_xg(ch2, lk, "T")$failure))
lk_na <- shot_xg_lookup(transform(sh, xg = NA_real_))                        # found, but xg all empty
stopifnot(grepl("every xg is NA", join_shot_xg(ch, lk_na, "T")$failure))
r <- join_shot_xg(ch[ch$match_id == "m9", ], lk, "T")                        # no overlap: not judged
stopifnot(is.null(r$failure), r$n_covered == 0L)
r <- join_shot_xg(ch[!ch$is_shot, ], lk, "T")                                # no shots at all
stopifnot(is.null(r$failure), "xg" %in% names(r$chains), r$n_shots == 0L)
stopifnot(inherits(try(shot_xg_lookup(rbind(sh, sh[1, ])), silent = TRUE), "try-error"))  # duplicate key
stopifnot(all(is.na(shot_xg_lookup(sh[, c("match_id", "event_id", "xg")])$xgot)))         # no xgot column

cat("PASS: test_chain_seasons.R\n")
