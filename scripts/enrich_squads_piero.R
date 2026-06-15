#!/usr/bin/env Rscript
# enrich_squads_piero.R — add the pool-independent `piero` column to
# blog/wc2026_squads.parquet.
#
# Piero is computed ONCE in build_blog_data.R over the canonical reference
# population (the full rated pool in blog/ratings.parquet) and persisted there
# as a `piero` column plus blog/piero_reference.json (the reference constants).
# See PIERO-POOL-INDEPENDENCE.md.
#
# wc2026_squads.parquet is built upstream by panna step 12 and arrives here as a
# pass-through file (it has no access to ratings.parquet at its own build time),
# so we attach piero here, where ratings.parquet exists. Two paths, preferred
# first:
#   1. LEFT JOIN piero from ratings.parquet on player_id — the common case;
#      squad players are the same career-trait players, so the overlap gets the
#      IDENTICAL number for free.
#   2. For squad players absent from ratings.parquet (rare untracked-league
#      call-ups), piero is left NA (see the note at the join below for why a
#      recompute is deliberately not done).
#
# Rationale doc: pannaverse/PIERO-POOL-INDEPENDENCE.md (in the parent repo, not
# pannadata). Idempotent: re-running overwrites any existing `piero` column.

suppressPackageStartupMessages({ library(arrow); library(dplyr) })

squads_path  <- "blog/wc2026_squads.parquet"
ratings_path <- "blog/ratings.parquet"
ref_path     <- "blog/piero_reference.json"

if (!file.exists(squads_path)) {
  cat("::notice::", squads_path, "not present — skipping Piero enrichment\n")
  quit(status = 0)
}
if (!file.exists(ratings_path)) {
  cat("::warning::", ratings_path, "not present — cannot attach Piero to squads\n")
  quit(status = 0)
}

squads  <- as.data.frame(read_parquet(squads_path))
ratings <- as.data.frame(read_parquet(ratings_path))

if (!"player_id" %in% names(squads) || !"player_id" %in% names(ratings)) {
  cat("::warning::player_id missing from squads or ratings — skipping Piero enrichment\n")
  quit(status = 0)
}
if (!"piero" %in% names(ratings)) {
  cat("::warning::ratings.parquet has no piero column (old build?) — skipping Piero enrichment\n")
  quit(status = 0)
}

# 1. LEFT JOIN piero from ratings on player_id (identical numbers for the overlap).
squads$piero <- NULL  # drop any stale column so the join is the single source
# Coerce the join key to character on BOTH sides. wc2026_squads (panna step 12)
# and ratings.parquet (this build) are written by different codebases, so a
# silent int64-vs-string player_id type mismatch would make left_join match 0
# rows and ship an all-NA piero. Character is the safe common type.
squads$player_id  <- as.character(squads$player_id)
ratings$player_id <- as.character(ratings$player_id)
# One piero per player_id (ratings.parquet is already unique per player, but
# guard against any dup so the join can't fan out the squad rows).
piero_lut <- ratings |>
  select(player_id, piero) |>
  filter(!is.na(player_id)) |>
  group_by(player_id) |> slice(1) |> ungroup()
squads <- left_join(squads, piero_lut, by = "player_id")
n_joined <- sum(!is.na(squads$piero))
cat("Piero join:", n_joined, "/", nrow(squads), "squad players matched from ratings.parquet\n")
# Coverage gate — squad players are tracked-league internationals, so the vast
# majority are in ratings.parquet. A near-zero join means player_id drift, not a
# real roster: fail loudly. (The workflow step is continue-on-error, so this
# surfaces as a red step without blocking the rest of the build, and the blog
# falls back to its client-side compute — never wrong data.)
if (n_joined == 0)
  stop("Piero squad join matched 0 rows — player_id type/key drift between wc2026_squads and ratings.parquet")
if (n_joined / nrow(squads) < 0.5)
  warning(sprintf("Piero squad join coverage only %.0f%% (%d/%d) — possible player_id drift",
                  100 * n_joined / nrow(squads), n_joined, nrow(squads)))

# 2. Squad players absent from ratings.parquet (rare untracked-league call-ups)
# keep piero = NA. We deliberately do NOT recompute from the reference constants:
# the squad parquet's `panna` is the LATEST-SEASON xrapm (renamed by panna step
# 12's 12_export_wc2026_blog.R), not the career trait the constants were
# calibrated on, so a recompute would ship a mildly miscalibrated number. An
# honest NA ("—" on the page) beats a wrong rating. If step 12 is later changed
# to carry the CAREER panna into wc2026_squads, an exact recompute (z-blend over
# the persisted reference mean/sd, NEVER the squad pool's own stats) can be
# reinstated here.
unmatched <- which(is.na(squads$piero))
if (length(unmatched) > 0)
  cat(sprintf("::notice::%d squad players absent from ratings.parquet — piero left NA\n",
              length(unmatched)))

write_parquet(squads, squads_path)
cat("wc2026_squads.parquet:", sum(!is.na(squads$piero)), "/", nrow(squads),
    "players have piero (written", squads_path, ")\n")
