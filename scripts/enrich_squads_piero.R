#!/usr/bin/env Rscript
# enrich_squads_piero.R — make blog/wc2026_squads.parquet's career ratings
# (panna/offense/defense/epr/psr) and the composite `piero` column match
# blog/ratings.parquet exactly, by sourcing them from there.
#
# Piero (and the canonical career ratings) are computed ONCE in build_blog_data.R
# over the full rated pool in blog/ratings.parquet, with the piero reference
# constants persisted to blog/piero_reference.json. See PIERO-POOL-INDEPENDENCE.md.
#
# wc2026_squads.parquet is built upstream by panna step 12, which fills its
# panna/offense/defense from the LATEST-SEASON xrapm — a different metric than
# ratings.parquet's career trait — and has no access to ratings.parquet at its
# own build time. So the same player showed different Panna/Piero on the WC page
# vs the all-leagues leaderboard. We fix it here, where ratings.parquet exists:
#   1. LEFT JOIN panna/offense/defense/epr/psr + piero from ratings.parquet on
#      player_id and OVERWRITE the squad's values — squad players are the same
#      career-trait players, so the overlap becomes byte-identical for free, and
#      the blog's client-side bridge can be retired.
#   2. For squad players absent from ratings.parquet (rare untracked-league
#      call-ups), their step-12 season ratings are kept and piero is left NA
#      (see the note at the join below for why a recompute is deliberately not
#      done).
#
# Rationale doc: pannaverse/PIERO-POOL-INDEPENDENCE.md (in the parent repo, not
# pannadata). Idempotent: re-running re-sources the same values.

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
# Anchor on `panna` (always present), NOT `piero`: the component CAREER ratings
# are what fix the WC-page divergence, and ratings.parquet may legitimately ship
# the components before `piero` (the blog still computes piero client-side in the
# transition window — the canon_cols intersect below sources piero only if it's
# there).
if (!"panna" %in% names(ratings)) {
  cat("::warning::ratings.parquet has no panna column — skipping squad rating enrichment\n")
  quit(status = 0)
}

# 1. LEFT JOIN the canonical CAREER ratings (panna/offense/defense/epr/psr) AND
# the pool-independent `piero` from ratings.parquet on player_id, and OVERWRITE
# the squad's columns with them for matched players.
#
# Why overwrite, not just attach piero: panna step 12 fills the squad's
# panna/offense/defense from the LATEST-SEASON xrapm — a *different metric* than
# the career trait in ratings.parquet — so the WC page showed a different Panna
# (and therefore Piero) for the same player than the all-leagues leaderboard
# (e.g. Kimmich 0.26/0.30 vs 0.31/0.39). The blog masked this with a client-side
# bridge that re-read ratings.parquet. Sourcing the career values HERE fixes it
# at the data source for the tracked-league majority, so the WC squad columns are
# byte-identical to ratings.parquet and the blog bridge can be retired. See
# pannaverse/PIERO-POOL-INDEPENDENCE.md.
#
# Coerce the join key to character on BOTH sides. wc2026_squads (panna step 12)
# and ratings.parquet (this build) are written by different codebases, so a
# silent int64-vs-string player_id type mismatch would make left_join match 0
# rows and ship all-NA. Character is the safe common type.
squads$player_id  <- as.character(squads$player_id)
ratings$player_id <- as.character(ratings$player_id)

# Canonical columns to source from ratings.parquet (piero always; the component
# career ratings when present). One row per player_id — ratings.parquet is
# already unique, but guard against any dup so the join can't fan out squad rows.
canon_cols <- intersect(c("panna", "offense", "defense", "epr", "psr", "piero"),
                        names(ratings))
canon_lut <- ratings |>
  select(player_id, all_of(canon_cols)) |>
  filter(!is.na(player_id)) |>
  group_by(player_id) |> slice(1) |> ungroup()
# Suffix so we can coalesce against the squad's own season values.
names(canon_lut)[match(canon_cols, names(canon_lut))] <- paste0(canon_cols, ".canon")
squads <- left_join(squads, canon_lut, by = "player_id")

matched  <- squads$player_id %in% canon_lut$player_id
n_joined <- sum(matched)
for (col in canon_cols) {
  cc <- paste0(col, ".canon")
  if (col %in% names(squads)) {
    # prefer the canonical (career) value; keep the squad's own where unmatched
    squads[[col]] <- ifelse(!is.na(squads[[cc]]), squads[[cc]], squads[[col]])
  } else {
    squads[[col]] <- squads[[cc]]   # e.g. piero, which the squad parquet lacks
  }
  squads[[cc]] <- NULL
}
cat(sprintf("Career-rating join: %d / %d squad players matched from ratings.parquet (cols: %s)\n",
            n_joined, nrow(squads), paste(canon_cols, collapse = ", ")))

# Coverage gate — squad players are tracked-league internationals, so the vast
# majority are in ratings.parquet. A near-zero join means player_id drift, not a
# real roster: fail loudly. (The workflow step is continue-on-error, so this
# surfaces as a red step without blocking the rest of the build, and the blog
# falls back to its client-side compute — never wrong data.)
if (n_joined == 0)
  stop("Squad rating join matched 0 rows — player_id type/key drift between wc2026_squads and ratings.parquet")
if (n_joined / nrow(squads) < 0.5)
  warning(sprintf("Squad rating join coverage only %.0f%% (%d/%d) — possible player_id drift",
                  100 * n_joined / nrow(squads), n_joined, nrow(squads)))

# 2. Squad players absent from ratings.parquet (rare untracked-league call-ups)
# keep their step-12 latest-season panna/offense/defense and get piero = NA. We
# deliberately do NOT recompute piero from the reference constants: those are
# calibrated on the CAREER trait, but the unmatched rows still carry season
# xrapm, so a recompute would ship a mildly miscalibrated number. An honest NA
# ("—" on the page) beats a wrong rating.
unmatched <- sum(!matched)
if (unmatched > 0)
  cat(sprintf("::notice::%d squad players absent from ratings.parquet — career ratings left as season values, piero NA\n",
              unmatched))

write_parquet(squads, squads_path)
cat(sprintf("wc2026_squads.parquet: %d / %d players carry canonical career ratings + piero (written %s)\n",
            n_joined, nrow(squads), squads_path))
