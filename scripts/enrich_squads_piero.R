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
#      call-ups), recompute piero using the PERSISTED reference constants from
#      piero_reference.json — NEVER the squad pool's own stats (that reproduces
#      the divergence bug). NA if a required metric is missing.
#
# Idempotent: re-running overwrites any existing `piero` column.

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
# One piero per player_id (ratings.parquet is already unique per player, but
# guard against any dup so the join can't fan out the squad rows).
piero_lut <- ratings |>
  select(player_id, piero) |>
  filter(!is.na(player_id)) |>
  group_by(player_id) |> slice(1) |> ungroup()
squads <- left_join(squads, piero_lut, by = "player_id")
n_joined <- sum(!is.na(squads$piero))
cat("Piero join:", n_joined, "/", nrow(squads), "squad players matched from ratings.parquet\n")

# 2. Unmatched squad players (absent from ratings.parquet — rare untracked-league
# call-ups). RESOLVED (was TODO): leave their piero = NA rather than recompute.
# The squad parquet's `panna` is the LATEST-SEASON xrapm (panna step 12 renames
# it), not the career trait the reference constants were calibrated on, so a
# recompute would ship a mildly miscalibrated number for these players. An
# honest NA ("—" on the page) beats a wrong rating — the whole point of this fix
# is trust. Flip RECOMPUTE_UNMATCHED to TRUE only once step 12 carries the
# CAREER panna into wc2026_squads (then the recompute below is exact).
RECOMPUTE_UNMATCHED <- FALSE
unmatched <- which(is.na(squads$piero))
if (RECOMPUTE_UNMATCHED && length(unmatched) > 0 && file.exists(ref_path) &&
    requireNamespace("jsonlite", quietly = TRUE)) {
  ref <- jsonlite::fromJSON(ref_path, simplifyVector = FALSE)
  metric_names <- names(ref$metrics)          # e.g. panna / epr / psr present in ref
  weights <- unlist(ref$weights[metric_names])

  recompute_piero <- function(row) {
    # weighted renormalized z-blend over the metrics PRESENT for this row,
    # using the REFERENCE pool's mean/sd (never the squad pool's).
    acc <- 0; sw <- 0
    for (m in metric_names) {
      v <- suppressWarnings(as.numeric(row[[m]]))
      if (length(v) == 1 && is.finite(v)) {
        mu <- ref$metrics[[m]]$mean; sd <- ref$metrics[[m]]$sd
        z <- (v - mu) / sd
        acc <- acc + weights[[m]] * z
        sw  <- sw  + weights[[m]]
      }
    }
    if (sw <= 0) return(NA_real_)
    blend <- acc / sw
    round(((blend - ref$blend$mean) / ref$blend$sd) * ref$panna$sd + ref$panna$mean, 4)
  }

  # CAVEAT on the recompute path (unmatched squad players ONLY — the common
  # join path above is unaffected and correct):
  # The reference constants were built on ratings.parquet's `panna`, which is the
  # CAREER trait. But panna step 12 (12_export_wc2026_blog.R, ~L218-222) builds
  # the squad parquet's `panna` by renaming the LATEST-SEASON `xrapm` — i.e. a
  # SEASON metric, not the career trait. Feeding that into the career-calibrated
  # reference is a scale mismatch for the handful of recomputed players.
  # TODO(human): decide the right behaviour for untracked-league call-ups —
  # either (a) have step 12 carry the career `panna` into wc2026_squads (then
  # this recompute is exact), or (b) leave their piero = NA rather than ship a
  # mildly miscalibrated number. Until then this recompute is best-effort and
  # only affects players genuinely absent from ratings.parquet.
  needed <- intersect(metric_names, names(squads))
  if (length(needed) == length(metric_names)) {
    for (i in unmatched) {
      squads$piero[i] <- recompute_piero(as.list(squads[i, metric_names, drop = FALSE]))
    }
    n_recomp <- sum(!is.na(squads$piero[unmatched]))
    cat("Piero recompute (reference constants):", n_recomp, "/", length(unmatched),
        "unmatched squad players rated\n")
  } else {
    cat("::warning::squads missing metric column(s) [",
        paste(setdiff(metric_names, names(squads)), collapse = ", "),
        "] — leaving unmatched rows' piero = NA\n")
  }
} else if (length(unmatched) > 0) {
  cat("::warning::", length(unmatched), "unmatched squad players and no",
      ref_path, "/jsonlite — their piero stays NA\n")
}

write_parquet(squads, squads_path)
cat("wc2026_squads.parquet:", sum(!is.na(squads$piero)), "/", nrow(squads),
    "players have piero (written", squads_path, ")\n")
