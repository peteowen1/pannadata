#!/usr/bin/env Rscript
#' Initialize Understat Backfill
#'
#' Downloads existing data from GitHub Releases and builds the manifest.
#' Run this once before starting the backfill process.
#'
#' Usage:
#'   cd pannaverse
#'   Rscript pannadata/scripts/understat/init_backfill.R

# Load panna from development
devtools::load_all("panna")

# Set pannadata directory
pannadata_dir("pannadata/data")

cat("\n", strrep("=", 60), "\n")
cat("INITIALIZING UNDERSTAT BACKFILL\n")
cat(strrep("=", 60), "\n\n")

cat("Data directory:", pannadata_dir(), "\n\n")

# Step 1: Download existing data
cat("Step 1: Downloading existing Understat data from GitHub Releases...\n")
tryCatch({
  pb_download_source(
    source_type = "understat",
    repo = "peteowen1/pannadata",
    dest = pannadata_dir(),
    verbose = TRUE
  )
  cat("Downloaded existing data.\n\n")
}, error = function(e) {
  cat("No existing data or download failed:", e$message, "\n\n")
})

# Step 2: Build manifest from cached data
manifest_path <- file.path(pannadata_dir(), "understat-manifest.parquet")

cat("Step 2: Building manifest from cached data...\n")
manifest <- build_understat_manifest_from_cache(pannadata_dir())

if (nrow(manifest) > 0) {
  save_understat_manifest(manifest, manifest_path)
  cat(sprintf("Built manifest with %d matches.\n", nrow(manifest)))

  cat("\nLeague breakdown:\n")
  print(table(manifest$league))

  cat("\nSeason breakdown:\n")
  print(table(manifest$season))

  # Show ID ranges
  cat("\nID ranges per league:\n")
  for (lg in unique(manifest$league)) {
    ids <- manifest$match_id[manifest$league == lg]
    cat(sprintf("  %s: %d - %d (n=%d)\n", lg, min(ids), max(ids), length(ids)))
  }
} else {
  cat("No existing data found. Starting fresh.\n")
  # Create empty manifest
  save_understat_manifest(
    data.frame(
      match_id = integer(0),
      league = character(0),
      season = character(0),
      scraped_at = as.POSIXct(character(0)),
      stringsAsFactors = FALSE
    ),
    manifest_path
  )
}

cat("\n", strrep("=", 60), "\n")
cat("INITIALIZATION COMPLETE\n")
cat(strrep("=", 60), "\n\n")

cat("Next steps:\n")
cat("  1. Check backfill status:  Rscript pannadata/scripts/understat/backfill_understat.R --status\n")
cat("  2. Run first chunk:        Rscript pannadata/scripts/understat/backfill_understat.R\n")
cat("  3. Or specific range:      Rscript pannadata/scripts/understat/backfill_understat.R --start 15000 --end 17000\n")
cat("\n")
