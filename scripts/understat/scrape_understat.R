#!/usr/bin/env Rscript
#' Understat Smart Scraper
#'
#' Scrapes match data from Understat using per-league ID tracking. Unlike the
#' old approach that scanned forward from a global max ID, this scraper tracks
#' each league independently to handle Understat's interleaved ID structure.
#'
#' Understat data includes:
#'   - xGChain: Expected goals from possession chains
#'   - xGBuildup: Expected goals from buildup play (excluding shot/assist)
#'   - Shot-level xG with coordinates
#'
#' Data flow:
#'   Understat website -> panna::smart_scrape_understat() -> parquet files
#'   parquet files -> panna::build_consolidated_understat_parquet() -> consolidated
#'   consolidated -> panna::pb_upload_source() -> GitHub Releases
#'
#' Output structure:
#'   data/understat/roster/{league}/{season}.parquet
#'   data/understat/shots/{league}/{season}.parquet
#'   data/understat/metadata/{league}/{season}.parquet
#'   data/understat-manifest.parquet (tracks all scraped matches)
#'
#' Usage:
#'   Rscript scrape_understat.R                       # Auto-detect, default params
#'   Rscript scrape_understat.R --lookback 50         # Look back 50 IDs from max
#'   Rscript scrape_understat.R --max-misses 100      # Stop after 100 consecutive misses
#'   Rscript scrape_understat.R --upload              # Upload after scrape
#'   Rscript scrape_understat.R --rebuild-manifest    # Rebuild manifest from cache
#'
#' Note: Match IDs are interleaved across leagues. Each league occupies a distinct
#' ID band (~200-300 IDs apart). The scraper processes each league independently.

library(panna)

# =============================================================================
# Configuration
# =============================================================================

# Request delay (seconds between Understat requests)
DELAY <- 3

# Leagues covered by Understat (ordered by ID range: lowest to highest)
LEAGUES <- c("RUS", "ENG", "ESP", "FRA", "ITA", "GER")

# =============================================================================
# Main Scraper Function
# =============================================================================

scrape_understat_smart <- function(manifest_path,
                                    lookback = 20,
                                    max_misses = 50,
                                    upload_after = FALSE,
                                    rebuild_manifest = FALSE) {

  # -------------------------------------------------------------------------
  # Build or load manifest
  # -------------------------------------------------------------------------
  if (rebuild_manifest || !file.exists(manifest_path)) {
    cat("Building manifest from existing cache...\n")
    manifest <- build_understat_manifest_from_cache(pannadata_dir())

    if (nrow(manifest) > 0) {
      save_understat_manifest(manifest, manifest_path)
      cat(sprintf("Built manifest with %d matches\n", nrow(manifest)))
      cat("\nLeague breakdown:\n")
      print(table(manifest$league))
    } else {
      cat("No existing data to build manifest from\n")
    }
  } else {
    manifest <- load_understat_manifest(manifest_path)
    cat(sprintf("Loaded existing manifest with %d matches\n", nrow(manifest)))
  }

  # -------------------------------------------------------------------------
  # Run smart scraper
  # -------------------------------------------------------------------------
  cat("\n", strrep("=", 60), "\n")
  cat("PANNA SMART UNDERSTAT SCRAPE\n")
  cat(strrep("=", 60), "\n\n")
  cat("Manifest path:", manifest_path, "\n")
  cat("Lookback:", lookback, "IDs\n")
  cat("Max misses per league:", max_misses, "\n")
  cat("Delay:", DELAY, "seconds\n")
  cat("Started:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n")

  results <- smart_scrape_understat(
    manifest_path = manifest_path,
    leagues = LEAGUES,
    lookback = lookback,
    max_misses = max_misses,
    delay = DELAY,
    verbose = TRUE
  )

  # -------------------------------------------------------------------------
  # Summary
  # -------------------------------------------------------------------------
  cat("\n", strrep("=", 60), "\n")
  cat("SCRAPE SUMMARY\n")
  cat(strrep("=", 60), "\n")
  print(table(results$status))

  if (any(results$status == "success")) {
    cat("\nNew matches by league:\n")
    print(table(results$league[results$status == "success"]))
  }

  # Show manifest stats
  manifest <- load_understat_manifest(manifest_path)
  cat("\nManifest totals by league:\n")
  print(table(manifest$league))

  cat("\nFinished:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")

  # -------------------------------------------------------------------------
  # Build consolidated parquet files
  # -------------------------------------------------------------------------
  understat_dir <- file.path(pannadata_dir(), "understat")
  if (dir.exists(understat_dir)) {
    cat("\nBuilding consolidated Understat parquet files...\n")
    stats <- build_consolidated_understat_parquet(
      output_dir = file.path(pannadata_dir(), "consolidated"),
      verbose = TRUE
    )

    if (nrow(stats) > 0) {
      cat(sprintf("Built %d consolidated files (%.1f MB total)\n",
                  nrow(stats), sum(stats$size_mb)))
    }
  }

  # -------------------------------------------------------------------------
  # Upload to GitHub Releases (optional)
  # -------------------------------------------------------------------------
  if (upload_after && dir.exists(understat_dir)) {
    understat_files <- list.files(
      understat_dir,
      pattern = "\\.parquet$",
      recursive = TRUE
    )

    if (length(understat_files) > 0) {
      cat(sprintf("\nFound %d Understat parquet files\n", length(understat_files)))
      cat("Uploading to GitHub Releases...\n")

      pb_upload_source(
        source_type = "understat",
        repo = "peteowen1/pannadata",
        source = pannadata_dir(),
        verbose = TRUE
      )
    }
  }

  invisible(results)
}

# =============================================================================
# CLI Entry Point
# =============================================================================

if (!interactive()) {
  args <- commandArgs(trailingOnly = TRUE)

  # Parse arguments
  lookback <- 20
  max_misses <- 50
  upload_after <- FALSE
  rebuild_manifest <- FALSE

  i <- 1
  while (i <= length(args)) {
    if (args[i] == "--lookback" && i < length(args)) {
      lookback <- as.integer(args[i + 1])
      i <- i + 2
    } else if (args[i] == "--max-misses" && i < length(args)) {
      max_misses <- as.integer(args[i + 1])
      i <- i + 2
    } else if (args[i] == "--upload") {
      upload_after <- TRUE
      i <- i + 1
    } else if (args[i] == "--rebuild-manifest") {
      rebuild_manifest <- TRUE
      i <- i + 1
    } else {
      i <- i + 1
    }
  }

  # Set pannadata directory (assumes script is in pannadata/scripts/understat/)
  script_dir <- dirname(sys.frame(1)$ofile)
  data_dir <- file.path(dirname(dirname(script_dir)), "data")
  pannadata_dir(data_dir)

  # Manifest path
  manifest_path <- file.path(data_dir, "understat-manifest.parquet")

  scrape_understat_smart(
    manifest_path = manifest_path,
    lookback = lookback,
    max_misses = max_misses,
    upload_after = upload_after,
    rebuild_manifest = rebuild_manifest
  )
}
