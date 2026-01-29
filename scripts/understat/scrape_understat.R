#!/usr/bin/env Rscript
#' Understat Daily Scraper
#'
#' Scrapes match data from Understat using numeric match IDs. Unlike FBref,
#' Understat doesn't block GitHub Actions, so this can run via workflow.
#'
#' Understat data includes:
#'   - xGChain: Expected goals from possession chains
#'   - xGBuildup: Expected goals from buildup play (excluding shot/assist)
#'   - Shot-level xG with coordinates
#'
#' Data flow:
#'   Understat website -> panna::bulk_scrape_understat() -> parquet files
#'   parquet files -> panna::build_consolidated_understat_parquet() -> consolidated
#'   consolidated -> panna::pb_upload_source() -> GitHub Releases
#'
#' Output structure:
#'   data/understat/roster/{league}/{season}.parquet
#'   data/understat/shots/{league}/{season}.parquet
#'   data/understat/metadata/{league}/{season}.parquet
#'
#' Usage:
#'   Rscript scrape_understat.R                          # Auto-detect ID range
#'   Rscript scrape_understat.R --start 28000 --end 28500  # Specific range
#'   Rscript scrape_understat.R --upload                 # Upload after scrape
#'
#' Note: Match IDs are sequential integers. The scraper auto-detects the
#' latest cached ID and scrapes forward from there.

library(panna)

# =============================================================================
# Configuration
# =============================================================================

# Request delay (seconds between Understat requests)
DELAY <- 3

# Leagues covered by Understat
LEAGUES <- c("ENG", "ESP", "GER", "ITA", "FRA", "RUS")

# =============================================================================
# Helper Functions
# =============================================================================

#' Find the maximum cached Understat match ID across all leagues
find_max_cached_id <- function() {
  all_cached_ids <- c()

  for (league in LEAGUES) {
    for (season in 2014:2025) {
      ids <- tryCatch(
        get_cached_understat_ids(league, season),
        error = function(e) character(0)
      )
      all_cached_ids <- c(all_cached_ids, as.integer(ids))
    }
  }

  if (length(all_cached_ids) > 0) {
    max(all_cached_ids, na.rm = TRUE)
  } else {
    NULL
  }
}

# =============================================================================
# Main Scraper Function
# =============================================================================

scrape_understat <- function(start_id = NULL, end_id = NULL,
                              skip_cached = TRUE, upload_after = FALSE) {

  # -------------------------------------------------------------------------
  # Auto-detect ID range if not provided
  # -------------------------------------------------------------------------
  if (is.null(start_id) || is.null(end_id)) {
    max_cached <- find_max_cached_id()

    if (!is.null(max_cached)) {
      start_id <- max_cached + 1
      end_id <- start_id + 499
      message(sprintf("Auto-detected: max cached ID = %d", max_cached))
      message(sprintf("Scraping ID range: %d to %d", start_id, end_id))
    } else {
      # First run - start from a known recent range
      # 2024-25 season IDs are approximately 27000-29000+
      start_id <- 28000
      end_id <- 28499
      message("No cached data found, starting fresh from ID 28000")
    }
  }

  cat("\n", strrep("=", 60), "\n")
  cat("PANNA DAILY UNDERSTAT SCRAPE\n")
  cat(strrep("=", 60), "\n\n")
  cat("ID Range:", start_id, "to", end_id, "\n")
  cat("Skip cached:", skip_cached, "\n")
  cat("Started:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n")

  # -------------------------------------------------------------------------
  # Run bulk scrape
  # -------------------------------------------------------------------------
  results <- bulk_scrape_understat(
    start_id = start_id,
    end_id = end_id,
    delay = DELAY,
    skip_cached = skip_cached,
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
    cat("\nLeague breakdown:\n")
    print(table(results$league[results$status == "success"]))
  }

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
  start_id <- NULL
  end_id <- NULL
  skip_cached <- TRUE
  upload_after <- FALSE

  i <- 1
  while (i <= length(args)) {
    if (args[i] == "--start" && i < length(args)) {
      start_id <- as.integer(args[i + 1])
      i <- i + 2
    } else if (args[i] == "--end" && i < length(args)) {
      end_id <- as.integer(args[i + 1])
      i <- i + 2
    } else if (args[i] == "--force") {
      skip_cached <- FALSE
      i <- i + 1
    } else if (args[i] == "--upload") {
      upload_after <- TRUE
      i <- i + 1
    } else {
      i <- i + 1
    }
  }

  # Set pannadata directory (assumes script is in pannadata/scripts/understat/)
  script_dir <- dirname(sys.frame(1)$ofile)
  data_dir <- file.path(dirname(dirname(script_dir)), "data")
  pannadata_dir(data_dir)

  scrape_understat(
    start_id = start_id,
    end_id = end_id,
    skip_cached = skip_cached,
    upload_after = upload_after
  )
}
