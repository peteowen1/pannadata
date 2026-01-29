#!/usr/bin/env Rscript
#' FBref Daily Scraper
#'
#' Scrapes match data from FBref for all Big 5 leagues, cups, and international
#' competitions. Uses the panna package functions for scraping and storage.
#'
#' Data flow:
#'   FBref website -> panna::scrape_comp_season() -> RDS files
#'   RDS files -> panna::build_all_parquet() -> parquet files
#'   parquet files -> panna::pb_upload_source() -> GitHub Releases
#'
#' Output structure:
#'   data/fbref/{table_type}/{league}/{season}/{match_id}.rds
#'   data/fbref/{table_type}/{league}/{season}.parquet
#'
#' Table types: summary, passing, passing_types, defense, possession,
#'              misc, keeper, shots, events, metadata
#'
#' Usage:
#'   Rscript scrape_fbref.R                    # Scrape current season
#'   Rscript scrape_fbref.R --force            # Force rescrape all
#'   Rscript scrape_fbref.R --upload           # Upload to GitHub after scrape
#'
#' Note: FBref blocks GitHub Actions IPs. Run from Oracle Cloud VM or local machine.

library(panna)

# =============================================================================
# Configuration
# =============================================================================

# Request delay (seconds between FBref requests)
DELAY <- 4

# Table types to scrape
TABLE_TYPES <- c(
  "summary", "passing", "passing_types", "defense",
  "possession", "misc", "keeper", "shots", "events", "metadata"
)

# Current season (updates automatically mid-year)
get_current_season <- function() {
  year <- as.integer(format(Sys.Date(), "%Y"))
  month <- as.integer(format(Sys.Date(), "%m"))
  if (month < 7) {
    paste0(year - 1, "-", year)
  } else {
    paste0(year, "-", year + 1)
  }
}

# National team season (different calendar)
get_current_national_season <- function() {
  year <- as.integer(format(Sys.Date(), "%Y"))
  month <- as.integer(format(Sys.Date(), "%m"))
  if (month < 7) {
    paste0(year - 1, "-", year)
  } else {
    paste0(year, "-", year + 1)
  }
}

# =============================================================================
# Main Scraper Function
# =============================================================================

scrape_fbref <- function(force_rescrape = FALSE, upload_after = FALSE) {
  CURRENT_SEASON <- get_current_season()

  # All competitions to scrape
  CLUB_COMPS <- c(
    list_competitions("league"),
    list_competitions("european"),
    list_competitions("cup")
  )
  NATIONAL_COMPS <- list_competitions("national_team")

  cat("\n", strrep("=", 60), "\n")
  cat("PANNA DAILY FBREF SCRAPE\n")
  cat(strrep("=", 60), "\n\n")
  cat("Mode:", if (force_rescrape) "FORCE RESCRAPE" else "INCREMENTAL", "\n")
  cat("Delay:", DELAY, "seconds\n")
  cat("Current season:", CURRENT_SEASON, "\n")
  cat("Started:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n")

  total_scraped <- 0

  # -------------------------------------------------------------------------
  # Club Competitions
  # -------------------------------------------------------------------------
  cat("*** CLUB COMPETITIONS ***\n")
  for (comp in CLUB_COMPS) {
    cat("\n", strrep("-", 40), "\n")
    cat(comp, "- Season:", CURRENT_SEASON, "\n")
    cat(strrep("-", 40), "\n")

    n <- tryCatch(
      scrape_comp_season(comp, CURRENT_SEASON, TABLE_TYPES, DELAY,
                         force_rescrape = force_rescrape, max_matches = Inf),
      error = function(e) {
        cat("  Error:", e$message, "\n")
        0
      }
    )
    total_scraped <- total_scraped + n
  }

  # -------------------------------------------------------------------------
  # National Team Competitions
  # -------------------------------------------------------------------------
  cat("\n\n*** NATIONAL TEAM COMPETITIONS ***\n")
  for (comp in NATIONAL_COMPS) {
    cat("\n", strrep("-", 40), "\n")
    cat(comp, "\n")
    cat(strrep("-", 40), "\n")

    if (is_tournament_competition(comp) && comp != "NATIONS_LEAGUE") {
      comp_seasons <- tryCatch(
        get_tournament_years(comp),
        error = function(e) character(0)
      )
      comp_seasons <- comp_seasons[as.numeric(comp_seasons) >= 2020]
    } else {
      comp_seasons <- get_current_national_season()
    }

    if (length(comp_seasons) == 0) {
      cat("  No active seasons\n")
      next
    }

    for (season in comp_seasons) {
      cat("  Season:", season, "\n")
      n <- tryCatch(
        scrape_comp_season(comp, season, TABLE_TYPES, DELAY,
                           force_rescrape = force_rescrape, max_matches = Inf),
        error = function(e) {
          cat("    Error:", e$message, "\n")
          0
        }
      )
      total_scraped <- total_scraped + n
    }
  }

  # -------------------------------------------------------------------------
  # Summary
  # -------------------------------------------------------------------------
  cat("\n\n", strrep("=", 60), "\n")
  cat("DAILY FBREF SCRAPE COMPLETE\n")
  cat(strrep("=", 60), "\n")
  cat("\nNew matches scraped:", total_scraped, "\n")
  cat("Finished:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")

  # -------------------------------------------------------------------------
  # Build parquet files
  # -------------------------------------------------------------------------
  if (total_scraped > 0) {
    cat("\nBuilding parquet files from RDS...\n")
    stats <- build_all_parquet(verbose = TRUE)
    cat(sprintf("Built %d parquet files (%.1f MB total)\n",
                nrow(stats), sum(stats$size_mb)))

    cat("\nBuilding consolidated parquet files...\n")
    cons_stats <- build_consolidated_parquet(
      output_dir = file.path(pannadata_dir(), "consolidated"),
      verbose = TRUE
    )
    cat(sprintf("Built %d consolidated files (%.1f MB total)\n",
                nrow(cons_stats), sum(cons_stats$size_mb)))
  }

  # -------------------------------------------------------------------------
  # Upload to GitHub Releases (optional)
  # -------------------------------------------------------------------------
  if (upload_after) {
    cat("\nUploading to GitHub Releases...\n")
    pb_upload_source(
      source_type = "fbref",
      repo = "peteowen1/pannadata",
      source = pannadata_dir(),
      verbose = TRUE
    )
  }

  invisible(total_scraped)
}

# =============================================================================
# CLI Entry Point
# =============================================================================

if (!interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  force_rescrape <- "--force" %in% args
  upload_after <- "--upload" %in% args

  # Set pannadata directory (assumes script is in pannadata/scripts/fbref/)
  script_dir <- dirname(sys.frame(1)$ofile)
  data_dir <- file.path(dirname(dirname(script_dir)), "data")
  pannadata_dir(data_dir)

  scrape_fbref(force_rescrape = force_rescrape, upload_after = upload_after)
}
