#!/usr/bin/env Rscript
#' Understat Historical Backfill Script
#'
#' Backfills missing Understat data for seasons 2020-2024 (IDs ~11000-28000).
#' Designed to run in chunks to avoid timeouts and allow resumption.
#'
#' The full backfill (~17,000 IDs at 3s delay) takes ~14 hours.
#' Recommended: Run in ~2000 ID chunks (about 1.5-2 hours each).
#'
#' Usage:
#'   Rscript backfill_understat.R                    # Auto-detect next chunk
#'   Rscript backfill_understat.R --start 15000 --end 17000  # Specific range
#'   Rscript backfill_understat.R --chunk-size 1000  # Smaller chunks
#'   Rscript backfill_understat.R --status           # Show progress only
#'
#' Progress is tracked in the manifest file, so you can stop and resume anytime.

# Load panna from development
devtools::load_all("panna")

# =============================================================================
# Configuration
# =============================================================================

# Full backfill range (2020-2024 data is roughly in this range)
BACKFILL_START <- 11263   # After Russian 2019 data ends
BACKFILL_END   <- 27999   # Before 2025 data starts

# Default chunk size (IDs per run)
DEFAULT_CHUNK_SIZE <- 2000

# Request delay
DELAY <- 3

# =============================================================================
# Helper Functions
# =============================================================================

#' Get backfill progress from manifest
get_backfill_progress <- function(manifest_path) {
  manifest <- load_understat_manifest(manifest_path)

  if (nrow(manifest) == 0) {
    return(list(
      total_matches = 0,
      backfill_matches = 0,
      min_backfill_id = NA,
      max_backfill_id = NA,
      gaps = list()
    ))
  }

  # Filter to backfill range
  backfill_ids <- manifest$match_id[manifest$match_id >= BACKFILL_START &
                                      manifest$match_id <= BACKFILL_END]

  # Find gaps in coverage
  if (length(backfill_ids) > 0) {
    all_ids <- BACKFILL_START:BACKFILL_END
    missing_ids <- setdiff(all_ids, backfill_ids)

    # Group consecutive missing IDs into ranges
    if (length(missing_ids) > 0) {
      gaps <- list()
      start <- missing_ids[1]
      end <- missing_ids[1]

      for (i in 2:length(missing_ids)) {
        if (missing_ids[i] == end + 1) {
          end <- missing_ids[i]
        } else {
          gaps[[length(gaps) + 1]] <- c(start, end)
          start <- missing_ids[i]
          end <- missing_ids[i]
        }
      }
      gaps[[length(gaps) + 1]] <- c(start, end)
    } else {
      gaps <- list()
    }
  } else {
    gaps <- list(c(BACKFILL_START, BACKFILL_END))
  }

  list(
    total_matches = nrow(manifest),
    backfill_matches = length(backfill_ids),
    min_backfill_id = if (length(backfill_ids) > 0) min(backfill_ids) else NA,
    max_backfill_id = if (length(backfill_ids) > 0) max(backfill_ids) else NA,
    gaps = gaps
  )
}


#' Print backfill status
print_backfill_status <- function(manifest_path) {
  progress <- get_backfill_progress(manifest_path)

  cat("\n", strrep("=", 60), "\n")
  cat("UNDERSTAT BACKFILL STATUS\n")
  cat(strrep("=", 60), "\n\n")

  cat("Backfill range:", BACKFILL_START, "to", BACKFILL_END, "\n")
  cat("Total IDs in range:", BACKFILL_END - BACKFILL_START + 1, "\n\n")

  cat("Manifest stats:\n")
  cat("  Total matches:", progress$total_matches, "\n")
  cat("  Backfill matches:", progress$backfill_matches, "\n")

  if (!is.na(progress$min_backfill_id)) {
    cat("  Min backfill ID:", progress$min_backfill_id, "\n")
    cat("  Max backfill ID:", progress$max_backfill_id, "\n")
  }

  cat("\nGaps in coverage:\n")
  if (length(progress$gaps) == 0) {
    cat("  None! Backfill complete.\n")
  } else if (length(progress$gaps) <= 10) {
    for (gap in progress$gaps) {
      cat(sprintf("  %d - %d (%d IDs)\n", gap[1], gap[2], gap[2] - gap[1] + 1))
    }
  } else {
    # Show first 5 and last 5 gaps
    cat(sprintf("  Found %d gaps. First 5:\n", length(progress$gaps)))
    for (gap in progress$gaps[1:5]) {
      cat(sprintf("    %d - %d (%d IDs)\n", gap[1], gap[2], gap[2] - gap[1] + 1))
    }
    cat("  ...\n  Last 5:\n")
    for (gap in tail(progress$gaps, 5)) {
      cat(sprintf("    %d - %d (%d IDs)\n", gap[1], gap[2], gap[2] - gap[1] + 1))
    }
  }

  # Estimate remaining time
  if (length(progress$gaps) > 0) {
    total_missing <- sum(sapply(progress$gaps, function(g) g[2] - g[1] + 1))
    est_hours <- (total_missing * DELAY) / 3600
    cat(sprintf("\nEstimated time remaining: %.1f hours (%d IDs at %ds delay)\n",
                est_hours, total_missing, DELAY))
  }

  cat("\n")
}


#' Get next chunk to process
get_next_chunk <- function(manifest_path, chunk_size) {
  progress <- get_backfill_progress(manifest_path)

  if (length(progress$gaps) == 0) {
    return(NULL)  # Backfill complete
  }

  # Get the first gap
  first_gap <- progress$gaps[[1]]
  start_id <- first_gap[1]
  end_id <- min(first_gap[2], start_id + chunk_size - 1)

  c(start_id, end_id)
}


# =============================================================================
# Main Backfill Function
# =============================================================================

run_backfill <- function(manifest_path, start_id = NULL, end_id = NULL,
                          chunk_size = DEFAULT_CHUNK_SIZE, status_only = FALSE) {

  # Show status
  print_backfill_status(manifest_path)

  if (status_only) {
    return(invisible(NULL))
  }

  # Determine chunk to process
  if (is.null(start_id) || is.null(end_id)) {
    chunk <- get_next_chunk(manifest_path, chunk_size)
    if (is.null(chunk)) {
      cat("Backfill complete! No more gaps to fill.\n")
      return(invisible(NULL))
    }
    start_id <- chunk[1]
    end_id <- chunk[2]
  }

  # Validate range
  if (start_id > end_id) {
    stop("start_id must be <= end_id")
  }

  n_ids <- end_id - start_id + 1
  est_time <- (n_ids * DELAY) / 60

  cat("\n", strrep("=", 60), "\n")
  cat("STARTING BACKFILL CHUNK\n")
  cat(strrep("=", 60), "\n\n")
  cat("Range:", start_id, "to", end_id, sprintf("(%d IDs)\n", n_ids))
  cat("Estimated time:", sprintf("%.0f minutes\n", est_time))
  cat("Started:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n")

  # Run bulk scrape
  results <- bulk_scrape_understat(
    start_id = start_id,
    end_id = end_id,
    delay = DELAY,
    skip_cached = TRUE,  # Skip if already in cache (from prior runs)
    verbose = TRUE
  )

  # Update manifest with new matches
  manifest <- load_understat_manifest(manifest_path)

  success_results <- results[results$status == "success", ]
  if (nrow(success_results) > 0) {
    new_entries <- data.frame(
      match_id = as.integer(success_results$match_id),
      league = success_results$league,
      season = success_results$season,
      scraped_at = Sys.time(),
      stringsAsFactors = FALSE
    )

    # Add to manifest (avoiding duplicates)
    new_entries <- new_entries[!new_entries$match_id %in% manifest$match_id, ]
    if (nrow(new_entries) > 0) {
      manifest <- rbind(manifest, new_entries)
      save_understat_manifest(manifest, manifest_path)
    }
  }

  # Summary
  cat("\n", strrep("=", 60), "\n")
  cat("CHUNK COMPLETE\n")
  cat(strrep("=", 60), "\n")
  print(table(results$status))

  if (any(results$status == "success")) {
    cat("\nNew matches by league:\n")
    print(table(results$league[results$status == "success"]))

    cat("\nNew matches by season:\n")
    print(table(results$season[results$status == "success"]))
  }

  cat("\nFinished:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")

  # Show updated status
  print_backfill_status(manifest_path)

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
  chunk_size <- DEFAULT_CHUNK_SIZE
  status_only <- FALSE

  i <- 1
  while (i <= length(args)) {
    if (args[i] == "--start" && i < length(args)) {
      start_id <- as.integer(args[i + 1])
      i <- i + 2
    } else if (args[i] == "--end" && i < length(args)) {
      end_id <- as.integer(args[i + 1])
      i <- i + 2
    } else if (args[i] == "--chunk-size" && i < length(args)) {
      chunk_size <- as.integer(args[i + 1])
      i <- i + 2
    } else if (args[i] == "--status") {
      status_only <- TRUE
      i <- i + 1
    } else {
      i <- i + 1
    }
  }

  # Set pannadata directory (assumes running from pannaverse root)
  pannadata_dir("pannadata/data")

  # Manifest path
  manifest_path <- file.path(pannadata_dir(), "understat-manifest.parquet")

  run_backfill(
    manifest_path = manifest_path,
    start_id = start_id,
    end_id = end_id,
    chunk_size = chunk_size,
    status_only = status_only
  )
}
