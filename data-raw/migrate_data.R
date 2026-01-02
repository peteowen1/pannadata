# migrate_data.R
#
# Migrates data from the old panna cache structure to the new pannadata
# hierarchical structure.
#
# Old: panna/data/fbref_matches/{tabletype}/{LEAGUE}_{SEASON}_{ID}.rds
# New: pannadata/data/{tabletype}/{league}/{season}/{id}.rds

# Configuration
OLD_BASE_DIR <- "C:/Users/peteo/OneDrive/Documents/panna/data/fbref_matches"
NEW_BASE_DIR <- "C:/Users/peteo/OneDrive/Documents/pannaverse/pannadata/data"

# Table types to migrate
TABLE_TYPES <- c("metadata", "summary", "passing", "passing_types",
                 "defense", "possession", "misc", "keeper", "shots", "fixtures")

#' Parse old filename format
#'
#' Handles league codes with underscores (e.g., COPA_DEL_REY).
#' Format: {LEAGUE}_{SEASON}_{ID}.rds
#'
#' @param filename Old filename
#' @return List with league, season, fbref_id
parse_old_filename <- function(filename) {
  # Remove .rds extension
  base <- gsub("\\.rds$", "", filename)

  # Split by underscore
  parts <- strsplit(base, "_")[[1]]
  n <- length(parts)

  if (n < 3) {
    return(NULL)
  }

  # Parse from end: last is fbref_id, second-to-last is season
  fbref_id <- parts[n]
  season <- parts[n - 1]

  # Everything before season is league
  league <- paste(parts[1:(n - 2)], collapse = "_")

  list(
    league = league,
    season = season,
    fbref_id = fbref_id
  )
}


#' Migrate a single table type
#'
#' @param table_type Table type to migrate
#' @param old_base Old base directory
#' @param new_base New base directory
#' @param dry_run If TRUE, just report what would be done
#' @return Number of files migrated
migrate_table_type <- function(table_type, old_base, new_base, dry_run = FALSE) {
  old_dir <- file.path(old_base, table_type)

  if (!dir.exists(old_dir)) {
    message(sprintf("  %s: directory not found, skipping", table_type))
    return(0)
  }

  files <- list.files(old_dir, pattern = "\\.rds$")
  n_total <- length(files)

  if (n_total == 0) {
    message(sprintf("  %s: no files found", table_type))
    return(0)
  }

  message(sprintf("  %s: migrating %d files...", table_type, n_total))

  n_migrated <- 0
  n_failed <- 0

  for (i in seq_along(files)) {
    file <- files[i]

    # Parse old filename
    parsed <- parse_old_filename(file)

    if (is.null(parsed)) {
      warning(sprintf("    Could not parse: %s", file))
      n_failed <- n_failed + 1
      next
    }

    # Create new directory
    new_dir <- file.path(new_base, table_type, parsed$league, parsed$season)

    if (!dry_run && !dir.exists(new_dir)) {
      dir.create(new_dir, recursive = TRUE)
    }

    # New filename (just the ID)
    new_file <- file.path(new_dir, paste0(parsed$fbref_id, ".rds"))

    if (!dry_run) {
      # Copy file
      old_path <- file.path(old_dir, file)
      file.copy(old_path, new_file, overwrite = TRUE)
    }

    n_migrated <- n_migrated + 1

    # Progress every 1000 files
    if (i %% 1000 == 0) {
      message(sprintf("    Progress: %d / %d", i, n_total))
    }
  }

  message(sprintf("  %s: migrated %d, failed %d", table_type, n_migrated, n_failed))
  n_migrated
}


#' Run full migration
#'
#' @param dry_run If TRUE, just report what would be done
migrate_all <- function(dry_run = FALSE) {
  if (dry_run) {
    message("=== DRY RUN - No files will be copied ===\n")
  }

  message(sprintf("Migrating from: %s", OLD_BASE_DIR))
  message(sprintf("Migrating to:   %s\n", NEW_BASE_DIR))

  if (!dir.exists(OLD_BASE_DIR)) {
    stop("Old directory does not exist: ", OLD_BASE_DIR)
  }

  if (!dry_run && !dir.exists(NEW_BASE_DIR)) {
    dir.create(NEW_BASE_DIR, recursive = TRUE)
  }

  total_migrated <- 0

  for (tt in TABLE_TYPES) {
    n <- migrate_table_type(tt, OLD_BASE_DIR, NEW_BASE_DIR, dry_run)
    total_migrated <- total_migrated + n
  }

  message(sprintf("\n=== Migration complete: %d files ===", total_migrated))

  invisible(total_migrated)
}


#' Verify migration
#'
#' Compares file counts between old and new directories.
verify_migration <- function() {
  message("Verifying migration...\n")

  for (tt in TABLE_TYPES) {
    old_dir <- file.path(OLD_BASE_DIR, tt)
    new_base <- file.path(NEW_BASE_DIR, tt)

    old_count <- if (dir.exists(old_dir)) {
      length(list.files(old_dir, pattern = "\\.rds$"))
    } else {
      0
    }

    new_count <- if (dir.exists(new_base)) {
      length(list.files(new_base, pattern = "\\.rds$", recursive = TRUE))
    } else {
      0
    }

    status <- if (old_count == new_count) "OK" else "MISMATCH"
    message(sprintf("  %s: old=%d, new=%d [%s]", tt, old_count, new_count, status))
  }
}


# Main execution
if (interactive()) {
  message("=== Data Migration Script ===\n")
  message("This script migrates data from the old panna structure to pannadata.\n")
  message("Run migrate_all(dry_run = TRUE) to preview changes.")
  message("Run migrate_all() to perform the migration.")
  message("Run verify_migration() to check file counts.\n")
} else {
  # If run non-interactively, do a dry run by default
  migrate_all(dry_run = TRUE)
}
