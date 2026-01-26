# upload_individual_parquets.R
#
# Uploads individual parquet files to GitHub releases for efficient remote loading.
# This enables the new DuckDB-based query system to download only what's needed.
#
# Instead of: Download 100MB ZIP -> extract -> filter in R
# Now:        Download 5MB parquet -> SQL filter -> return results

library(piggyback)

#' Upload individual parquet files to GitHub release
#'
#' Finds all parquet files in pannadata and uploads them individually
#' to the GitHub release. This enables efficient per-table downloads.
#'
#' @param repo GitHub repository
#' @param tag Release tag (default: "latest")
#' @param source_dir Source directory containing parquet files
#' @param verbose Print progress messages
upload_individual_parquets <- function(repo = "peteowen1/pannadata",
                                        tag = "latest",
                                        source_dir = NULL,
                                        verbose = TRUE) {

  if (is.null(source_dir)) {
    # Find pannadata directory
    source_dir <- file.path(dirname(getwd()), "pannadata")
    if (!dir.exists(source_dir)) {
      source_dir <- getwd()
    }
  }

  if (verbose) message("Source directory: ", source_dir)

# Find all parquet files organized by table type
  # Structure: {source_dir}/{table_type}/{league}/{season}.parquet
  parquet_files <- list.files(source_dir, pattern = "\\.parquet$",
                               recursive = TRUE, full.names = TRUE)

  if (length(parquet_files) == 0) {
    stop("No parquet files found in ", source_dir)
  }

  if (verbose) message("Found ", length(parquet_files), " parquet files")

  # Group files by table type (first directory level)
  file_info <- lapply(parquet_files, function(f) {
    rel_path <- sub(paste0("^", normalizePath(source_dir, winslash = "/"), "/?"), "",
                    normalizePath(f, winslash = "/"))
    parts <- strsplit(rel_path, "/")[[1]]
    list(
      full_path = f,
      rel_path = rel_path,
      table_type = parts[1],
      size_mb = file.size(f) / 1024 / 1024
    )
  })

  # Get unique table types
  table_types <- unique(sapply(file_info, function(x) x$table_type))
  if (verbose) message("Table types: ", paste(table_types, collapse = ", "))

  # For each table type, combine all league/season parquets into one file
  # This creates: summary.parquet, events.parquet, shots.parquet, etc.
  temp_dir <- tempdir()

  combined_files <- list()

  for (tt in table_types) {
    tt_files <- parquet_files[sapply(file_info, function(x) x$table_type == tt)]

    if (length(tt_files) == 0) next

    if (verbose) message("\nProcessing ", tt, " (", length(tt_files), " files)...")

    # Read and combine all parquet files for this table type
    all_data <- lapply(tt_files, function(f) {
      tryCatch(arrow::read_parquet(f), error = function(e) NULL)
    })
    all_data <- all_data[!sapply(all_data, is.null)]

    if (length(all_data) == 0) {
      if (verbose) message("  No valid data for ", tt)
      next
    }

    combined <- dplyr::bind_rows(all_data)

    # Write combined parquet
    output_file <- file.path(temp_dir, paste0(tt, ".parquet"))
    arrow::write_parquet(combined, output_file)

    size_mb <- file.size(output_file) / 1024 / 1024
    if (verbose) message("  Combined: ", nrow(combined), " rows, ", round(size_mb, 2), " MB")

    combined_files[[tt]] <- output_file
  }

  # Ensure release exists
  if (verbose) message("\nChecking release...")
  tryCatch({
    piggyback::pb_list(repo = repo, tag = tag)
  }, error = function(e) {
    if (verbose) message("Creating new release...")
    piggyback::pb_new_release(repo = repo, tag = tag)
  })

  # Upload each combined parquet file
  if (verbose) message("\nUploading to GitHub release...")

  for (tt in names(combined_files)) {
    f <- combined_files[[tt]]
    if (verbose) message("  Uploading ", basename(f), "...")

    tryCatch({
      piggyback::pb_upload(
        file = f,
        repo = repo,
        tag = tag,
        overwrite = TRUE
      )
    }, error = function(e) {
      warning("Failed to upload ", tt, ": ", e$message)
    })
  }

  if (verbose) {
    message("\nUpload complete!")
    message("Files uploaded: ", paste(names(combined_files), collapse = ", "))
    message("\nUsers can now use:")
    message('  load_summary(league = "ENG")  # Downloads only summary.parquet')
  }

  invisible(combined_files)
}

# Run if executed directly
if (interactive()) {
  message("=== Upload Individual Parquets ===")
  message("This will upload individual parquet files to the GitHub release.")
  message("This enables efficient per-table downloads instead of downloading the entire ZIP.\n")

  response <- readline("Continue? (y/n): ")
  if (tolower(response) == "y") {
    upload_individual_parquets()
  }
}
