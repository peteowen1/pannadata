# run_upload.R
# Simple script to upload individual parquets
# Run this in RStudio: source("pannadata/data-raw/run_upload.R")

library(piggyback)
library(arrow)
library(dplyr)

cat("=== Uploading Individual Parquet Files ===\n\n")

# Configuration
repo <- "peteowen1/pannadata"
tag <- "latest"
source_dir <- "C:/Users/peteo/OneDrive/Documents/pannaverse/pannadata/data"

cat("Source directory:", source_dir, "\n")
cat("Repository:", repo, "\n")
cat("Tag:", tag, "\n\n")

# Find all parquet files
parquet_files <- list.files(source_dir, pattern = "\\.parquet$",
                            recursive = TRUE, full.names = TRUE)
cat("Found", length(parquet_files), "parquet files\n\n")

# Get unique table types (first directory level after data/)
get_table_type <- function(f) {
  rel <- sub(paste0(normalizePath(source_dir, winslash = "/"), "/?"), "",
             normalizePath(f, winslash = "/"))
  strsplit(rel, "/")[[1]][1]
}

table_types <- unique(sapply(parquet_files, get_table_type))
cat("Table types:", paste(table_types, collapse = ", "), "\n\n")

# Process each table type
temp_dir <- tempdir()
uploaded_files <- character()

for (tt in table_types) {
  cat("Processing", tt, "...\n")

  # Get files for this table type
  tt_files <- parquet_files[sapply(parquet_files, get_table_type) == tt]
  cat("  Files:", length(tt_files), "\n")

  # Read and combine
  all_data <- lapply(tt_files, function(f) {
    tryCatch(arrow::read_parquet(f), error = function(e) NULL)
  })
  all_data <- all_data[!sapply(all_data, is.null)]

  if (length(all_data) == 0) {
    cat("  Skipping - no valid data\n")
    next
  }

  combined <- dplyr::bind_rows(all_data)
  cat("  Rows:", format(nrow(combined), big.mark = ","), "\n")

  # Write combined parquet
  output_file <- file.path(temp_dir, paste0(tt, ".parquet"))
  arrow::write_parquet(combined, output_file)

  size_mb <- file.size(output_file) / 1024 / 1024
  cat("  Size:", round(size_mb, 2), "MB\n")

  # Upload
  cat("  Uploading...\n")
  tryCatch({
    piggyback::pb_upload(
      file = output_file,
      repo = repo,
      tag = tag,
      overwrite = TRUE
    )
    uploaded_files <- c(uploaded_files, tt)
    cat("  Done!\n\n")
  }, error = function(e) {
    cat("  ERROR:", e$message, "\n\n")
  })
}

cat("\n=== Upload Complete ===\n")
cat("Uploaded:", paste(uploaded_files, collapse = ", "), "\n")
cat("\nUsers can now use:\n")
cat('  load_summary(league = "ENG")  # Downloads only summary.parquet (~5MB)\n')
