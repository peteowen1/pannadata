# migrate_to_parquet.R
# One-time migration: build parquet files from existing RDS data
#
# Run this once to create initial parquet files from your existing
# RDS data cache before the first parquet-based upload.
#
# Usage:
#   setwd("pannadata")
#   source("data-raw/migrate_to_parquet.R")

# Load panna - use dev version if available, otherwise installed
if (file.exists("../panna/DESCRIPTION")) {
  devtools::load_all("../panna")
} else {
  library(panna)
}

cat("=== Migrating pannadata to Parquet Format ===\n\n")

# Ensure we're in the right directory
if (!file.exists("data")) {
  stop("Run this from the pannadata directory (data/ folder not found)")
}

# Set pannadata directory to current location
pannadata_dir(file.path(getwd(), "data"))

# Count existing RDS files
rds_count <- length(list.files("data", pattern = "\\.rds$", recursive = TRUE))
cat(sprintf("Found %d RDS files to process\n\n", rds_count))

# Build all parquet files
cat("Building parquet files (this may take several minutes)...\n\n")
stats <- build_all_parquet(verbose = TRUE)

cat("\n=== Migration Summary ===\n")
cat(sprintf("Parquet files created: %d\n", nrow(stats)))
cat(sprintf("Total parquet size: %.1f MB\n", sum(stats$size_mb)))

# Show breakdown by table type
if (nrow(stats) > 0) {
  cat("\nBy table type:\n")
  by_type <- aggregate(cbind(n_files = n_matches, size_mb = size_mb) ~ table_type,
                       data = stats, FUN = sum)
  print(by_type, row.names = FALSE)
}

cat("\n=== Next Steps ===\n")
cat("1. Review the parquet files in data/{table_type}/{league}/\n")
cat("2. Run: source('data-raw/upload_to_release.R') to upload\n")
cat("3. RDS files remain for incremental updates\n")
