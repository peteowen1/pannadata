# upload_to_release.R
# Upload pannadata parquet files to GitHub Releases
#
# This script builds parquet files from RDS and uploads only parquet.
# RDS files remain local for incremental updates.
#
# Usage:
#   setwd("pannadata")
#   source("data-raw/upload_to_release.R")

# Load panna - use dev version if available, otherwise installed
if (file.exists("../panna/DESCRIPTION")) {
  devtools::load_all("../panna")
} else {
  library(panna)
}

cat("=== Upload pannadata (parquet only) to GitHub Releases ===\n\n")

# Configuration
REPO <- "peteowen1/pannadata"
TAG <- "latest"

# Ensure we're in the right directory
if (!file.exists("data")) {
  stop("Run this from the pannadata directory (data/ folder not found)")
}

# Set pannadata directory
pannadata_dir(file.path(getwd(), "data"))

# Step 1: Build all parquet files from RDS
cat("Building parquet files from RDS...\n\n")
stats <- build_all_parquet(verbose = TRUE)

if (nrow(stats) == 0) {
  stop("No parquet files were created. Check that RDS files exist in data/")
}

cat(sprintf("\nBuilt %d parquet files (%.1f MB total)\n",
            nrow(stats), sum(stats$size_mb)))

# Step 2: Upload parquet files only
cat("\nUploading parquet files to GitHub Releases...\n")
pb_upload_parquet(repo = REPO, tag = TAG, verbose = TRUE)

cat("\n=== Done ===\n")
cat("Your parquet data is now available at:\n")
cat(sprintf("https://github.com/%s/releases/tag/%s\n", REPO, TAG))
