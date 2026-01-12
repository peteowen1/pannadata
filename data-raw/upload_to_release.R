# upload_to_release.R
# Upload local pannadata cache to GitHub Releases
#
# Run this once to seed the GitHub Release with your existing data.
# After this, the daily GitHub Actions workflow will update incrementally.
#
# Usage:
#   setwd("pannadata")
#   source("data-raw/upload_to_release.R")

library(piggyback)

cat("=== Upload pannadata to GitHub Releases ===\n\n")

# Configuration
REPO <- "peteowen1/pannadata"
TAG <- "latest"

# Ensure we're in the right directory
if (!file.exists("data")) {

stop("Run this from the pannadata directory (data/ folder not found)")
}

# Create release if it doesn't exist
cat("Checking for existing release...\n")
tryCatch({
pb_list(repo = REPO, tag = TAG)
cat("Release '", TAG, "' already exists\n", sep = "")
}, error = function(e) {
cat("Creating new release '", TAG, "'...\n", sep = "")
pb_new_release(repo = REPO, tag = TAG)
})

# Zip the data directory
zip_file <- "pannadata.zip"
cat("\nZipping data directory...\n")

if (file.exists(zip_file)) {
file.remove(zip_file)
}

zip(zip_file, files = "data", extras = "-r")

zip_size <- file.size(zip_file) / (1024 * 1024)
cat(sprintf("Created %s (%.1f MB)\n", zip_file, zip_size))

# Upload to GitHub Releases
cat("\nUploading to GitHub Releases...\n")
pb_upload(
file = zip_file,
repo = REPO,
tag = TAG,
overwrite = TRUE
)

cat("Upload complete!\n")

# Cleanup
file.remove(zip_file)
cat("Cleaned up local zip file\n")

cat("\n=== Done ===\n")
cat("Your data is now available at:\n")
cat(sprintf("https://github.com/%s/releases/tag/%s\n", REPO, TAG))
