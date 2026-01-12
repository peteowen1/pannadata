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

# Zip the data directory using PowerShell (Windows) or system zip (Unix)
zip_file <- "pannadata.zip"
cat("\nZipping data directory (this may take a minute)...\n")

if (file.exists(zip_file)) {
  file.remove(zip_file)
}

# Use PowerShell on Windows, system zip on Unix
if (.Platform$OS.type == "windows") {
  # PowerShell Compress-Archive is quieter and more reliable on Windows
  ps_cmd <- sprintf(
    'Compress-Archive -Path "data" -DestinationPath "%s" -Force',
    zip_file
  )
  result <- system2("powershell", args = c("-Command", ps_cmd), stdout = TRUE, stderr = TRUE)
} else {
  # Unix: use zip with quiet flag
  result <- system2("zip", args = c("-rq", zip_file, "data"), stdout = TRUE, stderr = TRUE)
}

if (!file.exists(zip_file)) {
  stop("Failed to create zip file. Error: ", paste(result, collapse = "\n"))
}

zip_size <- file.size(zip_file) / (1024 * 1024)
cat(sprintf("Created %s (%.1f MB)\n", zip_file, zip_size))

# Upload to GitHub Releases
cat("\nUploading to GitHub Releases (this may take a few minutes)...\n")
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
