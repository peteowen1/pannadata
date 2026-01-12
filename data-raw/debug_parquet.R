# debug_parquet.R
# Verify parquet files are correct

library(arrow)
library(dplyr)

cat("=== Parquet Verification ===\n\n")

base_dir <- "data"

# Get all table types
table_types <- list.dirs(base_dir, recursive = FALSE, full.names = FALSE)

results <- list()

for (tt in table_types) {
  cat(sprintf("\n--- %s ---\n", toupper(tt)))

  tt_dir <- file.path(base_dir, tt)
  leagues <- list.dirs(tt_dir, recursive = FALSE, full.names = FALSE)

  for (lg in leagues) {
    lg_dir <- file.path(tt_dir, lg)
    parquet_files <- list.files(lg_dir, pattern = "\\.parquet$", full.names = TRUE)

    for (pq_file in parquet_files) {
      season <- gsub("\\.parquet$", "", basename(pq_file))

      # Read parquet and check
      df <- tryCatch({
        read_parquet(pq_file)
      }, error = function(e) {
        cat(sprintf("  ERROR reading %s/%s: %s\n", lg, season, e$message))
        return(NULL)
      })

      if (is.null(df)) next

      n_rows <- nrow(df)
      cols <- names(df)

      # Check for fbref_id
      if ("fbref_id" %in% cols) {
        n_matches <- length(unique(df$fbref_id))
      } else {
        n_matches <- NA
        cat(sprintf("  WARNING: %s/%s has no fbref_id column!\n", lg, season))
        cat(sprintf("    Columns: %s\n", paste(head(cols, 10), collapse = ", ")))
      }

      file_size <- file.size(pq_file) / 1024  # KB

      results[[length(results) + 1]] <- data.frame(
        table_type = tt,
        league = lg,
        season = season,
        n_rows = n_rows,
        n_matches = n_matches,
        size_kb = round(file_size, 1),
        has_fbref_id = "fbref_id" %in% cols,
        stringsAsFactors = FALSE
      )

      cat(sprintf("  %s/%s: %d rows, %d matches, %.1f KB\n",
                  lg, season, n_rows, n_matches, file_size))
    }
  }
}

# Summary
cat("\n\n=== SUMMARY ===\n")
all_results <- do.call(rbind, results)

# Check for missing fbref_id
missing_id <- all_results[is.na(all_results$n_matches) | !all_results$has_fbref_id, ]
if (nrow(missing_id) > 0) {
  cat("\nParquet files WITHOUT fbref_id:\n")
  print(missing_id[, c("table_type", "league", "season")], row.names = FALSE)
}

# By table type
cat("\nBy table type:\n")
by_type <- all_results %>%
  group_by(table_type) %>%
  summarise(
    n_files = n(),
    total_rows = sum(n_rows),
    total_matches = sum(n_matches, na.rm = TRUE),
    total_mb = round(sum(size_kb) / 1024, 2),
    .groups = "drop"
  )
print(as.data.frame(by_type), row.names = FALSE)

cat("\n\nTotal parquet files:", nrow(all_results), "\n")
cat("Total size:", round(sum(all_results$size_kb) / 1024, 1), "MB\n")
