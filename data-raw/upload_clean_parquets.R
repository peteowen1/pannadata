# upload_clean_parquets.R
#
# Uploads CLEANED individual parquet files to GitHub releases.
# Fixes:
# 1. Converts numeric columns to proper types
# 2. Removes junk columns (x, x_2, performance, etc.)
# 3. Standardizes column names across all data

library(piggyback)
library(arrow)
library(dplyr)

cat("=== Uploading CLEANED Individual Parquet Files ===\n\n")

# Configuration
repo <- "peteowen1/pannadata"
tag <- "latest"
source_dir <- "C:/Users/peteo/OneDrive/Documents/pannaverse/pannadata/data"

# ============================================
# Define column specifications for each table type
# ============================================

# Columns to keep and their target types
summary_spec <- list(
  # Keep as character
  character = c("player", "number", "nation", "pos", "age", "team", "match_url",
                "league", "season", "fbref_id"),
  # Convert to numeric
  numeric = c("min", "gls", "ast", "pk", "p_katt", "sh", "so_t", "crd_y", "crd_r",
              "fls", "fld", "off", "crs", "tkl_w", "int", "og", "p_kwon", "p_kcon",
              "touches", "tkl", "blocks", "x_g", "npx_g", "x_ag", "sca", "gca",
              "cmp", "att", "cmp_percent", "prg_p", "carries", "prg_c",
              "att_2", "succ"),
  # Keep as logical
  logical = c("is_home")
)

events_spec <- list(
  character = c("player", "team", "match_url", "league", "season", "fbref_id",
                "event_type", "score"),
  numeric = c("minute", "effective_minute"),
  logical = c("is_home", "is_goal", "is_sub", "is_red_card", "is_yellow_card",
              "is_second_yellow", "is_own_goal", "is_penalty")
)

shots_spec <- list(
  character = c("player", "team", "match_url", "league", "season", "fbref_id",
                "minute", "outcome", "body_part", "situation", "notes"),
  numeric = c("x_g", "distance", "x_coord", "y_coord", "psxg"),
  logical = c("is_home", "is_penalty", "is_own_goal")
)

metadata_spec <- list(
  character = c("match_url", "league", "season", "fbref_id", "home_team", "away_team",
                "date", "time", "venue", "referee", "attendance"),
  numeric = c("home_goals", "away_goals", "home_xg", "away_xg"),
  logical = c()
)

# Keeper spec for goalkeeper-specific columns
keeper_spec <- list(
  character = c("player", "team", "match_url", "league", "season", "fbref_id"),
  numeric = c("min", "so_ta", "ga", "saves", "save_percent", "cs", "ps_att", "p_katt",
              "p_ksv", "p_kmiss", "launched_cmp", "launched_att", "launched_cmp_percent",
              "passes_att", "passes_thr", "passes_launch_percent", "passes_avg_len",
              "goal_kicks_att", "goal_kicks_launch_percent", "goal_kicks_avg_len",
              "opp_crosses", "opp_crosses_stp", "opp_crosses_stp_percent",
              "sweeper_opa", "sweeper_avg_dist", "psxg", "psxg_plus_minus"),
  logical = c("is_home")
)

# Generic spec for other tables
generic_spec <- list(
  character = c("player", "team", "match_url", "league", "season", "fbref_id"),
  numeric = NULL,  # Will convert all non-character, non-logical to numeric
  logical = c("is_home")
)

# ============================================
# Helper functions
# ============================================

#' Clean and convert a data frame
#'
#' IMPORTANT: This function normalizes ALL columns to character first,
#' then applies consistent type conversions. This prevents bind_rows() errors
#' when Arrow auto-parses some files differently than others.
clean_dataframe <- function(df, spec) {
  # Remove junk columns
  junk_pattern <- "^(x|x_\\d+|performance|performance_\\d+|club)$"
  junk_cols <- grep(junk_pattern, names(df), value = TRUE)
  if (length(junk_cols) > 0) {
    df <- df[, !names(df) %in% junk_cols, drop = FALSE]
  }

  # Get all columns to keep
  keep_cols <- unique(c(spec$character, spec$numeric, spec$logical))
  keep_cols <- keep_cols[keep_cols %in% names(df)]

  # Also keep columns that might be useful but not in spec
  extra_cols <- setdiff(names(df), c(keep_cols, junk_cols))

  # Subset to relevant columns
  df <- df[, c(keep_cols, extra_cols), drop = FALSE]

  # CRITICAL FIX: First normalize ALL columns to character to ensure consistency
  # This prevents bind_rows() errors when Arrow auto-parses some files differently
  for (col in names(df)) {
    if (!is.character(df[[col]]) && !is.logical(df[[col]])) {
      df[[col]] <- as.character(df[[col]])
    }
  }

  # Now apply consistent type conversions
  # Convert numeric columns (from character)
  for (col in spec$numeric) {
    if (col %in% names(df)) {
      df[[col]] <- suppressWarnings(as.numeric(df[[col]]))
    }
  }

  # Convert logical columns
  for (col in spec$logical) {
    if (col %in% names(df)) {
      if (!is.logical(df[[col]])) {
        # Handle various logical representations
        val <- tolower(as.character(df[[col]]))
        df[[col]] <- val %in% c("true", "1", "yes")
      }
    }
  }

  # For extra columns, try to convert to numeric if they look numeric
  for (col in extra_cols) {
    if (is.character(df[[col]])) {
      # Check if it looks numeric (most non-NA values are numeric)
      test_vals <- df[[col]][!is.na(df[[col]]) & df[[col]] != ""]
      if (length(test_vals) > 0) {
        numeric_test <- suppressWarnings(as.numeric(test_vals))
        if (sum(!is.na(numeric_test)) / length(test_vals) > 0.8) {
          df[[col]] <- suppressWarnings(as.numeric(df[[col]]))
        }
      }
    }
  }

  df
}

#' Get spec for a table type
get_spec <- function(table_type) {
  switch(table_type,
         "summary" = summary_spec,
         "events" = events_spec,
         "shots" = shots_spec,
         "metadata" = metadata_spec,
         "keeper" = keeper_spec,
         generic_spec)
}

# ============================================
# Process and upload each table type
# ============================================

# Find all table types
table_types <- list.dirs(source_dir, recursive = FALSE, full.names = FALSE)
cat("Table types found:", paste(table_types, collapse = ", "), "\n\n")

temp_dir <- tempdir()
uploaded_files <- character()

for (tt in table_types) {
  cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
  cat("Processing:", tt, "\n")

  tt_dir <- file.path(source_dir, tt)
  parquet_files <- list.files(tt_dir, pattern = "\\.parquet$",
                              recursive = TRUE, full.names = TRUE)

  if (length(parquet_files) == 0) {
    cat("  No parquet files found, skipping\n")
    next
  }

  cat("  Source files:", length(parquet_files), "\n")

  # Get spec for this table type
  spec <- get_spec(tt)

  # Read all parquet files
  all_data <- lapply(parquet_files, function(f) {
    tryCatch({
      df <- arrow::read_parquet(f)
      clean_dataframe(df, spec)
    }, error = function(e) {
      cat("  Error reading", basename(f), ":", e$message, "\n")
      NULL
    })
  })
  all_data <- all_data[!sapply(all_data, is.null)]

  if (length(all_data) == 0) {
    cat("  No valid data after cleaning, skipping\n")
    next
  }

  # Combine
  combined <- dplyr::bind_rows(all_data)
  cat("  Combined rows:", format(nrow(combined), big.mark = ","), "\n")
  cat("  Columns:", ncol(combined), "\n")

  # Show column types
  col_types <- sapply(combined, function(x) class(x)[1])
  type_summary <- table(col_types)
  cat("  Column types:", paste(names(type_summary), "=", type_summary, collapse = ", "), "\n")

  # Write cleaned parquet
  output_file <- file.path(temp_dir, paste0(tt, ".parquet"))
  arrow::write_parquet(combined, output_file)

  size_mb <- file.size(output_file) / 1024 / 1024
  cat("  Output size:", round(size_mb, 2), "MB\n")

  # Upload
  cat("  Uploading to GitHub...\n")
  tryCatch({
    piggyback::pb_upload(
      file = output_file,
      repo = repo,
      tag = tag,
      overwrite = TRUE
    )
    uploaded_files <- c(uploaded_files, tt)
    cat("  ✓ Done!\n\n")
  }, error = function(e) {
    cat("  ✗ ERROR:", e$message, "\n\n")
  })
}

# ============================================
# Summary
# ============================================
cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
cat("=== Upload Complete ===\n")
cat("Uploaded:", paste(uploaded_files, collapse = ", "), "\n\n")

cat("Changes made:\n")
cat("  • Converted numeric columns to proper types\n")
cat("  • Removed junk columns (x, x_2, performance, etc.)\n")
cat("  • Standardized column structure\n\n")

cat("Test with:\n")
cat('  devtools::load_all()\n')
cat('  clear_remote_cache()\n')
cat('  tst <- load_summary()\n')
cat('  str(tst)  # Should show numeric types now\n')
