# run_migration.R
# Execute the data migration

# 1. Run Migration ----

source("C:/Users/peteo/OneDrive/Documents/pannaverse/pannadata/data-raw/migrate_data.R")
migrate_all(dry_run = TRUE)
