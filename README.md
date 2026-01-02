# pannadata

Data repository for the pannaverse ecosystem. Contains cached football match data from FBref.

## Structure

```
data/
├── {tabletype}/
│   └── {league}/
│       └── {season}/
│           └── {fbref_id}.rds
└── fixtures/
    └── {league}/
        └── {season}/
            └── fixtures.rds
```

## Table Types

- `metadata` - Match info (teams, scores, date)
- `summary` - Player summary stats
- `passing` - Passing statistics
- `passing_types` - Pass type breakdowns
- `defense` - Defensive statistics
- `possession` - Possession statistics
- `misc` - Miscellaneous stats
- `keeper` - Goalkeeper statistics
- `shots` - Shot-level data with xG

## Usage

This data is accessed via the `panna` package:

```r
library(panna)

# Load data
summary <- load_summary("ENG", "2024-2025")
shots <- load_shots()
```

## Data Storage

- **Local**: `data/` folder (gitignored)
- **Remote**: GitHub Releases for sharing/backup

## Syncing Data

```r
# Download from GitHub Releases
panna::pb_download_data()

# Upload local data to GitHub Releases
panna::pb_upload_data()
```
