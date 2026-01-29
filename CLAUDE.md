# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`pannadata` is a **data repository** (not an R package) that stores cached football match data for the pannaverse ecosystem. All data functions live in the sibling `panna` package.

## Data Sources

The repository stores data from three sources, each with its own folder structure:

| Source | Folder | Description |
|--------|--------|-------------|
| FBref | `data/fbref/` | Primary source - Big 5 leagues, cups, international |
| Opta | `data/opta/` | Big 5 leagues since 2010, 271 columns per player |
| Understat | `data/understat/` | Big 5 + Russia, xGChain/xGBuildup metrics |

## Data Source Scraping

| Source | Where to Scrape | Reason |
|--------|----------------|--------|
| **FBref** | Oracle Cloud VM | FBref blocks GitHub Actions IP addresses |
| **Understat** | GitHub Actions | No IP restrictions |
| **Opta** | GitHub Actions | No IP restrictions |

### FBref Scraping (VM)
FBref blocks requests from GitHub Actions IP ranges. The daily FBref scrape runs on the Oracle Cloud VM via cron job:
- **VM**: `opc@168.138.108.69`
- **Cron**: Runs daily at 6 AM UTC
- **Script**: `/home/opc/scraper/daily_scrape.R`
- **Wrapper**: `/home/opc/scraper/run_scrape.sh`
- **Logs**: `/home/opc/scraper/logs/`
- **Upload target**: `fbref-latest` GitHub Release
- **Upload format**: Zip archive of parquet files via `pb_upload_parquet()`
- **Notification**: Triggers `scrape-notification.yml` workflow on completion

### Understat/Opta Scraping (GitHub Actions)
These sources don't block GitHub Actions and use workflows in `.github/workflows/`.

## Data Storage

- **Local**: `data/` folder (gitignored, too large for git)
- **Remote**: GitHub Releases at `peteowen1/pannadata`
- **Format**: RDS for individual matches, parquet for bulk storage/transfer

### GitHub Releases Structure

| Release Tag | Source | Contents |
|-------------|--------|----------|
| `fbref-latest` | FBref (VM) | Individual parquet files + tar.gz archive |
| `understat-latest` | Understat (GHA) | Parquet files |
| `opta-latest` | Opta (GHA) | Parquet files |

### File Structure

```
data/fbref/{tabletype}/{league}/{season}/{fbref_id}.rds   # FBref individual matches
data/fbref/{tabletype}/{league}/{season}.parquet          # FBref aggregated season
data/opta/{league}/{season}/{opta_id}.rds                 # Opta individual matches
data/understat/{tabletype}/{league}/{season}.parquet      # Understat aggregated
data/fixtures/{league}/{season}/fixtures.rds              # FBref match schedules (legacy location)
```

Table types: `summary`, `passing`, `passing_types`, `defense`, `possession`, `misc`, `keeper`, `shots`, `events`, `metadata`

Leagues: `ENG`, `ESP`, `GER`, `ITA`, `FRA` (Big 5), `UCL`, `UEL` (European), `FA_CUP`, `EFL_CUP`, `COPA_DEL_REY`, `COPPA_ITALIA`, `COUPE_DE_FRANCE`, `DFB_POKAL` (Cups), `WC`, `EURO`, `COPA_AMERICA`, `AFCON`, `ASIAN_CUP`, `GOLD_CUP`, `NATIONS_LEAGUE` (International)

## Common Commands

All commands require the `panna` package:

```r
# Load panna from sibling repo
devtools::load_all("../panna")

# Set data directory (required before data operations)
panna::pannadata_dir(file.path(getwd(), "data"))

# Download all data from GitHub Releases
panna::pb_download_source("fbref")
panna::pb_download_source("understat")
panna::pb_download_source("opta")

# Upload local data to GitHub Releases
panna::pb_upload_parquet(repo = "peteowen1/pannadata", tag = "latest")

# Build parquet files from RDS (aggregates individual match files)
panna::build_all_parquet(verbose = TRUE)
```

### Utility Scripts

Run from pannadata directory:

```r
source("data-raw/migrate_to_parquet.R")   # One-time RDS to parquet conversion
source("data-raw/upload_to_release.R")    # Build parquet + upload to releases
source("data-raw/debug_parquet.R")        # Verify parquet files are correct
```

## FBref Match IDs

**NEVER fabricate or guess FBref match IDs.** The 8-character hex IDs (e.g., `74125d47`) are specific to each match and cannot be derived from team names or dates.

To find valid IDs:
1. Check existing files: `list.files("data/fbref/metadata/ENG/2024-2025/")`
2. Load metadata: `readRDS("data/fbref/metadata/ENG/2024-2025/74125d47.rds")$match_url`
3. Navigate to FBref and copy from URL

## GitHub Actions

The `daily-scrape.yml` workflow is currently **disabled** (renamed to `.disabled`). When enabled, it:
- Runs at 6 AM UTC daily
- Downloads existing parquet data from releases
- Runs incremental scrape for current season (all leagues/cups)
- Uses 4-second delay between requests
- Builds parquet from RDS and uploads to releases

To enable: rename `.github/workflows/daily-scrape.yml.disabled` to `daily-scrape.yml`

## Column Documentation

See `DATA_DICTIONARY.md` for complete column documentation for all table types.
