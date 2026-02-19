# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`pannadata` is a **data repository** (not an R package) that stores cached football match data for the pannaverse ecosystem. All data functions live in the sibling `panna` package.

## Data Sources

The repository stores data from three sources, each with its own folder structure:

| Source | Folder | Description |
|--------|--------|-------------|
| FBref | `data/fbref/` | Primary source - Big 5 leagues, cups, international |
| Opta | `data/opta/` | 15 leagues (Big 5 + NED/POR/TUR/ENG2/SCO + UCL/UEL/UECL + WC/EURO), 2013+ |
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

### Scraping Scripts

Located in `scripts/`, organized by data source:

```
scripts/
├── fbref/
│   └── scrape_fbref.R        # FBref scraping (runs on VM, not GHA)
├── opta/
│   ├── scrape_opta.py         # Main Opta scraper (called by GHA workflow)
│   ├── opta_scraper.py        # Opta scraping library
│   ├── consolidate_opta.py    # Rebuild consolidated parquets from raw files
│   ├── discover_seasons.py    # Find available seasons for each league
│   └── build_manifest.py      # Build file manifest for validation
└── understat/
    ├── scrape_understat.R     # Main Understat scraper (called by GHA workflow)
    ├── backfill_understat.R   # Historical data backfill
    └── init_backfill.R        # Backfill initialization
```

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
data/opta/{tabletype}/{league}/{season}.parquet           # Opta aggregated season
data/understat/{tabletype}/{league}/{season}.parquet      # Understat aggregated
data/fixtures/{league}/{season}/fixtures.rds              # FBref match schedules (legacy location)
```

FBref table types: `summary`, `passing`, `passing_types`, `defense`, `possession`, `misc`, `keeper`, `shots`, `events`, `metadata`
Opta table types: `events`, `lineups`, `match_events`, `player_stats`, `shot_events`, `shots`, `xmetrics`, `fixtures`

### Consolidation Gotcha

`consolidate_opta.py` reads raw files from `opta/{table_type}/{league}/` and rebuilds consolidated files from scratch. Running it locally after scraping only one league will **destroy** other leagues' data in the consolidated files. Only run the full consolidation in GHA (which downloads all raw files first), or consolidate individual table types with a targeted script.

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

## GitHub Actions

The `daily-fbref-scrape.yml` workflow is currently **disabled** (renamed to `.disabled`). When enabled, it:
- Runs at 6 AM UTC daily
- Downloads existing parquet data from releases
- Runs incremental scrape for current season (all leagues/cups)
- Uses 4-second delay between requests
- Builds parquet from RDS and uploads to releases

To enable: rename `.github/workflows/daily-fbref-scrape.yml.disabled` to `daily-fbref-scrape.yml`

### Active Workflows
- `daily-opta-scrape.yml` - Daily Opta data scrape via GitHub Actions
- `daily-understat-scrape.yml` - Daily Understat data scrape via GitHub Actions
- `scrape-notification.yml` - Triggered on FBref scrape completion from VM

## Column Documentation

See `DATA_DICTIONARY.md` for complete column documentation for all table types.
