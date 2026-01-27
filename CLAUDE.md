# CLAUDE.md

## Overview

`pannadata` is a **data repository** (not an R package). It stores cached FBref match data for the pannaverse ecosystem.

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
- **Uploads**: Results pushed to `latest` GitHub Release

### Understat/Opta Scraping (GitHub Actions)
These sources don't block GitHub Actions and use workflows in `.github/workflows/`.

## Structure

```
pannadata/
├── README.md
├── .gitignore        # Ignores data/ folder
├── data-raw/
│   └── migrate_data.R
└── data/             # Gitignored - stored in GitHub Releases
    └── {tabletype}/{league}/{season}/{id}.rds
```

## Key Points

- **No R code** - all functions are in the `panna` package
- **Data is gitignored** - too large for git, use GitHub Releases
- **Hierarchical structure** - `data/{tabletype}/{league}/{season}/{id}.rds`

## Syncing with GitHub Releases

Use `panna` package functions:
```r
panna::pb_download_data()  # Download from releases
panna::pb_upload_data()    # Upload to releases
```
