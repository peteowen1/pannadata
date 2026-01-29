# Pannadata Scraping Scripts

This directory contains scraping scripts for all three data sources used in
the panna rating system.

## Data Sources Overview

| Source | Language | Runs On | Schedule | Coverage |
|--------|----------|---------|----------|----------|
| [FBref](fbref/) | R | Oracle VM | 6 AM UTC | Big 5 + cups + international |
| [Opta](opta/) | Python | GitHub Actions | 5 AM UTC | Big 5 leagues (2010+) |
| [Understat](understat/) | R | GitHub Actions | 7 AM UTC | Big 5 + Russia |

## Why Different Environments?

- **FBref blocks GitHub Actions IPs** - Must run from Oracle Cloud VM or local
- **Opta/Understat don't block** - Can run directly in GitHub Actions

## Quick Start

### FBref (from VM or local)
```bash
cd pannadata/scripts/fbref
Rscript scrape_fbref.R
```

### Opta
```bash
cd pannadata/scripts/opta
pip install -r requirements.txt
python scrape_big5.py
```

### Understat
```bash
cd pannadata/scripts/understat
Rscript scrape_understat.R
```

## Data Output

All scrapers output to `pannadata/data/`:

```
data/
├── fbref/           # FBref data (RDS + parquet)
│   ├── summary/
│   ├── passing/
│   ├── defense/
│   └── ...
├── opta/            # Opta data (parquet only)
│   ├── player_stats/
│   ├── events/
│   ├── lineups/
│   └── ...
├── understat/       # Understat data (parquet only)
│   ├── roster/
│   ├── shots/
│   └── metadata/
└── consolidated/    # Combined files for fast queries
    ├── summary.parquet
    ├── opta_player_stats.parquet
    └── understat_roster.parquet
```

## GitHub Actions Workflows

| Workflow | Source | Schedule |
|----------|--------|----------|
| `daily-fbref-scrape.yml.disabled` | FBref | Disabled (runs on VM) |
| `daily-opta-scrape.yml` | Opta | 5 AM UTC |
| `daily-understat-scrape.yml` | Understat | 7 AM UTC |

## GitHub Releases

Data is uploaded to GitHub Releases for distribution:

| Release Tag | Contents |
|-------------|----------|
| `fbref-latest` | FBref parquet archive |
| `opta-latest` | Opta parquet files |
| `understat-latest` | Understat parquet files |

Download in R:
```r
library(panna)
pb_download_source("fbref")
pb_download_source("opta")
pb_download_source("understat")
```

## Unique Data per Source

| Source | Unique Features |
|--------|-----------------|
| FBref | StatsBomb xG, comprehensive passing, international competitions |
| Opta | 263 columns, progressive carries, set piece details, event x/y coords |
| Understat | xGChain, xGBuildup, Russia league |

## See Also

- [FBref README](fbref/README.md) - FBref scraper details
- [Opta README](opta/README.md) - Opta scraper details
- [Understat README](understat/README.md) - Understat scraper details
- [pannadata CLAUDE.md](../CLAUDE.md) - Repository overview
- [panna package](../../panna/) - R package for data loading and analysis
