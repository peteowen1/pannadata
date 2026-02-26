# Pannadata Scraping Scripts

This directory contains scraping scripts for all three data sources used in
the panna rating system.

## Data Sources Overview

| Source | Language | Runs On | Schedule | Coverage |
|--------|----------|---------|----------|----------|
| [Opta](opta/) | Python | GitHub Actions | 5 AM UTC | 15 leagues (2013+) |
| [Understat](understat/) | R | GitHub Actions | 7 AM UTC | Big 5 + Russia |
| [FBref](fbref/) | R | Oracle VM | 6 AM UTC | Big 5 + cups + international |

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
python scrape_opta.py
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
├── opta/            # Opta data (parquet per season + consolidated)
│   ├── player_stats/
│   ├── events/
│   ├── lineups/
│   ├── opta_player_stats.parquet   # Consolidated (9 types total:
│   ├── opta_shots.parquet          #   player_stats, shots, shot_events,
│   ├── opta_fixtures.parquet       #   events, lineups, fixtures,
│   ├── opta_match_stats.parquet    #   match_stats, skills, xmetrics)
│   └── opta_*.parquet              # All uploaded to opta-latest release
├── understat/       # Understat data (parquet only)
│   ├── roster/
│   ├── shots/
│   ├── metadata/
│   ├── understat_roster.parquet    # Consolidated
│   └── understat_shots.parquet     # Consolidated
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
- [pannadata README](../README.md) - Repository overview
- [panna package](../../panna/) - R package for data loading and analysis
