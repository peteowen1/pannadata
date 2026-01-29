# Understat Scraper

R-based scraper for Understat match data using the `panna` package.

## Important Note

Unlike FBref, **Understat does not block GitHub Actions**. This scraper runs
daily via GitHub Actions workflow (`.github/workflows/daily-understat-scrape.yml`).

## Files

| File | Description |
|------|-------------|
| `scrape_understat.R` | Main scraper script |

## Dependencies

Requires the `panna` R package:
```r
remotes::install_github("peteowen1/panna@dev")
```

## Usage

```bash
# Auto-detect ID range (continues from last cached ID)
Rscript scrape_understat.R

# Specific ID range
Rscript scrape_understat.R --start 28000 --end 28500

# Force rescrape cached matches
Rscript scrape_understat.R --force

# Scrape and upload to GitHub Releases
Rscript scrape_understat.R --upload
```

## Data Flow

```
Understat website
    ↓ (panna::bulk_scrape_understat)
Parquet files: data/understat/{table}/{league}/{season}.parquet
    ↓ (panna::build_consolidated_understat_parquet)
Consolidated: data/consolidated/understat_{table}.parquet
    ↓ (panna::pb_upload_source)
GitHub Releases: peteowen1/pannadata @ understat-latest
```

## Table Types

| Table | Description |
|-------|-------------|
| roster | Player stats including xGChain, xGBuildup |
| shots | Shot-level data with xG and coordinates |
| metadata | Match info (date, teams, score) |

## Unique Metrics

Understat provides metrics not available from FBref:

| Metric | Description |
|--------|-------------|
| xGChain | xG from all possessions a player was involved in |
| xGBuildup | xG from possessions excluding shot/assist |
| xG | Shot-level expected goals |

## Leagues

| Code | League |
|------|--------|
| ENG | Premier League |
| ESP | La Liga |
| GER | Bundesliga |
| ITA | Serie A |
| FRA | Ligue 1 |
| RUS | Russian Premier League |

## Match IDs

Understat uses sequential integer match IDs (e.g., 28001, 28002...).
The scraper auto-detects the last cached ID and scrapes forward.

Current ID ranges (approximate):
- 2024-25 season: 27000-29000+
- 2023-24 season: 25000-27000
- Earlier seasons: 1-25000

## Rate Limiting

Understat requires 3+ second delays between requests. The scraper handles
this via `bulk_scrape_understat(..., delay = 3)`.
