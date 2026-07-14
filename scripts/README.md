# Pannadata Scraping Scripts

Scraping and data-build scripts for the panna rating system. **Opta is the only active
data source** (consolidated 2026-04-18). The retired FBref/Understat scrapers were removed
2026-07-14 — recover from git history if ever needed; their last published data remains on
the `fbref-latest` / `understat-latest` releases.

## Opta Scraper

| Language | Runs On | Schedule | Coverage |
|----------|---------|----------|----------|
| Python ([opta/](opta/)) | GitHub Actions (`daily-opta-scrape.yml`) | 5 AM UTC daily | Tiered league catalog (2013+) |

```bash
cd pannadata/scripts/opta
pip install -r requirements.txt
python scrape_opta.py
```

## Data Output

The scraper writes to `pannadata/data/opta/` (gitignored) — per-season parquet files plus
consolidated `opta_*.parquet` files (player_stats, shots, shot_events, events, lineups,
fixtures, match_stats, skills, xmetrics) — all uploaded to the `opta-latest` release.

Download in R:
```r
library(panna)
pb_download_opta()   # incremental sync from opta-latest
```

## See Also

- [Opta README](opta/README.md) - Opta scraper details
- [pannadata README](../README.md) - Repository overview
- `../CLAUDE.md` - data-bus gotchas (event backfills, coverage gates, manifest healing)
- [panna package](../../panna/) - R package for data loading and analysis
