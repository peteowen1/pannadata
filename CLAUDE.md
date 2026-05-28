# CLAUDE.md — pannadata

Data repository for the pannaverse ecosystem. Stores cached match data from three sources, managed via GitHub Releases (not git-tracked).

## Data Sources

| Source | Scraper | Environment | Schedule | Release Tag |
|--------|---------|-------------|----------|-------------|
| **Opta** ⭐ | Python (`scripts/opta/`) | GitHub Actions | 5 AM UTC daily | `opta-latest` |
| Understat (deprecated) | R (`scripts/understat/`) | (disabled — workflow `.disabled`) | — | `understat-latest` |
| FBref (deprecated) | R (`scripts/fbref/`) | (disabled — workflow `.disabled`; Oracle VM scrape also retired) | — | `fbref-latest` |

As of 2026-04-18 the project consolidated on Opta. Understat and FBref scrape workflows are disabled (`.disabled` extension) and slated for archival; their code remains in the repo for reference but does not run on a schedule. Build new features against Opta only.

## Directory Structure

```
data/
├── opta/
│   ├── events/           # Goal/sub/card events with event_type set (used for splint boundaries)
│   ├── match_events/     # ALL raw events with type_id (used for SPADL conversion AND for type_id == 30 period-end markers consumed by panna's extract_period_end_times())
│   ├── lineups/          # Match lineup data
│   ├── fixtures/         # Fixture/result data
│   ├── shots/            # Shot-level data
│   ├── shot_events/      # Detailed shot events
│   ├── xmetrics/         # xG/xA/xPass per player (from panna pipeline)
│   ├── events_consolidated/  # Merged event files
│   ├── models/           # Legacy model copies (canonical source: pannamodels package)
│   └── opta_*.parquet    # Consolidated player stats, shots, lineups
├── fbref/
│   ├── defense/          # Defensive stats per league/season
│   ├── events/           # Match events (RDS per match)
│   └── metadata/         # League/season metadata
└── understat/
    ├── events/           # Match events (parquet per season)
    ├── metadata/         # League metadata
    ├── roster/           # Player rosters
    └── understat_*.parquet  # Consolidated files
```

**Data is NOT in git** — the `data/` directory is gitignored. All data is stored in GitHub Releases and downloaded via `panna::pb_download_source()`.

## Scripts

### Scraping

```bash
# Opta (Python — usually runs via GHA)
cd scripts/opta && pip install -r requirements.txt
python scrape_opta.py

# Understat (R — usually runs via GHA)
Rscript scripts/understat/scrape_understat.R

# FBref (R — runs on Oracle VM)
Rscript scripts/fbref/scrape_fbref.R
```

### Blog Data

```r
# Build blog data (triggered by panna predictions pipeline via repository_dispatch)
source("scripts/build_blog_data.R")     # Ratings parquet from xRAPM + SPM
source("scripts/build_player_meta.R")   # Player details (player-details.parquet)
source("scripts/build_shot_data.R")     # Shot data for visualizations
source("scripts/build_chains_ci.R")     # Possession chains with EPV equity
```

**Blog deliverables on R2** (`inthegame-data/football/`):

| File | Source | Purpose |
|------|--------|---------|
| `ratings.parquet` | `build_blog_data.R` | Seasonal player ratings |
| `player-details.parquet` | `build_player_meta.R` | Player bio (id, name, team, league, position) |
| `game-logs.parquet` | panna step 10b → `blog-latest` pass-through | Per-match EPV+WPA+PSV value metrics |
| `chains-{CODE}.parquet` | `build_chains_ci.R` + equity join | Possession chains with per-action EPV equity |
| `predictions.parquet` | panna step 10 → `blog-latest` pass-through | Match predictions |

### Data Utilities

```r
# Upload/migration scripts in data-raw/
source("data-raw/upload_to_release.R")           # Upload files to GitHub Release
source("data-raw/upload_clean_parquets.R")        # Clean and upload parquets
source("data-raw/migrate_to_parquet.R")           # Convert legacy formats to parquet
```

## GitHub Actions

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `daily-opta-scrape.yml` | 5 AM UTC | Python Opta scraper → `opta-latest` release |
| `build-blog-data.yml` | `repository_dispatch` | Build blog data → Cloudflare R2 |
| `daily-understat-scrape.yml.disabled` | (disabled) | Understat scraper — retired with FBref/Understat deprecation |
| `daily-fbref-scrape.yml.disabled` | (disabled) | FBref scraper — also retired (Oracle VM scrape gone) |
| `scrape-notification.yml.disabled` | (disabled) | Notification on scrape success/failure |

## Data Bus Pattern

GitHub Releases serve as the data bus between repos:

```
pannadata scrapers → GitHub Releases (opta-latest, fbref-latest, understat-latest)
    ↓
panna::pb_download_source() downloads from releases
    ↓
panna pipelines process → upload to predictions-latest, blog-latest
    ↓
pannadata build-blog-data.yml → Cloudflare R2 (inthegame-data bucket)
    ↓
inthegame-blog reads from R2
```

## Key Files

- `DATA_DICTIONARY.md` — Column definitions for all data sources
- `BLOG_DATA_SETUP.md` — Blog delivery chain (R2 bucket setup)
- `scripts/opta/all_competitions.json` — Opta competition config
- `scripts/opta/README.md` — Opta scraper documentation

## Gotchas

- **FBref match IDs are opaque** — 8-char hex IDs (e.g., `74125d47`) cannot be guessed. Look up from `data/fbref/metadata/` or fbref.com.
- **Opta scraper is Python**, everything else is R — check Python deps separately.
- **`build_blog_data.R` smart join** — auto-detects `player_id` vs `player_name` for joins. Step 10 of panna predictions now exports `player_id`.
- **`build_player_meta.R` must include `player_name`** in output for blog data join to work (even when using `player_id` as primary key).
- **`build_player_meta.R` smart path** — reads from `source/opta_lineups.parquet` (CI) or `data/opta/opta_lineups.parquet` (local). Outputs `blog/player-details.parquet`.
- **Equity join in chains** — `build_chains_ci.R` left-joins `source/action_equity.parquet` (from panna step 10c) onto chains by `match_id + event_id`. ~84% match rate (SPADL merges duels, drops non-gameplay).
- **TOURNAMENT_DATE_EXCEPTIONS is load-bearing** (`scripts/opta/scrape_opta.py`) — when a scrape dispatch returns `Found 0 total, 0 played, 0 new`, the FIRST hypothesis should be a missing date exception, not "Opta gap". Two common patterns trip the default Aug–Jul windowing:
  - **COVID-delayed tournaments** played outside their nominal year (EURO 2020 → 2021, AFCON 2021 Cameroon → Jan-Feb 2022, AFC Asian Cup 2023 Qatar → Jan-Feb 2024)
  - **Shared season names** between a main tournament and its qualifiers (e.g. `"2022 Qatar"` is used for WC 2022 itself AND for CONMEBOL/CAF/AFC WC qualifier cycles that played 2019–2022; the narrow Nov-Dec 2022 window catches main but misses all qualifiers)
  Fix is to add a `(start, end)` tuple to `TOURNAMENT_DATE_EXCEPTIONS` covering the actual play window for both senses. Pre-fix, AFC WC Qualifiers 2014 Brazil returned 12 matches; post-fix it returned 148.
- **discover_seasons.py + merge_discovered_seasons.py workflow** — when adding new comps or backfilling old cycles, run `python discover_seasons.py` (queries Opta tournamentcalendar for every comp in `opta_scraper.py::COMPETITIONS`), then `python merge_discovered_seasons.py` to add new (label → season_id) pairs to the main `seasons.json`. Watch for non-UTF-8 bytes in season names — Opta's API has sent latin-1 / cp1252 encoded responses for accented characters in the past; the merge normalizes apostrophes but check the diff before committing.
