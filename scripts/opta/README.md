# Opta Scraper

Python-based scraper for TheAnalyst/Opta match data.

## Important Note

**Opta does not block GitHub Actions.** This scraper runs daily via GitHub
Actions workflow (`.github/workflows/daily-opta-scrape.yml`).

## Files

| File | Description |
|------|-------------|
| `opta_scraper.py` | Main scraper class with API endpoints and data extraction |
| `scrape_big5.py` | CLI script to scrape Big 5 leagues |
| `consolidate_opta.py` | Consolidate season parquets into single files |
| `discover_seasons.py` | Discover available season IDs from API |
| `seasons.json` | Cache of known season IDs |
| `requirements.txt` | Python dependencies |

## Dependencies

```bash
pip install -r requirements.txt
```

Requirements:
- requests>=2.28.0
- pandas>=2.0.0
- pyarrow>=14.0.0

## Usage

```bash
# Scrape current season for all Big 5 leagues
python scrape_big5.py

# Specific leagues
python scrape_big5.py --leagues EPL La_Liga

# Specific seasons
python scrape_big5.py --seasons 2024-2025 2023-2024

# Multiple recent seasons
python scrape_big5.py --recent 3

# Force rescrape existing matches
python scrape_big5.py --force

# Discover available seasons
python discover_seasons.py

# Consolidate parquet files
python consolidate_opta.py
```

## Data Flow

```
TheAnalyst API
    ↓ (opta_scraper.py)
Raw JSON: scripts/opta/data/raw/{league}/{season}/{match_id}_stats.json
    ↓ (scrape_big5.py)
Parquet files: data/opta/{table}/{league}/{season}.parquet
    ↓ (consolidate_opta.py)
Consolidated: data/consolidated/opta_{table}.parquet
    ↓ (GitHub Actions)
GitHub Releases: peteowen1/pannadata @ opta-latest
```

## Table Types

| Table | Description |
|-------|-------------|
| player_stats | 263 columns per player-match (comprehensive box score) |
| shots | Aggregated shot data per player |
| shot_events | Individual shots with x/y coordinates |
| events | Goals, cards, substitutions with timing |
| lineups | Starting XI, subs, minutes played |

### Fields added under issue #192

- `lineups` and `player_stats`: `sub_position`, `captain`, `known_name`, `short_first_name`, `short_last_name`; `lineups` also `team_average_age`, `team_kit_json`, `team_official_json`.
- `events`: `sub_reason` (Injury / Tactical) on substitutions.
- The possession feed (`OptaScraper.get_possession()`) is still not called by the scraper: no real payload could be fetched when this was done, so no parser was shipped (see the issue #192 thread).

### Backfill plan for past matches (nothing run yet)

The daily scrape only processes matches not in the manifest, so the new lineup/`player_stats`/`events` columns are null for past matches until they are re-fetched. They all come from the `matchstats` feed, so the cheapest route is a small script modelled on `rebuild_events.py` that refetches only `matchstats` (1 call a match). Sized from `opta-manifest.parquet` (266,921 matches; 252,303 with lineups) at the scraper's 1-2 s pause per call (~1.5 s average):

| Scope | Matches | Calls | Time |
|-------|---------|-------|------|
| matchstats only, season start >= 2025 | 32,592 | 32.6k | ~14 h |
| matchstats only, >= 2023 | 67,682 | 67.7k | ~28 h |
| matchstats only, >= 2020 | 117,153 | 117k | ~49 h |
| `--force` re-scrape (stats + events), >= 2025 | 32,592 | 65k | ~27 h |

Suggested start: the current season, split by league across several runs. `lineups`/`player_stats` merge by `(match_id, player_id)` so re-fetched rows overwrite and old rows keep null in the new columns; `events` rows for a re-scraped match are replaced by the consolidator.

## Key Columns (player_stats)

### Shooting
- goals, totalScoringAtt, ontargetScoringAtt
- bigChanceScored, bigChanceMissed, bigChanceCreated

### Passing
- passSuccess, passAccuracy, progressivePass
- keyPass, finalThirdPass, longPassAccuracy

### Possession
- touches, touchesInBox, carries
- progressiveCarries, successfulDribbles

### Defense
- tackles, interceptions, blocks
- aerialWon, aerialLost, duelWon

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `/soccerdata/match/{provider_id}` | Match list for a season |
| `/soccerdata/matchstats/{provider_id}/{match_id}` | Player stats, lineups |
| `/soccerdata/matchevent/{provider_id}/{match_id}` | Event-level data |
| `/soccerdata/tournamentcalendar/{provider_id}` | Available seasons |

## Leagues

| Code | League | Competition ID |
|------|--------|----------------|
| EPL | Premier League | 2kwbbcootiqqgmrzs6o5inle5 |
| La_Liga | La Liga | 34pl8szyvrbwcmfkuocjm3r6t |
| Bundesliga | Bundesliga | 6by3h89i2eykc341oz7lv1ddd |
| Serie_A | Serie A | 1r097lpxe0xn03ihb7wi98kao |
| Ligue_1 | Ligue 1 | dm5ka0os1e3dxcp3vh05kmp33 |

## Rate Limiting

TheAnalyst API requires 1+ second delays between requests. The scraper
handles this via `OptaScraper._rate_limit(min_delay=1.0)`.

## Note on xG

TheAnalyst displays xG values on their website, but these are calculated
client-side using a model. The raw API only provides shot coordinates
and outcomes - no xG values. Use shot_events x/y coordinates to build
your own xG model if needed.
