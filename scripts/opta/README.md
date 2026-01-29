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
