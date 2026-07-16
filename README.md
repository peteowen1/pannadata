# pannadata

Data repository for the pannaverse ecosystem. Contains cached football match data from Opta
(the sole active source since 2026-04-18; legacy Understat/FBref data stays frozen on its
release tags).

## Data Coverage

### Opta (Primary Source)

15 leagues with ~280 columns per player match, plus event-level data with x/y coordinates.

| League | Opta Code | R Alias | Seasons | Data Types |
|--------|-----------|---------|---------|------------|
| Premier League | EPL | ENG | 2013+ | player_stats, shots, shot_events, events, match_events, lineups, fixtures |
| La Liga | La_Liga | ESP | 2013+ | player_stats, shots, shot_events, events, match_events, lineups, fixtures |
| Bundesliga | Bundesliga | GER | 2013+ | player_stats, shots, shot_events, events, match_events, lineups, fixtures |
| Serie A | Serie_A | ITA | 2013+ | player_stats, shots, shot_events, events, match_events, lineups, fixtures |
| Ligue 1 | Ligue_1 | FRA | 2013+ | player_stats, shots, shot_events, events, match_events, lineups, fixtures |
| Eredivisie | Eredivisie | NED | 2013+ | player_stats, shots, shot_events, events, match_events, lineups, fixtures |
| Primeira Liga | Primeira_Liga | POR | 2013+ | player_stats, shots, shot_events, events, match_events, lineups, fixtures |
| Super Lig | Super_Lig | TUR | 2013+ | player_stats, shots, shot_events, events, match_events, lineups, fixtures |
| Championship | Championship | ENG2 | 2013+ | player_stats, shots, shot_events, events, match_events, lineups, fixtures |
| Scottish Premiership | Scottish_Premiership | SCO | 2019+ | player_stats, shots, shot_events, events, match_events, lineups, fixtures |
| Champions League | UCL | UCL | 2013+ | player_stats, shots, shot_events, events, match_events, lineups, fixtures |
| Europa League | UEL | UEL | 2013+ | player_stats, shots, shot_events, events, match_events, lineups, fixtures |
| Conference League | Conference_League | UECL | 2021+ | player_stats, shots, shot_events, events, match_events, lineups, fixtures |
| World Cup | World_Cup | WC | 2014, 2018 | player_stats, shots, shot_events, events, match_events, lineups, fixtures |
| Euros | UEFA_Euros | EURO | 2016, 2024 | player_stats, shots, shot_events, events, match_events, lineups, fixtures |

> **Note:** The "Opta Code" is used in filesystem paths and raw data. The "R Alias" is the shorthand accepted by `panna` R package functions like `load_opta_stats()`. Both work interchangeably in the R API.

(The full set of scrapable competitions is much larger — see the tiered catalog in
`scripts/opta/competition_metadata.py`.)

## Data Structure

```
data/
└── opta/
    ├── {data_type}/
    │   └── {league}/
    │       └── {season}.parquet            # Per-season parquet files
    ├── fixtures/
    │   └── {league}/
    │       └── {season}.parquet            # Fixture parquets (all match statuses)
    ├── xmetrics/
    │   └── {league}/
    │       └── {season}.parquet            # Pre-computed xG/xA/xPass per player
    ├── models/
    │   ├── xg_model.rds                    # Pre-trained xG model
    │   ├── xpass_model.rds                 # Pre-trained xPass model
    │   └── epv_model.rds                   # Pre-trained EPV model
    ├── events_consolidated/
    │   └── events_{league}.parquet         # Per-league consolidated events
    ├── opta_player_stats.parquet           # Consolidated player stats (all leagues)
    ├── opta_shots.parquet                  # Consolidated shots (all leagues)
    ├── opta_shot_events.parquet            # Consolidated shot events (all leagues)
    ├── opta_events.parquet                 # Consolidated events (all leagues)
    ├── opta_lineups.parquet                # Consolidated lineups (all leagues)
    ├── opta_fixtures.parquet               # Consolidated fixtures (all leagues)
    ├── opta_match_stats.parquet            # Consolidated match stats (from panna pipeline)
    ├── opta_skills.parquet                 # Consolidated skills (from panna pipeline)
    └── opta_xmetrics.parquet               # Consolidated xmetrics (from panna pipeline)
```

### Opta Data Types

| Type | Description | Key Columns |
|------|-------------|-------------|
| `player_stats` | Per-match player statistics | ~280 columns: goals, assists, passes, tackles, etc. |
| `shots` | Shot data per match | shot location, body part, outcome |
| `shot_events` | Individual shots with coordinates | x, y, type_id, body_part, minute |
| `events` | Goals, cards, substitutions | event type, minute, player |
| `match_events` | All events with x/y coordinates | SPADL-ready, used for EPV |
| `lineups` | Starting XI and substitutions | player, position, minutes |
| `fixtures` | Match fixtures and results | date, teams, score, status |

## Usage

This data is accessed via the `panna` package:

```r
library(panna)

# Download / sync data (incremental, asset-level)
pb_download_opta()

# Load Opta data
opta_stats <- load_opta_stats("EPL", "2024-2025")
opta_shots <- load_opta_shots("EPL", "2024-2025")
match_events <- load_opta_match_events("EPL", "2024-2025")
lineups <- load_opta_lineups("EPL", "2024-2025")
fixtures <- load_opta_fixtures("EPL")
xmetrics <- load_opta_xmetrics("EPL", "2024-2025")
```

## Data Storage

- **Local**: `data/` folder (gitignored, too large for git)
- **Remote**: GitHub Releases (tag-based archives)
- **Format**: Parquet throughout (RDS only for model objects)

### GitHub Release Tags

| Release Tag | Contents |
|-------------|----------|
| opta-latest | Consolidated Opta parquets (player_stats, shots, shot_events, events, lineups, fixtures, match_stats, skills, xmetrics, manifest) + per-league events |
| fbref-latest | Frozen legacy FBref parquet archives (scraper retired) |
| understat-latest | Frozen legacy Understat parquet archives (scraper retired) |
| epv-models | Pre-trained xG, xPass, EPV models |
| ratings-data | Seasonal xRAPM + SPM parquets (from panna pipeline) |

> Release `createdAt`/`publishedAt` reflect tag creation, not data freshness — check
> asset-level `updatedAt` (workflows reuse tags and re-upload assets daily).

## Documentation

See [DATA_DICTIONARY.md](DATA_DICTIONARY.md) for complete column definitions.

## Related

- [panna](https://github.com/peteowen1/panna) - R package for player ratings
- [pannaverse](https://github.com/peteowen1/pannaverse) - Monorepo container
