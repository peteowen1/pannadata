# pannadata

Data repository for the pannaverse ecosystem. Contains cached football match data from Opta, Understat, and FBref.

## Data Coverage

### Opta (Primary Source)

15 leagues with 263 columns per player match, plus event-level data with x/y coordinates.

| League | Opta Code | R Alias | Seasons | Data Types |
|--------|-----------|---------|---------|------------|
| Premier League | EPL | ENG | 2013-2025 | player_stats, shots, shot_events, events, match_events, lineups, fixtures |
| La Liga | La_Liga | ESP | 2013-2025 | player_stats, shots, shot_events, events, match_events, lineups, fixtures |
| Bundesliga | Bundesliga | GER | 2013-2025 | player_stats, shots, shot_events, events, match_events, lineups, fixtures |
| Serie A | Serie_A | ITA | 2013-2025 | player_stats, shots, shot_events, events, match_events, lineups, fixtures |
| Ligue 1 | Ligue_1 | FRA | 2013-2025 | player_stats, shots, shot_events, events, match_events, lineups, fixtures |
| Eredivisie | Eredivisie | NED | 2013-2025 | player_stats, shots, shot_events, events, match_events, lineups, fixtures |
| Primeira Liga | Primeira_Liga | POR | 2013-2025 | player_stats, shots, shot_events, events, match_events, lineups, fixtures |
| Super Lig | Super_Lig | TUR | 2013-2025 | player_stats, shots, shot_events, events, match_events, lineups, fixtures |
| Championship | Championship | ENG2 | 2013-2025 | player_stats, shots, shot_events, events, match_events, lineups, fixtures |
| Scottish Premiership | Scottish_Premiership | SCO | 2019-2025 | player_stats, shots, shot_events, events, match_events, lineups, fixtures |
| Champions League | UCL | UCL | 2013-2025 | player_stats, shots, shot_events, events, match_events, lineups, fixtures |
| Europa League | UEL | UEL | 2013-2025 | player_stats, shots, shot_events, events, match_events, lineups, fixtures |
| Conference League | Conference_League | UECL | 2021-2025 | player_stats, shots, shot_events, events, match_events, lineups, fixtures |
| World Cup | World_Cup | WC | 2014, 2018 | player_stats, shots, shot_events, events, match_events, lineups, fixtures |
| Euros | UEFA_Euros | EURO | 2016, 2024 | player_stats, shots, shot_events, events, match_events, lineups, fixtures |

> **Note:** The "Opta Code" is used in filesystem paths and raw data. The "R Alias" is the shorthand accepted by `panna` R package functions like `load_opta_stats()`. Both work interchangeably in the R API.

### Understat

| League | Seasons | Features |
|--------|---------|----------|
| EPL | 2014-2024 | xGChain, xGBuildup |
| La_Liga | 2014-2024 | xGChain, xGBuildup |
| Bundesliga | 2014-2024 | xGChain, xGBuildup |
| Serie_A | 2014-2024 | xGChain, xGBuildup |
| Ligue_1 | 2014-2024 | xGChain, xGBuildup |
| RFPL | 2014-2024 | xGChain, xGBuildup |

### FBref

| League | Seasons | xG Model |
|--------|---------|----------|
| Premier League (ENG) | 2017-2025 | StatsBomb |
| La Liga (ESP) | 2017-2025 | StatsBomb |
| Bundesliga (GER) | 2017-2025 | StatsBomb |
| Serie A (ITA) | 2017-2025 | StatsBomb |
| Ligue 1 (FRA) | 2017-2025 | StatsBomb |
| Champions League (UCL) | 2017-2025 | StatsBomb |
| Europa League (UEL) | 2017-2025 | StatsBomb |
| Domestic Cups | Various | StatsBomb |
| International | Various | StatsBomb |

## Data Structure

```
data/
├── opta/
│   ├── {data_type}/
│   │   └── {league}/
│   │       └── {season}.parquet            # Per-season parquet files
│   ├── fixtures/
│   │   └── {league}/
│   │       └── {season}.parquet            # Fixture parquets (all match statuses)
│   ├── xmetrics/
│   │   └── {league}/
│   │       └── {season}.parquet            # Pre-computed xG/xA/xPass per player
│   ├── models/
│   │   ├── xg_model.rds                    # Pre-trained xG model
│   │   ├── xpass_model.rds                 # Pre-trained xPass model
│   │   └── epv_model.rds                   # Pre-trained EPV model
│   ├── opta_player_stats.parquet           # Consolidated player stats (all leagues)
│   ├── opta_shots.parquet                  # Consolidated shots (all leagues)
│   └── opta_fixtures.parquet               # Consolidated fixtures (all leagues)
├── understat/
│   └── {tabletype}/{league}/{season}.parquet
├── fbref/
│   ├── {tabletype}/
│   │   └── {league}/
│   │       └── {season}/
│   │           └── {match_id}.rds          # Individual match files
│   └── {tabletype}/{league}/{season}.parquet
└── metadata/
    └── {league}/
        └── {season}/                       # Match metadata
```

### Opta Data Types

| Type | Description | Key Columns |
|------|-------------|-------------|
| `player_stats` | Per-match player statistics | 263 columns: goals, assists, passes, tackles, etc. |
| `shots` | Shot data per match | shot location, body part, outcome |
| `shot_events` | Individual shots with coordinates | x, y, xG, player, minute |
| `events` | Goals, cards, substitutions | event type, minute, player |
| `match_events` | All events with x/y coordinates | SPADL-ready, used for EPV |
| `lineups` | Starting XI and substitutions | player, position, minutes |
| `fixtures` | Match fixtures and results | date, teams, score, status |

### FBref Table Types

| Table | Description |
|-------|-------------|
| `summary` | Player summary stats (goals, assists, xG) |
| `passing` | Passing by distance and type |
| `defense` | Tackles, blocks, interceptions |
| `possession` | Touches, carries, take-ons |
| `keeper` | Goalkeeper statistics |
| `misc` | Fouls, aerials, recoveries |
| `shots` | Shot-level data with xG |
| `metadata` | Match info (teams, scores, date) |

## Usage

This data is accessed via the `panna` package:

```r
library(panna)

# Download data (first time)
pb_download_source("opta")
pb_download_source("understat")
pb_download_source("fbref")

# Load Opta data (primary)
opta_stats <- load_opta_stats("EPL", "2024-2025")
opta_shots <- load_opta_shots("EPL", "2024-2025")
match_events <- load_opta_match_events("EPL", "2024-2025")
lineups <- load_opta_lineups("EPL", "2024-2025")
fixtures <- load_opta_fixtures("EPL")
xmetrics <- load_opta_xmetrics("EPL", "2024-2025")

# Load Understat data
roster <- load_understat_roster("ENG", "2024")

# Load FBref data
summary <- load_summary("ENG", "2024-2025")
passing <- load_passing("ENG", "2024-2025")
shots <- load_shots()
```

## Data Storage

- **Local**: `data/` folder (gitignored, too large for git)
- **Remote**: GitHub Releases (tag-based archives)
- **Format**: RDS for individual matches, Parquet for bulk storage and consolidated files

### GitHub Release Tags

| Release Tag | Contents |
|-------------|----------|
| opta-latest | Consolidated Opta files (player_stats, shots, fixtures) |
| fbref-latest | FBref parquet archives |
| understat-latest | Understat parquet archives |
| epv-models | Pre-trained xG, xPass, EPV models |

## Syncing Data

```r
# Download from GitHub Releases
panna::pb_download_source("opta")      # Download Opta data
panna::pb_download_source("understat") # Download Understat data
panna::pb_download_source("fbref")     # Download FBref data
panna::pb_download_source("all")       # Download everything

# Upload local data to GitHub Releases
panna::pb_upload_parquet(repo = "peteowen1/pannadata", tag = "latest")
```

## Documentation

See [DATA_DICTIONARY.md](DATA_DICTIONARY.md) for complete column definitions.

## Related

- [panna](https://github.com/peteowen1/panna) - R package for player ratings
- [pannaverse](https://github.com/peteowen1/pannaverse) - Monorepo container
