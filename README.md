# pannadata

Data repository for the pannaverse ecosystem. Contains cached football match data from FBref, Opta, and Understat.

## Data Coverage

### FBref (Primary Source)

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

### Opta

| League | Seasons | Columns |
|--------|---------|---------|
| EPL | 2010-2025 | 271 |
| La_Liga | 2010-2025 | 271 |
| Bundesliga | 2010-2025 | 271 |
| Serie_A | 2010-2025 | 271 |
| Ligue_1 | 2010-2025 | 271 |

### Understat

| League | Seasons | Features |
|--------|---------|----------|
| EPL | 2014-2024 | xGChain, xGBuildup |
| La_Liga | 2014-2024 | xGChain, xGBuildup |
| Bundesliga | 2014-2024 | xGChain, xGBuildup |
| Serie_A | 2014-2024 | xGChain, xGBuildup |
| Ligue_1 | 2014-2024 | xGChain, xGBuildup |
| RFPL | 2014-2024 | xGChain, xGBuildup |

## Data Structure

```
data/
├── fbref/
│   ├── {tabletype}/
│   │   └── {league}/
│   │       └── {season}/
│   │           └── {match_id}.rds
│   └── {tabletype}/{league}/{season}.parquet
├── opta/
│   └── {league}/{season}/{match_id}.rds
└── understat/
    └── {tabletype}/{league}/{season}.parquet
```

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
pb_download_source("fbref")
pb_download_source("opta")
pb_download_source("understat")

# Load FBref data
summary <- load_summary("ENG", "2024-2025")
passing <- load_passing("ENG", "2024-2025")
shots <- load_shots()

# Load Opta data
opta_stats <- load_opta_stats("EPL", "2024-2025")

# Load Understat data
roster <- load_understat_roster("EPL", "2024")
```

## Data Storage

- **Local**: `data/` folder (gitignored, too large for git)
- **Remote**: GitHub Releases at tag `latest`
- **Format**: RDS for individual matches, Parquet for bulk storage

## Syncing Data

```r
# Download from GitHub Releases
panna::pb_download_source("fbref")
panna::pb_download_source("opta")
panna::pb_download_source("understat")
panna::pb_download_source("all")  # Download everything

# Upload local data to GitHub Releases
panna::pb_upload_parquet(repo = "peteowen1/pannadata", tag = "latest")
```

## Documentation

See [DATA_DICTIONARY.md](DATA_DICTIONARY.md) for complete column definitions.

## Related

- [panna](https://github.com/peteowen1/panna) - R package for player ratings
- [pannaverse](https://github.com/peteowen1/pannaverse) - Monorepo container
