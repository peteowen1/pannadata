# FBref Scraper

R-based scraper for FBref match data using the `panna` package.

## Important Note

**FBref blocks GitHub Actions IP addresses.** This scraper must be run from:
- Oracle Cloud VM (cron job at 6 AM UTC)
- Local development machine

## Files

| File | Description |
|------|-------------|
| `scrape_fbref.R` | Main scraper script |

## Dependencies

Requires the `panna` R package:
```r
remotes::install_github("peteowen1/panna@dev")
```

## Usage

```bash
# Incremental scrape (only new matches)
Rscript scrape_fbref.R

# Force rescrape all matches
Rscript scrape_fbref.R --force

# Scrape and upload to GitHub Releases
Rscript scrape_fbref.R --upload
```

## Data Flow

```
FBref website
    ↓ (panna::scrape_comp_season)
RDS files: data/fbref/{table}/{league}/{season}/{match_id}.rds
    ↓ (panna::build_all_parquet)
Parquet files: data/fbref/{table}/{league}/{season}.parquet
    ↓ (panna::pb_upload_source)
GitHub Releases: peteowen1/pannadata @ fbref-latest
```

## Table Types

| Table | Description |
|-------|-------------|
| summary | Basic match stats (goals, shots, possession) |
| passing | Pass completion, progressive passes, key passes |
| passing_types | Pass types (corners, crosses, through balls) |
| defense | Tackles, interceptions, blocks |
| possession | Touches, carries, dribbles |
| misc | Cards, fouls, offsides |
| keeper | Goalkeeper stats (saves, goals against) |
| shots | Shot-level data with xG |
| events | Goals, cards, substitutions with timing |
| metadata | Match info (date, teams, score, referee) |

## Competitions

### Leagues (Big 5)
- ENG (Premier League)
- ESP (La Liga)
- GER (Bundesliga)
- ITA (Serie A)
- FRA (Ligue 1)

### European
- UCL (Champions League)
- UEL (Europa League)

### Cups
- FA_CUP, EFL_CUP (England)
- COPA_DEL_REY (Spain)
- DFB_POKAL (Germany)
- COPPA_ITALIA (Italy)
- COUPE_DE_FRANCE (France)

### International
- WC (World Cup)
- EURO (European Championship)
- COPA_AMERICA
- NATIONS_LEAGUE
- AFCON, ASIAN_CUP, GOLD_CUP

## Oracle Cloud VM Setup

The daily scrape runs on the VM via cron:
```bash
# SSH to VM
ssh -i ~/.ssh/oracle-cloud-key opc@168.138.108.69

# View cron job
crontab -l

# View recent logs
tail -100 /home/opc/scraper/logs/latest.log
```

## Rate Limiting

FBref requires 4+ second delays between requests. The scraper automatically
handles this via `scrape_comp_season(..., delay = 4)`.
