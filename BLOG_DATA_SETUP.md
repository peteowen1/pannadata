# Blog Data Setup

The blog fetches data from GitHub Releases on this repo:

```
https://github.com/peteowen1/pannadata/releases/download/blog-latest/panna_ratings.parquet
https://github.com/peteowen1/pannadata/releases/download/blog-latest/match_predictions.parquet
```

## Player Ratings

`panna_ratings.parquet` — latest season player ratings (xRAPM + SPM).

| Column | Type | Description |
|---|---|---|
| `panna_rank` | integer | Overall rank (1 = best) |
| `player_name` | string | Player name |
| `panna` | double | Overall rating (offense - defense) |
| `offense` | double | Contribution to creating xG |
| `defense` | double | Contribution to preventing xG (negative = good) |
| `spm_overall` | double | SPM overall rating |
| `total_minutes` | double | Total minutes played |
| `panna_percentile` | double | Percentile ranking (0-100) |

## Match Predictions

`match_predictions.parquet` — predicted outcomes for upcoming and recent matches.

| Column | Type | Description |
|---|---|---|
| `match_id` | string | Unique match identifier |
| `match_date` | date | Match date |
| `league` | string | League code |
| `season` | string | Season string |
| `home_team` | string | Home team name |
| `away_team` | string | Away team name |
| `pred_home_goals` | double | Predicted home goals |
| `pred_away_goals` | double | Predicted away goals |
| `prob_H` | double | Home win probability |
| `prob_D` | double | Draw probability |
| `prob_A` | double | Away win probability |
| `predicted_result` | string | Most likely result (H/D/A) |

## How to Update

From `panna/`, run:

```r
# Option 1: Standalone
source("data-raw/match-predictions-opta/10_export_blog_data.R")

# Option 2: As part of the prediction pipeline
run_steps$step_10_export_blog_data <- TRUE
source("data-raw/match-predictions-opta/run_predictions_opta.R")
```

Requires:
- `cache-opta/07_seasonal_ratings.rds` (from Opta RAPM pipeline)
- `cache-predictions-opta/predictions.parquet` (from prediction pipeline step 07)
- `gh` CLI authenticated with push access to peteowen1/pannadata

## Verify

```bash
gh release view blog-latest --repo peteowen1/pannadata
gh release download blog-latest --pattern "panna_ratings.parquet" --dir /tmp --repo peteowen1/pannadata
```
