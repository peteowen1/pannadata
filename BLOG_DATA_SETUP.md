# Blog Data Setup

Blog data is available from two locations depending on which pipeline produced it:

- **Cloudflare R2** (from GHA workflow `build-blog-data.yml`): `inthegame-data/panna_ratings.parquet`
- **GitHub Releases** (from panna pipeline `10_export_blog_data.R`):
  - `https://github.com/peteowen1/pannadata/releases/download/blog-latest/panna_ratings.parquet`
  - `https://github.com/peteowen1/pannadata/releases/download/blog-latest/match_predictions.parquet`

## Player Ratings

`panna_ratings.parquet` — latest season player ratings (xRAPM + SPM).

Produced by either path. The GHA workflow uploads to Cloudflare R2; the panna pipeline uploads to GitHub Releases `blog-latest`.

| Column | Type | Description |
|---|---|---|
| `panna_rank` | integer | Overall rank (1 = best) |
| `player_name` | string | Player name |
| `panna` | double | Overall xRAPM rating (positive = better) |
| `offense` | double | Contribution to creating xG |
| `defense` | double | Contribution to preventing xG (negative = good) |
| `spm_overall` | double | SPM overall rating |
| `total_minutes` | double | Total minutes played |
| `panna_percentile` | double | Percentile ranking (0-100) |

## Match Predictions

`match_predictions.parquet` — predicted outcomes for upcoming and recent matches.

Produced by `panna/data-raw/match-predictions-opta/10_export_blog_data.R` only (not by `scripts/build_blog_data.R`).

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

### Player Ratings (two paths)

**Option A: GHA workflow** — `build-blog-data.yml` (workflow_dispatch). Downloads from `ratings-data` release, aggregates via `scripts/build_blog_data.R`, uploads to Cloudflare R2.

**Option B: Panna pipeline** — from `panna/`:

```r
source("data-raw/match-predictions-opta/10_export_blog_data.R")
```

Requires: `cache-skills/06_seasonal_ratings.rds` (preferred, from Skills pipeline) or `cache-opta/07_seasonal_ratings.rds` (fallback, from Opta RAPM pipeline), `gh` CLI authenticated.

### Match Predictions

From `panna/`:

```r
# Standalone
source("data-raw/match-predictions-opta/10_export_blog_data.R")

# As part of the prediction pipeline
run_steps$step_10_export_blog_data <- TRUE
source("data-raw/match-predictions-opta/run_predictions_opta.R")
```

Requires: `cache-predictions-opta/predictions.parquet` (from prediction pipeline step 07), `gh` CLI authenticated.

## Verify

```bash
gh release view blog-latest --repo peteowen1/pannadata
gh release download blog-latest --pattern "panna_ratings.parquet" --dir /tmp --repo peteowen1/pannadata
```
