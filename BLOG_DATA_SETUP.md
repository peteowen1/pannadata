# Blog Data Setup

The blog fetches `panna_ratings.parquet` live from this repo via:

```
https://raw.githubusercontent.com/peteowen1/pannadata/main/blog/panna_ratings.parquet
```

## What to add

Create `blog/panna_ratings.parquet` on the `main` branch with these columns:

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

## How

Add a GitHub Action or script that generates the parquet and commits it to `blog/`.
See torpdata or bouncerdata repos for a working example (`build-blog-data.yml` + `build_blog_data.R`).

## Verify

```bash
curl -sI "https://raw.githubusercontent.com/peteowen1/pannadata/main/blog/panna_ratings.parquet" | head -3
# Should return HTTP/1.1 200 OK
```
