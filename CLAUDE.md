# CLAUDE.md

## Overview

`pannadata` is a **data repository** (not an R package). It stores cached FBref match data for the pannaverse ecosystem.

## Structure

```
pannadata/
├── README.md
├── .gitignore        # Ignores data/ folder
├── data-raw/
│   └── migrate_data.R
└── data/             # Gitignored - stored in GitHub Releases
    └── {tabletype}/{league}/{season}/{id}.rds
```

## Key Points

- **No R code** - all functions are in the `panna` package
- **Data is gitignored** - too large for git, use GitHub Releases
- **Hierarchical structure** - `data/{tabletype}/{league}/{season}/{id}.rds`

## Syncing with GitHub Releases

Use `panna` package functions:
```r
panna::pb_download_data()  # Download from releases
panna::pb_upload_data()    # Upload to releases
```
