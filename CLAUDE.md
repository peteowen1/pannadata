# CLAUDE.md — pannadata

Data repository for the pannaverse ecosystem. Stores cached match data from three sources, managed via GitHub Releases (not git-tracked).

## Data Sources

| Source | Scraper | Environment | Schedule | Release Tag |
|--------|---------|-------------|----------|-------------|
| **Opta** ⭐ | Python (`scripts/opta/`) | GitHub Actions | 5 AM UTC daily | `opta-latest` |
| Understat (deprecated) | R (`scripts/understat/`) | (disabled — workflow `.disabled`) | — | `understat-latest` |
| FBref (deprecated) | R (`scripts/fbref/`) | (disabled — workflow `.disabled`; Oracle VM scrape also retired) | — | `fbref-latest` |

As of 2026-04-18 the project consolidated on Opta. Understat and FBref scrape workflows are disabled (`.disabled` extension) and slated for archival; their code remains in the repo for reference but does not run on a schedule. Build new features against Opta only.

## Directory Structure

```
data/
├── opta/
│   ├── events/           # Goal/sub/card events with event_type set (used for splint boundaries)
│   ├── match_events/     # ALL raw events with type_id (used for SPADL conversion AND for type_id == 30 period-end markers consumed by panna's extract_period_end_times())
│   ├── lineups/          # Match lineup data
│   ├── fixtures/         # Fixture/result data
│   ├── shots/            # Shot-level data
│   ├── shot_events/      # Detailed shot events
│   ├── xmetrics/         # xG/xA/xPass per player (from panna pipeline)
│   ├── events_consolidated/  # Merged event files
│   ├── models/           # Legacy model copies (canonical source: pannamodels package)
│   └── opta_*.parquet    # Consolidated player stats, shots, lineups
├── fbref/
│   ├── defense/          # Defensive stats per league/season
│   ├── events/           # Match events (RDS per match)
│   └── metadata/         # League/season metadata
└── understat/
    ├── events/           # Match events (parquet per season)
    ├── metadata/         # League metadata
    ├── roster/           # Player rosters
    └── understat_*.parquet  # Consolidated files
```

**Data is NOT in git** — the `data/` directory is gitignored. All data is stored in GitHub Releases and downloaded via `panna::pb_download_source()`.

## Scripts

### Scraping

```bash
# Opta (Python — usually runs via GHA)
cd scripts/opta && pip install -r requirements.txt
python scrape_opta.py

# Understat (R — usually runs via GHA)
Rscript scripts/understat/scrape_understat.R

# FBref (R — runs on Oracle VM)
Rscript scripts/fbref/scrape_fbref.R
```

### Blog Data

```r
# Build blog data (triggered by panna predictions pipeline via repository_dispatch)
source("scripts/build_blog_data.R")     # Ratings parquet from xRAPM + SPM
source("scripts/build_player_meta.R")   # Player details (player-details.parquet)
source("scripts/build_shot_data.R")     # Shot data for visualizations
source("scripts/build_chains_ci.R")     # Possession chains with EPV equity
```

**Blog deliverables on R2** (`inthegame-data/football/`):

| File | Source | Purpose |
|------|--------|---------|
| `ratings.parquet` | `build_blog_data.R` | Seasonal player ratings |
| `player-details.parquet` | `build_player_meta.R` | Player bio (id, name, team, league, position) |
| `game-logs.parquet` | panna step 10b → `blog-latest` pass-through | Per-match EPV+WPA+PSV value metrics (current-season alias, fast default) |
| `game-logs-<season>.parquet` | panna step 10b per-season → `blog-latest` pass-through | Historical per-season value metrics, fetched on-demand by the blog Value tab (kept per-season, ~6MB each — never concatenated) |
| `chains-{CODE}.parquet` | `build_chains_ci.R` + equity join | Possession chains with per-action EPV equity |
| `predictions.parquet` | panna step 10 → `blog-latest` pass-through | Match predictions |

### Data Utilities

```r
# Upload/migration scripts in data-raw/
source("data-raw/upload_to_release.R")           # Upload files to GitHub Release
source("data-raw/upload_clean_parquets.R")        # Clean and upload parquets
source("data-raw/migrate_to_parquet.R")           # Convert legacy formats to parquet
```

## GitHub Actions

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `daily-opta-scrape.yml` | 5 AM UTC / `workflow_dispatch` | Python Opta scraper + consolidate + coverage check → `opta-latest` release. Dispatch supports `leagues` (space-separated, underscored), `seasons`, `recent`, `tier`, `force_rescrape`. Coverage check scopes to dispatched leagues. |
| `build-blog-data.yml` | `repository_dispatch` (`predictions-complete`) | Build blog data + run `build_player_positions.R` → Cloudflare R2 |
| `daily-understat-scrape.yml.disabled` | (disabled) | Understat scraper — retired with FBref/Understat deprecation |
| `daily-fbref-scrape.yml.disabled` | (disabled) | FBref scraper — also retired (Oracle VM scrape gone) |
| `scrape-notification.yml.disabled` | (disabled) | Notification on scrape success/failure |

## Data Bus Pattern

GitHub Releases serve as the data bus between repos:

```
pannadata scrapers → GitHub Releases (opta-latest, fbref-latest, understat-latest)
    ↓
panna::pb_download_source() downloads from releases
    ↓
panna pipelines process → upload to predictions-latest, blog-latest
    ↓
pannadata build-blog-data.yml → Cloudflare R2 (inthegame-data bucket)
    ↓
inthegame-blog reads from R2
```

## Key Files

- `DATA_DICTIONARY.md` — Column definitions for all data sources
- `BLOG_DATA_SETUP.md` — Blog delivery chain (R2 bucket setup)
- `scripts/opta/all_competitions.json` — Opta competition config
- `scripts/opta/README.md` — Opta scraper documentation

## Gotchas

- **FBref match IDs are opaque** — 8-char hex IDs (e.g., `74125d47`) cannot be guessed. Look up from `data/fbref/metadata/` or fbref.com.
- **Opta scraper is Python**, everything else is R — check Python deps separately.
- **`build_blog_data.R` smart join** — auto-detects `player_id` vs `player_name` for joins. Step 10 of panna predictions now exports `player_id`.
- **`build_player_meta.R` must include `player_name`** in output for blog data join to work (even when using `player_id` as primary key).
- **`build_player_meta.R` smart path** — reads from `source/opta_lineups.parquet` (CI) or `data/opta/opta_lineups.parquet` (local). Outputs `blog/player-details.parquet`.
- **EPV-credit join in chains** — `build_chains_ci.R` left-joins `source/action_equity.parquet` (from panna step 10c) onto chains by `match_id + event_id`, producing the **`epv_credit`** column. ~84% match rate (SPADL merges duels, drops non-gameplay). `epv_credit` is per-action **player credit** (`player_credit` ≈ `epv_delta`) — **sum it, never diff it**, and don't correlate it against `calculate_action_epv()$epv` (state vs delta is ~0 by construction; that mistake produced a false "corrupt join" alarm 2026-06-02, see `pannaverse/CHAINS-EQUITY-BUG-2026-06-02.md`). **Renamed from `equity` → `epv_credit` 2026-06-03** to end the collision with the worker's `equity` = EPV *state*; the file name (`action_equity.parquet`) is unchanged, `build_chains_ci.R` reads `epv_credit ?? equity` from the source during the transition, and the blog reads `epv_credit ?? equity` from the chains. The join is now guarded: if either join inflates row count or matches < `MIN_JOIN_MATCH_FRAC` (0.80) of chain actions **in covered matches** (matches present in the lookup — uncovered seasons/comps are excluded from the floor), that comp is skipped (its prior parquet left intact) and recorded; healthy comps still build and the run **fails at the end** with the offender list (so one drifted comp doesn't block the others, but CI still goes red). A separate case — 0 overlap with the lookup — `warning()`s loudly (it ships an all-NA column) rather than failing, since it's legitimate for a comp outside the current-season alias.
- **TOURNAMENT_DATE_EXCEPTIONS is load-bearing** (`scripts/opta/scrape_opta.py`) — when a scrape dispatch returns `Found 0 total, 0 played, 0 new`, the FIRST hypothesis should be a missing date exception, not "Opta gap". Two common patterns trip the default Aug–Jul windowing:
  - **COVID-delayed tournaments** played outside their nominal year (EURO 2020 → 2021, AFCON 2021 Cameroon → Jan-Feb 2022, AFC Asian Cup 2023 Qatar → Jan-Feb 2024)
  - **Shared season names** between a main tournament and its qualifiers (e.g. `"2022 Qatar"` is used for WC 2022 itself AND for CONMEBOL/CAF/AFC WC qualifier cycles that played 2019–2022; the narrow Nov-Dec 2022 window catches main but misses all qualifiers)
  Fix is to add a `(start, end)` tuple to `TOURNAMENT_DATE_EXCEPTIONS` covering the actual play window for both senses. Pre-fix, AFC WC Qualifiers 2014 Brazil returned 12 matches; post-fix it returned 148.
- **discover_seasons.py + merge_discovered_seasons.py workflow** — when adding new comps or backfilling old cycles, run `python discover_seasons.py` (queries Opta tournamentcalendar for every comp in `opta_scraper.py::COMPETITIONS`), then `python merge_discovered_seasons.py` to add new (label → season_id) pairs to the main `seasons.json`. Watch for non-UTF-8 bytes in season names — Opta's API has sent latin-1 / cp1252 encoded responses for accented characters in the past; the merge normalizes apostrophes but check the diff before committing.
- **`check_events_coverage.py` is layer-2 defense for the data bus** (`scripts/opta/`, runs in `daily-opta-scrape.yml` AFTER consolidate, BEFORE upload). Verifies every (competition, season) pair in `opta_player_stats.parquet` is also represented in `events_consolidated/events_<comp>.parquet`. Filed because Championship 265/557 silently shipped to opta-latest 2026-05-29 and downstream blog Value tab capped at GP=33. Exits 1 with an error annotation listing offenders when any league has gap > `EVENTS_COVERAGE_GAP_THRESHOLD` (default 20; settable via repo var). Threshold accepts `"Inf"` (case-insensitive) for warn-only mode. **`--leagues` scopes enforcement** to a space-separated list — workflow passes `${{ inputs.leagues }}` so `workflow_dispatch` runs only fail on their targeted leagues, not unrelated stale-coverage gaps elsewhere in opta-latest. Cron-mode (empty inputs.leagues) keeps full-catalog enforcement.
- **opta-latest has ~40 leagues with stale events_consolidated** (as of 2026-05-29) — Bulgarian First 0/135, Cypriot First 0/133, Tunisian Ligue 1 0/134, plus most non-Big5 leagues and cups. Cron-mode coverage check fails until these are backfilled (see `rebuild_events.py` below). Daily Opta scrape will report `failure` for runs without an `inputs.leagues` scope until then. **Doesn't block targeted dispatches.** **The fix is `rebuild-events.yml`, not `force_rescrape`** — dispatch it per-comp against the offenders from `check_events_coverage.py`. First real backfill of blog leagues (Championship/UEL/Conference_League) was run 2026-06-03; the tool had been built 2026-05-30 but never dispatched, which is why the backlog sat open.
- **`rebuild_events.py` is THE fix for short events_consolidated** (`scripts/opta/`, dispatched via `rebuild-events.yml`, manual-only). Root cause it targets: `opta-manifest.parquet` marks a match `complete` once *any* artifact (player_stats) lands, without tracking *which* artifact types were written — so a match whose player_stats succeeded but `match_events` silently failed stays `complete` forever and the daily scraper's discovery layer never re-fetches its events. `rebuild_events.py` bypasses the manifest: reads match_ids straight from `opta_player_stats.parquet`, re-fetches the events artifact for the missing ones, writes per-(comp,season) parquets that `consolidate_events_by_league()` picks up. Dispatch (single comp per run, hundreds of Opta API calls — operator's rate-limit budget): `gh -R peteowen1/pannadata workflow run rebuild-events.yml -f competition=Championship -f season=2025-2026 -f dry_run=false -f run_consolidate=true`. Use `dry_run=true` first (0 API calls, lists target match_ids). `competition` is the **Opta** name (`Championship`, `UEL`, `Conference_League`), not the panna code. Phase 2 (manifest artifact-type tracking, so the daily scrape self-heals and the gap can't recur) is not built yet. See `scripts/opta/REBUILD_EVENTS_DESIGN.md`.
- **Event-less registry: `event_less_match_ids.parquet` (on `opta-latest`)** — some matches have player_stats but NO Opta event feed (cup qualifier rounds — e.g. 22 UEL, 45 UECL for 2025-2026). These can NEVER be backfilled, so `rebuild_events.py` records each empty-response match_id into this registry (cols: `match_id, competition, season, reason, detected_at`; deduped on match_id) and — crucially — treats empty-response as a DATA FACT, not a failure: `matches_failed` now counts only real API/parse errors, so a comp that is *entirely* event-less still exits 0 and its consolidate+upload (incl. the registry) runs. Downstream, panna's `check_events_coverage()` subtracts this registry from the expected-events denominator so those matches don't trip an unsatisfiable coverage gate (see panna/CLAUDE.md). The daily cron's layer-2 `check_events_coverage.py` does NOT yet subtract the registry — a follow-up; for now its `--leagues` scoping / `Inf` threshold handle the ~37-league non-blog backlog. First registry seeded 2026-06-03 from the UEL/UECL rebuild logs.
- **`force_rescrape=true` bypasses the workflow's "zero new matches" early-exit** but does NOT make `scrape_opta.py` re-fetch matches already in the manifest. Confirmed 2026-05-29: `--force --leagues Championship --recent 1` returned `Total matches: 0` because the scraper's discovery layer treats matches in the manifest as "done." Backfilling stale `events_consolidated` is `rebuild_events.py`'s job (above), not `force_rescrape`'s — the latter won't clear the backlog.
- **Workflow `leagues` input format** — space-separated, **underscores instead of spaces** for multi-word names (e.g. `EPL La_Liga Championship`, NOT `"La Liga"`). Mirrors how Opta competition names appear in `opta_player_stats.parquet::competition` and `events_<comp>.parquet` filenames.
- **`build_player_positions.R` self-downloads `opta_player_stats.parquet`** in the workflow even though an earlier step had it in `source/`. The earlier step's cleanup hook deletes the file before this step runs; without the self-download, position derivation silently produces NA opta_position mode for every player and the parquet ships malformed. Added 2026-05-29 to fix a missing-dependency regression.
- **Release `createdAt` / `publishedAt` lie about freshness** (cross-ref `~/.claude/CLAUDE.md`) — `opta-latest`'s tag is months old; only the per-asset `updatedAt` reflects actual data age. Use `gh -R peteowen1/pannadata release view opta-latest --json assets --jq '[.assets[] | {name, updated: .updatedAt}]'`.
