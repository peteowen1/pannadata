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
```

Understat/FBref scrapers (`scripts/understat/`, `scripts/fbref/`) are retired — code kept for reference only, do not run.

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
| `match-stats-{CODE}.parquet` | `rebuild_match_stats.R` (per-league, incl. `match-stats-WC.parquet`) | Per-match team/player box stats for blog match pages |
| `wc2026_*.parquet` | panna steps 11+12 → `blog-latest` pass-through | WC 2026 sim outputs: `predictions`, `simulation`, `groups`, `team_strength`, `squads`, `knockout_probs` |

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
| `rebuild-events.yml` | Manual dispatch only | Backfill short `events_consolidated` per comp via `rebuild_events.py` (see Gotchas — this, not `force_rescrape`, is the fix for stale events) |
| `football-player-meta.yml` | Mondays 5 AM UTC / manual | Player bios (age, nationality) + face-cropped webp headshots (2 variants) → R2. Resumable: HEAD-checks R2 and only fetches new players. To re-crop all, run `node scripts/build-football-headshots.mjs --reprocess` locally — the dispatch has no inputs and never passes that flag |
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
- `scripts/opta/opta_entitlement_catalog.csv` — **every competition + season our Opta outlet can actually fetch** (2,232 comps / 25,356 comp-seasons), one row per (competition, season) with `competition_id`, `season_id`, dates, and `our_key` (our short code if scraped). Regenerate with `python scripts/opta/build_entitlement_catalog.py` (no API key needed — uses the outlet token). Use this to look up correct Opta competition/season IDs before adding a league. NOTE: the IDs in `opta_scraper.py::COMPETITIONS` for MLS/Liga_MX/Argentine_Liga_Profesional/Saudi_League were wrong/stale (404'd) until 2026-06-05 — always validate a new comp's ID against this catalog, not by guessing.
- `league_strength.csv` — **consolidated league-strength reference** (committed; also on `opta-latest` as `league_strength.{parquet,csv}`). One row per league with **two independent strength metrics** (since 2026-06-11): `offset_tot` = club EPV offset vs UCL group stage ("how hard is it to produce there"; higher = stronger, 0 = UCL; EPV-rated leagues only, NA elsewhere) and `xrapm_mean`/`xrapm_rank` = minutes-weighted mean player xRAPM, latest season, 450+ min ("how good are the players"; covers every rated league incl. MLS/Liga_MX/Argentina/Saudi). Plus `confederation` + Elo prior. Built by `panna/debug/build_league_strength.R`. The metrics deliberately disagree in places (Championship: #4 club league by offset — `strength_rank` 7 incl. EURO/WC/UCL — but #14 by player quality). Small-n offsets (SCO, ENG2, and especially MLS at n_obs=5) are noisy — treat as soft; BEL/AUS have rows with NA offsets, TUN/CAF have no rows at all.
- `scripts/opta/README.md` — Opta scraper documentation

## Gotchas

**TL;DR fix matrix** (symptom → fix; details below): stale/short `events_consolidated` → `rebuild-events.yml` (NOT `force_rescrape`); scrape returns `Found 0 total, 0 new` → missing `TOURNAMENT_DATE_EXCEPTIONS` entry (NOT "Opta gap"); cron daily-opta-scrape red on non-blog leagues → backlog backfill via `rebuild-events.yml` / daily heal pass; "corrupt" chains equity → metric-mismatch, not a join bug (don't diff `epv_credit`).

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
- **Event-less registry: `event_less_match_ids.parquet` (on `opta-latest`)** — some matches have player_stats but NO Opta event feed (cup qualifier rounds — e.g. 22 UEL, 45 UECL for 2025-2026). These can NEVER be backfilled, so `rebuild_events.py` records each empty-response match_id into this registry (cols: `match_id, competition, season, reason, detected_at`; deduped on match_id) and — crucially — treats empty-response as a DATA FACT, not a failure: `matches_failed` now counts only real API/parse errors, so a comp that is *entirely* event-less still exits 0 and its consolidate+upload (incl. the registry) runs. Both coverage gates subtract this registry from the expected-events denominator so those matches don't trip an unsatisfiable gate: panna's `check_events_coverage()` (see panna/CLAUDE.md) AND the daily cron's layer-2 `check_events_coverage.py` (via `--eventless-registry data/opta/event_less_match_ids.parquet`, downloaded + passed in `daily-opta-scrape.yml`). First registry seeded 2026-06-03 from the UEL/UECL rebuild logs (then UCL added by its rebuild).
- **Daily scrape self-heals stranded events** (`scrape_opta.py::heal_unavailable_events`, Phase-2, 2026-06-03) — root cause: `event_unavailable=True` (set at `scrape_opta.py` for any match >7 days old that fails to yield complete events) permanently skips a match, and `reconcile_events_with_manifest()` only revisits `has_match_events=True` entries — so a TRANSIENT failure (events DO exist, e.g. the 292 Championship matches) strands forever. After the normal scrape, the heal pass re-attempts up to `--heal-cap` (default **200/run**) `event_unavailable` matches for `--heal-season` (default `2025-2026`) that the registry hasn't confirmed: events found → heal (append per-(comp,season) parquets + flip manifest flags); still empty → record to the registry. **Scoped to one season on purpose** — the manifest holds ~105k genuinely-event-less HISTORICAL matches (10k+/season back to 2010) whose blind re-confirmation would waste ~500 days of API budget for zero blog value. Best-effort (wrapped; never fails the scrape), skipped on `--force`/fixtures-only. Validate selection with `--heal-dry-run` (0 API calls). This is what converges the ~37-league non-blog cron backlog to green over ~weeks without manual `rebuild-events.yml` dispatches (which remain the way to backfill a specific comp on demand or an OLD season the heal pass doesn't target).
- **`force_rescrape=true` bypasses the workflow's "zero new matches" early-exit** but does NOT make `scrape_opta.py` re-fetch matches already in the manifest. Confirmed 2026-05-29: `--force --leagues Championship --recent 1` returned `Total matches: 0` because the scraper's discovery layer treats matches in the manifest as "done." Backfilling stale `events_consolidated` is `rebuild_events.py`'s job (above), not `force_rescrape`'s — the latter won't clear the backlog.
- **Workflow `leagues` input format** — space-separated, **underscores instead of spaces** for multi-word names (e.g. `EPL La_Liga Championship`, NOT `"La Liga"`). Mirrors how Opta competition names appear in `opta_player_stats.parquet::competition` and `events_<comp>.parquet` filenames.
- **`build_player_positions.R` self-downloads `opta_player_stats.parquet`** in the workflow even though an earlier step had it in `source/`. The earlier step's cleanup hook deletes the file before this step runs; without the self-download, position derivation silently produces NA opta_position mode for every player and the parquet ships malformed. Added 2026-05-29 to fix a missing-dependency regression.
- **Release `createdAt` / `publishedAt` lie about freshness** (cross-ref `~/.claude/CLAUDE.md`) — `opta-latest`'s tag is months old; only the per-asset `updatedAt` reflects actual data age. Use `gh -R peteowen1/pannadata release view opta-latest --json assets --jq '[.assets[] | {name, updated: .updatedAt}]'`.
