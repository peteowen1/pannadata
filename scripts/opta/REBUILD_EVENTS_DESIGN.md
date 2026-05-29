# `rebuild_events.py` — design doc

> **Status:** Design only as of 2026-05-29. No implementation yet.
> Files this would touch: `scripts/opta/rebuild_events.py` (new),
> `.github/workflows/daily-opta-scrape.yml` (new optional input).

## The problem

As of 2026-05-29, `opta-latest` has **~40 leagues with stale `events_consolidated`**
(Bulgarian First League 0/135, Cypriot First 0/133, Tunisian Ligue 1 0/134,
Coupe de France 63/201, Ekstraklasa 0/143, Championship 265/557, etc).
Player stats and lineups for these matches *are* in `opta-latest`. Only the
per-comp `events_consolidated/events_<comp>.parquet` files are short.

This breaks the layer-2 defense (`check_events_coverage.py`) for the daily
cron — every cron run fails because the full-catalog check sees the gaps,
even though the cron itself didn't introduce them. We worked around it by
adding `--leagues` scoping for `workflow_dispatch` runs (2026-05-29
`a336470`), but cron is still red.

### Why `force_rescrape=true --leagues <comp>` doesn't fix it

`scrape_opta.py` discovers matches via Opta's `tournamentcalendar` endpoint,
then consults `opta-manifest.parquet` to skip matches already marked
`complete`. A match marked complete from a past scrape that succeeded for
player_stats but silently failed for events stays complete forever — the
manifest doesn't track *which* artifact types are populated for that
match_id. Confirmed today: `--force --leagues Championship --recent 1`
returned `Total matches: 0` because the discovery layer saw nothing new
to fetch.

The right semantic for backfilling stale events isn't "rescrape" — it's
"re-fetch the events artifact for known match IDs."

## Proposed solution

A standalone Python script that reads match IDs directly from
`opta_player_stats.parquet` for a given (league, season) and fetches only
the events artifact for each, bypassing both the discovery layer and the
manifest-skip check.

### Function signature

```python
def rebuild_events_for_league(
    *,
    competition: str,             # e.g. "Bulgarian_First_League"
    season: str,                  # e.g. "2025-2026"
    opta_dir: Path,               # data/opta/ root
    scraper: OptaScraper,         # reused for rate limiting + auth state
    only_missing: bool = True,    # default: skip matches already in
                                  # opta/match_events/{comp}/{season}/
    max_retries: int = 3,
    rate_limit_min: float = 1.5,
    rate_limit_max: float = 3.0,
) -> RebuildSummary:
    """
    Re-fetch event-level data for every match in opta_player_stats.parquet
    matching (competition, season), bypassing scrape_opta's manifest cache.

    Writes per-match parquet files to:
        opta/match_events/{competition}/{season}/{match_id}.parquet
        opta/events/{competition}/{season}/{match_id}.parquet
        opta/shot_events/{competition}/{season}/{match_id}.parquet

    Returns a RebuildSummary with per-match status (succeeded / skipped /
    failed) and aggregate counts. Does NOT call consolidate_opta.py —
    the caller must do that to rebuild events_consolidated.
    """
```

### CLI shape

```bash
# Single league-season
python rebuild_events.py \
    --player-stats data/opta/opta_player_stats.parquet \
    --opta-dir data/opta \
    --competition Bulgarian_First_League \
    --season 2025-2026

# Multiple leagues, comma-separated
python rebuild_events.py \
    --player-stats data/opta/opta_player_stats.parquet \
    --opta-dir data/opta \
    --competitions "Bulgarian_First_League,Cypriot_First,Tunisian_Ligue_1" \
    --season 2025-2026

# Drive from the coverage check's offender list (the natural workflow)
python check_events_coverage.py ... --json-offenders /tmp/offenders.json
python rebuild_events.py --from-offenders /tmp/offenders.json
```

### Output

`RebuildSummary` JSON:

```json
{
  "competition": "Bulgarian_First_League",
  "season": "2025-2026",
  "matches_total": 135,
  "matches_already_present": 0,
  "matches_fetched": 132,
  "matches_failed": 3,
  "failed_match_ids": ["abc...", "def...", "ghi..."],
  "elapsed_seconds": 1284,
  "rate_limit_avg_seconds": 2.1
}
```

## Workflow integration

Two integration points, both keep cron-mode unchanged:

1. **New workflow_dispatch input `rebuild_events`** on `daily-opta-scrape.yml`:
   - Type: string (space-separated competition list, like the existing `leagues`)
   - When non-empty, after the regular scrape step run `rebuild_events.py`
     for those leagues, then `consolidate_opta.py` again, then the coverage
     check
   - Mutually exclusive with `seasons` (which targets cycles, not leagues)

2. **Optional: chain after coverage-check failure** — when the coverage
   check fails, emit the offender list as a workflow artifact + open a
   tracking issue. A separate `rebuild-events.yml` workflow on dispatch
   reads the artifact and calls rebuild_events.py. Manual trigger only —
   never automated, since a runaway rebuild could hit Opta API rate limits.

## Implementation plan

Order of work:

1. **Phase 1 — narrow MVP** (~2 hrs)
   - Single-(comp, season) function + CLI
   - Reuse `OptaScraper.get_match_events()` + parsers
   - Per-match parquet writes (no consolidate, no manifest update)
   - Test against Bulgarian_First_League 2025-2026 (135 matches, no
     prior events, smallest blast radius if something goes wrong)

2. **Phase 2 — manifest reconciliation** (~1 hr)
   - After successful event fetch, update `opta-manifest.parquet` so the
     entry's `has_match_events`/`has_events`/`has_shot_events` flags reflect
     reality. (Use existing `reconcile_events_with_manifest()` helper.)
   - Without this, the next regular scrape may attempt to re-fetch.

3. **Phase 3 — multi-comp + workflow wiring** (~2 hrs)
   - Comma-separated `--competitions` flag
   - `--from-offenders` JSON input shape
   - Modify `check_events_coverage.py` to emit `--json-offenders` output
   - New `workflow_dispatch` input on `daily-opta-scrape.yml`

4. **Phase 4 — batch backfill** (sequential, ~1 day wall clock)
   - Iterate the ~40 backlog leagues, one league at a time, in 2-5 league
     batches. Conservative rate limiting (2s avg between requests).
   - Estimated: 40 leagues × ~150 avg matches × 2s = ~3.3 hrs of API time.
     Realistically 6-8 hrs end-to-end with retries and consolidation passes.
   - Run as a series of `workflow_dispatch` invocations during low-traffic
     hours; the rebuild_events workflow can be the cron's "Phase B"
     after the regular scrape completes.

## Risk + mitigations

| Risk | Mitigation |
|------|-----------|
| Opta API rate-limit / IP block from bulk re-fetch | Conservative random 1.5-3s delay; cap per-run at 250 matches; exponential backoff on 429 (already in `_fetch_raw`) |
| Per-match parquet writes corrupt the per-comp consolidate read | Write to a temp file + rename atomically; consolidate already uses defensive read |
| Race with the daily 5 AM UTC cron | Schedule manual rebuild runs at low-traffic times; the cron's consolidate step is idempotent and won't drop new per-match parquets |
| Manifest divergence (events fetched but `has_match_events` still False) | Phase 2 reconciliation step is essential, not optional |
| Catastrophic API change mid-run | Run small batches; fail fast on schema mismatch in `extract_match_events()` |

## Testing plan

1. **Pre-deploy smoke test** — single match (`Bulgarian_First_League` 2025-2026,
   smallest known short league). Verify the 3 per-match parquets land
   with the expected schema.
2. **Single-league dry run** — `--dry-run` flag prints what would be
   fetched without calling the API; verify the match ID list matches
   `opta_player_stats.parquet`'s rows for that (comp, season).
3. **Single-league live** — Bulgarian_First_League full backfill (135
   matches, ~5 min). Confirm `check_events_coverage.py --leagues
   Bulgarian_First_League` passes after consolidate.
4. **Multi-league batch** — 5 leagues in one run. Confirm no
   interference between (comp, season) pairs and total elapsed time
   matches sequential estimate.
5. **Full backlog clearance** — staged across 1-2 sessions over a week.

## Open questions

1. **Manifest schema** — does the current manifest distinguish "events
   never fetched" from "events fetched and empty (real Opta gap)"? If not,
   Phase 2 needs to add a flag or rebuild_events will keep re-fetching
   matches that genuinely have no events.
2. **Shot vs event vs match_event** — `scrape_opta.py` produces 3 event
   artifact types per match: `events` (subset with event_type set, used for
   splint boundaries), `match_events` (ALL events with type_id, used for
   SPADL), `shot_events` (shot detail). All three should be regenerated
   together; verify which the coverage check actually inspects.
3. **Stale-detection scoping** — should the coverage check's offender
   output distinguish "0/N coverage" (never scraped) from "200/500 partial"
   (interrupted scrape)? Both need rebuild, but the former might warrant
   a manifest reset for the (comp, season) pair to clear potentially-bad
   "complete" entries.
4. **Workflow dispatch concurrency** — multiple `rebuild_events` dispatches
   in flight would race on `opta-latest`. Should the workflow take a lock
   (e.g., reject if another rebuild is in_progress) or rely on operator
   discipline?

## What this design explicitly does NOT do

- **Doesn't replace `scrape_opta.py`**. That script's "discover new matches
  via tournamentcalendar + fetch all artifacts" model is still correct
  for the daily cron path.
- **Doesn't fetch player_stats/lineups/fixtures** that are already
  present and complete. Narrowly targeted to the events gap.
- **Doesn't validate event content**. If Opta legitimately has zero
  events for a match (rare but happens for postponed matches that play
  later), this script will dutifully write a zero-row parquet and the
  coverage check will still flag it. A separate "Opta gap audit" tool is
  a future need.

---

*Filed 2026-05-29 as the natural follow-up to today's wave-5b investigation
+ the `--leagues` scoping fix (`a336470`). When the daily cron's full
coverage check is consistently red, this is the script that makes it
green.*
