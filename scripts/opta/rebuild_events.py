"""Backfill match_events (and shot_events) for matches whose match_ids exist in
opta_player_stats.parquet but are absent from per-league match_events files.

The opta scraper tracks each match_id in opta-manifest.parquet. Once a match
is marked complete in the manifest, the scraper's discovery layer never
re-fetches it — even if specific artifacts (events, shot_events) were never
actually written. Result: a long tail of (comp, season) pairs where
events_consolidated/events_<comp>.parquet has fewer matches than
opta_player_stats.parquet says exist.

This script bypasses the manifest entirely. It reads match_ids directly from
opta_player_stats.parquet, calls Opta's matchevent endpoint for each, and
writes per-(comp, season) match_events + shot_events parquets that the
existing consolidate_events_by_league() will then pick up.

Design doc: scripts/opta/REBUILD_EVENTS_DESIGN.md
Issue: pannadata (closing the 40-league events_consolidated backlog)

Phase 1 scope (this implementation):
  - Single (competition, season) per invocation
  - --only-missing flag (default): skip match_ids already in the per-league
    match_events parquet
  - Rate-limited Opta API calls (reuses OptaScraper's existing throttling)
  - Per-match writes batched via combine_and_save pattern
  - Does NOT touch the manifest (Phase 2)
  - Does NOT trigger consolidation (caller must run consolidate_opta.py after)

CLI examples
------------
Single league-season:
  python rebuild_events.py \\
      --player-stats data/opta/opta_player_stats.parquet \\
      --opta-dir data/opta \\
      --competition Bulgarian_First_League \\
      --season 2025-2026

Dry-run (lists missing match_ids without API calls):
  python rebuild_events.py \\
      --player-stats data/opta/opta_player_stats.parquet \\
      --opta-dir data/opta \\
      --competition Bulgarian_First_League \\
      --season 2025-2026 \\
      --dry-run
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow.parquet as pq

# Reuse the production scraper for the matchevent endpoint + parsing.
from opta_scraper import OptaScraper


@dataclass
class RebuildSummary:
    competition: str
    season: str
    matches_in_player_stats: int
    matches_already_present: int
    matches_attempted: int
    matches_succeeded: int
    matches_failed: int
    failed_match_ids: list
    match_events_rows_written: int
    shot_events_rows_written: int
    elapsed_seconds: float
    # Matches that returned an empty event response — Opta has player_stats but
    # no event feed (e.g. cup qualifiers). These are a DATA FACT, not a run
    # failure: recorded to the eventless registry and excluded from the exit
    # code so a comp that is genuinely event-less still consolidates + uploads.
    matches_eventless: int = 0
    eventless_match_ids: list = field(default_factory=list)

    def to_dict(self):
        return asdict(self)


def _existing_match_ids(parquet_path: Path) -> set:
    """Return the set of match_ids already present in the per-league parquet,
    or empty set if the file doesn't exist."""
    if not parquet_path.exists():
        return set()
    try:
        tbl = pq.read_table(parquet_path, columns=["match_id"])
        return set(tbl.column("match_id").to_pylist())
    except Exception as e:
        print(f"  WARNING: could not read {parquet_path} ({e}); treating as empty")
        return set()


def _combine_and_save(new_rows: list, output_path: Path, dedup_cols: list) -> int:
    """Combine new rows with existing parquet, dedup, write. Returns final row count.
    Mirrors scrape_opta.py's combine_and_save (kept local to avoid the manifest
    plumbing that wraps the original)."""
    if not new_rows:
        return 0
    new_df = pd.DataFrame(new_rows)
    if output_path.exists():
        try:
            existing_df = pd.read_parquet(output_path)
            combined = pd.concat([existing_df, new_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=dedup_cols, keep="last")
        except Exception as e:
            print(f"  WARNING: could not read existing {output_path} ({e}); writing new rows only")
            combined = new_df
    else:
        combined = new_df
    output_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(output_path, index=False)
    return len(combined)


def _update_eventless_registry(opta_dir: Path, competition: str, season: str,
                                eventless_ids: list) -> int:
    """Record match_ids whose event fetch returned an empty response into a
    persistent registry (`event_less_match_ids.parquet` at the opta-dir root).

    These are matches Opta has player_stats for but provides NO event feed for
    (confirmed event-less under an explicit, non-throttled fetch — e.g. cup
    qualifier rounds). The downstream coverage check subtracts them from the
    'expected events' denominator so they stop tripping an unsatisfiable gate.
    Dedups on match_id, preserving the first-seen detected_at. Returns the
    registry's total row count (0 if nothing to record)."""
    if not eventless_ids:
        return 0
    registry_path = opta_dir / "event_less_match_ids.parquet"
    new_df = pd.DataFrame({
        "match_id": list(eventless_ids),
        "competition": competition,
        "season": season,
        "reason": "empty_response",
        "detected_at": date.today().isoformat(),
    })
    if registry_path.exists():
        try:
            existing_df = pd.read_parquet(registry_path)
            combined = pd.concat([existing_df, new_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=["match_id"], keep="first")
        except Exception as e:
            print(f"  WARNING: could not read {registry_path} ({e}); writing new rows only")
            combined = new_df
    else:
        combined = new_df
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(registry_path, index=False)
    return len(combined)


def _player_stats_match_ids(player_stats_path: Path, comp: str, season: str) -> set:
    """Return the set of distinct match_ids in player_stats for (comp, season).
    Read once per invocation to avoid re-scanning a large parquet."""
    if not player_stats_path.exists():
        raise FileNotFoundError(f"player_stats parquet not found: {player_stats_path}")
    tbl = pq.read_table(
        player_stats_path,
        columns=["match_id", "competition", "season"],
        filters=[("competition", "=", comp), ("season", "=", season)],
    )
    return set(tbl.column("match_id").to_pylist())


def rebuild_events_for_league(
    *,
    competition: str,
    season: str,
    opta_dir: Path,
    player_stats_path: Path,
    scraper: Optional[OptaScraper] = None,
    only_missing: bool = True,
    dry_run: bool = False,
    max_matches: Optional[int] = None,
) -> RebuildSummary:
    """Backfill match_events + shot_events for the given (comp, season).

    Parameters
    ----------
    competition, season : str
        Competition code (e.g. "Bulgarian_First_League") and season label
        (e.g. "2025-2026"). Must match the values in opta_player_stats.parquet.
    opta_dir : Path
        Root of the opta data tree (sibling to `match_events/`, `shot_events/`).
    player_stats_path : Path
        Path to opta_player_stats.parquet. Source of truth for which match_ids
        should exist for (comp, season).
    scraper : OptaScraper, optional
        Reuse an existing scraper instance (preserves auth + rate-limit state).
        Constructed fresh if None.
    only_missing : bool
        If True (default), only fetch match_ids absent from the per-league
        match_events parquet. If False, fetch every match in player_stats.
    dry_run : bool
        If True, print what would be fetched and exit without API calls.
    max_matches : int, optional
        Cap on matches fetched per invocation (rate-limit safety).
    """
    t0 = time.time()
    if scraper is None:
        scraper = OptaScraper(data_dir=str(opta_dir.parent))

    me_path = opta_dir / "match_events" / competition / f"{season}.parquet"
    se_path = opta_dir / "shot_events" / competition / f"{season}.parquet"

    # Read player_stats once — used both for the universe count and for
    # `to_fetch` derivation. Previous version called the helper twice which
    # re-scanned the (potentially hundreds-of-MB) parquet a second time.
    ps_ids = _player_stats_match_ids(player_stats_path, competition, season)
    existing = _existing_match_ids(me_path)
    if only_missing:
        to_fetch = sorted(ps_ids - existing)
    else:
        to_fetch = sorted(ps_ids)
    n_in_ps = len(ps_ids)
    if max_matches is not None and len(to_fetch) > max_matches:
        print(f"  Capping fetch list at --max-matches={max_matches} "
              f"(skipping {len(to_fetch) - max_matches} matches for next run)")
        to_fetch = to_fetch[:max_matches]

    print(f"\n=== rebuild_events: {competition} / {season} ===")
    print(f"  player_stats matches:      {n_in_ps}")
    print(f"  already in match_events:   {len(existing)}")
    print(f"  to fetch this run:         {len(to_fetch)}")

    if dry_run:
        print(f"  DRY RUN — listing first 10:")
        for mid in to_fetch[:10]:
            print(f"    {mid}")
        return RebuildSummary(
            competition=competition, season=season,
            matches_in_player_stats=n_in_ps,
            matches_already_present=len(existing),
            matches_attempted=0, matches_succeeded=0, matches_failed=0,
            failed_match_ids=[], match_events_rows_written=0,
            shot_events_rows_written=0,
            elapsed_seconds=time.time() - t0,
        )

    if not to_fetch:
        print("  Nothing to fetch — coverage already complete.")
        return RebuildSummary(
            competition=competition, season=season,
            matches_in_player_stats=n_in_ps,
            matches_already_present=len(existing),
            matches_attempted=0, matches_succeeded=0, matches_failed=0,
            failed_match_ids=[], match_events_rows_written=0,
            shot_events_rows_written=0,
            elapsed_seconds=time.time() - t0,
        )

    n_succeeded = 0
    # Two distinct buckets: genuine errors (API/parse — operational problems
    # that should fail the run) vs empty responses (Opta has no event feed for
    # this match — a data fact recorded to the registry, NOT a run failure).
    error_ids: list = []
    eventless_ids: list = []
    accumulated_match_events: list = []
    accumulated_shot_events: list = []

    for i, mid in enumerate(to_fetch, start=1):
        print(f"  [{i}/{len(to_fetch)}] {mid}...", end=" ", flush=True)
        try:
            event_data = scraper.get_match_events(mid)
        except Exception as e:
            print(f"FAILED (API error: {e})")
            error_ids.append(mid)
            continue
        if not event_data:
            # Empty response = Opta provides no events for this match. Not an
            # error — record it as event-less so coverage stops expecting it.
            print("EVENTLESS (empty response)")
            eventless_ids.append(mid)
            continue

        try:
            match_events = scraper.extract_all_match_events(event_data)
            shot_events = scraper.extract_shot_events(event_data)
        except Exception as e:
            print(f"FAILED (parse error: {e})")
            error_ids.append(mid)
            continue

        accumulated_match_events.extend([asdict(e) for e in match_events])
        accumulated_shot_events.extend([asdict(s) for s in shot_events])
        n_succeeded += 1
        print(f"OK ({len(match_events)} events, {len(shot_events)} shot_events)")

    # Single combine-and-save pass at the end — minimises parquet writes.
    me_rows = _combine_and_save(accumulated_match_events, me_path,
                                 dedup_cols=["match_id", "event_id"]) \
        if accumulated_match_events else 0
    se_rows = _combine_and_save(accumulated_shot_events, se_path,
                                 dedup_cols=["match_id", "event_id"]) \
        if accumulated_shot_events else 0

    if me_rows:
        print(f"\n  Wrote {me_path}: {me_rows} total rows")
    if se_rows:
        print(f"  Wrote {se_path}: {se_rows} total rows")

    # Persist the event-less match_ids so the coverage check can exclude them.
    registry_rows = _update_eventless_registry(opta_dir, competition, season,
                                                eventless_ids)
    if eventless_ids:
        print(f"  Recorded {len(eventless_ids)} event-less match_id(s) "
              f"→ event_less_match_ids.parquet ({registry_rows} total)")

    summary = RebuildSummary(
        competition=competition, season=season,
        matches_in_player_stats=n_in_ps,
        matches_already_present=len(existing),
        matches_attempted=len(to_fetch),
        matches_succeeded=n_succeeded,
        matches_failed=len(error_ids),
        failed_match_ids=error_ids,
        match_events_rows_written=me_rows,
        shot_events_rows_written=se_rows,
        elapsed_seconds=time.time() - t0,
        matches_eventless=len(eventless_ids),
        eventless_match_ids=eventless_ids,
    )
    print(f"\n=== Summary ===")
    print(json.dumps(summary.to_dict(), indent=2))
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                      formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--player-stats", required=True, type=Path,
                        help="Path to opta_player_stats.parquet")
    parser.add_argument("--opta-dir", required=True, type=Path,
                        help="Root opta data dir (parent of match_events/, shot_events/)")
    parser.add_argument("--competition", required=True,
                        help="Competition code (e.g. Bulgarian_First_League)")
    parser.add_argument("--season", required=True,
                        help='Season label (e.g. "2025-2026")')
    # BooleanOptionalAction so `--only-missing` / `--no-only-missing` both
    # toggle the flag intuitively (vs the older confusing pattern where
    # `--only-missing` is a no-op and `--all` is the secret off-switch).
    parser.add_argument("--only-missing", action=argparse.BooleanOptionalAction,
                        default=True,
                        help="Only fetch match_ids absent from existing per-league "
                             "parquet (default). Use --no-only-missing to fetch "
                             "every match in player_stats.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be fetched without making API calls")
    parser.add_argument("--max-matches", type=int, default=None,
                        help="Cap on matches per invocation (rate-limit safety)")
    args = parser.parse_args()

    if not args.player_stats.exists():
        print(f"ERROR: player_stats parquet not found: {args.player_stats}",
              file=sys.stderr)
        return 2
    if not args.opta_dir.exists():
        print(f"ERROR: opta_dir not found: {args.opta_dir}", file=sys.stderr)
        return 2

    summary = rebuild_events_for_league(
        competition=args.competition,
        season=args.season,
        opta_dir=args.opta_dir,
        player_stats_path=args.player_stats,
        only_missing=args.only_missing,
        dry_run=args.dry_run,
        max_matches=args.max_matches,
    )
    return 0 if summary.matches_failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
