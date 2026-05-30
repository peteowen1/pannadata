"""Verify events_consolidated parquets cover all matches in player_stats.

Run from the daily-opta-scrape workflow AFTER `consolidate_opta.py` and
BEFORE upload. Refuses to proceed when any (league, season) has more
than --gap-threshold matches present in opta_player_stats.parquet but
missing from the corresponding events_consolidated/events_<league>.parquet.

This catches the failure mode observed 2026-05-29 where
events_Championship.parquet on opta-latest had only 265 of 557 played
matches, silently producing incomplete game_logs.parquet downstream.

Exit codes:
    0  all leagues OK or within tolerance
    1  ANY league exceeded --gap-threshold (output lists offenders)
    2  argument / IO error
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pyarrow.parquet as pq


def unique_match_ids(parquet_path: Path, filter_expr=None) -> set[str]:
    """Return distinct match_ids from a parquet file (optionally filtered)."""
    if not parquet_path.exists():
        return set()
    try:
        if filter_expr is not None:
            table = pq.read_table(parquet_path, columns=["match_id"], filters=filter_expr)
        else:
            table = pq.read_table(parquet_path, columns=["match_id"])
        col = table.column("match_id")
        return set(col.to_pylist())
    except Exception as exc:
        print(f"  WARN: could not read {parquet_path.name}: {exc}", file=sys.stderr)
        return set()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--player-stats", required=True, type=Path,
                        help="Path to opta_player_stats.parquet")
    parser.add_argument("--events-dir", required=True, type=Path,
                        help="Path to events_consolidated/ directory")
    parser.add_argument("--season", required=True,
                        help='Season label to check, e.g. "2025-2026"')
    def _threshold_type(s):
        """Accept positive int OR the literal 'Inf' (case-insensitive) to disable."""
        if s.strip().lower() == "inf":
            return float("inf")
        return int(s)
    parser.add_argument("--gap-threshold", type=_threshold_type, default=20,
                        help="Per-league gap above which to fail. "
                             "Use 'Inf' to disable (warn-only mode). Default 20.")
    parser.add_argument("--leagues", default="",
                        help="Space-separated list of competition codes to check "
                             "(matches Opta competition names with spaces -> underscores, "
                             "e.g. \"EPL La_Liga Championship\"). Empty = check all "
                             "competitions in player_stats for the season. Use to scope "
                             "a workflow_dispatch run's check to just the leagues it "
                             "scraped, so unrelated stale-coverage gaps don't block "
                             "the targeted upload.")
    args = parser.parse_args()

    leagues_filter: set[str] = set(args.leagues.split()) if args.leagues.strip() else set()
    if leagues_filter:
        print(f"::notice::Scoping coverage check to {len(leagues_filter)} league(s): "
              f"{', '.join(sorted(leagues_filter))}")

    if not args.player_stats.exists():
        print(f"ERROR: player_stats file not found: {args.player_stats}", file=sys.stderr)
        return 2
    if not args.events_dir.exists():
        print(f"ERROR: events_consolidated dir not found: {args.events_dir}", file=sys.stderr)
        return 2

    # Get all (competition, season) pairs from player_stats matching the season
    ps_table = pq.read_table(
        args.player_stats,
        columns=["competition", "season", "match_id"],
        filters=[("season", "=", args.season)],
    )
    if ps_table.num_rows == 0:
        print(f"::warning::No player_stats rows for season={args.season} — nothing to check")
        return 0

    # Group by competition (filtered to --leagues if given)
    import collections
    ps_by_comp: dict[str, set[str]] = collections.defaultdict(set)
    comps = ps_table.column("competition").to_pylist()
    mids = ps_table.column("match_id").to_pylist()
    for comp, mid in zip(comps, mids):
        if leagues_filter and comp not in leagues_filter:
            continue
        ps_by_comp[comp].add(mid)

    if leagues_filter and not ps_by_comp:
        # No player_stats rows for any of the requested leagues this season.
        # Common for dispatch runs targeting old tournament cycles that don't
        # appear in the current season label. Pass; the daily cron-mode check
        # still owns full-catalog health.
        print(f"::notice::No player_stats rows for any of the requested leagues "
              f"in season={args.season} — passing (nothing in scope to verify)")
        return 0

    print(f"=== Events coverage check for season={args.season} ===")
    print(f"{'comp':<28} {'ps_matches':>11} {'ev_matches':>11} {'gap':>6} {'status':<8}")
    print("-" * 78)

    offenders: list[tuple[str, int, int, int]] = []
    for comp in sorted(ps_by_comp.keys()):
        ev_path = args.events_dir / f"events_{comp}.parquet"
        ps_n = len(ps_by_comp[comp])
        if not ev_path.exists():
            # events_consolidated/events_<comp>.parquet missing entirely.
            # Above gap_threshold = silent skip of a real coverage gap
            # for an active competition. Count as an offender so the
            # operator sees it as FAIL (not just printed in the table).
            if ps_n > args.gap_threshold:
                status = "FAIL-no-file"
                offenders.append((comp, ps_n, 0, ps_n))
            else:
                status = "no-file"
            print(f"{comp:<28} {ps_n:>11} {'-':>11} {ps_n if status == 'FAIL-no-file' else '-':>6} {status}")
            continue
        ev_ids = unique_match_ids(ev_path, filter_expr=[("season", "=", args.season)])
        ps_ids = ps_by_comp[comp]
        gap = len(ps_ids - ev_ids)
        if gap == 0:
            status = "OK"
        elif gap <= args.gap_threshold:
            status = "warn"
        else:
            status = "FAIL"
            offenders.append((comp, ps_n, len(ev_ids), gap))
        print(f"{comp:<28} {ps_n:>11} {len(ev_ids):>11} {gap:>6} {status}")

    print("-" * 78)
    if offenders:
        print(f"\n::error::events_consolidated coverage FAILED for {len(offenders)} league(s) "
              f"(gap > {args.gap_threshold}):")
        for comp, ps_n, ev_n, gap in offenders:
            print(f"  - {comp}: {ev_n}/{ps_n} matches covered ({gap} missing)")
        print("\nNext step: re-scrape the affected leagues with force_rescrape=true to "
              "rebuild the events_consolidated parquet.")
        return 1

    print(f"\nAll leagues within tolerance (gap_threshold={args.gap_threshold}).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
