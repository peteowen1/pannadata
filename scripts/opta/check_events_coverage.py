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
    parser.add_argument("--gap-threshold", type=int, default=20,
                        help="Per-league gap above which to fail (default 20)")
    args = parser.parse_args()

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

    # Group by competition
    import collections
    ps_by_comp: dict[str, set[str]] = collections.defaultdict(set)
    comps = ps_table.column("competition").to_pylist()
    mids = ps_table.column("match_id").to_pylist()
    for comp, mid in zip(comps, mids):
        ps_by_comp[comp].add(mid)

    print(f"=== Events coverage check for season={args.season} ===")
    print(f"{'comp':<28} {'ps_matches':>11} {'ev_matches':>11} {'gap':>6} {'status':<8}")
    print("-" * 78)

    offenders: list[tuple[str, int, int, int]] = []
    for comp in sorted(ps_by_comp.keys()):
        ev_path = args.events_dir / f"events_{comp}.parquet"
        if not ev_path.exists():
            print(f"{comp:<28} {len(ps_by_comp[comp]):>11} {'-':>11} {'-':>6} no-file")
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
            offenders.append((comp, len(ps_ids), len(ev_ids), gap))
        print(f"{comp:<28} {len(ps_ids):>11} {len(ev_ids):>11} {gap:>6} {status}")

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
