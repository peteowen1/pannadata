"""
Reprocess cached Opta JSON to fix lineup minutes and event timing.

Reads cached raw JSON from data/raw/{league}/{season}/*_stats.json,
re-extracts lineups and events using the fixed scraper methods,
and overwrites the affected parquet files.

Usage:
    python reprocess_lineups.py                          # All leagues, all seasons
    python reprocess_lineups.py --leagues EPL La_Liga    # Specific leagues
    python reprocess_lineups.py --seasons 2013-2014      # Specific seasons
    python reprocess_lineups.py --dry-run                # Preview without overwriting
"""

import argparse
import json
import sys
from pathlib import Path
from dataclasses import asdict

import pandas as pd

from opta_scraper import OptaScraper


def get_raw_dir() -> Path:
    return Path(__file__).parent / "data" / "raw"


def get_opta_dir() -> Path:
    return Path(__file__).parent.parent.parent / "data" / "opta"


def reprocess_season(scraper: OptaScraper, league: str, season: str,
                     raw_dir: Path, opta_dir: Path, dry_run: bool = False) -> dict:
    """Reprocess a single league-season from cached JSON."""
    season_raw = raw_dir / league / season
    if not season_raw.exists():
        return {"skipped": True, "reason": "no raw dir"}

    stats_files = sorted(season_raw.glob("*_stats.json"))
    if not stats_files:
        return {"skipped": True, "reason": "no stats files"}

    # Load existing parquets for before/after comparison
    lineups_path = opta_dir / "lineups" / league / f"{season}.parquet"
    events_path = opta_dir / "events" / league / f"{season}.parquet"

    before_stats = {}
    if lineups_path.exists():
        try:
            old_lineups = pd.read_parquet(lineups_path)
            starters = old_lineups[old_lineups["is_starter"]]
            before_stats["total_players"] = len(old_lineups)
            before_stats["starters_with_mins"] = int((starters["minutes_played"] > 0).sum())
            before_stats["total_starters"] = len(starters)
        except Exception:
            before_stats = {}

    # Reprocess all matches
    all_lineups = []
    all_events = []
    match_count = 0
    warnings = []

    for stats_file in stats_files:
        try:
            with open(stats_file, encoding="utf-8") as f:
                match_data = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            warnings.append(f"  Failed to read {stats_file.name}: {e}")
            continue

        match_id = match_data.get("matchInfo", {}).get("id", "")
        if not match_id:
            continue

        # Re-extract with fixed scraper
        lineups = scraper.extract_lineups(match_data)
        events = scraper.extract_match_events(match_data)

        all_lineups.extend([asdict(l) for l in lineups])
        all_events.extend([asdict(e) for e in events])
        match_count += 1

        # Validate
        lineup_warns = scraper.validate_lineups(lineups, match_id)
        event_warns = scraper.validate_events(events, match_id)
        warnings.extend(lineup_warns)
        warnings.extend(event_warns)

    if not all_lineups:
        return {"skipped": True, "reason": "no lineups extracted"}

    # Build DataFrames
    new_lineups_df = pd.DataFrame(all_lineups)
    new_events_df = pd.DataFrame(all_events) if all_events else pd.DataFrame()

    # After stats
    after_stats = {}
    starters = new_lineups_df[new_lineups_df["is_starter"]]
    after_stats["total_players"] = len(new_lineups_df)
    after_stats["starters_with_mins"] = int((starters["minutes_played"] > 0).sum())
    after_stats["total_starters"] = len(starters)

    # Compute coverage
    before_pct = (
        f"{before_stats['starters_with_mins']}/{before_stats['total_starters']}"
        if before_stats else "N/A"
    )
    after_pct = (
        f"{after_stats['starters_with_mins']}/{after_stats['total_starters']}"
    )

    # Save
    if not dry_run:
        lineups_path.parent.mkdir(parents=True, exist_ok=True)
        new_lineups_df.to_parquet(lineups_path, index=False)

        if not new_events_df.empty:
            events_path.parent.mkdir(parents=True, exist_ok=True)
            new_events_df.to_parquet(events_path, index=False)

    return {
        "matches": match_count,
        "lineups": len(new_lineups_df),
        "events": len(new_events_df),
        "before_mins_coverage": before_pct,
        "after_mins_coverage": after_pct,
        "warnings": warnings,
    }


def main():
    parser = argparse.ArgumentParser(description="Reprocess cached Opta JSON for lineup/event fixes")
    parser.add_argument("--leagues", nargs="+", help="Specific leagues to reprocess")
    parser.add_argument("--seasons", nargs="+", help="Specific seasons to reprocess")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing")
    args = parser.parse_args()

    sys.stdout.reconfigure(encoding="utf-8")

    raw_dir = get_raw_dir()
    opta_dir = get_opta_dir()
    scraper = OptaScraper()

    if not raw_dir.exists():
        print(f"Raw data directory not found: {raw_dir}")
        return

    # Discover leagues/seasons from raw dir
    all_leagues = sorted([d.name for d in raw_dir.iterdir() if d.is_dir()])
    if args.leagues:
        all_leagues = [l for l in all_leagues if l in args.leagues]

    total_fixed = 0
    total_seasons = 0

    for league in all_leagues:
        league_dir = raw_dir / league
        all_seasons = sorted([d.name for d in league_dir.iterdir() if d.is_dir()])
        if args.seasons:
            all_seasons = [s for s in all_seasons if s in args.seasons]

        for season in all_seasons:
            result = reprocess_season(scraper, league, season, raw_dir, opta_dir, args.dry_run)

            if result.get("skipped"):
                continue

            total_seasons += 1
            before = result["before_mins_coverage"]
            after = result["after_mins_coverage"]
            status = "DRY-RUN" if args.dry_run else "SAVED"

            # Only print if there was a change or if early season
            changed = before != after
            prefix = "*" if changed else " "
            print(
                f"{prefix} {league:25s} {season:12s} | "
                f"{result['matches']:3d} matches | "
                f"mins coverage: {before:>12s} -> {after:>12s} | "
                f"{result['events']:4d} events | {status}"
            )

            if changed:
                total_fixed += 1

            for w in result.get("warnings", []):
                print(w)

    print(f"\nDone: {total_seasons} seasons processed, {total_fixed} had coverage changes")
    if args.dry_run:
        print("(dry-run mode — no files were written)")


if __name__ == "__main__":
    main()
