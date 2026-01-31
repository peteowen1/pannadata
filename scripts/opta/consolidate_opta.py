#!/usr/bin/env python3
"""
Consolidate Opta parquet files into single files per table type.

Reads from: opta/{table_type}/{league}/{season}.parquet
Writes to:  consolidated/opta_{table_type}.parquet
           consolidated/match_events/events_{league}.parquet (per-league events)
"""

import os
import glob
import pandas as pd
from pathlib import Path


def consolidate_events_by_league(opta_dir="opta", output_dir="consolidated"):
    """Consolidate match_events by league (too large for single file)."""
    opta_path = Path(opta_dir)
    events_dir = opta_path / "match_events"
    output_path = Path(output_dir) / "match_events"
    output_path.mkdir(parents=True, exist_ok=True)

    if not events_dir.exists():
        print("No match_events directory found")
        return

    # Find all leagues
    leagues = [d.name for d in events_dir.iterdir() if d.is_dir()]
    print(f"Consolidating match_events for {len(leagues)} leagues...")

    for league in sorted(leagues):
        league_dir = events_dir / league
        parquet_files = list(league_dir.glob("*.parquet"))

        if not parquet_files:
            continue

        dfs = []
        for f in parquet_files:
            try:
                df = pd.read_parquet(f)
                df['competition'] = league
                df['season'] = f.stem
                dfs.append(df)
            except Exception as e:
                print(f"  Warning: Error reading {f}: {e}")

        if not dfs:
            continue

        combined = pd.concat(dfs, ignore_index=True)

        # Dedupe by match_id + event_id
        if 'match_id' in combined.columns and 'event_id' in combined.columns:
            before = len(combined)
            combined = combined.drop_duplicates(subset=['match_id', 'event_id'])
            if len(combined) < before:
                print(f"  {league}: Removed {before - len(combined):,} duplicates")

        output_file = output_path / f"events_{league}.parquet"
        combined.to_parquet(output_file, index=False, compression='gzip')

        size_mb = output_file.stat().st_size / (1024 * 1024)
        print(f"  {league}: {len(parquet_files)} seasons, {len(combined):,} rows, {size_mb:.1f}MB")

    print(f"Events consolidation complete: {len(leagues)} leagues")


def consolidate_opta(opta_dir="opta", output_dir="consolidated"):
    """Consolidate all Opta parquet files by table type."""
    opta_path = Path(opta_dir)
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    if not opta_path.exists():
        print(f"Opta directory not found: {opta_dir}")
        return

    # Find all table types (subdirectories of opta/)
    # Exclude match_events - consolidated separately by league (too large for single file)
    table_types = [d.name for d in opta_path.iterdir() if d.is_dir() and d.name != 'match_events']
    print(f"Found table types: {table_types}")

    for table_type in table_types:
        tt_dir = opta_path / table_type
        parquet_files = list(tt_dir.glob("**/*.parquet"))

        if not parquet_files:
            print(f"  Skipping {table_type} - no parquet files")
            continue

        print(f"Consolidating opta_{table_type}... Found {len(parquet_files)} files")

        # Read all into dataframes, adding competition and season columns
        dfs = []
        for f in parquet_files:
            try:
                df = pd.read_parquet(f)
                # Extract competition (league) and season from path
                # Path: opta/{table_type}/{league}/{season}.parquet
                competition = f.parent.name  # e.g., "EPL"
                season = f.stem  # e.g., "2024-2025"
                df['competition'] = competition
                df['season'] = season
                dfs.append(df)
            except Exception as e:
                print(f"  Warning: Error reading {f}: {e}")

        if not dfs:
            print(f"  Skipping {table_type} - no valid data")
            continue

        # Concatenate with pandas (handles type differences across seasons)
        combined = pd.concat(dfs, ignore_index=True)

        # Deduplicate based on table type
        # - Most tables use match_id + player_id
        # - Event tables (shot_events, match_events) use match_id + event_id
        before_count = len(combined)
        if 'match_id' in combined.columns:
            if 'event_id' in combined.columns and table_type in ['shot_events', 'match_events']:
                # Event tables: dedupe by match_id + event_id
                combined = combined.drop_duplicates(subset=['match_id', 'event_id'])
            elif 'player_id' in combined.columns:
                # Player tables: dedupe by match_id + player_id
                combined = combined.drop_duplicates(subset=['match_id', 'player_id'])
            if len(combined) < before_count:
                print(f"  Removed {before_count - len(combined):,} duplicate rows")

        # Write consolidated parquet
        output_file = output_path / f"opta_{table_type}.parquet"
        combined.to_parquet(output_file, index=False)

        size_mb = output_file.stat().st_size / (1024 * 1024)
        print(f"  Wrote {output_file}: {len(combined):,} rows, {size_mb:.1f} MB")

    print("Consolidation complete!")


if __name__ == "__main__":
    consolidate_opta()
    consolidate_events_by_league()
