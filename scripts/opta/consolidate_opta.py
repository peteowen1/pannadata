#!/usr/bin/env python3
"""
Consolidate Opta parquet files into single files per table type.

Reads from: opta/{table_type}/{league}/{season}.parquet
Writes to:  consolidated/opta_{table_type}.parquet
           consolidated/match_events/events_{league}.parquet (per-league events)
"""

import pandas as pd
from pathlib import Path


def consolidate_events_by_league(opta_dir="opta", output_dir="consolidated"):
    """Consolidate match_events by league (too large for single file).

    Merges existing consolidated per-league files with newly scraped
    hierarchical files, deduplicating by match_id + event_id.
    """
    opta_path = Path(opta_dir)
    events_dir = opta_path / "match_events"
    output_path = Path(output_dir) / "match_events"
    output_path.mkdir(parents=True, exist_ok=True)

    # Find all leagues (from both new data and existing consolidated files)
    new_leagues = {d.name for d in events_dir.iterdir() if d.is_dir()} if events_dir.exists() else set()
    existing_leagues = set()
    for f in output_path.glob("events_*.parquet"):
        existing_leagues.add(f.stem.replace("events_", ""))
    leagues = sorted(new_leagues | existing_leagues)
    print(f"Consolidating match_events for {len(leagues)} leagues...")

    for league in leagues:
        league_dir = events_dir / league
        parquet_files = list(league_dir.glob("*.parquet")) if league_dir.exists() else []

        # Read existing consolidated file first (contains historical data)
        dfs = []
        existing_file = output_path / f"events_{league}.parquet"
        if existing_file.exists():
            try:
                existing_df = pd.read_parquet(existing_file)
                dfs.append(existing_df)
                print(f"  {league}: Loaded {len(existing_df):,} existing rows")
            except Exception as e:
                print(f"  Warning: Error reading existing {existing_file}: {e}")

        if not parquet_files and not dfs:
            continue

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
    """Consolidate all Opta parquet files by table type.

    Merges existing consolidated files with newly scraped hierarchical files,
    deduplicating by appropriate keys. In GHA, existing consolidated files are
    downloaded to the parent directory (e.g., data/opta_player_stats.parquet)
    before consolidation runs.
    """
    opta_path = Path(opta_dir)
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    # Parent directory where GHA downloads existing consolidated files
    parent_dir = opta_path.parent

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

        # Read existing consolidated file first (contains historical data)
        # Check both parent dir (where GHA downloads) and output dir (from previous run)
        dfs = []
        for existing_path in [
            parent_dir / f"opta_{table_type}.parquet",
            output_path / f"opta_{table_type}.parquet",
        ]:
            if existing_path.exists():
                try:
                    existing_df = pd.read_parquet(existing_path)
                    dfs.append(existing_df)
                    print(f"  Loaded {len(existing_df):,} existing rows from {existing_path}")
                except Exception as e:
                    print(f"  Warning: Error reading existing {existing_path}: {e}")
                break  # Only load from one source to avoid double-counting

        # Read new hierarchical data, adding competition and season columns
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
            else:
                # Tables without player_id or event_id (e.g. fixtures): dedupe by match_id
                combined = combined.drop_duplicates(subset=['match_id'])
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
