#!/usr/bin/env python3
"""
Consolidate Opta parquet files into single files per table type.

Reads from: opta/{table_type}/{league}/{season}.parquet
Writes to:  opta/opta_{table_type}.parquet
           opta/events_consolidated/events_{league}.parquet (per-league events)
"""

import json
import logging
import shutil
import sys
from datetime import datetime, timezone

import pandas as pd
from pathlib import Path

from competition_metadata import get_competition_metadata, PANNA_ALIASES

logger = logging.getLogger(__name__)


def consolidate_events_by_league(opta_dir="opta", output_dir="opta"):
    """Consolidate match_events by league (too large for single file).

    Merges existing consolidated per-league files with newly scraped
    hierarchical files, deduplicating by match_id + event_id.
    """
    opta_path = Path(opta_dir)
    events_dir = opta_path / "match_events"
    output_path = Path(output_dir) / "events_consolidated"
    output_path.mkdir(parents=True, exist_ok=True)

    # Find all leagues (from both new data and existing consolidated files)
    new_leagues = {d.name for d in events_dir.iterdir() if d.is_dir()} if events_dir.exists() else set()
    existing_leagues = set()
    for f in output_path.glob("events_*.parquet"):
        existing_leagues.add(f.stem.replace("events_", ""))
    leagues = sorted(new_leagues | existing_leagues)
    print(f"Consolidating match_events for {len(leagues)} leagues...")

    errors = 0
    for league in leagues:
        league_dir = events_dir / league
        parquet_files = list(league_dir.glob("*.parquet")) if league_dir.exists() else []

        # Read existing consolidated file first (contains historical data)
        dfs = []
        existing_count = 0
        existing_file = output_path / f"events_{league}.parquet"
        if existing_file.exists():
            try:
                existing_df = pd.read_parquet(existing_file)
                existing_count = len(existing_df)
                dfs.append(existing_df)
                print(f"  {league}: Loaded {existing_count:,} existing rows")
            except (pd.errors.ParserError, FileNotFoundError, OSError, ValueError) as e:
                print(f"  ERROR: Failed to read existing {existing_file}: {e}")
                print(f"  Skipping {league} to prevent data loss")
                errors += 1
                continue

        if not parquet_files and not dfs:
            continue

        for f in parquet_files:
            try:
                df = pd.read_parquet(f)
                df['competition'] = league
                df['season'] = f.stem
                dfs.append(df)
            except (pd.errors.ParserError, FileNotFoundError, OSError, ValueError) as e:
                print(f"  Warning: Error reading {f}: {e}")

        if not dfs:
            continue

        combined = pd.concat(dfs, ignore_index=True)

        # Dedupe by match_id + event_id, keeping latest data
        if 'match_id' in combined.columns and 'event_id' in combined.columns:
            before = len(combined)
            combined = combined.drop_duplicates(subset=['match_id', 'event_id'], keep='last')
            if len(combined) < before:
                print(f"  {league}: Removed {before - len(combined):,} duplicates")

        # Sanity check: don't write if row count drops more than 10%
        if existing_count > 0 and len(combined) < existing_count * 0.9:
            pct_loss = 100 * (1 - len(combined) / existing_count)
            print(f"  ERROR: {league} row count dropped from {existing_count:,} to "
                  f"{len(combined):,} ({pct_loss:.1f}% loss). Skipping write to prevent data loss.")
            errors += 1
            continue

        output_file = output_path / f"events_{league}.parquet"
        # Backup existing file before overwriting
        if output_file.exists():
            backup_file = output_file.with_suffix('.parquet.backup')
            shutil.copy2(output_file, backup_file)
        combined.to_parquet(output_file, index=False, compression='gzip')

        size_mb = output_file.stat().st_size / (1024 * 1024)
        print(f"  {league}: {len(parquet_files)} seasons, {len(combined):,} rows, {size_mb:.1f}MB")

    print(f"Events consolidation complete: {len(leagues)} leagues")
    return errors


def consolidate_opta(opta_dir="opta", output_dir="opta"):
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
        return 0

    # Find all table types (subdirectories of opta/)
    # Exclude match_events (consolidated separately by league), events_consolidated (output dir), and models
    exclude = {'match_events', 'events_consolidated', 'models'}
    table_types = [d.name for d in opta_path.iterdir() if d.is_dir() and d.name not in exclude]
    print(f"Found table types: {table_types}")

    errors = 0
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
        existing_count = 0
        for existing_path in [
            parent_dir / f"opta_{table_type}.parquet",
            output_path / f"opta_{table_type}.parquet",
        ]:
            if existing_path.exists():
                try:
                    existing_df = pd.read_parquet(existing_path)
                    existing_count = len(existing_df)
                    dfs.append(existing_df)
                    print(f"  Loaded {existing_count:,} existing rows from {existing_path}")
                    break  # Successfully loaded - don't try other sources
                except (pd.errors.ParserError, FileNotFoundError, OSError, ValueError) as e:
                    print(f"  Warning: Error reading existing {existing_path}: {e}")
                    # Fall through to try the next path

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
            except (pd.errors.ParserError, FileNotFoundError, OSError, ValueError) as e:
                print(f"  Warning: Error reading {f}: {e}")

        if not dfs:
            print(f"  Skipping {table_type} - no valid data")
            continue

        # Concatenate with pandas (handles type differences across seasons)
        combined = pd.concat(dfs, ignore_index=True)

        # Deduplicate based on table type (keep='last' so new data wins over stale)
        before_count = len(combined)
        if 'match_id' in combined.columns:
            if 'event_id' in combined.columns and table_type in ['shot_events', 'match_events']:
                combined = combined.drop_duplicates(subset=['match_id', 'event_id'], keep='last')
            elif table_type == 'events' and all(c in combined.columns for c in ['match_id', 'event_type', 'minute', 'player_id']):
                dedup_cols = ['match_id', 'event_type', 'minute', 'player_id']
                if 'second' in combined.columns:
                    dedup_cols.append('second')
                combined = combined.drop_duplicates(subset=dedup_cols, keep='last')
            elif 'player_id' in combined.columns:
                combined = combined.drop_duplicates(subset=['match_id', 'player_id'], keep='last')
            else:
                combined = combined.drop_duplicates(subset=['match_id'], keep='last')
            if len(combined) < before_count:
                print(f"  Removed {before_count - len(combined):,} duplicate rows")

        # Sanity check: don't write if row count drops more than 10%
        if existing_count > 0 and len(combined) < existing_count * 0.9:
            pct_loss = 100 * (1 - len(combined) / existing_count)
            print(f"  ERROR: {table_type} row count dropped from {existing_count:,} to "
                  f"{len(combined):,} ({pct_loss:.1f}% loss). Skipping write to prevent data loss.")
            errors += 1
            continue

        # Write consolidated parquet (backup existing first)
        output_file = output_path / f"opta_{table_type}.parquet"
        if output_file.exists():
            backup_file = output_file.with_suffix('.parquet.backup')
            shutil.copy2(output_file, backup_file)
        combined.to_parquet(output_file, index=False)

        size_mb = output_file.stat().st_size / (1024 * 1024)
        print(f"  Wrote {output_file}: {len(combined):,} rows, {size_mb:.1f} MB "
              f"(existing={existing_count:,} + new={before_count - existing_count:,} - dupes={before_count - len(combined):,})")

    print("Consolidation complete!")
    return errors


def generate_catalog(opta_dir="opta", manifest_path="opta-manifest.parquet",
                     output_path="opta/opta-catalog.json"):
    """Generate a JSON catalog of all available Opta competitions and data.

    Combines manifest (per-match has_* flags) with filesystem scan of
    consolidated parquets to build a comprehensive data catalog.
    """
    opta_path = Path(opta_dir)
    manifest_file = Path(manifest_path)

    # Data types we track
    data_types = ["player_stats", "shots", "shot_events", "match_events", "lineups",
                  "events", "fixtures"]

    # 1. Build competition/season info from manifest
    comp_data = {}
    if manifest_file.exists():
        try:
            mf = pd.read_parquet(manifest_file)
            for (comp, season), group in mf.groupby(["competition", "season"]):
                if comp not in comp_data:
                    comp_data[comp] = {"seasons": set(), "n_matches": 0, "data_types": set()}
                comp_data[comp]["seasons"].add(season)
                comp_data[comp]["n_matches"] += len(group)
                # Check has_* flags
                for dt in ["player_stats", "shots", "match_events", "lineups"]:
                    col = f"has_{dt}"
                    if col in group.columns and group[col].any():
                        comp_data[comp]["data_types"].add(dt)
            print(f"Catalog: loaded {len(mf):,} manifest entries across {len(comp_data)} competitions")
        except Exception as e:
            print(f"Catalog: warning reading manifest: {e}")

    # 2. Also scan consolidated parquets for competition/season columns
    for dt in data_types:
        consolidated_file = opta_path / f"opta_{dt}.parquet"
        if not consolidated_file.exists():
            continue
        try:
            df = pd.read_parquet(consolidated_file, columns=["competition", "season"])
            for comp in df["competition"].unique():
                if comp not in comp_data:
                    comp_data[comp] = {"seasons": set(), "n_matches": 0, "data_types": set()}
                comp_data[comp]["data_types"].add(dt)
                comp_data[comp]["seasons"].update(
                    df.loc[df["competition"] == comp, "season"].unique()
                )
        except Exception as e:
            print(f"Catalog: warning scanning {consolidated_file}: {e}")

    # Also scan events_consolidated/ for per-league event files
    events_dir = opta_path / "events_consolidated"
    if events_dir.exists():
        for f in events_dir.glob("events_*.parquet"):
            comp = f.stem.replace("events_", "")
            if comp not in comp_data:
                comp_data[comp] = {"seasons": set(), "n_matches": 0, "data_types": set()}
            comp_data[comp]["data_types"].add("match_events")
            try:
                df = pd.read_parquet(f, columns=["season"])
                comp_data[comp]["seasons"].update(df["season"].unique())
            except Exception:
                pass

    # 3. Build catalog JSON
    competitions = {}
    for code in sorted(comp_data.keys()):
        meta = get_competition_metadata(code)
        info = comp_data[code]
        competitions[code] = {
            "name": meta["name"],
            "country": meta["country"],
            "type": meta["type"],
            "tier": meta["tier"],
            "seasons": sorted(info["seasons"], reverse=True),
            "n_matches": info["n_matches"],
            "data_types": sorted(info["data_types"]),
        }

    catalog = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "competitions": competitions,
        "panna_aliases": PANNA_ALIASES,
    }

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w") as f:
        json.dump(catalog, f, indent=2)

    size_kb = output.stat().st_size / 1024
    print(f"Catalog: wrote {output} ({len(competitions)} competitions, {size_kb:.1f} KB)")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    errors = consolidate_opta()
    errors += consolidate_events_by_league()

    # Generate data catalog
    generate_catalog()

    if errors:
        logger.error("%d error(s) occurred during consolidation", errors)
        sys.exit(1)
