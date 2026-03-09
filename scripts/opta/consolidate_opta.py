#!/usr/bin/env python3
"""
Consolidate Opta parquet files into single files per table type.

Reads from: opta/{table_type}/{league}/{season}.parquet
Writes to:  opta/opta_{table_type}.parquet
           opta/events_consolidated/events_{league}.parquet (per-league events)

Uses a league-by-league approach to minimize memory usage: splits existing
consolidated files into per-league temp files, processes one league at a
time, and writes output via PyArrow ParquetWriter. This keeps peak memory
at ~1 league's worth of data instead of the entire dataset.
"""

import gc
import json
import logging
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.lib
import pyarrow.parquet as pq

from competition_metadata import get_competition_metadata, PANNA_ALIASES

logger = logging.getLogger(__name__)


def _get_dedup_cols(table_type):
    """Return deduplication columns for a given table type."""
    if table_type in ('shot_events', 'match_events'):
        return ['match_id', 'event_id']
    elif table_type == 'events':
        return ['match_id', 'event_type', 'minute', 'player_id']
    elif table_type == 'fixtures':
        return ['match_id']
    else:
        # player_stats, lineups, shots, fixtures (all other table types)
        return ['match_id', 'player_id']


def _split_existing_by_league(parquet_file, temp_dir, batch_size=50_000):
    """Split a consolidated parquet into per-league temp files.

    Reads the file in batches to avoid loading it entirely into memory.
    Uses the file's declared schema for all writers to avoid data loss
    when columns only appear in certain row ranges.
    Returns total row count of the existing file.
    """
    pf = pq.ParquetFile(parquet_file)
    file_schema = pf.schema_arrow
    writers = {}
    total = 0

    try:
        for batch in pf.iter_batches(batch_size=batch_size):
            table = pa.Table.from_batches([batch])
            total += len(table)

            if 'competition' not in table.column_names:
                raise ValueError(
                    f"Existing file {parquet_file} is missing 'competition' column. "
                    f"Columns found: {table.column_names}"
                )

            competitions = pc.unique(table.column('competition')).to_pylist()
            for league in competitions:
                mask = pc.equal(table.column('competition'), league)
                filtered = table.filter(mask)
                if len(filtered) == 0:
                    continue

                filtered = _align_table(filtered, file_schema)
                if league not in writers:
                    league_file = temp_dir / f"{league}.parquet"
                    writers[league] = pq.ParquetWriter(str(league_file), file_schema)
                writers[league].write_table(filtered)

            del table
        gc.collect()
    finally:
        for w in writers.values():
            w.close()
        del writers
        gc.collect()

    return total


def _align_table(table, target_schema):
    """Align a PyArrow table to match the target schema.

    Adds missing columns as null, drops extra columns, and casts types.
    Tries safe casting first, then unsafe, then null-fills as last resort.
    """
    columns = {}
    for field in target_schema:
        if field.name in table.column_names:
            col = table.column(field.name)
            if col.type != field.type:
                try:
                    col = col.cast(field.type)
                except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                    try:
                        col = col.cast(field.type, safe=False)
                    except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as e:
                        print(f"    WARNING: Column '{field.name}' could not be cast from "
                              f"{col.type} to {field.type}, filling with nulls: {e}")
                        col = pa.nulls(len(table), type=field.type)
            columns[field.name] = col
        else:
            columns[field.name] = pa.nulls(len(table), type=field.type)
    return pa.table(columns)


def _build_unified_schema(existing_file, new_files):
    """Build a unified schema from all source files.

    Handles type conflicts (e.g., int64 vs float64) by promoting all
    numeric conflicts to float64. Falls back to string if types are
    truly incompatible (e.g., timestamp vs bool).
    """
    schemas = []
    if existing_file and existing_file.exists():
        try:
            schemas.append(pq.read_schema(existing_file))
        except (OSError, pa.ArrowInvalid, pa.ArrowNotImplementedError) as e:
            print(f"  WARNING: Could not read schema from existing {existing_file}: {e}")
    for f in new_files:
        try:
            schemas.append(pq.read_schema(f))
        except (OSError, pa.ArrowInvalid, pa.ArrowNotImplementedError) as e:
            print(f"  WARNING: Could not read schema from {f}: {e}")

    if not schemas:
        return None

    try:
        return pa.unify_schemas(schemas, promote_options='default')
    except pa.ArrowTypeError:
        # Manual resolution: collect all fields, resolve conflicts
        field_types = {}
        for schema in schemas:
            for field in schema:
                if field.name not in field_types:
                    field_types[field.name] = field.type
                elif field_types[field.name] != field.type:
                    existing_type = field_types[field.name]
                    new_type = field.type
                    numeric_types = {pa.int8(), pa.int16(), pa.int32(), pa.int64(),
                                     pa.uint8(), pa.uint16(), pa.uint32(), pa.uint64(),
                                     pa.float16(), pa.float32(), pa.float64()}
                    if existing_type in numeric_types and new_type in numeric_types:
                        field_types[field.name] = pa.float64()
                    else:
                        print(f"  WARNING: Type conflict for '{field.name}': "
                              f"{existing_type} vs {new_type}, falling back to string")
                        field_types[field.name] = pa.string()

        fields = [pa.field(name, dtype) for name, dtype in field_types.items()]
        return pa.schema(fields)


def _cleanup_temp_dir(temp_dir):
    """Remove temp directory, logging failures instead of ignoring them."""
    try:
        shutil.rmtree(temp_dir)
    except OSError as e:
        print(f"  WARNING: Could not clean up temp dir {temp_dir}: {e}")


def _cast_mixed_to_string(col):
    """Cast object-dtype column to string, preserving nulls.

    Whole floats become '2' not '2.0' — important for columns like score
    that may contain a mix of int, float, and string values across seasons.
    """
    return col.where(col.isna(), col.apply(
        lambda v: str(int(v)) if isinstance(v, float) and v == int(v) else str(v)
    ))


def consolidate_events_by_league(opta_dir="opta", output_dir="opta"):
    """Consolidate match_events by league (too large for single file).

    Merges existing consolidated per-league files with newly scraped
    hierarchical files. Uses PyArrow streaming to avoid loading entire
    league histories into memory.

    Approach: read new season files into pandas (small), collect their
    match_ids, then stream-read the existing consolidated file in batches,
    filtering OUT rows whose match_id is in the new set (whole-match
    replacement ensures re-scraped matches fully supersede old data).
    New data is internally deduped by match_id + event_id. Write filtered
    existing batches + new data via ParquetWriter (zstd compression).
    Peak memory: ~batch_size rows + new season data for the league.
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
        existing_file = output_path / f"events_{league}.parquet"

        if not parquet_files and not existing_file.exists():
            continue

        # Skip leagues with no new data — existing consolidated file is already correct
        if not parquet_files:
            print(f"  {league}: No new data, skipping")
            continue

        # Phase 1: Read new season files into pandas (small — just recent data)
        new_dfs = []
        for f in parquet_files:
            try:
                df = pd.read_parquet(f)
                df['competition'] = league
                df['season'] = f.stem
                new_dfs.append(df)
            except (pd.errors.ParserError, FileNotFoundError, OSError, ValueError,
                    pyarrow.lib.ArrowInvalid) as e:
                print(f"  ERROR: Failed to read {f}: {e}")
                errors += 1

        new_dfs = [df for df in new_dfs if not df.empty]
        if not new_dfs and not existing_file.exists():
            continue

        # Concat and deduplicate new data by match_id + event_id
        if new_dfs:
            new_df = pd.concat(new_dfs, ignore_index=True)
            del new_dfs
            if 'match_id' not in new_df.columns:
                print(f"  ERROR: {league} new data missing 'match_id' column. "
                      f"Columns: {list(new_df.columns)}. Skipping league.")
                del new_df
                errors += 1
                continue
            if 'event_id' in new_df.columns:
                new_df = new_df.drop_duplicates(subset=['match_id', 'event_id'], keep='last')
            new_match_ids = set(new_df['match_id'].unique())
        else:
            new_df = None
            new_match_ids = set()

        # Phase 2: Build unified schema from existing + new files
        unified_schema = _build_unified_schema(
            existing_file if existing_file.exists() else None,
            parquet_files
        )
        if unified_schema is None:
            print(f"  ERROR: Could not determine schema for {league}")
            if new_df is not None:
                del new_df
            errors += 1
            continue

        # Ensure competition and season columns are in the schema
        for col_name in ('competition', 'season'):
            if col_name not in unified_schema.names:
                unified_schema = unified_schema.append(pa.field(col_name, pa.string()))

        # Phase 3: Stream-write output — filtered existing batches, then new data
        output_file = output_path / f"events_{league}.parquet"
        temp_output = output_file.with_suffix('.parquet.new')
        writer = None
        existing_count = 0
        total_rows = 0
        batch_size = 50_000

        try:
            # Stream existing file, filtering out match_ids that will be replaced
            if existing_file.exists():
                try:
                    pf = pq.ParquetFile(existing_file)
                    existing_count = pf.metadata.num_rows
                    print(f"  {league}: Streaming {existing_count:,} existing rows")

                    for batch in pf.iter_batches(batch_size=batch_size):
                        table = pa.Table.from_batches([batch])

                        # Filter out rows whose match_id is in the new data
                        if new_match_ids and 'match_id' in table.column_names:
                            match_ids = table.column('match_id')
                            # Build mask: keep rows NOT in new_match_ids
                            mask = pc.invert(pc.is_in(match_ids, pa.array(list(new_match_ids))))
                            table = table.filter(mask)

                        if len(table) == 0:
                            del table
                            continue

                        table = _align_table(table, unified_schema)
                        if writer is None:
                            writer = pq.ParquetWriter(
                                str(temp_output), unified_schema, compression='zstd'
                            )
                        writer.write_table(table)
                        total_rows += len(table)
                        del table

                    del pf
                    gc.collect()
                except (pd.errors.ParserError, FileNotFoundError, OSError, ValueError,
                        pyarrow.lib.ArrowInvalid) as e:
                    print(f"  ERROR: Failed to stream existing {existing_file}: {e}")
                    print(f"  Skipping {league} to prevent data loss")
                    errors += 1
                    if new_df is not None:
                        del new_df
                    # Close writer and clean up partial temp file
                    if writer:
                        try:
                            writer.close()
                        except Exception:
                            pass
                        writer = None  # prevent double-close in finally
                    gc.collect()
                    try:
                        temp_output.unlink(missing_ok=True)
                    except OSError:
                        pass
                    continue

            # Append new data
            if new_df is not None and not new_df.empty:
                # Cast mixed-type columns to string before PyArrow conversion
                # (unified schema may specify string for columns with type conflicts,
                # e.g. score columns that are int in some seasons and string in others)
                if unified_schema is not None:
                    for field in unified_schema:
                        if field.type == pa.string() and field.name in new_df.columns:
                            col = new_df[field.name]
                            if col.dtype == object:
                                new_df[field.name] = _cast_mixed_to_string(col)
                new_table = pa.Table.from_pandas(new_df, preserve_index=False)
                del new_df
                new_table = _align_table(new_table, unified_schema)
                if writer is None:
                    writer = pq.ParquetWriter(
                        str(temp_output), unified_schema, compression='zstd'
                    )
                writer.write_table(new_table)
                total_rows += len(new_table)
                del new_table
                gc.collect()
            elif new_df is not None:
                del new_df
        except Exception as e:
            print(f"  ERROR: Unexpected error processing {league}: {e}")
            errors += 1
            try:
                temp_output.unlink(missing_ok=True)
            except OSError:
                pass
            gc.collect()
            continue
        finally:
            if writer:
                writer.close()

        if total_rows == 0:
            try:
                temp_output.unlink(missing_ok=True)
            except OSError:
                pass
            print(f"  {league}: No valid data after processing, skipping")
            continue

        # Sanity check: don't write if row count drops more than 10%
        if existing_count > 0 and total_rows < existing_count * 0.9:
            pct_loss = 100 * (1 - total_rows / existing_count)
            print(f"  ERROR: {league} row count dropped from {existing_count:,} to "
                  f"{total_rows:,} ({pct_loss:.1f}% loss). Skipping write to prevent data loss.")
            try:
                temp_output.unlink(missing_ok=True)
            except OSError:
                pass
            errors += 1
            continue

        # Replace existing with new (backup first)
        backup_file = output_file.with_suffix('.parquet.backup')
        backup_created = False
        if output_file.exists():
            try:
                shutil.copy2(output_file, backup_file)
                backup_created = True
            except OSError as e:
                print(f"  ERROR: Failed to create backup for {output_file}: {e}")
                print(f"  Skipping {league} to prevent data loss (cannot backup)")
                try:
                    temp_output.unlink(missing_ok=True)
                except OSError:
                    pass
                errors += 1
                continue

        try:
            shutil.move(str(temp_output), str(output_file))
        except OSError as e:
            print(f"  ERROR: Failed to move temp to {output_file}: {e}")
            if backup_created and backup_file.exists():
                try:
                    shutil.copy2(backup_file, output_file)
                    print(f"  Restored {output_file} from backup")
                except OSError as restore_err:
                    print(f"  CRITICAL: Could not restore backup: {restore_err}")
            errors += 1
            continue

        if backup_created and backup_file.exists():
            try:
                backup_file.unlink()
            except OSError as e:
                logger.debug("Could not remove backup %s: %s (harmless)", backup_file, e)

        size_mb = output_file.stat().st_size / (1024 * 1024)
        print(f"  {league}: {len(parquet_files)} seasons, {total_rows:,} rows, {size_mb:.1f}MB")
        gc.collect()

    print(f"Events consolidation complete: {len(leagues)} leagues")
    return errors


def consolidate_opta(opta_dir="opta", output_dir="opta"):
    """Consolidate all Opta parquet files by table type.

    Uses a league-by-league approach to minimize memory:
    1. Split existing consolidated file into per-league temp files (batch read)
    2. For each league: merge existing + new per-league files, dedupe
    3. Write output via ParquetWriter (one league at a time in memory)

    Peak memory: ~1 league's data instead of the entire dataset.
    """
    opta_path = Path(opta_dir)
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    if not opta_path.exists():
        print(f"ERROR: Opta directory not found: {opta_dir}")
        return 1

    # Find all table types (subdirectories of opta/)
    # Exclude: match_events (consolidated separately by league), events_consolidated (output dir),
    # models (pre-trained ML models), xmetrics (generated by panna R pipeline, not Python scraper)
    exclude = {'match_events', 'events_consolidated', 'models', 'xmetrics'}
    table_types = [d.name for d in opta_path.iterdir() if d.is_dir() and d.name not in exclude]
    print(f"Found table types: {table_types}")

    errors = 0
    for table_type in table_types:
        tt_dir = opta_path / table_type
        new_files = list(tt_dir.glob("**/*.parquet"))

        if not new_files:
            print(f"  Skipping {table_type} - no parquet files")
            continue

        existing_path = output_path / f"opta_{table_type}.parquet"
        print(f"Consolidating opta_{table_type}... ({len(new_files)} per-league files)")

        # Group new files by league
        new_by_league = {}
        for f in new_files:
            league = f.parent.name
            new_by_league.setdefault(league, []).append(f)

        # Phase 1: Split existing consolidated into per-league temp files
        temp_dir = Path(tempfile.mkdtemp(prefix=f"opta_{table_type}_"))
        existing_total = 0
        existing_leagues = set()

        if existing_path.exists():
            try:
                print(f"  Splitting existing {existing_path.name} by league...")
                existing_total = _split_existing_by_league(existing_path, temp_dir)
                existing_leagues = {f.stem for f in temp_dir.glob("*.parquet")}
                print(f"  Split {existing_total:,} existing rows into {len(existing_leagues)} leagues")
                # Validate split didn't lose rows
                expected_total = pq.read_metadata(existing_path).num_rows
                if existing_total != expected_total:
                    print(f"  WARNING: Split produced {existing_total:,} rows but file has "
                          f"{expected_total:,}. Some rows may have been lost during split.")
            except (OSError, pa.ArrowInvalid, pa.ArrowNotImplementedError,
                    pd.errors.ParserError, ValueError, KeyError) as e:
                print(f"  ERROR: Failed to split existing {existing_path}: {e}")
                print(f"  Skipping {table_type} to prevent data loss")
                _cleanup_temp_dir(temp_dir)
                errors += 1
                continue

        # Phase 2: Determine unified schema
        unified_schema = _build_unified_schema(existing_path, new_files)
        if unified_schema is None:
            print(f"  ERROR: Could not determine schema for {table_type}")
            _cleanup_temp_dir(temp_dir)
            errors += 1
            continue

        # Ensure competition and season columns are in the schema
        for col_name in ('competition', 'season'):
            if col_name not in unified_schema.names:
                unified_schema = unified_schema.append(pa.field(col_name, pa.string()))

        # Phase 3: Process each league, write to output
        all_leagues = sorted(existing_leagues | set(new_by_league.keys()))
        dedup_cols = _get_dedup_cols(table_type)

        output_file = output_path / f"opta_{table_type}.parquet"
        temp_output = output_file.with_suffix('.parquet.new')
        writer = None
        total_rows = 0

        try:
            for league in all_leagues:
                dfs = []

                # Load existing data for this league from temp file
                league_temp = temp_dir / f"{league}.parquet"
                if league_temp.exists():
                    try:
                        dfs.append(pd.read_parquet(league_temp))
                    except (pd.errors.ParserError, FileNotFoundError, OSError, ValueError,
                            pyarrow.lib.ArrowInvalid) as e:
                        print(f"  ERROR: Failed to read existing data for {league} from temp: {e}")
                        print(f"  Existing {league} data will be LOST in output.")
                        errors += 1

                # Load new per-league files
                for f in new_by_league.get(league, []):
                    try:
                        df = pd.read_parquet(f)
                        df['competition'] = league
                        df['season'] = f.stem
                        dfs.append(df)
                    except (pd.errors.ParserError, FileNotFoundError, OSError, ValueError,
                            pyarrow.lib.ArrowInvalid) as e:
                        print(f"  ERROR: Failed to read {f}: {e}")
                        errors += 1

                non_empty = [df for df in dfs if not df.empty]
                if not non_empty:
                    del dfs
                    continue
                dfs = non_empty

                # Concat within this league
                league_df = pd.concat(dfs, ignore_index=True)
                del dfs

                # Dedupe within this league
                valid_dedup = [c for c in dedup_cols if c in league_df.columns]
                if valid_dedup and 'match_id' in league_df.columns:
                    before = len(league_df)
                    # For events table, also use 'second' if available
                    if table_type == 'events' and 'second' in league_df.columns:
                        if 'second' not in valid_dedup:
                            valid_dedup.append('second')
                    league_df = league_df.drop_duplicates(subset=valid_dedup, keep='last')
                    dupes = before - len(league_df)
                    if dupes > 0:
                        print(f"    {league}: removed {dupes:,} duplicates")

                # Cast mixed-type columns to string before PyArrow conversion
                # (unified schema may specify string for columns with type conflicts,
                # e.g. score columns that are int in some seasons and string in others)
                if unified_schema is not None:
                    for field in unified_schema:
                        if field.type == pa.string() and field.name in league_df.columns:
                            col = league_df[field.name]
                            if col.dtype == object:
                                league_df[field.name] = _cast_mixed_to_string(col)

                # Convert to PyArrow and align schema
                table = pa.Table.from_pandas(league_df, preserve_index=False)
                del league_df
                table = _align_table(table, unified_schema)

                if writer is None:
                    writer = pq.ParquetWriter(str(temp_output), unified_schema)
                writer.write_table(table)
                total_rows += len(table)
                del table
                gc.collect()
        finally:
            if writer:
                writer.close()
            _cleanup_temp_dir(temp_dir)

        if total_rows == 0:
            try:
                temp_output.unlink(missing_ok=True)
            except OSError:
                pass
            print(f"  Skipping {table_type} - no valid data after processing")
            continue

        # Sanity check: don't write if row count drops more than 10%
        if existing_total > 0 and total_rows < existing_total * 0.9:
            pct_loss = 100 * (1 - total_rows / existing_total)
            print(f"  ERROR: {table_type} row count dropped from {existing_total:,} to "
                  f"{total_rows:,} ({pct_loss:.1f}% loss). Skipping write to prevent data loss.")
            try:
                temp_output.unlink(missing_ok=True)
            except OSError:
                pass
            errors += 1
            continue

        # Replace existing with new (backup first)
        backup_file = output_file.with_suffix('.parquet.backup')
        backup_created = False
        if output_file.exists():
            try:
                shutil.copy2(output_file, backup_file)
                backup_created = True
            except OSError as e:
                print(f"  ERROR: Failed to create backup for {output_file}: {e}")
                print(f"  Skipping {table_type} to prevent data loss (cannot backup)")
                try:
                    temp_output.unlink(missing_ok=True)
                except OSError:
                    pass
                errors += 1
                continue

        try:
            shutil.move(str(temp_output), str(output_file))
        except OSError as e:
            print(f"  ERROR: Failed to move temp to {output_file}: {e}")
            if backup_created and backup_file.exists():
                try:
                    shutil.copy2(backup_file, output_file)
                    print(f"  Restored {output_file} from backup")
                except OSError as restore_err:
                    print(f"  CRITICAL: Could not restore backup: {restore_err}")
            errors += 1
            continue

        if backup_created and backup_file.exists():
            try:
                backup_file.unlink()
            except OSError as e:
                logger.debug("Could not remove backup %s: %s (harmless)", backup_file, e)

        size_mb = output_file.stat().st_size / (1024 * 1024)
        new_count = sum(len(files) for files in new_by_league.values())
        print(f"  Wrote {output_file}: {total_rows:,} rows, {size_mb:.1f} MB "
              f"(existing={existing_total:,}, {new_count} new files, {len(all_leagues)} leagues)")

        gc.collect()

    print("Consolidation complete!")
    return errors


def generate_catalog(opta_dir="opta", manifest_path="opta-manifest.parquet",
                     output_path="opta/opta-catalog.json"):
    """Generate a JSON catalog of all available Opta competitions and data.

    Combines three data sources to build a comprehensive catalog:
    1. Manifest (per-match has_* flags for match counts)
    2. Consolidated parquets (competition/season columns per data type)
    3. Per-league event files in events_consolidated/
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
        except (pd.errors.ParserError, FileNotFoundError, OSError, ValueError,
                pyarrow.lib.ArrowInvalid, KeyError) as e:
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
        except (pd.errors.ParserError, FileNotFoundError, OSError, ValueError,
                pyarrow.lib.ArrowInvalid, KeyError) as e:
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
            except (pd.errors.ParserError, FileNotFoundError, OSError, ValueError,
                    pyarrow.lib.ArrowInvalid, KeyError) as e:
                print(f"Catalog: warning scanning {f}: {e}")

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

    if not competitions:
        print("Catalog: ERROR - no competitions found, skipping catalog write")
        return 1

    catalog = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "competitions": competitions,
        "panna_aliases": PANNA_ALIASES,
    }

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(output, "w") as f:
            json.dump(catalog, f, indent=2)
        size_kb = output.stat().st_size / 1024
        print(f"Catalog: wrote {output} ({len(competitions)} competitions, {size_kb:.1f} KB)")
    except OSError as e:
        print(f"Catalog: ERROR writing {output}: {e}")
        return 1

    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    errors = consolidate_opta()
    errors += consolidate_events_by_league()

    # Generate data catalog (non-fatal)
    try:
        catalog_errors = generate_catalog()
        if catalog_errors:
            logger.warning("Catalog generation had errors")
    except (OSError, ValueError, KeyError, TypeError, pd.errors.ParserError, pyarrow.lib.ArrowInvalid) as e:
        logger.warning("Catalog generation failed: %s", e, exc_info=True)

    if errors:
        logger.error("%d error(s) occurred during consolidation", errors)
        sys.exit(1)
