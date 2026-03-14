#!/usr/bin/env python3
"""
One-time fix: consolidate local per-season match_events into per-league
parquet files and upload to opta-latest release.

Fixes EPL, Championship, and Scottish_Premiership which were bootstrapped
with incomplete data on the release.

Run from pannadata root:
    python scripts/opta/fix_events_upload.py
"""

import subprocess
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

LEAGUES = ["EPL", "Championship", "Scottish_Premiership"]
DATA_DIR = Path("data/opta/match_events")
OUTPUT_DIR = Path("data/opta/events_consolidated")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def consolidate_league(league):
    league_dir = DATA_DIR / league
    if not league_dir.exists():
        print(f"  {league}: directory not found, skipping")
        return None

    parquet_files = sorted(league_dir.glob("*.parquet"))
    if not parquet_files:
        print(f"  {league}: no parquet files found")
        return None

    dfs = []
    for f in parquet_files:
        try:
            df = pd.read_parquet(f)
            df["competition"] = league
            df["season"] = f.stem
            dfs.append(df)
            print(f"    {f.stem}: {len(df):,} rows")
        except Exception as e:
            print(f"    ERROR reading {f}: {e}")

    if not dfs:
        return None

    combined = pd.concat(dfs, ignore_index=True)

    # Deduplicate by match_id + event_id
    if "match_id" in combined.columns and "event_id" in combined.columns:
        before = len(combined)
        combined = combined.drop_duplicates(subset=["match_id", "event_id"], keep="last")
        dupes = before - len(combined)
        if dupes > 0:
            print(f"    Removed {dupes:,} duplicates")

    output_file = OUTPUT_DIR / f"events_{league}.parquet"
    pq.write_table(
        pa.Table.from_pandas(combined, preserve_index=False),
        str(output_file),
        compression="zstd",
    )
    size_mb = output_file.stat().st_size / (1024 * 1024)
    print(f"  {league}: {len(combined):,} rows, {len(parquet_files)} seasons -> {size_mb:.1f} MB")
    return output_file


def upload_to_release(files):
    for f in files:
        print(f"Uploading {f.name}...")
        result = subprocess.run(
            ["gh", "release", "upload", "opta-latest", str(f), "--clobber",
             "--repo", "peteowen1/pannadata"],
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            print(f"  Uploaded {f.name}")
        else:
            print(f"  ERROR: {result.stderr}")
            return False
    return True


if __name__ == "__main__":
    print("Consolidating local match_events for EPL/Championship/Scottish_Premiership...")
    output_files = []
    for league in LEAGUES:
        print(f"\n{league}:")
        result = consolidate_league(league)
        if result:
            output_files.append(result)

    if not output_files:
        print("\nNo files to upload")
        sys.exit(1)

    print(f"\n{len(output_files)} files ready for upload:")
    for f in output_files:
        size_mb = f.stat().st_size / (1024 * 1024)
        print(f"  {f.name}: {size_mb:.1f} MB")

    response = input("\nUpload to opta-latest release? [y/N] ")
    if response.strip().lower() == "y":
        if upload_to_release(output_files):
            print("\nDone! All files uploaded.")
        else:
            print("\nUpload failed.")
            sys.exit(1)
    else:
        print("Skipping upload. Files are ready in data/opta/events_consolidated/")
