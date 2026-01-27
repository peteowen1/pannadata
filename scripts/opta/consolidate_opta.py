#!/usr/bin/env python3
"""
Consolidate Opta parquet files into single files per table type.

Reads from: opta/{table_type}/{league}/{season}.parquet
Writes to:  consolidated/opta_{table_type}.parquet
"""

import os
import glob
import pandas as pd
from pathlib import Path


def consolidate_opta(opta_dir="opta", output_dir="consolidated"):
    """Consolidate all Opta parquet files by table type."""
    opta_path = Path(opta_dir)
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    if not opta_path.exists():
        print(f"Opta directory not found: {opta_dir}")
        return

    # Find all table types (subdirectories of opta/)
    table_types = [d.name for d in opta_path.iterdir() if d.is_dir()]
    print(f"Found table types: {table_types}")

    for table_type in table_types:
        tt_dir = opta_path / table_type
        parquet_files = list(tt_dir.glob("**/*.parquet"))

        if not parquet_files:
            print(f"  Skipping {table_type} - no parquet files")
            continue

        print(f"Consolidating opta_{table_type}... Found {len(parquet_files)} files")

        # Read all into dataframes
        dfs = []
        for f in parquet_files:
            try:
                dfs.append(pd.read_parquet(f))
            except Exception as e:
                print(f"  Warning: Error reading {f}: {e}")

        if not dfs:
            print(f"  Skipping {table_type} - no valid data")
            continue

        # Concatenate with pandas (handles type differences across seasons)
        combined = pd.concat(dfs, ignore_index=True)

        # Write consolidated parquet
        output_file = output_path / f"opta_{table_type}.parquet"
        combined.to_parquet(output_file, index=False)

        size_mb = output_file.stat().st_size / (1024 * 1024)
        print(f"  Wrote {output_file}: {len(combined):,} rows, {size_mb:.1f} MB")

    print("Consolidation complete!")


if __name__ == "__main__":
    consolidate_opta()
