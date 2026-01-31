#!/usr/bin/env python3
"""
Build Opta manifest from existing parquet files.

The manifest tracks which matches have been scraped, allowing the scraper
to quickly determine what's new without downloading gigabytes of data.

Manifest structure (parquet):
- match_id: str (unique identifier)
- competition: str (league name)
- season: str (e.g., "2024-2025")
- date: str (match date)
- home_team: str
- away_team: str
- has_player_stats: bool
- has_shots: bool
- has_match_events: bool
- has_lineups: bool
"""

import pandas as pd
from pathlib import Path
import argparse


def build_manifest(opta_dir: str = "../../data/opta", output_path: str = None):
    """Build manifest from existing parquet files."""
    opta_path = Path(opta_dir)

    if not opta_path.exists():
        print(f"Opta directory not found: {opta_dir}")
        return None

    # We'll build the manifest from match_events (most complete table)
    # and cross-reference with other tables
    manifest_records = []

    # Track which match_ids exist in each table type
    table_match_ids = {
        'player_stats': set(),
        'shots': set(),
        'match_events': set(),
        'lineups': set(),
    }

    # Scan each table type
    for table_type in table_match_ids.keys():
        table_dir = opta_path / table_type
        if not table_dir.exists():
            continue

        for league_dir in table_dir.iterdir():
            if not league_dir.is_dir():
                continue
            competition = league_dir.name

            for parquet_file in league_dir.glob("*.parquet"):
                season = parquet_file.stem
                try:
                    df = pd.read_parquet(parquet_file, columns=['match_id'])
                    match_ids = set(df['match_id'].unique())

                    for mid in match_ids:
                        table_match_ids[table_type].add((mid, competition, season))

                except Exception as e:
                    print(f"  Warning: Error reading {parquet_file}: {e}")

    print(f"Found matches by table type:")
    for table_type, matches in table_match_ids.items():
        print(f"  {table_type}: {len(matches)} match-league-season combinations")

    # Build manifest from match_events (primary source) with metadata
    # For match metadata, we need to read from a table that has team info
    # Let's check lineups or player_stats for team names

    # First pass: collect all unique (match_id, competition, season) tuples
    all_matches = set()
    for matches in table_match_ids.values():
        all_matches.update(matches)

    print(f"\nTotal unique match-league-season combinations: {len(all_matches)}")

    # Build manifest records
    for match_id, competition, season in sorted(all_matches):
        has_events = (match_id, competition, season) in table_match_ids['match_events']
        record = {
            'match_id': match_id,
            'competition': competition,
            'season': season,
            'has_player_stats': (match_id, competition, season) in table_match_ids['player_stats'],
            'has_shots': (match_id, competition, season) in table_match_ids['shots'],
            'has_match_events': has_events,
            'has_lineups': (match_id, competition, season) in table_match_ids['lineups'],
            'event_unavailable': not has_events,  # Mark as unavailable if no events
        }
        manifest_records.append(record)

    manifest_df = pd.DataFrame(manifest_records)

    # Output path
    if output_path is None:
        output_path = opta_path.parent / "opta-manifest.parquet"
    else:
        output_path = Path(output_path)

    manifest_df.to_parquet(output_path, index=False, compression='gzip')

    size_kb = output_path.stat().st_size / 1024
    print(f"\nManifest saved to: {output_path}")
    print(f"  Records: {len(manifest_df):,}")
    print(f"  Size: {size_kb:.1f} KB")
    print(f"  Competitions: {manifest_df['competition'].nunique()}")
    print(f"  Seasons: {manifest_df['season'].nunique()}")

    # Summary by competition
    print("\nMatches per competition (top 20):")
    comp_counts = manifest_df.groupby('competition').size().sort_values(ascending=False)
    for comp, count in comp_counts.head(20).items():
        print(f"  {comp}: {count:,}")

    return manifest_df


def main():
    parser = argparse.ArgumentParser(description="Build Opta manifest from existing parquet files")
    parser.add_argument("--opta-dir", default="../../data/opta",
                       help="Path to opta data directory")
    parser.add_argument("--output", "-o",
                       help="Output path for manifest (default: data/opta-manifest.parquet)")
    args = parser.parse_args()

    build_manifest(args.opta_dir, args.output)


if __name__ == "__main__":
    main()
