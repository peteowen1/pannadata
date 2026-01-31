"""
Scrape Opta Leagues - Comprehensive Data Collection

Scrapes all configured leagues from Opta API. Uses a manifest file to track
which matches have already been scraped, avoiding the need to download
gigabytes of data to check for existing matches.

Data is saved to pannadata/data/opta/ with structure:
- pannadata/data/opta/player_stats/{league}/{season}.parquet
- pannadata/data/opta/shots/{league}/{season}.parquet
- pannadata/data/opta/match_events/{league}/{season}.parquet
- pannadata/data/opta/lineups/{league}/{season}.parquet

Usage:
    python scrape_opta.py                           # Scrape all leagues, current season
    python scrape_opta.py --leagues EPL La_Liga     # Specific leagues
    python scrape_opta.py --seasons 2024-2025 2023-2024  # Specific seasons
    python scrape_opta.py --leagues EPL --seasons 2024-2025 2023-2024
    python scrape_opta.py --recent 1                # Most recent season per league
"""

import json
import argparse
import sys
from pathlib import Path
from datetime import datetime
from opta_scraper import OptaScraper, MatchEvent, ShotEvent, PlayerLineup, AllMatchEvent
from dataclasses import asdict
import pandas as pd


def load_manifest(manifest_path: Path, include_unavailable: bool = True) -> tuple:
    """Load existing match IDs from manifest file.

    Args:
        manifest_path: Path to manifest parquet file
        include_unavailable: If True, also skip matches marked as event_unavailable

    Returns:
        Tuple of (complete_matches, unavailable_matches) as sets of (match_id, competition, season)
    """
    if not manifest_path.exists():
        print("No manifest found - will scrape all matches")
        return set(), set()

    try:
        df = pd.read_parquet(manifest_path)

        # Matches with complete event data
        complete = df[df['has_match_events'] == True]
        complete_set = set(zip(complete['match_id'], complete['competition'], complete['season']))

        # Matches where event data was unavailable (404)
        unavailable_set = set()
        if 'event_unavailable' in df.columns:
            unavailable = df[df['event_unavailable'] == True]
            unavailable_set = set(zip(unavailable['match_id'], unavailable['competition'], unavailable['season']))

        print(f"Loaded manifest: {len(complete_set):,} complete, {len(unavailable_set):,} unavailable")
        return complete_set, unavailable_set
    except Exception as e:
        print(f"Warning: Error loading manifest: {e}")
        return set(), set()


def update_manifest(manifest_path: Path, new_matches: list):
    """Update manifest with newly scraped matches.

    new_matches: list of dicts with match_id, competition, season, and has_* flags
    """
    if not new_matches:
        return

    new_df = pd.DataFrame(new_matches)

    if manifest_path.exists():
        existing_df = pd.read_parquet(manifest_path)
        # Combine and deduplicate (keep new records for same match_id/competition/season)
        combined = pd.concat([existing_df, new_df], ignore_index=True)
        combined = combined.drop_duplicates(
            subset=['match_id', 'competition', 'season'],
            keep='last'
        )
    else:
        combined = new_df

    combined.to_parquet(manifest_path, index=False, compression='gzip')
    print(f"Updated manifest: {len(combined):,} total matches")


def get_pannadata_dir():
    """Find pannadata/data directory relative to this script"""
    script_dir = Path(__file__).parent
    # Script is in pannadata/scripts/opta/, go up to pannadata/data
    pannadata_dir = script_dir.parent.parent / "data"
    return pannadata_dir


def load_seasons_config():
    """Load seasons configuration from JSON file"""
    script_dir = Path(__file__).parent
    config_path = script_dir / "seasons.json"

    if not config_path.exists():
        print("Seasons config not found. Running discovery first...")
        from discover_seasons import main as discover_main
        discover_main()

    with open(config_path) as f:
        return json.load(f)


def is_future_season(season_name: str) -> bool:
    """Check if a season is entirely in the future (hasn't started yet).

    Returns True for seasons where the start date is in the future.
    For league seasons like 2024-2025, checks if August 2024 has passed.
    For tournaments like 2029 Morocco, checks if the year has started.
    """
    import re
    from datetime import datetime

    current_date = datetime.now()
    current_year = current_date.year
    current_month = current_date.month

    # Extract year(s) from season name
    years = re.findall(r'20\d\d', season_name)

    if not years:
        return False  # Can't determine, assume not future

    if len(years) >= 2:
        # League format: 2024-2025
        start_year = int(years[0])
        # Season starts in August of the first year
        # If we're past August of start_year, season has started
        if start_year > current_year:
            return True
        if start_year == current_year and current_month < 8:
            return True  # Before August of the start year
        return False
    else:
        # Tournament format: 2025 Morocco or just 2025
        year = int(years[0])
        # Single-year tournaments - consider future if year hasn't started
        return year > current_year


def get_season_date_range(season_name: str) -> tuple:
    """Get start and end dates for a season (Aug-May typical)

    Handles both league seasons (2024-2025) and tournament seasons (2025 Morocco, 2024/2025)
    """
    import re

    # Extract year(s) from season name
    years = re.findall(r'20\d\d', season_name)

    if len(years) >= 2:
        # League format: 2024-2025 or 2024/2025
        year_start = int(years[0])
        year_end = int(years[1])
    elif len(years) == 1:
        # Tournament format: 2025 Morocco or just 2025
        year = int(years[0])
        # For single-year tournaments, cover the full year
        year_start = year - 1  # Start from previous Aug
        year_end = year
    else:
        # Fallback: current year
        from datetime import datetime
        year_start = datetime.now().year - 1
        year_end = datetime.now().year

    # Date ranges to cover full season (API returns max 100 per request)
    date_ranges = [
        (f"{year_start}-08-01", f"{year_start}-09-30"),
        (f"{year_start}-10-01", f"{year_start}-11-30"),
        (f"{year_start}-12-01", f"{year_end}-01-31"),
        (f"{year_end}-02-01", f"{year_end}-03-31"),
        (f"{year_end}-04-01", f"{year_end}-05-31"),
        (f"{year_end}-06-01", f"{year_end}-07-31"),  # For summer tournaments like AFCON
    ]
    return date_ranges


def scrape_season(scraper: OptaScraper, competition: str, season_name: str,
                  season_id: str, complete_matches: set, unavailable_matches: set,
                  force_rescrape: bool = False, retry_unavailable: bool = False):
    """Scrape a full season of data for a competition with all data types.

    Args:
        scraper: OptaScraper instance
        competition: League name (e.g., "EPL")
        season_name: Season identifier (e.g., "2024-2025")
        season_id: Opta season ID
        complete_matches: Set of (match_id, competition, season) tuples with complete data
        unavailable_matches: Set of (match_id, competition, season) tuples with 404 events
        force_rescrape: If True, ignore manifest and rescrape all matches
        retry_unavailable: If True, retry matches that previously had 404 for events

    Returns:
        dict with scraping results and list of new match records for manifest
    """

    print(f"\n{'='*60}")
    print(f"Scraping {competition} {season_name}")
    print(f"Season ID: {season_id}")
    print(f"{'='*60}")

    # Raw JSON cache in scripts/opta/data/raw
    raw_dir = scraper.data_dir / "raw" / competition / season_name

    # Processed parquet goes to pannadata/data/opta/
    pannadata_dir = get_pannadata_dir()
    opta_dir = pannadata_dir / "opta"

    # Create all output directories
    data_types = ["player_stats", "shots", "shot_events", "match_events", "events", "lineups"]
    output_dirs = {dt: opta_dir / dt / competition for dt in data_types}
    for d in output_dirs.values():
        d.mkdir(parents=True, exist_ok=True)

    # Use manifest to check for existing matches
    existing_match_ids = set()
    skipped_unavailable = 0
    if not force_rescrape:
        # Get match_ids with complete data from manifest
        for match_id, comp, season in complete_matches:
            if comp == competition and season == season_name:
                existing_match_ids.add(match_id)

        # Also skip matches where event data was unavailable (404), unless retrying
        if not retry_unavailable:
            for match_id, comp, season in unavailable_matches:
                if comp == competition and season == season_name:
                    if match_id not in existing_match_ids:
                        existing_match_ids.add(match_id)
                        skipped_unavailable += 1

        if existing_match_ids:
            msg = f"Manifest shows {len(existing_match_ids)} matches to skip"
            if skipped_unavailable:
                msg += f" ({skipped_unavailable} with unavailable events)"
            print(msg)

    date_ranges = get_season_date_range(season_name)

    # Collectors for all data types
    all_player_stats = []
    all_shots = []
    all_shot_events = []
    all_match_events = []  # ALL events with x/y coords (~2000/match)
    all_events = []
    all_lineups = []
    new_matches_scraped = 0
    new_manifest_records = []  # Track new matches for manifest update

    for start_date, end_date in date_ranges:
        print(f"\nFetching {start_date} to {end_date}...")

        matches = scraper.get_season_matches(season_id, start_date, end_date)
        played = [
            m for m in matches
            if m.get("liveData", {}).get("matchDetails", {}).get("matchStatus") == "Played"
        ]

        new_matches = [m for m in played if m["matchInfo"]["id"] not in existing_match_ids]
        print(f"  Found {len(matches)} total, {len(played)} played, {len(new_matches)} new")

        for i, match in enumerate(new_matches):
            match_id = match["matchInfo"]["id"]
            match_desc = match["matchInfo"]["description"]
            match_date = match["matchInfo"]["date"]

            print(f"  [{i+1}/{len(new_matches)}] {match_date[:10]} {match_desc}...", end=" ", flush=True)

            # Get matchstats data
            stats = scraper.get_match_stats(match_id)
            if not stats:
                print("FAILED (stats)")
                continue

            existing_match_ids.add(match_id)
            new_matches_scraped += 1

            # Track for manifest update
            has_events = False  # Will be set to True if we get event data

            # Extract from matchstats
            player_df = scraper.extract_all_player_stats(stats)
            all_player_stats.append(player_df)

            shots = scraper.extract_player_shots(stats)
            all_shots.extend([asdict(s) for s in shots])

            events = scraper.extract_match_events(stats)
            all_events.extend([asdict(e) for e in events])

            lineups = scraper.extract_lineups(stats)
            all_lineups.extend([asdict(l) for l in lineups])

            # Get matchevent data (event-level with x/y coords)
            event_data = scraper.get_match_events(match_id)
            if event_data:
                shot_events = scraper.extract_shot_events(event_data)
                all_shot_events.extend([asdict(s) for s in shot_events])

                # Extract ALL events with x/y coords (passes, tackles, aerials, etc.)
                match_events = scraper.extract_all_match_events(event_data)
                all_match_events.extend([asdict(e) for e in match_events])
                has_events = True

            # Add manifest record for this match
            # Don't mark as event_unavailable if match is recent (< 7 days old)
            # Events may become available after a few days
            from datetime import datetime, timedelta
            match_dt = datetime.fromisoformat(match_date.replace('Z', '+00:00'))
            is_recent = (datetime.now(match_dt.tzinfo) - match_dt) < timedelta(days=7)

            new_manifest_records.append({
                'match_id': match_id,
                'competition': competition,
                'season': season_name,
                'has_player_stats': True,
                'has_shots': len(shots) > 0,
                'has_match_events': has_events,
                'has_lineups': len(lineups) > 0,
                'event_unavailable': not has_events and not is_recent,  # Only mark if old AND no events
            })

            # Save raw JSON files
            raw_dir.mkdir(parents=True, exist_ok=True)
            with open(raw_dir / f"{match_id}_stats.json", "w") as f:
                json.dump(stats, f)
            if event_data:
                with open(raw_dir / f"{match_id}_events.json", "w") as f:
                    json.dump(event_data, f)

            print("OK")

    # Helper to combine new data with existing parquet
    def combine_and_save(new_data, output_path, dedup_cols):
        if not new_data:
            if output_path.exists():
                return pd.read_parquet(output_path)
            return pd.DataFrame()

        if isinstance(new_data[0], pd.DataFrame):
            new_df = pd.concat(new_data, ignore_index=True)
        else:
            new_df = pd.DataFrame(new_data)

        if output_path.exists():
            existing_df = pd.read_parquet(output_path)
            combined = pd.concat([existing_df, new_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=dedup_cols)
        else:
            combined = new_df

        if not combined.empty:
            combined.to_parquet(output_path, index=False)
        return combined

    # Combine and save all data types
    results = {}
    results["player_stats"] = combine_and_save(
        all_player_stats,
        output_dirs["player_stats"] / f"{season_name}.parquet",
        ["match_id", "player_id"]
    )
    results["shots"] = combine_and_save(
        all_shots,
        output_dirs["shots"] / f"{season_name}.parquet",
        ["match_id", "player_id"]
    )
    results["shot_events"] = combine_and_save(
        all_shot_events,
        output_dirs["shot_events"] / f"{season_name}.parquet",
        ["match_id", "event_id"]
    )
    results["match_events"] = combine_and_save(
        all_match_events,
        output_dirs["match_events"] / f"{season_name}.parquet",
        ["match_id", "event_id"]
    )
    results["events"] = combine_and_save(
        all_events,
        output_dirs["events"] / f"{season_name}.parquet",
        ["match_id", "event_type", "minute", "player_id"]
    )
    results["lineups"] = combine_and_save(
        all_lineups,
        output_dirs["lineups"] / f"{season_name}.parquet",
        ["match_id", "player_id"]
    )

    # Summary
    print(f"\n{competition} {season_name} Complete:")
    print(f"  New matches scraped: {new_matches_scraped}")
    print(f"  Total matches: {len(existing_match_ids)}")
    for name, df in results.items():
        if not df.empty:
            print(f"  {name}: {len(df)} records")

    return {
        "competition": competition,
        "season": season_name,
        "new_matches": new_matches_scraped,
        "total_matches": len(existing_match_ids),
        "player_records": len(results["player_stats"]),
        "shot_records": len(results["shots"]),
        "shot_event_records": len(results["shot_events"]),
        "match_event_records": len(results["match_events"]),
        "event_records": len(results["events"]),
        "lineup_records": len(results["lineups"]),
        "manifest_records": new_manifest_records,
    }


def main():
    """Scrape Big 5 leagues"""
    sys.stdout.reconfigure(encoding='utf-8')

    # Load seasons config
    seasons_config = load_seasons_config()

    # Parse command line args
    parser = argparse.ArgumentParser(description="Scrape Big 5 European leagues from Opta")
    parser.add_argument("--leagues", nargs="+",
                       choices=list(seasons_config.keys()),
                       help="Specific leagues to scrape (default: all)")
    parser.add_argument("--seasons", nargs="+",
                       help="Specific seasons to scrape (e.g., 2024-2025 2023-2024)")
    parser.add_argument("--force", action="store_true",
                       help="Force re-scrape of existing matches")
    parser.add_argument("--retry-unavailable", action="store_true",
                       help="Retry matches that previously had 404 for event data")
    parser.add_argument("--recent", type=int, default=0,
                       help="Scrape only the N most recent seasons per league")
    args = parser.parse_args()

    # Determine what to scrape
    leagues_to_scrape = args.leagues or list(seasons_config.keys())

    # Build scrape plan
    scrape_plan = []
    for league in leagues_to_scrape:
        if league not in seasons_config:
            print(f"Warning: Unknown league {league}")
            continue

        league_seasons = seasons_config[league]

        if args.seasons:
            # Filter to specified seasons
            seasons = [(s, sid) for s, sid in league_seasons.items() if s in args.seasons]
        elif args.recent > 0:
            # Get N most recent seasons
            sorted_seasons = sorted(league_seasons.items(), reverse=True)
            seasons = sorted_seasons[:args.recent]
        else:
            # Default: current season only
            sorted_seasons = sorted(league_seasons.items(), reverse=True)
            seasons = sorted_seasons[:1]

        for season_name, season_id in seasons:
            scrape_plan.append((league, season_name, season_id))

    # Filter out future seasons (e.g., Club_World_Cup 2029, UEFA_Euros 2028)
    original_count = len(scrape_plan)
    scrape_plan = [(l, s, sid) for l, s, sid in scrape_plan if not is_future_season(s)]
    if len(scrape_plan) < original_count:
        skipped = original_count - len(scrape_plan)
        print(f"Filtered out {skipped} future seasons from scrape plan")

    # Initialize scraper
    script_dir = Path(__file__).parent
    scraper = OptaScraper(data_dir=str(script_dir / "data"))

    # Load manifest (tracks which matches have been scraped)
    pannadata_dir = get_pannadata_dir()
    manifest_path = pannadata_dir / "opta-manifest.parquet"
    if args.force:
        complete_matches, unavailable_matches = set(), set()
    else:
        complete_matches, unavailable_matches = load_manifest(manifest_path)

    print("=" * 60)
    print("OPTA LEAGUES SCRAPER")
    print("=" * 60)
    print(f"Leagues: {leagues_to_scrape}")
    print(f"Scrape plan: {len(scrape_plan)} league-seasons")
    for league, season, _ in scrape_plan:
        print(f"  - {league} {season}")

    # Execute scraping
    results = []
    all_new_manifest_records = []
    for league, season_name, season_id in scrape_plan:
        try:
            result = scrape_season(scraper, league, season_name, season_id,
                                   complete_matches=complete_matches,
                                   unavailable_matches=unavailable_matches,
                                   force_rescrape=args.force,
                                   retry_unavailable=args.retry_unavailable)
            results.append(result)
            # Collect manifest records from this season
            all_new_manifest_records.extend(result.get("manifest_records", []))
        except Exception as e:
            print(f"ERROR scraping {league} {season_name}: {e}")
            results.append({
                "competition": league,
                "season": season_name,
                "error": str(e)
            })

    # Update manifest with new matches
    if all_new_manifest_records:
        update_manifest(manifest_path, all_new_manifest_records)

    # Final summary
    print("\n" + "=" * 60)
    print("SCRAPING COMPLETE")
    print("=" * 60)

    total_new = sum(r.get("new_matches", 0) for r in results)
    total_matches = sum(r.get("total_matches", 0) for r in results)
    total_players = sum(r.get("player_records", 0) for r in results)
    total_shots = sum(r.get("shot_records", 0) for r in results)

    print(f"New matches scraped: {total_new}")
    print(f"Total matches: {total_matches}")
    print(f"Total player records: {total_players}")
    print(f"Total shot records: {total_shots}")

    # Save results summary
    summary_path = script_dir / "data" / "scrape_summary.json"
    with open(summary_path, "w") as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "results": results,
            "totals": {
                "new_matches": total_new,
                "total_matches": total_matches,
                "player_records": total_players,
                "shot_records": total_shots,
            }
        }, f, indent=2)

    print(f"\nSummary saved to {summary_path}")


if __name__ == "__main__":
    main()
