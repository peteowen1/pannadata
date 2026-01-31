"""
Scrape Big 5 Leagues - Opta Data Collection

Comprehensive scraper for all Big 5 European leagues:
- EPL (Premier League)
- La_Liga (La Liga)
- Bundesliga
- Serie_A (Serie A)
- Ligue_1 (Ligue 1)

Data is saved to pannadata/data/opta/ with structure:
- pannadata/data/opta/player_stats/{league}/{season}.parquet
- pannadata/data/opta/shots/{league}/{season}.parquet

Usage:
    python scrape_big5.py                           # Scrape all leagues, current season
    python scrape_big5.py --leagues EPL La_Liga     # Specific leagues
    python scrape_big5.py --seasons 2024-2025 2023-2024  # Specific seasons
    python scrape_big5.py --leagues EPL --seasons 2024-2025 2023-2024
"""

import json
import argparse
import sys
from pathlib import Path
from datetime import datetime
from opta_scraper import OptaScraper, MatchEvent, ShotEvent, PlayerLineup, AllMatchEvent
from dataclasses import asdict
import pandas as pd


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
                  season_id: str, force_rescrape: bool = False):
    """Scrape a full season of data for a competition with all data types"""

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

    # Check for existing matches - only skip if match_events exists (the most complete table)
    # This ensures we rescrape matches that are missing event data
    existing_match_ids = set()
    if not force_rescrape:
        # Check match_events parquet - this is the critical table with all x/y event data
        # Only skip matches that already have match_events scraped
        existing_events_path = output_dirs["match_events"] / f"{season_name}.parquet"
        if existing_events_path.exists():
            try:
                existing_df = pd.read_parquet(existing_events_path, columns=['match_id'])
                existing_match_ids = set(existing_df['match_id'].unique())
            except Exception:
                pass  # If file is corrupt, rescrape everything
        if existing_match_ids:
            print(f"Found {len(existing_match_ids)} matches with complete event data")

    date_ranges = get_season_date_range(season_name)

    # Collectors for all data types
    all_player_stats = []
    all_shots = []
    all_shot_events = []
    all_match_events = []  # ALL events with x/y coords (~2000/match)
    all_events = []
    all_lineups = []
    new_matches_scraped = 0

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

    # Initialize scraper
    script_dir = Path(__file__).parent
    scraper = OptaScraper(data_dir=str(script_dir / "data"))

    print("=" * 60)
    print("BIG 5 LEAGUES SCRAPER")
    print("=" * 60)
    print(f"Leagues: {leagues_to_scrape}")
    print(f"Scrape plan: {len(scrape_plan)} league-seasons")
    for league, season, _ in scrape_plan:
        print(f"  - {league} {season}")

    # Execute scraping
    results = []
    for league, season_name, season_id in scrape_plan:
        try:
            result = scrape_season(scraper, league, season_name, season_id,
                                   force_rescrape=args.force)
            results.append(result)
        except Exception as e:
            print(f"ERROR scraping {league} {season_name}: {e}")
            results.append({
                "competition": league,
                "season": season_name,
                "error": str(e)
            })

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
