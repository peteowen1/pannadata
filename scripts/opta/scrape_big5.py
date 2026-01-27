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
from opta_scraper import OptaScraper
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
    """Get start and end dates for a season (Aug-May typical)"""
    year_start = int(season_name.split("-")[0])

    # Date ranges to cover full season (API returns max 100 per request)
    date_ranges = [
        (f"{year_start}-08-01", f"{year_start}-09-30"),
        (f"{year_start}-10-01", f"{year_start}-11-30"),
        (f"{year_start}-12-01", f"{year_start+1}-01-31"),
        (f"{year_start+1}-02-01", f"{year_start+1}-03-31"),
        (f"{year_start+1}-04-01", f"{year_start+1}-05-31"),
    ]
    return date_ranges


def scrape_season(scraper: OptaScraper, competition: str, season_name: str,
                  season_id: str, force_rescrape: bool = False):
    """Scrape a full season of data for a competition"""

    print(f"\n{'='*60}")
    print(f"Scraping {competition} {season_name}")
    print(f"Season ID: {season_id}")
    print(f"{'='*60}")

    # Raw JSON cache in scripts/opta/data/raw
    raw_dir = scraper.data_dir / "raw" / competition / season_name

    # Processed parquet goes to pannadata/data/opta/
    pannadata_dir = get_pannadata_dir()
    opta_dir = pannadata_dir / "opta"
    player_stats_dir = opta_dir / "player_stats" / competition
    shots_dir = opta_dir / "shots" / competition

    existing_match_ids = set()
    if raw_dir.exists() and not force_rescrape:
        existing_match_ids = {f.stem for f in raw_dir.glob("*.json")}
        print(f"Found {len(existing_match_ids)} already scraped matches")

    date_ranges = get_season_date_range(season_name)

    all_player_stats = []
    all_shots = []
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

            stats = scraper.get_match_stats(match_id)
            if not stats:
                print("FAILED")
                continue

            existing_match_ids.add(match_id)
            new_matches_scraped += 1

            # Extract data
            player_df = scraper.extract_all_player_stats(stats)
            all_player_stats.append(player_df)

            shots = scraper.extract_player_shots(stats)
            shot_dicts = [asdict(s) for s in shots]
            all_shots.extend(shot_dicts)

            # Save raw JSON
            raw_dir.mkdir(parents=True, exist_ok=True)
            with open(raw_dir / f"{match_id}.json", "w") as f:
                json.dump(stats, f)

            print("OK")

    # Combine with existing processed data
    # Files are saved as: pannadata/data/opta/{table_type}/{league}/{season}.parquet
    player_stats_dir.mkdir(parents=True, exist_ok=True)
    shots_dir.mkdir(parents=True, exist_ok=True)

    existing_players_path = player_stats_dir / f"{season_name}.parquet"
    existing_shots_path = shots_dir / f"{season_name}.parquet"

    # Combine player stats
    if existing_players_path.exists() and all_player_stats:
        existing_players = pd.read_parquet(existing_players_path)
        new_players = pd.concat(all_player_stats, ignore_index=True)
        combined_players = pd.concat([existing_players, new_players], ignore_index=True)
        combined_players = combined_players.drop_duplicates(subset=['match_id', 'player_id'])
    elif all_player_stats:
        combined_players = pd.concat(all_player_stats, ignore_index=True)
    elif existing_players_path.exists():
        combined_players = pd.read_parquet(existing_players_path)
    else:
        combined_players = pd.DataFrame()

    # Combine shots
    if existing_shots_path.exists() and all_shots:
        existing_shots = pd.read_parquet(existing_shots_path)
        new_shots = pd.DataFrame(all_shots)
        combined_shots = pd.concat([existing_shots, new_shots], ignore_index=True)
        combined_shots = combined_shots.drop_duplicates(subset=['match_id', 'player_id'])
    elif all_shots:
        combined_shots = pd.DataFrame(all_shots)
    elif existing_shots_path.exists():
        combined_shots = pd.read_parquet(existing_shots_path)
    else:
        combined_shots = pd.DataFrame()

    # Save to pannadata/data/opta/
    if not combined_players.empty:
        combined_players.to_parquet(existing_players_path, index=False)
        print(f"  Saved: {existing_players_path}")
    if not combined_shots.empty:
        combined_shots.to_parquet(existing_shots_path, index=False)
        print(f"  Saved: {existing_shots_path}")

    # Summary
    print(f"\n{competition} {season_name} Complete:")
    print(f"  New matches scraped: {new_matches_scraped}")
    print(f"  Total matches: {len(existing_match_ids)}")
    print(f"  Total player records: {len(combined_players)}")
    print(f"  Total shot records: {len(combined_shots)}")

    return {
        "competition": competition,
        "season": season_name,
        "new_matches": new_matches_scraped,
        "total_matches": len(existing_match_ids),
        "player_records": len(combined_players),
        "shot_records": len(combined_shots),
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
