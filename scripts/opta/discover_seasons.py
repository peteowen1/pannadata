"""
Discover Available Seasons for Big 5 Leagues

Queries the Opta tournament calendar API to find all available seasons
for each Big 5 league.
"""

import json
from pathlib import Path
from opta_scraper import OptaScraper


def main():
    """Discover and save all available seasons for Big 5 leagues"""
    script_dir = Path(__file__).parent
    scraper = OptaScraper(data_dir=str(script_dir / "data"))

    print("=" * 60)
    print("DISCOVERING AVAILABLE SEASONS")
    print("=" * 60)

    all_seasons = {}

    for competition in scraper.COMPETITIONS.keys():
        print(f"\n{competition}:")
        print("-" * 40)

        seasons = scraper.discover_seasons(competition)

        if seasons:
            all_seasons[competition] = seasons
            for name, season_id in sorted(seasons.items(), reverse=True):
                print(f"  {name}: {season_id}")
        else:
            print("  No seasons found")

    # Save to JSON file
    output_path = script_dir / "data" / "seasons.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(all_seasons, f, indent=2)

    print(f"\n{'=' * 60}")
    print(f"Saved to {output_path}")
    print("=" * 60)

    # Generate SEASONS dict for opta_scraper.py
    print("\nPython dict for SEASONS:")
    print("-" * 40)
    print("SEASONS = {")
    for comp, seasons in all_seasons.items():
        print(f"    # {comp}")
        for name, season_id in sorted(seasons.items(), reverse=True):
            key = f"{comp}_{name}"
            print(f'    "{key}": "{season_id}",')
    print("}")


if __name__ == "__main__":
    main()
