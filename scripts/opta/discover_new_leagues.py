"""
One-off: discover seasons for newly-added leagues and merge into seasons.json.
No API key needed (OptaScraper hits performfeeds with browser headers).
Run from scripts/opta/:  python discover_new_leagues.py
"""
import json
from pathlib import Path
from opta_scraper import OptaScraper

NEW_LEAGUES = ["MLS", "Liga_MX", "Argentine_Liga_Profesional", "Saudi_League"]

def main():
    script_dir = Path(__file__).parent
    seasons_path = script_dir / "seasons.json"

    scraper = OptaScraper(data_dir=str(script_dir / "data"))

    with open(seasons_path) as f:
        seasons = json.load(f)

    print("=" * 60)
    for comp in NEW_LEAGUES:
        print(f"\n{comp}:")
        found = scraper.discover_seasons(comp)
        if not found:
            print("  No seasons found")
            continue
        existing = seasons.get(comp, {})
        added = {k: v for k, v in found.items() if k not in existing}
        existing.update(found)
        seasons[comp] = existing
        for name, sid in sorted(found.items(), reverse=True):
            tag = "NEW" if name in added else "have"
            print(f"  [{tag}] {name}: {sid}")
        print(f"  -> {len(found)} seasons ({len(added)} new)")

    with open(seasons_path, "w") as f:
        json.dump(seasons, f, indent=2)
    print("\n" + "=" * 60)
    print(f"Saved {seasons_path}")
    for comp in NEW_LEAGUES:
        print(f"  {comp}: {len(seasons.get(comp, {}))} seasons in seasons.json")

if __name__ == "__main__":
    main()
