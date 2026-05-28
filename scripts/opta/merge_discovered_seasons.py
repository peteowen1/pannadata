"""
Merge newly-discovered seasons from discover_seasons.py output into the main
seasons.json (which the scraper reads). Preserves existing entries and adds
any new (season_name -> season_id) pairs not yet present.

Run after `python discover_seasons.py`.
"""
import json
from pathlib import Path

script_dir = Path(__file__).parent
main_path = script_dir / "seasons.json"
discovered_path = script_dir / "data" / "seasons.json"

with open(main_path) as f:
    main = json.load(f)
with open(discovered_path) as f:
    discovered = json.load(f)

added = []
for comp, seasons in discovered.items():
    if comp not in main:
        main[comp] = {}
        added.append(f"  NEW COMP: {comp} ({len(seasons)} seasons)")
        for s_name, s_id in seasons.items():
            main[comp][s_name] = s_id
            added.append(f"    + {comp} | {s_name}")
        continue

    # Existing comp: add new seasons only
    existing_names = set(main[comp].keys())
    for s_name, s_id in seasons.items():
        if s_name not in existing_names:
            main[comp][s_name] = s_id
            added.append(f"  + {comp} | {s_name}: {s_id}")

if not added:
    print("No new seasons to merge.")
else:
    print(f"=== Adding {len(added)} new entries ===")
    for line in added:
        print(line)
    with open(main_path, "w") as f:
        json.dump(main, f, indent=2, sort_keys=False, ensure_ascii=False)
    print(f"\nWrote {main_path}")
