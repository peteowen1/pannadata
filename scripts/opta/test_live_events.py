"""
Test whether Opta API serves live match event data.

Run during a live match:
  cd pannadata
  python scripts/opta/test_live_events.py

Or specify a known match ID:
  python scripts/opta/test_live_events.py --match-id <id>
"""
import sys
import json
from datetime import datetime, timedelta
from opta_scraper import OptaScraper

scraper = OptaScraper()

# Season IDs for leagues we want to test (add more as needed)
SEASONS = {
    "EPL": "51r6ph2woavlbbpk8f29nynf8",
    "Serie A": "emdmtfr1v8rey2qru3xzfwges",
    "La Liga": "80zg2v1cuqcfhphn56u4qpyqc",
    "Bundesliga": "2bchmrj23l9u42d68ntcekob8",
    "Ligue 1": "dbxs75cag7zyip5re0ppsanmc",
    "AFC CL Two": "bx57cmq1edfq53ckfk791supi",
    "AFC CL Elite": "3qixixbgvsajit71o6mxb7x4a",
    "A-League": "bi1u4m4xssntipznvmocabh39",
}

# Check for --match-id argument
match_id = None
if "--match-id" in sys.argv:
    idx = sys.argv.index("--match-id")
    if idx + 1 < len(sys.argv):
        match_id = sys.argv[idx + 1]

if match_id:
    print(f"Testing match ID: {match_id}\n")
else:
    # Search all leagues for today/tomorrow matches
    today = datetime.now().strftime("%Y-%m-%d")
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    matches = []

    print(f"Scanning {len(SEASONS)} leagues for matches {today} to {tomorrow}...\n")
    for league, sid in SEASONS.items():
        league_matches = scraper.get_season_matches(sid, today, tomorrow)
        if league_matches:
            print(f"  {league}: {len(league_matches)} matches")
            matches.extend(league_matches)

    if not matches:
        next_week = (datetime.now() + timedelta(days=14)).strftime("%Y-%m-%d")
        print(f"\nNo matches today/tomorrow. Trying {today} to {next_week}...")
        for league, sid in SEASONS.items():
            league_matches = scraper.get_season_matches(sid, today, next_week)
            if league_matches:
                print(f"  {league}: {len(league_matches)} matches")
                matches.extend(league_matches)

    if not matches:
        print("No upcoming matches found. Try passing --match-id directly.")
        sys.exit(0)

    for m in matches:
        mi = m.get("matchInfo", {})
        conts = mi.get("contestant", [])
        home = next((c["name"] for c in conts if c.get("position") == "home"), "?")
        away = next((c["name"] for c in conts if c.get("position") == "away"), "?")
        status = m.get("liveData", {}).get("matchDetails", {}).get("matchStatus", "Unknown")
        dt = mi.get("date", "?")
        tm = mi.get("time", "")
        mid = mi.get("id")

        marker = ""
        if status in ("Playing", "FirstHalf", "SecondHalf", "HalfTime"):
            marker = " *** LIVE ***"
        elif status == "Played":
            marker = " (finished)"

        print(f"  {home} vs {away} | {dt} {tm} | status={status} | id={mid}{marker}")

        # Auto-select a live match, or fall back to most recent played
        if status in ("Playing", "FirstHalf", "SecondHalf", "HalfTime"):
            match_id = mid
            print(f"\n  → Found LIVE match! Testing events...\n")
            break

    if not match_id:
        # No live match — try the first played match for a baseline test
        played = [m for m in matches if m.get("liveData", {}).get("matchDetails", {}).get("matchStatus") == "Played"]
        if played:
            match_id = played[-1]["matchInfo"]["id"]
            desc = played[-1]["matchInfo"].get("description", match_id)
            print(f"\nNo live match found. Testing most recent played: {desc} ({match_id})\n")
        else:
            # Try a fixture (scheduled match) to see what the API returns
            fixture = [m for m in matches if m.get("liveData", {}).get("matchDetails", {}).get("matchStatus") == "Fixture"]
            if fixture:
                match_id = fixture[0]["matchInfo"]["id"]
                desc = fixture[0]["matchInfo"].get("description", match_id)
                print(f"\nNo live/played match. Testing fixture: {desc} ({match_id})\n")

if not match_id:
    print("No match ID to test.")
    sys.exit(0)

# Test 1: Match events (matchevent endpoint — full event data with x/y)
print(f"=== Testing matchevent/{match_id} ===")
event_data = scraper.get_match_events(match_id)
if event_data:
    live_data = event_data.get("liveData", {})
    status = live_data.get("matchDetails", {}).get("matchStatus", "?")
    print(f"  Status: {status}")

    events = live_data.get("event", [])
    print(f"  Events: {len(events)}")

    if events:
        # Show first 5 events
        print(f"  First 5 events:")
        for e in events[:5]:
            etype = e.get("typeId", "?")
            desc = e.get("type", {}).get("name", "?") if isinstance(e.get("type"), dict) else e.get("type", "?")
            x = e.get("x", "?")
            y = e.get("y", "?")
            period = e.get("periodId", "?")
            time_min = e.get("timeMin", "?")
            player = e.get("playerName", e.get("player", {}).get("name", "?") if isinstance(e.get("player"), dict) else "?")
            print(f"    {desc} | x={x} y={y} | period={period} min={time_min} | player={player}")

        # Count events with x/y
        with_xy = sum(1 for e in events if e.get("x") is not None and e.get("y") is not None)
        print(f"\n  Events with x/y coordinates: {with_xy}/{len(events)} ({100*with_xy//max(len(events),1)}%)")

        # Event type distribution
        types = {}
        for e in events:
            t = e.get("type", {}).get("name", "?") if isinstance(e.get("type"), dict) else str(e.get("typeId", "?"))
            types[t] = types.get(t, 0) + 1
        print(f"\n  Event types (top 10):")
        for t, count in sorted(types.items(), key=lambda x: -x[1])[:10]:
            print(f"    {t}: {count}")
    else:
        print("  No events returned — API may not serve events for this match status")
else:
    print("  API returned no data (404 or error)")

# Test 2: Match stats (for comparison)
print(f"\n=== Testing matchstats/{match_id} ===")
stats_data = scraper.get_match_stats(match_id)
if stats_data:
    status = stats_data.get("liveData", {}).get("matchDetails", {}).get("matchStatus", "?")
    print(f"  Status: {status}")
    lineup = stats_data.get("liveData", {}).get("lineUp", [])
    if lineup:
        total_players = sum(len(t.get("player", [])) for t in lineup)
        print(f"  Players in lineup: {total_players}")
    else:
        print("  No lineup data")
else:
    print("  API returned no data")

print("\nDone.")
