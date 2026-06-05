"""
Build a reference catalog of EVERY competition + season our Opta outlet can
actually fetch (the entitlement), by paginating the tournamentcalendar feed.

No API key needed (OptaScraper uses the performfeeds outlet token in PROVIDER_ID).

The tournamentcalendar response nests each competition's seasons
(competition[].tournamentCalendar[]), so a single paginated sweep yields both
the competition list AND all known seasons — no per-competition calls.

Outputs (committed for future reference):
  scripts/opta/opta_entitlement_catalog.csv   one row per (competition, season)
  scripts/opta/opta_entitlement_catalog.json  nested {competitions:[...]}

Run from scripts/opta/:  python build_entitlement_catalog.py
"""
import json
import csv
from pathlib import Path
from opta_scraper import OptaScraper


def country_of(c):
    v = c.get("country")
    return v.get("name", "") if isinstance(v, dict) else (v or "")


def main():
    script_dir = Path(__file__).parent
    s = OptaScraper(data_dir=str(script_dir / "data"))
    print(f"Outlet PROVIDER_ID: {s.PROVIDER_ID[:8]}...")

    # Reverse map comp_id -> our internal short key (if configured)
    id_to_key = {cid: k for k, cid in s.COMPETITIONS.items()}

    # --- Paginate the full entitlement ---
    seen_ids = set()
    competitions = []
    page_size = 100
    for pg in range(1, 100):  # generous cap; we break on empty/short page
        data, status = s._fetch_raw(
            f"tournamentcalendar/{s.PROVIDER_ID}",
            {"_pgSz": str(page_size), "_pgNm": str(pg)},
        )
        comps = data.get("competition", []) if data else []
        if not comps:
            break
        new = 0
        for c in comps:
            cid = c.get("id", "")
            if cid in seen_ids:
                continue
            seen_ids.add(cid)
            competitions.append(c)
            new += 1
        print(f"  page {pg}: http={status} got={len(comps)} new={new} total={len(competitions)}")
        if len(comps) < page_size and new == 0:
            break

    print(f"\nTotal distinct competitions entitled: {len(competitions)}")

    # --- Flatten to (competition, season) rows ---
    rows = []
    nested = []
    for c in competitions:
        cid = c.get("id", "")
        cname = c.get("name", "")
        ccode = c.get("competitionCode", "")
        ctype = c.get("competitionFormat", "") or c.get("type", "")
        country = country_of(c)
        our_key = id_to_key.get(cid, "")
        seasons = c.get("tournamentCalendar", []) or []
        nested.append({
            "competition_id": cid, "competition_name": cname,
            "competition_code": ccode, "country": country,
            "our_key": our_key, "n_seasons": len(seasons),
            "seasons": [
                {"season_id": tc.get("id", ""),
                 "season_name": tc.get("name", ""),
                 "start_date": tc.get("startDate", ""),
                 "end_date": tc.get("endDate", "")}
                for tc in seasons
            ],
        })
        if not seasons:
            rows.append([country, cname, ccode, our_key, cid, "", "", "", ""])
        for tc in seasons:
            rows.append([
                country, cname, ccode, our_key, cid,
                tc.get("name", ""), tc.get("id", ""),
                tc.get("startDate", ""), tc.get("endDate", ""),
            ])

    # --- Write CSV (utf-8) ---
    csv_path = script_dir / "opta_entitlement_catalog.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["country", "competition_name", "competition_code",
                    "our_key", "competition_id", "season_name", "season_id",
                    "start_date", "end_date"])
        w.writerows(rows)

    json_path = script_dir / "opta_entitlement_catalog.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({"n_competitions": len(competitions),
                   "n_competition_season_rows": len(rows),
                   "competitions": nested}, f, indent=2, ensure_ascii=False)

    print(f"\nSaved:")
    print(f"  {csv_path}  ({len(rows)} comp-season rows)")
    print(f"  {json_path}")

    # --- Quick look: are the target leagues reachable? ---
    print("\n=== Target leagues in entitlement ===")
    import re
    want = re.compile(
        r"major league soccer|liga mx|liga bbva|saudi|professional league|"
        r"liga profesional argentina|j1|j.?league|k league|k.?1|a-league men",
        re.I)
    for n in nested:
        if want.search(n["competition_name"]):
            print(f"  {n['country']:16s} | {n['competition_name']:34s} | "
                  f"id={n['competition_id']} | seasons={n['n_seasons']} | "
                  f"our_key={n['our_key'] or '-'}")


if __name__ == "__main__":
    main()
