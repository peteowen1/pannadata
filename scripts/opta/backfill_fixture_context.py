"""One-off backfill of the fixture context columns (venue, kickoff, referee,
attendance; pannadata#254) for seasons already on the bus.

Hand-run. Fetches the fixture list only (no per-match calls) for every
competition-season in an existing opta_fixtures.parquet and writes per-season
files under <out>/opta/fixtures/, never under pannadata/data/opta/. It uploads
nothing. Every raw API response is kept under <out>/raw/.

A season is marked done in <out>/status.csv only when no window failed and
every match_id the bus already holds for it came back. Rerun to resume: done
seasons are skipped. The run stops itself at --stop-at (local time) so it never
overlaps the daily scrape on the same Opta token.

Usage:
    python backfill_fixture_context.py --bus <opta_fixtures.parquet> --out <dir> [--stop-at 01:00] [--limit N]
"""
import argparse
import contextlib
import csv
import gzip
import io
import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

import scrape_opta
from competition_metadata import get_competition_metadata
from opta_scraper import OptaScraper

STATUS_COLS = ["competition", "season", "done", "n_bus", "n_new", "n_bus_missing",
               "n_failed_windows", "seconds", "finished", "note"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bus", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--stop-at", default="01:00", help="local HH:MM to stop at")
    ap.add_argument("--limit", type=int, default=0, help="stop after N seasons (smoke test)")
    args = ap.parse_args()

    out = Path(args.out)
    (out / "raw").mkdir(parents=True, exist_ok=True)
    status_path = out / "status.csv"

    now = datetime.now()
    hh, mm = map(int, args.stop_at.split(":"))
    stop = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if stop <= now:
        stop += timedelta(days=1)
    print(f"Start {now:%Y-%m-%d %H:%M}, hard stop {stop:%Y-%m-%d %H:%M}", flush=True)

    bus = pd.read_parquet(args.bus, columns=["match_id", "competition", "season"])
    print(f"Bus fixtures: {len(bus):,} rows, {bus.match_id.nunique():,} matches, "
          f"{bus.groupby(['competition', 'season']).ngroups:,} competition-seasons", flush=True)
    bus_ids = {k: set(g.match_id) for k, g in bus.groupby(["competition", "season"])}

    cfg = scrape_opta.load_seasons_config()
    plan, no_id = [], []
    for (comp, season) in bus_ids:
        sid = (cfg.get(comp) or {}).get(season)
        (plan if sid else no_id).append((comp, season, sid))
    if no_id:
        print(f"{len(no_id)} competition-seasons on the bus have no season id in seasons.json "
              f"(skipped): {[(c, s) for c, s, _ in no_id][:10]}", flush=True)

    def years(s):
        import re
        y = re.findall(r"(?:19|20)\d\d", s)
        return int(y[-1]) if y else 0
    plan.sort(key=lambda p: (get_competition_metadata(p[0]).get("tier", 99), -years(p[1]), p[0]))

    done = set()
    if status_path.exists():
        st = pd.read_csv(status_path)
        done = {(r.competition, r.season) for r in st.itertuples() if str(r.done) == "True"}
    else:
        with open(status_path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(STATUS_COLS)
    todo = [p for p in plan if (p[0], p[1]) not in done]
    print(f"Plan: {len(plan):,} seasons, {len(done):,} already done, {len(todo):,} to do", flush=True)

    # Write into <out>, never pannadata/data.
    scrape_opta.get_pannadata_dir = lambda: out
    scraper = OptaScraper(data_dir=str(out / "scrdata"))
    ctx = {"comp": None, "season": None}
    fetch = scraper.get_season_matches

    def fetch_and_keep(season_id, start_date, end_date):
        matches = fetch(season_id, start_date, end_date)
        if matches:
            d = out / "raw" / ctx["comp"] / ctx["season"].replace("/", "_")
            d.mkdir(parents=True, exist_ok=True)
            with gzip.open(d / f"{start_date}_{end_date}.json.gz", "wt", encoding="utf-8") as f:
                json.dump(matches, f)
        return matches
    scraper.get_season_matches = fetch_and_keep

    n_run = 0
    for comp, season, sid in todo:
        if datetime.now() >= stop:
            print(f"Hard stop reached at {datetime.now():%H:%M}; {len(todo) - n_run} seasons left", flush=True)
            break
        if args.limit and n_run >= args.limit:
            break
        ctx["comp"], ctx["season"] = comp, season
        failed_before = len(scraper.failed_windows)
        t0 = time.time()
        note, n_new, missing = "", 0, len(bus_ids[(comp, season)])
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                res = scrape_opta.scrape_season(scraper, comp, season, sid,
                                                complete_matches=set(), unavailable_matches=set(),
                                                scrape_types=["fixtures"], include_future=True)
            path = out / "opta" / "fixtures" / comp / f"{season}.parquet"
            if path.exists():
                new = pd.read_parquet(path, columns=["match_id"])
                n_new = len(new)
                missing = len(bus_ids[(comp, season)] - set(new.match_id))
            else:
                note = "no file written"
        except Exception as e:  # one bad season must not end a six-hour run
            note = f"{type(e).__name__}: {e}"[:200]
        n_failed = len(scraper.failed_windows) - failed_before
        ok = (not note) and n_failed == 0 and missing == 0
        with open(status_path, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([comp, season, ok, len(bus_ids[(comp, season)]), n_new, missing,
                                    n_failed, round(time.time() - t0, 1),
                                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"), note])
        n_run += 1
        print(f"[{n_run}/{len(todo)}] {comp} {season}: {'ok' if ok else 'NOT DONE'} "
              f"bus={len(bus_ids[(comp, season)])} new={n_new} missing={missing} "
              f"failed_windows={n_failed} {time.time() - t0:.0f}s {note}", flush=True)

    print(f"Finished {datetime.now():%Y-%m-%d %H:%M}: ran {n_run} seasons, "
          f"{len(scraper.failed_windows)} failed windows in total", flush=True)


if __name__ == "__main__":
    sys.exit(main())
