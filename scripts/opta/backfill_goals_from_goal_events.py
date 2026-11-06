#!/usr/bin/env python3
"""One-off repair for panna#431: fill missing player goals/assists/own goals
in a consolidated opta_player_stats.parquet from the archived raw payloads.

Why: before 2015-16 (and in some smaller 2023-24 competitions) most matches
come with an empty per-player stat block, so `goals`, `goalAssist` and
`ownGoals` are NA for nearly every scorer. Consumers sum NA as 0 (EPL 2013-14
summed to 124 goals; the real total is 989 plus 45 own goals). Every raw
payload still lists each goal in liveData.goal with scorer and assister, and
where both sources exist they agree exactly. The scraper now fills these at
extraction (opta_scraper.goal_event_counts); this script applies the same fill
to rows already published.

Only NA cells are filled, and only with a non-zero count, so a value the stat
block sent is never changed. Candidate matches are those where the player rows'
goals + own goals fall short of the final score.

Usage (from scripts/opta/):
    python backfill_goals_from_goal_events.py IN.parquet OUT.parquet [--dry-run]
        [--raw-dir data/raw] [--report report.csv]
"""

import argparse
import json
import sys
import time
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from opta_scraper import GOAL_EVENT_STATS, goal_event_counts


def _num(col):
    return pc.fill_null(pc.cast(col, pa.float64()), 0.0)


def find_candidates(pf):
    """Matches whose player rows sum to fewer goals + own goals than the score."""
    cols = ["match_id", "competition", "season", "goals", "ownGoals", "home_score", "away_score"]
    t = pf.read(columns=cols)
    t = pa.table({
        "match_id": t["match_id"], "competition": t["competition"], "season": t["season"],
        "scored": pc.add(_num(t["goals"]), _num(t["ownGoals"])),
        "final": pc.add(_num(t["home_score"]), _num(t["away_score"])),
    })
    agg = t.group_by("match_id").aggregate([
        ("competition", "min"), ("season", "min"), ("scored", "sum"), ("final", "max"),
    ])
    short = agg.filter(pc.less(agg["scored_sum"], agg["final_max"]))
    cand = dict(zip(short["match_id"].to_pylist(),
                    zip(short["competition_min"].to_pylist(), short["season_min"].to_pylist())))
    return cand, t.num_rows, agg.num_rows


def load_counts(candidates, raw_dir):
    counts = {}  # match_id -> {stat: {player_id: n}}
    missing = defaultdict(int)
    for m, (comp, season) in candidates.items():
        f = raw_dir / str(comp) / str(season) / f"{m}_stats.json"
        if not f.exists():
            missing[(comp, season)] += 1
            continue
        with open(f, encoding="utf-8") as fh:
            counts[m] = goal_event_counts(json.load(fh).get("liveData", {}))
    return counts, missing


def patch(pf, out_path, counts, dry_run):
    schema = pf.schema_arrow
    writer = None if dry_run else pq.ParquetWriter(out_path, schema, compression="snappy")
    filled = defaultdict(lambda: defaultdict(int))  # (comp, season) -> stat -> n filled
    conflicts = 0
    try:
        for rg in range(pf.num_row_groups):
            t = pf.read_row_group(rg)
            mids = t["match_id"].to_pylist()
            pids = t["player_id"].to_pylist()
            comps = t["competition"].to_pylist()
            seas = t["season"].to_pylist()
            for stat in GOAL_EVENT_STATS:
                idx = schema.get_field_index(stat)
                vals = t[stat].to_pylist()
                changed = False
                for i, m in enumerate(mids):
                    c = counts.get(m)
                    if c is None:
                        continue
                    n = c[stat].get(pids[i], 0)
                    if n == 0:
                        continue
                    if vals[i] is None:
                        vals[i] = n
                        filled[(comps[i], seas[i])][stat] += n
                        changed = True
                    elif vals[i] != n:
                        conflicts += 1
                if changed:
                    t = t.set_column(idx, schema.field(idx), pa.array(vals, type=schema.field(idx).type))
            if writer is not None:
                writer.write_table(t)
    finally:
        if writer is not None:
            writer.close()
    return filled, conflicts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("inp")
    ap.add_argument("out")
    ap.add_argument("--raw-dir", default=str(Path(__file__).parent / "data" / "raw"))
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--report", default=None)
    a = ap.parse_args()

    t0 = time.time()
    pf = pq.ParquetFile(a.inp)
    candidates, nrows, nmatches = find_candidates(pf)
    print(f"Input: {nrows:,} rows, {nmatches:,} matches; {len(candidates):,} matches short of the final score")
    counts, missing = load_counts(candidates, Path(a.raw_dir))
    n_missing = sum(missing.values())
    print(f"Raw payloads found for {len(counts):,} of {len(candidates):,} candidates; {n_missing:,} missing ({time.time()-t0:.0f}s)")
    filled, conflicts = patch(pf, a.out, counts, a.dry_run)
    print(f"Conflicts (stat block value differs from goal list, left unchanged): {conflicts}")
    tot = {s: sum(v[s] for v in filled.values()) for s in GOAL_EVENT_STATS}
    print("Filled totals:", tot, f"({time.time()-t0:.0f}s)")

    rows = sorted(set(filled) | set(missing), key=lambda k: (str(k[0]), str(k[1])))
    lines = ["competition,season,goals_filled,assists_filled,own_goals_filled,candidates_without_raw"]
    for k in rows:
        f = filled.get(k, {})
        lines.append(f"{k[0]},{k[1]},{f.get('goals',0)},{f.get('goalAssist',0)},{f.get('ownGoals',0)},{missing.get(k,0)}")
    if a.report:
        Path(a.report).write_text("\n".join(lines) + "\n", encoding="utf-8")
        print(f"Report: {a.report}")

    if not a.dry_run:
        out = pq.ParquetFile(a.out)
        if out.metadata.num_rows != nrows or out.schema_arrow != pf.schema_arrow:
            print("ERROR: output row count or schema differs from input", file=sys.stderr)
            sys.exit(1)
        print(f"Wrote {a.out}: {out.metadata.num_rows:,} rows, schema unchanged")


if __name__ == "__main__":
    main()
