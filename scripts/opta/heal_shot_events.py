#!/usr/bin/env python3
"""Self-heal: re-derive MISSING shot_events from the stored match-events feed.

Root cause this fixes: shots and match_events are extracted from the SAME event
feed (opta_scraper.extract_shot_events / extract_all_match_events), but a match
can end up with its match_events written while its shot_events were not (a since-
fixed extraction bug, or a transient failure) — and the opta-manifest "complete"
flag then blocks re-processing. Result: matches that HAVE events but NO shots in
the consolidated opta_shot_events.parquet (e.g. 12 of the 16 WC-2026 group games,
2026-06). The shots are fully re-derivable from the stored events with ZERO API
calls — events_consolidated/events_<comp>.parquet stores every event's type_id,
x/y, and full qualifier set as `qualifier_json` (see opta_scraper.AllMatchEvent).

This script finds match_ids present in the events feed but absent from
opta_shot_events, re-derives their shot rows with the SAME mapping the scraper
uses (extract_shot_events), and appends them. xg/xgot are left NaN — the existing
enrich_shots_xg / xgot step fills them downstream. Idempotent: only adds matches
that are entirely missing shots. Atomic write (<out>.tmp then os.replace).

Wire into the daily scrape / build-blog-data AFTER consolidate so it self-heals
every run (then a future extraction-bug window can't strand matches permanently).

Usage (from repo root, after downloading the consolidated shots + events files):
    python scripts/opta/heal_shot_events.py [shots_parquet] [events_path] [out] [--dry-run]
Defaults: data/opta/opta_shot_events.parquet, data/opta/events_consolidated (dir
or single parquet), in-place overwrite. Optional 4th arg path to the event-less
registry to exclude (matches Opta genuinely has no events for).
"""

import json
import os
import sys

import numpy as np
import pandas as pd
import pyarrow.dataset as ds

SHOT_TYPE_IDS = [13, 14, 15, 16]  # miss, post, attempt saved, goal


def _quals(qualifier_json):
    """{qualifierId(int): value} from the stored qualifier_json dict (keyed by
    stringified id). Malformed JSON raises — never silently impute on a heal."""
    d = json.loads(qualifier_json) if qualifier_json else {}
    return {int(k): v for k, v in d.items()}


def _gm(quals, qid):
    """Goal-mouth coord (q102=y, q103=z). European-locale decimals -> float.
    Absent/blank -> NaN, never 0 (mirrors extract_shot_events._gm)."""
    v = quals.get(qid)
    if v in (None, ""):
        return np.nan
    return float(v.replace(",", ".") if isinstance(v, str) else v)


def derive_shot_row(ev):
    """Map one stored shot-type event row -> the opta_shot_events schema, using
    the SAME qualifier logic as opta_scraper.extract_shot_events."""
    q = _quals(ev["qualifier_json"])
    if 15 in q:
        body_part = "Head"
    elif 72 in q:
        body_part = "LeftFoot"
    else:
        body_part = "RightFoot"
    if 9 in q:
        situation = "Penalty"
    elif 25 in q:
        situation = "Corner"
    elif 24 in q or 26 in q:
        situation = "SetPiece"
    else:
        situation = "OpenPlay"
    return {
        "match_id": ev["match_id"], "event_id": ev["event_id"],
        "player_id": ev["player_id"], "player_name": ev["player_name"],
        "team_id": ev["team_id"], "minute": ev["minute"], "second": ev["second"],
        "x": float(ev["x"]), "y": float(ev["y"]), "outcome": ev["outcome"],
        "is_goal": ev["type_id"] == 16, "type_id": ev["type_id"],
        "body_part": body_part, "situation": situation, "big_chance": 214 in q,
        "competition": ev["competition"], "season": ev["season"],
        "xg": np.nan, "goalmouth_y": _gm(q, 102), "goalmouth_z": _gm(q, 103),
        "xgot": np.nan,
    }


def main():
    shots_path = sys.argv[1] if len(sys.argv) > 1 else "data/opta/opta_shot_events.parquet"
    events_path = sys.argv[2] if len(sys.argv) > 2 else "data/opta/events_consolidated"
    out_path = sys.argv[3] if len(sys.argv) > 3 else shots_path
    dry_run = "--dry-run" in sys.argv
    eventless_path = next((a for a in sys.argv[4:] if not a.startswith("--")), None)

    shots = pd.read_parquet(shots_path)
    have_shots = set(shots["match_id"].unique())
    print(f"opta_shot_events: {len(shots):,} rows, {len(have_shots):,} matches")

    ev_shots = ds.dataset(events_path).to_table(
        columns=["match_id", "event_id", "type_id", "player_id", "player_name",
                 "team_id", "minute", "second", "x", "y", "outcome",
                 "qualifier_json", "competition", "season"],
        filter=ds.field("type_id").isin(SHOT_TYPE_IDS),
    ).to_pandas()
    have_events = set(ev_shots["match_id"].unique())

    eventless = set()
    if eventless_path and os.path.exists(eventless_path):
        eventless = set(pd.read_parquet(eventless_path)["match_id"].unique())

    missing = (have_events - have_shots) - eventless
    print(f"matches with shot-events in feed: {len(have_events):,}")
    print(f"matches MISSING from shot_events (to heal): {len(missing):,}")
    if not missing:
        print("nothing to heal — opta_shot_events already covers every events match.")
        return

    to_add = ev_shots[ev_shots["match_id"].isin(missing)].copy()
    no_q = to_add["qualifier_json"].isna() | (to_add["qualifier_json"] == "")
    if no_q.mean() > 0.01:
        sys.exit(f"ABORT: {no_q.mean():.1%} of shot rows to heal carry no qualifiers "
                 "— a real shot always has qualifiers; check the events input.")
    to_add = to_add[~no_q]
    rows = pd.DataFrame([derive_shot_row(r) for _, r in to_add.iterrows()])

    # Guard: healed matches should have a sane shot count (a full match is ~10-45
    # total shots). Flag degenerate per-match counts before writing.
    pm = rows.groupby("match_id").size()
    bad = pm[(pm < 3) | (pm > 60)]
    print(f"healing {len(rows):,} shots across {pm.size:,} matches "
          f"(per-match min {pm.min()}, median {int(pm.median())}, max {pm.max()})")
    if len(bad):
        print(f"  WARNING: {len(bad)} healed matches have an unusual shot count "
              f"(<3 or >60): {bad.to_dict()}")

    rows = rows[shots.columns]  # exact column order match
    out = pd.concat([shots, rows], ignore_index=True)
    out = out.drop_duplicates(subset=["match_id", "event_id"], keep="first")
    print(f"opta_shot_events: {len(shots):,} -> {len(out):,} rows "
          f"(+{len(out) - len(shots):,}); matches {len(have_shots):,} -> "
          f"{out['match_id'].nunique():,}")
    print("NOTE: healed shots have xg/xgot = NaN — the enrich_shots_xg / xgot step "
          "must run after this to populate them.")
    if dry_run:
        print("--dry-run: not writing.")
        return
    tmp = f"{out_path}.tmp"
    out.to_parquet(tmp, index=False)
    os.replace(tmp, out_path)
    print(f"written: {out_path}")


if __name__ == "__main__":
    main()
