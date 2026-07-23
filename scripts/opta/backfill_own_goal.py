#!/usr/bin/env python3
"""One-off backfill: add an `is_own_goal` marker to the consolidated
opta_shot_events.parquet from the events feed's stored qualifiers — no
re-scrape needed.

pannadata#105: opta_shot_events carried no own-goal marker, so every
consumer that only sees shot events (xG/xGOT training preps, the serving-side
NA guard in enrich_shots_xg.R/enrich_shots_xgot.R) was stuck with the
positional heuristic `type_id == 16 & x < 50`. Opta qualifier 28 marks own
goals directly — the same marker panna's SPADL conversion already trusts
(add_xgot_to_spadl(), post panna#143/#148). The events feed's qualifier_json
(events_consolidated/events_<comp>.parquet) stores each event's full
qualifier set keyed by stringified qualifier ID, so the marker is
re-derivable: join shots to events on (match_id, event_id), check for key
"28", and write.

The scraper now captures this on new matches (opta_scraper.ShotEvent,
pannadata#105); historical shot rows predate that change.

Usage (from the repo root, after downloading the consolidated shots file
and the per-league events_consolidated files):

    python scripts/opta/backfill_own_goal.py \
        [shots_parquet] [events_path] [out_parquet]

Defaults: data/opta/opta_shot_events.parquet,
data/opta/events_consolidated (a directory of per-league parquets; a single
parquet file also works), in-place overwrite of the shots file. The write
is atomic: <out>.tmp then os.replace.

Guards — the script refuses to write when any fails (a one-off repair must
crash, never impute):
  - events coverage < 50% of shot rows (an id-format break; an expected
    coverage gap from the events_consolidated backlog is only reported)
  - > 1% of shot-type event rows carry no qualifiers (a real shot always
    has qualifiers — points at a parse or upstream-data problem)
  - marker-vs-heuristic agreement < 99% on goals (type_id==16) we can match:
    the issue's own sizing note measured 100% agreement between qualifier 28
    and the x<50 heuristic on the current xGOT training window (5,782 own
    goals, all x<50, max 49.7) — a parse regression would show up as
    disagreement here, not as a plausible-looking own-goal rate.
"""

import json
import os
import sys

import pandas as pd
import pyarrow.dataset as ds

SHOT_TYPE_IDS = [13, 14, 15, 16]
GOAL_TYPE_ID = 16
OWN_GOAL_QUALIFIER = "28"


def has_own_goal_qualifier(qualifier_json):
    """True iff qualifier 28 is present in the event's qualifier set.
    Malformed JSON raises: a one-off repair must crash, never impute."""
    d = json.loads(qualifier_json)
    return OWN_GOAL_QUALIFIER in d


def main():
    shots_path = sys.argv[1] if len(sys.argv) > 1 else "data/opta/opta_shot_events.parquet"
    events_path = sys.argv[2] if len(sys.argv) > 2 else "data/opta/events_consolidated"
    out_path = sys.argv[3] if len(sys.argv) > 3 else shots_path

    shots = pd.read_parquet(shots_path)
    print(f"shots: {len(shots):,} rows")
    if "is_own_goal" in shots.columns:
        have = shots["is_own_goal"].notna().mean()
        print(f"existing is_own_goal column present ({have:.1%} non-null) "
              "— will coalesce, filling only the gaps")

    events = ds.dataset(events_path).to_table(
        columns=["match_id", "event_id", "type_id", "qualifier_json"],
        filter=ds.field("type_id").isin(SHOT_TYPE_IDS),
    ).to_pandas()
    print(f"shot-type event rows: {len(events):,}")

    no_quals = events["qualifier_json"].isna() | (events["qualifier_json"] == "")
    if no_quals.any():
        frac = no_quals.mean()
        print(f"event rows with NO qualifiers: {int(no_quals.sum()):,} ({frac:.2%}) "
              "— dropped, no own-goal marker derivable for them")
        if frac > 0.01:
            sys.exit(
                "ABORT: >1% of shot-type event rows carry no qualifiers — a real "
                "shot always has qualifiers; check the events input before writing."
            )
        events = events[~no_quals]

    events["is_own_goal"] = events["qualifier_json"].map(has_own_goal_qualifier)

    dup_mask = events.duplicated(subset=["match_id", "event_id"], keep=False)
    if dup_mask.any():
        print(f"duplicate (match_id, event_id) event rows: {int(dup_mask.sum()):,} "
              "— keeping last")
    events = events.drop_duplicates(subset=["match_id", "event_id"], keep="last")

    merged = shots.merge(
        events[["match_id", "event_id", "is_own_goal"]].rename(
            columns={"is_own_goal": "_og_derived"}),
        on=["match_id", "event_id"],
        how="left",
    )
    matched = merged["_og_derived"].notna()
    match_rate = matched.mean()
    print(f"events coverage: {match_rate:.1%} of shots have an events row "
          f"({int((~matched).sum()):,} unmatched — events backlog; keep unknown, "
          "re-run after rebuild-events heals them)")
    if match_rate < 0.50:
        sys.exit(
            f"ABORT: only {match_rate:.1%} of shots matched an events row — "
            "that is an id-format break, not a coverage gap; check inputs."
        )

    # Parse-correctness gate: cross-check the marker against the existing
    # positional heuristic on matched GOALS only (own goals are only
    # meaningful/measured for type_id==16). The issue's sizing note found
    # 100% agreement (5,782/5,782) on the current xGOT training window —
    # a parse regression would show up as disagreement here.
    goals = merged.loc[matched & (merged["type_id"] == GOAL_TYPE_ID)].copy()
    if len(goals) < 100:
        sys.exit(f"ABORT: only {len(goals)} matched goals — too few to "
                 "validate the marker; check the events input.")
    heuristic_og = goals["x"] < 50
    marker_og = goals["_og_derived"].astype(bool)
    agree = (heuristic_og == marker_og).mean()
    print(f"marker vs x<50 heuristic agreement on {len(goals):,} matched goals: "
          f"{agree:.2%} ({int(marker_og.sum()):,} marker-flagged, "
          f"{int(heuristic_og.sum()):,} heuristic-flagged)")
    if agree < 0.99:
        sys.exit(
            f"ABORT: marker/heuristic agreement only {agree:.2%} (expected >=99%) "
            "— the qualifier-28 parse is likely broken; refusing to write."
        )

    # Coalesce: keep any value the scraper already wrote, fill the rest.
    if "is_own_goal" in shots.columns:
        merged["is_own_goal"] = merged["is_own_goal"].fillna(merged["_og_derived"])
    else:
        merged["is_own_goal"] = merged["_og_derived"]
    merged = merged.drop(columns=["_og_derived"])

    filled = int(merged["is_own_goal"].notna().sum())
    n_og = int(merged["is_own_goal"].fillna(False).astype(bool).sum())
    print(f"shots with is_own_goal after backfill: {filled:,} / {len(merged):,} "
          f"({filled / len(merged):.1%}); {n_og:,} flagged as own goals")

    tmp_path = f"{out_path}.tmp"
    merged.to_parquet(tmp_path, index=False)
    os.replace(tmp_path, out_path)
    print(f"written: {out_path}")


if __name__ == "__main__":
    main()
