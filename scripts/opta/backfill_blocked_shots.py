#!/usr/bin/env python3
"""One-off backfill: add the blocked-shot marker (`is_blocked`) to the
consolidated opta_shot_events.parquet from the events feed's stored
qualifiers — no re-scrape needed.

Opta qualifier 82 flags a shot blocked by an outfield defender before it
reached the goal frame. type_id=15 ("Attempt Saved") is an umbrella Opta
uses for BOTH real goalkeeper saves and these blocked shots — a blocked
shot never crosses the goal-line plane, so its goalmouth_y/z (qualifiers
102/103, see backfill_goalmouth.py) is a placeholder, not a real crossing
point (panna#176: 98%+ of blocked shots carry goalmouth_z==19, the
frame-height midpoint). panna's xGOT model excludes is_blocked shots from
its on-target population — see OPTA_REFERENCE.md's on-target formula
`(type 15 & !q82) | type 16`.

The scraper now captures is_blocked on new matches (opta_scraper.ShotEvent),
but historical shot rows predate that change. Same re-derivation trick as
backfill_goalmouth.py: join shots to events on (match_id, event_id), check
qualifier 82's presence in qualifier_json, and write.

Usage (from the repo root, after downloading the consolidated shots file
and the per-league events_consolidated files):

    python scripts/opta/backfill_blocked_shots.py \
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
  - the derived is_blocked rate on matched type_id==15 rows falls outside
    [5%, 95%] (a real parse should land well inside that band — panna#176
    measured ~56% on an EPL 2024-25 sample; a value near 0% or 100% points
    at a qualifier-id or join regression, not real variation)
"""

import os
import sys

import pandas as pd
import pyarrow.dataset as ds

SHOT_TYPE_IDS = [13, 14, 15, 16]
SAVED_TYPE_ID = 15
BLOCKED_QUALIFIER_ID = "82"


def main():
    shots_path = sys.argv[1] if len(sys.argv) > 1 else "data/opta/opta_shot_events.parquet"
    events_path = sys.argv[2] if len(sys.argv) > 2 else "data/opta/events_consolidated"
    out_path = sys.argv[3] if len(sys.argv) > 3 else shots_path

    shots = pd.read_parquet(shots_path)
    print(f"shots: {len(shots):,} rows")
    if "is_blocked" in shots.columns:
        have = shots["is_blocked"].notna().mean()
        print(f"existing is_blocked column present ({have:.1%} non-null) "
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
              "— dropped, no blocked-marker derivable for them")
        if frac > 0.01:
            sys.exit(
                "ABORT: >1% of shot-type event rows carry no qualifiers — a real "
                "shot always has qualifiers; check the events input before writing."
            )
        events = events[~no_quals]

    # Presence-only check: a real shot's qualifier_json is a flat dict keyed
    # by stringified qualifier id (cf. backfill_goalmouth.py's docstring),
    # e.g. '{"102":"52.6","103":"19","82":null}' — q82 present (any value,
    # incl. null) means blocked, regardless of what value it carries.
    events["is_blocked"] = events["qualifier_json"].str.contains(
        f'"{BLOCKED_QUALIFIER_ID}":', regex=False
    )

    dup_mask = events.duplicated(subset=["match_id", "event_id"], keep=False)
    if dup_mask.any():
        print(f"duplicate (match_id, event_id) event rows: {int(dup_mask.sum()):,} "
              "— keeping last")
    events = events.drop_duplicates(subset=["match_id", "event_id"], keep="last")

    merged = shots.merge(
        events[["match_id", "event_id", "is_blocked"]].rename(columns={"is_blocked": "_is_blocked"}),
        on=["match_id", "event_id"],
        how="left",
    )
    matched = merged["_is_blocked"].notna()
    match_rate = matched.mean()
    print(f"events coverage: {match_rate:.1%} of shots have an events row "
          f"({int((~matched).sum()):,} unmatched — events backlog; keep NA, "
          "re-run after rebuild-events heals them)")
    if match_rate < 0.50:
        sys.exit(
            f"ABORT: only {match_rate:.1%} of shots matched an events row — "
            "that is an id-format break, not a coverage gap; check inputs."
        )

    # Sanity gate scoped to type_id==15 (the population that actually mixes
    # saves and blocks — 13/14/16 shots aren't expected to carry q82).
    saved = merged.loc[matched & (merged["type_id"] == SAVED_TYPE_ID)]
    blocked_rate = saved["_is_blocked"].mean() if len(saved) else float("nan")
    print(f"type_id==15 (Attempt Saved) blocked-shot rate: {blocked_rate:.1%} "
          f"of {len(saved):,} matched rows (panna#176 measured ~56% on an "
          "EPL 2024-25 sample)")
    if not (len(saved) and 0.05 <= blocked_rate <= 0.95):
        sys.exit(
            f"ABORT: type_id==15 blocked-rate {blocked_rate:.1%} is outside "
            "the plausible [5%, 95%] band — check qualifier id / join before writing."
        )

    # Unmatched rows have _is_blocked == NaN from the left join, so fillna
    # correctly leaves them NA (a real coverage gap, not a false "not blocked").
    if "is_blocked" in shots.columns:
        merged["is_blocked"] = merged["is_blocked"].fillna(merged["_is_blocked"])
    else:
        merged["is_blocked"] = merged["_is_blocked"]
    merged = merged.drop(columns=["_is_blocked"])

    filled = int(merged["is_blocked"].notna().sum())
    print(f"shots with is_blocked after backfill: {filled:,} / {len(merged):,} "
          f"({filled / len(merged):.1%})")

    tmp_path = f"{out_path}.tmp"
    merged.to_parquet(tmp_path, index=False)
    os.replace(tmp_path, out_path)
    print(f"written: {out_path}")


if __name__ == "__main__":
    main()
