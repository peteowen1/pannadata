#!/usr/bin/env python3
"""One-off repair for issue #66: re-derive the `situation` column of the
consolidated opta_shot_events.parquet from the events feed's stored
qualifiers.

Scrapes before 2026-06 used a shifted qualifier mapping (q22 "Regular play"
tagged SetPiece, q24 "Set piece" tagged Corner, q25 "From corner" / q26
"Free kick" unhandled), leaving ~68% of shots mislabelled SetPiece. The
events table stores every event's full qualifier list as qualifier_json,
so the correct situation is re-derivable without re-scraping: join shots
to events on (match_id, event_id), recompute with the corrected mapping
(the same one the scraper now uses), and rewrite.

Usage (from the repo root, after the daily-scrape download step or with
local copies of the consolidated files):

    python scripts/opta/fix_shot_situations.py \
        [shots_parquet] [events_parquet] [out_parquet]

Defaults: data/opta/opta_shot_events.parquet,
data/opta/opta_events.parquet, in-place overwrite of the shots file.

Prints before/after situation distributions and refuses to write when
fewer than 95% of shot rows match an events row (a low match rate means
an id-format or coverage problem, not a labelling one).

NOTE: panna's xG model was trained with situation-derived features
(is_open_play / is_set_piece / is_corner) computed from the OLD labels.
Re-labelling shifts those feature inputs at enrichment time, so plan an
xG model retrain on the repaired data alongside (or shortly after)
running this — see the discussion on issue #66.
"""

import json
import sys

import pandas as pd
import pyarrow.dataset as ds

SHOT_TYPE_IDS = [13, 14, 15, 16]


def derive_situation(qualifier_ids):
    """Corrected Opta mapping, identical to opta_scraper.extract_shots."""
    if 9 in qualifier_ids:
        return "Penalty"
    if 25 in qualifier_ids:
        return "Corner"
    if 24 in qualifier_ids or 26 in qualifier_ids:
        return "SetPiece"
    return "OpenPlay"


def qualifier_ids_of(qualifier_json):
    if not qualifier_json:
        return set()
    try:
        return {q.get("qualifierId") for q in json.loads(qualifier_json)}
    except (ValueError, TypeError, AttributeError):
        return set()


def main():
    shots_path = sys.argv[1] if len(sys.argv) > 1 else "data/opta/opta_shot_events.parquet"
    events_path = sys.argv[2] if len(sys.argv) > 2 else "data/opta/opta_events.parquet"
    out_path = sys.argv[3] if len(sys.argv) > 3 else shots_path

    shots = pd.read_parquet(shots_path)
    print(f"shots: {len(shots):,} rows")
    print("situation BEFORE:", shots["situation"].value_counts().to_dict())

    events = ds.dataset(events_path).to_table(
        columns=["match_id", "event_id", "type_id", "qualifier_json"],
        filter=ds.field("type_id").isin(SHOT_TYPE_IDS),
    ).to_pandas()
    print(f"shot-type event rows with qualifiers: {len(events):,}")

    events["situation_fixed"] = events["qualifier_json"].map(
        lambda qj: derive_situation(qualifier_ids_of(qj))
    )
    events = events.drop_duplicates(subset=["match_id", "event_id"], keep="last")

    merged = shots.merge(
        events[["match_id", "event_id", "situation_fixed"]],
        on=["match_id", "event_id"],
        how="left",
    )
    match_rate = merged["situation_fixed"].notna().mean()
    print(f"events match rate: {match_rate:.1%}")
    if match_rate < 0.95:
        sys.exit(
            f"ABORT: only {match_rate:.1%} of shot rows matched an events row "
            "(expected ~100%) — check id formats / events coverage before writing."
        )

    changed = (
        merged["situation_fixed"].notna()
        & (merged["situation_fixed"] != merged["situation"])
    ).sum()
    merged["situation"] = merged["situation_fixed"].fillna(merged["situation"])
    merged = merged.drop(columns=["situation_fixed"])

    print(f"rows relabelled: {changed:,}")
    print("situation AFTER:", merged["situation"].value_counts().to_dict())

    merged.to_parquet(out_path, index=False)
    print(f"written: {out_path}")


if __name__ == "__main__":
    main()
