#!/usr/bin/env python3
"""One-off repair for issue #66: re-derive the `situation` column of the
consolidated opta_shot_events.parquet from the events feed's stored
qualifiers.

Scrapes before 2026-06 used a shifted qualifier mapping (q22 "Regular play"
tagged SetPiece, q24 "Set piece" tagged Corner, q25 "From corner" / q26
"Free kick" unhandled), leaving ~68% of shots mislabelled SetPiece. The
match_events table — shipped per-league as
events_consolidated/events_<comp>.parquet — stores every event's full
qualifier set as `qualifier_json`, a dict keyed by stringified qualifier
ID, e.g. '{"22":"","140":"45.3"}' (see opta_scraper.AllMatchEvent). The
correct situation is therefore re-derivable without re-scraping: join
shots to events on (match_id, event_id), recompute with the corrected
mapping (the same one the scraper now uses), and rewrite.

Usage (from the repo root, after downloading the consolidated shots file
and the per-league events_consolidated files):

    python scripts/opta/fix_shot_situations.py \
        [shots_parquet] [events_path] [out_parquet]

Defaults: data/opta/opta_shot_events.parquet,
data/opta/events_consolidated (a directory of per-league parquets; a
single parquet file also works), in-place overwrite of the shots file.
The write is atomic: <out>.tmp then os.replace.

Guards — the script refuses to write when any fails:
  - events match rate < 95% of shot rows (id-format or coverage problem)
  - > 1% of shot-type event rows carry no qualifiers (a real shot always
    has qualifiers — points at a parse or upstream-data problem)
  - duplicate (match_id, event_id) event rows disagreeing on the derived
    situation
  - degenerate output: zero derived Penalty or Corner rows, or one class
    covering > 95% of derived labels
  - Penalty drift: q9 mapped to Penalty under BOTH the old and new
    mappings, so the Penalty count must be ~unchanged

NOTE: panna's xG model was trained with situation-derived features
(is_open_play / is_set_piece / is_corner) computed from the OLD labels.
Re-labelling shifts those feature inputs at enrichment time, so plan an
xG model retrain on the repaired data alongside (or shortly after)
running this — see the discussion on issue #66.
"""

import json
import os
import sys

import pandas as pd
import pyarrow.dataset as ds

SHOT_TYPE_IDS = [13, 14, 15, 16]


def derive_situation(qualifier_ids):
    """Corrected Opta mapping, identical to opta_scraper.extract_shot_events."""
    if 9 in qualifier_ids:
        return "Penalty"
    if 25 in qualifier_ids:
        return "Corner"
    if 24 in qualifier_ids or 26 in qualifier_ids:
        return "SetPiece"
    return "OpenPlay"


def qualifier_ids_of(qualifier_json):
    """qualifier_json is a dict keyed by stringified qualifier ID (the
    scraper writes "" for events with no qualifiers — anomalous on a shot;
    callers drop those rows before mapping). Malformed JSON raises: a
    one-off repair must crash, never impute."""
    return {int(k) for k in json.loads(qualifier_json)}


def main():
    shots_path = sys.argv[1] if len(sys.argv) > 1 else "data/opta/opta_shot_events.parquet"
    events_path = sys.argv[2] if len(sys.argv) > 2 else "data/opta/events_consolidated"
    out_path = sys.argv[3] if len(sys.argv) > 3 else shots_path

    shots = pd.read_parquet(shots_path)
    print(f"shots: {len(shots):,} rows")
    before_counts = shots["situation"].value_counts().to_dict()
    print("situation BEFORE:", before_counts)

    events = ds.dataset(events_path).to_table(
        columns=["match_id", "event_id", "type_id", "qualifier_json"],
        filter=ds.field("type_id").isin(SHOT_TYPE_IDS),
    ).to_pandas()
    print(f"shot-type event rows: {len(events):,}")

    no_quals = events["qualifier_json"].isna() | (events["qualifier_json"] == "")
    if no_quals.any():
        frac = no_quals.mean()
        print(f"event rows with NO qualifiers: {int(no_quals.sum()):,} ({frac:.2%}) "
              "— dropped, no fix derivable for them")
        if frac > 0.01:
            sys.exit(
                "ABORT: >1% of shot-type event rows carry no qualifiers — a real "
                "shot always has qualifiers; check the events input before writing."
            )
        events = events[~no_quals]

    events["situation_fixed"] = events["qualifier_json"].map(
        lambda qj: derive_situation(qualifier_ids_of(qj))
    )

    derived = events["situation_fixed"].value_counts()
    print("derived situation (events side):", derived.to_dict())
    if derived.get("Penalty", 0) == 0 or derived.get("Corner", 0) == 0:
        sys.exit(
            "ABORT: derived zero Penalty or Corner rows — the qualifier parse "
            "is almost certainly broken; refusing to write."
        )
    if (derived / len(events)).max() > 0.95:
        sys.exit(
            "ABORT: one situation class covers >95% of derived labels — "
            "degenerate output points at a parse problem; refusing to write."
        )

    dup_mask = events.duplicated(subset=["match_id", "event_id"], keep=False)
    if dup_mask.any():
        conflicts = (
            events.loc[dup_mask]
            .groupby(["match_id", "event_id"])["situation_fixed"]
            .nunique()
        )
        n_conflict = int((conflicts > 1).sum())
        print(f"duplicate (match_id, event_id) rows: {int(dup_mask.sum()):,} "
              f"in {len(conflicts):,} groups, {n_conflict:,} with conflicting "
              "derived situations")
        if n_conflict > 0:
            sys.exit(
                "ABORT: duplicate event rows disagree on the derived situation "
                "— resolve the events input before writing."
            )
    events = events.drop_duplicates(subset=["match_id", "event_id"], keep="last")

    merged = shots.merge(
        events[["match_id", "event_id", "situation_fixed"]],
        on=["match_id", "event_id"],
        how="left",
    )
    unmatched = int(merged["situation_fixed"].isna().sum())
    match_rate = 1 - unmatched / len(merged)
    print(f"events match rate: {match_rate:.1%} ({unmatched:,} shot rows "
          "unmatched — they keep their OLD, possibly wrong, labels)")
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

    # q9 -> Penalty under both the old and new mappings, so the Penalty count
    # is the parse canary: any real movement means the derivation is broken.
    before_pen = before_counts.get("Penalty", 0)
    after_pen = int((merged["situation"] == "Penalty").sum())
    if abs(after_pen - before_pen) > max(50, 0.02 * before_pen):
        sys.exit(
            f"ABORT: Penalty count moved {before_pen:,} -> {after_pen:,} but "
            "should be ~unchanged (q9 was correct under both mappings) — "
            "parse problem; refusing to write."
        )

    print(f"rows relabelled: {changed:,}")
    print("situation AFTER:", merged["situation"].value_counts().to_dict())

    tmp_path = f"{out_path}.tmp"
    merged.to_parquet(tmp_path, index=False)
    os.replace(tmp_path, out_path)
    print(f"written: {out_path}")


if __name__ == "__main__":
    main()
