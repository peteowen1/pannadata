#!/usr/bin/env python3
"""One-off backfill: correct `goalmouth_y` for Miss shots (type_id==13) in the
consolidated opta_shot_events.parquet, from the events feed's stored
qualifiers -- no re-scrape needed.

panna#175 / inthegame-blog#489: Opta qualifier 102 (goal-mouth y) is
reliable for on-target-ish shots (Post/Saved/Goal) but scatters far outside
the goal frame for genuine misses. Qualifier 231 was validated as a correct
y-coordinate via a natural experiment: Post-hit shots (which MUST cross the
line at the known post positions, 45.2/54.8) land almost exactly there when
read via q231. The scraper now uses q231 for Miss shots going forward
(opta_scraper.ShotEvent); this script corrects historical rows.

Unlike backfill_goalmouth.py's fillna-only coalesce (goalmouth_y is already
100% present for misses -- just wrong, not NA), this script explicitly
OVERWRITES goalmouth_y for type_id==13 rows with the q231-derived value
(falling back to the existing q102-derived value when q231 is absent, ~13.6%
of misses -- no worse than the pre-fix value for that residual). All other
rows and columns are left completely untouched.

No working z/height (q103) replacement exists for misses -- q230 tracks
shot distance (a confidence score, not a coordinate), q147 is too sparse
(~13.5% presence) to rely on. goalmouth_z is NOT touched by this script.

Usage (from the repo root, after downloading the consolidated shots file
and the per-league events_consolidated files):

    python scripts/opta/backfill_miss_shot_y.py \
        [shots_parquet] [events_path] [out_parquet]

Defaults: data/opta/opta_shot_events.parquet,
data/opta/events_consolidated (a directory of per-league parquets; a single
parquet file also works), in-place overwrite of the shots file. The write
is atomic: <out>.tmp then os.replace.

Guards — the script refuses to write when any fails (a one-off repair must
crash, never impute):
  - events coverage < 50% of shot rows (an id-format break; an expected
    coverage gap from the events_consolidated backlog is only reported)
  - > 1% of Miss-type event rows carry no qualifiers (a real shot always
    has qualifiers — points at a parse or upstream-data problem)
  - q231 presence on matched Miss shots falls outside [70%, 95%] (panna#175
    measured ~86.4% on a WC sample; well outside that band points at a
    qualifier-id or join regression, not real variation)
  - frame sanity: checked on the q231-derived values THEMSELVES (not the
    blended output) -- q231 alone is tight around the goal frame (panna#175
    measured std~2.5, 1-99% range ~44-56 on a WC sample); the fallback
    residual (~13.6% of misses, using the old, wide-scattering q102 value)
    is expected to widen the blended output's own tails and is not gated.
"""

import json
import os
import sys

import pandas as pd
import pyarrow.dataset as ds

MISS_TYPE_ID = 13


def main():
    shots_path = sys.argv[1] if len(sys.argv) > 1 else "data/opta/opta_shot_events.parquet"
    events_path = sys.argv[2] if len(sys.argv) > 2 else "data/opta/events_consolidated"
    out_path = sys.argv[3] if len(sys.argv) > 3 else shots_path

    shots = pd.read_parquet(shots_path)
    print(f"shots: {len(shots):,} rows")
    if "goalmouth_y" not in shots.columns:
        sys.exit("ABORT: shots file lacks goalmouth_y -- run backfill_goalmouth.py first.")

    events = ds.dataset(events_path).to_table(
        columns=["match_id", "event_id", "type_id", "qualifier_json"],
        filter=ds.field("type_id") == MISS_TYPE_ID,
    ).to_pandas()
    print(f"Miss-type event rows: {len(events):,}")

    no_quals = events["qualifier_json"].isna() | (events["qualifier_json"] == "")
    if no_quals.any():
        frac = no_quals.mean()
        print(f"event rows with NO qualifiers: {int(no_quals.sum()):,} ({frac:.2%}) "
              "— dropped, no correction derivable for them")
        if frac > 0.01:
            sys.exit(
                "ABORT: >1% of Miss-type event rows carry no qualifiers — a real "
                "shot always has qualifiers; check the events input before writing."
            )
        events = events[~no_quals]

    def _extract(qjson, qid):
        # qualifier_json is a flat dict keyed by stringified qualifier id,
        # e.g. '{"102":"52.6","231":"49.8"}' (cf. backfill_goalmouth.py).
        d = json.loads(qjson)
        v = d.get(str(qid))
        if v in (None, ""):
            return None
        return float(v.replace(",", ".") if isinstance(v, str) else v)

    events["q231"] = events["qualifier_json"].map(lambda s: _extract(s, 231))

    dup_mask = events.duplicated(subset=["match_id", "event_id"], keep=False)
    if dup_mask.any():
        print(f"duplicate (match_id, event_id) event rows: {int(dup_mask.sum()):,} "
              "— keeping last")
    events = events.drop_duplicates(subset=["match_id", "event_id"], keep="last")

    merged = shots.merge(
        events[["match_id", "event_id", "q231"]],
        on=["match_id", "event_id"],
        how="left",
    )
    is_miss = merged["type_id"] == MISS_TYPE_ID
    # "matched" means "Miss shot with an events row" -- q231 itself may
    # still be None (the ~13.6% residual), that's fine, checked separately.
    miss_matched = merged.merge(
        events[["match_id", "event_id"]].assign(_m=1),
        on=["match_id", "event_id"], how="left",
    )["_m"].notna()
    match_rate = miss_matched[is_miss].mean() if is_miss.any() else float("nan")
    print(f"events coverage: {match_rate:.1%} of Miss shots have an events row "
          f"({int((is_miss & ~miss_matched).sum()):,} unmatched — events backlog; "
          "left untouched, re-run after rebuild-events heals them)")
    if match_rate < 0.50:
        sys.exit(
            f"ABORT: only {match_rate:.1%} of Miss shots matched an events row — "
            "that is an id-format break, not a coverage gap; check inputs."
        )

    q231_rate = merged.loc[is_miss & miss_matched, "q231"].notna().mean()
    print(f"q231 presence on matched Miss shots: {q231_rate:.1%} "
          "(panna#175 measured ~86.4% on a WC sample)")
    if not (0.70 <= q231_rate <= 0.95):
        sys.exit(
            f"ABORT: q231 presence {q231_rate:.1%} is outside the plausible "
            "[70%, 95%] band — check qualifier id / join before writing."
        )

    # Frame sanity on q231 ITSELF (not the blended output -- the fallback
    # residual legitimately inherits q102's wide scatter, see module docstring).
    q231_vals = merged.loc[is_miss, "q231"].dropna()
    if len(q231_vals) < 100:
        sys.exit(f"ABORT: only {len(q231_vals)} Miss shots have q231 -- too few to validate.")
    q_med, q_lo, q_hi = q231_vals.median(), q231_vals.quantile(0.01), q231_vals.quantile(0.99)
    print(f"q231 (Miss shots): median={q_med:.1f}  1%={q_lo:.1f}  99%={q_hi:.1f} "
          "(expect ~50 / tightly inside [40,60])")
    if not (45 <= q_med <= 55 and q_lo >= 40 and q_hi <= 60):
        sys.exit(
            "ABORT: q231 does not sit tightly mid-goal — the parse is almost "
            "certainly broken; refusing to write."
        )

    # Overwrite: q231 where present, else keep the existing (q102-derived)
    # value -- never worse than the pre-fix data for the residual.
    corrected = merged["goalmouth_y"].copy()
    use_q231 = is_miss & merged["q231"].notna()
    corrected[use_q231] = merged.loc[use_q231, "q231"]
    n_changed = int((is_miss & merged["q231"].notna() & (merged["goalmouth_y"] != merged["q231"])).sum())
    print(f"Miss shots with goalmouth_y corrected: {n_changed:,} / {int(is_miss.sum()):,}")

    shots["goalmouth_y"] = corrected.values
    tmp_path = f"{out_path}.tmp"
    shots.to_parquet(tmp_path, index=False)
    os.replace(tmp_path, out_path)
    print(f"written: {out_path}")


if __name__ == "__main__":
    main()
