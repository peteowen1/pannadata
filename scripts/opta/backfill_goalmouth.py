#!/usr/bin/env python3
"""One-off backfill: add goal-mouth placement (`goalmouth_y`, `goalmouth_z`)
to the consolidated opta_shot_events.parquet from the events feed's stored
qualifiers — no re-scrape needed.

Opta qualifier 102 = goal-mouth y (where the shot crosses the goal-line
plane, on the 0-100 pitch-width scale; posts ~45.2/54.8) and 103 = z
(height; crossbar ~38). These are present on EVERY shot — on- and
off-target — because Opta projects the crossing point even for misses.
The scraper now captures them on new matches (opta_scraper.ShotEvent), but
historical shot rows predate that change. The match_events table —
events_consolidated/events_<comp>.parquet — stores each event's full
qualifier set as `qualifier_json`, a dict keyed by stringified qualifier ID
with the VALUE retained, e.g. '{"102":"52.6","103":"19"}' (cf.
opta_scraper.AllMatchEvent). So the placement is re-derivable: join shots to
events on (match_id, event_id), pull 102/103, and rewrite.

Usage (from the repo root, after downloading the consolidated shots file
and the per-league events_consolidated files):

    python scripts/opta/backfill_goalmouth.py \
        [shots_parquet] [events_path] [out_parquet]

Defaults: data/opta/opta_shot_events.parquet,
data/opta/events_consolidated (a directory of per-league parquets; a single
parquet file also works), in-place overwrite of the shots file. The write
is atomic: <out>.tmp then os.replace.

Goalmouth coverage is NOT uniform over history: Opta only reliably carries
102/103 from the 2021-22 season onward (older feeds have them on ~55% of
shots). Older shots therefore keep NA — a permanent data fact, not fixable —
and the xGOT model trains on the complete window (>= GM_COMPLETE_FROM).

Guards — the script refuses to write when any fails (a one-off repair must
crash, never impute):
  - events coverage < 50% of shot rows (an id-format break; an expected
    coverage gap from the events_consolidated backlog is only reported)
  - > 1% of shot-type event rows carry no qualifiers (a real shot always
    has qualifiers — points at a parse or upstream-data problem)
  - recent (>= GM_COMPLETE_FROM) on-target shots with a derived gm_y < 99%
    (these feeds are complete, so a gap is a parse/join regression; older
    seasons are reported per-year, not gated)
  - frame sanity: the median gm_y of GOALS must sit mid-goal (48-52) and
    their 1-99% gm_y span must fall inside [43, 57] (the posts). A parse
    that mangles the coordinate fails this and aborts.
"""

import json
import os
import re
import sys

import pandas as pd
import pyarrow.dataset as ds

SHOT_TYPE_IDS = [13, 14, 15, 16]
ON_TARGET_TYPE_IDS = [15, 16]
GOAL_TYPE_ID = 16
# Opta goalmouth qualifiers (102/103) are only reliably present from the
# 2021-22 season onward (older feeds carry them on ~55% of shots, missing
# not-at-random). Seasons ending >= this year form the complete training
# window and are where the parse-correctness gate applies; older shots
# legitimately keep NA and are reported, not gated.
GM_COMPLETE_FROM = 2021


def season_end_year(season):
    """Trailing 4-digit year of a season label ('2024-2025'->2025, '2024'
    ->2024, '2025 Qatar'->2025). None when no year is present."""
    years = re.findall(r"\d{4}", str(season))
    return int(max(years)) if years else None


def goalmouth_of(qualifier_json):
    """Return (gm_y, gm_z) floats from a shot's qualifier_json, or (None,
    None) when absent. qualifier_json keeps the qualifier VALUE, so 102/103
    carry the coordinate. Malformed JSON raises: a one-off repair must
    crash, never impute. Blank/missing coordinate -> None (NaN), never 0."""
    d = json.loads(qualifier_json)

    def _f(key):
        v = d.get(key)
        if v in (None, ""):
            return None
        # Opta's API occasionally returns European-locale decimals with a
        # comma (e.g. "49,8026") — normalise before float (cf. CLAUDE.md note
        # on latin-1/cp1252 responses). A genuinely unparseable value raises.
        return float(v.replace(",", ".") if isinstance(v, str) else v)

    return _f("102"), _f("103")


def main():
    shots_path = sys.argv[1] if len(sys.argv) > 1 else "data/opta/opta_shot_events.parquet"
    events_path = sys.argv[2] if len(sys.argv) > 2 else "data/opta/events_consolidated"
    out_path = sys.argv[3] if len(sys.argv) > 3 else shots_path

    shots = pd.read_parquet(shots_path)
    print(f"shots: {len(shots):,} rows")
    already = [c for c in ("goalmouth_y", "goalmouth_z") if c in shots.columns]
    if already:
        have = shots[already[0]].notna().mean()
        print(f"existing {already} columns present ({have:.1%} non-null) "
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
              "— dropped, no placement derivable for them")
        if frac > 0.01:
            sys.exit(
                "ABORT: >1% of shot-type event rows carry no qualifiers — a real "
                "shot always has qualifiers; check the events input before writing."
            )
        events = events[~no_quals]

    gm = events["qualifier_json"].map(goalmouth_of)
    events["gm_y"] = [t[0] for t in gm]
    events["gm_z"] = [t[1] for t in gm]

    # Frame sanity from GOALS (must sit inside the posts / under the bar).
    goals = events.loc[events["type_id"] == GOAL_TYPE_ID, "gm_y"].dropna()
    if len(goals) < 100:
        sys.exit(f"ABORT: only {len(goals)} goals carry a gm_y — too few to "
                 "validate the frame; check the events input.")
    g_med, g_lo, g_hi = goals.median(), goals.quantile(0.01), goals.quantile(0.99)
    print(f"GOALS gm_y: median={g_med:.1f}  1%={g_lo:.1f}  99%={g_hi:.1f} "
          "(expect ~50 / inside posts ~45-55)")
    if not (48 <= g_med <= 52 and g_lo >= 43 and g_hi <= 57):
        sys.exit(
            "ABORT: goals' gm_y does not sit mid-goal inside the posts — the "
            "102/103 parse is almost certainly broken; refusing to write."
        )

    dup_mask = events.duplicated(subset=["match_id", "event_id"], keep=False)
    if dup_mask.any():
        print(f"duplicate (match_id, event_id) event rows: {int(dup_mask.sum()):,} "
              "— keeping last")
    events = events.drop_duplicates(subset=["match_id", "event_id"], keep="last")

    merged = shots.merge(
        events[["match_id", "event_id", "gm_y", "gm_z"]],
        on=["match_id", "event_id"],
        how="left",
    )
    # A shot is "matched" if its (match_id, event_id) is present in the events
    # table at all — even when that event carried no 102/103 (rare). Coverage
    # below reflects the known events_consolidated backlog, NOT a parse bug:
    # unmatched shots have no event feed yet and correctly keep NA placement.
    matched = shots.merge(
        events[["match_id", "event_id"]].assign(_m=1),
        on=["match_id", "event_id"], how="left",
    )["_m"].notna()
    match_rate = matched.mean()
    print(f"events coverage: {match_rate:.1%} of shots have an events row "
          f"({int((~matched).sum()):,} unmatched — events backlog; keep NA, "
          "re-run after rebuild-events heals them)")
    # Catastrophe floor only: near-zero means an id-format break, not coverage.
    if match_rate < 0.50:
        sys.exit(
            f"ABORT: only {match_rate:.1%} of shots matched an events row — "
            "that is an id-format break, not a coverage gap; check inputs."
        )

    # Parse gate, scoped to the complete window: of on-target shots we COULD
    # match, those in recent seasons must virtually all carry a crossing point
    # (an on-target shot always crosses the plane, and 2021-22+ feeds are
    # complete). Older seasons are reported, not gated — their sparsity is a
    # permanent data fact, not a parse bug.
    ot = merged.loc[matched & merged["type_id"].isin(ON_TARGET_TYPE_IDS)].copy()
    ot["end_year"] = ot["season"].map(season_end_year)
    cov = ot.groupby("end_year")["gm_y"].agg(n="size", have=lambda v: v.notna().mean())
    print("on-target gm_y coverage by season end-year (matched):")
    for yr, row in cov.iterrows():
        flag = "" if (yr is not None and yr >= GM_COMPLETE_FROM and row["have"] >= 0.99) else "  <-"
        print(f"  {yr}: n={int(row['n']):>7,}  {row['have']:6.1%}{flag}")
    recent = ot[ot["end_year"].fillna(0) >= GM_COMPLETE_FROM]
    recent_have = recent["gm_y"].notna().mean() if len(recent) else float("nan")
    print(f"training window (>= {GM_COMPLETE_FROM}): {recent_have:.1%} of "
          f"{len(recent):,} on-target shots have gm_y")
    if not (len(recent) and recent_have >= 0.99):
        sys.exit(
            f"ABORT: recent on-target shots only {recent_have:.1%} have gm_y "
            f"(expected ~100% from {GM_COMPLETE_FROM}-onward) — that is a "
            "parse/join regression, not the known historical sparsity."
        )

    # Coalesce: keep any value the scraper already wrote, fill the rest.
    for col in ("goalmouth_y", "goalmouth_z"):
        src = "gm_y" if col == "goalmouth_y" else "gm_z"
        if col in shots.columns:
            merged[col] = merged[col].fillna(merged[src])
        else:
            merged[col] = merged[src]
    merged = merged.drop(columns=["gm_y", "gm_z"])

    filled = int(merged["goalmouth_y"].notna().sum())
    print(f"shots with goalmouth_y after backfill: {filled:,} / {len(merged):,} "
          f"({filled / len(merged):.1%})")

    tmp_path = f"{out_path}.tmp"
    merged.to_parquet(tmp_path, index=False)
    os.replace(tmp_path, out_path)
    print(f"written: {out_path}")


if __name__ == "__main__":
    main()
