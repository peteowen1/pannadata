#!/usr/bin/env python3
"""One-off clean-up for pannadata#173 / N003: phantom 0:00 copies in opta_events.parquet.

Older scraper versions stamped events with no timeMinSec as 0:00. A later re-scrape
added correctly timed rows, and the keyed dedup (match_id, event_type, minute,
player_id) kept both. This drops every 0:00 row that has a timed twin (same match,
event type and player key) and keeps 0:00 rows with no timed twin.

Covers goal, card and substitution rows (cards/subs were duplicated the same way).
Never overwrites the input unless --in-place is given. Uploading is NOT done here.

    python clean_untimed_event_duplicates.py IN.parquet --output OUT.parquet
"""
import argparse
import sys
from pathlib import Path

import pandas as pd

KEYS = {
    "goal": ["team_id", "player_id"],
    "yellow_card": ["player_id"],
    "red_card": ["player_id"],
    "second_yellow": ["player_id"],
    "substitution": ["player_on_id", "player_off_id"],
}


def phantom_mask(df: pd.DataFrame) -> pd.Series:
    """True for 0:00 rows that have a timed row with the same match/type/key."""
    zero = (df["minute"].fillna(0) == 0) & (df["second"].fillna(0) == 0)
    drop = pd.Series(False, index=df.index)
    for etype, keys in KEYS.items():
        sel = df["event_type"] == etype
        if not sel.any():
            continue
        cols = ["match_id"] + keys
        sub = df.loc[sel, cols].fillna("")
        timed = sub[~zero[sel]].drop_duplicates()
        merged = sub.merge(timed.assign(_t=True), on=cols, how="left")
        drop[sel] = (zero[sel].to_numpy() & merged["_t"].fillna(False).to_numpy())
    return drop


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("input", type=Path)
    ap.add_argument("--output", type=Path)
    ap.add_argument("--in-place", action="store_true", help="overwrite the input file")
    a = ap.parse_args(argv)
    if not a.output and not a.in_place:
        ap.error("give --output (or --in-place); refusing to guess")
    out = a.input if a.in_place else a.output
    if out.resolve() == a.input.resolve() and not a.in_place:
        ap.error("--output must differ from the input unless --in-place")

    df = pd.read_parquet(a.input)
    drop = phantom_mask(df)
    zero = (df["minute"].fillna(0) == 0) & (df["second"].fillna(0) == 0)
    rep = pd.DataFrame({"rows": df["event_type"].value_counts(),
                        "zero_before": df[zero]["event_type"].value_counts(),
                        "dropped": df[drop]["event_type"].value_counts()}).fillna(0).astype(int)
    print(f"input rows {len(df):,}; dropping {int(drop.sum()):,}; keeping {int((~drop).sum()):,}")
    print(rep.to_string())
    g = df["event_type"] == "goal"
    print(f"goal rows {int(g.sum()):,} -> {int((g & ~drop).sum()):,}; "
          f"goal 0:00 rows {int((g & zero).sum()):,} -> {int((g & zero & ~drop).sum()):,}")
    df[~drop].to_parquet(out, index=False)
    print(f"wrote {out}")


if __name__ == "__main__":
    sys.exit(main())
