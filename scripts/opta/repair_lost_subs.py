"""Repair opta_events substitutions lost to the old re-save key (R196).

Until pannadata#181 the scraper re-saved events on (match_id, event_type,
minute, player_id). Substitution rows carry player_id '', so every sub made in
the same minute of a match (either team) collapsed to one row. This rebuilds
the lost rows from the raw per-competition events (events_<comp>.parquet:
type 18 = player off, type 19 = player on), which kept them all. No API calls.

Also drops the 0:00 placeholder copy of a sub (one per match, mostly
2010-2018 feeds) wherever the same sub is stored at its real time.

Rules, all measured on opta-latest 2026-10-13 (see the R196 triage note):
- Pairing: event_id is not in time order before ~2019, so offs and ons are
  paired inside exact (match, team, period, minute, second) groups, k-th off
  with k-th on by event_id. Groups with unequal counts are skipped. Where a
  group holds more than one pair the times are right but which player
  replaced which is a guess.
- Clock: pre-~2019 feeds store the display minute (raw minute + 1, second 0);
  newer feeds store the raw time. A rebuilt row takes the clock offset of a
  kept sub in the same match and raw minute, else the match's own offset,
  else the raw clock. Otherwise a re-added sub would sit up to a minute from
  its kept sibling and cut a fake splint.
- A rebuilt pair is added only when its on-player is not already stored as
  coming on in that match.

Usage:
    python repair_lost_subs.py --events opta_events.parquet --raw-dir DIR \
        --lineups opta_lineups.parquet --out opta_events.parquet [--dry-run]
"""
import argparse
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

RAW_COLS = ["match_id", "event_id", "type_id", "player_id", "player_name",
            "team_id", "minute", "second", "period_id", "competition", "season"]
GROUP = ["match_id", "team_id", "period_id", "minute", "second"]
KEY = ["match_id", "player_on_id", "player_off_id"]
# Refuse to write if the repair adds more than this many rows: the 2026-10-13
# dry run added 51,043, so a much bigger number means a rule broke.
MAX_ADDED = 80_000


def read_raw_subs(raw_dir):
    files = sorted(Path(raw_dir).glob("events_*.parquet"))
    if not files:
        raise SystemExit(f"no events_*.parquet in {raw_dir}")
    parts = [ds.dataset(f).to_table(columns=RAW_COLS,
                                    filter=ds.field("type_id").isin([18, 19])).to_pandas()
             for f in files]
    s = pd.concat(parts, ignore_index=True).drop_duplicates(["match_id", "event_id"])
    print(f"raw sub events: {len(s):,} in {s.match_id.nunique():,} matches from {len(files)} files")
    return s


def pair_subs(s):
    """One row per (off, on) pair, from exact-timestamp groups that balance."""
    s = s.sort_values(GROUP + ["event_id"]).reset_index(drop=True)
    s["k"] = s.groupby(GROUP + ["type_id"]).cumcount()
    cnt = s.groupby(GROUP + ["type_id"]).size().unstack("type_id", fill_value=0)
    cnt = cnt.reindex(columns=[18, 19], fill_value=0)
    cnt.columns = ["n_off", "n_on"]
    cnt = cnt.reset_index()
    bal = cnt[cnt.n_off == cnt.n_on]
    print(f"timestamp groups {len(cnt):,}, balanced {len(bal):,} "
          f"({100 * len(bal) / max(len(cnt), 1):.2f}%)")
    offs = s[s.type_id == 18].merge(bal[GROUP], on=GROUP)
    ons = s[s.type_id == 19].merge(bal[GROUP + ["n_on"]], on=GROUP)
    p = ons.merge(offs[GROUP + ["k", "player_id", "player_name"]], on=GROUP + ["k"],
                  suffixes=("_on", "_off"))
    return pd.DataFrame({
        "match_id": p.match_id, "team_id": p.team_id, "period_id": p.period_id,
        "raw_min": p.minute.astype("int64"), "raw_sec": p.second.astype("int64"),
        "player_on_id": p.player_id_on, "player_on_name": p.player_name_on,
        "player_off_id": p.player_id_off, "player_off_name": p.player_name_off,
        "competition": p.competition, "season": p.season, "ambiguous": p.n_on > 1,
    })


def repair(events, pairs):
    """Return (repaired events, stats). events = the opta_events table."""
    is_sub = events.event_type == "substitution"
    sub = events[is_sub]
    zero = is_sub & (events.minute == 0) & (events.second == 0)
    timed = sub[~zero.loc[sub.index]]
    z = events[zero].merge(timed[KEY].drop_duplicates(), on=KEY, how="left", indicator=True)
    drop_idx = events[zero].index[(z._merge == "both").to_numpy()]

    t = timed[KEY + ["minute", "second"]].drop_duplicates(KEY)
    m = pairs.merge(t, on=KEY, how="left", indicator=True)
    kept, lost = m[m._merge == "both"].copy(), m[m._merge == "left_only"].copy()

    # Clock offset of kept subs: stored minus raw.
    kept["off"] = kept.minute - kept.raw_min
    kept["no_sec"] = (kept.second == 0) & (kept.raw_sec != 0)
    sib = (kept.groupby(["match_id", "period_id", "raw_min"])
           .agg(off=("off", "first"), no_sec=("no_sec", "max")).reset_index())
    whole = (kept.groupby("match_id")
             .agg(m_off=("off", "median"), m_no_sec=("no_sec", "mean")).reset_index())
    lost = lost.drop(columns=["_merge", "minute", "second"])
    lost = lost.merge(sib, on=["match_id", "period_id", "raw_min"], how="left")
    lost = lost.merge(whole, on="match_id", how="left")
    lost["clock_src"] = "sibling"
    lost.loc[lost.off.isna() & lost.m_off.notna(), "clock_src"] = "match"
    lost.loc[lost.off.isna() & lost.m_off.isna(), "clock_src"] = "raw"
    off = lost.off.fillna(lost.m_off.round()).fillna(0)
    no_sec = lost.no_sec.where(lost.no_sec.notna(), lost.m_no_sec > 0.5).fillna(False).astype(bool)
    lost["minute"] = (lost.raw_min + off).astype("int64")
    lost["second"] = lost.raw_sec.where(~no_sec, 0).astype("int64")

    on_seen = timed[["match_id", "player_on_id"]].drop_duplicates()
    lost = lost.merge(on_seen, on=["match_id", "player_on_id"], how="left", indicator="seen")
    add = lost[lost.seen == "left_only"].drop_duplicates(KEY)

    new = pd.DataFrame({c: pd.Series(dtype=events[c].dtype) for c in events.columns})
    new = pd.concat([new, pd.DataFrame({
        "match_id": add.match_id, "event_type": "substitution",
        "minute": add.minute, "second": add.second, "team_id": add.team_id,
        "player_id": "", "player_name": "",
        "player_on_id": add.player_on_id, "player_on_name": add.player_on_name,
        "player_off_id": add.player_off_id, "player_off_name": add.player_off_name,
        "assist_player_id": "", "assist_player_name": "",
        "competition": add.competition, "season": add.season,
    })], ignore_index=True)[events.columns]
    for c in events.columns:
        new[c] = new[c].astype(events[c].dtype)

    out = pd.concat([events.drop(index=drop_idx), new], ignore_index=True)
    stats = {
        "rows_before": len(events), "rows_after": len(out),
        "placeholders_dropped": len(drop_idx), "added": len(add),
        "added_matches": add.match_id.nunique(),
        "added_ambiguous": int(add.ambiguous.sum()),
        "clock_src": add.clock_src.value_counts().to_dict(),
        "skipped_on_already_stored": int((lost.seen == "both").sum()),
    }
    return out, stats


def lineup_agreement(events, lineups, raw_matches):
    """Share of matches (with raw events and lineup subs) whose sub rows
    equal the lineup sub-on count."""
    lon = (lineups[(lineups.is_starter == False) & (lineups.minutes_played > 0)]  # noqa: E712
           .groupby("match_id").size())
    sub = events[(events.event_type == "substitution")
                 & ~((events.minute == 0) & (events.second == 0))]
    n = sub[KEY].drop_duplicates().groupby("match_id").size()
    c = pd.concat([lon.rename("lu"), n.rename("ev")], axis=1).fillna(0)
    c = c[c.index.isin(raw_matches) & (c.lu > 0)]
    return {"matches": len(c), "equal": float((c.ev == c.lu).mean()),
            "over": int((c.ev > c.lu).sum()), "under": int((c.ev < c.lu).sum())}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--events", required=True)
    ap.add_argument("--raw-dir", required=True)
    ap.add_argument("--lineups", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)

    events_tbl = pq.read_table(a.events)
    events = events_tbl.to_pandas()
    lineups = pq.read_table(a.lineups, columns=["match_id", "is_starter", "minutes_played"]).to_pandas()
    raw = read_raw_subs(a.raw_dir)
    pairs = pair_subs(raw)
    out, st = repair(events, pairs)
    raw_m = set(raw.match_id)
    before, after = lineup_agreement(events, lineups, raw_m), lineup_agreement(out, lineups, raw_m)
    for k, v in st.items():
        print(f"{k}: {v:,}" if isinstance(v, int) else f"{k}: {v}")
    print(f"lineup agreement over {before['matches']:,} matches: {100 * before['equal']:.1f}% -> "
          f"{100 * after['equal']:.1f}% (after: {after['over']:,} over, {after['under']:,} under)")

    if st["added"] > MAX_ADDED:
        sys.exit(f"::error::refusing: {st['added']:,} rows to add is above {MAX_ADDED:,}")
    if after["equal"] < before["equal"]:
        sys.exit("::error::refusing: lineup agreement got worse")
    if st["rows_after"] != st["rows_before"] - st["placeholders_dropped"] + st["added"]:
        sys.exit("::error::row arithmetic does not add up")
    if a.dry_run:
        print("DRY RUN: nothing written")
        return 0
    # Same schema as the input (pandas would widen strings and add its metadata).
    schema = events_tbl.schema.remove_metadata()
    tbl = pa.Table.from_pandas(out, schema=schema, preserve_index=False).replace_schema_metadata(
        events_tbl.schema.metadata)
    pq.write_table(tbl, a.out, compression="zstd")
    print(f"wrote {a.out} ({len(out):,} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
