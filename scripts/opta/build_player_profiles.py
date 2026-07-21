"""Build opta_players.parquet — one row per player_id with DOB/nationality/height.

pannadata#112: an Age column was wanted on the explainer leaderboards, but no
DOB field exists anywhere in the locally persisted Opta data. Investigation
(2026-07-21) confirmed the Opta MA1 matchstats/lineup player block does NOT
carry dateOfBirth, birthDate, nationality, height, or preferred foot — grepped
234k+ locally-archived raw payloads (2000-2026, every scraped competition)
for those keys: zero hits. A single probe of the Opta squads/person endpoint
(our outlet token) returned HTTP 403 (errorCode 10300, not entitled), so that
surface isn't available either. See pannadata/CLAUDE.md and issue #112 for
the full writeup.

The only viable DOB/nationality/height source today is the reep register
(CC0, github.com/withqwerty/reep) keyed by Opta player_id (`key_opta`) — the
SAME source scripts/build-football-bio.mjs already uses for the blog's
player-bio.json, but that pipeline only covers players present in the live
blog ratings.parquet and only ships to R2. This script instead covers every
player_id ever seen in opta_player_stats.parquet and ships to opta-latest
(the pannadata data bus), so any consumer (not just the blog) can use it.

`dob_raw` (opta_scraper.extract_all_player_stats) is preferred over reep when
present — it's currently always None (see confirmation above) but is captured
defensively in case Opta ever adds the field; this script is already wired to
prefer it the moment it shows up.

Dedup rule (pannadata#86 — duplicate player_ids for the same match): each
player_id appears once per match in opta_player_stats.parquet. Per-column,
"latest non-null wins": rows are sorted by match_date and the last non-null
value per player_id is kept (handles rare cases where an early/thin-feed
match has a blank name field but a later match doesn't). This does NOT solve
#86's harder problem (the SAME real person split across DIFFERENT player_ids)
-- that needs an identity crosswalk, out of scope here. This table's grain is
one row per Opta player_id; a future canonical_player_id column could be
joined on without restructuring it.

Usage (from repo root, after downloading opta_player_stats.parquet):
    python scripts/opta/build_player_profiles.py \\
        [--player-stats data/opta/opta_player_stats.parquet] \\
        [--out data/opta/opta_players.parquet] \\
        [--reep-csv path/to/local/people.csv]   # skip the network download
        [--no-reep]                             # skip reep entirely (opta-only fields)
"""
from __future__ import annotations

import argparse
import os
import sys
import tempfile
from datetime import datetime, timezone

import pandas as pd
import pyarrow.parquet as pq
import requests

REEP_URL = "https://raw.githubusercontent.com/withqwerty/reep/main/data/people.csv"


def _safe_parse_date(value):
    """Parse an ISO-ish date string/Timestamp to a datetime.date, else None.

    Never raises. Missing/blank/garbage input becomes None (NA) rather than a
    crash or a silently-wrong default — per the pannadata "no silent
    imputation" convention (mirrors _to_int_or_0's defensive intent in
    opta_scraper.py, applied to dates instead of ints).
    """
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    s = str(value).strip().rstrip("Z")
    if not s:
        return None
    try:
        ts = pd.to_datetime(s, errors="raise")
    except (ValueError, TypeError):
        return None
    if pd.isna(ts):
        return None
    return ts.date()


def _safe_float_or_none(value):
    """float(value), else None. Never raises."""
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    try:
        f = float(value)
    except (ValueError, TypeError):
        return None
    if f != f:  # NaN
        return None
    return f


def _last_non_null(series: pd.Series):
    """Last non-null value in a (pre-sorted) Series, else None.

    Applied to a per-player_id group already sorted by match_date ascending,
    this is the "latest non-null wins" dedup rule.
    """
    non_null = series.dropna()
    return non_null.iloc[-1] if len(non_null) else None


def dedupe_player_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse opta_player_stats.parquet (one row per player-match) to one
    row per player_id, using "latest non-null wins" (see module docstring).
    """
    if df.empty:
        cols = ["player_id", "player_name", "first_name", "last_name", "dob_raw",
                "first_match_date", "last_match_date", "n_matches"]
        return pd.DataFrame(columns=cols)

    df = df.sort_values(["player_id", "match_date"], kind="stable")
    grouped = df.groupby("player_id", sort=False)

    agg = {
        "player_name": grouped["player_name"].apply(_last_non_null),
        "first_name": grouped["first_name"].apply(_last_non_null),
        "last_name": grouped["last_name"].apply(_last_non_null),
        "first_match_date": grouped["match_date"].min(),
        "last_match_date": grouped["match_date"].max(),
        "n_matches": grouped["match_id"].nunique(),
    }
    if "dob_raw" in df.columns:
        agg["dob_raw"] = grouped["dob_raw"].apply(_last_non_null)

    out = pd.DataFrame(agg).reset_index()
    if "dob_raw" not in out.columns:
        out["dob_raw"] = None
    return out


def load_reep(csv_path: str) -> pd.DataFrame:
    """Load the reep CC0 register, deduped to one row per Opta player_id.

    Only the columns this table needs are read (the full file is ~450k rows
    covering every sport reep tracks). Malformed rows are skipped, never
    crash the build.
    """
    usecols = ["key_opta", "date_of_birth", "nationality", "height_cm"]
    reep = pd.read_csv(csv_path, usecols=usecols, low_memory=False)
    reep = reep.dropna(subset=["key_opta"])
    # Defensive: dedupe should be a no-op (reep's key_opta is already unique
    # as of the 2026-07-21 snapshot -- 50,498 rows, 50,498 unique keys), but
    # a future reep release adding a duplicate must not silently double-join.
    reep = reep.drop_duplicates(subset=["key_opta"], keep="last")
    return reep.rename(columns={"key_opta": "player_id"})


def build_profiles(player_stats: pd.DataFrame, reep: pd.DataFrame | None) -> pd.DataFrame:
    """Merge deduped opta player stats with the reep register into the final
    opta_players.parquet shape.

    Field priority: Opta-native wins when present (currently never, for dob
    -- see module docstring), reep fills every gap. nationality/height_cm
    have no Opta-native source at all, so they are reep-only.
    """
    profiles = dedupe_player_stats(player_stats)

    profiles["dob"] = profiles["dob_raw"].apply(_safe_parse_date)
    profiles["dob_source"] = profiles["dob"].apply(lambda d: "opta" if d is not None else None)
    profiles["nationality"] = None
    profiles["nationality_source"] = None
    profiles["height_cm"] = None
    profiles["height_source"] = None

    if reep is not None and not reep.empty:
        reep_by_id = reep.set_index("player_id")

        reep_dob = profiles["player_id"].map(reep_by_id["date_of_birth"]).apply(_safe_parse_date)
        use_reep_dob = profiles["dob"].isna() & reep_dob.notna()
        profiles.loc[use_reep_dob, "dob"] = reep_dob[use_reep_dob]
        profiles.loc[use_reep_dob, "dob_source"] = "reep"

        reep_nat = profiles["player_id"].map(reep_by_id["nationality"])
        profiles["nationality"] = reep_nat
        profiles.loc[reep_nat.notna(), "nationality_source"] = "reep"

        reep_height = profiles["player_id"].map(reep_by_id["height_cm"]).apply(_safe_float_or_none)
        profiles["height_cm"] = reep_height
        profiles.loc[reep_height.notna(), "height_source"] = "reep"

    profiles["built_at"] = datetime.now(timezone.utc).isoformat()

    return profiles[[
        "player_id", "player_name", "first_name", "last_name",
        "dob", "dob_source", "nationality", "nationality_source",
        "height_cm", "height_source",
        "n_matches", "first_match_date", "last_match_date", "built_at",
    ]]


def _download_reep(dest_path: str, url: str = REEP_URL) -> bool:
    """Best-effort download of the reep register. Returns False (never
    raises) on any network failure so the caller can proceed opta-only."""
    try:
        # timeout is load-bearing: this runs inside the daily scrape BEFORE the
        # upload step, and a stalled connection with no timeout would eat the
        # job's multi-hour budget (continue-on-error only catches exits, not
        # hangs) and block the next cron via the concurrency lock.
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        with open(dest_path, "wb") as f:
            f.write(resp.content)
        return True
    except Exception as e:  # noqa: BLE001 - genuinely want to swallow all network errors here
        print(f"  WARNING: reep register download failed ({e}) — building opta-only profiles")
        return False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--player-stats", default="data/opta/opta_player_stats.parquet")
    parser.add_argument("--out", default="data/opta/opta_players.parquet")
    parser.add_argument("--reep-csv", default=None, help="Local reep people.csv path (skips download)")
    parser.add_argument("--reep-url", default=REEP_URL)
    parser.add_argument("--no-reep", action="store_true", help="Skip the reep register entirely")
    args = parser.parse_args()

    if not os.path.exists(args.player_stats):
        sys.exit(f"ABORT: {args.player_stats} not found — run consolidate_opta.py first.")

    available_cols = {f.name for f in pq.read_schema(args.player_stats)}
    want_cols = ["match_id", "match_date", "player_id", "player_name",
                 "first_name", "last_name"]
    if "dob_raw" in available_cols:
        want_cols.append("dob_raw")
    cols = [c for c in want_cols if c in available_cols]
    missing_required = {"match_id", "match_date", "player_id", "player_name"} - set(cols)
    if missing_required:
        sys.exit(f"ABORT: {args.player_stats} missing required columns: {sorted(missing_required)}")

    player_stats = pd.read_parquet(args.player_stats, columns=cols)
    print(f"opta_player_stats: {len(player_stats):,} rows, "
          f"{player_stats['player_id'].nunique():,} unique player_ids")

    reep = None
    reep_tmp = None
    if not args.no_reep:
        reep_csv_path = args.reep_csv
        if reep_csv_path is None:
            reep_tmp = tempfile.NamedTemporaryFile(prefix="reep_people_", suffix=".csv", delete=False)
            reep_tmp.close()
            if _download_reep(reep_tmp.name, args.reep_url):
                reep_csv_path = reep_tmp.name
            else:
                reep_csv_path = None
        if reep_csv_path is not None:
            try:
                reep = load_reep(reep_csv_path)
                print(f"reep register: {len(reep):,} unique key_opta rows")
            except Exception as e:  # noqa: BLE001
                print(f"  WARNING: failed to parse reep register ({e}) — building opta-only profiles")
                reep = None

    profiles = build_profiles(player_stats, reep)

    n = len(profiles)
    dob_pct = 100 * profiles["dob"].notna().mean() if n else 0.0
    nat_pct = 100 * profiles["nationality"].notna().mean() if n else 0.0
    print(f"opta_players: {n:,} rows | dob coverage {dob_pct:.1f}% | "
          f"nationality coverage {nat_pct:.1f}%")

    tmp = f"{args.out}.tmp"
    profiles.to_parquet(tmp, index=False)
    os.replace(tmp, args.out)
    print(f"written: {args.out}")

    if reep_tmp is not None:
        try:
            os.unlink(reep_tmp.name)
        except OSError:
            pass


if __name__ == "__main__":
    main()
