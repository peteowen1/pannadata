"""Tests for build_player_profiles.py (pannadata#112).

Covers the defensive date coercion (present/missing/malformed DOB, mirroring
the _to_int_or_0-style guard the repo adopted after the Euro-1980 incident)
and the "latest non-null wins" dedup rule for pannadata#86.
"""
import math
from datetime import date

import pandas as pd
import pytest

from build_player_profiles import (
    _last_non_null,
    _safe_float_or_none,
    _safe_parse_date,
    build_profiles,
    dedupe_player_stats,
    load_reep,
)


# ---------------------------------------------------------------------------
# _safe_parse_date
# ---------------------------------------------------------------------------

def test_safe_parse_date_valid():
    assert _safe_parse_date("1987-06-24") == date(1987, 6, 24)


def test_safe_parse_date_trailing_z():
    # Opta date fields elsewhere in the codebase use a trailing "Z"
    # (e.g. match_date "2019-08-16Z") -- the parser must strip it.
    assert _safe_parse_date("1987-06-24Z") == date(1987, 6, 24)


def test_safe_parse_date_missing():
    assert _safe_parse_date(None) is None
    assert _safe_parse_date("") is None
    assert _safe_parse_date("   ") is None
    assert _safe_parse_date(float("nan")) is None


def test_safe_parse_date_malformed_never_raises():
    # Garbage strings, wrong types -- must degrade to None, never crash the
    # build (the Euro-1980 lesson: a corrupt field must not take the whole
    # pipeline down or silently produce a wrong date).
    assert _safe_parse_date("Unknown") is None
    assert _safe_parse_date("not-a-date") is None
    assert _safe_parse_date(12345) is None
    assert _safe_parse_date({"weird": "dict"}) is None
    assert _safe_parse_date([1, 2, 3]) is None


# ---------------------------------------------------------------------------
# _safe_float_or_none
# ---------------------------------------------------------------------------

def test_safe_float_or_none():
    assert _safe_float_or_none("182") == 182.0
    assert _safe_float_or_none(182.5) == 182.5
    assert _safe_float_or_none(None) is None
    assert _safe_float_or_none("") is None
    assert _safe_float_or_none("tall") is None
    assert _safe_float_or_none(float("nan")) is None


# ---------------------------------------------------------------------------
# _last_non_null
# ---------------------------------------------------------------------------

def test_last_non_null():
    assert _last_non_null(pd.Series([None, "A", None, "B"])) == "B"
    assert _last_non_null(pd.Series([None, None])) is None
    assert _last_non_null(pd.Series([], dtype=object)) is None


# ---------------------------------------------------------------------------
# dedupe_player_stats
# ---------------------------------------------------------------------------

def _stats_df(rows):
    cols = ["match_id", "match_date", "player_id", "player_name", "first_name", "last_name"]
    return pd.DataFrame(rows, columns=cols)


def test_dedupe_latest_non_null_wins():
    # p1 has a blank last_name on its first (earlier) match but a real one
    # on its second (later) match -- the later non-null value must win, not
    # simply the last row in file order or the first non-null one.
    df = _stats_df([
        ("m1", "2020-01-01", "p1", "L. Messi", "Lionel", None),
        ("m2", "2020-06-01", "p1", "L. Messi", "Lionel", "Messi"),
        ("m1", "2020-01-01", "p2", "K. Mbappe", "Kylian", "Mbappe"),
    ])
    out = dedupe_player_stats(df).set_index("player_id")
    assert out.loc["p1", "last_name"] == "Messi"
    assert out.loc["p1", "n_matches"] == 2
    assert out.loc["p1", "first_match_date"] == "2020-01-01"
    assert out.loc["p1", "last_match_date"] == "2020-06-01"
    assert out.loc["p2", "n_matches"] == 1


def test_dedupe_missing_dob_raw_column_is_all_none():
    df = _stats_df([("m1", "2020-01-01", "p1", "X", "X", "X")])
    out = dedupe_player_stats(df)
    assert "dob_raw" in out.columns
    assert out["dob_raw"].iloc[0] is None


def test_dedupe_empty_input():
    df = _stats_df([])
    out = dedupe_player_stats(df)
    assert len(out) == 0
    assert "player_id" in out.columns


def test_dedupe_dob_raw_latest_non_null_wins():
    cols = ["match_id", "match_date", "player_id", "player_name", "first_name", "last_name", "dob_raw"]
    df = pd.DataFrame([
        ("m1", "2020-01-01", "p1", "X", "X", "X", None),
        ("m2", "2020-06-01", "p1", "X", "X", "X", "1998-04-01"),
    ], columns=cols)
    out = dedupe_player_stats(df).set_index("player_id")
    assert out.loc["p1", "dob_raw"] == "1998-04-01"


# ---------------------------------------------------------------------------
# build_profiles (opta-native vs reep priority, malformed reep rows)
# ---------------------------------------------------------------------------

def test_build_profiles_opta_native_dob_wins_over_reep():
    cols = ["match_id", "match_date", "player_id", "player_name", "first_name", "last_name", "dob_raw"]
    stats = pd.DataFrame([
        ("m1", "2020-01-01", "p1", "Player One", "Player", "One", "1990-01-01"),
    ], columns=cols)
    reep = pd.DataFrame([
        {"player_id": "p1", "date_of_birth": "1999-12-31", "nationality": "England", "height_cm": "180"},
    ])
    out = build_profiles(stats, reep).set_index("player_id")
    assert out.loc["p1", "dob"] == date(1990, 1, 1)
    assert out.loc["p1", "dob_source"] == "opta"
    assert out.loc["p1", "nationality"] == "England"
    assert out.loc["p1", "nationality_source"] == "reep"
    assert out.loc["p1", "height_cm"] == 180.0


def test_build_profiles_reep_fills_gap_when_opta_missing():
    cols = ["match_id", "match_date", "player_id", "player_name", "first_name", "last_name"]
    stats = pd.DataFrame([
        ("m1", "2020-01-01", "p2", "Player Two", "Player", "Two"),
    ], columns=cols)
    reep = pd.DataFrame([
        {"player_id": "p2", "date_of_birth": "1995-05-05", "nationality": "France", "height_cm": None},
    ])
    out = build_profiles(stats, reep).set_index("player_id")
    assert out.loc["p2", "dob"] == date(1995, 5, 5)
    assert out.loc["p2", "dob_source"] == "reep"
    assert out.loc["p2", "height_cm"] is None
    assert out.loc["p2", "height_source"] is None


def test_build_profiles_malformed_reep_date_becomes_na_not_crash():
    cols = ["match_id", "match_date", "player_id", "player_name", "first_name", "last_name"]
    stats = pd.DataFrame([
        ("m1", "2020-01-01", "p3", "Player Three", "Player", "Three"),
    ], columns=cols)
    reep = pd.DataFrame([
        {"player_id": "p3", "date_of_birth": "not-a-real-date", "nationality": "Brazil", "height_cm": "tall"},
    ])
    out = build_profiles(stats, reep).set_index("player_id")
    assert out.loc["p3", "dob"] is None
    assert out.loc["p3", "dob_source"] is None
    assert out.loc["p3", "nationality"] == "Brazil"
    assert out.loc["p3", "height_cm"] is None


def test_build_profiles_no_reep_leaves_reep_fields_null():
    cols = ["match_id", "match_date", "player_id", "player_name", "first_name", "last_name"]
    stats = pd.DataFrame([
        ("m1", "2020-01-01", "p4", "Player Four", "Player", "Four"),
    ], columns=cols)
    out = build_profiles(stats, None).set_index("player_id")
    assert out.loc["p4", "dob"] is None
    assert out.loc["p4", "nationality"] is None
    assert out.loc["p4", "height_cm"] is None


def test_build_profiles_unmatched_player_id_stays_null():
    # A player_id absent from reep must not crash the .map() lookup or
    # produce a spurious value.
    cols = ["match_id", "match_date", "player_id", "player_name", "first_name", "last_name"]
    stats = pd.DataFrame([
        ("m1", "2020-01-01", "p5", "Unmatched Player", "Unmatched", "Player"),
    ], columns=cols)
    reep = pd.DataFrame([
        {"player_id": "someone_else", "date_of_birth": "1990-01-01", "nationality": "Spain", "height_cm": "175"},
    ])
    out = build_profiles(stats, reep).set_index("player_id")
    assert out.loc["p5", "dob"] is None
    assert pd.isna(out.loc["p5", "nationality"])


# ---------------------------------------------------------------------------
# load_reep
# ---------------------------------------------------------------------------

def test_load_reep_dedupes_and_drops_blank_keys(tmp_path):
    csv_path = tmp_path / "people.csv"
    csv_path.write_text(
        "key_opta,date_of_birth,nationality,height_cm,unused\n"
        "p1,1990-01-01,England,180,x\n"
        "p1,1990-01-01,England,181,y\n"  # duplicate key_opta -- keep='last' -> 181
        ",1990-01-01,France,175,z\n"      # blank key_opta -- dropped
    )
    out = load_reep(str(csv_path))
    assert list(out["player_id"]) == ["p1"]
    assert out.iloc[0]["height_cm"] == 181
