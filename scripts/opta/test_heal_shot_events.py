"""heal_shot_events.py against the live opta_shot_events schema (24 columns as of
2026-10-10). It crashed with KeyError ['is_own_goal', 'is_blocked', 'thin_feed']
on every scrape from at least 2026-10-04 because derive_shot_row() never set
those columns, so no stranded match was healed.
"""

import json
import sys

import pandas as pd
import pytest

import heal_shot_events as heal

LIVE_COLS = ["match_id", "event_id", "player_id", "player_name", "team_id", "minute",
             "second", "x", "y", "outcome", "is_goal", "type_id", "body_part",
             "situation", "big_chance", "competition", "season", "xg", "goalmouth_y",
             "goalmouth_z", "xgot", "is_own_goal", "is_blocked", "thin_feed"]


def _shots(extra_cols=()):
    row = {"match_id": "m_have", "event_id": 1, "player_id": "p1", "player_name": "A",
           "team_id": "t1", "minute": 10.0, "second": 0.0, "x": 90.0, "y": 50.0,
           "outcome": 1.0, "is_goal": False, "type_id": 15.0, "body_part": "RightFoot",
           "situation": "OpenPlay", "big_chance": False, "competition": "EPL",
           "season": "2025-2026", "xg": 0.1, "goalmouth_y": 50.0, "goalmouth_z": 5.0,
           "xgot": 0.2, "is_own_goal": False, "is_blocked": False, "thin_feed": False}
    for c in extra_cols:
        row[c] = 1.5
    return pd.DataFrame([row])


def _events():
    # Match "m_new" has events but no shots: 4 shot-type events, one own goal
    # (q28) and one blocked shot (q82).
    quals = [{"72": ""}, {"28": ""}, {"82": "", "15": ""}, {}]
    rows = []
    for i, (q, t) in enumerate(zip(quals, [13, 16, 15, 16])):
        rows.append({"match_id": "m_new", "event_id": 100 + i, "type_id": t,
                     "player_id": f"p{i}", "player_name": f"P{i}", "team_id": "t2",
                     "minute": 20 + i, "second": 0, "x": 88.0, "y": 40.0, "outcome": 1,
                     "qualifier_json": json.dumps(q), "competition": "EPL",
                     "season": "2025-2026"})
    # The match that already has shots, so it must not be re-added.
    rows.append(dict(rows[0], match_id="m_have", event_id=1))
    return pd.DataFrame(rows)


def _run(tmp_path, shots, monkeypatch):
    shots_path = tmp_path / "shots.parquet"
    events_path = tmp_path / "events.parquet"
    shots.to_parquet(shots_path, index=False)
    _events().to_parquet(events_path, index=False)
    monkeypatch.setattr(sys, "argv", ["heal", str(shots_path), str(events_path), str(shots_path)])
    heal.main()
    return pd.read_parquet(shots_path)


def test_heals_with_live_schema(tmp_path, monkeypatch):
    out = _run(tmp_path, _shots(), monkeypatch)
    assert list(out.columns) == LIVE_COLS
    new = out[out["match_id"] == "m_new"].set_index("event_id")
    assert len(new) == 4
    assert new["is_own_goal"].tolist() == [False, True, False, False]
    assert new["is_blocked"].tolist() == [False, False, True, False]
    assert new["thin_feed"].isna().all()  # set later by score_shots_context.R
    assert new["xg"].isna().all()
    # The existing row is untouched.
    have = out[out["match_id"] == "m_have"]
    assert len(have) == 1 and have["thin_feed"].iloc[0] == False  # noqa: E712


def test_unknown_new_column_is_filled_and_named(tmp_path, monkeypatch, capsys):
    out = _run(tmp_path, _shots(extra_cols=["future_col"]), monkeypatch)
    assert "future_col" in out.columns
    assert out.loc[out["match_id"] == "m_new", "future_col"].isna().all()
    assert "no rule for ['future_col']" in capsys.readouterr().out
