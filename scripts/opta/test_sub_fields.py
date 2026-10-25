"""Tests for the fields captured under issue #192: subPosition, subReason,
captain/name variants, team-level lineup blocks, and the possession feed.

The possession payload shape is a guess (no saved payload existed when this was
written); these tests pin the defensive behaviour, not the real feed.
"""
import math

import pandas as pd

from opta_scraper import OptaScraper
from consolidate_opta import consolidate_opta


def _match_data():
    return {
        "matchInfo": {
            "id": "m1", "date": "2025-12-21T16:30:00Z", "description": "A v B",
            "contestant": [{"id": "t1", "name": "A", "position": "home"}],
        },
        "liveData": {
            "matchDetails": {"scores": {"ft": {"home": 1, "away": 0}}},
            "substitute": [{
                "contestantId": "t1", "playerOnId": "p2", "playerOnName": "Two",
                "playerOffId": "p1", "playerOffName": "One",
                "timeMin": 60, "subReason": "Injury",
            }],
            "lineUp": [{
                "contestantId": "t1", "formationUsed": "4-4-2",
                "averageAge": "26.4",
                "kit": {"id": "k1", "colour1": "#fff"},
                "teamOfficial": {"id": "c1", "type": "manager"},
                "player": [
                    {"playerId": "p1", "matchName": "One", "firstName": "A", "lastName": "One",
                     "shirtNumber": 1, "position": "Goalkeeper", "formationPlace": "1",
                     "captain": "yes", "knownName": "Oney",
                     "shortFirstName": "A.", "shortLastName": "One",
                     "stat": [{"type": "minsPlayed", "value": "60"}]},
                    {"playerId": "p2", "matchName": "Two", "shirtNumber": 12,
                     "position": "Substitute", "subPosition": "Goalkeeper",
                     "stat": [{"type": "minsPlayed", "value": "30"}]},
                ],
            }],
        },
    }


def test_lineups_keep_new_fields():
    lu = {l.player_id: l for l in OptaScraper().extract_lineups(_match_data())}
    assert lu["p2"].sub_position == "Goalkeeper"
    assert lu["p1"].sub_position == ""
    assert lu["p1"].captain is True and lu["p2"].captain is False
    assert lu["p1"].known_name == "Oney"
    assert (lu["p1"].short_first_name, lu["p1"].short_last_name) == ("A.", "One")
    assert lu["p1"].team_average_age == 26.4
    assert '"colour1":"#fff"' in lu["p1"].team_kit_json
    assert '"manager"' in lu["p2"].team_official_json


def test_lineups_missing_new_fields_default():
    data = _match_data()
    for k in ("averageAge", "kit", "teamOfficial"):
        del data["liveData"]["lineUp"][0][k]
    lu = OptaScraper().extract_lineups(data)[0]
    assert math.isnan(lu.team_average_age)
    assert lu.team_kit_json == "" and lu.team_official_json == ""


def test_player_stats_keep_new_fields():
    df = OptaScraper().extract_all_player_stats(_match_data()).set_index("player_id")
    assert df.loc["p2", "sub_position"] == "Goalkeeper"
    assert bool(df.loc["p1", "captain"]) is True
    assert df.loc["p1", "known_name"] == "Oney"


def test_substitution_event_keeps_sub_reason():
    events = OptaScraper().extract_match_events(_match_data())
    sub = [e for e in events if e.event_type == "substitution"][0]
    assert sub.sub_reason == "Injury"


def test_substitution_without_reason_is_empty_string():
    data = _match_data()
    del data["liveData"]["substitute"][0]["subReason"]
    sub = OptaScraper().extract_match_events(data)[0]
    assert sub.sub_reason == ""


def test_possession_list_wave():
    payload = {"matchInfo": {"id": "m1"}, "liveData": {"possessionWave": [
        {"interval": "5", "startMinute": 0, "endMinute": 5, "home": 55.5, "away": 44.5},
        {"interval": "5", "startMinute": 5, "endMinute": 10, "home": 40, "away": 60},
    ]}}
    rows = OptaScraper().extract_possession(payload)
    assert [r["wave_index"] for r in rows] == [0, 1]
    assert rows[0]["home_possession"] == 55.5 and rows[1]["away_possession"] == 60
    assert rows[0]["match_id"] == "m1" and rows[0]["end_minute"] == 5
    assert '"home":55.5' in rows[0]["raw_json"]


def test_possession_dict_wave_keyed_by_interval():
    payload = {"possessionWave": {"5min": [{"home": 50, "away": 50}],
                                  "15min": [{"home": 51, "away": 49}]}}
    rows = OptaScraper().extract_possession(payload, "m9")
    assert [(r["interval"], r["wave_index"]) for r in rows] == [("5min", 0), ("15min", 1)]


def test_possession_without_wave_gives_marker_row():
    rows = OptaScraper().extract_possession({"matchInfo": {"id": "m1"}})
    assert len(rows) == 1 and rows[0]["wave_index"] == -1
    assert OptaScraper().extract_possession(None) == []


def test_consolidate_possession_replaces_rescraped_match(tmp_path):
    opta = tmp_path / "opta"
    (opta / "possession" / "EPL").mkdir(parents=True)
    # m1 had 3 waves; the re-scrape returns 2. Ten untouched matches keep the
    # row-count guard (>10% drop refuses to write) out of the way.
    old = pd.DataFrame({
        "match_id": ["m1"] * 3 + [f"x{i}" for i in range(10)],
        "wave_index": [0, 1, 2] + [0] * 10,
        "home_possession": [1.0, 2.0, 3.0] + [5.0] * 10})
    old["competition"], old["season"] = "EPL", "2025-2026"
    old.to_parquet(opta / "opta_possession.parquet", index=False)
    new = pd.DataFrame({"match_id": ["m1", "m1"], "wave_index": [0, 1],
                        "home_possession": [9.0, 8.0]})
    new.to_parquet(opta / "possession" / "EPL" / "2025-2026.parquet", index=False)
    assert consolidate_opta(str(opta), str(opta)) == 0
    out = pd.read_parquet(opta / "opta_possession.parquet")
    m1 = out[out.match_id == "m1"].sort_values("wave_index")
    assert list(zip(m1.wave_index, m1.home_possession)) == [(0, 9.0), (1, 8.0)]
    assert len(out) == 12
