"""Tests for the fields captured under issue #192: subPosition, subReason,
captain/name variants and team-level lineup blocks.
"""
import math

import pandas as pd

from opta_scraper import OptaScraper


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
