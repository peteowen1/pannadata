"""panna#431: goals/assists/own goals filled from liveData.goal when the
per-player stat block omits them (most pre-2015-16 matches send none)."""
import math

from opta_scraper import OptaScraper, goal_event_counts


def _match(stat_p1=None):
    return {
        "matchInfo": {"id": "m1", "date": "2013-08-17Z", "description": "A v B",
                      "contestant": [{"id": "t1", "name": "A", "position": "home"},
                                     {"id": "t2", "name": "B", "position": "away"}]},
        "liveData": {
            "matchDetails": {"scores": {"ft": {"home": 3, "away": 1}}},
            "goal": [
                {"contestantId": "t1", "type": "G", "scorerId": "p1", "assistPlayerId": "p2"},
                {"contestantId": "t1", "type": "PG", "scorerId": "p1"},
                {"contestantId": "t1", "type": "OG", "scorerId": "p3"},
                {"contestantId": "t2", "type": "G", "scorerId": "p3", "assistPlayerId": "p4"},
            ],
            "lineUp": [
                {"contestantId": "t1", "player": [
                    {"playerId": "p1", "matchName": "One", "stat": stat_p1 or []},
                    {"playerId": "p2", "matchName": "Two", "stat": []},
                ]},
                {"contestantId": "t2", "player": [
                    {"playerId": "p3", "matchName": "Three", "stat": []},
                    {"playerId": "p4", "matchName": "Four", "stat": []},
                    {"playerId": "p5", "matchName": "Five", "stat": []},
                ]},
            ],
        },
    }


def test_goal_event_counts():
    c = goal_event_counts(_match()["liveData"])
    assert c["goals"] == {"p1": 2, "p3": 1}  # G + PG count, OG does not
    assert c["ownGoals"] == {"p3": 1}
    assert c["goalAssist"] == {"p2": 1, "p4": 1}


def test_empty_stat_block_filled_from_goal_list():
    df = OptaScraper().extract_all_player_stats(_match()).set_index("player_id")
    assert df.loc["p1", "goals"] == 2
    assert df.loc["p2", "goalAssist"] == 1
    assert df.loc["p3", "goals"] == 1 and df.loc["p3", "ownGoals"] == 1
    # a player with no goal involvement stays missing, as Opta omits zeros
    assert math.isnan(df.loc["p5", "goals"])


def test_stat_block_value_is_never_overridden():
    df = OptaScraper().extract_all_player_stats(
        _match(stat_p1=[{"type": "goals", "value": "5"}])).set_index("player_id")
    assert df.loc["p1", "goals"] == 5


def test_no_goal_list_is_harmless():
    m = _match()
    del m["liveData"]["goal"]
    df = OptaScraper().extract_all_player_stats(m)
    assert "goals" not in df.columns
