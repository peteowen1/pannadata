"""pannadata#173 / N003: untimed goal/card/sub records duplicating timed ones."""
import opta_scraper as o
from opta_scraper import OptaScraper


def _events(live):
    return OptaScraper.extract_match_events(OptaScraper.__new__(OptaScraper),
                                            {"matchInfo": {"id": "m"}, "liveData": live})


def test_untimed_goal_dropped_when_timed_twin_exists():
    live = {"goal": [
        {"scorerId": "p1", "contestantId": "t1"},
        {"scorerId": "p1", "contestantId": "t1", "timeMin": 31, "timeMinSec": "31:10"},
    ]}
    ev = [e for e in _events(live) if e.event_type == "goal"]
    assert [(e.minute, e.second) for e in ev] == [(31, 10)]


def test_genuine_untimed_goal_kept_without_twin():
    live = {"goal": [{"scorerId": "p2", "contestantId": "t1"}]}
    ev = _events(live)
    assert len(ev) == 1 and (ev[0].minute, ev[0].second) == (0, 0)


def test_real_minute_zero_goal_kept():
    live = {"goal": [{"scorerId": "p1", "contestantId": "t1", "timeMin": 0}]}
    assert len(_events(live)) == 1


def test_untimed_card_and_sub_dropped():
    live = {"card": [{"playerId": "p", "type": "YC", "contestantId": "t"},
                     {"playerId": "p", "type": "YC", "contestantId": "t", "timeMin": 40}],
            "substitute": [{"playerOnId": "a", "playerOffId": "b", "contestantId": "t"},
                           {"playerOnId": "a", "playerOffId": "b", "contestantId": "t", "timeMin": 60}]}
    assert sorted(e.minute for e in _events(live)) == [40, 60]


def test_zero_share():
    rows = [{"event_type": "goal", "minute": 0, "second": 0}] * 5 + \
           [{"event_type": "goal", "minute": 30, "second": 0}] * 25
    assert abs(o.zero_minute_goal_share(rows) - 5 / 30) < 1e-9
    assert o.zero_minute_goal_share(rows[:3]) is None


def test_cleanup_drops_only_twinned_zero_rows():
    import pandas as pd
    import clean_untimed_event_duplicates as c
    df = pd.DataFrame([
        dict(match_id="m", event_type="goal", minute=0.0, second=0.0, team_id="t", player_id="p1"),
        dict(match_id="m", event_type="goal", minute=31.0, second=0.0, team_id="t", player_id="p1"),
        dict(match_id="m", event_type="goal", minute=0.0, second=0.0, team_id="t", player_id="p2"),
        dict(match_id="m2", event_type="goal", minute=0.0, second=0.0, team_id="t", player_id="p1"),
    ])
    assert c.phantom_mask(df).tolist() == [True, False, False, False]
