"""R196: the events dedup key must keep simultaneous substitutions."""
import pandas as pd

from consolidate_opta import _get_dedup_cols


def test_events_key_keeps_double_substitution():
    ev = pd.DataFrame({
        "match_id": ["m1", "m1", "m1"],
        "event_type": ["substitution", "substitution", "substitution"],
        "minute": [64, 64, 64],
        "second": [2, 2, 2],
        "player_id": ["", "", ""],
        "player_on_id": ["a", "b", "a"],
        "player_off_id": ["x", "y", "x"],
    })
    key = _get_dedup_cols("events") + ["second"]
    out = ev.drop_duplicates(subset=key, keep="last")
    assert len(out) == 2                      # the real double change survives
    assert set(out.player_on_id) == {"a", "b"}  # the exact repeat is still removed


def test_other_keys_unchanged():
    assert _get_dedup_cols("match_events") == ["match_id", "event_id"]
    assert _get_dedup_cols("lineups") == ["match_id", "player_id"]


def test_null_and_empty_substitution_ids_dedupe_together():
    old = pd.DataFrame({"match_id": ["m1"], "event_type": ["goal"], "minute": [10], "second": [5],
                        "player_id": ["p1"], "player_on_id": [None], "player_off_id": [None]})
    new = old.assign(player_on_id="", player_off_id="")
    both = pd.concat([old, new], ignore_index=True)
    key = _get_dedup_cols("events") + ["second"]
    for c in ("player_on_id", "player_off_id", "player_id"):
        both[c] = both[c].fillna("")
    assert len(both.drop_duplicates(subset=key, keep="last")) == 1
