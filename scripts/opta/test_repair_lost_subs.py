"""R196: rebuild substitutions the old re-save key collapsed."""
import pandas as pd

from repair_lost_subs import pair_subs, repair, lineup_agreement

EV_COLS = ["match_id", "event_type", "minute", "second", "team_id", "player_id", "player_name",
           "player_on_id", "player_on_name", "player_off_id", "player_off_name",
           "assist_player_id", "assist_player_name", "competition", "season"]


def raw_row(eid, t, pid, team, mn, sc, match="m1", period=2):
    return dict(match_id=match, event_id=eid, type_id=t, player_id=pid, player_name=pid.upper(),
                team_id=team, minute=mn, second=sc, period_id=period, competition="EPL", season="2016-2017")


def ev_row(on, off, mn, sc, team="h", match="m1"):
    return dict(match_id=match, event_type="substitution", minute=float(mn), second=float(sc),
                team_id=team, player_id="", player_name="", player_on_id=on, player_on_name=on.upper(),
                player_off_id=off, player_off_name=off.upper(), assist_player_id="",
                assist_player_name="", competition="EPL", season="2016-2017")


def goal():
    return dict(match_id="m1", event_type="goal", minute=10.0, second=5.0, team_id="h", player_id="g",
                player_name="G", player_on_id="", player_on_name="", player_off_id="",
                player_off_name="", assist_player_id="", assist_player_name="",
                competition="EPL", season="2016-2017")


def test_lost_sibling_sub_is_added_on_the_stored_clock():
    # Old feed: raw 52:04 is stored as 53:00. Two home subs at 52:04 and one away
    # sub at 52:30; the old key kept only the first.
    raw = pd.DataFrame([raw_row(9, 18, "a_off", "h", 52, 4), raw_row(3, 19, "a_on", "h", 52, 4),
                        raw_row(7, 18, "b_off", "h", 52, 4), raw_row(1, 19, "b_on", "h", 52, 4),
                        raw_row(5, 18, "c_off", "a", 52, 30), raw_row(2, 19, "c_on", "a", 52, 30)])
    events = pd.DataFrame([goal(), ev_row("a_on", "a_off", 53, 0)])[EV_COLS]
    out, st = repair(events, pair_subs(raw))
    subs = out[out.event_type == "substitution"].set_index("player_on_id")
    assert st["added"] == 2
    assert set(subs.index) == {"a_on", "b_on", "c_on"}
    assert (subs.loc["b_on", "minute"], subs.loc["b_on", "second"]) == (53, 0)
    assert (subs.loc["c_on", "minute"], subs.loc["c_on", "second"]) == (53, 0)
    assert subs.loc["c_on", "team_id"] == "a"
    assert (out[out.event_type == "goal"].values == events[events.event_type == "goal"].values).all()


def test_placeholder_dropped_only_when_a_timed_twin_exists():
    raw = pd.DataFrame([raw_row(1, 18, "x_off", "h", 60, 0), raw_row(2, 19, "x_on", "h", 60, 0)])
    events = pd.DataFrame([ev_row("x_on", "x_off", 0, 0), ev_row("x_on", "x_off", 61, 0),
                           ev_row("y_on", "y_off", 0, 0)])[EV_COLS]
    out, st = repair(events, pair_subs(raw))
    assert st["placeholders_dropped"] == 1
    assert st["added"] == 0
    kept = out.set_index("player_on_id")
    assert kept.loc["x_on", "minute"] == 61          # the timed copy stays
    assert kept.loc["y_on", "minute"] == 0           # a lone 0:00 row is left alone


def test_unbalanced_group_is_skipped_and_known_on_player_not_readded():
    raw = pd.DataFrame([raw_row(1, 18, "p_off", "h", 70, 0), raw_row(2, 19, "p_on", "h", 70, 0),
                        raw_row(3, 19, "q_on", "h", 70, 0),             # no matching off
                        raw_row(4, 18, "r_off", "h", 80, 0), raw_row(5, 19, "p_on", "h", 80, 0)])
    events = pd.DataFrame([ev_row("p_on", "zz", 70, 0)])[EV_COLS]
    pairs = pair_subs(raw)
    assert "q_on" not in set(pairs.player_on_id)
    out, st = repair(events, pairs)
    assert st["added"] == 0                          # p_on already stored as coming on
    assert len(out) == 1


def test_ambiguous_flag_and_dtypes_kept():
    raw = pd.DataFrame([raw_row(1, 18, "a", "h", 30, 0), raw_row(2, 18, "b", "h", 30, 0),
                        raw_row(3, 19, "c", "h", 30, 0), raw_row(4, 19, "d", "h", 30, 0)])
    pairs = pair_subs(raw)
    assert pairs.ambiguous.all() and len(pairs) == 2
    events = pd.DataFrame([goal()])[EV_COLS]
    out, st = repair(events, pairs)
    assert st["added"] == 2 and st["added_ambiguous"] == 2
    assert list(out.dtypes) == list(events.dtypes)


def test_lineup_agreement_counts_sub_rows_per_match():
    events = pd.DataFrame([ev_row("a", "b", 50, 0), ev_row("c", "d", 60, 0, match="m2")])[EV_COLS]
    lu = pd.DataFrame({"match_id": ["m1", "m2", "m2"], "is_starter": [False, False, False],
                       "minutes_played": [40, 30, 20]})
    r = lineup_agreement(events, lu, {"m1", "m2"})
    assert r == {"matches": 2, "equal": 0.5, "over": 0, "under": 1}


def test_placeholder_only_sub_is_replaced_not_doubled():
    # Stored only as a 0:00 placeholder; the raw events have its real time.
    raw = pd.DataFrame([raw_row(1, 18, "x_off", "h", 60, 0), raw_row(2, 19, "x_on", "h", 60, 0)])
    events = pd.DataFrame([goal(), ev_row("x_on", "x_off", 0, 0)])[EV_COLS]
    out, st = repair(events, pair_subs(raw))
    subs = out[out.event_type == "substitution"]
    assert len(subs) == 1 and subs.minute.iloc[0] == 60
    assert st["added"] == 1 and st["placeholders_dropped"] == 1


def test_blank_player_id_group_is_skipped(tmp_path):
    import pyarrow as pa, pyarrow.parquet as pq
    from repair_lost_subs import read_raw_subs
    raw = pd.DataFrame([raw_row(1, 18, "a_off", "h", 60, 0), raw_row(2, 19, "", "h", 60, 0),
                        raw_row(3, 18, "b_off", "h", 70, 0), raw_row(4, 19, "b_on", "h", 70, 0)])
    pq.write_table(pa.Table.from_pandas(raw, preserve_index=False), tmp_path / "events_EPL.parquet")
    pairs = pair_subs(read_raw_subs(tmp_path))
    assert list(pairs.player_on_id) == ["b_on"]
