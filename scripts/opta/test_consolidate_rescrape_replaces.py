"""pannadata#245: a re-scraped match replaces its stored rows in every table.

Opta revises a match after the first scrape. A shot can be reclassified (its
event keeps its id but is no longer a shot), so the new shot_events for that
match have one row fewer. The keyed dedup only overwrites rows that come back,
so before this fix the withdrawn row stayed in opta_shot_events.parquet: the
22 Nov 2026 Portugal re-scrape left 6 such rows across 4 matches.
"""
import pandas as pd
import pytest

from consolidate_opta import consolidate_opta


# consolidate_opta() refuses to write a table that loses more than 10% of its
# rows, so the stored file needs enough other matches around the one revised.
PAD = 40


def _write(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def _run(tmp_path, monkeypatch, table, stored, rescraped):
    """Stored consolidated file + one re-scraped per-season file -> output rows."""
    src = tmp_path / "opta"
    out = tmp_path / "out"
    out.mkdir()
    _write(out / f"opta_{table}.parquet", stored)
    _write(src / table / "Primeira_Liga" / "2026-2027.parquet", rescraped)
    # The consolidated file was written first, so it is older than the source;
    # force anyway so the test never depends on file-time resolution.
    monkeypatch.setenv("OPTA_CONSOLIDATE_FORCE", "1")
    assert consolidate_opta(str(src), str(out)) == 0
    return pd.read_parquet(out / f"opta_{table}.parquet")


def _shot(match_id, event_id, second, big_chance=False):
    return {"match_id": match_id, "event_id": event_id, "minute": 21, "second": second,
            "type_id": 15, "big_chance": big_chance,
            "competition": "Primeira_Liga", "season": "2026-2027"}


def test_withdrawn_shot_does_not_survive_a_rescrape(tmp_path, monkeypatch):
    stored = [_shot("m1", 101, 23), _shot("m1", 102, 58), _shot("m1", 103, 40),
              _shot("m2", 201, 10)] + [_shot(f"pad{i}", 900 + i, 5) for i in range(PAD)]
    # m1 re-scraped: 102 is no longer a shot, 101 moved 3 seconds, 103 gained the tag.
    rescraped = [_shot("m1", 101, 26), _shot("m1", 103, 40, big_chance=True)]
    for r in rescraped:
        del r["competition"], r["season"]   # per-season files carry neither

    got = _run(tmp_path, monkeypatch, "shot_events", stored, rescraped)

    m1 = got[got.match_id == "m1"].sort_values("event_id")
    assert list(m1.event_id) == [101, 103]            # 102 is gone
    assert list(m1.second) == [26, 40]                # the revised time is stored
    assert list(m1.big_chance) == [False, True]       # and the revised tag
    # A match that was not re-scraped is untouched.
    assert list(got[got.match_id == "m2"].event_id) == [201]
    assert len(got) == 3 + PAD


@pytest.mark.parametrize("table", ["player_stats", "lineups", "shots"])
def test_player_tables_drop_a_player_the_rescrape_no_longer_lists(tmp_path, monkeypatch, table):
    row = lambda m, p, v: {"match_id": m, "player_id": p, "value": v,
                           "competition": "Primeira_Liga", "season": "2026-2027"}
    stored = [row("m1", "a", 1), row("m1", "b", 1), row("m2", "a", 1)]
    stored += [row(f"pad{i}", "a", 1) for i in range(PAD)]
    rescraped = [{"match_id": "m1", "player_id": "a", "value": 2}]

    got = _run(tmp_path, monkeypatch, table, stored, rescraped)

    m1 = got[got.match_id == "m1"]
    assert list(m1.player_id) == ["a"] and list(m1.value) == [2]
    assert list(got[got.match_id == "m2"].player_id) == ["a"]
    assert len(got) == 2 + PAD


def test_fixtures_are_merged_by_match_and_nothing_else_is_dropped(tmp_path, monkeypatch):
    fx = lambda m, d: {"match_id": m, "match_date": d,
                       "competition": "Primeira_Liga", "season": "2026-2027"}
    stored = [fx("m1", "2026-11-07Z"), fx("m2", "2026-11-08Z")]
    rescraped = [{"match_id": "m1", "match_date": "2026-11-09Z"}]   # m1 rescheduled

    got = _run(tmp_path, monkeypatch, "fixtures", stored, rescraped).sort_values("match_id")

    assert list(got.match_id) == ["m1", "m2"]
    assert list(got.match_date) == ["2026-11-09Z", "2026-11-08Z"]
