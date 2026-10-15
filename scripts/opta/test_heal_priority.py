"""Tests for the blog-first ordering in heal_unavailable_events().

2026-10-15: the 58 CAF_CL 2026-2027 matches sat at heal-queue positions
249-306 of 2,076, behind alphabetically earlier non-blog cups, while panna
step 10b aborted the whole season over them. Blog competitions now go first.

Imported by bare name (`from scrape_opta import ...`) — pytest must run with
scripts/opta as rootdir (see pytest.ini).
"""
import pandas as pd

from scrape_opta import HEAL_PRIORITY_COMPETITIONS, heal_unavailable_events


def _write_manifest(opta_dir, rows):
    path = opta_dir / "opta-manifest.parquet"
    pd.DataFrame(rows, columns=["match_id", "competition", "season",
                                "event_unavailable"]).to_parquet(path, index=False)
    return path


def test_blog_competitions_fill_the_cap_first(opta_dir, capsys):
    season = "2026-2027"
    rows = [(f"b{i:02d}", "Belgian_Cup", season, True) for i in range(5)]
    rows += [(f"c{i:02d}", "CAF_CL", season, True) for i in range(3)]
    path = _write_manifest(opta_dir, rows)

    summary = heal_unavailable_events(None, opta_dir, path, season,
                                      cap=3, dry_run=True)

    assert "CAF_CL" in HEAL_PRIORITY_COMPETITIONS
    assert "Belgian_Cup" not in HEAL_PRIORITY_COMPETITIONS
    assert summary["attempted"] == 3
    out = capsys.readouterr().out
    assert "3 from blog competitions" in out
    assert "CAF_CL: 3 match(es)" in out
    assert "Belgian_Cup" not in out


def test_non_blog_still_healed_when_room_left(opta_dir, capsys):
    season = "2026-2027"
    rows = [("b01", "Belgian_Cup", season, True),
            ("c01", "CAF_CL", season, True),
            ("old", "CAF_CL", "2025-2026", True),   # other season: excluded
            ("ok1", "EPL", season, False)]          # not unavailable: excluded
    path = _write_manifest(opta_dir, rows)

    summary = heal_unavailable_events(None, opta_dir, path, season,
                                      cap=10, dry_run=True)

    assert summary["attempted"] == 2
    out = capsys.readouterr().out
    assert "Belgian_Cup: 1 match(es)" in out
    assert "CAF_CL: 1 match(es)" in out
