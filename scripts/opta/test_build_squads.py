"""Tests for build_squads.py's filtering and guards (pannadata#123).

The squads feed is the authoritative answer to "who is at this club right now",
but it is not a clean list of players: measured on the EPL 2026/2027 calendar
(2026-08-22) it returned 592 `type == "player"` alongside 20 `coach` and 22
`assistant coach`, and those 42 staff entries were exactly the 42 with a null
position. Publishing them unfiltered would put coaches into the ratings join.

These pin the filtering and the refuse-to-publish guards. No network: every
test drives fetch_competition() with a stubbed scraper.
"""
import pytest

import build_squads
from build_squads import fetch_competition, MIN_SQUAD, MIN_CLUBS


class FakeScraper:
    """Minimal stand-in: returns a canned calendar and squad payload."""

    def __init__(self, squads, calendar=True):
        self._squads = squads
        self._calendar = calendar

    def get_active_tournament_calendar(self, comp):
        if not self._calendar:
            return None
        return {"id": "tmcl123", "name": "2026/2027",
                "startDate": "2026-08-21", "endDate": "2027-05-30"}

    def get_squads(self, tmcl):
        return {"squad": self._squads}


class _DispatchScraper:
    """Routes get_active_tournament_calendar/get_squads to a per-comp
    FakeScraper, for tests exercising more than one competition against a
    single `scraper` instance (matches main()'s real usage: one OptaScraper()
    instance, fetch_competition() called once per comp)."""

    def __init__(self, scrapers_by_comp):
        self._scrapers = scrapers_by_comp
        self._last_comp = None

    def get_active_tournament_calendar(self, comp):
        self._last_comp = comp
        return self._scrapers[comp].get_active_tournament_calendar(comp)

    def get_squads(self, tmcl):
        return self._scrapers[self._last_comp].get_squads(tmcl)


def _club(name, n_players, n_coaches=0, n_inactive=0, cid="c1", start=0):
    people = [{"id": f"p{start + i}", "type": "player", "active": "yes",
               "position": "Midfielder"} for i in range(n_players)]
    people += [{"id": f"coach{start + i}", "type": "coach", "active": "yes",
                "position": None} for i in range(n_coaches)]
    people += [{"id": f"x{start + i}", "type": "player", "active": "no",
                "position": "Defender"} for i in range(n_inactive)]
    return {"contestantId": cid, "contestantClubName": name,
            "contestantName": name + " FC", "person": people}


def _full_league(n_clubs=MIN_CLUBS, size=MIN_SQUAD + 5, **kw):
    return [_club(f"Club{i}", size, cid=f"c{i}", start=i * 100, **kw)
            for i in range(n_clubs)]


def test_coaching_staff_are_excluded():
    squads = _full_league(n_clubs=MIN_CLUBS, size=MIN_SQUAD + 5, n_coaches=3)
    rows, problems = fetch_competition(FakeScraper(squads), "EPL")
    assert problems == []
    assert all(not r["player_id"].startswith("coach") for r in rows)
    assert len(rows) == MIN_CLUBS * (MIN_SQUAD + 5)


def test_inactive_players_are_excluded():
    squads = _full_league(n_coaches=0, n_inactive=4)
    rows, _ = fetch_competition(FakeScraper(squads), "EPL")
    assert all(not r["player_id"].startswith("x") for r in rows)


def test_a_thin_club_is_reported_and_dropped_not_published():
    # A club with almost nobody in it silently empties its panels on the site,
    # which is worse than not publishing -- it must surface as a problem.
    squads = _full_league()
    squads.append(_club("Thin", MIN_SQUAD - 1, cid="cThin", start=9000))
    rows, problems = fetch_competition(FakeScraper(squads), "EPL")
    assert any("Thin" in p for p in problems)
    assert all(r["team"] != "Thin" for r in rows)


def test_too_few_clubs_yields_no_rows():
    squads = _full_league(n_clubs=MIN_CLUBS - 1)
    rows, problems = fetch_competition(FakeScraper(squads), "EPL")
    assert rows == []
    assert any("club(s) returned" in p for p in problems)


def test_missing_calendar_is_a_problem_not_a_crash():
    rows, problems = fetch_competition(FakeScraper([], calendar=False), "EPL")
    assert rows == []
    assert any("no active tournament calendar" in p for p in problems)


def test_club_name_and_id_both_exported():
    # team_id matters because ratings.parquet carries only a club NAME, and
    # four of 96 names differ between the two sources. A consumer grouping a
    # club's players across both files needs the id to avoid splitting them.
    squads = _full_league(n_clubs=MIN_CLUBS)
    rows, _ = fetch_competition(FakeScraper(squads), "EPL")
    assert rows[0]["team"] == "Club0"
    assert rows[0]["team_id"] == "c0"
    assert rows[0]["league"] == "EPL"
    assert rows[0]["season"] == "2026-2027"


def test_player_without_an_id_is_reported():
    squads = _full_league()
    squads[0]["person"].append({"id": None, "type": "player", "active": "yes"})
    rows, problems = fetch_competition(FakeScraper(squads), "EPL")
    assert any("no id" in p for p in problems)
    assert all(r["player_id"] is not None for r in rows)


# --- main(): the refuse-to-write contract -----------------------------------
# The guards inside fetch_competition() are covered above, but main() is what
# the workflow actually calls (`if ! python scripts/opta/build_squads.py`), and
# it owns the two decisions that matter most: the cross-squad duplicate check,
# and whether a guard failure is allowed to write anyway. Neither was exercised
# until now, so a regression there would ship a row-multiplying or partial file
# with CI green.

def _run_main(monkeypatch, tmp_path, squads, extra_argv=()):
    out = tmp_path / "squads.parquet"
    monkeypatch.setattr(build_squads, "OptaScraper", lambda *a, **k: FakeScraper(squads))
    monkeypatch.setattr(build_squads.sys, "argv",
                        ["build_squads.py", "--comps", "EPL", "--out", str(out), *extra_argv])
    return build_squads.main(), out


def test_main_resolves_a_repeated_cross_squad_pattern_by_keeping_the_earlier_comp(monkeypatch, tmp_path):
    # A player in two squads would multiply his rows on the ratings join if
    # left unresolved. At Big-5-only scale this never happened in practice and
    # main() refused outright (a cross-league loan was the only plausible
    # cause). Widened to 49 leagues (pannadata#132), a real case surfaced
    # immediately: reserve/feeder-team structures (e.g. Auckland FC senior
    # squad vs "Auckland II" in a separate domestic competition) legitimately
    # share players. main() now auto-resolves this specific shape -- MULTIPLE
    # players sharing the exact same pair of leagues, which is what makes it
    # a structural (repeated) relationship rather than an ambiguous one-off --
    # by keeping the row from whichever competition is listed EARLIER in
    # `--comps`, dropping the rest. A single (non-repeated) duplicate is a
    # DIFFERENT case -- see test_main_refuses_a_one_off_cross_squad_duplicate
    # below -- since it could just as easily be an in-flight transfer, where
    # keeping the earlier comp would silently keep the WRONG (stale) one.
    out = tmp_path / "squads.parquet"
    senior = _full_league(n_clubs=MIN_CLUBS)
    reserve = _full_league(n_clubs=MIN_CLUBS)
    # Give the reserve league's own players distinct ids (avoid accidental
    # overlap with senior's), then re-introduce TWO intentionally shared ids
    # -- a repeated pattern, the case under test.
    for club in reserve:
        for person in club["person"]:
            person["id"] = "r_" + person["id"]
    dup1 = senior[0]["person"][0]["id"]
    dup2 = senior[0]["person"][1]["id"]
    reserve[0]["person"][0]["id"] = dup1
    reserve[0]["person"][1]["id"] = dup2

    # Each comp needs its own scraper instance (different squad payload),
    # routed through _DispatchScraper since main() creates one OptaScraper()
    # and calls fetch_competition() with it once per comp.
    scrapers = {"SeniorComp": FakeScraper(senior), "ReserveComp": FakeScraper(reserve)}
    monkeypatch.setattr(build_squads, "OptaScraper",
                        lambda *a, **k: _DispatchScraper(scrapers))
    monkeypatch.setattr(build_squads.sys, "argv",
                        ["build_squads.py", "--comps", "SeniorComp", "ReserveComp",
                         "--out", str(out)])
    rc = build_squads.main()
    assert rc == 0
    assert out.exists()

    import pandas as pd
    df = pd.read_parquet(out)
    for dup in (dup1, dup2):
        rows = df[df["player_id"] == dup]
        assert len(rows) == 1
        assert rows.iloc[0]["league"] == "SeniorComp"


def test_main_refuses_a_one_off_cross_squad_duplicate(monkeypatch, tmp_path):
    # A SINGLE player_id shared between two squads, with no other player
    # repeating that same league pair, is NOT auto-resolved -- it looks
    # exactly like a mid-season loan or an in-flight transfer, where the
    # earlier-listed competition could easily be the STALE one. Refusing
    # loudly (rather than confidently guessing) is the safe default here,
    # matching the pre-#132 behaviour for this specific shape.
    out = tmp_path / "squads.parquet"
    senior = _full_league(n_clubs=MIN_CLUBS)
    reserve = _full_league(n_clubs=MIN_CLUBS)
    for club in reserve:
        for person in club["person"]:
            person["id"] = "r_" + person["id"]
    dup = senior[0]["person"][0]["id"]
    reserve[0]["person"][0]["id"] = dup

    scrapers = {"SeniorComp": FakeScraper(senior), "ReserveComp": FakeScraper(reserve)}
    monkeypatch.setattr(build_squads, "OptaScraper",
                        lambda *a, **k: _DispatchScraper(scrapers))
    monkeypatch.setattr(build_squads.sys, "argv",
                        ["build_squads.py", "--comps", "SeniorComp", "ReserveComp",
                         "--out", str(out)])
    rc = build_squads.main()
    assert rc != 0
    assert not out.exists()


def test_main_refuses_to_write_a_partial_file_by_default(monkeypatch, tmp_path):
    squads = _full_league()
    squads.append(_club("Thin", MIN_SQUAD - 1, cid="cThin", start=9000))
    rc, out = _run_main(monkeypatch, tmp_path, squads)
    assert rc != 0
    assert not out.exists()


def test_main_allow_partial_is_opt_in_only(monkeypatch, tmp_path):
    # --allow-partial must stay a deliberate choice; if it ever becomes the
    # default, a club with nobody in it reaches the site silently.
    squads = _full_league()
    squads.append(_club("Thin", MIN_SQUAD - 1, cid="cThin", start=9000))
    rc, out = _run_main(monkeypatch, tmp_path, squads, extra_argv=("--allow-partial",))
    assert rc == 0
    assert out.exists()


def test_main_refuses_even_with_allow_partial_when_most_comps_return_nothing(monkeypatch, tmp_path):
    # --allow-partial exists to absorb ONE flaky/thin league among many, not
    # to silently ship a badly-degraded file when the feed itself is having a
    # broad (but not total) outage -- that would sail past the `not all_rows`
    # check (some comps DID return rows) while covering only a fraction of
    # what was requested, with nothing louder than a workflow log line
    # nobody's watching for a continue-on-error step (review finding,
    # pannadata#132). 3 of 4 comps returning nothing (75% > the 25% floor)
    # must refuse even with --allow-partial.
    out = tmp_path / "squads.parquet"
    ok_squads = _full_league()
    scrapers = {
        "OkComp": FakeScraper(ok_squads),
        "DeadComp1": FakeScraper([], calendar=False),
        "DeadComp2": FakeScraper([], calendar=False),
        "DeadComp3": FakeScraper([], calendar=False),
    }
    monkeypatch.setattr(build_squads, "OptaScraper",
                        lambda *a, **k: _DispatchScraper(scrapers))
    monkeypatch.setattr(build_squads.sys, "argv",
                        ["build_squads.py", "--comps",
                         "OkComp", "DeadComp1", "DeadComp2", "DeadComp3",
                         "--allow-partial", "--out", str(out)])
    rc = build_squads.main()
    assert rc != 0
    assert not out.exists()


def test_main_allow_partial_still_writes_when_one_of_many_comps_fails(monkeypatch, tmp_path):
    # The counterpart to the above: 1 of 5 comps failing (20%, under the 25%
    # floor) is exactly the "one flaky league" case --allow-partial is FOR,
    # and must still write.
    out = tmp_path / "squads.parquet"
    ok = _full_league()
    scrapers = {f"OkComp{i}": FakeScraper(ok) for i in range(4)}
    scrapers["DeadComp"] = FakeScraper([], calendar=False)
    monkeypatch.setattr(build_squads, "OptaScraper",
                        lambda *a, **k: _DispatchScraper(scrapers))
    monkeypatch.setattr(build_squads.sys, "argv",
                        ["build_squads.py", "--comps", *scrapers.keys(),
                         "--allow-partial", "--out", str(out)])
    rc = build_squads.main()
    assert rc == 0
    assert out.exists()


def test_main_writes_on_a_clean_run(monkeypatch, tmp_path):
    rc, out = _run_main(monkeypatch, tmp_path, _full_league())
    assert rc == 0
    assert out.exists()
    import pandas as pd
    df = pd.read_parquet(out)
    assert set(["player_id", "team", "team_id", "league", "position",
                "season", "build_id"]).issubset(df.columns)
    assert df["player_id"].is_unique
