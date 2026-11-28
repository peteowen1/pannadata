"""Venue, kickoff, referee and attendance are kept from the match-list feed.

Until 2026-11-28 the fixture record kept ids, date, status, teams and scores
only; the venue (with coordinates), referee and attendance in the same payload
were dropped, so panna's weather features never ran. No network.
"""
import math

import scrape_opta
from opta_scraper import fixture_context_fields

PLAYED = {
    "matchInfo": {
        "id": "m1", "date": "2026-11-27Z", "localDate": "2026-11-27", "localTime": "20:00:00",
        "week": "12", "coverageLevel": "15", "var": "1", "contestant": [],
        "venue": {"id": "v1", "neutral": "no", "longName": "The City Ground",
                  "shortName": "City Ground", "latitude": "52.939913226",
                  "longitude": "-1.132894009"},
    },
    "liveData": {
        "matchDetails": {"matchStatus": "Played"},
        "matchDetailsExtra": {
            "attendance": "30851",
            "matchOfficial": [
                {"id": "a1", "type": "Assistant referee 1", "firstName": "Tim", "lastName": "Wood"},
                {"id": "r1", "type": "Main", "firstName": "Andy", "lastName": "Madley"},
            ],
        },
    },
}


def test_played_match_keeps_every_field():
    out = fixture_context_fields(PLAYED)
    assert out["venue_id"] == "v1"
    assert out["venue_name"] == "The City Ground"
    assert out["venue_latitude"] == 52.939913226
    assert out["venue_longitude"] == -1.132894009
    assert out["venue_neutral"] is False
    assert (out["local_date"], out["local_time"], out["match_week"]) == ("2026-11-27", "20:00:00", "12")
    # the referee is the "Main" official wherever he sits in the list
    assert (out["referee_id"], out["referee_name"]) == ("r1", "Andy Madley")
    assert out["attendance"] == 30851.0
    assert '"id":"a1"' in out["match_officials_json"]


def test_fixture_before_referee_is_appointed():
    m = {"matchInfo": {"id": "m2", "localTime": "14:00:00",
                       "venue": {"id": "v2", "neutral": "yes", "shortName": "Wembley",
                                 "latitude": "51.556", "longitude": "-0.2796"}},
         "liveData": {"matchDetails": {"matchStatus": "Fixture"}}}
    out = fixture_context_fields(m)
    assert out["venue_name"] == "Wembley" and out["venue_neutral"] is True
    assert out["referee_id"] is None and out["referee_name"] is None
    assert math.isnan(out["attendance"])
    assert out["match_officials_json"] == ""


def test_old_season_with_no_venue_or_extras_is_all_missing():
    out = fixture_context_fields({"matchInfo": {"id": "m3"}, "liveData": {}})
    assert out["venue_id"] is None and out["venue_neutral"] is None
    assert math.isnan(out["venue_latitude"]) and math.isnan(out["attendance"])
    assert out["referee_id"] is None


def test_odd_shapes_do_not_raise():
    one = {"liveData": {"matchDetailsExtra": {"matchOfficial": {"id": "r", "type": "Main",
                                                                "lastName": "Oliver"}}}}
    assert fixture_context_fields(one)["referee_id"] == "r"
    for officials in ([None], ["x"], "x", 7):
        out = fixture_context_fields({"liveData": {"matchDetailsExtra": {"matchOfficial": officials}}})
        assert out["referee_id"] is None
    assert fixture_context_fields({"matchInfo": {"venue": "Anfield"}})["venue_id"] is None
    assert fixture_context_fields({"liveData": {"matchDetailsExtra": []}})["referee_id"] is None


def test_scrape_season_writes_the_columns(tmp_path, monkeypatch):
    class Stub:
        data_dir = tmp_path
        failed_windows = []

        def get_season_matches(self, season_id, start_date, end_date):
            return [PLAYED]

    monkeypatch.setattr(scrape_opta, "get_pannadata_dir", lambda: tmp_path / "data")
    import pandas as pd
    scrape_opta.scrape_season(Stub(), "EPL", "2026-2027", "sid", set(), set(),
                              scrape_types=["fixtures"])
    (path,) = list((tmp_path / "data").rglob("fixtures/EPL/2026-2027.parquet"))
    fx = pd.read_parquet(path)
    assert len(fx) == 1
    assert fx.iloc[0]["referee_name"] == "Andy Madley"
    assert fx.iloc[0]["venue_latitude"] == 52.939913226
