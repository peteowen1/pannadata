"""Tests for the defensive dob_raw capture in opta_scraper (pannadata#112).

The Opta MA1 lineup/player block has never been observed to carry
dateOfBirth/birthDate (see build_player_profiles.py's module docstring for
the investigation), but extract_all_player_stats() captures it defensively
so a future feed change is picked up for free. These tests pin that it never
crashes on a missing/malformed value, matching the _to_int_or_0 precedent
(Euro-1980 incident) applied to a string field.
"""
from opta_scraper import OptaScraper, _to_str_or_none


def test_to_str_or_none_present():
    assert _to_str_or_none("1990-01-01") == "1990-01-01"


def test_to_str_or_none_missing():
    assert _to_str_or_none(None) is None
    assert _to_str_or_none("") is None
    assert _to_str_or_none(0) is None


def test_to_str_or_none_non_string_coerced_not_crashed():
    # A hypothetical malformed payload (nested object instead of a string)
    # must not raise -- it gets stringified rather than corrupting the
    # column's dtype at to_parquet() time.
    assert _to_str_or_none({"unexpected": "shape"}) == "{'unexpected': 'shape'}"
    assert _to_str_or_none(19900101) == "19900101"


def _match_data(player_extra=None):
    return {
        "matchInfo": {"id": "m1", "date": "2020-01-01Z", "description": "Test v Test"},
        "liveData": {
            "matchDetails": {"scores": {"ft": {"home": 1, "away": 0}}},
            "lineUp": [
                {
                    "contestantId": "t1",
                    "formationUsed": "4-4-2",
                    "player": [
                        {
                            "playerId": "p1",
                            "matchName": "T. Player",
                            "firstName": "Test",
                            "lastName": "Player",
                            "shirtNumber": 10,
                            "position": "Midfielder",
                            "positionSide": "Centre",
                            "stat": [],
                            **(player_extra or {}),
                        }
                    ],
                }
            ],
        },
    }


def test_extract_all_player_stats_dob_present():
    scraper = OptaScraper.__new__(OptaScraper)  # no __init__ deps needed for a pure extractor
    df = scraper.extract_all_player_stats(_match_data({"dateOfBirth": "1990-05-15"}))
    assert df.iloc[0]["dob_raw"] == "1990-05-15"


def test_extract_all_player_stats_dob_missing():
    scraper = OptaScraper.__new__(OptaScraper)
    df = scraper.extract_all_player_stats(_match_data())
    assert df.iloc[0]["dob_raw"] is None


def test_extract_all_player_stats_birthdate_fallback_key():
    # Some Opta feed variants use birthDate instead of dateOfBirth.
    scraper = OptaScraper.__new__(OptaScraper)
    df = scraper.extract_all_player_stats(_match_data({"birthDate": "1988-02-29"}))
    assert df.iloc[0]["dob_raw"] == "1988-02-29"


def test_extract_all_player_stats_dob_never_crashes_scrape():
    # A structurally weird value (e.g. an empty dict, which some feeds use
    # as a "no data" placeholder instead of omitting the key) must not raise.
    scraper = OptaScraper.__new__(OptaScraper)
    df = scraper.extract_all_player_stats(_match_data({"dateOfBirth": {}}))
    assert df.iloc[0]["dob_raw"] is None
