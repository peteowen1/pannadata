"""Tests for get_season_date_range (R088, R089): competition-aware windows.

No network. The catalog test reads opta_entitlement_catalog.csv + seasons.json
shipped in this directory.
"""
import csv
import json
from datetime import date
from pathlib import Path

import pytest

from scrape_opta import get_season_date_range

HERE = Path(__file__).parent


def _covers(ranges, day):
    return any(a <= day <= b for a, b in ranges)


def test_euro_2020_finals_window_only_for_uefa_euros():
    assert get_season_date_range("2020", "UEFA_Euros") == [("2021-06-01", "2021-07-31")]


@pytest.mark.parametrize("comp", ["MLS", "Brazilian_Serie_A", "Allsvenskan", "Irish_Premier"])
def test_bare_2020_calendar_year_leagues_get_2020_not_euro_window(comp):
    ranges = get_season_date_range("2020", comp)
    assert _covers(ranges, "2020-03-01")
    assert _covers(ranges, "2020-12-01")
    # Must not be the Jun-Jul 2021 EURO window
    assert ranges != [("2021-06-01", "2021-07-31")]


def test_euro_qualifiers_2024_cycle_starts_march_2023():
    ranges = get_season_date_range("2024 Germany", "UEFA_Euro_Qualifiers")
    assert _covers(ranges, "2023-03-23") and _covers(ranges, "2024-03-26")


def test_euro_finals_2024_keeps_default_window():
    ranges = get_season_date_range("2024 Germany", "UEFA_Euros")
    assert _covers(ranges, "2024-06-14") and _covers(ranges, "2024-07-14")


def test_wc_2026_uefa_qualifiers_united_states_label():
    ranges = get_season_date_range("2026 Canada-Mexico-United States", "UEFA_WC_Qualifiers")
    assert _covers(ranges, "2025-03-21") and _covers(ranges, "2026-03-31")


def test_wc_2026_other_confederations_use_usa_label_exception():
    ranges = get_season_date_range("2026 Canada-Mexico-USA", "CAF_WC_Qualifiers")
    assert _covers(ranges, "2023-11-15") and _covers(ranges, "2025-11-16")


def test_no_competition_falls_back_to_competition_agnostic_rules():
    # Bare 2020 without a competition is the calendar-year default, never EURO.
    assert get_season_date_range("2020") != [("2021-06-01", "2021-07-31")]
    assert get_season_date_range("2022 Qatar") == [("2019-09-01", "2022-12-31")]


def test_intl_friendlies_calendar_year():
    assert get_season_date_range("Intl_Friendlies_2019") == [("2019-01-01", "2019-12-31")]


def test_regression_cases_cover_catalog_dates():
    """Windows for the seasons named in R088/R089 cover every catalog date."""
    seasons = json.load(open(HERE / "seasons.json", encoding="utf-8"))
    catalog = {r["season_id"]: r for r in
               csv.DictReader(open(HERE / "opta_entitlement_catalog.csv", encoding="utf-8"))}
    cases = [
        ("UEFA_Euros", "2020"), ("UEFA_Euro_Qualifiers", "2020"),
        ("UEFA_Euro_Qualifiers", "2024 Germany"),
        ("UEFA_WC_Qualifiers", "2026 Canada-Mexico-United States"),
        ("MLS", "2020"), ("Eliteserien", "2020"),
        ("Allsvenskan", "2020"), ("Irish_Premier", "2020"), ("NZ_National_League", "2020"),
        ("Kazakhstan_Premier", "2020"), ("AFC_WC_Qualifiers", "2026 Canada-Mexico-USA"),
        ("CAF_WC_Qualifiers", "2026 Canada-Mexico-USA"),
        ("CONMEBOL_WC_Qualifiers", "2026 Canada-Mexico-USA"),
    ]
    for comp, season in cases:
        row = catalog[seasons[comp][season]]
        start = row["start_date"].rstrip("Z")
        end = row["end_date"].rstrip("Z")
        ranges = get_season_date_range(season, comp)
        assert _covers(ranges, start), (comp, season, "start", start)
        assert _covers(ranges, end), (comp, season, "end", end)


def test_brazil_2020_start_covered():
    # Brazilian_Serie_A 2020 ran to Feb 2021 (COVID); the end of the season is
    # a separate, older gap and is deliberately not asserted here.
    ranges = get_season_date_range("2020", "Brazilian_Serie_A")
    assert _covers(ranges, "2020-08-09")
