"""pannadata#241: every window of a planned season is fetched, however far ahead.

The 65-day lookahead used to skip future windows inside scrape_season, so
opta_fixtures.parquet stopped ~65 days past "today" for any league not seeded
by hand. No network: the scraper is a stub and output goes to tmp_path.
"""
from datetime import datetime, timedelta

import pytest

import scrape_opta


class _StubScraper:
    def __init__(self, tmp_path):
        self.data_dir = tmp_path
        self.windows = []

    def get_season_matches(self, season_id, start_date, end_date):
        self.windows.append((start_date, end_date))
        # one scheduled game per window, dated at the window start
        return [{
            "matchInfo": {"id": f"m-{start_date}", "date": f"{start_date}T15:00:00Z",
                          "contestant": []},
            "liveData": {"matchDetails": {"matchStatus": "Fixture"}},
        }]


@pytest.fixture
def stub(tmp_path, monkeypatch):
    monkeypatch.setattr(scrape_opta, "get_pannadata_dir", lambda: tmp_path / "data")
    return _StubScraper(tmp_path)


def _season_label():
    """A season that started last August and ends next July, whatever today is."""
    today = datetime.now().date()
    start = today.year if today.month >= 8 else today.year - 1
    return f"{start}-{start + 1}", start + 1


def test_windows_beyond_the_lookahead_are_still_fetched(stub):
    label, end_year = _season_label()
    scrape_opta.scrape_season(stub, "Primeira_Liga", label, "sid", set(), set(),
                              scrape_types=["fixtures"])
    last_window_start = f"{end_year}-06-01"
    days_ahead = (datetime.strptime(last_window_start, "%Y-%m-%d").date()
                  - datetime.now().date()).days
    if days_ahead <= scrape_opta.FIXTURE_LOOKAHEAD_DAYS:
        pytest.skip("today is too close to the end of the season to exercise the old cut")
    assert (last_window_start, f"{end_year}-07-31") in stub.windows
    # every window the season defines was requested
    assert stub.windows == scrape_opta.get_season_date_range(label, "Primeira_Liga")


def test_future_fixture_rows_reach_the_season_file(stub, tmp_path):
    import pandas as pd
    label, end_year = _season_label()
    scrape_opta.scrape_season(stub, "Primeira_Liga", label, "sid", set(), set(),
                              scrape_types=["fixtures"])
    path = tmp_path / "data" / "opta" / "fixtures" / "Primeira_Liga" / f"{label}.parquet"
    df = pd.read_parquet(path)
    assert len(df) == len(stub.windows)
    assert df["match_date"].max() >= f"{end_year}-06-01"
