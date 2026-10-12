"""R087: a failed page-1 call is not an empty window. No network."""
import pytest

from opta_scraper import OptaScraper


@pytest.fixture
def scraper(tmp_path):
    return OptaScraper(data_dir=str(tmp_path))


def _stub(scraper, responses):
    calls = iter(responses)
    scraper._fetch_raw = lambda endpoint, params, max_retries=3: next(calls)


def test_404_on_page_one_is_a_legitimately_empty_window(scraper):
    _stub(scraper, [(None, 404)])
    assert scraper.get_season_matches("sid", "2025-01-01", "2025-02-28") == []
    assert scraper.failed_windows == []


def test_200_without_match_key_is_empty_not_failed(scraper):
    _stub(scraper, [({"other": 1}, 200)])
    assert scraper.get_season_matches("sid", "2025-01-01", "2025-02-28") == []
    assert scraper.failed_windows == []


@pytest.mark.parametrize("status", [0, 400, 403, 429, 500, 503])
def test_transport_or_auth_failure_on_page_one_is_recorded(scraper, status):
    _stub(scraper, [(None, status)])
    assert scraper.get_season_matches("sid", "2025-01-01", "2025-02-28") == []
    assert scraper.failed_windows == [{
        "season_id": "sid", "start_date": "2025-01-01",
        "end_date": "2025-02-28", "status": status,
    }]


def test_healthy_window_records_nothing(scraper):
    _stub(scraper, [({"match": [{"matchInfo": {"id": "m1"}}]}, 200)])
    out = scraper.get_season_matches("sid", "2025-01-01", "2025-02-28")
    assert [m["matchInfo"]["id"] for m in out] == ["m1"]
    assert scraper.failed_windows == []
