"""OptaScraper.get_squads() pages the squads feed (pannadata#234).

The feed returns 20 clubs per page unless asked for more, so before paging
every league with more than 20 clubs lost the last ones alphabetically
(Championship 20 of 24 on 2026-11-17). A page past the end is a 404.
No network: _fetch_raw is stubbed.
"""
from opta_scraper import OptaScraper


def _scraper(pages, tmp_path):
    """pages: list of club counts per page; any page beyond the list 404s.
    A count of None means that page fails with a non-404 status."""
    s = OptaScraper(data_dir=str(tmp_path))
    calls = []

    def fake(endpoint, params, max_retries=3):
        calls.append(dict(params))
        n = int(params["_pgNm"])
        if n > len(pages):
            return None, 404
        if pages[n - 1] is None:
            return None, 503
        start = sum(pages[: n - 1])
        return {"lastUpdated": "x", "squad": [{"contestantId": f"c{start + i}"} for i in range(pages[n - 1])]}, 200

    s._fetch_raw = fake
    return s, calls


def test_short_first_page_is_the_whole_list(tmp_path):
    s, calls = _scraper([24], tmp_path)
    out = s.get_squads("t")
    assert len(out["squad"]) == 24 and out["lastUpdated"] == "x"
    assert calls[0]["_pgSz"] == "100" and len(calls) == 1


def test_follows_pages_until_a_short_one(tmp_path):
    s, _ = _scraper([100, 100, 7], tmp_path)
    out = s.get_squads("t")
    assert len(out["squad"]) == 207
    assert len({c["contestantId"] for c in out["squad"]}) == 207


def test_exactly_full_last_page_then_404_keeps_everything(tmp_path):
    s, _ = _scraper([100], tmp_path)
    assert len(s.get_squads("t")["squad"]) == 100


def test_page_one_failure_returns_none(tmp_path):
    s, _ = _scraper([], tmp_path)          # page 1 404s
    assert s.get_squads("t") is None


def test_later_page_failure_returns_none_not_partial(tmp_path):
    s, _ = _scraper([100, None], tmp_path)
    assert s.get_squads("t") is None
