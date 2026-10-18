"""Catalog our_key column vs the scraper registry (no network)."""
import csv
import json
from pathlib import Path

from build_entitlement_catalog import remap_our_key
from opta_scraper import OptaScraper

HERE = Path(__file__).parent


def test_catalog_our_key_matches_registry():
    """our_key must be exactly what COMPETITIONS says, so it can't hide coverage."""
    id_to_key = {cid: k for k, cid in OptaScraper.COMPETITIONS.items()}
    rows = list(csv.DictReader(open(HERE / "opta_entitlement_catalog.csv", encoding="utf-8")))
    stale = {(r["competition_name"], r["our_key"], id_to_key.get(r["competition_id"], ""))
             for r in rows if r["our_key"] != id_to_key.get(r["competition_id"], "")}
    assert not stale, f"run: python build_entitlement_catalog.py --remap-only ({sorted(stale)[:5]})"


def test_every_registered_competition_has_seasons():
    seasons = json.load(open(HERE / "seasons.json", encoding="utf-8"))
    for key in ("Concacaf_WC_Qualifiers", "Concacaf_Nations_League"):
        assert key in OptaScraper.COMPETITIONS
        assert seasons[key]


def test_remap_our_key_roundtrip(tmp_path):
    p = tmp_path / "c.csv"
    p.write_text("competition_id,our_key,season_id\na,,s1\nb,STALE,s2\nc,,s3\n", encoding="utf-8")
    n, changed = remap_our_key(p, {"A": "a", "C": "c"})
    rows = list(csv.DictReader(open(p, encoding="utf-8")))
    assert (n, changed) == (3, 3)
    assert [r["our_key"] for r in rows] == ["A", "", "C"]
