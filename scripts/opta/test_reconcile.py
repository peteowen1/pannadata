"""Tests for reconcile_events_with_manifest() in scrape_opta.py.

This is the panna#59 regression guard: it invalidates manifest entries that
claim has_match_events=True but whose match_id is absent from the consolidated
opta_events.parquet (a "scrape gap"), so the match gets re-scraped instead of
silently losing its goals downstream.

Imported by bare name (`from scrape_opta import ...`) — pytest must run with
scripts/opta as rootdir (see pytest.ini).
"""
import pandas as pd
import pyarrow
import pytest

from scrape_opta import reconcile_events_with_manifest


def test_regression_pin_invalidates_missing_keeps_present(opta_dir, write_events):
    """A manifest entry that events.parquet lacks is invalidated and dropped;
    a present one is kept. Pin the panna#59 behaviour.

    To stay under the 25% hard-fail threshold, the scoped set has many present
    matches and a single missing one (1/8 = 12.5% invalidation).
    """
    season = "2025-2026"
    comp = "Championship"
    present = [(f"m{i}", comp, season) for i in range(7)]
    missing = ("gap_match", comp, season)

    # events.parquet contains only the present matches (the gap match's events
    # silently failed to publish).
    write_events(present)

    complete = set(present) | {missing}
    scrape_plan = [(comp, season, "sid_123")]

    pruned, n_invalidated = reconcile_events_with_manifest(opta_dir, complete, scrape_plan)

    assert n_invalidated == 1
    assert missing not in pruned
    for p in present:
        assert p in pruned
    assert len(pruned) == len(present)


def test_corrupt_parquet_is_resilient(opta_dir, write_events):
    """A corrupt/unreadable opta_events.parquet logs an error and returns the
    complete set unchanged (no crash)."""
    path = opta_dir / "opta_events.parquet"
    path.write_bytes(b"this is not a parquet file")

    complete = {("m1", "EPL", "2025-2026"), ("m2", "EPL", "2025-2026")}
    scrape_plan = [("EPL", "2025-2026", "sid")]

    pruned, n_invalidated = reconcile_events_with_manifest(opta_dir, complete, scrape_plan)

    assert pruned == complete
    assert n_invalidated == 0


def test_out_of_scope_entries_preserved(opta_dir, write_events):
    """Only matches whose (competition, season) is in scrape_plan are
    considered. Out-of-scope manifest entries are preserved even if absent
    from events.parquet."""
    # events.parquet has nothing for the out-of-scope league.
    write_events([("e1", "EPL", "2025-2026")])

    in_scope_present = ("e1", "EPL", "2025-2026")
    out_of_scope_absent = ("g1", "Bundesliga", "2025-2026")
    complete = {in_scope_present, out_of_scope_absent}

    # Plan only covers EPL — the Bundesliga entry must not be touched.
    scrape_plan = [("EPL", "2025-2026", "sid")]

    pruned, n_invalidated = reconcile_events_with_manifest(opta_dir, complete, scrape_plan)

    assert n_invalidated == 0
    assert out_of_scope_absent in pruned
    assert in_scope_present in pruned


def test_empty_complete_matches(opta_dir):
    """Empty complete_matches short-circuits to (complete_matches, 0) before
    any file access."""
    complete = set()
    pruned, n_invalidated = reconcile_events_with_manifest(opta_dir, complete, [("EPL", "2025-2026", "sid")])
    assert pruned == complete
    assert n_invalidated == 0


def test_missing_events_file(opta_dir):
    """No opta_events.parquet (first run, nothing consolidated yet) returns
    (complete_matches, 0)."""
    complete = {("m1", "EPL", "2025-2026")}
    assert not (opta_dir / "opta_events.parquet").exists()

    pruned, n_invalidated = reconcile_events_with_manifest(opta_dir, complete, [("EPL", "2025-2026", "sid")])
    assert pruned == complete
    assert n_invalidated == 0


def test_all_present_no_invalidation(opta_dir, write_events):
    """When every scoped complete match is present in events.parquet, nothing
    is invalidated and the set is returned unchanged."""
    season = "2025-2026"
    comp = "La_Liga"
    matches = [(f"m{i}", comp, season) for i in range(5)]
    write_events(matches)

    complete = set(matches)
    scrape_plan = [(comp, season, "sid")]

    pruned, n_invalidated = reconcile_events_with_manifest(opta_dir, complete, scrape_plan)

    assert n_invalidated == 0
    assert pruned == complete


def test_schema_drift_raises_runtimeerror(opta_dir, write_events):
    """A readable parquet missing a required column (match_id) must raise
    RuntimeError rather than silently returning an empty (100%-invalidation)
    frame."""
    # Valid parquet, but the required `match_id` column has been renamed/dropped.
    write_events(
        [("EPL", "2025-2026"), ("EPL", "2025-2026")],
        columns=("competition", "season"),
    )

    complete = {("m1", "EPL", "2025-2026")}
    scrape_plan = [("EPL", "2025-2026", "sid")]

    with pytest.raises(RuntimeError, match="missing required columns"):
        reconcile_events_with_manifest(opta_dir, complete, scrape_plan)


def test_over_25pct_invalidation_raises(opta_dir, write_events):
    """When >25% of scoped complete entries would be invalidated, raise
    RuntimeError (the panna#59 regression guard — wrong path invalidated 100%
    and burned GHA time)."""
    season = "2025-2026"
    comp = "EPL"
    present = [("p1", comp, season)]            # 1 present
    absent = [(f"a{i}", comp, season) for i in range(3)]  # 3 absent -> 75% invalidation

    write_events(present)

    complete = set(present) | set(absent)
    scrape_plan = [(comp, season, "sid")]

    with pytest.raises(RuntimeError, match="refusing to"):
        reconcile_events_with_manifest(opta_dir, complete, scrape_plan)
