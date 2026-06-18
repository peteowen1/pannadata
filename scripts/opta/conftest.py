"""Shared pytest fixtures for the Opta scraper test suite.

Lives in scripts/opta/ so the bare-name imports used by the scraper modules
(`from scrape_opta import ...`) resolve when pytest is invoked from here.
"""
import pandas as pd
import pytest


@pytest.fixture
def opta_dir(tmp_path):
    """A throwaway stand-in for pannadata/data/opta — an isolated tmp subdir.

    reconcile_events_with_manifest() reads the consolidated
    `opta_dir / "opta_events.parquet"`; tests write that file via the
    `write_events` fixture below.
    """
    d = tmp_path / "opta"
    d.mkdir()
    return d


@pytest.fixture
def write_events(opta_dir):
    """Write a real opta_events.parquet under opta_dir.

    Exercises the genuine parquet schema path (pandas/pyarrow) rather than
    monkeypatching, so reconcile's schema check and predicate-pushdown filter
    run against an actual file.

    Args:
        rows: iterable of (match_id, competition, season) tuples.
        extra_cols: optional dict {col_name: [values...]} of additional
            columns to include (e.g. to add unrelated columns, or — when the
            required trio is overridden via `columns=` — to model schema drift).
        columns: optional explicit column list to write instead of the default
            ("match_id", "competition", "season"). Use to drop/rename a
            required column for the schema-drift test.

    Returns the Path to the written parquet.
    """
    def _write(rows, extra_cols=None, columns=("match_id", "competition", "season")):
        rows = list(rows)
        data = {}
        for i, col in enumerate(columns):
            data[col] = [r[i] for r in rows]
        if extra_cols:
            for col, vals in extra_cols.items():
                data[col] = list(vals)
        df = pd.DataFrame(data)
        path = opta_dir / "opta_events.parquet"
        df.to_parquet(path, index=False)
        return path

    return _write
