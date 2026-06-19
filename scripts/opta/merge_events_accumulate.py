#!/usr/bin/env python3
"""Accumulate a freshly-built single-season events_<comp>.parquet onto the prior
consolidated file, so rebuild-events ADDS a season instead of replacing the file.

rebuild_events.py fetches one season into the runner; consolidate_opta.py then
rebuilds events_<comp>.parquet from only that season. Without this merge, the
upload clobbers every other season already on opta-latest. Here we union the
prior consolidated (downloaded from opta-latest) with the new season, letting the
freshly-scraped data win for any overlapping match_ids (a genuine re-scrape).

Usage: merge_events_accumulate.py <prior.parquet> <new.parquet> <out.parquet>
A missing/empty prior is fine (first season for the comp).
"""
import sys
import pandas as pd

prior_path, new_path, out_path = sys.argv[1], sys.argv[2], sys.argv[3]

new = pd.read_parquet(new_path)
try:
    prior = pd.read_parquet(prior_path)
    if prior.empty:
        raise ValueError("empty prior")
except Exception as e:  # missing file, unreadable, or empty
    print(f"merge_events_accumulate: no usable prior ({e}); shipping new only")
    prior = new.iloc[0:0]

# Drop prior rows whose match_id is in the new (refreshed) data, then concat —
# this keeps all events for non-overlapping matches and replaces re-scraped ones.
new_ids = set(new["match_id"].unique())
keep = prior[~prior["match_id"].isin(new_ids)]
out = pd.concat([keep, new], ignore_index=True)
out.to_parquet(out_path, index=False)

print(
    f"accumulate: prior={prior['match_id'].nunique()} matches, "
    f"new={new['match_id'].nunique()}, union={out['match_id'].nunique()} matches"
)
