"""panna#175: Miss shots' goalmouth_y comes from q102, never q231 (the keeper's y)."""
import json

import numpy as np
import pandas as pd

from backfill_goalmouth import MISS_TYPE_ID, apply_goalmouth, goalmouth_of


def q(**kv):
    return json.dumps({k.lstrip("q"): v for k, v in kv.items()})


def test_goalmouth_of_uses_q102_for_misses():
    # A wide miss: q102 far outside the posts, q231 (keeper) mid-goal.
    assert goalmouth_of(q(q102="12.5", q103="20", q231="50.1"), MISS_TYPE_ID) == (12.5, 20.0)


def test_goalmouth_of_on_target_unchanged():
    assert goalmouth_of(q(q102="47.0", q103="10", q231="49.0"), 16) == (47.0, 10.0)


def test_goalmouth_of_missing_and_comma_decimal():
    assert goalmouth_of(q(q103="5"), MISS_TYPE_ID) == (None, 5.0)
    assert goalmouth_of(q(q102="49,8"), 15) == (49.8, None)


def test_apply_goalmouth_overwrites_miss_y_only():
    merged = pd.DataFrame({
        "type_id":     [13,   13,     15,   16,   13],
        "goalmouth_y": [50.1, 50.4,   47.0, np.nan, 49.9],  # rows 0,1,4 = q231-era misses
        "goalmouth_z": [20.0, np.nan, 10.0, 5.0,  30.0],
        "gm_y":        [12.5, 88.0,   46.0, 51.0, np.nan],  # row 4: no events value
        "gm_z":        [21.0, 25.0,   11.0, 6.0,  np.nan],
    })
    out = apply_goalmouth(merged)
    assert list(out.columns) == ["type_id", "goalmouth_y", "goalmouth_z"]
    # Misses with a q102 value are overwritten ...
    assert out.loc[0, "goalmouth_y"] == 12.5 and out.loc[1, "goalmouth_y"] == 88.0
    # ... a miss with no derived value keeps what it had (never goes NA) ...
    assert out.loc[4, "goalmouth_y"] == 49.9
    # ... a non-miss keeps its existing value, and a gap is filled.
    assert out.loc[2, "goalmouth_y"] == 47.0 and out.loc[3, "goalmouth_y"] == 51.0
    # z is coalesce-only for every type.
    assert out.loc[0, "goalmouth_z"] == 20.0 and out.loc[1, "goalmouth_z"] == 25.0


def test_apply_goalmouth_idempotent():
    merged = pd.DataFrame({"type_id": [13, 15], "goalmouth_y": [12.5, 47.0], "goalmouth_z": [1.0, 2.0],
                           "gm_y": [12.5, 46.0], "gm_z": [1.0, 2.0]})
    first = apply_goalmouth(merged.copy())
    again = apply_goalmouth(first.assign(gm_y=[12.5, 46.0], gm_z=[1.0, 2.0]))
    pd.testing.assert_frame_equal(first, again)
