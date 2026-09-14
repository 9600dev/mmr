"""Strategy responses tolerate numpy/pandas scalars, never non-finite values (review 2026-09-11)."""
import json

import numpy as np
import pandas as pd
import pytest

from trader.strategy.callback_worker import _json_default


def test_numpy_and_pandas_scalars_in_metadata_encode():
    payload = {'i': np.int64(3), 'b': np.bool_(True), 'f': np.float32(1.5), 'arr': np.array([1, 2]),
               'ts': pd.Timestamp('2026-09-11 14:30', tz='UTC')}
    encoded = json.loads(json.dumps(payload, allow_nan=False, default=_json_default))
    assert encoded == {'i': 3, 'b': True, 'f': 1.5, 'arr': [1, 2], 'ts': '2026-09-11T14:30:00+00:00'}


def test_non_finite_values_are_still_refused():
    """A NaN signal field is a real strategy fault the parent must not act on."""
    with pytest.raises(ValueError):
        json.dumps({'p': np.float64('nan')}, allow_nan=False, default=_json_default)
    with pytest.raises(ValueError):
        json.dumps({'p': float('inf')}, allow_nan=False, default=_json_default)


def test_unknown_objects_still_raise():
    with pytest.raises(TypeError, match='not JSON serialisable'):
        json.dumps({'x': object()}, default=_json_default)
