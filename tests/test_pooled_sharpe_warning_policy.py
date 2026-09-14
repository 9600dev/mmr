"""The supported float64 calculation survives strict deprecation policy."""

import math
import warnings

import numpy as np

from trader.simulation.selection_bias import _block_moments, _pooled_sharpe


def test_float64_pooled_calculation_uses_supported_numeric_type_parameters():
    moments = _block_moments(np.array([[1.0], [3.0], [1.0], [3.0]]), 2)

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        result = _pooled_sharpe(*moments, (0, 1))

    np.testing.assert_allclose(result, [math.sqrt(3)], rtol=1e-12, atol=0)
