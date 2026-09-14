"""Resolvable-variance boundaries from actual finite return observations.

The tiny observations exercise binary64 arithmetic, not a tradable return
forecast. Block moments are always produced from observations, never invented.
"""

import math

import numpy as np
import pytest

from trader.simulation.selection_bias import _block_moments, _pooled_sharpe


@pytest.mark.parametrize(("factor", "expected"), [
    (0.5, 0.0),
    (1.0, 0.0),
    (2.0, math.sqrt(2.0)),
])
def test_exact_noise_floor_remains_unresolved_with_real_block_moments(factor, expected):
    # At factor=1 the two-return sample variance is exactly 450360 binary64
    # subnormal units, the rounded 1e-10 * minimum-normal noise floor. The
    # mean is nonzero, so admitting equality would produce Sharpe sqrt(2).
    # Half and double scales pin both sides without fabricated moment tables.
    a = float.fromhex("0x1.da88066804ad6p-529") * factor
    returns = np.array([[a], [3.0 * a], [a], [3.0 * a]])
    moments = _block_moments(returns, 2)

    result = _pooled_sharpe(*moments, (0,))

    np.testing.assert_allclose(result, [expected], rtol=1e-12, atol=0)
