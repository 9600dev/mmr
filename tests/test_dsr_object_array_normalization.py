"""Public return arrays preserve finite real values across dtype normalization."""

import numpy as np

from trader.simulation.selection_bias import deflated_sharpe


def test_object_array_of_finite_real_returns_retains_zero_mean_probability():
    # One unselected trial has benchmark zero. These nonconstant returns have
    # mean zero, so the PSR normal argument is zero and its probability is 1/2.
    returns = np.array([-1.0, 0.0, 1.0], dtype=object)

    assert deflated_sharpe(returns, [0.0], bars_per_year=252.0) == 0.5
