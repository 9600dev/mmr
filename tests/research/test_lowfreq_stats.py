"""DSR deflation (D01 c): N from the family trial count, V from recorded trial
Sharpes; unavailable V with N > 1 gives no DSR (fail closed), never plain PSR."""
from __future__ import annotations

import numpy as np
import pytest

from trader.research.lowfreq import stats
from trader.research.lowfreq.verdict import GateInputs, decide
from trader.simulation.backtest_stats import probabilistic_sharpe


def _returns(seed=0, n=600, mu=0.0008, sd=0.01):
    return np.random.default_rng(seed).normal(mu, sd, n)


def test_single_trial_is_plain_psr_against_zero():
    r = _returns()
    assert stats.deflated_sharpe_n(r, 1, []) == pytest.approx(probabilistic_sharpe(r, benchmark_sharpe=0.0))
    assert stats.deflated_sharpe_n(r, 1, [1.2]) == pytest.approx(probabilistic_sharpe(r, benchmark_sharpe=0.0))


def test_more_trials_deflate_and_unmeasurable_spread_gives_no_dsr():
    r = _returns()
    one = stats.deflated_sharpe_n(r, 1, [])
    many = stats.deflated_sharpe_n(r, 12, [0.2, 0.6, 1.1, 0.4])
    assert many is not None and one is not None and many < one
    assert stats.deflated_sharpe_n(r, 3, [1.0]) is None        # N > 1 but one recorded Sharpe: V unknown
    assert stats.deflated_sharpe_n(r, 3, []) is None
    assert stats.deflated_sharpe_n(r, 3, [float('nan'), 0.5]) is None
    assert stats.deflated_sharpe_n(r[:2], 1, []) is None       # too short


def test_no_dsr_cannot_pass():
    g = GateInputs(1.0, 0.8, 0.03, 2.5, None, -0.2, -0.22, 0.8, 0.01, 1)
    assert decide(g)['verdict'] == 'PROMISING' and decide(g)['gates']['c_dsr_ge_0_90'] is False
