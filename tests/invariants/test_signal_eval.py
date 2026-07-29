"""SPEC: a signal must be scored against the future, never against the present.

The forward-return alignment is this module's version of the walk-forward
leakage property, and it fails the same way: invisibly. Off by one and you are
correlating a signal with the return that PRODUCED it, which yields a large,
stable, entirely fake IC — and the output looks exactly like a discovery. A
plausible number, a plausible spread, a strategy built on it.

So the first group builds frames whose right answer is known by construction:
a signal that is literally tomorrow's return must score IC = 1.0, and the same
signal shifted so it is YESTERDAY's return must score ~0. If the alignment
were off by one, those two results would swap, and nothing else in the output
would look different.

The second group is about not fabricating observations. A missing signal is
not a neutral signal; a delisted instrument's missing return is not a zero
return. Filling either puts an invented data point in the middle of the
cross-section, at the exact moments the data is telling you something.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from hypothesis import given, settings, strategies as st

from trader.simulation.signal_eval import (
    forward_returns, ic_t_statistic, information_coefficient,
    newey_west_variance, period_ic,
    periods_needed_for_significance, quantile_returns, rank_turnover,
    summarise_ic)


def _panel(n_periods=60, n_names=20, seed=0):
    rng = np.random.default_rng(seed)
    idx = pd.date_range('2020-01-01', periods=n_periods, freq='D')
    cols = [1000 + i for i in range(n_names)]
    steps = rng.normal(0, 0.02, size=(n_periods, n_names))
    prices = pd.DataFrame(100 * np.exp(np.cumsum(steps, axis=0)),
                          index=idx, columns=cols)
    return prices


class TestTheSignalIsScoredAgainstTheFuture:
    """Frames whose answer is known by construction. If the alignment were off
    by one, these two results would swap and nothing else would look wrong."""

    def test_a_perfect_oracle_scores_one(self):
        """A signal that IS the next period's return must score IC = 1.0. If
        this fails low, the signal is being compared against the wrong bar."""
        prices = _panel()
        oracle = prices.shift(-1) / prices - 1.0
        ic = information_coefficient(oracle, prices, horizon=1)
        assert len(ic) > 20
        assert ic.mean() == pytest.approx(1.0, abs=1e-9), (
            f'a signal equal to the forward return scored {ic.mean():.4f}, '
            f'not 1.0 — the alignment is wrong')

    def test_yesterdays_return_is_not_predictive_of_tomorrow(self):
        """The same construction shifted the other way. On a random walk this
        must be ~0; if it comes back near 1.0 the frame is being correlated
        with itself."""
        prices = _panel()
        past = prices / prices.shift(1) - 1.0
        ic = information_coefficient(past, prices, horizon=1)
        assert abs(ic.mean()) < 0.2, (
            f'past returns scored IC {ic.mean():.3f} on a random walk — '
            f'the signal is being compared against its own bar')

    def test_the_current_price_is_not_a_signal(self):
        """Price level should carry no rank information about the next return
        on a driftless walk. A large IC here means the forward frame is
        leaking the level."""
        prices = _panel(seed=3)
        ic = information_coefficient(prices, prices, horizon=1)
        assert abs(ic.mean()) < 0.2

    @settings(max_examples=50, deadline=None)
    @given(h=st.integers(min_value=1, max_value=10))
    def test_row_t_holds_the_return_from_t_to_t_plus_h(self, h):
        prices = _panel(n_periods=40, n_names=3, seed=1)
        fwd = forward_returns(prices, h)
        col = prices.columns[0]
        for t in range(len(prices) - h):
            expected = prices[col].iloc[t + h] / prices[col].iloc[t] - 1.0
            assert fwd[col].iloc[t] == pytest.approx(expected)

    @settings(max_examples=30, deadline=None)
    @given(h=st.integers(min_value=1, max_value=10))
    def test_the_unobservable_tail_is_left_empty(self, h):
        """The last h rows have no forward return yet. Filling them would put
        a fabricated observation at the end of the sample, where it carries
        the most weight over the result."""
        fwd = forward_returns(_panel(n_periods=40, n_names=3), h)
        assert fwd.iloc[-h:].isna().all().all()
        assert not fwd.iloc[:-h].isna().all().all()

    def test_a_zero_or_negative_horizon_is_refused(self):
        """Horizon 0 correlates a signal with its own bar. That is not a
        degenerate case to handle gracefully, it is the bug this file exists
        to prevent, so it raises."""
        for bad in (0, -1):
            with pytest.raises(ValueError):
                forward_returns(_panel(), bad)


class TestMissingDataIsNotFabricated:

    def test_names_missing_either_side_are_dropped_not_filled(self):
        sig = pd.Series([1.0, 2.0, 3.0, np.nan, 5.0, 6.0], index=range(6))
        fwd = pd.Series([0.1, 0.2, 0.3, 0.4, np.nan, 0.6], index=range(6))
        # 4 usable pairs -> below the 5-name floor -> None, not a number
        assert period_ic(sig, fwd) is None

    def test_too_thin_a_cross_section_returns_none(self):
        """A rank correlation over 3 names is noise with a decimal point."""
        sig = pd.Series([1.0, 2.0, 3.0])
        fwd = pd.Series([0.1, 0.2, 0.3])
        assert period_ic(sig, fwd) is None

    def test_a_constant_signal_has_no_ordering_to_score(self):
        sig = pd.Series([1.0] * 10)
        fwd = pd.Series(np.arange(10, dtype=float))
        assert period_ic(sig, fwd) is None

    def test_an_empty_ic_series_summarises_to_unknown_not_zero(self):
        s = summarise_ic(pd.Series([], dtype=float))
        assert s.n_periods == 0
        assert s.mean_ic is None and s.t_stat is None


class TestTheHonestyChecks:
    """A weak result and an unmeasured one are different, and the difference
    decides whether more data would help or the idea is dead."""

    def test_one_period_cannot_yield_a_t_statistic(self):
        assert ic_t_statistic(0.05, 0.1, 1) is None

    def test_zero_dispersion_yields_none_not_infinity(self):
        assert ic_t_statistic(0.05, 0.0, 100) is None

    def test_a_tiny_ic_needs_an_enormous_sample(self):
        """IC 0.005 against dispersion 0.10 is not a small edge — it needs
        ~1,600 periods before it is distinguishable from nothing."""
        n = periods_needed_for_significance(0.005, 0.10)
        assert n is not None and 1_500 < n < 1_700

    def test_a_strong_ic_needs_few_periods(self):
        n = periods_needed_for_significance(0.10, 0.10)
        assert n is not None and n < 500

    def test_a_zero_ic_never_becomes_significant(self):
        assert periods_needed_for_significance(0.0, 0.1) is None

    @settings(max_examples=200, deadline=None)
    @given(mean=st.floats(min_value=-1, max_value=1, allow_nan=False,
                          allow_infinity=False),
           std=st.floats(min_value=1e-6, max_value=2.0, allow_nan=False,
                         allow_infinity=False),
           n=st.integers(min_value=2, max_value=10_000))
    def test_the_t_statistic_grows_with_the_sample(self, mean, std, n):
        a = ic_t_statistic(mean, std, n)
        b = ic_t_statistic(mean, std, n * 4)
        assert a is not None and b is not None
        assert abs(b) >= abs(a) - 1e-9


class TestQuantilesAndTurnover:

    def test_an_oracle_signal_produces_a_monotone_spread(self):
        prices = _panel(n_periods=80, n_names=40, seed=5)
        oracle = prices.shift(-1) / prices - 1.0
        q = quantile_returns(oracle, prices, horizon=1, n_buckets=5)
        means = q.mean()
        assert list(means) == sorted(means), (
            f'a perfect signal produced a non-monotone spread: {list(means)}')
        assert means.iloc[-1] > means.iloc[0]

    def test_a_constant_signal_has_no_turnover(self):
        prices = _panel(n_periods=30, n_names=10)
        sig = pd.DataFrame(np.tile(np.arange(10.0), (30, 1)),
                           index=prices.index, columns=prices.columns)
        assert rank_turnover(sig).mean() == pytest.approx(0.0, abs=1e-12)

    def test_a_reshuffling_signal_has_high_turnover(self):
        """Costs scale with this, so a strong IC that reshuffles every period
        can still be unprofitable."""
        rng = np.random.default_rng(1)
        prices = _panel(n_periods=60, n_names=30)
        sig = pd.DataFrame(rng.normal(size=(60, 30)),
                           index=prices.index, columns=prices.columns)
        assert rank_turnover(sig).mean() > 0.2


class TestCounterexamplesFoundBySymbolicExecution:
    """Pinned regressions. Each was found by CrossHair, not by sampling, and
    each returned a plausible-looking value or raised where the contract said
    it could not."""

    def test_a_denormal_dispersion_does_not_divide_by_zero(self):
        """std/sqrt(n) underflows to exactly zero for a denormal std, so the
        division raised. Same class as the order_math denormal bug."""
        assert ic_t_statistic(6.4758e-319, 5e-324, 4) is None

    def test_a_non_finite_target_yields_none_not_nan(self):
        """NaN is neither a period count nor 'not measurable', and returning it
        silently violated the postcondition."""
        assert periods_needed_for_significance(-0.5, 0.5,
                                               target_t=float('nan')) is None
        assert periods_needed_for_significance(0.05, 0.1, target_t=0.0) is None

    def test_newey_west_refuses_a_degenerate_sample(self):
        assert newey_west_variance((1.0,), 0) is None
        assert newey_west_variance((), 3) is None

    def test_newey_west_on_independent_data_matches_the_naive_variance(self):
        """With lags=0 the correction reduces to the plain variance of the
        mean — the property that makes the h=1 rows comparable to the
        corrected ones."""
        vals = tuple(float(x) for x in [0.1, -0.2, 0.3, 0.05, -0.15, 0.2])
        n = len(vals)
        mean = sum(vals) / n
        naive = sum((v - mean) ** 2 for v in vals) / n / n
        assert newey_west_variance(vals, 0) == pytest.approx(naive)
