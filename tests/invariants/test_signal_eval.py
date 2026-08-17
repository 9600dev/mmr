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


class TestNeweyWestIsActuallyPinned:
    """The mutation run put this module at 56.1%, worst of all 18 measured, with
    survivors concentrated here - in the one function whose correction killed
    four false positives in a single day. Two tests were constraining the
    Bartlett weighting, the lag loop and the autocovariance sum between them,
    which is to say: nothing was.

    A statistic carrying this much inferential weight needs properties with
    known answers, not just degenerate-input guards.
    """

    def test_the_exact_value_on_a_hand_computed_case(self):
        """Arithmetic pinned end to end. Any mutation to the autocovariance sum,
        the Bartlett weight, or the /n normalisation moves this."""
        vals = (1.0, 2.0, 3.0, 4.0)
        # mean 2.5; deviations -1.5 -0.5 0.5 1.5
        # gamma0 = (2.25+0.25+0.25+2.25)/4 = 1.25
        # gamma1 = ((-1.5)(-0.5) + (-0.5)(0.5) + (0.5)(1.5))/4 = (0.75-0.25+0.75)/4 = 0.3125
        # Bartlett weight at lag 1 with lags=1: 1 - 1/2 = 0.5
        # total = 1.25 + 2*0.5*0.3125 = 1.5625;  /n = 0.390625
        assert newey_west_variance(vals, 1) == pytest.approx(0.390625)

    def test_lags_zero_is_the_naive_variance_of_the_mean(self):
        vals = (0.4, -0.2, 0.7, 0.1, -0.5, 0.3)
        n = len(vals)
        mean = sum(vals) / n
        naive = sum((v - mean) ** 2 for v in vals) / n / n
        assert newey_west_variance(vals, 0) == pytest.approx(naive)

    def test_positive_serial_correlation_INCREASES_the_variance(self):
        """The entire purpose of the correction. A mutant that makes it a no-op,
        or flips the sign of the covariance term, fails here - and that mutant
        would have let today's four false positives through."""
        rng = np.random.default_rng(0)
        e = rng.normal(size=400)
        # AR(1) with strong positive autocorrelation.
        x, prev = [], 0.0
        for v in e:
            prev = 0.8 * prev + v
            x.append(prev)
        vals = tuple(float(v) for v in x)
        naive = newey_west_variance(vals, 0)
        corrected = newey_west_variance(vals, 10)
        assert naive is not None and corrected is not None
        assert corrected > naive * 1.5, (
            f'correction barely moved on a strongly autocorrelated series: '
            f'{naive:.3e} -> {corrected:.3e}')

    def test_negative_serial_correlation_DECREASES_it(self):
        rng = np.random.default_rng(1)
        e = rng.normal(size=400)
        x, prev = [], 0.0
        for v in e:
            prev = -0.7 * prev + v
            x.append(prev)
        vals = tuple(float(v) for v in x)
        assert newey_west_variance(vals, 6) < newey_west_variance(vals, 0)

    @settings(max_examples=150, deadline=None)
    @given(k=st.floats(min_value=0.1, max_value=100.0, allow_nan=False,
                       allow_infinity=False),
           seed=st.integers(min_value=0, max_value=500))
    def test_scaling_the_series_scales_the_variance_by_k_squared(self, k, seed):
        """A variance is quadratic in scale. Kills mutants that swap a
        multiplication for an addition or drop a squaring."""
        rng = np.random.default_rng(seed)
        vals = tuple(float(v) for v in rng.normal(size=60))
        base = newey_west_variance(vals, 4)
        scaled = newey_west_variance(tuple(v * k for v in vals), 4)
        assert base is not None and scaled is not None
        assert scaled == pytest.approx(base * k * k, rel=1e-9)

    @settings(max_examples=150, deadline=None)
    @given(shift=st.floats(min_value=-1e3, max_value=1e3, allow_nan=False,
                           allow_infinity=False),
           seed=st.integers(min_value=0, max_value=500))
    def test_shifting_the_series_leaves_the_variance_alone(self, shift, seed):
        """Deviations are taken from the mean, so a constant offset cannot
        matter. Kills mutants that drop the mean subtraction."""
        rng = np.random.default_rng(seed)
        vals = tuple(float(v) for v in rng.normal(size=60))
        base = newey_west_variance(vals, 3)
        moved = newey_west_variance(tuple(v + shift for v in vals), 3)
        assert base is not None and moved is not None
        assert moved == pytest.approx(base, rel=1e-6, abs=1e-12)

    def test_a_constant_series_has_no_variance(self):
        assert newey_west_variance((5.0,) * 20, 3) is None

    def test_a_bandwidth_larger_than_the_sample_is_clamped(self):
        """Writing this test found a real defect. The Bartlett weight is
        1 - lag/(bandwidth+1), and the implementation used the REQUESTED
        bandwidth - so asking for 99 lags on four observations weighted the one
        computable lag at 0.99 instead of 0.75, over-correcting 25x. The
        bandwidth is now clamped to n-1 before the weights are formed, so an
        over-wide request behaves as the widest supportable one."""
        vals = (1.0, 2.0, 1.5, 2.5)
        assert newey_west_variance(vals, 99) == newey_west_variance(vals, 3)
        assert newey_west_variance(vals, 3) == newey_west_variance(vals, 3)

    def test_the_clamp_matters_for_a_realistic_case(self):
        """Not just a toy: the event study passes horizon-1 as the bandwidth,
        which can exceed the number of event dates on a thin threshold."""
        vals = tuple(float(v) for v in (0.1, -0.2, 0.15, 0.05, -0.1, 0.2))
        wide = newey_west_variance(vals, 62)      # h=63 on 6 dates
        supportable = newey_west_variance(vals, 5)
        assert wide == pytest.approx(supportable)

    def test_the_bartlett_weight_declines_with_lag(self):
        """Weight at lag l is 1 - l/(lags+1), so a series whose only
        correlation sits at a FAR lag gets less correction than the same
        correlation at lag 1. A mutant using a constant weight fails."""
        n = 200
        near = [0.0] * n
        far = [0.0] * n
        rng = np.random.default_rng(3)
        base = rng.normal(size=n)
        for i in range(n):
            near[i] = base[i] + (base[i - 1] if i >= 1 else 0.0)
            far[i] = base[i] + (base[i - 8] if i >= 8 else 0.0)
        a = newey_west_variance(tuple(float(v) for v in near), 10)
        b = newey_west_variance(tuple(float(v) for v in far), 10)
        assert a is not None and b is not None
        assert a > b, ('lag-1 correlation should attract more weight than '
                       'lag-8 correlation under Bartlett weighting')


class TestSummariseIcReportsTheNumbersItWasGiven:
    """Every field of the summary, pinned on a series computed by hand.

    The 2026-08-17 mutation run found summarise_ic was only ever exercised
    through its empty-series early return: a mutant that DELETED the t_stat
    argument from the constructor call survived, which is only possible if no
    test reaches the constructor at all. These pin each field to a value
    worked out by hand, so any arithmetic or wiring change is caught.
    """

    # values: [0.1, -0.1, 0.3, 0.1] -> mean 0.1, devs [0, -0.2, 0.2, 0],
    # ddof=1 var 0.08/3, std 0.16329932, naive t = 0.1/(std/2) = 1.22474487,
    # hit rate 3/4, IR = 0.1/std = 0.61237244.
    VALS = [0.1, -0.1, 0.3, 0.1]

    def test_the_h1_summary_by_hand(self):
        s = summarise_ic(pd.Series(self.VALS), horizon=1)
        assert s.n_periods == 4
        assert s.mean_ic == pytest.approx(0.1)
        assert s.std_ic == pytest.approx(math.sqrt(0.08 / 3))
        assert s.naive_t_stat == pytest.approx(1.2247448714)
        assert s.hit_rate == pytest.approx(0.75)
        assert s.ir == pytest.approx(0.6123724357)

    def test_at_horizon_one_the_t_stat_is_the_naive_one(self):
        """No overlap at h=1, so there is nothing to correct — the corrected
        and naive statistics must be identical, and present."""
        s = summarise_ic(pd.Series(self.VALS), horizon=1)
        assert s.t_stat is not None
        assert s.t_stat == s.naive_t_stat

    def test_above_horizon_one_the_correction_uses_h_minus_1_lags(self):
        """The NW bandwidth is horizon-1. On this series lags=2 and lags=3
        give different variances, so a mutant passing `horizon` instead of
        `horizon - 1` produces a different t and is caught."""
        h = 3
        s = summarise_ic(pd.Series(self.VALS), horizon=h)
        var = newey_west_variance(tuple(self.VALS), h - 1)
        assert s.t_stat == pytest.approx(0.1 / math.sqrt(var))
        wrong = newey_west_variance(tuple(self.VALS), h)
        assert var != wrong, 'the two bandwidths must be distinguishable here'
        assert s.t_stat != pytest.approx(0.1 / math.sqrt(wrong))
        assert s.naive_t_stat == pytest.approx(1.2247448714)
        assert s.t_stat != s.naive_t_stat

    def test_two_periods_cannot_support_a_newey_west_correction(self):
        """NW needs n > 2; below that the naive statistic must be reported
        unchanged, not a correction computed from one lag of one pair."""
        s = summarise_ic(pd.Series([0.1, 0.3]), horizon=5)
        assert s.naive_t_stat == pytest.approx(2.0)   # mean .2 / (std .1414/sqrt 2)
        assert s.t_stat == s.naive_t_stat

    def test_nans_are_dropped_not_counted(self):
        s = summarise_ic(pd.Series([0.1, np.nan, -0.1, 0.3, np.nan, 0.1]),
                         horizon=1)
        assert s.n_periods == 4
        assert s.mean_ic == pytest.approx(0.1)

    def test_a_single_period_reports_its_mean_and_nothing_it_cannot_know(self):
        s = summarise_ic(pd.Series([0.2]), horizon=1)
        assert s.n_periods == 1
        assert s.mean_ic == pytest.approx(0.2)
        assert s.std_ic is None
        assert s.naive_t_stat is None
        assert s.t_stat is None
        assert s.ir is None
        assert s.hit_rate == pytest.approx(1.0)

    def test_hit_rate_counts_strictly_positive_periods(self):
        s = summarise_ic(pd.Series([0.0, 0.1, -0.1, 0.2]), horizon=1)
        assert s.hit_rate == pytest.approx(0.5)   # 0.0 is not a hit


class TestNeweyWestByHand:
    """The variance formula pinned on values small enough to work by hand.

    values (1,2,3,4), lags=1: mean 2.5, dev (-1.5,-.5,.5,1.5),
    gamma0 = 5/4 = 1.25, lag-1 cov = (0.75 - 0.25 + 0.75)/4 = 0.3125,
    Bartlett weight 1 - 1/2 = 0.5, total = 1.25 + 2(0.5)(0.3125) = 1.5625,
    variance of the mean = 1.5625/4 = 0.390625.
    """

    def test_the_worked_example(self):
        assert newey_west_variance((1.0, 2.0, 3.0, 4.0), 1) == pytest.approx(0.390625)

    def test_zero_lags_is_the_plain_variance_of_the_mean(self):
        # gamma0/n with population variance: 1.25/4
        assert newey_west_variance((1.0, 2.0, 3.0, 4.0), 0) == pytest.approx(0.3125)

    def test_the_bandwidth_is_clamped_to_the_sample(self):
        """Asking for 99 lags on four observations must equal asking for the
        3 the sample can actually support — the weights come from the clamped
        value (the 25x over-correction bug, pinned)."""
        vals = (0.1, -0.1, 0.3, 0.1)
        assert newey_west_variance(vals, 99) == newey_west_variance(vals, 3)


class TestPeriodIcScoresRanksNotLevels:
    """Spearman by default, exact at the boundary of enough names."""

    def test_five_monotone_names_score_exactly_one(self):
        """Five names is the minimum the docstring promises to score — and a
        perfectly monotone pair is exactly 1.0 under the DEFAULT method."""
        sig = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
        fwd = pd.Series([0.01, 0.02, 0.03, 0.04, 0.05])
        assert period_ic(sig, fwd) == pytest.approx(1.0)

    def test_four_names_are_too_thin(self):
        sig = pd.Series([1.0, 2.0, 3.0, 4.0])
        fwd = pd.Series([0.01, 0.02, 0.03, 0.04])
        assert period_ic(sig, fwd) is None

    def test_the_default_method_is_rank_not_level(self):
        """An outlier that wrecks the linear correlation leaves the rank
        correlation at exactly 1.0. A mutant that swaps the default to
        pearson (or garbles the method string) fails this."""
        sig = pd.Series([1.0, 2.0, 3.0, 4.0, 100.0])
        fwd = pd.Series([0.01, 0.02, 0.03, 0.04, 0.05])
        assert period_ic(sig, fwd) == pytest.approx(1.0)

    def test_a_reversed_ordering_scores_minus_one(self):
        sig = pd.Series([5.0, 4.0, 3.0, 2.0, 1.0])
        fwd = pd.Series([0.01, 0.02, 0.03, 0.04, 0.05])
        assert period_ic(sig, fwd) == pytest.approx(-1.0)


class TestQuantileReturnsByHand:
    """Bucket means pinned exactly on ten names in five buckets of two."""

    def _frames(self, n_names=10, n_periods=3):
        idx = pd.date_range('2020-01-01', periods=n_periods, freq='D')
        cols = list(range(n_names))
        sig = pd.DataFrame([[float(i + 1) for i in cols]] * n_periods,
                           index=idx, columns=cols)
        # price grows by (i+1)% per step for name i -> forward return is
        # exactly (i+1)/100 every period, monotone in the signal.
        prices = pd.DataFrame(
            [[(1.0 + (i + 1) / 100.0) ** t for i in cols] for t in range(n_periods)],
            index=idx, columns=cols)
        return sig, prices

    def test_bucket_means_are_exact(self):
        sig, prices = self._frames()
        q = quantile_returns(sig, prices, horizon=1, n_buckets=5)
        assert list(q.columns) == [0, 1, 2, 3, 4]
        # names (1,2)% -> 0.015, (3,4)% -> 0.035 ... (9,10)% -> 0.095
        for b, want in enumerate([0.015, 0.035, 0.055, 0.075, 0.095]):
            assert q.iloc[0, b] == pytest.approx(want)

    def test_exactly_two_per_bucket_is_enough(self):
        """len == n_buckets * 2 must be scored; the guard is strictly-less.
        Ten names, five buckets: rows must exist."""
        sig, prices = self._frames(n_names=10)
        q = quantile_returns(sig, prices, horizon=1, n_buckets=5)
        assert len(q) > 0

    def test_nine_names_in_five_buckets_is_too_thin(self):
        sig, prices = self._frames(n_names=9)
        q = quantile_returns(sig, prices, horizon=1, n_buckets=5)
        assert len(q) == 0

    def test_rows_are_periods(self):
        sig, prices = self._frames(n_periods=4)
        q = quantile_returns(sig, prices, horizon=1, n_buckets=5)
        # 4 periods, the last has no forward return -> 3 scored rows
        assert len(q) == 3
        assert all(ts in sig.index for ts in q.index)


class TestRankTurnoverByHand:
    """Turnover pinned exactly, in percentile-rank units.

    Three names; between the two periods the top two swap and the third
    holds. Percentile ranks (1/3, 2/3, 1) -> (2/3, 1/3, 1): deltas
    (1/3, 1/3, 0), mean exactly 2/9. Integer ranks would give 2/3 — so this
    also pins that ranks are PERCENTILE ranks, keeping turnover comparable
    across cross-sections of different width.
    """

    def test_the_worked_example(self):
        idx = pd.date_range('2020-01-01', periods=2, freq='D')
        sig = pd.DataFrame([[1.0, 2.0, 3.0],
                            [2.0, 1.0, 3.0]], index=idx, columns=['a', 'b', 'c'])
        turn = rank_turnover(sig)
        assert len(turn) == 1                      # first period has no prior
        assert turn.index[0] == idx[1]
        assert turn.iloc[0] == pytest.approx(2.0 / 9.0)

    def test_a_full_reversal_of_two_names(self):
        idx = pd.date_range('2020-01-01', periods=3, freq='D')
        sig = pd.DataFrame([[1.0, 2.0], [2.0, 1.0], [1.0, 2.0]],
                           index=idx, columns=['a', 'b'])
        turn = rank_turnover(sig)
        # pct ranks swing between (0.5, 1.0) and (1.0, 0.5): mean |delta| 0.5
        assert list(turn) == pytest.approx([0.5, 0.5])


class TestScalarHelpersByHand:
    def test_t_statistic_worked_example(self):
        # se = 0.2/sqrt(25) = 0.04 -> t = 2.5
        assert ic_t_statistic(0.1, 0.2, 25) == pytest.approx(2.5)

    def test_periods_needed_worked_example(self):
        # (2.0 * 0.2 / 0.1)^2 = 16 at the default target of t=2
        assert periods_needed_for_significance(0.1, 0.2) == pytest.approx(16.0)

    def test_periods_needed_scales_with_the_target(self):
        assert periods_needed_for_significance(0.1, 0.2, target_t=3.0) == pytest.approx(36.0)

    def test_forward_return_value_is_the_simple_return(self):
        idx = pd.date_range('2020-01-01', periods=3, freq='D')
        prices = pd.DataFrame({1000: [100.0, 110.0, 99.0]}, index=idx)
        fwd = forward_returns(prices, 1)
        assert fwd.iloc[0, 0] == pytest.approx(0.10)
        assert fwd.iloc[1, 0] == pytest.approx(-0.10)


class TestInformationCoefficientAlignsItsInputs:
    def test_only_the_common_names_and_periods_are_scored(self):
        prices = _panel(n_periods=12, n_names=8)
        fwd = forward_returns(prices, 1)
        sig = fwd.copy()
        # extra name and extra period on the signal side must be ignored
        sig['NOT_A_PRICE'] = 1.0
        sig.loc[pd.Timestamp('2030-01-01')] = 1.0
        ic = information_coefficient(sig, prices, horizon=1)
        assert len(ic) == 11              # 12 periods, last has no forward return
        assert all(v == pytest.approx(1.0) for v in ic)


class TestSecondMutationPass:
    """Kills for the 2026-08-17 second-pass survivors — each test names the
    code change it exists to catch. The survivors NOT killed here are
    documented equivalents (Bartlett weight zeroes the extra lag; the
    denormal/se guard re-rejects what a loosened first guard admits; qcut on
    rank(method='first') cannot raise, making its except-path unreachable).
    """

    def test_buckets_are_formed_by_the_signal_not_the_return(self):
        """A mutant bucketed by the FORWARD RETURN itself — undetectable when
        the signal and return orderings agree, so here they disagree."""
        idx = pd.date_range('2020-01-01', periods=2, freq='D')
        cols = list(range(10))
        # signal ranks names 1..10; returns are anti-monotone in the signal
        sig = pd.DataFrame([[float(i + 1) for i in cols]] * 2,
                           index=idx, columns=cols)
        prices = pd.DataFrame(
            [[1.0] * 10, [1.0 + (10 - i) / 100.0 for i in cols]],
            index=idx, columns=cols)
        q = quantile_returns(sig, prices, horizon=1, n_buckets=5)
        # bucket 0 = lowest SIGNAL = names 0,1 = returns .10,.09 -> .095
        assert q.iloc[0, 0] == pytest.approx(0.095)
        assert q.iloc[0, 4] == pytest.approx(0.015)

    def test_default_horizon_is_one_everywhere(self):
        idx = pd.date_range('2020-01-01', periods=3, freq='D')
        prices = pd.DataFrame({1000: [100.0, 110.0, 99.0]}, index=idx)
        fwd = forward_returns(prices)                      # no horizon arg
        assert fwd.iloc[0, 0] == pytest.approx(0.10)
        assert np.isnan(fwd.iloc[2, 0])                    # exactly one NaN tail row
        assert not np.isnan(fwd.iloc[1, 0])

    def test_default_bucket_count_is_five(self):
        idx = pd.date_range('2020-01-01', periods=2, freq='D')
        cols = list(range(10))
        sig = pd.DataFrame([[float(i) for i in cols]] * 2, index=idx, columns=cols)
        prices = pd.DataFrame([[1.0] * 10, [1.0 + i / 100.0 for i in cols]],
                              index=idx, columns=cols)
        q = quantile_returns(sig, prices, horizon=1)       # no n_buckets arg
        assert list(q.columns) == [0, 1, 2, 3, 4]

    def test_information_coefficient_honors_its_horizon(self):
        """Oracle built at h=2 scores 1.0 only if the forward frame really is
        h=2 — a default-horizon fallback inside the loop breaks this."""
        prices = _panel(n_periods=12, n_names=8)
        sig = forward_returns(prices, 2)
        ic = information_coefficient(sig, prices, horizon=2)
        assert len(ic) == 10
        assert all(v == pytest.approx(1.0) for v in ic)
        # and the default really is h=1: the h=1 oracle scores 1.0 with no arg
        ic1 = information_coefficient(forward_returns(prices, 1), prices)
        assert all(v == pytest.approx(1.0) for v in ic1)

    def test_information_coefficient_passes_the_method_through(self):
        """With an outlier, spearman scores 1.0 and pearson must NOT — a
        mutant that drops the method= pass-through fails the pearson half."""
        idx = pd.date_range('2020-01-01', periods=3, freq='D')
        cols = list(range(5))
        prices = pd.DataFrame(
            [[1.0] * 5,
             [1.01, 1.02, 1.03, 1.04, 2.0],   # outlier forward return
             [1.0] * 5],
            index=idx, columns=cols)
        sig_row = [1.0, 2.0, 3.0, 4.0, 5.0]
        sig = pd.DataFrame([sig_row] * 3, index=idx, columns=cols)
        sp = information_coefficient(sig, prices, horizon=1, method='spearman')
        pe = information_coefficient(sig, prices, horizon=1, method='pearson')
        assert sp.iloc[0] == pytest.approx(1.0)
        assert pe.iloc[0] < 0.999

    def test_ic_series_is_float_dtyped_with_unscoreable_periods_dropped(self):
        prices = _panel(n_periods=10, n_names=8)
        sig = forward_returns(prices, 1)
        sig.iloc[3] = np.nan                    # one period unscoreable
        ic = information_coefficient(sig, prices, horizon=1)
        assert ic.dtype == np.float64
        assert prices.index[3] not in ic.index

    def test_newey_west_engages_at_horizon_two(self):
        """h=2 is the FIRST overlapping horizon; a mutant moving the branch to
        h>2 leaves the naive statistic uncorrected exactly there."""
        vals = [0.1, -0.1, 0.3, 0.1]
        s = summarise_ic(pd.Series(vals), horizon=2)
        var = newey_west_variance(tuple(vals), 1)
        assert s.t_stat == pytest.approx(0.1 / math.sqrt(var))
        assert s.t_stat != s.naive_t_stat

    def test_newey_west_engages_at_three_periods(self):
        """n=3 is the smallest sample the correction accepts."""
        vals = [0.1, -0.1, 0.3]
        s = summarise_ic(pd.Series(vals), horizon=2)
        var = newey_west_variance(tuple(vals), 1)
        assert s.t_stat == pytest.approx(s.mean_ic / math.sqrt(var))
        assert s.t_stat != s.naive_t_stat

    def test_newey_west_computes_at_two_observations(self):
        """n=2 supports gamma0: (1,2) -> mean 1.5, gamma0 0.25, var 0.125."""
        assert newey_west_variance((1.0, 2.0), 0) == pytest.approx(0.125)

    def test_a_nan_mean_yields_none_not_nan(self):
        assert ic_t_statistic(float('nan'), 0.2, 25) is None

    def test_periods_needed_accepts_any_positive_target(self):
        # target 0.5: (0.5 * 0.2 / 0.1)^2 = 1.0
        assert periods_needed_for_significance(0.1, 0.2, target_t=0.5) == pytest.approx(1.0)

    def test_periods_needed_zero_dispersion_is_none_not_zero(self):
        assert periods_needed_for_significance(0.1, 0.0) is None

    def test_two_distinct_values_are_an_ordering(self):
        """nunique == 2 is scoreable on both sides; the guard is strictly
        fewer-than-two."""
        fwd = pd.Series([0.01, 0.02, 0.03, 0.04, 0.05])
        assert period_ic(pd.Series([1.0, 1.0, 1.0, 2.0, 2.0]), fwd) is not None
        sig = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
        assert period_ic(sig, pd.Series([0.01, 0.01, 0.01, 0.02, 0.02])) is not None

    def test_one_thin_period_does_not_stop_the_scan(self):
        """The too-thin guard must CONTINUE, not break: a thin early period
        followed by full ones still yields the later rows."""
        idx = pd.date_range('2020-01-01', periods=3, freq='D')
        cols = list(range(10))
        sig = pd.DataFrame([[float(i) for i in cols]] * 3, index=idx, columns=cols)
        sig.iloc[0, :7] = np.nan                # period 0: 3 names < 10 needed
        prices = pd.DataFrame(
            [[1.0] * 10, [1.0 + i / 100.0 for i in cols], [1.0] * 10],
            index=idx, columns=cols)
        q = quantile_returns(sig, prices, horizon=1, n_buckets=5)
        assert idx[1] in q.index                # the later period was scored

    def test_turnover_is_per_period_not_pooled(self):
        """Period 2 swaps the top pair (2/9); period 3 changes nothing (0).
        A pooled mean would smear both to 1/9."""
        idx = pd.date_range('2020-01-01', periods=3, freq='D')
        sig = pd.DataFrame([[1.0, 2.0, 3.0],
                            [2.0, 1.0, 3.0],
                            [2.0, 1.0, 3.0]], index=idx, columns=['a', 'b', 'c'])
        turn = rank_turnover(sig)
        assert turn.dtype == np.float64
        assert list(turn) == pytest.approx([2.0 / 9.0, 0.0])

    def test_a_refused_horizon_names_the_problem(self):
        with pytest.raises(ValueError, match='horizon'):
            forward_returns(_panel(n_periods=5, n_names=3), 0)

    def test_summarise_accepts_an_object_dtyped_series(self):
        """The dtype=float coercion is load-bearing: an object series from a
        ragged upstream join must still summarise numerically."""
        s = summarise_ic(pd.Series([0.1, -0.1, 0.3, 0.1], dtype=object), horizon=1)
        assert s.mean_ic == pytest.approx(0.1)
        assert s.naive_t_stat == pytest.approx(1.2247448714)


class TestThirdMutationPass:
    """Default-argument kills the second pass missed. The 12 survivors beyond
    these are documented equivalents — see the mutation ledger in
    scripts/run_mutation.sh for the classification rationale."""

    def test_quantile_returns_defaults_to_horizon_one(self):
        idx = pd.date_range('2020-01-01', periods=3, freq='D')
        cols = list(range(10))
        sig = pd.DataFrame([[float(i) for i in cols]] * 3, index=idx, columns=cols)
        prices = pd.DataFrame(
            [[(1.0 + (i + 1) / 100.0) ** t for i in cols] for t in range(3)],
            index=idx, columns=cols)
        q = quantile_returns(sig, prices)          # no horizon arg
        assert len(q) == 2                          # h=1 scores all but the last
        assert q.iloc[0, 0] == pytest.approx(0.015)

    def test_quantile_returns_honors_horizon_two(self):
        """The h=2 bucket mean is (1.01^2-1 + 1.02^2-1)/2 = 0.03025 — a
        default-horizon fallback inside the function reports 0.015 instead."""
        idx = pd.date_range('2020-01-01', periods=3, freq='D')
        cols = list(range(10))
        sig = pd.DataFrame([[float(i) for i in cols]] * 3, index=idx, columns=cols)
        prices = pd.DataFrame(
            [[(1.0 + (i + 1) / 100.0) ** t for i in cols] for t in range(3)],
            index=idx, columns=cols)
        q = quantile_returns(sig, prices, horizon=2, n_buckets=5)
        assert len(q) == 1
        assert q.iloc[0, 0] == pytest.approx((1.01 ** 2 - 1 + 1.02 ** 2 - 1) / 2)

    def test_summarise_ic_defaults_to_horizon_one(self):
        """At the default horizon there is no overlap, so no correction: the
        t statistic must BE the naive one."""
        s = summarise_ic(pd.Series([0.1, -0.1, 0.3, 0.1]))   # no horizon arg
        assert s.t_stat == s.naive_t_stat
        assert s.t_stat == pytest.approx(1.2247448714)
