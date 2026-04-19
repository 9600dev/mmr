"""Tests for backtest statistical-confidence computations.

These lock the behaviour of the five tests documented in
``trader/simulation/backtest_stats.py``. The exact numeric values aren't
portable across scipy versions for every edge case, so we prefer tests
about *relative behaviour* (e.g. PSR monotonic in n, tighter CI with more
data) over literal-value assertions.
"""

import math

import numpy as np
import pytest

from trader.simulation.backtest_stats import (
    BacktestStats,
    bootstrap_ci,
    compute_all,
    longest_losing_streak,
    mc_losing_streak_p95,
    probabilistic_sharpe,
    round_trip_pnls,
    t_test_mean_zero,
)


# ---------------------------------------------------------------------------
# round_trip_pnls — FIFO fill pairing
# ---------------------------------------------------------------------------

class TestRoundTripPnls:

    def test_simple_winner(self):
        """100 shares bought at $100, sold at $105 → $500 P&L, minus
        commissions."""
        trades = [
            {'timestamp': '2026-01-01', 'conid': 1, 'action': 'BUY',
             'quantity': 100, 'price': 100.0, 'commission': 1.0},
            {'timestamp': '2026-01-02', 'conid': 1, 'action': 'SELL',
             'quantity': 100, 'price': 105.0, 'commission': 1.0},
        ]
        pnls = round_trip_pnls(trades)
        assert len(pnls) == 1
        # $500 gross - $2 commission = $498
        assert pnls[0] == pytest.approx(498.0)

    def test_fifo_partial_close(self):
        """BUY 100 @ 100, BUY 100 @ 110, SELL 150 @ 120 → closes the
        first 100 (pnl 100*$20=$2000) and 50 of the second (50*$10=$500).
        Remaining 50 from the second BUY stays open and is not counted."""
        trades = [
            {'timestamp': '2026-01-01', 'conid': 1, 'action': 'BUY',
             'quantity': 100, 'price': 100.0, 'commission': 0.0},
            {'timestamp': '2026-01-02', 'conid': 1, 'action': 'BUY',
             'quantity': 100, 'price': 110.0, 'commission': 0.0},
            {'timestamp': '2026-01-03', 'conid': 1, 'action': 'SELL',
             'quantity': 150, 'price': 120.0, 'commission': 0.0},
        ]
        pnls = round_trip_pnls(trades)
        assert len(pnls) == 2
        assert pnls[0] == pytest.approx(2000.0)
        assert pnls[1] == pytest.approx(500.0)

    def test_unmatched_buy_ignored(self):
        """Open position at end of backtest is not a round-trip."""
        trades = [
            {'timestamp': '2026-01-01', 'conid': 1, 'action': 'BUY',
             'quantity': 100, 'price': 100.0, 'commission': 0.0},
        ]
        assert round_trip_pnls(trades) == []

    def test_sell_without_prior_buy_ignored(self):
        """No short-sell lots — a SELL without an opening BUY is discarded
        (the backtester already rejects it at fill time)."""
        trades = [
            {'timestamp': '2026-01-01', 'conid': 1, 'action': 'SELL',
             'quantity': 50, 'price': 100.0, 'commission': 0.0},
        ]
        assert round_trip_pnls(trades) == []

    def test_accepts_action_enum_format(self):
        """trades_json may contain either 'BUY'/'SELL' or 'Action.BUY'/
        'Action.SELL' depending on how the enum str()'d when written."""
        trades = [
            {'timestamp': '2026-01-01', 'conid': 1, 'action': 'Action.BUY',
             'quantity': 10, 'price': 50.0, 'commission': 0.0},
            {'timestamp': '2026-01-02', 'conid': 1, 'action': 'Action.SELL',
             'quantity': 10, 'price': 55.0, 'commission': 0.0},
        ]
        assert round_trip_pnls(trades) == [pytest.approx(50.0)]

    def test_multiple_conids_independent_fifo(self):
        trades = [
            {'timestamp': '2026-01-01', 'conid': 1, 'action': 'BUY',
             'quantity': 10, 'price': 100.0, 'commission': 0.0},
            {'timestamp': '2026-01-02', 'conid': 2, 'action': 'BUY',
             'quantity': 10, 'price': 200.0, 'commission': 0.0},
            {'timestamp': '2026-01-03', 'conid': 1, 'action': 'SELL',
             'quantity': 10, 'price': 110.0, 'commission': 0.0},
            {'timestamp': '2026-01-04', 'conid': 2, 'action': 'SELL',
             'quantity': 10, 'price': 195.0, 'commission': 0.0},
        ]
        pnls = sorted(round_trip_pnls(trades))
        assert pnls == [pytest.approx(-50.0), pytest.approx(100.0)]


# ---------------------------------------------------------------------------
# Probabilistic Sharpe
# ---------------------------------------------------------------------------

class TestPSR:

    def test_returns_none_below_min_sample(self):
        assert probabilistic_sharpe(np.array([0.01, 0.02])) is None

    def test_half_on_zero_variance(self):
        """All-same returns → Sharpe undefined → report 0.5 (no signal),
        not NaN or a spurious 1.0."""
        assert probabilistic_sharpe(np.zeros(20)) == 0.5

    def test_monotonic_in_sample_size(self):
        """Draws from the same positive-edge process should produce a
        higher PSR at larger n (more evidence → tighter confidence in a
        positive mean)."""
        rng = np.random.default_rng(42)
        r_small = rng.normal(0.002, 0.005, size=30)
        rng = np.random.default_rng(42)  # reset so "same" means same draws too
        r_large = rng.normal(0.002, 0.005, size=600)
        assert probabilistic_sharpe(r_large) > probabilistic_sharpe(r_small)

    def test_near_one_for_clear_positive_edge(self):
        rng = np.random.default_rng(1)
        # Strong mean, low variance, many samples → almost certainly real
        r = rng.normal(0.002, 0.005, size=500)
        assert probabilistic_sharpe(r) > 0.95

    def test_near_zero_for_clear_negative_edge(self):
        rng = np.random.default_rng(2)
        r = rng.normal(-0.002, 0.005, size=500)
        assert probabilistic_sharpe(r) < 0.05

    def test_noisy_small_sample_is_ambiguous(self):
        """A 30-bar run with tiny mean and large variance should not
        produce strong confidence either way."""
        rng = np.random.default_rng(3)
        r = rng.normal(0.0003, 0.02, size=30)
        psr = probabilistic_sharpe(r)
        assert 0.3 < psr < 0.8


# ---------------------------------------------------------------------------
# t-test
# ---------------------------------------------------------------------------

class TestTTest:

    def test_single_value_returns_none(self):
        assert t_test_mean_zero(np.array([0.05])) == (None, None)

    def test_zero_variance_returns_none(self):
        assert t_test_mean_zero(np.ones(50)) == (None, None)

    def test_clearly_positive_mean_has_tiny_p(self):
        rng = np.random.default_rng(0)
        pnls = rng.normal(10.0, 5.0, size=200)  # mean 10, std 5, 200 trades
        _, p = t_test_mean_zero(pnls)
        assert p < 1e-6

    def test_noise_around_zero_has_large_p(self):
        rng = np.random.default_rng(1)
        pnls = rng.normal(0.0, 5.0, size=50)
        _, p = t_test_mean_zero(pnls)
        assert p > 0.05


# ---------------------------------------------------------------------------
# Bootstrap CI
# ---------------------------------------------------------------------------

class TestBootstrap:

    def test_returns_none_below_min_sample(self):
        assert bootstrap_ci(np.arange(5), np.mean) is None

    def test_ci_contains_point_estimate_with_high_probability(self):
        """The 95% CI on a mean should contain the true mean in ~95% of
        samples from the generating distribution. Here we check that the
        CI for a known-mean sample actually contains that mean."""
        rng = np.random.default_rng(0)
        vals = rng.normal(3.0, 2.0, size=200)
        ci = bootstrap_ci(vals, lambda s: np.mean(s, axis=-1), n_boot=2000)
        assert ci is not None
        lo, hi = ci
        # CI should bracket the true mean (3.0) with high probability
        assert lo < 3.0 < hi

    def test_ci_tightens_with_sample_size(self):
        """More data → narrower CI."""
        rng = np.random.default_rng(0)
        small = rng.normal(1.0, 1.0, size=20)
        large = rng.normal(1.0, 1.0, size=500)
        ci_small = bootstrap_ci(small, lambda s: np.mean(s, axis=-1), n_boot=2000)
        ci_large = bootstrap_ci(large, lambda s: np.mean(s, axis=-1), n_boot=2000)
        assert (ci_large[1] - ci_large[0]) < (ci_small[1] - ci_small[0])

    def test_non_vectorised_fallback(self):
        """Bootstrap accepts a scalar-only statistic via row-by-row fallback
        when the vectorised call returns the wrong shape. The fallback
        must produce a well-formed CI (lo <= hi) and not crash."""
        rng = np.random.default_rng(0)
        vals = rng.normal(0.0, 1.0, size=50)
        # Scalar-returning stat — vectorised path returns a 0-d result,
        # shape check fails, row-by-row fallback engages.
        ci = bootstrap_ci(vals, lambda s: float(np.mean(s)), n_boot=500)
        assert ci is not None
        assert ci[0] <= ci[1]


# ---------------------------------------------------------------------------
# Losing streak & MC
# ---------------------------------------------------------------------------

class TestLosingStreak:

    def test_empty(self):
        assert longest_losing_streak([]) == 0

    def test_all_winners(self):
        assert longest_losing_streak([1.0, 2.0, 3.0]) == 0

    def test_all_losers(self):
        assert longest_losing_streak([-1.0, -2.0, -3.0]) == 3

    def test_interrupted_streak(self):
        """Run of 3 losses, then a winner, then 2 losses — longest is 3."""
        assert longest_losing_streak([-1, -1, -1, 1, -1, -1]) == 3

    def test_zero_breaks_streak(self):
        """Zero is not strictly negative → breaks the streak."""
        assert longest_losing_streak([-1, -1, 0, -1]) == 2

    def test_mc_small_sample_none(self):
        assert mc_losing_streak_p95(np.array([-1.0, 1.0])) is None

    def test_mc_produces_reasonable_estimate(self):
        """50/50 win/loss split → MC 95th percentile streak is a handful
        (log-scale with n), not 0 and not all-n."""
        pnls = np.array([1, -1] * 50, dtype=float)
        p95 = mc_losing_streak_p95(pnls, n_sim=500)
        assert 3 < p95 < 15


# ---------------------------------------------------------------------------
# compute_all — orchestration
# ---------------------------------------------------------------------------

class TestComputeAll:

    def test_empty_inputs_gives_all_nones(self):
        stats = compute_all(None, None)
        assert stats.n_trades == 0
        assert stats.n_bar_returns == 0
        assert stats.psr is None
        assert stats.t_stat is None
        assert stats.return_ci_lo is None
        assert stats.sharpe_ci_lo is None
        assert stats.losing_streak_actual is None

    def test_full_populates_every_field(self):
        """Given enough trades and bar returns, every optional field is
        populated (no Nones in the stats bundle)."""
        rng = np.random.default_rng(0)

        # 200 round-trips, each a small winner on average. Use strictly
        # increasing ISO timestamps so FIFO pairing preserves ordering.
        trades = []
        for i in range(200):
            pnl_per_share = rng.normal(0.1, 1.0)
            entry = 100.0
            exit_px = entry + pnl_per_share
            trades.append({'timestamp': f'2026-01-01T{i:06d}', 'conid': 1,
                           'action': 'BUY', 'quantity': 10, 'price': entry,
                           'commission': 0.0})
            trades.append({'timestamp': f'2026-01-01T{i:06d}.5', 'conid': 1,
                           'action': 'SELL', 'quantity': 10, 'price': exit_px,
                           'commission': 0.0})

        bar_returns = rng.normal(0.0005, 0.01, size=500)
        stats = compute_all(trades, bar_returns, bars_per_year=252.0)

        assert stats.n_trades == 200
        assert stats.n_bar_returns == 500
        assert stats.psr is not None
        assert stats.t_stat is not None
        assert stats.p_value is not None
        assert stats.return_ci_lo is not None
        assert stats.return_ci_hi is not None
        assert stats.sharpe_ci_lo is not None
        assert stats.sharpe_ci_hi is not None
        assert stats.pnl_skew is not None
        assert stats.pnl_excess_kurtosis is not None
        assert stats.losing_streak_actual is not None
        assert stats.losing_streak_mc_95 is not None

        # Sanity: CI ordered
        assert stats.return_ci_lo <= stats.return_ci_hi
        assert stats.sharpe_ci_lo <= stats.sharpe_ci_hi

    def test_few_trades_degrades_gracefully(self):
        """3 trades → per-trade stats mostly None, but distribution shape
        (skew/kurt, needs >= 3) should still be defined."""
        trades = [
            {'timestamp': '2026-01-01', 'conid': 1, 'action': 'BUY',
             'quantity': 1, 'price': 100.0, 'commission': 0.0},
            {'timestamp': '2026-01-02', 'conid': 1, 'action': 'SELL',
             'quantity': 1, 'price': 102.0, 'commission': 0.0},
            {'timestamp': '2026-01-03', 'conid': 1, 'action': 'BUY',
             'quantity': 1, 'price': 101.0, 'commission': 0.0},
            {'timestamp': '2026-01-04', 'conid': 1, 'action': 'SELL',
             'quantity': 1, 'price': 100.5, 'commission': 0.0},
            {'timestamp': '2026-01-05', 'conid': 1, 'action': 'BUY',
             'quantity': 1, 'price': 99.0, 'commission': 0.0},
            {'timestamp': '2026-01-06', 'conid': 1, 'action': 'SELL',
             'quantity': 1, 'price': 100.0, 'commission': 0.0},
        ]
        stats = compute_all(trades, None)
        assert stats.n_trades == 3
        assert stats.pnl_skew is not None
        assert stats.pnl_excess_kurtosis is not None
        # Bootstrap CI needs >= 10 → None at 3 trades
        assert stats.return_ci_lo is None
