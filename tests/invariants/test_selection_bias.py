"""SPEC: what a selection-bias correction must say, including when it hurts.

Every other statistic here answers "is this run real?". These two know the run
was CHOSEN, and the properties below exist because a correction for selection
bias has an obvious failure mode: being too polite. A deflation that never
actually deflates, or an overfitting probability that never reaches 0.5, would
pass every smoke test and rubber-stamp the whole roster.

So the load-bearing properties are the ones with a KNOWN right answer:

  * a family of pure noise must produce PBO ≈ 0.5. Not "high", not "elevated"
    — one half, because the in-sample winner's out-of-sample rank is uniform
    when nothing has any edge. If this test passes only because the estimator
    is biased upward, it would also flag genuine edges, so the noise case is
    checked from BOTH sides.
  * a family containing one genuinely superior trial must produce a LOW PBO.
    An estimator that always says "overfit" is as useless as one that never
    does, and would get switched off the first time it condemned a real edge.

Together those pin the estimator between the two ways it can be worthless.
"""

from __future__ import annotations

import math

import numpy as np
import pytest

from hypothesis import assume, given, settings, strategies as st

from trader.simulation.backtest_stats import probabilistic_sharpe
from trader.simulation.selection_bias import (
    align_equity_curves, deflated_sharpe, expected_max_sharpe,
    infer_periods_per_year, pbo_cscv)


_SETTINGS = settings(max_examples=200, deadline=None)

_SHARPE = st.floats(min_value=-5.0, max_value=5.0,
                    allow_nan=False, allow_infinity=False)


def _noise_matrix(rng, T: int, N: int) -> np.ndarray:
    return rng.normal(0.0, 0.01, size=(T, N))


class TestDeflationCanOnlyReduceConfidence:
    """The direction property. Deflating against a harder benchmark must never
    make a result look BETTER — if it could, the correction would be a way to
    launder a weak backtest rather than to check one."""

    @_SETTINGS
    @given(sharpes=st.lists(_SHARPE, min_size=2, max_size=40),
           seed=st.integers(min_value=0, max_value=10_000))
    def test_dsr_never_exceeds_psr(self, sharpes, seed):
        rng = np.random.default_rng(seed)
        returns = rng.normal(0.0005, 0.01, size=250)
        psr = probabilistic_sharpe(returns)
        dsr = deflated_sharpe(returns, sharpes, bars_per_year=252.0)
        assert psr is not None and dsr is not None
        assert dsr <= psr + 1e-9, (
            f'deflation raised confidence: PSR={psr:.4f} -> DSR={dsr:.4f}')

    def test_a_single_untried_run_deflates_by_nothing(self):
        """With one trial there was no selection, so DSR must equal PSR
        exactly. This is the boundary the asymptotic formula cannot express —
        it evaluates Φ⁻¹(0) = -∞ at N=1 — so it is special-cased, and if that
        special case is ever dropped this test fails rather than the function
        silently returning nan."""
        rng = np.random.default_rng(7)
        returns = rng.normal(0.0005, 0.01, size=250)
        assert deflated_sharpe(returns, [1.4]) == pytest.approx(
            probabilistic_sharpe(returns))

    @_SETTINGS
    @given(variance=st.floats(min_value=1e-6, max_value=4.0,
                              allow_nan=False, allow_infinity=False),
           a=st.integers(min_value=2, max_value=200),
           b=st.integers(min_value=2, max_value=200))
    def test_more_trials_means_a_harder_benchmark(self, variance, a, b):
        """Searching more must never lower the bar. Monotonicity in N is the
        whole economic content of the correction."""
        assume(a < b)
        lo, hi = expected_max_sharpe(a, variance), expected_max_sharpe(b, variance)
        assert lo is not None and hi is not None
        assert hi >= lo - 1e-12

    def test_identical_trials_barely_deflate(self):
        """Twenty-seven cells that all behave the same is not twenty-seven
        experiments. The variance term carries that: it is measured from the
        sweep rather than assumed, so a grid with no spread deflates by
        nothing, which is the honest answer."""
        assert expected_max_sharpe(27, 0.0) == 0.0


class TestPBOOnDataWhoseAnswerIsKnown:
    """The two cases that pin the estimator between its failure modes."""

    def test_pure_noise_gives_one_half(self):
        """No column has any edge, so the in-sample winner is arbitrary and its
        out-of-sample rank is uniform — half the time in the bottom half. Any
        estimator that reports much less than 0.5 here would bless a sweep over
        noise, which is the exact failure this whole module exists to prevent.

        Averaged over several seeds because a single 12,870-combination pass is
        itself a random variable; the band is on the mean, and it is tight
        enough that a systematically biased estimator cannot sit inside it.
        """
        got = []
        for seed in range(6):
            rng = np.random.default_rng(seed)
            result = pbo_cscv(_noise_matrix(rng, 400, 12), n_splits=10)
            assert result is not None
            got.append(result.pbo)
        mean = float(np.mean(got))
        assert 0.35 <= mean <= 0.65, f'noise family scored PBO={mean:.3f}, want ~0.5'

    def test_a_real_edge_is_not_condemned(self):
        """One column has genuine drift and the rest are noise. The in-sample
        winner is then almost always that column, and it stays best out of
        sample, so PBO must be LOW. An estimator that cries overfitting here
        would be discarded the first time it condemned something real — and
        then the noise case would stop being caught too."""
        rng = np.random.default_rng(11)
        m = _noise_matrix(rng, 400, 12)
        m[:, 3] += 0.004          # a persistent, genuine edge
        result = pbo_cscv(m, n_splits=10)
        assert result is not None
        assert result.pbo <= 0.15, (
            f'a genuinely dominant trial was called overfit: PBO={result.pbo:.3f}')
        assert result.median_oos_rank > 0.5

    def test_the_result_is_deterministic(self):
        """CSCV enumerates every symmetric split rather than sampling, so two
        runs over the same matrix must agree exactly. A stochastic answer here
        would make the number unciteable in a deploy decision."""
        rng = np.random.default_rng(3)
        m = _noise_matrix(rng, 200, 8)
        assert pbo_cscv(m, n_splits=8).pbo == pbo_cscv(m, n_splits=8).pbo


class TestPBOStaysInRangeAndRefusesWhatItCannotAnswer:

    @_SETTINGS
    @given(seed=st.integers(min_value=0, max_value=5_000),
           n_trials=st.integers(min_value=2, max_value=8),
           n_obs=st.integers(min_value=20, max_value=120))
    def test_pbo_is_always_a_probability(self, seed, n_trials, n_obs):
        rng = np.random.default_rng(seed)
        result = pbo_cscv(_noise_matrix(rng, n_obs, n_trials), n_splits=6)
        assert result is None or 0.0 <= result.pbo <= 1.0

    def test_a_narrow_sweep_carries_its_own_caveat(self):
        """The estimate's spread is driven by the rank grid, which has only
        N+1 positions, so a 12-trial sweep reading 0.25 is entirely consistent
        with pure noise. The qualification therefore travels ON the result: a
        number that gets pasted into a deploy decision must carry the reason it
        cannot be read literally, because the docstring will not come with it.
        """
        rng = np.random.default_rng(4)
        narrow = pbo_cscv(_noise_matrix(rng, 400, 8), n_splits=10)
        assert narrow is not None and narrow.caveat is not None
        assert '8 trials' in narrow.caveat

        wide = pbo_cscv(_noise_matrix(rng, 400, 40), n_splits=10)
        assert wide is not None and wide.caveat is None

    def test_one_trial_is_refused_not_guessed(self):
        """A rank among one thing carries no information. Returning 0.0 would
        read as 'no overfitting detected', which is the opposite of the
        truth — nothing was tested at all."""
        rng = np.random.default_rng(1)
        assert pbo_cscv(_noise_matrix(rng, 200, 1)) is None

    def test_an_odd_split_count_is_refused(self):
        """CSCV's symmetry depends on halving the blocks. An odd S cannot be
        halved, and quietly rounding would break the property the estimator
        rests on."""
        rng = np.random.default_rng(1)
        assert pbo_cscv(_noise_matrix(rng, 200, 5), n_splits=7) is None

    def test_a_short_series_shrinks_the_split_rather_than_refusing(self):
        """A one-year daily backtest has ~250 rows; at S=16 that is fine, but a
        60-row run is normal too and should still get an answer from fewer
        blocks rather than a None the caller renders as 'unknown'."""
        rng = np.random.default_rng(2)
        result = pbo_cscv(_noise_matrix(rng, 24, 5), n_splits=16)
        assert result is not None and result.n_splits < 16


class TestCurveAlignment:
    """CSCV compares columns row by row. If the columns are not the same
    instants, it measures the misalignment and reports it as overfitting."""

    def _days(self, n, start=1, step=1.0, skip=()):
        return [{'timestamp': f'2026-01-{i:02d} 00:00:00-05:00',
                 'value': 100.0 + i * step}
                for i in range(start, start + n) if i not in skip]

    def test_curves_are_aligned_on_date_not_full_timestamp(self):
        """Decimation appends each run's true final point, so every curve ends
        on its own last intraday minute. Matching whole timestamps would drop
        the final row of every column for a reason that has nothing to do with
        the data."""
        a = self._days(10)
        b = self._days(10, step=2.0)
        b[-1] = {'timestamp': '2026-01-10 19:57:00-05:00', 'value': 121.0}
        aligned = align_equity_curves([a, b])
        assert aligned is not None
        m, n_used, n_dropped = aligned
        assert n_used == 2 and n_dropped == 0
        assert m.shape == (9, 2)

    def test_one_short_curve_is_dropped_rather_than_truncating_everyone(self):
        """The failure this rule exists for: pooling 495 real ORB runs across
        two venues collapsed the common index from 250 rows to 12, and CSCV
        still returned a confident-looking 20%. Losing a column shows up in the
        returned count; losing 95% of the rows does not."""
        full = [self._days(20, step=float(k)) for k in range(1, 9)]
        stub = [{'timestamp': '2026-01-01 00:00:00-05:00', 'value': 100.0},
                {'timestamp': '2026-01-02 00:00:00-05:00', 'value': 101.0},
                {'timestamp': '2026-01-03 00:00:00-05:00', 'value': 102.0}]
        aligned = align_equity_curves(full + [stub])
        assert aligned is not None
        m, n_used, n_dropped = aligned
        assert n_dropped == 1, 'the stub should be dropped, not shrink the index'
        assert n_used == 8
        assert m.shape[0] >= 18, f'index collapsed to {m.shape[0]} rows'

    def test_an_interior_hole_carries_forward_instead_of_dropping_the_row(self):
        """A venue holiday one run observed and the rest did not is 'no change
        recorded', not a reason to discard that date for every column.

        Note this only engages once the index is set by a QUORUM rather than by
        unanimity: with two curves the quorum is both of them, so a hole in
        either simply leaves that day out. Ten curves is the realistic case —
        one straggler must not cost the other nine a row.
        """
        curves = [self._days(12, step=float(k)) for k in range(1, 10)]
        curves.append(self._days(12, step=11.0, skip=(5,)))
        aligned = align_equity_curves(curves)
        assert aligned is not None
        m, n_used, n_dropped = aligned
        assert n_used == 10 and n_dropped == 0
        assert m.shape == (11, 10), 'the hole cost every column a row'
        # Row i is the move from index[i] to index[i+1]. The straggler has no
        # mark on day 5, so its day-4 -> day-5 move is flat (carried forward)
        # while every other column shows a real return on the same row.
        assert m[3, 9] == 0.0
        assert m[3, 0] != 0.0

    def test_disjoint_curves_are_refused(self):
        a = [{'timestamp': f'2026-01-{i:02d} 00:00:00-05:00', 'value': 100.0 + i}
             for i in range(1, 11)]
        b = [{'timestamp': f'2026-05-{i:02d} 00:00:00-05:00', 'value': 100.0 + i}
             for i in range(1, 11)]
        assert align_equity_curves([a, b]) is None

    def test_a_malformed_point_does_not_take_down_the_curve(self):
        a = self._days(10)
        b = self._days(10, step=2.0)
        b[4] = {'timestamp': '2026-01-05 00:00:00-05:00'}      # no value
        aligned = align_equity_curves([a, b])
        assert aligned is not None and aligned[0].shape[1] == 2


class TestAnnualisationIsInferredNotAssumed:
    """A live bug, pinned. The stored equity curve is decimated to one point
    per day on write, but the run's `bar_size` still says '1 min'. Anything
    that annualises the blob's returns with the bar-size factor of 98,280
    overstates the result by √(98280/252) ≈ 19.7× — which is exactly how the
    first DSR reading came back at 0.974 for a family of 495 trials."""

    def test_a_daily_decimated_curve_reads_as_daily(self):
        ts = [f'2026-{1 + i // 21:02d}-{1 + i % 21:02d} 00:00:00-05:00'
              for i in range(250)]
        ppy = infer_periods_per_year(ts)
        assert ppy is not None
        assert 200 < ppy < 400, f'daily curve inferred as {ppy:.0f} periods/year'

    def test_a_minute_curve_reads_as_minutes(self):
        ts = [f'2026-01-05 {9 + i // 60:02d}:{i % 60:02d}:00-05:00'
              for i in range(300)]
        ppy = infer_periods_per_year(ts)
        assert ppy is not None and ppy > 50_000

    def test_unparseable_timestamps_return_none_rather_than_a_default(self):
        """Falling back to 252 here would be a silent wrong answer. The caller
        can choose a fallback; this function must not choose one for it."""
        assert infer_periods_per_year(['not-a-date', 'nor-this', 'x']) is None
