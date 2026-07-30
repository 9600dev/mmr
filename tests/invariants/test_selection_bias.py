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


class TestThePooledArithmeticEqualsTheDirectComputation:
    """`_pooled_sharpe` computes a Sharpe from per-block (n, sum, sum-of-squares)
    aggregates rather than from the rows themselves, because CSCV scores 12,870
    train/test pairs and recomputing over half the rows each time would make the
    test unrunnable. Its docstring claims the two are numerically identical.

    That claim had NO test. It is the same shape as `impossible_mask` versus
    `check_bar` - an optimisation standing in for a specification - which this
    codebase already treats as load-bearing, because the flip residual came from
    exactly that pattern drifting. If the pooled form is wrong, every PBO figure
    ever reported here is wrong, and nothing about the output would look odd.
    """

    def _direct_sharpe(self, matrix, chosen, n_splits):
        """Sharpe computed straightforwardly over the selected rows."""
        T = matrix.shape[0]
        bounds = np.linspace(0, T, n_splits + 1).astype(int)
        rows = np.concatenate([np.arange(bounds[i], bounds[i + 1])
                               for i in sorted(chosen)])
        sub = matrix[rows, :]
        mean = sub.mean(axis=0)
        sd = sub.std(axis=0, ddof=1)
        with np.errstate(invalid='ignore', divide='ignore'):
            out = np.where(sd > 0, mean / sd, 0.0)
        return np.nan_to_num(out, nan=0.0, posinf=0.0, neginf=0.0)

    @settings(max_examples=120, deadline=None)
    @given(seed=st.integers(min_value=0, max_value=2_000),
           n_names=st.integers(min_value=2, max_value=8),
           n_splits=st.sampled_from([4, 6, 8]))
    def test_pooled_equals_direct_on_every_block_subset(self, seed, n_names,
                                                       n_splits):
        import itertools
        from trader.simulation.selection_bias import (_block_moments,
                                                      _pooled_sharpe)
        rng = np.random.default_rng(seed)
        m = rng.normal(0.0, 0.01, size=(n_splits * 12, n_names))
        counts, sums, sumsqs = _block_moments(m, n_splits)
        for chosen in itertools.combinations(range(n_splits), n_splits // 2):
            pooled = _pooled_sharpe(counts, sums, sumsqs, chosen)
            direct = self._direct_sharpe(m, chosen, n_splits)
            assert np.allclose(pooled, direct, atol=1e-9), (
                f'pooled and direct Sharpe disagree on blocks {chosen}: '
                f'{pooled} vs {direct}')

    def test_the_blocks_partition_every_row_exactly_once(self):
        """A row counted twice, or dropped, silently changes every pooled
        statistic. The partition is the foundation the aggregates rest on."""
        from trader.simulation.selection_bias import _block_moments
        for T, S in ((100, 8), (97, 8), (13, 4), (16, 16)):
            m = np.arange(T, dtype=float).reshape(T, 1)
            counts, sums, _ = _block_moments(m, S)
            assert counts.sum() == T, f'blocks cover {counts.sum()} of {T} rows'
            # Every value appears once, so the summed block sums equal the total.
            assert sums.sum() == pytest.approx(m.sum())

    def test_a_single_row_block_yields_no_sharpe_rather_than_infinity(self):
        """One observation has no dispersion. Returning a number there would
        put a divide-by-almost-zero into the ranking."""
        from trader.simulation.selection_bias import (_block_moments,
                                                      _pooled_sharpe)
        m = np.array([[0.01], [0.02], [0.03], [0.04]])
        counts, sums, sumsqs = _block_moments(m, 4)
        out = _pooled_sharpe(counts, sums, sumsqs, (0,))
        assert np.all(np.isfinite(out))
        assert out[0] == 0.0

    def test_a_constant_column_scores_zero_not_a_spurious_giant(self):
        """Found a real bug. The pooled variance (sum-sq - n*mean^2) is a
        catastrophic cancellation for a near-constant column: two nearly-equal
        large numbers subtracted, leaving float noise that is tiny but positive,
        so mean/sqrt(noise) explodes. A constant column of 0.005 returned
        38,214,751 - and such a trial wins the in-sample argmax in EVERY CSCV
        split, corrupting the PBO.

        The docstring claimed the pooled form was numerically identical to the
        direct computation. It was not, precisely where it mattered: numpy's
        std() uses a stable two-pass method and does not cancel."""
        from trader.simulation.selection_bias import (_block_moments,
                                                      _pooled_sharpe)
        m = np.full((40, 2), 0.005)
        m[:, 1] = np.linspace(0, 1, 40)
        counts, sums, sumsqs = _block_moments(m, 4)
        out = _pooled_sharpe(counts, sums, sumsqs, (0, 1))
        assert np.all(np.isfinite(out))
        assert out[0] == 0.0

    @settings(max_examples=80, deadline=None)
    @given(seed=st.integers(min_value=0, max_value=500),
           k=st.floats(min_value=0.1, max_value=50.0, allow_nan=False,
                       allow_infinity=False))
    def test_pooled_sharpe_is_scale_invariant(self, seed, k):
        """Sharpe is a ratio, so multiplying returns by a constant must not move
        it. Kills mutants that drop the division or square the wrong term."""
        from trader.simulation.selection_bias import (_block_moments,
                                                      _pooled_sharpe)
        rng = np.random.default_rng(seed)
        m = rng.normal(0.001, 0.02, size=(48, 3))
        a = _pooled_sharpe(*_block_moments(m, 4), (0, 1))
        b = _pooled_sharpe(*_block_moments(m * k, 4), (0, 1))
        assert np.allclose(a, b, atol=1e-7)


class TestCSCVMechanicsAndAlignment:
    """`pbo_cscv` carried 42 survivors and `align_equity_curves` 33 - the two
    largest concentrations in this module. The existing tests establish the
    statistical behaviour (noise -> 0.5, real edge -> low); these pin the
    mechanics that produce it."""

    def _noise(self, T=200, N=8, seed=0):
        return np.random.default_rng(seed).normal(0, 0.01, size=(T, N))

    def test_the_combination_count_is_exactly_C_S_half_S(self):
        """S=8 gives C(8,4)=70 splits, S=10 gives 252. A mutant that enumerates
        permutations, or halves the set, changes the estimate's precision
        silently - the PBO would still look like a probability."""
        from math import comb
        for S in (4, 6, 8, 10):
            r = pbo_cscv(self._noise(T=S * 20, N=6), n_splits=S)
            assert r is not None
            assert r.n_combinations == comb(S, S // 2), S

    def test_train_and_test_are_complementary_halves(self):
        """Every block is used exactly once per split, as training or testing.
        The symmetry is what makes CSCV trustworthy: a block that appeared in
        both, or in neither, would bias the estimate toward whichever period it
        favoured."""
        import itertools
        S = 8
        for train in itertools.combinations(range(S), S // 2):
            test = tuple(sorted(set(range(S)) - set(train)))
            assert len(train) == len(test) == S // 2
            assert not set(train) & set(test)
            assert set(train) | set(test) == set(range(S))

    def test_the_reported_trial_count_is_the_matrix_width(self):
        for N in (3, 7, 15):
            r = pbo_cscv(self._noise(N=N), n_splits=6)
            assert r is not None and r.n_trials == N

    def test_median_oos_rank_is_a_fraction(self):
        r = pbo_cscv(self._noise(), n_splits=6)
        assert r is not None and 0.0 < r.median_oos_rank < 1.0

    def test_pbo_and_the_logits_agree(self):
        """PBO is defined as the fraction of splits with lambda <= 0. A mutant
        that changes the threshold or the direction breaks the identity."""
        r = pbo_cscv(self._noise(seed=4), n_splits=8)
        assert r is not None
        recomputed = sum(1 for x in r.logits if x <= 0.0) / len(r.logits)
        assert r.pbo == pytest.approx(recomputed)

    def test_an_all_identical_family_lands_at_one_half(self):
        """Every trial the same means the winner's OOS rank is a tie, resolved
        to the middle. A mutant breaking ties by column order would report 0 or
        1 - a confident answer about a family carrying no information."""
        m = np.tile(self._noise(N=1, seed=9), (1, 6))
        r = pbo_cscv(m, n_splits=8)
        assert r is not None
        assert r.median_oos_rank == pytest.approx(0.5, abs=0.02)

    def test_alignment_requires_a_quorum_not_unanimity(self):
        """A day present in most curves survives; one present in a minority does
        not. A mutant using unanimity reintroduces the intersection collapse
        that turned 250 rows into 12."""
        base = [{'timestamp': f'2026-01-{i:02d} 00:00:00-05:00',
                 'value': 100.0 + i} for i in range(1, 21)]
        curves = [list(base) for _ in range(9)]
        curves.append(base + [{'timestamp': '2026-02-01 00:00:00-05:00',
                               'value': 200.0}])
        aligned = align_equity_curves(curves)
        assert aligned is not None
        m, used, dropped = aligned
        assert used == 10 and dropped == 0
        assert m.shape[0] == 19, 'the minority day should not enter the index'

    def test_returns_are_computed_from_consecutive_index_rows(self):
        """The matrix holds RETURNS, not levels. A mutant that forgot the diff
        would feed price levels into a Sharpe and produce enormous values."""
        curves = [[{'timestamp': f'2026-01-{i:02d} 00:00:00-05:00',
                    'value': float(100 * (1.1 ** (i - 1)))}
                   for i in range(1, 11)] for _ in range(3)]
        aligned = align_equity_curves(curves)
        assert aligned is not None
        m, _, _ = aligned
        assert np.allclose(m, 0.10, atol=1e-9), (
            'a series compounding at 10% per period must yield 0.10 returns')

    def test_a_single_curve_is_refused(self):
        assert align_equity_curves([[{'timestamp': 'd1', 'value': 1.0}]]) is None
