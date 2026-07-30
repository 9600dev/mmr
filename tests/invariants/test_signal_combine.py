"""SPEC: standardise across NAMES, never across time.

A time-series z-score at row t divides by a standard deviation computed from
the whole sample - including the future - and the resulting "signal" knows how
volatile the next five years will be. It is the same class of error as a
misaligned forward return and just as invisible: a well-behaved frame of
plausible numbers, and an IC that is partly a measurement of hindsight.

So the first group pins the axis. The rest cover the two ways a combination
can quietly misrepresent itself: letting units decide the weighting, and
treating a missing input as a bearish view.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from hypothesis import assume, given, settings, strategies as st

from trader.simulation.signal_combine import (
    combine, combined_ic_estimate, cross_sectional_zscore, neutralise,
    signal_correlations)


def _frame(rows=8, cols=6, seed=0, scale=1.0, offset=0.0):
    rng = np.random.default_rng(seed)
    return pd.DataFrame(rng.normal(offset, scale, size=(rows, cols)),
                        index=pd.date_range('2024-01-01', periods=rows),
                        columns=[100 + i for i in range(cols)])


class TestStandardisationIsCrossSectional:

    def test_each_period_is_standardised_independently(self):
        """Every row must come out with mean 0 and sd 1 across names. If the
        axis were time, rows would not."""
        z = cross_sectional_zscore(_frame(rows=10, cols=20))
        assert np.allclose(z.mean(axis=1).to_numpy(), 0.0, atol=1e-12)
        assert np.allclose(z.std(axis=1, ddof=1).to_numpy(), 1.0, atol=1e-12)

    def test_scaling_one_period_does_not_touch_another(self):
        """The decisive test for the axis. Multiplying one row by 1000 must
        leave every other row's z-scores identical - if standardisation
        touched the time axis, they would all move."""
        f = _frame(rows=6, cols=10)
        z1 = cross_sectional_zscore(f)
        f2 = f.copy()
        f2.iloc[3] = f2.iloc[3] * 1000.0
        z2 = cross_sectional_zscore(f2)
        untouched = [i for i in range(6) if i != 3]
        assert np.allclose(z1.iloc[untouched].to_numpy(),
                           z2.iloc[untouched].to_numpy(), atol=1e-12)

    def test_a_row_is_invariant_to_its_own_units(self):
        """A signal in percent and the same signal in basis points must
        standardise identically, which is the whole reason to z-score before
        weighting."""
        f = _frame(rows=5, cols=12)
        assert np.allclose(cross_sectional_zscore(f).to_numpy(),
                           cross_sectional_zscore(f * 10_000).to_numpy(),
                           atol=1e-9)

    def test_a_thin_cross_section_is_emptied_not_standardised(self):
        """A z-score over three names is not a ranking, it is noise with a
        mean subtracted from it."""
        f = _frame(rows=3, cols=3)
        assert cross_sectional_zscore(f, min_names=5).isna().all().all()

    def test_a_constant_row_does_not_divide_by_zero(self):
        f = _frame(rows=4, cols=8)
        f.iloc[2] = 5.0
        z = cross_sectional_zscore(f)
        assert z.iloc[2].isna().all()
        assert not z.iloc[0].isna().any()


class TestCombination:

    def test_units_do_not_decide_the_weighting(self):
        """Two signals differing only in scale must combine identically to two
        that do not. Weighting raw signals would let dollars outvote percent."""
        a, b = _frame(seed=1), _frame(seed=2)
        plain = combine({'a': a, 'b': b})
        scaled = combine({'a': a * 1e6, 'b': b})
        assert np.allclose(plain.to_numpy(), scaled.to_numpy(),
                           atol=1e-9, equal_nan=True)

    def test_a_missing_input_is_not_a_bearish_view(self):
        """A name present in one signal and absent from another must keep its
        contribution from the first, not be pulled toward zero by the gap."""
        a, b = _frame(seed=3), _frame(seed=4)
        b.iloc[:, 0] = np.nan          # one name missing from signal b
        out = combine({'a': a, 'b': b})
        za = cross_sectional_zscore(a)
        assert out.iloc[:, 0].notna().all()
        assert np.allclose(out.iloc[:, 0].to_numpy(),
                           za.iloc[:, 0].to_numpy(), atol=1e-9)

    def test_a_zero_weight_excludes_a_signal_entirely(self):
        a, b = _frame(seed=5), _frame(seed=6)
        only_a = combine({'a': a, 'b': b}, weights={'a': 1.0, 'b': 0.0})
        assert np.allclose(only_a.to_numpy(),
                           cross_sectional_zscore(a).to_numpy(),
                           atol=1e-9, equal_nan=True)

    def test_combining_a_signal_with_itself_changes_nothing(self):
        """The degenerate case. Two perfectly correlated signals are one
        signal, and the combination must not pretend otherwise."""
        a = _frame(seed=7)
        assert np.allclose(combine({'a': a, 'copy': a.copy()}).to_numpy(),
                           cross_sectional_zscore(a).to_numpy(),
                           atol=1e-9, equal_nan=True)

    def test_no_signals_yields_none(self):
        assert combine({}) is None


class TestTheCombinationFormula:

    def test_uncorrelated_signals_scale_as_sqrt_k(self):
        base = combined_ic_estimate(0.02, 1, 0.0)
        four = combined_ic_estimate(0.02, 4, 0.0)
        assert four == pytest.approx(base * 2.0)

    def test_perfectly_correlated_signals_add_nothing(self):
        """rho = 1 means every signal is the same signal."""
        assert combined_ic_estimate(0.02, 5, 1.0) == pytest.approx(0.02)

    @settings(max_examples=200, deadline=None)
    @given(ic=st.floats(min_value=0.001, max_value=0.2, allow_nan=False,
                        allow_infinity=False),
           k=st.integers(min_value=2, max_value=30),
           rho=st.floats(min_value=0.0, max_value=1.0, allow_nan=False,
                         allow_infinity=False))
    def test_correlation_can_only_reduce_the_benefit(self, ic, k, rho):
        at_zero = combined_ic_estimate(ic, k, 0.0)
        at_rho = combined_ic_estimate(ic, k, rho)
        assert at_zero is not None and at_rho is not None
        assert at_rho <= at_zero + 1e-12

    def test_a_perfectly_hedged_set_is_refused_not_infinite(self):
        """rho = -1/(k-1) drives the denominator to zero. That is the
        algebraic edge of a fully hedged set, not an infinite IC."""
        assert combined_ic_estimate(0.02, 3, -0.5) is None


class TestNeutralisation:

    def test_every_group_averages_to_zero_afterwards(self):
        """After neutralising, the book can prefer one name in a sector over
        another but can no longer prefer the sector."""
        f = _frame(rows=5, cols=6, offset=3.0)
        groups = {100: 'tech', 101: 'tech', 102: 'tech',
                  103: 'energy', 104: 'energy', 105: 'energy'}
        out = neutralise(f, groups)
        for label in ('tech', 'energy'):
            cols = [c for c in f.columns if groups[c] == label]
            assert np.allclose(out[cols].mean(axis=1).to_numpy(), 0.0, atol=1e-12)

    def test_relative_order_within_a_group_survives(self):
        """Neutralising removes the group's average view, not its ranking."""
        f = _frame(rows=4, cols=6)
        groups = {c: ('a' if i < 3 else 'b') for i, c in enumerate(f.columns)}
        out = neutralise(f, groups)
        cols = [c for i, c in enumerate(f.columns) if i < 3]
        assert (f[cols].rank(axis=1).to_numpy()
                == out[cols].rank(axis=1).to_numpy()).all()

    def test_names_without_a_group_are_left_alone(self):
        """Pooling them would create a group whose only shared property is
        missing metadata."""
        f = _frame(rows=3, cols=4)
        out = neutralise(f, {100: 'x', 101: 'x'})
        assert np.allclose(out[[102, 103]].to_numpy(), f[[102, 103]].to_numpy())

    def test_a_group_of_one_is_not_demeaned_to_zero(self):
        """Demeaning a single name against itself would delete its signal."""
        f = _frame(rows=3, cols=3)
        out = neutralise(f, {100: 'solo', 101: 'pair', 102: 'pair'})
        assert np.allclose(out[[100]].to_numpy(), f[[100]].to_numpy())


class TestCorrelationDiagnostic:

    def test_a_signal_correlates_perfectly_with_itself(self):
        a = _frame(rows=40, cols=25, seed=9)
        c = signal_correlations({'a': a, 'same': a.copy()})
        assert c['a|same'] == pytest.approx(1.0, abs=1e-9)

    def test_independent_signals_correlate_near_zero(self):
        c = signal_correlations({'a': _frame(rows=60, cols=40, seed=11),
                                 'b': _frame(rows=60, cols=40, seed=12)})
        assert abs(c['a|b']) < 0.25

    def test_a_non_conid_column_label_does_not_take_down_the_book(self):
        """Found in the field: a 490-name panel contained two instruments
        stored under their TICKER rather than their conid (BRK.B, ORCL), and
        int() on the column label raised - taking down the whole
        neutralisation for a store inconsistency that is not this function's
        business. A label that cannot be a conid has no sector, which is the
        same situation as a conid with no metadata: leave it alone.
        """
        f = _frame(rows=3, cols=4)
        f.columns = [100, 101, 'BRK.B', 'ORCL']
        out = neutralise(f, {100: 'x', 101: 'x'})
        assert np.allclose(out[['BRK.B', 'ORCL']].to_numpy(),
                           f[['BRK.B', 'ORCL']].to_numpy())
        assert np.allclose(out[[100, 101]].mean(axis=1).to_numpy(), 0.0,
                           atol=1e-12)


class TestCombineArithmeticAndCorrelationBoundaries:
    """`combine` carried 26 surviving mutants and `signal_correlations` 12 - the
    largest concentrations in this module. The existing tests establish the
    contract; these pin the arithmetic that implements it."""

    def test_the_weighted_mean_is_actually_weighted(self):
        """Two signals at 3:1 must land three-quarters of the way toward the
        first. A mutant that ignores weights, or sums instead of averaging,
        passes every equal-weight test."""
        a = pd.DataFrame({10: [1.0], 11: [-1.0], 12: [0.0]})
        b = pd.DataFrame({10: [-1.0], 11: [1.0], 12: [0.0]})
        out = combine({'a': a, 'b': b}, weights={'a': 3.0, 'b': 1.0},
                      min_names=3)
        za = cross_sectional_zscore(a, min_names=3)
        zb = cross_sectional_zscore(b, min_names=3)
        expected = (3.0 * za + 1.0 * zb) / 4.0
        assert np.allclose(out.to_numpy(), expected.to_numpy(),
                           atol=1e-9, equal_nan=True)

    def test_a_negative_weight_inverts_a_signal(self):
        """Weights are signed on purpose: a signal believed backwards can be
        flipped rather than rewritten. A mutant taking abs() of the weight
        before applying it would silently stop honouring that."""
        a = pd.DataFrame({10: [2.0], 11: [-2.0], 12: [0.0], 13: [1.0], 14: [-1.0]})
        pos = combine({'a': a}, weights={'a': 1.0})
        neg = combine({'a': a}, weights={'a': -1.0})
        assert np.allclose(pos.to_numpy(), -neg.to_numpy(),
                           atol=1e-9, equal_nan=True)

    def test_the_divisor_counts_CONTRIBUTING_signals_per_name(self):
        """A name present in one of two signals must be averaged over one, not
        two - otherwise a missing input halves its score, which is the 'missing
        is bearish' error in disguise."""
        a = _frame(rows=1, cols=6, seed=1)
        b = _frame(rows=1, cols=6, seed=2)
        b.iloc[0, 0] = np.nan
        out = combine({'a': a, 'b': b}, min_names=5)
        za = cross_sectional_zscore(a, min_names=5)
        assert out.iloc[0, 0] == pytest.approx(za.iloc[0, 0])

    def test_all_zero_weights_yield_nothing(self):
        a, b = _frame(seed=1), _frame(seed=2)
        assert combine({'a': a, 'b': b}, weights={'a': 0.0, 'b': 0.0}) is None

    def test_a_non_finite_weight_is_skipped_not_propagated(self):
        """One bad weight must not NaN the whole book."""
        a, b = _frame(seed=3), _frame(seed=4)
        out = combine({'a': a, 'b': b},
                      weights={'a': 1.0, 'b': float('nan')})
        assert out is not None
        assert np.allclose(out.to_numpy(),
                           cross_sectional_zscore(a).to_numpy(),
                           atol=1e-9, equal_nan=True)

    def test_correlation_is_signed(self):
        """An inverted copy must correlate -1, not +1. A mutant taking abs()
        would report a perfectly hedged pair as perfectly redundant, which is
        the exact opposite conclusion."""
        a = _frame(rows=40, cols=25, seed=5)
        c = signal_correlations({'a': a, 'inv': -a})
        assert c['a|inv'] == pytest.approx(-1.0, abs=1e-9)

    def test_every_pair_is_reported_once(self):
        """Three signals give three pairs, not six and not nine. A mutant that
        double-counts would halve every mean-rho estimate fed to the
        combination formula."""
        f = {k: _frame(rows=30, cols=20, seed=i)
             for i, k in enumerate(('a', 'b', 'c'))}
        assert len(signal_correlations(f)) == 3

    def test_a_single_signal_has_no_pairs(self):
        assert signal_correlations({'a': _frame(rows=30, cols=20)}) == {}

    def test_a_too_thin_cross_section_is_skipped_not_zeroed(self):
        """Correlating 4 names is noise. Reporting 0.0 for it would drag a mean
        rho toward zero and overstate how diversified a set is."""
        a = _frame(rows=30, cols=4, seed=7)
        b = _frame(rows=30, cols=4, seed=8)
        assert signal_correlations({'a': a, 'b': b}) == {}
