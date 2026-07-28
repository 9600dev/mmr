"""SPEC: walk-forward folds must never be judged on data they were fitted on.

This is the property the whole technique rests on. Walk-forward exists because
selecting and reporting on the same data produced PBO 58-70% on this
codebase's own sweeps — the in-sample winner landing in the bottom half out of
sample more often than not. If a fold's test window can begin before its
training window ends, walk-forward silently becomes the thing it was built to
replace, and the output looks identical either way: same shape, same columns,
a plausible equity curve, a number someone deploys on.

That invisibility is why leakage is asserted here and contracted at runtime
rather than left to review. A reviewer reading `train_end` and `test_start` in
a loop cannot see an off-by-one; a property over generated inputs can.

The second group is about the procedure being honest when it CANNOT answer.
A period too short to hold a fold must yield no folds — not a shrunken window
that answers a different question, and not one fold that overlaps itself.
"""

from __future__ import annotations

import datetime as dt

import pytest

from hypothesis import assume, given, settings, strategies as st

from trader.simulation.walk_forward import (
    FoldResult, pick_best, plan_fold_offsets, resolve_folds,
    selection_stability)


_SETTINGS = settings(max_examples=500, deadline=None)

_DAYS = st.integers(min_value=0, max_value=4000)
_WINDOW = st.integers(min_value=1, max_value=500)


class TestNoLeakage:
    """The one that matters. Every other property here is secondary."""

    @_SETTINGS
    @given(total=_DAYS, train=_WINDOW, test=_WINDOW,
           anchored=st.booleans())
    def test_a_fold_is_never_tested_on_what_it_was_fitted_on(
            self, total, train, test, anchored):
        for f in plan_fold_offsets(total, train, test, anchored):
            assert f.test_start >= f.train_end, (
                f'fold {f.index} trains to {f.train_end} but tests from '
                f'{f.test_start} — {f.train_end - f.test_start} day(s) of '
                f'the test window were used to choose the parameters')

    @_SETTINGS
    @given(total=_DAYS, train=_WINDOW, test=_WINDOW, anchored=st.booleans())
    def test_each_day_is_judged_at_most_once(self, total, train, test, anchored):
        """Overlapping test windows would double-count the same days in the
        stitched equity curve, inflating both the return and the sample size
        every downstream statistic is computed from."""
        folds = plan_fold_offsets(total, train, test, anchored)
        for a, b in zip(folds, folds[1:]):
            assert a.test_end <= b.test_start

    @_SETTINGS
    @given(total=_DAYS, train=_WINDOW, test=_WINDOW, anchored=st.booleans())
    def test_nothing_extends_past_the_available_data(
            self, total, train, test, anchored):
        """A test window running off the end of the data would be scored on
        whatever the backtester returns for an empty range, which is not a
        result — it is the absence of one."""
        for f in plan_fold_offsets(total, train, test, anchored):
            assert f.test_end <= total
            assert f.train_start >= 0

    @_SETTINGS
    @given(total=_DAYS, train=_WINDOW, test=_WINDOW, anchored=st.booleans())
    def test_windows_are_never_empty(self, total, train, test, anchored):
        for f in plan_fold_offsets(total, train, test, anchored):
            assert f.train_end > f.train_start
            assert f.test_end > f.test_start

    @_SETTINGS
    @given(start=st.dates(min_value=dt.date(2000, 1, 1),
                          max_value=dt.date(2035, 1, 1)),
           total=st.integers(min_value=0, max_value=1500),
           train=_WINDOW, test=_WINDOW)
    def test_leakage_survives_the_calendar(self, start, total, train, test):
        """The offsets are clean; resolving them onto dates must not
        reintroduce an overlap."""
        for f in resolve_folds(start, plan_fold_offsets(total, train, test)):
            assert f.test_start >= f.train_end
            assert f.train_start < f.train_end < f.test_end


class TestItRefusesRatherThanImprovises:

    @pytest.mark.parametrize('total,train,test', [(0, 60, 20), (10, 60, 20),
                                                  (79, 60, 20), (59, 1, 60)])
    def test_too_short_a_period_yields_no_folds(self, total, train, test):
        """Not an error, and deliberately not a shrunken window. 'What can you
        tell me out-of-sample from 40 days with a 60-day lookback' has the
        answer 'nothing', and quietly shrinking the window would answer a
        different question than the caller asked."""
        assert plan_fold_offsets(total, train, test) == []

    def test_exactly_enough_data_yields_exactly_one_fold(self):
        folds = plan_fold_offsets(80, 60, 20)
        assert len(folds) == 1
        assert folds[0] == (0, 0, 60, 60, 80)

    def test_a_non_positive_window_is_refused(self):
        """A zero-length training window would 'select' on nothing."""
        import deal
        for bad in ((100, 0, 20), (100, 60, 0), (100, -5, 20)):
            with pytest.raises((deal.PreContractError, ValueError)):
                plan_fold_offsets(*bad)


class TestTheTwoModesDifferAsAdvertised:

    def test_rolling_windows_stay_the_same_length(self):
        folds = plan_fold_offsets(300, 60, 20, anchored=False)
        assert len({f.train_end - f.train_start for f in folds}) == 1

    def test_anchored_windows_grow_from_a_fixed_start(self):
        folds = plan_fold_offsets(300, 60, 20, anchored=True)
        assert {f.train_start for f in folds} == {0}
        lengths = [f.train_end - f.train_start for f in folds]
        assert lengths == sorted(lengths) and lengths[0] < lengths[-1]

    def test_both_modes_produce_the_same_test_windows(self):
        """Only the training data differs. If the test windows moved too, the
        two modes would not be comparable and choosing between them would mean
        choosing which period to be judged on."""
        a = plan_fold_offsets(300, 60, 20, anchored=False)
        b = plan_fold_offsets(300, 60, 20, anchored=True)
        assert [(f.test_start, f.test_end) for f in a] == \
               [(f.test_start, f.test_end) for f in b]


class TestSelectionIsHonestAboutMissingScores:

    def test_an_unscored_cell_is_dropped_not_treated_as_zero(self):
        """A failed backtest has not told you the cell is mediocre; it has told
        you nothing. Scoring it 0.0 would rank it above every genuinely losing
        cell and deploy a configuration that was never evaluated."""
        assert pick_best([({'A': 1}, None), ({'A': 2}, -0.5)]) == {'A': 2}

    def test_all_unscored_yields_no_choice(self):
        assert pick_best([({'A': 1}, None), ({'A': 2}, None)]) is None

    def test_ties_resolve_to_grid_order(self):
        """Two runs over the same grid must make the same choice, or the
        walk-forward result is not reproducible."""
        assert pick_best([({'A': 1}, 2.0), ({'A': 2}, 2.0)]) == {'A': 1}

    def test_lower_is_better_is_honoured(self):
        assert pick_best([({'A': 1}, 5.0), ({'A': 2}, 1.0)],
                         higher_is_better=False) == {'A': 2}


class TestSelectionStabilityIsReported:
    """A procedure that picks a different winner every fold has found noise,
    and its out-of-sample equity being positive is luck rather than evidence.
    The number is as informative as the returns."""

    def _fr(self, i, cell):
        from trader.simulation.walk_forward import Fold
        d = dt.date(2026, 1, 1)
        return FoldResult(Fold(i, d, d, d, d), cell, 1.0, {})

    def test_a_procedure_that_never_changes_its_mind_scores_one(self):
        rs = [self._fr(i, {'A': 1}) for i in range(5)]
        assert selection_stability(rs) == 1.0

    def test_a_procedure_that_always_changes_scores_zero(self):
        rs = [self._fr(i, {'A': i}) for i in range(5)]
        assert selection_stability(rs) == 0.0

    def test_undefined_below_two_folds(self):
        assert selection_stability([]) is None
        assert selection_stability([self._fr(0, {'A': 1})]) is None


class TestTheProcedureSelectsOnTrainOnly:
    """The sequencing is where a leak would hide once the arithmetic is right.
    Injecting the backtest runner lets this be checked with no market data at
    all — the recorded calls ARE the evidence."""

    def _harness(self):
        from trader.simulation.walk_forward import run_walk_forward
        calls = []

        def run_backtest(cell, start, end):
            calls.append((dict(cell), start, end))
            # Make cell A look best on every window, so a leak would show up
            # as the test window being scored for more than one cell.
            return {'sharpe': 9.0 if cell['A'] == 1 else 0.1}

        folds = resolve_folds(dt.date(2026, 1, 1),
                              plan_fold_offsets(120, 60, 20))
        cells = [{'A': 1}, {'A': 2}, {'A': 3}]
        results = run_walk_forward(folds, cells, run_backtest,
                                   lambda m: m.get('sharpe'))
        return folds, cells, calls, results

    def test_every_cell_is_run_on_train_and_only_the_winner_on_test(self):
        folds, cells, calls, results = self._harness()
        for fold in folds:
            train_calls = [c for c in calls
                           if (c[1], c[2]) == (fold.train_start, fold.train_end)]
            test_calls = [c for c in calls
                          if (c[1], c[2]) == (fold.test_start, fold.test_end)]
            assert len(train_calls) == len(cells), 'every cell must be fitted'
            assert len(test_calls) == 1, (
                f'{len(test_calls)} cells were scored on the test window — '
                f'reporting the best of those is the original sin with extra '
                f'steps')

    def test_the_reported_choice_is_the_train_winner(self):
        _, _, _, results = self._harness()
        assert all(r.chosen == {'A': 1} for r in results)
        assert all(r.train_score == 9.0 for r in results)

    def test_a_fold_where_nothing_scores_runs_no_test(self):
        """If no cell could be evaluated, there is no decision to judge. Running
        the test window anyway would attribute a result to a choice that was
        never made."""
        from trader.simulation.walk_forward import run_walk_forward
        calls = []

        def run_backtest(cell, start, end):
            calls.append((start, end))
            return None

        folds = resolve_folds(dt.date(2026, 1, 1), plan_fold_offsets(120, 60, 20))
        results = run_walk_forward(folds, [{'A': 1}], run_backtest,
                                   lambda m: m.get('sharpe'))
        assert all(r.chosen is None and r.test_metrics == {} for r in results)
        for fold in folds:
            assert (fold.test_start, fold.test_end) not in calls
