"""Calculation, alignment and provenance contracts for research utilities.

The synthetic examples have known arithmetic answers. Passing them does not
validate a strategy, a statistical model, or a profitability claim.
"""

import datetime as dt
import math

import numpy as np
import pytest

from trader.simulation import selection_bias
from trader.simulation.selection_bias import (
    _block_moments,
    _pooled_sharpe,
    align_equity_curves,
    deflated_sharpe,
    expected_max_sharpe,
    infer_periods_per_year,
    pbo_cscv,
)
from trader.simulation.walk_forward import (
    Fold,
    FoldOffsets,
    FoldResult,
    pick_best,
    plan_fold_offsets,
    resolve_folds,
    run_walk_forward,
    selection_stability,
)


@pytest.mark.parametrize(("trials", "expected"), [
    (1, 0.0),
    (2, 0.51975534428059389),
    (3, 0.85280449615069476),
    (5, 1.1925940010147891),
])
def test_expected_maximum_matches_reference_values_for_unit_trial_variance(trials, expected):
    # Reference values evaluate the documented Gaussian expected-maximum
    # approximation using an independent inverse-normal implementation.
    assert expected_max_sharpe(trials, 1.0) == pytest.approx(expected, abs=1e-12)


@pytest.mark.parametrize("variance", [-1.0, math.inf, math.nan])
def test_invalid_trial_variance_is_unknown_not_a_selection_benchmark(variance):
    assert expected_max_sharpe(2, variance) is None


def test_unresolvable_large_trial_count_is_unknown():
    # At this exact integer, one normal quantile argument rounds to one.
    # A nonfinite expected maximum cannot be used as a measured benchmark.
    assert expected_max_sharpe(2 ** 53, 1.0) is None


def test_default_deflation_preserves_units_sample_variance_and_three_real_returns():
    returns = np.array([-1.0, 0.0, 1.0, math.nan, math.inf])
    annualised = [0.0, 2.0 * math.sqrt(252.0)]

    result = deflated_sharpe(returns, annualised)

    # Three real returns have mean zero, so PSR reduces to Phi(-SR*sqrt(2)).
    # Per-period trial Sharpes [0, 2] have SAMPLE variance 2. The resulting
    # argument is -2*0.51975534428059389, whose normal CDF is below.
    assert result == pytest.approx(0.14928364483426521, rel=0, abs=1e-12)


def test_float32_trial_observations_keep_the_reference_deflation_precision():
    annualised = np.array([0.0, 1.0, 3.0], dtype=np.float32)

    result = deflated_sharpe(np.array([-1.0, 0.0, 1.0]), annualised)

    # Across-trial sample variance is 7/3; dividing by 252 gives 1/108.
    # The probability is Phi(-0.85280449615069476/sqrt(54)).
    assert result == pytest.approx(0.45380566801725009, rel=0, abs=1e-12)


@pytest.mark.parametrize("periods_per_year", [0.25, 1.0])
def test_positive_low_observation_frequencies_remain_evaluable(periods_per_year):
    annualised = [0.0, 2.0 * math.sqrt(periods_per_year)]

    result = deflated_sharpe(np.array([-1.0, 0.0, 1.0]), annualised,
                             bars_per_year=periods_per_year)

    assert result == pytest.approx(0.14928364483426521, rel=0, abs=1e-12)


@pytest.mark.parametrize("periods_per_year", [0.0, -1.0, math.inf, math.nan])
def test_invalid_observation_frequency_cannot_authorize_deflation(periods_per_year):
    assert deflated_sharpe(np.array([-1.0, 0.0, 1.0]), [0.0, 1.0],
                           bars_per_year=periods_per_year) is None


def test_single_trial_needs_no_undefined_sample_variance_under_strict_numpy_policy():
    with np.errstate(divide="raise", invalid="raise"):
        result = deflated_sharpe(np.array([-1.0, 0.0, 1.0]), [1.0])

    assert result == pytest.approx(0.5)


def test_two_observations_have_a_resolvable_pooled_sharpe():
    moments = _block_moments(np.array([[1.0, -2.0], [3.0, 2.0]]), 2)

    result = _pooled_sharpe(*moments, (0, 1))

    np.testing.assert_allclose(result, [math.sqrt(2), 0.0], rtol=1e-12, atol=0)


def test_pooled_noise_floor_does_not_erase_a_resolvable_binary_variance():
    delta = 2.0 ** -16
    matrix = np.array([[1.0 - delta], [1.0 + delta]] * 2)
    moments = _block_moments(matrix, 2)

    result = _pooled_sharpe(*moments, (0, 1))

    # Mean 1, sample variance 4*delta**2/3: Sharpe = sqrt(3)/(2*delta).
    np.testing.assert_allclose(result, [32768.0 * math.sqrt(3)], rtol=1e-12)


@pytest.mark.parametrize("constant", [0.0, 1.0])
def test_degenerate_pooled_columns_are_zero_under_strict_numpy_policy(constant):
    moments = _block_moments(np.full((4, 1), constant), 2)

    with np.errstate(divide="raise", invalid="raise"):
        result = _pooled_sharpe(*moments, (0, 1))

    np.testing.assert_array_equal(result, [0.0])


def _reversal_matrix(dtype=float, scale=1):
    # A wins the first two observations; B wins the second two. In each
    # complementary test half the training winner is strictly worst.
    return np.array([[1, -1], [3, 1], [-1, 1], [1, 3]], dtype=dtype) * scale


def _assert_two_block_reversal(result):
    assert result is not None
    assert result.pbo == 1.0
    assert result.n_splits == 2
    assert result.n_combinations == 2
    assert result.n_trials == 2
    assert result.n_observations == 4
    assert result.median_oos_rank == pytest.approx(1 / 3)
    assert result.logits == pytest.approx([-math.log(2), -math.log(2)])
    assert result.caveat is not None


def test_default_cscv_accepts_the_smallest_supported_matrix():
    _assert_two_block_reversal(pbo_cscv(_reversal_matrix()))


def test_explicit_two_split_cscv_keeps_the_same_known_rank_result():
    _assert_two_block_reversal(pbo_cscv(_reversal_matrix(), n_splits=2))


def test_integer_matrix_is_converted_before_squared_moments_can_overflow():
    # The numeric matrix API accepts integer arrays. Scaling returns by 100
    # cannot change ranks, but int16 squaring without conversion would wrap.
    matrix = _reversal_matrix(dtype=np.int16, scale=100)

    _assert_two_block_reversal(pbo_cscv(matrix, n_splits=2))


def test_split_shrinking_keeps_the_largest_supported_even_block_count():
    matrix = np.tile(np.array([[1.0, 3.0], [3.0, 1.0]]), (4, 1))

    result = pbo_cscv(matrix, n_splits=8)

    assert result is not None
    assert result.n_splits == 4
    assert result.n_combinations == 6
    assert result.n_observations == 8
    assert result.logits == pytest.approx([0.0] * 6)


@pytest.mark.parametrize("bad", [math.nan, math.inf, -math.inf])
def test_nonfinite_returns_are_zeroed_before_cscv_moments(bad, monkeypatch):
    matrix = np.array([[bad, -0.1], [0.0, 0.3], [1.0, -1.0], [3.0, 1.0]])
    observed = []
    actual_moments = selection_bias._block_moments

    def observe_input(values, n_splits):
        observed.append(values.copy())
        return actual_moments(values, n_splits)

    monkeypatch.setattr(selection_bias, "_block_moments", observe_input)

    result = pbo_cscv(matrix, n_splits=2)

    _assert_two_block_reversal(result)
    assert len(observed) == 1
    np.testing.assert_array_equal(observed[0], [
        [0.0, -0.1], [0.0, 0.3], [1.0, -1.0], [3.0, 1.0],
    ])


def test_median_ties_use_the_inclusive_pbo_rule_and_keep_report_counts():
    # Identical trials tie at relative rank 1/2 in every split. The current
    # PBO definition counts logit==0, so its result is 1, not an edge claim.
    matrix = np.tile(np.array([[1.0], [3.0], [-1.0], [1.0]]), (1, 20))

    result = pbo_cscv(matrix, n_splits=2)

    assert result is not None
    assert result.pbo == 1.0
    assert result.median_oos_rank == 0.5
    assert result.logits == [0.0, 0.0]
    assert result.n_trials == 20
    assert result.n_observations == 4
    assert result.caveat is None


def _curve(values, style="date"):
    start = dt.date(2024, 1, 1)
    suffix = {"date": "", "T": "T16:00:00+00:00", "space": " 09:00:00-05:00"}[style]
    return [{"timestamp": (start + dt.timedelta(days=i)).isoformat() + suffix,
             "value": value} for i, value in enumerate(values)]


def test_empty_curve_and_mixed_timestamp_formats_preserve_a_five_date_window():
    values = [100.0, 200.0, 400.0, 800.0, 1600.0]

    result = align_equity_curves([
        [], _curve(values, "date"), _curve(values, "T"), _curve(values, "space"),
    ])

    assert result is not None
    matrix, used, dropped = result
    assert (used, dropped) == (3, 0)
    np.testing.assert_array_equal(matrix, np.ones((4, 3)))


def test_minority_extra_dates_do_not_redefine_the_modal_window():
    ten = [100.0 * 2 ** i for i in range(10)]
    twelve = [100.0 * 2 ** i for i in range(12)]
    curves = [_curve(ten) for _ in range(8)] + [_curve(twelve), _curve(twelve)]

    result = align_equity_curves(curves)

    assert result is not None
    matrix, used, dropped = result
    assert (used, dropped) == (10, 0)
    np.testing.assert_array_equal(matrix, np.ones((9, 10)))


def test_exact_ninety_percent_coverage_survives_and_carries_an_interior_mark():
    full = _curve([100.0 * 2 ** i for i in range(10)])
    missing_one = full[:4] + full[5:]
    curves = [list(full) for _ in range(9)] + [missing_one, full[:5]]

    result = align_equity_curves(curves)

    assert result is not None
    matrix, used, dropped = result
    assert (used, dropped) == (10, 1)
    expected = np.ones((9, 10))
    expected[3, 9] = 0.0       # the unmarked date carries the prior equity
    expected[4, 9] = 3.0       # the next mark records the full two-day move
    np.testing.assert_array_equal(matrix, expected)


def test_leading_unobserved_mark_is_zero_return_then_observed_growth():
    full = _curve([100.0 * 2 ** i for i in range(10)])
    curves = [list(full) for _ in range(9)] + [full[1:]]

    result = align_equity_curves(curves)

    assert result is not None
    matrix, used, dropped = result
    assert (used, dropped) == (10, 0)
    expected = np.ones((9, 10))
    expected[0, 9] = 0.0
    np.testing.assert_array_equal(matrix, expected)


def test_a_curve_with_one_finite_mark_needs_no_second_mark_to_clean_returns():
    result = align_equity_curves([
        _curve([math.nan, math.nan, 100.0, math.nan, math.nan]),
        _curve([100.0, 200.0, 400.0, 800.0, 1600.0]),
    ])

    assert result is not None
    matrix, used, dropped = result
    assert (used, dropped) == (2, 0)
    np.testing.assert_array_equal(matrix, np.column_stack([np.zeros(4), np.ones(4)]))


def test_undefined_aligned_returns_are_zero_under_strict_numpy_policy():
    curves = [_curve([0.0, 0.0, 1.0, 0.0, -1.0, 0.0]), _curve([100.0] * 6)]

    with np.errstate(divide="raise", invalid="raise"):
        result = align_equity_curves(curves)

    assert result is not None
    matrix, used, dropped = result
    assert (used, dropped) == (2, 0)
    np.testing.assert_array_equal(matrix, [[0, 0], [0, 0], [-1, 0], [0, 0], [-1, 0]])


def test_frequency_uses_all_three_real_unsorted_timestamps_after_a_bad_one():
    result = infer_periods_per_year([
        "not-a-timestamp", "2024-01-03T00:00:00+00:00",
        "2024-01-01T00:00:00+00:00", "2024-01-02T00:00:00+00:00",
    ])

    # Three observations over two calendar days: 3 * 365.25 / 2.
    assert result == pytest.approx(547.875, rel=0, abs=1e-12)


def test_three_identical_timestamps_do_not_imply_an_infinite_frequency():
    assert infer_periods_per_year(["2024-01-01T00:00:00+00:00"] * 3) is None


def test_default_fold_plan_rolls_and_covers_each_complete_test_interval():
    assert plan_fold_offsets(14, 4, 3) == [
        FoldOffsets(0, 0, 4, 4, 7),
        FoldOffsets(1, 3, 7, 7, 10),
        FoldOffsets(2, 6, 10, 10, 13),
    ]


def test_calendar_projection_preserves_fold_identity_and_warmup_provenance():
    result = resolve_folds(dt.date(2024, 2, 27), [FoldOffsets(7, 2, 5, 5, 9)],
                           warmup_days=2)

    assert result == [Fold(
        7, dt.date(2024, 2, 29), dt.date(2024, 3, 3),
        dt.date(2024, 3, 3), dt.date(2024, 3, 7),
        dt.date(2024, 2, 27), dt.date(2024, 3, 1),
    )]


def _fold():
    return Fold(7, dt.date(2024, 1, 3), dt.date(2024, 1, 7),
                dt.date(2024, 1, 7), dt.date(2024, 1, 10),
                dt.date(2024, 1, 1), dt.date(2024, 1, 5))


@pytest.mark.parametrize(("second", "expected"), [({"lookback": 5}, 1.0),
                                                   ({"lookback": 10}, 0.0)])
def test_two_real_fold_choices_have_defined_stability(second, expected):
    results = [FoldResult(_fold(), {"lookback": 5}, 1.0, {}),
               FoldResult(_fold(), second, 2.0, {})]

    assert selection_stability(results) == expected


def test_minimising_runner_preserves_the_later_winner_score_metrics_and_callback():
    fold = _fold()
    cells = [{"lookback": 5}, {"lookback": 10}]
    calls, notifications = [], []
    heldout = {"return": -0.25, "trades": 2, "window": "heldout"}

    def run(cell, start, end):
        calls.append((dict(cell), start, end))
        if (start, end) == (fold.train_data_start, fold.train_end):
            return {"loss": 4.0 if cell == cells[0] else 1.0}
        assert (start, end) == (fold.test_data_start, fold.test_end)
        return heldout

    results = run_walk_forward([fold], cells, run, lambda metrics: metrics["loss"],
                               higher_is_better=False, on_fold=notifications.append)

    assert results == [FoldResult(fold, cells[1], 1.0, heldout)]
    assert notifications == results
    assert calls == [
        (cells[0], fold.train_data_start, fold.train_end),
        (cells[1], fold.train_data_start, fold.train_end),
        (cells[1], fold.test_data_start, fold.test_end),
    ]


def test_unmeasured_fold_has_unknown_score_and_no_heldout_backtest():
    fold = _fold()
    calls = []

    def run(cell, start, end):
        calls.append((cell, start, end))
        return None

    def score(_metrics):
        raise AssertionError("absent metrics must not receive a made-up score")

    result = run_walk_forward([fold], [{"lookback": 5}], run, score)

    assert result == [FoldResult(fold, None, None, {})]
    assert calls == [({"lookback": 5}, fold.train_data_start, fold.train_end)]


def test_missing_heldout_metrics_remain_an_empty_metrics_mapping():
    fold = _fold()

    def run(_cell, start, end):
        return {"score": 2.0} if end == fold.train_end else None

    result = run_walk_forward([fold], [{"lookback": 5}], run,
                              lambda metrics: metrics["score"])

    assert result == [FoldResult(fold, {"lookback": 5}, 2.0, {})]


def test_direct_selection_maximises_by_default_and_minimum_ties_keep_grid_order():
    first, second, third = {"lookback": 5}, {"lookback": 10}, {"lookback": 20}

    assert pick_best([(first, 1.0), (second, 3.0)]) == second
    assert pick_best([(first, 4.0), (second, 1.0), (third, 1.0)],
                     higher_is_better=False) == second


@pytest.mark.timeout(2)
@pytest.mark.parametrize("n_splits", [10 ** 12, 10 ** 100],
                         ids=["trillion-splits", "ten-to-hundred-splits"])
def test_large_even_split_request_shrinks_to_available_two_blocks(n_splits):
    # Only four observations are available. A huge valid request must retain
    # the same two complementary reversals as an explicit two-block request.
    _assert_two_block_reversal(pbo_cscv(_reversal_matrix(), n_splits=n_splits))


def test_negative_warmup_identifies_the_rejected_input():
    with pytest.raises(ValueError) as rejected:
        resolve_folds(dt.date(2024, 1, 1), [], warmup_days=-7)

    explanation = str(rejected.value)
    assert "warmup_days" in explanation
    assert "-7" in explanation
    assert ">= 0" in explanation
