"""Numeric panel and signal contracts, without imposing a storage dtype.

Expected values come from small stated portfolios and cross-sections. The
subnormal-price case exercises the existing arithmetic fallback; it makes no
claim that such a value is a tradable broker quote.
"""

import math

import numpy as np
import pandas as pd
import pytest

from trader.simulation.panel import (
    apply_no_trade_band,
    buffered_membership,
    normalise_weights,
    rebalance_orders,
    target_positions,
)
from trader.simulation.signal_combine import (
    combine,
    combined_ic_estimate,
    cross_sectional_zscore,
    neutralise,
    signal_correlations,
)


def _unit_row():
    # Five observations with mean zero and sample variance 4 / (5 - 1) == 1.
    return pd.DataFrame([[-1.0, -1.0, 0.0, 1.0, 1.0]], columns=list("ABCDE"))


def test_five_names_with_unit_dispersion_are_usable_by_default():
    frame = _unit_row()

    pd.testing.assert_frame_equal(cross_sectional_zscore(frame), frame,
                                  check_dtype=False)
    pd.testing.assert_frame_equal(combine({"only": frame}), frame,
                                  check_dtype=False)


def test_combination_honours_a_stricter_requested_name_floor():
    result = combine({"only": _unit_row()}, min_names=6)

    assert result is not None
    assert result.shape == (1, 5)
    assert result.isna().all().all()


def test_explicit_weights_do_not_admit_unspecified_signals():
    chosen = _unit_row()

    result = combine({"chosen": chosen, "unrequested": -chosen},
                     weights={"chosen": 2.0})

    pd.testing.assert_frame_equal(result, chosen, check_dtype=False)


@pytest.mark.parametrize("weight", [0.0, math.nan, math.inf, -math.inf])
def test_skipped_first_signal_does_not_suppress_later_contribution(weight):
    chosen = _unit_row()

    result = combine({"skipped": -chosen, "chosen": chosen},
                     weights={"skipped": weight, "chosen": 1.0})

    pd.testing.assert_frame_equal(result, chosen, check_dtype=False)


def test_a_missing_first_signal_adds_no_view_to_an_observed_name():
    first = pd.DataFrame([[np.nan, -1.0, 0.0, 1.0]], columns=list("ABCD"))
    second = pd.DataFrame([[1.0, -1.0, 0.0, np.nan]], columns=list("ABCD"))

    result = combine({"first": first, "second": second}, min_names=3)

    # Each three-name signal already has mean zero and sample deviation one.
    expected = pd.DataFrame([[1.0, -1.0, 0.0, 1.0]], columns=list("ABCD"))
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_unaligned_dates_and_names_keep_only_real_contributions():
    dates = pd.date_range("2024-01-01", periods=3, freq="D")
    first = pd.DataFrame([[-1.0, 0.0, 1.0]] * 2,
                         index=dates[:2], columns=list("ABC"))
    second = pd.DataFrame([[-1.0, 0.0, 1.0]] * 2,
                          index=dates[1:], columns=list("BCD"))

    result = combine({"first": first, "second": second}, min_names=3)

    expected = pd.DataFrame([
        [-1.0, 0.0, 1.0, np.nan],
        [-1.0, -0.5, 0.5, 1.0],
        [np.nan, -1.0, 0.0, 1.0],
    ], index=dates, columns=list("ABCD"))
    pd.testing.assert_frame_equal(result, expected, check_dtype=False,
                                  check_freq=False)


def test_constant_cross_sections_remain_missing_without_a_dtype_promise():
    constant = pd.DataFrame([[4.0] * 5], columns=list("ABCDE"))

    zscores = cross_sectional_zscore(constant)
    combined = combine({"constant": constant})

    assert zscores.shape == combined.shape == (1, 5)
    assert zscores.isna().all().all()
    assert combined.isna().all().all()


@pytest.mark.parametrize(("mean_ic", "n_signals"), [
    (0.02, 0), (0.02, -1), (math.inf, 2), (-math.inf, 2), (math.nan, 2),
])
def test_one_invalid_estimate_input_is_enough_to_make_it_unknown(mean_ic, n_signals):
    assert combined_ic_estimate(mean_ic, n_signals, 0.0) is None


@pytest.mark.parametrize("correlation", [-1.25, 1.25])
def test_single_signal_does_not_make_an_invalid_correlation_acceptable(correlation):
    assert combined_ic_estimate(0.03, 1, correlation) is None


@pytest.mark.parametrize("correlation", [-1.0, 1.0])
def test_single_signal_accepts_both_valid_correlation_boundaries(correlation):
    assert combined_ic_estimate(-0.03, 1, correlation) == pytest.approx(0.03)


def test_ten_name_nonlinear_monotone_pair_has_rank_correlation_one():
    first = pd.DataFrame([list(range(10))])
    second = pd.DataFrame([[n * n for n in range(10)]])

    result = signal_correlations({"rank": first, "square": second})

    # Squaring these nonnegative ranks preserves every ordering, despite a
    # Pearson coefficient below one for the nonlinear numeric relationship.
    assert result == pytest.approx({"rank|square": 1.0})


def test_correlation_summary_samples_every_fifth_common_period():
    dates = pd.date_range("2024-01-01", periods=11, freq="D")
    first = pd.DataFrame([list(range(10))] * 11, index=dates)
    second = first.copy()
    second.loc[dates[5]] = list(reversed(range(10)))

    result = signal_correlations({"first": first, "second": second})

    # Common periods 0, 5, 10 have rank correlations +1, -1, +1.
    assert result == pytest.approx({"first|second": 1 / 3})


@pytest.mark.parametrize("constant_side", ["first", "second"])
def test_constant_signal_pair_is_absent_instead_of_a_nan_estimate(constant_side):
    first = pd.DataFrame([list(range(10))])
    second = first.copy()
    if constant_side == "first":
        first.loc[0] = 4.0
    else:
        second.loc[0] = 4.0

    assert signal_correlations({"first": first, "second": second}) == {}


def test_two_distinct_signal_values_still_define_an_ordering():
    binary = pd.DataFrame([[0.0] * 5 + [1.0] * 5])

    assert signal_correlations({"binary": binary, "copy": binary.copy()}) == \
        pytest.approx({"binary|copy": 1.0})


def test_singleton_sector_does_not_stop_later_sector_neutralisation():
    signal = pd.DataFrame([[99.0, 2.0, 6.0, 7.0]], columns=[101, 102, 103, 104])

    result = neutralise(signal, {101: "singleton", 102: "pair", 103: "pair"})

    expected = pd.DataFrame([[99.0, -2.0, 2.0, 7.0]], columns=signal.columns)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


@pytest.mark.parametrize(("equity", "price", "expected"), [
    (0.5, 1.0, 0.25),
    (100.0, 0.25, 200.0),
])
def test_small_positive_equity_and_prices_support_fractional_sizing(equity, price, expected):
    assert target_positions({101: 0.5}, equity, {101: price},
                            allow_fractional=True) == {101: expected}


def test_zero_target_survives_missing_price_and_keeps_later_instructions():
    assert target_positions({101: 0.0, 202: 0.5}, 100.0, {202: 10.0}) == {
        101: 0.0, 202: 5.0,
    }


@pytest.mark.parametrize("weight", [math.nan, math.inf, -math.inf])
def test_invalid_weight_is_not_sized_and_does_not_abort_later_name(weight):
    result = target_positions({101: weight, 202: 0.5}, 100.0,
                              {101: 10.0, 202: 10.0})

    assert result == {101: 0.0, 202: 5.0}


@pytest.mark.parametrize("price", [None, 0.0, math.nan, math.inf])
def test_unpriceable_first_name_does_not_hide_later_target(price):
    prices = {202: 10.0}
    if price is not None:
        prices[101] = price

    assert target_positions({101: 0.5, 202: 0.5}, 100.0, prices) == {202: 5.0}


def test_default_rebalance_threshold_keeps_nonzero_fractional_deltas():
    assert rebalance_orders({101: 10.0, 202: 10.0}, {101: 10.5, 202: 9.5}) == {
        101: 0.5, 202: -0.5,
    }


@pytest.mark.parametrize(("current", "target"), [
    (math.inf, 2.0), (2.0, math.inf), (math.nan, 2.0), (2.0, math.nan),
])
def test_one_nonfinite_rebalance_value_does_not_hide_later_valid_order(current, target):
    result = rebalance_orders({101: current, 202: 3.0}, {101: target, 202: 7.0})

    assert result == {202: 4.0}


@pytest.mark.parametrize("small_delta", [0.0, 0.25])
def test_dust_first_delta_does_not_suppress_later_rebalance(small_delta):
    result = rebalance_orders({101: 10.0, 202: 1.0},
                              {101: 10.0 + small_delta, 202: 3.0}, min_shares=0.5)

    assert result == {202: 2.0}


@pytest.mark.parametrize("bad_rank", [math.nan, math.inf, -math.inf])
def test_invalid_first_rank_is_not_admitted_and_does_not_end_membership_scan(bad_rank):
    result = buffered_membership({101: bad_rank, 202: 0.875}, frozenset(),
                                 enter_pct=0.25, exit_pct=0.5)

    assert result == frozenset({202})


def test_membership_includes_exact_entry_and_held_exit_thresholds():
    ranks = {101: 0.75, 202: 0.5,
             303: math.nextafter(0.75, 0.0), 404: math.nextafter(0.5, 0.0)}

    result = buffered_membership(ranks, frozenset({202, 404}),
                                 enter_pct=0.25, exit_pct=0.5)

    assert result == frozenset({101, 202})


def test_default_gross_budget_is_one_and_preserves_relative_signed_weights():
    assert normalise_weights({101: 0.9, 202: -0.6}) == pytest.approx({
        101: 0.6, 202: -0.4,
    })


@pytest.mark.parametrize(("current", "target"), [(math.nan, 10.0), (10.0, math.inf)])
def test_invalid_first_band_position_does_not_hide_later_adjustment(current, target):
    result = apply_no_trade_band({101: current, 202: 10.0},
                                {101: target, 202: 12.0}, band=0.25)

    assert result == {202: 10.0}


@pytest.mark.parametrize(("current", "target"), [(0.0, 5.0), (5.0, 0.0)])
def test_first_entry_or_exit_does_not_stop_later_band_decision(current, target):
    result = apply_no_trade_band({101: current, 202: 10.0},
                                {101: target, 202: 12.0}, band=0.25)

    assert result == {101: target, 202: 10.0}


@pytest.mark.parametrize(("current", "target"), [
    (0.0, 0.5), (0.0, -0.5), (0.5, -0.5), (-0.5, 0.5),
])
def test_even_a_large_valid_band_cannot_suppress_an_entry_or_sign_flip(current, target):
    # The public precondition allows every nonnegative band, including > 1.
    assert apply_no_trade_band({101: current}, {101: target}, band=3.0) == {
        101: target,
    }


@pytest.mark.parametrize(("current", "target"), [
    (1.0, 1.01), (1.01, 1.0), (0.99, 1.01), (1.01, 0.99), (0.5, 0.51),
])
def test_small_same_side_adjustments_are_banded_around_one_share(current, target):
    assert apply_no_trade_band({101: current}, {101: target}, band=0.1) == {
        101: current,
    }


def test_zero_price_uses_the_share_band_instead_of_forcing_a_trade():
    assert apply_no_trade_band({101: 100.0}, {101: 101.0}, band=0.1,
                               prices={101: 0.0}) == {101: 100.0}


def test_small_positive_notional_still_uses_the_relative_band():
    assert apply_no_trade_band({101: 100.0}, {101: 101.0}, band=0.1,
                               prices={101: 0.005}) == {101: 100.0}


def test_underflowed_notional_retains_the_requested_target_without_division():
    # A positive binary64 price can underflow both products to zero. This is
    # a numeric robustness control, not a claim about valid exchange quotes.
    assert apply_no_trade_band({101: 0.5}, {101: 0.25}, band=2.0,
                               prices={101: 5e-324}) == {101: 0.25}
