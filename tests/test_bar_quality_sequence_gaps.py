"""Multi-row quality scans retain locality and continue after bad inputs."""

import math

import numpy as np
import pandas as pd
import pytest

from trader.data.bar_quality import (
    Bar,
    check_series,
    impossible_mask,
    price_spikes,
    spacing_findings,
    unexplained_jumps,
)


def _bar(ts, price=8.0):
    return Bar(ts, price, price, price, price, 20.0)


def test_series_preserves_the_location_of_each_individual_bar_error():
    bars = [_bar(0.0), _bar(60.0)._replace(volume=-7.0), _bar(120.0)]

    findings = check_series(bars)

    assert [(f.rule, f.severity, f.index) for f in findings] == [
        ("negative_volume", "error", 1),
    ]
    assert "-7.0" in findings[0].detail


@pytest.mark.parametrize("missing", [None, "not-a-timestamp"])
def test_spacing_skips_an_unreadable_timestamp_before_a_later_fractional_gap(missing):
    # Quality rules report corrupt source data; a missing/unparseable stamp
    # cannot prevent the remaining real rows from being checked.
    findings = spacing_findings([_bar(missing), _bar(10.0), _bar(11.25)], 1.0)

    assert [(f.rule, f.index) for f in findings] == [("non_multiple_gap", 2)]


def test_spacing_uses_elapsed_time_from_a_nonzero_origin():
    # These are exactly one second apart, despite their fractional origin.
    assert spacing_findings([_bar(10.125), _bar(11.125)], 1.0) == []


def test_spacing_does_not_treat_backward_time_or_end_to_start_wrap_as_a_gap():
    # Sequence ordering is reported separately by check_series. Neither the
    # backwards first pair nor an invented last-to-first pair is an interval.
    assert spacing_findings([_bar(1.25), _bar(0.0), _bar(1.0)], 1.0) == []


def test_a_quiet_first_leg_does_not_hide_the_later_spike():
    bars = [_bar(60.0 * i, price) for i, price in enumerate([8.0, 8.0, 16.0, 8.0])]

    findings = price_spikes(bars)

    assert [(f.rule, f.severity, f.index) for f in findings] == [
        ("price_spike", "error", 2),
    ]


def test_zero_previous_price_does_not_divide_by_zero_or_hide_a_later_jump():
    bars = [_bar(60.0 * i, price) for i, price in enumerate([0.0, 8.0, 16.0])]

    findings = unexplained_jumps(bars)

    assert [(f.rule, f.index) for f in findings] == [("large_jump", 2)]


@pytest.mark.parametrize("missing", [math.inf, -math.inf, math.nan])
def test_nonfinite_current_price_is_not_reported_as_a_measured_jump(missing):
    bars = [_bar(60.0 * i, price)
            for i, price in enumerate([8.0, missing, 8.0, 16.0])]

    findings = unexplained_jumps(bars)

    assert [(f.rule, f.index) for f in findings] == [("large_jump", 3)]


@pytest.mark.parametrize("bits", [0x7FF8000000000000, 0x7FF0000000000001])
def test_nan_prices_remain_missing_under_the_callers_strict_numpy_policy(bits):
    # Quiet and signaling IEEE NaNs both describe missing data, never prices.
    # Construct an array directly to retain its payload through frame creation.
    prices = np.array([bits, 0x4020000000000000], dtype=np.uint64).view(np.float64)
    frame = pd.DataFrame({"open": prices, "high": [9.0, 9.0],
                          "low": [7.0, 7.0], "close": [8.0, 8.0],
                          "volume": [20.0, 20.0]}, index=[17, 18])

    with np.errstate(invalid="raise"):
        mask = impossible_mask(frame)

    pd.testing.assert_series_equal(mask, pd.Series([True, False], index=frame.index))
