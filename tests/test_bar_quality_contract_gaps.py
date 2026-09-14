"""Examples for the documented data-quality contract, outside the human spec.

These tests are staged privately until the current mutation run finishes.
Expected findings come from the stated market-data rules, not another copy of
the production predicates. Diagnostic assertions preserve useful facts without
pinning arbitrary prose or capitalization.
"""
import math

import pandas as pd
import pytest

from trader.data.bar_quality import (
    Bar,
    _finite,
    check_bar,
    check_series,
    explained_by_split,
    impossible_mask,
    price_spikes,
    spacing_findings,
    unexplained_jumps,
)


def flat(close=8.0, ts=0.0):
    return Bar(ts, close, close, close, close, 25.0)


def price_series(prices):
    return [flat(price, 60.0 * index) for index, price in enumerate(prices)]


def finding_keys(findings):
    return [(finding.rule, finding.severity, finding.index) for finding in findings]


@pytest.mark.parametrize("value", [None, "not-a-price"])
def test_malformed_numeric_input_is_not_finite(value):
    assert _finite(value) is False


@pytest.mark.parametrize(
    "changes,rule,detail_facts",
    [
        ({"open": math.nan, "high": math.nan, "low": math.nan, "close": math.nan},
         "empty_bar", ("placeholder",)),
        ({"high": math.nan}, "ohlc_partially_missing", ("high",)),
        ({"volume": math.inf}, "volume_not_finite", ("volume", "inf")),
        ({"open": 0.0}, "non_positive_price", ("open", "0.0")),
        ({"high": 0.0}, "non_positive_price", ("high", "0.0")),
        ({"low": -2.0}, "non_positive_price", ("low", "-2.0")),
        ({"close": -3.0}, "non_positive_price", ("close", "-3.0")),
        ({"volume": -25.0}, "negative_volume", ("volume", "-25.0")),
        ({"high": 7.0}, "high_below_low", ("high", "7.0", "low", "8.0")),
        ({"high": 7.0, "low": 6.0}, "high_not_highest", ("high", "7.0", "open", "close")),
        ({"high": 10.0, "low": 9.0}, "low_not_lowest", ("low", "9.0", "open", "close")),
    ],
)
def test_bar_findings_preserve_stable_identity_location_and_offending_values(changes, rule, detail_facts):
    findings = check_bar(flat()._replace(**changes), 17)
    selected = [finding for finding in findings if finding.rule == rule]
    assert len(selected) == 1
    finding = selected[0]
    assert finding.index == 17
    assert finding.severity == ("warn" if rule == "empty_bar" else "error")
    assert isinstance(finding.detail, str)
    for fact in detail_facts:
        assert fact in finding.detail.lower()


@pytest.mark.parametrize("invalid", [math.nan, math.inf, -math.inf])
def test_invalid_timestamp_keeps_its_finding_and_does_not_hide_later_sequence_errors(invalid):
    findings = check_series([flat(ts=invalid), flat(ts=120.0), flat(ts=60.0), flat(ts=60.0)])
    assert finding_keys(findings) == [
        ("timestamp_not_finite", "error", 0),
        ("timestamps_out_of_order", "error", 2),
        ("duplicate_timestamp", "error", 3),
    ]
    assert str(invalid) in findings[0].detail.lower()
    assert "120.0" in findings[1].detail and "60.0" in findings[1].detail
    assert "60.0" in findings[2].detail and "index 2" in findings[2].detail


def test_a_duplicate_is_not_also_reported_as_time_running_backward():
    findings = check_series([flat(ts=60.0), flat(ts=60.0)])
    assert finding_keys(findings) == [("duplicate_timestamp", "error", 1)]
    assert "index 0" in findings[0].detail


def test_mask_retains_datetime_index_and_coerces_bad_text_without_losing_good_rows():
    index = pd.DatetimeIndex(["2026-01-02T14:30:00Z", "2026-01-02T14:31:00Z", "2026-01-02T14:32:00Z"], name="bar_time")
    frame = pd.DataFrame({"open": ["8", "8", "8"], "high": ["9", "broken", "9"],
                          "low": ["7", "7", "7"], "close": ["8", "8", "8"],
                          "volume": ["25", "25", "broken"]}, index=index)
    expected = pd.Series([False, True, True], index=index)
    pd.testing.assert_series_equal(impossible_mask(frame), expected)
    assert frame.loc[impossible_mask(frame)].index.equals(index[1:])


@pytest.mark.parametrize("low", [0.0, -0.25])
def test_nonpositive_low_is_rejected_even_when_all_other_ohlc_ordering_is_valid(low):
    frame = pd.DataFrame({"open": [8.0], "high": [9.0], "low": [low], "close": [8.5], "volume": [25.0]}, index=[73])
    pd.testing.assert_series_equal(impossible_mask(frame), pd.Series([True], index=[73]))


@pytest.mark.parametrize("frame", [None, pd.DataFrame(), pd.DataFrame(columns=["open", "high", "low", "close"])])
def test_no_rows_produce_an_empty_boolean_mask(frame):
    pd.testing.assert_series_equal(impossible_mask(frame), pd.Series([], dtype=bool))


def test_missing_schema_has_a_false_boolean_mask_with_the_original_row_labels():
    frame = pd.DataFrame({"bid": [8.0, 8.1]}, index=pd.Index(["first", "second"], name="source_row"))
    pd.testing.assert_series_equal(impossible_mask(frame), pd.Series([False, False], index=frame.index))


def test_nullable_numeric_prices_and_placeholders_preserve_mask_alignment():
    frame = pd.DataFrame({
        "open": pd.array([8.0, pd.NA, pd.NA], dtype="Float64"),
        "high": pd.array([9.0, 9.0, pd.NA], dtype="Float64"),
        "low": pd.array([7.0, 7.0, pd.NA], dtype="Float64"),
        "close": pd.array([8.5, 8.5, pd.NA], dtype="Float64"),
        "volume": pd.array([25.0, 25.0, pd.NA], dtype="Float64"),
    }, index=[10, 20, 30])
    pd.testing.assert_series_equal(impossible_mask(frame), pd.Series([False, True, False], index=frame.index))


@pytest.mark.parametrize("interval", [math.inf, -math.inf, math.nan, 0.0, -1.0])
def test_spacing_with_an_unusable_interval_does_not_invent_findings(interval):
    assert spacing_findings([flat(ts=0.0), flat(ts=90.0)], interval) == []


def test_spacing_continues_after_unreadable_timestamps_and_reports_subsecond_steps():
    findings = spacing_findings([flat(ts=math.nan), flat(ts=0.0), flat(ts=0.75)], 0.5)
    assert finding_keys(findings) == [("non_multiple_gap", "warn", 2)]
    assert "0.75" in findings[0].detail and "0.5" in findings[0].detail


@pytest.mark.parametrize("step", [1e-6, 1.0 - 0.5e-6, 1.0 + 0.5e-6])
def test_spacing_tolerates_dust_on_both_sides_of_a_whole_interval(step):
    assert spacing_findings([flat(ts=0.0), flat(ts=step)], 1.0) == []


def test_spacing_reports_a_step_beyond_the_dust_tolerance():
    findings = spacing_findings([flat(ts=0.0), flat(ts=2e-6)], 1.0)
    assert finding_keys(findings) == [("non_multiple_gap", "warn", 1)]


def test_jump_default_threshold_reports_a_move_just_above_twenty_five_percent():
    findings = unexplained_jumps([flat(128.0), flat(161.0, 60.0)])
    assert finding_keys(findings) == [("large_jump", "warn", 1)]
    assert "128" in findings[0].detail and "161" in findings[0].detail
    assert "25.8%" in findings[0].detail


def test_jump_threshold_is_strict_and_small_positive_prices_are_supported():
    assert unexplained_jumps(price_series([8.0, 10.0]), threshold=0.25) == []
    assert finding_keys(unexplained_jumps(price_series([0.5, 1.0]))) == [("large_jump", "warn", 1)]


def test_jump_scan_continues_after_bad_input_and_includes_the_last_pair():
    findings = unexplained_jumps(price_series([math.nan, 8.0, 8.0, 16.0]))
    assert finding_keys(findings) == [("large_jump", "warn", 3)]


def test_jump_scan_does_not_compare_the_first_bar_to_the_last_bar():
    assert unexplained_jumps(price_series([100.0, 120.0, 144.0])) == []


@pytest.mark.parametrize("threshold", [math.nan, math.inf, 0.0, -0.25])
def test_jump_scan_refuses_an_unusable_threshold(threshold):
    assert unexplained_jumps(price_series([8.0, 16.0]), threshold=threshold) == []


@pytest.mark.parametrize("prices", [(100.0, 165.0, 125.0), (0.125, 0.25, 0.125)])
def test_spike_defaults_and_subdollar_prices_preserve_round_trip_detection(prices):
    findings = price_spikes(price_series(prices))
    assert finding_keys(findings) == [("price_spike", "error", 1)]
    assert isinstance(findings[0].detail, str)
    for price in prices:
        assert format(price, "g") in findings[0].detail


def test_spike_default_tolerance_does_not_erase_a_persistent_level_change():
    assert price_spikes(price_series([8.0, 20.0, 12.0])) == []


def test_spike_exact_threshold_and_return_tolerance_are_inclusive():
    assert finding_keys(price_spikes(price_series([8.0, 12.0, 8.0]), threshold=0.5)) == [("price_spike", "error", 1)]
    assert finding_keys(price_spikes(price_series([8.0, 16.0, 10.0]), tolerance=0.25)) == [("price_spike", "error", 1)]


@pytest.mark.parametrize("bad_price", [0.0, -1.0, math.nan, math.inf])
def test_spike_scan_continues_after_bad_prices_and_keeps_the_later_location(bad_price):
    findings = price_spikes(price_series([bad_price, 8.0, 16.0, 8.0]))
    assert finding_keys(findings) == [("price_spike", "error", 2)]


@pytest.mark.parametrize("field", ["threshold", "tolerance"])
@pytest.mark.parametrize("invalid", [math.nan, math.inf, 0.0, -0.5])
def test_spike_detection_requires_usable_positive_configuration(field, invalid):
    assert price_spikes(price_series([8.0, 16.0, 8.0]), **{field: invalid}) == []


@pytest.mark.parametrize("field", ["move", "split_to", "split_from"])
def test_split_explanation_refuses_nonfinite_reference_values(field):
    args = {"move": -0.5, "split_to": 2.0, "split_from": 1.0}
    args[field] = math.inf
    assert explained_by_split(**args) is False


def test_no_split_is_not_an_explanation_but_a_reverse_split_can_be():
    assert explained_by_split(0.0, 1.0, 1.0) is False
    assert explained_by_split(1.0, 1.0, 2.0) is True


def test_split_explanation_uses_relative_inclusive_tolerance():
    # A 2:1 split predicts -50%; a 25% relative tolerance permits +/-12.5%.
    assert explained_by_split(-0.375, 2.0, 1.0, tolerance=0.25) is True
    assert explained_by_split(-0.3125, 2.0, 1.0, tolerance=0.25) is False
