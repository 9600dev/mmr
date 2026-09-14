"""Research input contracts with independent expected results.

The quantile exception case deliberately injects a library failure. It pins
continuation after that failure, without claiming an ordinary ranked row can
make the installed pandas implementation raise it.
"""

import datetime as dt
import math
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from trader.data.fundamentals import (
    Filing,
    extract_concepts,
    extract_polygon,
    latest_known,
    reporting_lag_days,
    safe_ratio,
)
from trader.simulation.signal_eval import (
    information_coefficient,
    period_ic,
    periods_needed_for_significance,
    quantile_returns,
    summarise_ic,
)


def _filing(conid, accession, accepted_at, period_end, *, amendment=False):
    return Filing(
        conid=conid,
        cik="0000000001",
        ticker="SYNTHETIC",
        form_type="10-Q/A" if amendment else "10-Q",
        accession_no=accession,
        accepted_at=accepted_at,
        period_end=period_end,
        is_amendment=amendment,
    )


@pytest.mark.parametrize("hour", [0, 23])
def test_same_day_filing_has_zero_reporting_lag(hour):
    period_end = dt.date(2024, 2, 29)
    accepted = dt.datetime(2024, 2, 29, hour, tzinfo=dt.timezone.utc)

    assert reporting_lag_days(period_end, accepted) == 0


@pytest.mark.parametrize("unusable_time", ["exact_boundary", "future", "naive"])
def test_unavailable_filing_does_not_hide_later_usable_input(unusable_time):
    as_of = dt.datetime(2024, 11, 10, 12, tzinfo=dt.timezone.utc)
    unavailable = {
        "exact_boundary": as_of,
        "future": as_of + dt.timedelta(hours=1),
        "naive": as_of.replace(tzinfo=None),
    }[unusable_time]
    unknown = _filing(101, "unknown", unavailable, dt.date(2024, 9, 30))
    known = _filing(
        202, "known", as_of - dt.timedelta(days=2), dt.date(2024, 9, 30)
    )

    assert latest_known([unknown, known], as_of) == {202: known}


@pytest.mark.parametrize("order", [(0, 1, 2), (1, 2, 0), (2, 0, 1)])
def test_latest_acceptance_wins_independent_of_input_and_period_order(order):
    as_of = dt.datetime(2024, 11, 10, tzinfo=dt.timezone.utc)
    amended = _filing(
        101, "amendment", dt.datetime(2024, 11, 1, tzinfo=dt.timezone.utc),
        dt.date(2024, 6, 30), amendment=True,
    )
    original = _filing(
        101, "original", dt.datetime(2024, 10, 25, tzinfo=dt.timezone.utc),
        dt.date(2024, 9, 30),
    )
    other = _filing(
        202, "other", dt.datetime(2024, 10, 28, tzinfo=dt.timezone.utc),
        dt.date(2024, 9, 30),
    )
    filings = (amended, original, other)

    assert latest_known([filings[i] for i in order], as_of) == {
        101: amended,
        202: other,
    }


@pytest.mark.parametrize("denominator", [math.inf, -math.inf, math.nan])
def test_nonfinite_ratio_denominator_is_unknown_not_a_rankable_zero(denominator):
    assert safe_ratio(7.5, denominator) is None


def test_real_zero_and_negative_fundamental_ratios_remain_measured():
    assert safe_ratio(0.0, 3.0) == 0.0
    assert safe_ratio(-6.0, 3.0) == -2.0


@pytest.mark.parametrize(
    "primary", ["not reported", {"raw": 7}, math.inf, math.nan],
    ids=["invalid_text", "invalid_shape", "infinite", "nan"],
)
def test_polygon_equity_uses_valid_alias_after_unusable_primary(primary):
    financials = SimpleNamespace(balance_sheet=SimpleNamespace(
        equity=SimpleNamespace(value=primary),
        equity_attributable_to_parent=SimpleNamespace(value="125.5"),
    ))

    result = extract_polygon(financials)

    assert result["stockholders_equity"] == 125.5
    assert result["total_assets"] is None
    assert result["revenue"] is None


@pytest.mark.parametrize(
    "primary_series", [[], {"value": "wrong series shape"}],
    ids=["empty_list", "mapping_instead_of_list"],
)
def test_xbrl_unusable_series_does_not_hide_valid_alias(primary_series):
    statements = {"BalanceSheet": {
        "StockholdersEquity": primary_series,
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest": [
            {"value": "75.25"},
            {"value": "999.0"},
        ],
    }}

    result = extract_concepts(statements)

    # The converter's first period is the current one; the second is older.
    assert result["stockholders_equity"] == 75.25
    assert result["total_assets"] is None


@pytest.mark.parametrize("primary", ["not reported", None])
def test_xbrl_conversion_failure_does_not_hide_valid_alias(primary):
    statements = {"IncomeStatement": {
        "NetIncomeLoss": [{"value": primary}],
        "ProfitLoss": [{"value": "-12.5"}],
    }}

    result = extract_concepts(statements)

    assert result["net_income"] == -12.5
    assert result["revenue"] is None


@pytest.mark.parametrize(
    ("mean_ic", "std_ic"),
    [
        (math.inf, 0.25), (-math.inf, 0.25), (math.nan, 0.25),
        (0.25, math.inf), (0.25, -math.inf), (0.25, math.nan),
    ],
)
def test_nonfinite_ic_inputs_cannot_certify_zero_required_periods(mean_ic, std_ic):
    assert periods_needed_for_significance(mean_ic, std_ic) is None


def test_significance_period_count_retains_a_known_finite_answer():
    # A signal-to-noise ratio of 1/2 needs sqrt(n) == 4 to reach t == 2.
    assert periods_needed_for_significance(0.25, 0.5) == 16.0
    assert periods_needed_for_significance(-0.25, 0.5) == 16.0


@pytest.mark.parametrize("bad_value", [math.inf, -math.inf])
def test_nonfinite_pearson_result_is_unknown_not_nan(bad_value):
    signal = pd.Series([1.0, 2.0, 3.0, 4.0, bad_value, 6.0])
    forward = pd.Series([0.02, 0.04, 0.01, 0.03, 0.05, 0.06])

    # Infinities have distinct ranks but cannot define a finite covariance.
    # Suppress only NumPy's expected invalid-arithmetic warning for this input.
    with np.errstate(invalid="ignore"):
        result = period_ic(signal, forward, method="pearson")

    assert result is None


def test_all_unscoreable_periods_return_an_empty_numeric_ic_series():
    dates = pd.date_range("2024-01-01", periods=3, freq="D")
    signal = pd.DataFrame([[1, 2, 3, 4, 5, 6]] * 3, index=dates)
    prices = pd.DataFrame([[100.0] * 6] * 3, index=dates)

    result = information_coefficient(signal, prices)

    # Two observable periods have constant returns, and the last has none.
    assert result.empty
    assert result.dtype == np.dtype("float64")


def test_float32_ic_observations_are_summarised_with_float64_precision():
    values = pd.Series([-1.0, math.nan, 0.25, 1.0], dtype="float32")

    result = summarise_ic(values)

    # Three exactly represented ICs: mean=1/12, sample variance=49/48.
    # Hence t=1/7, two positive periods, and IR=sqrt(3)/21.
    assert result.n_periods == 3
    assert result.mean_ic == pytest.approx(1 / 12, rel=0, abs=1e-12)
    assert result.std_ic == pytest.approx(7 / (4 * math.sqrt(3)), rel=0, abs=1e-12)
    assert result.t_stat == pytest.approx(1 / 7, rel=0, abs=1e-12)
    assert result.naive_t_stat == pytest.approx(1 / 7, rel=0, abs=1e-12)
    assert result.hit_rate == pytest.approx(2 / 3, rel=0, abs=1e-12)
    assert result.ir == pytest.approx(math.sqrt(3) / 21, rel=0, abs=1e-12)


def test_one_injected_bucket_failure_does_not_drop_later_usable_period(monkeypatch):
    dates = pd.date_range("2024-01-01", periods=3, freq="D")
    signal = pd.DataFrame([[1, 2, 3, 4, 5, 6]] * 3, index=dates)
    prices = pd.DataFrame([
        [100.0] * 6,
        [101.0, 102.0, 103.0, 104.0, 105.0, 106.0],
        [102.01, 104.04, 106.09, 108.16, 110.25, 112.36],
    ], index=dates)
    actual_qcut = pd.qcut

    def one_unavailable_period(values, *args, **kwargs):
        if values.name == dates[0]:
            raise ValueError("injected per-period quantile failure")
        return actual_qcut(values, *args, **kwargs)

    monkeypatch.setattr(pd, "qcut", one_unavailable_period)

    result = quantile_returns(signal, prices, n_buckets=2)

    assert result.index.equals(pd.DatetimeIndex([dates[1]]))
    assert set(result.columns) == {0, 1}
    assert result.loc[dates[1]].to_dict() == pytest.approx({0: 0.02, 1: 0.05})
