"""Research reports must preserve measured values, horizons and unknowns."""
import math

import pandas as pd
import pytest

from trader.simulation.signal_eval import evaluate


def _alternating_rank_panel():
    """Three observable two-day returns: 1%, 2%, ..., 6% in every row.

    Ascending/descending/ascending signals give ICs [1, -1, 1]. The last
    two signal rows have no observable two-day return and must not add ICs.
    """
    index = pd.date_range('2020-01-01', periods=5, freq='D')
    columns = list('ABCDEF')
    prices = pd.DataFrame([
        [100, 100, 100, 100, 100, 100],
        [100, 100, 100, 100, 100, 100],
        [101, 102, 103, 104, 105, 106],
        [101, 102, 103, 104, 105, 106],
        [102.01, 104.04, 106.09, 108.16, 110.25, 112.36],
    ], index=index, columns=columns)
    ascending = [1, 2, 3, 4, 5, 6]
    descending = [6, 5, 4, 3, 2, 1]
    signal = pd.DataFrame([
        ascending, descending, ascending, ascending, ascending,
    ], index=index, columns=columns)
    return signal, prices


def test_report_preserves_horizon_corrected_statistics_and_bucket_spread():
    signal, prices = _alternating_rank_panel()
    report = evaluate(signal, prices, horizon=2, n_buckets=2)

    # ICs [1, -1, 1] have mean 1/3 and sample variance 4/3, giving naive
    # t=1/2. The lag-one Bartlett estimate has variance of the mean 8/81,
    # hence corrected t=3/(2*sqrt(2)). None of these expectations call the
    # component helpers that assemble this report.
    expected = {
        'horizon': 2,
        'n_periods': 3,
        'periods_available': 3,
        'mean_ic': 1 / 3,
        'ic_t_stat': 3 / (2 * math.sqrt(2)),
        'ic_t_stat_naive': 0.5,
        'ic_hit_rate': 2 / 3,
        'ic_ir': math.sqrt(3) / 6,
        'periods_needed_for_t2': 48,
        # The bottom bucket returns [2%, 5%, 2%], top [5%, 2%, 5%].
        'top_minus_bottom': 0.01,
        # Rank changes are [1/2, 1/2, 0, 0] over four adjacent periods.
        'mean_turnover': 0.25,
    }
    assert set(report) == set(expected) | {'bucket_means'}
    for key, value in expected.items():
        assert report[key] == pytest.approx(value), key
    assert report['bucket_means'] == pytest.approx({0: 0.03, 1: 0.04})


def test_default_report_uses_one_day_and_five_buckets_without_inventing_precision():
    index = pd.date_range('2020-01-01', periods=3, freq='D')
    # Ten instruments compound at fixed daily returns 1%, ..., 10%.
    # There are exactly enough names for five two-name buckets.
    prices = pd.DataFrame([
        [100.0 * (1 + i / 100) ** day for i in range(1, 11)]
        for day in range(3)
    ], index=index)
    signal = pd.DataFrame([list(range(1, 11))] * 3, index=index)
    report = evaluate(signal, prices)

    assert report['horizon'] == 1
    assert report['n_periods'] == report['periods_available'] == 2
    assert report['mean_ic'] == pytest.approx(1.0)
    assert report['ic_hit_rate'] == 1.0
    assert report['bucket_means'] == pytest.approx({
        0: 0.015, 1: 0.035, 2: 0.055, 3: 0.075, 4: 0.095,
    })
    assert report['top_minus_bottom'] == pytest.approx(0.08)
    assert report['mean_turnover'] == 0.0
    # A constant IC has no estimated dispersion; it is not an infinite t-stat.
    assert report['ic_t_stat'] is None
    assert report['ic_t_stat_naive'] is None
    assert report['ic_ir'] is None
    assert report['periods_needed_for_t2'] is None


def test_empty_report_keeps_unobserved_statistics_unknown():
    signal, prices = _alternating_rank_panel()
    report = evaluate(signal.iloc[:0], prices.iloc[:0], horizon=2, n_buckets=2)

    assert report == {
        'horizon': 2,
        'n_periods': 0,
        'periods_available': 0,
        'mean_ic': None,
        'ic_t_stat': None,
        'ic_t_stat_naive': None,
        'ic_hit_rate': None,
        'ic_ir': None,
        'periods_needed_for_t2': None,
        'top_minus_bottom': None,
        'bucket_means': {},
        'mean_turnover': None,
    }


def test_one_usable_period_is_measured_but_has_no_significance_estimate():
    signal, prices = _alternating_rank_panel()
    report = evaluate(signal.iloc[:3], prices.iloc[:3], horizon=2, n_buckets=2)

    assert report['n_periods'] == report['periods_available'] == 1
    assert report['mean_ic'] == pytest.approx(1.0)
    assert report['ic_hit_rate'] == 1.0
    assert report['top_minus_bottom'] == pytest.approx(0.03)
    assert report['ic_t_stat'] is None
    assert report['ic_t_stat_naive'] is None
    assert report['ic_ir'] is None
    assert report['periods_needed_for_t2'] is None


def test_zero_information_and_spread_remain_measured_zeros():
    signal, prices = _alternating_rank_panel()
    report = evaluate(signal.iloc[:4], prices.iloc[:4], horizon=2, n_buckets=2)

    # ICs [1, -1] cancel. Equal top/bottom means are a measured zero spread.
    assert report['n_periods'] == report['periods_available'] == 2
    assert report['mean_ic'] == pytest.approx(0.0)
    assert report['ic_t_stat'] == pytest.approx(0.0)
    assert report['ic_t_stat_naive'] == pytest.approx(0.0)
    assert report['ic_ir'] == pytest.approx(0.0)
    assert report['ic_hit_rate'] == 0.5
    assert report['top_minus_bottom'] == pytest.approx(0.0)
    assert report['bucket_means'] == pytest.approx({0: 0.035, 1: 0.035})
    assert report['periods_needed_for_t2'] is None
