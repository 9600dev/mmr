"""Backtest confidence stats use the stored curve's real periodicity (2026-11-05).

The equity curve is decimated to daily points, but the Sharpe bootstrap CI was
annualised with sqrt(1-min bars per year): run 2946 showed [-9.8, 62.4] around
a 1.39 headline Sharpe (~20x too wide).
"""
import json
import math
from types import SimpleNamespace

import numpy as np
import pandas as pd

import trader.mmr_cli as cli


def _record(days=300, bar_size='1 min', freq='B', seed=0):
    rng = np.random.default_rng(seed)
    rets = rng.normal(0.0008, 0.01, days)
    values = 100_000 * np.cumprod(1 + rets)
    ts = pd.date_range('2025-07-22 16:00', periods=days, freq=freq, tz='America/New_York')
    curve = [{'timestamp': str(t), 'value': float(v)} for t, v in zip(ts, values)]
    return SimpleNamespace(trades_json='', equity_curve_json=json.dumps(curve), bar_size=bar_size), rets


def test_daily_curve_from_1min_run_annualises_daily():
    rec, rets = _record()
    stats, _ = cli._compute_bt_stats(rec)
    headline = rets.mean() / rets.std(ddof=1) * math.sqrt(252)
    assert stats.sharpe_ci_lo < headline < stats.sharpe_ci_hi
    assert stats.sharpe_ci_hi - stats.sharpe_ci_lo < 6, (stats.sharpe_ci_lo, stats.sharpe_ci_hi)


def test_curve_periodicity_detection():
    daily = [{'timestamp': str(t), 'value': 1.0} for t in pd.date_range('2026-01-01', periods=10, freq='B')]
    minute = [{'timestamp': str(t), 'value': 1.0} for t in pd.date_range('2026-01-01 10:00', periods=10, freq='min')]
    assert cli._curve_periods_per_year(daily) == 252.0
    assert cli._curve_periods_per_year(minute) is None
    assert cli._curve_periods_per_year([{'value': 1.0}] * 5) is None
