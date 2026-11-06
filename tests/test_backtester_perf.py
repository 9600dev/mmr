"""Backtester per-bar loop: semantics of the precomputed timeline + perf guards.

The bar-by-bar loop used to groupby the merged frame, slice a sub-DataFrame
per conid per timestamp and ``pd.concat`` it onto an expanding window — an
O(N²) copy that made a 35k-bar 1-min ORB backtest take ~26s live while the
strategy itself needed ~30ms. ``_BarTimeline`` resolves the grouping once.
These tests pin that it reproduces ``combined.groupby(combined.index)``
exactly, and guard against the per-bar pandas work creeping back.
Bit-identical results are pinned separately in test_backtester_pin.py.
"""
import datetime as dt
import time

import numpy as np
import pandas as pd

from backtest_synth import FrameStorage, make_minute_bars
from trader.objects import BarSize
from trader.simulation.backtester import Backtester, BacktestConfig, _BarTimeline

ORB = 'strategies/opening_range_breakout.py'


def _combined_with_dupes_and_nat():
    a = make_minute_bars(n_days=1, seed=1, duplicate_every=4).assign(conid=1)
    b = make_minute_bars(n_days=1, seed=2, drop_frac=0.3, duplicate_every=7).assign(conid=2)
    c = make_minute_bars(n_days=1, seed=3, drop_frac=0.6).assign(conid=3)
    combined = pd.concat([a, b, c], axis=0)
    # A NaT-keyed row: groupby drops it, so must the timeline.
    nat = combined.iloc[[5]].copy()
    nat.index = pd.DatetimeIndex([pd.NaT], tz='UTC', name='date')
    return pd.concat([combined, nat], axis=0).sort_index()


def test_timeline_reproduces_groupby_exactly():
    combined = _combined_with_dupes_and_nat()
    timeline = _BarTimeline(combined)
    expected = list(combined.groupby(combined.index))
    assert len(timeline) == len(expected)
    for (ts, spans), (exp_ts, group) in zip(timeline, expected):
        assert ts == exp_ts and type(ts) is type(exp_ts)
        assert list(spans) == list(group['conid'].unique())      # same conid order
        for conid, (first, last) in spans.items():
            bar = group[group['conid'] == conid].drop(columns=['conid'])
            assert timeline.open_at(first) == float(bar['open'].iloc[0])
            assert timeline.close_at(last) == float(bar['close'].iloc[-1])
            assert timeline.index_at(last) == bar.index[-1]
            row, exp_row = timeline.row(last), bar.iloc[-1]
            assert row.name == exp_row.name
            assert row.dtype == exp_row.dtype
            assert list(row.index) == list(exp_row.index)
            assert row.tolist() == exp_row.tolist()


def test_timeline_has_multi_row_bars_and_skips_nat():
    combined = _combined_with_dupes_and_nat()
    timeline = _BarTimeline(combined)
    assert combined.index.isna().sum() == 1
    assert all(not pd.isna(ts) for ts, _ in timeline)
    assert any(first != last for _, spans in timeline for first, last in spans.values())


def test_timeline_missing_open_column_fails_loudly_only_when_read():
    combined = make_minute_bars(n_days=1, seed=4).drop(columns=['open']).assign(conid=9)
    timeline = _BarTimeline(combined)   # constructing is fine — nothing read yet
    first, _ = next(iter(timeline))[1][9]
    try:
        timeline.open_at(first)
    except KeyError as ex:
        assert 'open' in str(ex)
    else:
        raise AssertionError('reading a missing open column must raise')


def test_run_does_no_per_bar_concat(monkeypatch):
    calls = {'n': 0}
    real_concat = pd.concat

    def counting_concat(*a, **kw):
        calls['n'] += 1
        return real_concat(*a, **kw)

    monkeypatch.setattr(pd, 'concat', counting_concat)
    df = make_minute_bars(n_days=4, seed=7)
    cfg = BacktestConfig(start_date=dt.datetime(2023, 1, 1), end_date=dt.datetime(2030, 1, 1),
                         bar_size=BarSize.Mins1)
    res = Backtester(FrameStorage({1: df}), cfg).run_from_module(ORB, 'OpeningRangeBreakout', [1])
    assert len(res.equity_curve) == len(df)
    # One concat merges the conids; the old loop did one per bar (2280 here).
    assert calls['n'] <= 3, calls['n']


def test_35k_bar_orb_backtest_is_fast():
    """~35k 1-min bars (a 90-day run). Was ~10.5s locally / ~23s live; now
    ~0.2s. The bound is deliberately loose (25x headroom) so it only trips
    if the O(N²) per-bar pandas work comes back, not on a slow machine."""
    df = make_minute_bars(n_days=62, seed=7)
    assert 34_000 < len(df) < 36_000
    cfg = BacktestConfig(start_date=dt.datetime(2023, 1, 1), end_date=dt.datetime(2030, 1, 1),
                         bar_size=BarSize.Mins1)
    bt = Backtester(FrameStorage({208813719: df}), cfg)
    t0 = time.perf_counter()
    res = bt.run_from_module(ORB, 'OpeningRangeBreakout', [208813719])
    elapsed = time.perf_counter() - t0
    assert res.total_trades > 0
    assert np.isfinite(res.equity_curve.to_numpy()).all()
    assert elapsed < 5.0, f'35k-bar backtest took {elapsed:.2f}s'
