"""``Backtester.run_panel`` accepts SEVERAL terminal prints per name - the live
D07 rule in ``trader/lowfreq_live/ledger.py`` needs one per stopped episode -
and still honours the harness's single timestamp exactly as before (C02's
declared out-of-scope change to ``trader/simulation/backtester.py``)."""
from __future__ import annotations

import datetime as dt
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from trader.objects import BarSize
from trader.simulation.backtester import BacktestConfig, Backtester
from trader.trading.strategy import Strategy


class _NoStorage:
    def get_tickdata(self, bar_size):
        raise RuntimeError('frames are injected')


class _Schedule(Strategy):
    """Target weights on named sessions; hold otherwise."""
    def __init__(self, schedule: Dict[str, Dict[str, float]]):
        super().__init__()
        self._s = {pd.Timestamp(k): v for k, v in schedule.items()}

    def precompute_panel(self, panel):
        return {}

    def on_panel(self, panel, state, index) -> Optional[Dict[Any, float]]:
        w = self._s.get(pd.Timestamp(panel.index[index]))
        if w is None:
            return None
        out: Dict[Any, float] = {c: 0.0 for c in panel['close'].columns}
        out.update(w)
        return out


def _frames():
    """B prints 09-01..09-03, is silent five sessions (recognised 09-10 by the
    ledger's rule), prints 09-11..09-15, then stops for good (recognised 09-22)."""
    days = pd.bdate_range('2026-09-01', periods=24)
    nan = np.nan
    closes = pd.DataFrame({'A': [10.0] * 24, 'B': [30, 30, 33] + [nan] * 5 + [30, 30, 30] + [nan] * 13}, index=days)
    opens = closes.shift(1).fillna(closes.iloc[0])
    frames = {}
    for t in closes.columns:
        f = pd.DataFrame({'open': opens[t], 'high': closes[t], 'low': closes[t], 'close': closes[t], 'volume': 1e6})
        frames[t] = f[closes[t].notna()]
    return frames, days


DEFAULT = {'2026-09-01': {'A': 0.5, 'B': 0.5}, '2026-09-14': {'A': 0.5, 'B': 0.5}}


def _run(terminal, schedule=None):
    frames, days = _frames()
    cfg = BacktestConfig(start_date=days[0].to_pydatetime(), end_date=(days[-1] + pd.Timedelta(days=1)).to_pydatetime(),
                         initial_capital=10_000.0, bar_size=BarSize.Days1, slippage_bps=5.0,
                         commission_per_share=0.005, fill_policy='next_open', panel_terminal_prints=terminal)
    return Backtester(_NoStorage(), cfg).run_panel(_Schedule(schedule or DEFAULT), ['A', 'B'], frames=frames)


def _sells_of_b(res):
    return [str(pd.Timestamp(t.timestamp).date()) for t in res.trades if t.conid == 'B' and str(t.action).endswith('SELL')]


def _net_b(res):
    return sum((1 if str(t.action).endswith('BUY') else -1) * t.quantity for t in res.trades if t.conid == 'B')


def test_a_single_timestamp_behaves_exactly_as_before():
    """The harness's hindsight rule (one timestamp per name) is unchanged, and
    every single-value spelling - including a STRING, which is iterable but must
    not be read character by character - means the same thing."""
    ts = pd.Timestamp('2026-09-10')
    base = _run({'B': ts})
    assert _sells_of_b(base) == ['2026-09-10'] and base.diagnostics['panel_forced_exits'] == 1
    for alt in ({'B': [ts]}, {'B': (ts,)}, {'B': {ts}}, {'B': pd.DatetimeIndex([ts])}, {'B': '2026-09-10'},
                {'B': dt.date(2026, 9, 10)}, {'B': np.datetime64('2026-09-10')}):
        res = _run(alt)
        assert res.diagnostics['panel_forced_exits'] == 1, alt
        pd.testing.assert_series_equal(res.equity_curve, base.equity_curve)
        assert [(t.timestamp, t.conid, t.quantity, t.price) for t in res.trades] == \
               [(t.timestamp, t.conid, t.quantity, t.price) for t in base.trades]


def test_several_terminal_prints_exit_each_held_episode():
    res = _run({'B': [pd.Timestamp('2026-09-10'), pd.Timestamp('2026-09-22')]})
    assert _sells_of_b(res) == ['2026-09-10', '2026-09-22']
    assert res.diagnostics['panel_forced_exits'] == 2
    assert _net_b(res) == 0                                  # nothing carried at a stale mark
    # each exit at the carried last print less slippage: 33 on 09-10, 30 on 09-22
    prices = {str(pd.Timestamp(t.timestamp).date()): t.price for t in res.trades
              if t.conid == 'B' and str(t.action).endswith('SELL')}
    assert prices['2026-09-10'] == 33.0 * (1 - 5e-4) and prices['2026-09-22'] == 30.0 * (1 - 5e-4)


def test_a_terminal_print_on_an_unheld_session_does_nothing():
    """B is not held on 09-10 when the first rebalance buys only A: that entry
    fires nothing (no trade, not counted); the 09-22 entry sells the position
    the 09-14 rebalance opened."""
    res = _run({'B': [pd.Timestamp('2026-09-10'), pd.Timestamp('2026-09-22')]},
               schedule={'2026-09-01': {'A': 1.0}, '2026-09-14': {'A': 0.5, 'B': 0.5}})
    assert _sells_of_b(res) == ['2026-09-22']
    assert res.diagnostics['panel_forced_exits'] == 1 and _net_b(res) == 0


def test_without_the_second_terminal_print_the_reentered_name_is_carried_forever():
    """The failure the extension removes: one timestamp per name leaves the
    second episode in the book at a stale mark with nothing to fill against."""
    res = _run({'B': pd.Timestamp('2026-09-10')})
    assert _sells_of_b(res) == ['2026-09-10']
    assert _net_b(res) > 0


def test_no_terminal_prints_is_still_the_default():
    res = _run(None)
    assert _sells_of_b(res) == [] and res.diagnostics['panel_forced_exits'] == 0
