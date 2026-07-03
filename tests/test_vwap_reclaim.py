"""VwapReclaim live path — it implemented only precompute()+on_bar() (backtest),
so like ORB it emitted nothing live until on_prices() was added."""
from types import SimpleNamespace

import numpy as np
import pandas as pd

from trader.objects import Action
from strategies.vwap_reclaim import VwapReclaim


def _reclaim_frame(reclaim=True):
    """US session (09:30 ET = 13:30 UTC in July): heavy higher open, a long dip
    below VWAP, then (if reclaim) a volume-confirmed close back above VWAP as the
    last bar — the classic gap-base-reclaim long setup."""
    n = 75
    base = pd.Timestamp('2026-07-06 13:30', tz='UTC')      # Mon 09:30 ET
    idx = pd.date_range(base, periods=n, freq='1min', name='date')
    close, vol = [], []
    for i in range(n):
        if i == 0:
            c, v = 102.0, 5000.0          # high-volume open → pulls VWAP up
        elif i < n - 1:
            c, v = 99.0, 1000.0           # sustained dip below VWAP
        else:
            c = 99.6 if reclaim else 99.0  # last bar: reclaim above VWAP (or not)
            v = 5000.0 if reclaim else 1000.0
        close.append(c); vol.append(v)
    return pd.DataFrame({'open': close, 'high': [c + 0.05 for c in close],
                         'low': [c - 0.05 for c in close], 'close': close,
                         'volume': vol, 'vwap': close, 'bar_count': [1] * n,
                         'bid': close, 'ask': close, 'last': close,
                         'last_size': [1] * n}, index=idx)


def _strategy():
    s = VwapReclaim()
    s._context = SimpleNamespace(name='vwap', paper_only=False, params={})
    return s


def test_reclaim_fires_buy_live():
    s = _strategy()
    sig = s.on_prices(_reclaim_frame(reclaim=True))
    assert sig is not None and sig.action == Action.BUY


def test_no_reclaim_no_signal():
    s = _strategy()
    assert s.on_prices(_reclaim_frame(reclaim=False)) is None


def test_live_matches_backtest_at_last_bar():
    s = _strategy()
    df = _reclaim_frame(reclaim=True)
    live = s.on_prices(df)
    state = s.precompute(df)
    bt = s.on_bar(df, state, len(df) - 1)
    assert (live is None) == (bt is None)
    if live is not None:
        assert live.action == bt.action


def test_too_few_bars_none():
    s = _strategy()
    assert s.on_prices(_reclaim_frame().iloc[:10]) is None
