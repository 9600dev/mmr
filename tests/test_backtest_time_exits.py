"""Tests for ``Signal.max_hold_bars`` and ``Signal.close_by_time`` —
the backtester's time-based exit primitives for day-trading strategies.

The backtester synthesizes a SELL when either condition triggers. Under
the default ``next_open`` fill policy the synthetic SELL fills at the
*next* bar's open, same as any other signal.
"""

import datetime as dt
from unittest.mock import MagicMock

import pandas as pd
import pytest

from trader.objects import Action, BarSize
from trader.simulation.backtester import Backtester, BacktestConfig
from trader.trading.strategy import Signal, Strategy


def _minute_bars(n: int, start: str = '2026-04-19 09:30:00', price: float = 100.0):
    """Build a ``n``-bar 1-minute DataFrame with constant OHLCV.

    Flat prices keep the test focused on the exit mechanics — any price
    path would work, but flat means there's no ambiguity about where a
    fill lands."""
    idx = pd.date_range(start=start, periods=n, freq='1min')
    return pd.DataFrame({
        'open': [price] * n, 'high': [price] * n,
        'low': [price] * n, 'close': [price] * n,
        'volume': [1000] * n, 'bar_count': [1] * n,
        'vwap': [price] * n, 'average': [price] * n,
    }, index=idx)


class _BuyOnceStrategy(Strategy):
    """Emits a single BUY on the 5th bar with configurable exit conditions,
    then stays silent. Lets the test isolate the backtester's time-exit
    machinery from strategy-side logic."""
    def __init__(self, max_hold_bars=None, close_by_time=None):
        super().__init__()
        self._max_hold = max_hold_bars
        self._close_by = close_by_time
        self._fired = False

    def on_prices(self, prices):
        if self._fired or len(prices) < 5:
            return None
        self._fired = True
        return Signal(
            source_name='test', action=Action.BUY,
            probability=1.0, risk=0.0, quantity=10,
            max_hold_bars=self._max_hold,
            close_by_time=self._close_by,
        )


def _run(strategy, bars):
    """Drive one conid of bars through the backtester with a minimal
    storage stub. Returns the BacktestResult."""
    storage = MagicMock()
    tick = MagicMock()
    tick.read.return_value = bars
    storage.get_tickdata.return_value = tick

    config = BacktestConfig(
        start_date=bars.index[0].to_pydatetime(),
        end_date=bars.index[-1].to_pydatetime() + dt.timedelta(minutes=1),
        initial_capital=100_000.0,
        bar_size=BarSize.Mins1,
        slippage_bps=0.0,
        commission_per_share=0.0,
    )
    bt = Backtester(storage=storage, config=config)
    return bt.run(strategy, conids=[1])


class TestMaxHoldBars:

    def test_auto_sell_fires_exactly_after_n_bars(self):
        """BUY on bar 5 with max_hold_bars=10 → synthetic SELL fires on
        bar 15 (5 + 10). With next_open fill policy, the SELL fills at
        bar 16's open."""
        strat = _BuyOnceStrategy(max_hold_bars=10)
        result = _run(strat, _minute_bars(30))

        buys = [t for t in result.trades if 'BUY' in str(t.action)]
        sells = [t for t in result.trades if 'SELL' in str(t.action)]
        assert len(buys) == 1
        assert len(sells) == 1, (
            f'expected exactly one synthesized SELL from max_hold_bars; '
            f'got {len(sells)}'
        )
        # Buy fills at bar 6 (signal on bar 5, next_open), sell fills at
        # bar 16 (signal on bar 15, next_open). 10 bars between fills.
        buy_bar = buys[0].timestamp
        sell_bar = sells[0].timestamp
        elapsed_bars = int((sell_bar - buy_bar).total_seconds() / 60)
        assert elapsed_bars == 10

    def test_never_fires_if_max_hold_not_reached(self):
        """max_hold_bars=50 on a 20-bar run → no synthetic SELL."""
        strat = _BuyOnceStrategy(max_hold_bars=50)
        result = _run(strat, _minute_bars(20))
        sells = [t for t in result.trades if 'SELL' in str(t.action)]
        assert sells == []
        # Position still open at end (ok — the test is about whether
        # auto-exit fires, not about liquidation at series end).

    def test_does_not_fire_without_field_set(self):
        """A BUY signal without max_hold_bars/close_by_time behaves like
        before — no synthetic SELL ever generated."""
        strat = _BuyOnceStrategy()  # both None
        result = _run(strat, _minute_bars(30))
        sells = [t for t in result.trades if 'SELL' in str(t.action)]
        assert sells == []


class TestCloseByTime:

    def test_fires_on_first_bar_past_time_of_day(self):
        """BUY on bar 5 (9:34) with close_by_time=09:45 — synthetic
        SELL fires on bar 15 (9:44-ish, whichever first-bar has
        time-of-day ≥ 9:45)."""
        bars = _minute_bars(30, start='2026-04-19 09:30:00')
        strat = _BuyOnceStrategy(close_by_time=dt.time(9, 45))
        result = _run(strat, bars)
        sells = [t for t in result.trades if 'SELL' in str(t.action)]
        assert len(sells) == 1
        # The synthetic SELL signal fires on the first bar whose
        # time-of-day ≥ 09:45; with next_open fill policy, the actual
        # fill is one bar later. Accept 09:46 as the fill timestamp.
        assert sells[0].timestamp.time() == dt.time(9, 46)

    def test_does_not_fire_when_time_never_reached(self):
        """bars run only 9:30-9:40; close_by_time=15:45 → no SELL."""
        strat = _BuyOnceStrategy(close_by_time=dt.time(15, 45))
        bars = _minute_bars(10, start='2026-04-19 09:30:00')
        result = _run(strat, bars)
        sells = [t for t in result.trades if 'SELL' in str(t.action)]
        assert sells == []


class TestBothFieldsSet:

    def test_whichever_triggers_first_wins_time(self):
        """Both set. close_by_time triggers before max_hold_bars in
        this setup — only one SELL fires."""
        bars = _minute_bars(60, start='2026-04-19 09:30:00')
        strat = _BuyOnceStrategy(
            max_hold_bars=100,  # won't trigger in 60 bars
            close_by_time=dt.time(9, 45),  # triggers
        )
        result = _run(strat, bars)
        sells = [t for t in result.trades if 'SELL' in str(t.action)]
        assert len(sells) == 1

    def test_whichever_triggers_first_wins_hold(self):
        """max_hold_bars triggers before close_by_time."""
        bars = _minute_bars(60, start='2026-04-19 09:30:00')
        strat = _BuyOnceStrategy(
            max_hold_bars=3,       # triggers quickly
            close_by_time=dt.time(15, 45),  # far in the future
        )
        result = _run(strat, bars)
        sells = [t for t in result.trades if 'SELL' in str(t.action)]
        assert len(sells) == 1


class TestSyntheticSellLabeling:

    def test_synthetic_sell_carries_identifying_source_name(self):
        """The synthesized SELL uses a distinctive ``source_name`` so
        trade logs / event stores can tell it apart from strategy-emitted
        sells later. Don't lose this when debugging / auditing."""
        strat = _BuyOnceStrategy(max_hold_bars=5)
        result = _run(strat, _minute_bars(30))
        # Synthetic source_name appears in the TradingEvent signal log.
        exit_signals = [
            s for s in result.signals
            if '__time_exit__' in (s.strategy_name or '')
        ]
        assert len(exit_signals) >= 1
