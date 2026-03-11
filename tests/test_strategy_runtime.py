"""Tests for StrategyRuntime.on_ticker_next — the live normalization path.

These tests verify that Ticker objects are normalized via normalize_ticker()
and accumulated correctly in the strategy runtime streams dict.
"""

import datetime as dt
import os
import sys

import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from unittest.mock import MagicMock, patch
from ib_async.contract import Contract
from ib_async.ticker import Ticker
from trader.data.market_data import NORMALIZED_COLUMNS, normalize_ticker
from trader.objects import Action
from trader.trading.strategy import Signal, Strategy, StrategyContext, StrategyState


# ---------------------------------------------------------------------------
# Minimal StrategyRuntime stub that only tests on_ticker_next logic
# ---------------------------------------------------------------------------

class StrategyRuntimeStub:
    """Stub that replicates only the on_ticker_next logic from strategy_runtime.py
    without requiring ZMQ, IB, or any service connections."""

    def __init__(self):
        self.strategies = {}  # conid -> List[Strategy]
        self.streams = {}  # conid -> pd.DataFrame
        self.signals_emitted = []  # Track signals for testing

    def _get_enabled_strategies(self, conid):
        if conid in self.strategies:
            return [s for s in self.strategies[conid]
                    if s.state == StrategyState.RUNNING or s.state == StrategyState.WAITING_HISTORICAL_DATA]
        return []

    def on_ticker_next(self, ticker):
        """Exact copy of StrategyRuntime.on_ticker_next logic."""
        if not ticker.contract:
            return

        conId = ticker.contract.conId
        normalized = normalize_ticker(ticker)

        if conId not in self.streams:
            self.streams[conId] = normalized
        else:
            self.streams[conId] = pd.concat([self.streams[conId], normalized], axis=0, copy=False)

        for strategy in self._get_enabled_strategies(conId):
            signal = strategy.on_prices(self.streams[conId])
            if signal:
                self.signals_emitted.append((conId, signal))


# ---------------------------------------------------------------------------
# Test strategies
# ---------------------------------------------------------------------------

class RecordingStrategy(Strategy):
    """Records each on_prices call for inspection."""

    def __init__(self):
        super().__init__()
        self.calls = []

    def on_prices(self, prices):
        self.calls.append(prices.copy())
        return None


class BuyOnThirdTick(Strategy):
    """Emits BUY signal on the 3rd tick."""

    def __init__(self):
        super().__init__()
        self._count = 0

    def on_prices(self, prices):
        self._count += 1
        if self._count == 3:
            return Signal("buy_on_third", Action.BUY, 0.9, 0.1)
        return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ticker(conid=4391, symbol="AMD", bid=150.0, ask=150.10,
                 last=150.05, volume=1000, time=None):
    t = Ticker()
    t.contract = Contract(conId=conid, symbol=symbol)
    t.bid = bid
    t.ask = ask
    t.last = last
    t.lastSize = 100
    t.volume = volume
    t.vwap = 150.03
    t.time = time or dt.datetime(2024, 6, 1, 14, 30, 0, tzinfo=dt.timezone.utc)
    return t


def _install_strategy_on_stub(runtime, strategy, conid=4391):
    from trader.data.data_access import TickStorage
    import logging
    from trader.objects import BarSize

    # Minimal install
    ctx = StrategyContext(
        name=strategy.__class__.__name__,
        bar_size=BarSize.Mins1,
        conids=[conid],
        universe=None,
        historical_days_prior=0,
        paper=True,
        storage=MagicMock(),
        universe_accessor=MagicMock(),
        logger=logging.getLogger("test"),
    )
    strategy.install(ctx)
    strategy.state = StrategyState.RUNNING

    if conid not in runtime.strategies:
        runtime.strategies[conid] = []
    runtime.strategies[conid].append(strategy)


# ---------------------------------------------------------------------------
# Tests: on_ticker_next normalization
# ---------------------------------------------------------------------------

class TestOnTickerNextNormalization:
    def test_single_tick_produces_normalized_df(self):
        runtime = StrategyRuntimeStub()
        ticker = _make_ticker()
        runtime.on_ticker_next(ticker)

        assert 4391 in runtime.streams
        df = runtime.streams[4391]
        assert list(df.columns) == NORMALIZED_COLUMNS
        assert len(df) == 1

    def test_no_contract_ignored(self):
        runtime = StrategyRuntimeStub()
        t = Ticker()
        t.contract = None
        runtime.on_ticker_next(t)
        assert len(runtime.streams) == 0

    def test_accumulation_over_multiple_ticks(self):
        runtime = StrategyRuntimeStub()
        base = dt.datetime(2024, 6, 1, 14, 30, 0, tzinfo=dt.timezone.utc)

        for i in range(10):
            ticker = _make_ticker(
                last=150.0 + i * 0.1,
                time=base + dt.timedelta(seconds=i),
            )
            runtime.on_ticker_next(ticker)

        df = runtime.streams[4391]
        assert len(df) == 10
        assert list(df.columns) == NORMALIZED_COLUMNS
        # Prices should be monotonically increasing
        assert df["close"].is_monotonic_increasing

    def test_multiple_conids_separate_streams(self):
        runtime = StrategyRuntimeStub()
        t1 = _make_ticker(conid=4391, symbol="AMD", last=150.0)
        t2 = _make_ticker(conid=265598, symbol="AAPL", last=175.0)

        runtime.on_ticker_next(t1)
        runtime.on_ticker_next(t2)

        assert 4391 in runtime.streams
        assert 265598 in runtime.streams
        assert len(runtime.streams[4391]) == 1
        assert len(runtime.streams[265598]) == 1
        assert runtime.streams[4391]["close"].iloc[0] == pytest.approx(150.0)
        assert runtime.streams[265598]["close"].iloc[0] == pytest.approx(175.0)

    def test_ib_sentinel_values_become_nan(self):
        runtime = StrategyRuntimeStub()
        ticker = _make_ticker()
        ticker.bid = 1.7976931348623157e+308  # IB sentinel
        ticker.ask = float('inf')
        ticker.volume = None

        runtime.on_ticker_next(ticker)
        df = runtime.streams[4391]
        assert np.isnan(df["bid"].iloc[0])
        assert np.isnan(df["ask"].iloc[0])
        assert np.isnan(df["volume"].iloc[0])

    def test_timezone_awareness(self):
        runtime = StrategyRuntimeStub()
        ticker = _make_ticker(time=dt.datetime(2024, 6, 1, 14, 30, tzinfo=dt.timezone.utc))
        runtime.on_ticker_next(ticker)

        df = runtime.streams[4391]
        assert df.index.tz is not None

    def test_naive_time_gets_utc(self):
        runtime = StrategyRuntimeStub()
        ticker = _make_ticker()
        ticker.time = dt.datetime(2024, 6, 1, 14, 30)  # naive
        runtime.on_ticker_next(ticker)

        df = runtime.streams[4391]
        assert df.index.tz is not None


# ---------------------------------------------------------------------------
# Tests: strategy dispatch via on_ticker_next
# ---------------------------------------------------------------------------

class TestStrategyDispatch:
    def test_strategy_receives_accumulated_data(self):
        runtime = StrategyRuntimeStub()
        recorder = RecordingStrategy()
        _install_strategy_on_stub(runtime, recorder, conid=4391)

        base = dt.datetime(2024, 6, 1, 14, 30, 0, tzinfo=dt.timezone.utc)
        for i in range(5):
            ticker = _make_ticker(time=base + dt.timedelta(seconds=i))
            runtime.on_ticker_next(ticker)

        # Strategy should be called 5 times with growing DataFrames
        assert len(recorder.calls) == 5
        for i, df in enumerate(recorder.calls):
            assert len(df) == i + 1
            assert list(df.columns) == NORMALIZED_COLUMNS

    def test_strategy_not_called_if_disabled(self):
        runtime = StrategyRuntimeStub()
        recorder = RecordingStrategy()
        _install_strategy_on_stub(runtime, recorder, conid=4391)
        recorder.state = StrategyState.DISABLED

        runtime.on_ticker_next(_make_ticker())
        assert len(recorder.calls) == 0

    def test_signal_emitted_on_buy(self):
        runtime = StrategyRuntimeStub()
        buyer = BuyOnThirdTick()
        _install_strategy_on_stub(runtime, buyer, conid=4391)

        base = dt.datetime(2024, 6, 1, 14, 30, 0, tzinfo=dt.timezone.utc)
        for i in range(5):
            runtime.on_ticker_next(_make_ticker(time=base + dt.timedelta(seconds=i)))

        assert len(runtime.signals_emitted) == 1
        conid, signal = runtime.signals_emitted[0]
        assert conid == 4391
        assert signal.action == Action.BUY

    def test_multiple_strategies_same_conid(self):
        runtime = StrategyRuntimeStub()
        rec1 = RecordingStrategy()
        rec2 = RecordingStrategy()
        _install_strategy_on_stub(runtime, rec1, conid=4391)
        _install_strategy_on_stub(runtime, rec2, conid=4391)

        runtime.on_ticker_next(_make_ticker())

        assert len(rec1.calls) == 1
        assert len(rec2.calls) == 1

    def test_strategy_on_different_conid_not_called(self):
        runtime = StrategyRuntimeStub()
        recorder = RecordingStrategy()
        _install_strategy_on_stub(runtime, recorder, conid=265598)  # AAPL

        runtime.on_ticker_next(_make_ticker(conid=4391))  # AMD tick
        assert len(recorder.calls) == 0


# ---------------------------------------------------------------------------
# Tests: normalize_ticker edge cases
# ---------------------------------------------------------------------------

class TestNormalizeTickerEdgeCases:
    def test_bid_ask_only_no_last(self):
        """When last is NaN, OHLC should use midpoint."""
        t = Ticker()
        t.contract = Contract(conId=1)
        t.bid = 100.0
        t.ask = 100.20
        t.last = float('nan')
        t.lastSize = float('nan')
        t.volume = 500
        t.vwap = 100.10
        t.time = dt.datetime(2024, 6, 1, 14, 30, tzinfo=dt.timezone.utc)

        df = normalize_ticker(t)
        expected_mid = (100.0 + 100.20) / 2
        assert df["close"].iloc[0] == pytest.approx(expected_mid)
        assert df["open"].iloc[0] == pytest.approx(expected_mid)

    def test_all_nan_ticker(self):
        """Ticker with no valid prices should produce all-NaN prices."""
        t = Ticker()
        t.contract = Contract(conId=1)
        t.bid = float('nan')
        t.ask = float('nan')
        t.last = float('nan')
        t.lastSize = float('nan')
        t.volume = float('nan')
        t.vwap = float('nan')
        t.time = dt.datetime(2024, 6, 1, 14, 30, tzinfo=dt.timezone.utc)

        df = normalize_ticker(t)
        assert np.isnan(df["close"].iloc[0])
        assert np.isnan(df["bid"].iloc[0])
        assert np.isnan(df["ask"].iloc[0])

    def test_last_preferred_over_midpoint(self):
        """When both last and bid/ask available, last is preferred for OHLC."""
        t = Ticker()
        t.contract = Contract(conId=1)
        t.bid = 99.0
        t.ask = 101.0
        t.last = 100.5
        t.lastSize = 50
        t.volume = 1000
        t.vwap = 100.3
        t.time = dt.datetime(2024, 6, 1, 14, 30, tzinfo=dt.timezone.utc)

        df = normalize_ticker(t)
        # OHLC should be last (100.5), not midpoint (100.0)
        assert df["close"].iloc[0] == pytest.approx(100.5)
        assert df["open"].iloc[0] == pytest.approx(100.5)
