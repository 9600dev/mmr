"""Pinned market-data regressions from the September architecture review.

Original counterexamples now assert the repaired completeness, session,
history-retry and incremental-dispatch behavior. No broker is used.
"""

from types import SimpleNamespace
from unittest.mock import Mock

import numpy as np
import pandas as pd
import pytest
from ib_async import Contract, Ticker

from trader.data.market_data import NORMALIZED_COLUMNS, resample_ticks_to_bars
from trader.data.market_session import in_session
from trader.objects import BarSize
from trader.strategy.strategy_runtime import StrategyRuntime
from trader.trading.strategy import StrategyState


def _frame(times, prices=None, volumes=None):
    index = pd.DatetimeIndex(times, name="date")
    prices = prices if prices is not None else [100.0] * len(index)
    frame = pd.DataFrame(index=index, columns=NORMALIZED_COLUMNS, dtype=float)
    for column in ("open", "high", "low", "close", "last", "bid", "ask", "vwap"):
        frame[column] = prices
    frame["volume"] = volumes if volumes is not None else np.arange(len(index)) + 100
    frame["last_size"] = 1.0
    return frame


def _runtime(tmp_path):
    runtime = StrategyRuntime.__new__(StrategyRuntime)
    runtime.streams = {}
    runtime._hist_bars = {}
    runtime._last_dispatched_bar = {}
    runtime._oos_bars = {}
    runtime._oos_logged = set()
    runtime._oos_last = {}
    runtime._tick_retention_days = 2
    runtime.history_duckdb_path = str(tmp_path / "history.duckdb")
    runtime.strategies = {}
    runtime._check_time_exit = Mock()
    return runtime


def _ticker(contract, timestamp, last, volume):
    ticker = Ticker(contract=contract, time=timestamp.to_pydatetime())
    ticker.last = last
    ticker.volume = volume
    return ticker


def test_history_becomes_visible_after_a_transient_priming_read_failure(tmp_path, monkeypatch):
    runtime = _runtime(tmp_path)
    history = _frame(pd.date_range("2026-09-08 13:30", periods=3, freq="min", tz="UTC"))
    read = Mock(side_effect=[OSError("temporary DB lock"), history])
    monkeypatch.setattr("trader.data.duckdb_store.DuckDBDataStore.read", read)

    assert runtime._strategy_frame(1, BarSize.Mins1) is None
    recovered = runtime._strategy_frame(1, BarSize.Mins1)

    assert recovered is not None and len(recovered) == 3


def test_forming_history_does_not_override_completed_live_frame(tmp_path):
    runtime = _runtime(tmp_path)
    observed_at = pd.Timestamp.now(tz="UTC")
    forming_start = observed_at.floor("min")
    runtime.streams[1] = _frame(pd.date_range(end=observed_at, periods=3, freq="min"))
    runtime._hist_bars[(1, BarSize.Mins1)] = _frame(
        pd.date_range(end=forming_start, periods=3, freq="min")
    )

    frame = runtime._strategy_frame(1, BarSize.Mins1)

    assert frame is not None
    assert frame.index[-1] < forming_start


def test_out_of_session_quotes_do_not_enter_later_strategy_windows(tmp_path):
    runtime = _runtime(tmp_path)
    runtime._hist_bars[(1, BarSize.Mins1)] = pd.DataFrame()
    sunday = pd.Timestamp("2026-09-06 15:00", tz="UTC")
    runtime.streams[1] = _frame(
        [sunday, pd.Timestamp("2026-09-08 13:30:10", tz="UTC")],
        [9999.0, 100.0],
    )
    # The oldest tick is less than two retained days before the new tick.
    on_prices = Mock(return_value=None)
    runtime.strategies[1] = [SimpleNamespace(
        state=StrategyState.RUNNING, name="review", bar_size=BarSize.Mins1,
        on_prices=on_prices,
    )]
    contract = Contract(conId=1, symbol="TEST", exchange="SMART", primaryExchange="NASDAQ", secType="STK")
    assert not runtime._bar_in_session(contract, sunday)
    ticker = _ticker(contract, pd.Timestamp("2026-09-08 13:31:10", tz="UTC"), 101.0, 105.0)

    runtime.on_ticker_next(ticker)

    on_prices.assert_called_once()
    assert sunday not in on_prices.call_args.args[0].index


def test_mid_session_subscription_does_not_invent_a_million_share_minute():
    ticks = _frame(
        pd.to_datetime(["2026-09-08T16:00:10Z", "2026-09-08T16:00:50Z", "2026-09-08T16:01:10Z"]),
        volumes=[1_000_000, 1_000_005, 1_000_010],
    )

    bars = resample_ticks_to_bars(ticks, "1min")

    # The initial partial minute may be omitted or marked unknown. If a
    # numeric observed volume is emitted it cannot include unseen prehistory.
    assert bars.empty or pd.isna(bars.iloc[0]["volume"]) or bars.iloc[0]["volume"] <= 5


def test_valid_daily_bar_reaches_daily_strategy(tmp_path):
    runtime = _runtime(tmp_path)
    prior_session = pd.Timestamp("2026-09-08", tz="America/New_York").tz_convert("UTC")
    runtime._hist_bars[(1, BarSize.Days1)] = _frame([prior_session])
    on_prices = Mock(return_value=None)
    runtime.strategies[1] = [SimpleNamespace(
        state=StrategyState.RUNNING, name="daily", bar_size=BarSize.Days1,
        on_prices=on_prices,
    )]
    ticker = _ticker(
        Contract(conId=1, symbol="TEST", exchange="SMART", primaryExchange="NASDAQ", secType="STK"),
        pd.Timestamp("2026-09-09 13:30:10", tz="UTC"), 101.0, 100,
    )

    runtime.on_ticker_next(ticker)

    on_prices.assert_called_once()


def test_hong_kong_lunch_break_is_out_of_session():
    assert in_session(pd.Timestamp("2026-09-08 11:30", tz="Asia/Hong_Kong"), "SEHK", sec_type="STK")
    assert not in_session(pd.Timestamp("2026-09-08 12:30", tz="Asia/Hong_Kong"), "SEHK", sec_type="STK")


def test_shared_bar_subscription_reuses_one_resample_per_tick(tmp_path, monkeypatch):
    import trader.data.market_data as market_data

    runtime = _runtime(tmp_path)
    runtime._hist_bars[(1, BarSize.Mins1)] = pd.DataFrame()
    runtime.streams[1] = _frame(pd.date_range("2026-09-08 13:30:10", periods=3, freq="min", tz="UTC"))
    runtime.strategies[1] = [SimpleNamespace(
        state=StrategyState.RUNNING, name=f"strategy_{i}", bar_size=BarSize.Mins1,
        on_prices=Mock(return_value=None),
    ) for i in range(4)]
    resample = Mock(wraps=market_data.resample_ticks_to_bars)
    monkeypatch.setattr(market_data, "resample_ticks_to_bars", resample)
    ticker = _ticker(
        Contract(conId=1, symbol="TEST", exchange="SMART", primaryExchange="NASDAQ", secType="STK"),
        pd.Timestamp("2026-09-08 13:33:10", tz="UTC"), 101.0, 110,
    )

    runtime.on_ticker_next(ticker)

    assert all(strategy.on_prices.call_count == 1 for strategy in runtime.strategies[1])
    assert resample.call_count <= 1
