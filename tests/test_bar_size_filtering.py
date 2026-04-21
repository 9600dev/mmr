"""Regression tests for bar_size filtering and NaN data handling.

Bug #3 (bar_size filtering): When multiple bar sizes existed for the same symbol
in DuckDB, TickData.read() returned ALL rows regardless of bar size. This caused
MaCrossover on AMD to generate 514 trades instead of 4 (daily + 1-min data mixed).

Bug #4 (NaN data handling): AAPL had 29 NaN rows from Massive API (future dates).
The backtester fed NaN rows to strategies, corrupting the equity curve and producing
NaN total_return.
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

from trader.data.duckdb_store import DuckDBDataStore
from trader.data.data_access import TickData, TickStorage
from trader.objects import Action, BarSize
from trader.simulation.backtester import Backtester, BacktestConfig
from trader.trading.strategy import Signal, Strategy, StrategyContext, StrategyState
from trader.data.universe import UniverseAccessor


# ---------------------------------------------------------------------------
# Bar size filtering
# ---------------------------------------------------------------------------

class TestBarSizeFiltering:
    """Verify TickData.read() filters by bar_size when multiple bar sizes exist."""

    def test_daily_and_minute_data_separated(self, tmp_duckdb_path):
        """Writing daily and 1-min data for the same symbol should not mix."""
        store = DuckDBDataStore(tmp_duckdb_path)

        # Write 10 daily bars
        daily_dates = pd.date_range("2024-06-01", periods=10, freq="1D", tz="UTC")
        daily_df = pd.DataFrame({
            "open": [100.0] * 10,
            "high": [101.0] * 10,
            "low": [99.0] * 10,
            "close": [100.5] * 10,
            "volume": [1000.0] * 10,
            "bar_size": ["1 day"] * 10,
        }, index=daily_dates)
        daily_df.index.name = "date"
        store.write("4391", daily_df)

        # Write 100 minute bars
        minute_dates = pd.date_range("2024-06-01 09:30", periods=100, freq="1min", tz="UTC")
        minute_df = pd.DataFrame({
            "open": [100.0] * 100,
            "high": [101.0] * 100,
            "low": [99.0] * 100,
            "close": [100.5] * 100,
            "volume": [500.0] * 100,
            "bar_size": ["1 min"] * 100,
        }, index=minute_dates)
        minute_df.index.name = "date"
        store.write("4391", minute_df)

        # TickData for daily should only return daily bars
        storage = TickStorage(tmp_duckdb_path)
        td_daily = storage.get_tickdata(BarSize.Days1)
        data_daily = td_daily.read(4391)
        assert len(data_daily) == 10, f"Expected 10 daily bars, got {len(data_daily)}"

        # TickData for minute should only return minute bars
        td_min = storage.get_tickdata(BarSize.Mins1)
        data_min = td_min.read(4391)
        assert len(data_min) == 100, f"Expected 100 minute bars, got {len(data_min)}"

    def test_raw_duckdb_read_without_filter_returns_all(self, tmp_duckdb_path):
        """DuckDBDataStore.read() without bar_size returns all rows."""
        store = DuckDBDataStore(tmp_duckdb_path)

        daily_dates = pd.date_range("2024-06-01", periods=5, freq="1D", tz="UTC")
        daily_df = pd.DataFrame({
            "close": [100.0] * 5,
            "bar_size": ["1 day"] * 5,
        }, index=daily_dates)
        daily_df.index.name = "date"
        store.write("TEST", daily_df)

        minute_dates = pd.date_range("2024-06-01 09:30", periods=3, freq="1min", tz="UTC")
        minute_df = pd.DataFrame({
            "close": [101.0] * 3,
            "bar_size": ["1 min"] * 3,
        }, index=minute_dates)
        minute_df.index.name = "date"
        store.write("TEST", minute_df)

        # Without filter: all rows
        all_data = store.read("TEST")
        assert len(all_data) == 8

        # With filter: only matching rows
        daily_data = store.read("TEST", bar_size="1 day")
        assert len(daily_data) == 5

        minute_data = store.read("TEST", bar_size="1 min")
        assert len(minute_data) == 3

    def test_null_bar_size_included_in_filter(self, tmp_duckdb_path):
        """Rows with NULL bar_size should be included when filtering."""
        store = DuckDBDataStore(tmp_duckdb_path)

        dates = pd.date_range("2024-06-01", periods=5, freq="1D", tz="UTC")
        df = pd.DataFrame({
            "close": [100.0] * 5,
            # No bar_size column → stored as NULL
        }, index=dates)
        df.index.name = "date"
        store.write("TEST", df)

        # Filter should still return rows with NULL bar_size
        data = store.read("TEST", bar_size="1 day")
        assert len(data) == 5

    def test_backtester_uses_correct_bar_size(self, tmp_duckdb_path):
        """Backtester should only use data matching the configured bar size."""
        store = DuckDBDataStore(tmp_duckdb_path)

        # Write 50 daily bars
        daily_dates = pd.date_range("2024-01-02", periods=50, freq="1D", tz="UTC")
        rng = np.random.default_rng(42)
        daily_close = 100 + np.cumsum(rng.normal(0.1, 1.0, 50))
        daily_df = pd.DataFrame({
            "open": daily_close - 0.5,
            "high": daily_close + 1.0,
            "low": daily_close - 1.0,
            "close": daily_close,
            "volume": rng.integers(500, 5000, 50).astype(float),
            "average": daily_close,
            "bar_count": [50] * 50,
            "bar_size": ["1 day"] * 50,
        }, index=daily_dates)
        daily_df.index.name = "date"
        store.write("4391", daily_df)

        # Write 500 minute bars (should NOT appear in daily backtest)
        minute_dates = pd.date_range("2024-01-02 09:30", periods=500, freq="1min", tz="UTC")
        minute_close = 100 + np.cumsum(rng.normal(0, 0.1, 500))
        minute_df = pd.DataFrame({
            "open": minute_close - 0.05,
            "high": minute_close + 0.1,
            "low": minute_close - 0.1,
            "close": minute_close,
            "volume": rng.integers(100, 1000, 500).astype(float),
            "average": minute_close,
            "bar_count": [10] * 500,
            "bar_size": ["1 min"] * 500,
        }, index=minute_dates)
        minute_df.index.name = "date"
        store.write("4391", minute_df)

        class CountingStrategy(Strategy):
            bar_count_seen = 0
            def on_prices(self, prices):
                CountingStrategy.bar_count_seen = len(prices)
                return None

        storage = TickStorage(tmp_duckdb_path)
        config = BacktestConfig(
            start_date=daily_dates[0].to_pydatetime(),
            end_date=daily_dates[-1].to_pydatetime(),
            bar_size=BarSize.Days1,
        )
        bt = Backtester(storage=storage, config=config)
        strategy = CountingStrategy()
        ctx = StrategyContext(
            name="test", bar_size=BarSize.Days1, conids=[4391],
            universe=None, historical_days_prior=0, paper_only=False,
            storage=storage, universe_accessor=UniverseAccessor.__new__(UniverseAccessor),
            logger=__import__('logging').getLogger('test'),
        )
        strategy.install(ctx)
        strategy.state = StrategyState.RUNNING
        result = bt.run(strategy, [4391])

        # Should have seen 50 daily bars, not 550 (50 daily + 500 minute)
        assert CountingStrategy.bar_count_seen == 50


# ---------------------------------------------------------------------------
# NaN data handling
# ---------------------------------------------------------------------------

class TestNaNDataHandling:
    """Verify backtester handles NaN rows gracefully."""

    def test_nan_rows_dropped(self, tmp_duckdb_path):
        """Backtester should drop rows where close is NaN."""
        store = DuckDBDataStore(tmp_duckdb_path)

        n_good = 50
        n_nan = 10
        dates = pd.date_range("2024-01-02", periods=n_good + n_nan, freq="1D", tz="UTC")
        rng = np.random.default_rng(42)
        close = np.concatenate([
            100 + np.cumsum(rng.normal(0.1, 1.0, n_good)),
            [np.nan] * n_nan,
        ])
        df = pd.DataFrame({
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "volume": np.where(np.isnan(close), np.nan, 1000.0),
            "average": close,
            "bar_count": np.where(np.isnan(close), np.nan, 50),
        }, index=dates)
        df.index.name = "date"
        store.write("4391", df)

        class NoOp(Strategy):
            def on_prices(self, p):
                return None

        storage = TickStorage(tmp_duckdb_path)
        config = BacktestConfig(
            start_date=dates[0].to_pydatetime(),
            end_date=dates[-1].to_pydatetime(),
            bar_size=BarSize.Days1,
        )
        bt = Backtester(storage=storage, config=config)
        strategy = NoOp()
        ctx = StrategyContext(
            name="test", bar_size=BarSize.Days1, conids=[4391],
            universe=None, historical_days_prior=0, paper_only=False,
            storage=storage, universe_accessor=UniverseAccessor.__new__(UniverseAccessor),
            logger=__import__('logging').getLogger('test'),
        )
        strategy.install(ctx)
        strategy.state = StrategyState.RUNNING
        result = bt.run(strategy, [4391])

        # Total return should be finite (not NaN)
        assert np.isfinite(result.total_return)
        # Equity curve should have n_good entries, not n_good + n_nan
        assert len(result.equity_curve) == n_good

    def test_all_nan_raises_no_data(self, tmp_duckdb_path):
        """If ALL rows are NaN after dropping, should raise ValueError."""
        store = DuckDBDataStore(tmp_duckdb_path)

        dates = pd.date_range("2024-01-02", periods=5, freq="1D", tz="UTC")
        df = pd.DataFrame({
            "close": [np.nan] * 5,
        }, index=dates)
        df.index.name = "date"
        store.write("4391", df)

        class NoOp(Strategy):
            def on_prices(self, p):
                return None

        storage = TickStorage(tmp_duckdb_path)
        config = BacktestConfig(
            start_date=dates[0].to_pydatetime(),
            end_date=dates[-1].to_pydatetime(),
            bar_size=BarSize.Days1,
        )
        bt = Backtester(storage=storage, config=config)
        strategy = NoOp()
        ctx = StrategyContext(
            name="test", bar_size=BarSize.Days1, conids=[4391],
            universe=None, historical_days_prior=0, paper_only=False,
            storage=storage, universe_accessor=UniverseAccessor.__new__(UniverseAccessor),
            logger=__import__('logging').getLogger('test'),
        )
        strategy.install(ctx)
        strategy.state = StrategyState.RUNNING

        with pytest.raises(ValueError, match="no historical data"):
            bt.run(strategy, [4391])

    def test_nan_in_middle_preserved(self, tmp_duckdb_path):
        """NaN in non-close columns should not cause row to be dropped."""
        store = DuckDBDataStore(tmp_duckdb_path)

        dates = pd.date_range("2024-01-02", periods=10, freq="1D", tz="UTC")
        df = pd.DataFrame({
            "open": [np.nan] * 5 + [100.0] * 5,  # some NaN opens
            "high": [101.0] * 10,
            "low": [99.0] * 10,
            "close": [100.0] * 10,  # all valid closes
            "volume": [1000.0] * 10,
        }, index=dates)
        df.index.name = "date"
        store.write("4391", df)

        class CountBars(Strategy):
            count = 0
            def on_prices(self, p):
                CountBars.count = len(p)
                return None

        storage = TickStorage(tmp_duckdb_path)
        config = BacktestConfig(
            start_date=dates[0].to_pydatetime(),
            end_date=dates[-1].to_pydatetime(),
            bar_size=BarSize.Days1,
        )
        bt = Backtester(storage=storage, config=config)
        strategy = CountBars()
        ctx = StrategyContext(
            name="test", bar_size=BarSize.Days1, conids=[4391],
            universe=None, historical_days_prior=0, paper_only=False,
            storage=storage, universe_accessor=UniverseAccessor.__new__(UniverseAccessor),
            logger=__import__('logging').getLogger('test'),
        )
        strategy.install(ctx)
        strategy.state = StrategyState.RUNNING
        result = bt.run(strategy, [4391])

        # All 10 bars should be processed (NaN is only in 'open', not 'close')
        assert CountBars.count == 10
        assert len(result.equity_curve) == 10


# ---------------------------------------------------------------------------
# Strategy roundtrip with new strategies
# ---------------------------------------------------------------------------

class TestNewStrategies:
    """Test new strategies can be loaded and backtested."""

    def _write_test_data(self, tmp_duckdb_path, n=100):
        store = DuckDBDataStore(tmp_duckdb_path)
        rng = np.random.default_rng(42)
        close = 100 + np.cumsum(rng.normal(0.1, 1.0, n))
        dates = pd.date_range("2024-01-02", periods=n, freq="1D", tz="UTC")
        df = pd.DataFrame({
            "open": close - 0.5,
            "high": close + 1.0,
            "low": close - 1.0,
            "close": close,
            "volume": rng.integers(500, 5000, n).astype(float),
            "average": close,
            "bar_count": [50] * n,
        }, index=dates)
        df.index.name = "date"
        store.write("4391", df)
        return dates

    def test_ensemble_strategy(self, tmp_duckdb_path):
        dates = self._write_test_data(tmp_duckdb_path)
        storage = TickStorage(tmp_duckdb_path)
        config = BacktestConfig(
            start_date=dates[0].to_pydatetime(),
            end_date=dates[-1].to_pydatetime(),
            bar_size=BarSize.Days1,
        )
        bt = Backtester(storage=storage, config=config)
        result = bt.run_from_module('strategies/ensemble.py', 'Ensemble', [4391])
        assert result is not None
        assert len(result.equity_curve) > 0

    def test_vbt_macd_bb_strategy(self, tmp_duckdb_path):
        dates = self._write_test_data(tmp_duckdb_path)
        storage = TickStorage(tmp_duckdb_path)
        config = BacktestConfig(
            start_date=dates[0].to_pydatetime(),
            end_date=dates[-1].to_pydatetime(),
            bar_size=BarSize.Days1,
        )
        bt = Backtester(storage=storage, config=config)
        result = bt.run_from_module('strategies/vbt_macd_bb.py', 'VbtMacdBB', [4391])
        assert result is not None
        assert len(result.equity_curve) > 0
        assert np.isfinite(result.total_return)
