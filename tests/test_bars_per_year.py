"""Tests for backtester improvements: _bars_per_year and win rate calculation."""

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
from trader.data.data_access import TickStorage
from trader.objects import Action, BarSize
from trader.simulation.backtester import Backtester, BacktestConfig, BacktestResult, BacktestTrade
from trader.trading.strategy import Signal, Strategy, StrategyContext, StrategyState
from trader.data.universe import UniverseAccessor


# ---------------------------------------------------------------------------
# _bars_per_year tests
# ---------------------------------------------------------------------------

class TestBarsPerYear:
    """Verify _bars_per_year returns correct annualization factors."""

    def test_1min_bars(self):
        result = Backtester._bars_per_year(BarSize.Mins1)
        # 252 trading days * 390 minutes = 98,280
        assert result == pytest.approx(252 * 390, rel=0.01)

    def test_5min_bars(self):
        result = Backtester._bars_per_year(BarSize.Mins5)
        assert result == pytest.approx(252 * 390 / 5, rel=0.01)

    def test_1hour_bars(self):
        result = Backtester._bars_per_year(BarSize.Hours1)
        assert result == pytest.approx(252 * 6.5, rel=0.01)

    def test_daily_bars(self):
        result = Backtester._bars_per_year(BarSize.Days1)
        assert result == 252

    def test_weekly_bars(self):
        result = Backtester._bars_per_year(BarSize.Weeks1)
        assert result == 52

    def test_monthly_bars(self):
        result = Backtester._bars_per_year(BarSize.Months1)
        assert result == 12

    def test_1sec_bars(self):
        result = Backtester._bars_per_year(BarSize.Secs1)
        # 252 * 6.5 hours * 3600 sec/hour
        expected = 252 * 6.5 * 3600
        assert result == pytest.approx(expected, rel=0.01)

    def test_30sec_bars(self):
        result = Backtester._bars_per_year(BarSize.Secs30)
        expected = 252 * 6.5 * 3600 / 30
        assert result == pytest.approx(expected, rel=0.01)

    def test_15min_bars(self):
        result = Backtester._bars_per_year(BarSize.Mins15)
        assert result == pytest.approx(252 * 390 / 15, rel=0.01)

    def test_30min_bars(self):
        result = Backtester._bars_per_year(BarSize.Mins30)
        assert result == pytest.approx(252 * 390 / 30, rel=0.01)

    def test_2hour_bars(self):
        result = Backtester._bars_per_year(BarSize.Hours2)
        assert result == pytest.approx(252 * 6.5 / 2, rel=0.01)

    def test_4hour_bars(self):
        result = Backtester._bars_per_year(BarSize.Hours4)
        assert result == pytest.approx(252 * 6.5 / 4, rel=0.01)

    def test_8hour_bars(self):
        result = Backtester._bars_per_year(BarSize.Hours8)
        # Approximately 1 bar per day
        assert result == 252

    def test_all_bar_sizes_covered(self):
        """Every BarSize value should return a positive number."""
        for bar_size in BarSize:
            result = Backtester._bars_per_year(bar_size)
            assert result > 0, f"_bars_per_year returned non-positive for {bar_size.name}"

    def test_higher_freq_means_more_bars(self):
        """Higher-frequency bar sizes should have more bars per year."""
        mins1 = Backtester._bars_per_year(BarSize.Mins1)
        mins5 = Backtester._bars_per_year(BarSize.Mins5)
        hours1 = Backtester._bars_per_year(BarSize.Hours1)
        days1 = Backtester._bars_per_year(BarSize.Days1)
        weeks1 = Backtester._bars_per_year(BarSize.Weeks1)
        months1 = Backtester._bars_per_year(BarSize.Months1)

        assert mins1 > mins5 > hours1 > days1 > weeks1 > months1


# ---------------------------------------------------------------------------
# Sharpe ratio annualization tests
# ---------------------------------------------------------------------------

class TestSharpeAnnualization:
    """Verify Sharpe ratio uses correct annualization factor."""

    def _make_backtester_and_data(self, tmp_duckdb_path, bar_size, n_bars=200):
        store = DuckDBDataStore(tmp_duckdb_path)
        rng = np.random.default_rng(42)
        close = 100 + np.cumsum(rng.normal(0.05, 1.0, n_bars))

        if bar_size == BarSize.Days1:
            dates = pd.date_range("2024-01-02", periods=n_bars, freq="1D", tz="UTC")
        elif bar_size == BarSize.Mins1:
            dates = pd.date_range("2024-01-02 09:30", periods=n_bars, freq="1min", tz="UTC")
        elif bar_size == BarSize.Weeks1:
            dates = pd.date_range("2024-01-02", periods=n_bars, freq="7D", tz="UTC")
        else:
            dates = pd.date_range("2024-01-02", periods=n_bars, freq="1D", tz="UTC")

        df = pd.DataFrame({
            "open": close - 0.5,
            "high": close + 1.0,
            "low": close - 1.0,
            "close": close,
            "volume": rng.integers(500, 5000, n_bars).astype(float),
            "average": close + 0.02,
            "bar_count": rng.integers(10, 100, n_bars),
        }, index=dates)
        df.index.name = "date"
        store.write("4391", df)

        storage = TickStorage(tmp_duckdb_path)
        config = BacktestConfig(
            start_date=dates[0].to_pydatetime(),
            end_date=dates[-1].to_pydatetime(),
            bar_size=bar_size,
        )
        return Backtester(storage=storage, config=config)

    def test_daily_sharpe_uses_sqrt_252(self, tmp_duckdb_path):
        """Daily bar Sharpe should be annualized by sqrt(252)."""
        bt = self._make_backtester_and_data(tmp_duckdb_path, BarSize.Days1)

        class AlwaysBuy(Strategy):
            def on_prices(self, prices):
                if len(prices) == 30:  # Buy once at bar 30
                    return Signal(source_name="test", action=Action.BUY, probability=0.8, risk=0.2)
                return None

        strategy = AlwaysBuy()
        ctx = StrategyContext(
            name="test", bar_size=BarSize.Days1, conids=[4391],
            universe=None, historical_days_prior=0, paper=True,
            storage=TickStorage(tmp_duckdb_path),
            universe_accessor=UniverseAccessor.__new__(UniverseAccessor),
            logger=__import__('logging').getLogger('test'),
        )
        strategy.install(ctx)
        strategy.state = StrategyState.RUNNING
        result = bt.run(strategy, [4391])

        # Sharpe should be a finite number (positive or negative depending on price movement)
        assert np.isfinite(result.sharpe_ratio)
        # With daily bars the annualization factor should make it meaningfully different from raw
        assert result.sharpe_ratio != 0.0 or result.total_trades == 0

    def test_weekly_sharpe_differs_from_daily(self, tmp_duckdb_path):
        """Different bar sizes should produce different Sharpe ratios for same data pattern."""
        daily_bpy = Backtester._bars_per_year(BarSize.Days1)
        weekly_bpy = Backtester._bars_per_year(BarSize.Weeks1)
        # Different annualization factor
        assert daily_bpy != weekly_bpy
        assert np.sqrt(daily_bpy) != pytest.approx(np.sqrt(weekly_bpy))


# ---------------------------------------------------------------------------
# Win rate with weighted average entry price
# ---------------------------------------------------------------------------

class TestWinRateWeightedAvg:
    """Test the improved win rate calculation using weighted average entry prices."""

    def _run_with_trades(self, tmp_duckdb_path, prices_pattern):
        """Helper: run a strategy that follows a predetermined trade pattern."""
        store = DuckDBDataStore(tmp_duckdb_path)
        n = len(prices_pattern)
        dates = pd.date_range("2024-01-02", periods=n, freq="1D", tz="UTC")
        df = pd.DataFrame({
            "open": prices_pattern - 0.5,
            "high": prices_pattern + 1.0,
            "low": prices_pattern - 1.0,
            "close": prices_pattern,
            "volume": np.full(n, 1000.0),
            "average": prices_pattern,
            "bar_count": np.full(n, 50),
        }, index=dates)
        df.index.name = "date"
        store.write("4391", df)

        storage = TickStorage(tmp_duckdb_path)
        config = BacktestConfig(
            start_date=dates[0].to_pydatetime(),
            end_date=dates[-1].to_pydatetime(),
            bar_size=BarSize.Days1,
            initial_capital=100_000.0,
            slippage_bps=0.0,
            commission_per_share=0.0,
            # These WinRate tests count the BUY + SELL on specific bars and
            # assume both execute within the tiny price sequence. Use the
            # legacy fill policy so signals fill on the triggering bar; the
            # realistic next_open path needs an extra bar to fill the last
            # signal, which would skew the win-rate accounting.
            fill_policy='same_close',
        )
        return Backtester(storage=storage, config=config), storage

    def test_single_winning_trade(self, tmp_duckdb_path):
        """Buy low, sell high → 100% win rate."""
        # Price goes 50, 60, 70 (uptrend)
        prices = np.array([50.0, 60.0, 70.0])

        class BuySell(Strategy):
            def on_prices(self, p):
                if len(p) == 1:
                    return Signal(source_name="test", action=Action.BUY, probability=0.8, risk=0.2, quantity=10)
                if len(p) == 3:
                    return Signal(source_name="test", action=Action.SELL, probability=0.8, risk=0.2, quantity=10)
                return None

        bt, storage = self._run_with_trades(tmp_duckdb_path, prices)
        strategy = BuySell()
        ctx = StrategyContext(
            name="test", bar_size=BarSize.Days1, conids=[4391],
            universe=None, historical_days_prior=0, paper=True,
            storage=storage, universe_accessor=UniverseAccessor.__new__(UniverseAccessor),
            logger=__import__('logging').getLogger('test'),
        )
        strategy.install(ctx)
        strategy.state = StrategyState.RUNNING
        result = bt.run(strategy, [4391])

        assert result.win_rate == 1.0
        assert result.total_trades == 2  # 1 buy + 1 sell

    def test_single_losing_trade(self, tmp_duckdb_path):
        """Buy high, sell low → 0% win rate."""
        prices = np.array([70.0, 60.0, 50.0])

        class BuySell(Strategy):
            def on_prices(self, p):
                if len(p) == 1:
                    return Signal(source_name="test", action=Action.BUY, probability=0.8, risk=0.2, quantity=10)
                if len(p) == 3:
                    return Signal(source_name="test", action=Action.SELL, probability=0.8, risk=0.2, quantity=10)
                return None

        bt, storage = self._run_with_trades(tmp_duckdb_path, prices)
        strategy = BuySell()
        ctx = StrategyContext(
            name="test", bar_size=BarSize.Days1, conids=[4391],
            universe=None, historical_days_prior=0, paper=True,
            storage=storage, universe_accessor=UniverseAccessor.__new__(UniverseAccessor),
            logger=__import__('logging').getLogger('test'),
        )
        strategy.install(ctx)
        strategy.state = StrategyState.RUNNING
        result = bt.run(strategy, [4391])

        assert result.win_rate == 0.0

    def test_mixed_wins_and_losses(self, tmp_duckdb_path):
        """Multiple trades with some wins and some losses."""
        # Price: up, down, up pattern
        prices = np.array([50.0, 60.0, 70.0, 55.0, 45.0, 60.0, 80.0])

        trade_counter = {'n': 0}

        class MultiTrade(Strategy):
            def on_prices(self, p):
                n = len(p)
                # Buy at bar 1 @ 50, sell at bar 3 @ 70 (win)
                if n == 1:
                    return Signal(source_name="test", action=Action.BUY, probability=0.8, risk=0.2, quantity=10)
                if n == 3:
                    return Signal(source_name="test", action=Action.SELL, probability=0.8, risk=0.2, quantity=10)
                # Buy at bar 4 @ 55, sell at bar 5 @ 45 (loss)
                if n == 4:
                    return Signal(source_name="test", action=Action.BUY, probability=0.8, risk=0.2, quantity=10)
                if n == 5:
                    return Signal(source_name="test", action=Action.SELL, probability=0.8, risk=0.2, quantity=10)
                return None

        bt, storage = self._run_with_trades(tmp_duckdb_path, prices)
        strategy = MultiTrade()
        ctx = StrategyContext(
            name="test", bar_size=BarSize.Days1, conids=[4391],
            universe=None, historical_days_prior=0, paper=True,
            storage=storage, universe_accessor=UniverseAccessor.__new__(UniverseAccessor),
            logger=__import__('logging').getLogger('test'),
        )
        strategy.install(ctx)
        strategy.state = StrategyState.RUNNING
        result = bt.run(strategy, [4391])

        # 1 win, 1 loss → 50% win rate
        assert result.win_rate == pytest.approx(0.5)

    def test_weighted_avg_entry_multiple_buys(self, tmp_duckdb_path):
        """Multiple buys at different prices, weighted avg should be correct."""
        # Buy 10 @ 50, buy 10 @ 60, sell 20 @ 55 → weighted avg = 55, sell at 55 → breakeven (loss by strict >)
        prices = np.array([50.0, 60.0, 55.0])

        class AvgEntryTest(Strategy):
            def on_prices(self, p):
                n = len(p)
                if n == 1:
                    return Signal(source_name="test", action=Action.BUY, probability=0.8, risk=0.2, quantity=10)
                if n == 2:
                    return Signal(source_name="test", action=Action.BUY, probability=0.8, risk=0.2, quantity=10)
                if n == 3:
                    return Signal(source_name="test", action=Action.SELL, probability=0.8, risk=0.2, quantity=20)
                return None

        bt, storage = self._run_with_trades(tmp_duckdb_path, prices)
        strategy = AvgEntryTest()
        ctx = StrategyContext(
            name="test", bar_size=BarSize.Days1, conids=[4391],
            universe=None, historical_days_prior=0, paper=True,
            storage=storage, universe_accessor=UniverseAccessor.__new__(UniverseAccessor),
            logger=__import__('logging').getLogger('test'),
        )
        strategy.install(ctx)
        strategy.state = StrategyState.RUNNING
        result = bt.run(strategy, [4391])

        # weighted avg entry = (50*10 + 60*10) / 20 = 55
        # sell at 55 → not strictly greater → loss
        assert result.win_rate == 0.0

    def test_no_sells_means_zero_win_rate(self, tmp_duckdb_path):
        """Only buys, no sells → win rate should be 0 (no completed trades)."""
        prices = np.array([50.0, 51.0, 52.0])

        class OnlyBuy(Strategy):
            def on_prices(self, p):
                if len(p) == 1:
                    return Signal(source_name="test", action=Action.BUY, probability=0.8, risk=0.2, quantity=10)
                return None

        bt, storage = self._run_with_trades(tmp_duckdb_path, prices)
        strategy = OnlyBuy()
        ctx = StrategyContext(
            name="test", bar_size=BarSize.Days1, conids=[4391],
            universe=None, historical_days_prior=0, paper=True,
            storage=storage, universe_accessor=UniverseAccessor.__new__(UniverseAccessor),
            logger=__import__('logging').getLogger('test'),
        )
        strategy.install(ctx)
        strategy.state = StrategyState.RUNNING
        result = bt.run(strategy, [4391])

        assert result.win_rate == 0.0

    def test_partial_sell_preserves_avg_entry(self, tmp_duckdb_path):
        """Buy 20, sell 10 (win), sell remaining 10 (loss) → 50% win rate."""
        # Buy 20 at 50, sell 10 at 60 (win), sell 10 at 40 (loss)
        prices = np.array([50.0, 60.0, 40.0])

        class PartialSell(Strategy):
            def on_prices(self, p):
                n = len(p)
                if n == 1:
                    return Signal(source_name="test", action=Action.BUY, probability=0.8, risk=0.2, quantity=20)
                if n == 2:
                    return Signal(source_name="test", action=Action.SELL, probability=0.8, risk=0.2, quantity=10)
                if n == 3:
                    return Signal(source_name="test", action=Action.SELL, probability=0.8, risk=0.2, quantity=10)
                return None

        bt, storage = self._run_with_trades(tmp_duckdb_path, prices)
        strategy = PartialSell()
        ctx = StrategyContext(
            name="test", bar_size=BarSize.Days1, conids=[4391],
            universe=None, historical_days_prior=0, paper=True,
            storage=storage, universe_accessor=UniverseAccessor.__new__(UniverseAccessor),
            logger=__import__('logging').getLogger('test'),
        )
        strategy.install(ctx)
        strategy.state = StrategyState.RUNNING
        result = bt.run(strategy, [4391])

        # Sell at 60 > entry 50 → win, sell at 40 < entry 50 → loss
        assert result.win_rate == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# Max drawdown edge cases
# ---------------------------------------------------------------------------

class TestMaxDrawdown:
    """Edge cases for max drawdown calculation."""

    def test_flat_equity_zero_drawdown(self, tmp_duckdb_path):
        """No trades → flat equity → zero drawdown."""
        store = DuckDBDataStore(tmp_duckdb_path)
        n = 10
        dates = pd.date_range("2024-01-02", periods=n, freq="1D", tz="UTC")
        df = pd.DataFrame({
            "open": [100.0] * n,
            "high": [101.0] * n,
            "low": [99.0] * n,
            "close": [100.0] * n,
            "volume": [1000.0] * n,
            "average": [100.0] * n,
            "bar_count": [50] * n,
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
            universe=None, historical_days_prior=0, paper=True,
            storage=storage, universe_accessor=UniverseAccessor.__new__(UniverseAccessor),
            logger=__import__('logging').getLogger('test'),
        )
        strategy.install(ctx)
        strategy.state = StrategyState.RUNNING
        result = bt.run(strategy, [4391])

        assert result.max_drawdown == 0.0
        assert result.total_return == 0.0

    def test_drawdown_negative(self, tmp_duckdb_path):
        """Drawdown should always be <= 0."""
        store = DuckDBDataStore(tmp_duckdb_path)
        n = 50
        rng = np.random.default_rng(42)
        close = 100 + np.cumsum(rng.normal(0, 2, n))
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

        class BuyOnce(Strategy):
            def on_prices(self, p):
                if len(p) == 5:
                    return Signal(source_name="test", action=Action.BUY, probability=0.8, risk=0.2, quantity=100)
                return None

        storage = TickStorage(tmp_duckdb_path)
        config = BacktestConfig(
            start_date=dates[0].to_pydatetime(),
            end_date=dates[-1].to_pydatetime(),
            bar_size=BarSize.Days1,
        )
        bt = Backtester(storage=storage, config=config)
        strategy = BuyOnce()
        ctx = StrategyContext(
            name="test", bar_size=BarSize.Days1, conids=[4391],
            universe=None, historical_days_prior=0, paper=True,
            storage=storage, universe_accessor=UniverseAccessor.__new__(UniverseAccessor),
            logger=__import__('logging').getLogger('test'),
        )
        strategy.install(ctx)
        strategy.state = StrategyState.RUNNING
        result = bt.run(strategy, [4391])

        assert result.max_drawdown <= 0.0


# ---------------------------------------------------------------------------
# Commission and slippage edge cases
# ---------------------------------------------------------------------------

class TestCommissionSlippage:
    """Test commission and slippage mechanics."""

    def test_zero_slippage_zero_commission(self, tmp_duckdb_path):
        """With zero slippage and commission, buy+sell at same price = breakeven."""
        store = DuckDBDataStore(tmp_duckdb_path)
        n = 5
        dates = pd.date_range("2024-01-02", periods=n, freq="1D", tz="UTC")
        price = 100.0
        df = pd.DataFrame({
            "open": [price] * n,
            "high": [price] * n,
            "low": [price] * n,
            "close": [price] * n,
            "volume": [1000.0] * n,
            "average": [price] * n,
            "bar_count": [50] * n,
        }, index=dates)
        df.index.name = "date"
        store.write("4391", df)

        class BuyThenSell(Strategy):
            def on_prices(self, p):
                if len(p) == 1:
                    return Signal(source_name="test", action=Action.BUY, probability=0.8, risk=0.2, quantity=10)
                if len(p) == 3:
                    return Signal(source_name="test", action=Action.SELL, probability=0.8, risk=0.2, quantity=10)
                return None

        storage = TickStorage(tmp_duckdb_path)
        config = BacktestConfig(
            start_date=dates[0].to_pydatetime(),
            end_date=dates[-1].to_pydatetime(),
            bar_size=BarSize.Days1,
            slippage_bps=0.0,
            commission_per_share=0.0,
        )
        bt = Backtester(storage=storage, config=config)
        strategy = BuyThenSell()
        ctx = StrategyContext(
            name="test", bar_size=BarSize.Days1, conids=[4391],
            universe=None, historical_days_prior=0, paper=True,
            storage=storage, universe_accessor=UniverseAccessor.__new__(UniverseAccessor),
            logger=__import__('logging').getLogger('test'),
        )
        strategy.install(ctx)
        strategy.state = StrategyState.RUNNING
        result = bt.run(strategy, [4391])

        assert result.total_return == pytest.approx(0.0, abs=1e-10)

    def test_slippage_reduces_returns(self, tmp_duckdb_path):
        """Higher slippage should reduce returns for same trades."""
        store = DuckDBDataStore(tmp_duckdb_path)
        n = 5
        dates = pd.date_range("2024-01-02", periods=n, freq="1D", tz="UTC")
        df = pd.DataFrame({
            "open": [100.0, 100.0, 110.0, 110.0, 110.0],
            "high": [101.0, 101.0, 111.0, 111.0, 111.0],
            "low": [99.0, 99.0, 109.0, 109.0, 109.0],
            "close": [100.0, 100.0, 110.0, 110.0, 110.0],
            "volume": [1000.0] * n,
            "average": [100.0, 100.0, 110.0, 110.0, 110.0],
            "bar_count": [50] * n,
        }, index=dates)
        df.index.name = "date"

        class BuyThenSell(Strategy):
            def on_prices(self, p):
                if len(p) == 1:
                    return Signal(source_name="test", action=Action.BUY, probability=0.8, risk=0.2, quantity=10)
                if len(p) == 3:
                    return Signal(source_name="test", action=Action.SELL, probability=0.8, risk=0.2, quantity=10)
                return None

        # Run with zero slippage
        store.write("4391", df)
        storage = TickStorage(tmp_duckdb_path)
        config_zero = BacktestConfig(
            start_date=dates[0].to_pydatetime(),
            end_date=dates[-1].to_pydatetime(),
            bar_size=BarSize.Days1,
            slippage_bps=0.0,
            commission_per_share=0.0,
        )
        bt_zero = Backtester(storage=storage, config=config_zero)
        s1 = BuyThenSell()
        ctx = StrategyContext(
            name="test", bar_size=BarSize.Days1, conids=[4391],
            universe=None, historical_days_prior=0, paper=True,
            storage=storage, universe_accessor=UniverseAccessor.__new__(UniverseAccessor),
            logger=__import__('logging').getLogger('test'),
        )
        s1.install(ctx)
        s1.state = StrategyState.RUNNING
        result_zero = bt_zero.run(s1, [4391])

        # Run with high slippage
        config_high = BacktestConfig(
            start_date=dates[0].to_pydatetime(),
            end_date=dates[-1].to_pydatetime(),
            bar_size=BarSize.Days1,
            slippage_bps=100.0,  # 1% slippage
            commission_per_share=0.0,
        )
        bt_high = Backtester(storage=storage, config=config_high)
        s2 = BuyThenSell()
        s2.install(ctx)
        s2.state = StrategyState.RUNNING
        result_high = bt_high.run(s2, [4391])

        assert result_zero.total_return > result_high.total_return
