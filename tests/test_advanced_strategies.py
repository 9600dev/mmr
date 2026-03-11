"""Tests for advanced strategies: PairsZScore, RegimeAdaptive, and tournament analytics."""

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
from trader.data.universe import UniverseAccessor
from trader.objects import Action, BarSize
from trader.simulation.backtester import Backtester, BacktestConfig, BacktestTrade
from trader.trading.strategy import Signal, Strategy, StrategyContext, StrategyState


# ---------------------------------------------------------------------------
# Hurst exponent
# ---------------------------------------------------------------------------

class TestHurstExponent:
    def test_trending_series_high_hurst(self):
        """A pure uptrend should have H > 0.5."""
        from strategies.regime_adaptive import _hurst_rs
        # Cumulative sum of positive increments = strong trend
        rng = np.random.default_rng(42)
        trend = np.cumsum(rng.uniform(0.5, 1.5, 200))
        returns = np.diff(trend) / trend[:-1]
        h = _hurst_rs(returns, max_lag=20)
        assert h > 0.5, f"Expected H > 0.5 for trending data, got {h}"

    def test_mean_reverting_series_low_hurst(self):
        """An oscillating series should have H < 0.5."""
        from strategies.regime_adaptive import _hurst_rs
        # Alternating +1/-1 = perfect mean-reversion
        oscillation = np.array([(-1) ** i * 0.01 for i in range(200)])
        h = _hurst_rs(oscillation, max_lag=20)
        assert h < 0.5, f"Expected H < 0.5 for mean-reverting data, got {h}"

    def test_random_walk_near_half(self):
        """A random walk should have H roughly near 0.5 (R/S estimator has variance)."""
        from strategies.regime_adaptive import _hurst_rs
        rng = np.random.default_rng(123)
        rw = rng.normal(0, 1, 1000)
        h = _hurst_rs(rw, max_lag=40)
        # R/S estimator is biased upward; accept a wide range
        assert 0.2 < h < 0.85, f"Expected H roughly near 0.5 for random walk, got {h}"

    def test_short_series_returns_half(self):
        """Too few data points should return 0.5 (neutral)."""
        from strategies.regime_adaptive import _hurst_rs
        h = _hurst_rs(np.array([0.01, -0.01, 0.01]), max_lag=20)
        assert h == 0.5


# ---------------------------------------------------------------------------
# RegimeAdaptive strategy
# ---------------------------------------------------------------------------

class TestRegimeAdaptive:
    def test_loads_and_runs(self, tmp_duckdb_path):
        """RegimeAdaptive can be loaded via backtester."""
        store = DuckDBDataStore(tmp_duckdb_path)
        rng = np.random.default_rng(42)
        n = 100
        close = 100 + np.cumsum(rng.normal(0.1, 1.0, n))
        dates = pd.date_range("2024-01-02", periods=n, freq="1D", tz="UTC")
        df = pd.DataFrame({
            "open": close - 0.5, "high": close + 1, "low": close - 1,
            "close": close, "volume": rng.integers(500, 5000, n).astype(float),
            "average": close, "bar_count": [50] * n,
        }, index=dates)
        df.index.name = "date"
        store.write("4391", df)

        storage = TickStorage(tmp_duckdb_path)
        config = BacktestConfig(
            start_date=dates[0].to_pydatetime(),
            end_date=dates[-1].to_pydatetime(),
            bar_size=BarSize.Days1,
        )
        bt = Backtester(storage=storage, config=config)
        result = bt.run_from_module("strategies/regime_adaptive.py", "RegimeAdaptive", [4391])
        assert result is not None
        assert len(result.equity_curve) > 0
        assert np.isfinite(result.total_return)

    def test_regime_detection_correct(self):
        """Verify regime detection: strong trend → 'trending', oscillation → 'reverting'."""
        from strategies.regime_adaptive import RegimeAdaptive

        strategy = RegimeAdaptive()
        ctx = StrategyContext(
            name="test", bar_size=BarSize.Days1, conids=[4391],
            universe=None, historical_days_prior=0, paper=True,
            storage=None, universe_accessor=UniverseAccessor.__new__(UniverseAccessor),
            logger=__import__("logging").getLogger("test"),
        )
        strategy.install(ctx)
        strategy.state = StrategyState.RUNNING

        # Feed a strong uptrend to the strategy and check _last_regime
        n = 100
        close = 100 + np.arange(n) * 2.0  # numpy array, not Series (avoids index alignment issue)
        dates = pd.date_range("2024-01-01", periods=n, freq="1D")
        prices = pd.DataFrame({
            "open": close - 0.5, "high": close + 1, "low": close - 1,
            "close": close, "volume": [1000.0] * n, "vwap": close,
            "bar_count": [50.0] * n, "bid": [np.nan] * n, "ask": [np.nan] * n,
            "last": close, "last_size": [np.nan] * n,
        }, index=dates)

        strategy.on_prices(prices)
        assert strategy._last_regime == "trending", f"Expected 'trending', got '{strategy._last_regime}'"

        # Now feed oscillating data
        strategy2 = RegimeAdaptive()
        strategy2.install(ctx)
        strategy2.state = StrategyState.RUNNING

        osc = 100 + np.array([(-1) ** i * 5 for i in range(n)]).astype(float)
        prices2 = pd.DataFrame({
            "open": osc - 0.5, "high": osc + 1, "low": osc - 1,
            "close": osc, "volume": [1000.0] * n, "vwap": osc,
            "bar_count": [50.0] * n, "bid": [np.nan] * n, "ask": [np.nan] * n,
            "last": osc, "last_size": [np.nan] * n,
        }, index=dates)

        strategy2.on_prices(prices2)
        assert strategy2._last_regime == "reverting", f"Expected 'reverting', got '{strategy2._last_regime}'"


# ---------------------------------------------------------------------------
# PairsZScore strategy
# ---------------------------------------------------------------------------

class TestPairsZScore:
    def test_loads_and_runs(self, tmp_duckdb_path):
        """PairsZScore can be loaded via backtester with 2 conids."""
        store = DuckDBDataStore(tmp_duckdb_path)
        rng = np.random.default_rng(42)
        n = 100
        dates = pd.date_range("2024-01-02", periods=n, freq="1D", tz="UTC")

        for conid in ["4391", "4815747"]:
            close = 100 + np.cumsum(rng.normal(0.1, 1.0, n))
            df = pd.DataFrame({
                "open": close - 0.5, "high": close + 1, "low": close - 1,
                "close": close, "volume": rng.integers(500, 5000, n).astype(float),
                "average": close, "bar_count": [50] * n,
            }, index=dates)
            df.index.name = "date"
            store.write(conid, df)

        storage = TickStorage(tmp_duckdb_path)
        config = BacktestConfig(
            start_date=dates[0].to_pydatetime(),
            end_date=dates[-1].to_pydatetime(),
            bar_size=BarSize.Days1,
        )
        bt = Backtester(storage=storage, config=config)
        result = bt.run_from_module("strategies/pairs_zscore.py", "PairsZScore", [4391, 4815747])
        assert result is not None
        assert np.isfinite(result.total_return)

    def test_diverging_pair_generates_signal(self, tmp_duckdb_path):
        """When two prices diverge, backtester should generate a trade."""
        store = DuckDBDataStore(tmp_duckdb_path)
        rng = np.random.default_rng(42)
        n = 100
        dates = pd.date_range("2024-01-02", periods=n, freq="1D", tz="UTC")

        # Two series: correlated initially, then conid 1 rises while conid 2 stays flat
        base = 100 + np.cumsum(rng.normal(0, 0.5, n))
        close1 = base.copy()
        close1[50:] = close1[50:] + np.arange(n - 50) * 3  # diverge up strongly

        close2 = base.copy()  # stays near base

        for conid, close in [("100", close1), ("200", close2)]:
            df = pd.DataFrame({
                "open": close - 0.5, "high": close + 1, "low": close - 1,
                "close": close, "volume": rng.integers(500, 5000, n).astype(float),
                "average": close, "bar_count": [50] * n,
            }, index=dates)
            df.index.name = "date"
            store.write(conid, df)

        storage = TickStorage(tmp_duckdb_path)
        config = BacktestConfig(
            start_date=dates[0].to_pydatetime(),
            end_date=dates[-1].to_pydatetime(),
            bar_size=BarSize.Days1,
        )
        bt = Backtester(storage=storage, config=config)
        result = bt.run_from_module("strategies/pairs_zscore.py", "PairsZScore", [100, 200])
        # Should generate at least 1 trade from the divergence
        assert result.total_trades >= 1, f"Expected trades from diverging pair, got {result.total_trades}"


# ---------------------------------------------------------------------------
# Tournament analytics
# ---------------------------------------------------------------------------

class TestTournamentAnalytics:
    def test_sortino_positive_for_profitable(self):
        from scripts.tournament import sortino_ratio
        # Pure upward equity curve
        equity = pd.Series([100, 101, 102, 103, 104, 105])
        s = sortino_ratio(equity, BarSize.Days1)
        assert s > 0

    def test_sortino_negative_for_losing(self):
        from scripts.tournament import sortino_ratio
        equity = pd.Series([100, 99, 98, 97, 96, 95])
        s = sortino_ratio(equity, BarSize.Days1)
        assert s < 0

    def test_calmar_positive_for_profitable(self):
        from scripts.tournament import calmar_ratio
        equity = pd.Series([100, 101, 102, 103, 104, 105])
        c = calmar_ratio(equity, BarSize.Days1)
        assert c > 0

    def test_profit_factor_infinite_all_wins(self):
        from scripts.tournament import profit_factor
        trades = [
            BacktestTrade(dt.datetime.now(), 1, Action.BUY, 10, 100.0, 0.05, 0.8, 0.2),
            BacktestTrade(dt.datetime.now(), 1, Action.SELL, 10, 110.0, 0.05, 0.7, 0.3),
        ]
        pf = profit_factor(trades)
        assert pf == float("inf")

    def test_profit_factor_with_losses(self):
        from scripts.tournament import profit_factor
        trades = [
            BacktestTrade(dt.datetime.now(), 1, Action.BUY, 10, 100.0, 0.05, 0.8, 0.2),
            BacktestTrade(dt.datetime.now(), 1, Action.SELL, 10, 90.0, 0.05, 0.7, 0.3),
            BacktestTrade(dt.datetime.now(), 1, Action.BUY, 10, 90.0, 0.05, 0.8, 0.2),
            BacktestTrade(dt.datetime.now(), 1, Action.SELL, 10, 110.0, 0.05, 0.7, 0.3),
        ]
        pf = profit_factor(trades)
        assert pf > 1.0  # Net profitable

    def test_max_consecutive_losses(self):
        from scripts.tournament import max_consecutive_losses
        trades = [
            BacktestTrade(dt.datetime.now(), 1, Action.BUY, 10, 100.0, 0.05, 0.8, 0.2),
            BacktestTrade(dt.datetime.now(), 1, Action.SELL, 10, 90.0, 0.05, 0.7, 0.3),  # loss
            BacktestTrade(dt.datetime.now(), 1, Action.BUY, 10, 90.0, 0.05, 0.8, 0.2),
            BacktestTrade(dt.datetime.now(), 1, Action.SELL, 10, 85.0, 0.05, 0.7, 0.3),  # loss
            BacktestTrade(dt.datetime.now(), 1, Action.BUY, 10, 85.0, 0.05, 0.8, 0.2),
            BacktestTrade(dt.datetime.now(), 1, Action.SELL, 10, 100.0, 0.05, 0.7, 0.3),  # win
        ]
        streak = max_consecutive_losses(trades)
        assert streak == 2

    def test_avg_trade_duration(self):
        from scripts.tournament import avg_trade_duration
        t0 = dt.datetime(2024, 1, 1)
        t1 = dt.datetime(2024, 1, 11)  # 10 days later
        trades = [
            BacktestTrade(t0, 1, Action.BUY, 10, 100.0, 0.05, 0.8, 0.2),
            BacktestTrade(t1, 1, Action.SELL, 10, 110.0, 0.05, 0.7, 0.3),
        ]
        avg = avg_trade_duration(trades)
        assert abs(avg - 10.0) < 0.01

    def test_rolling_beta_same_series(self):
        from scripts.tournament import rolling_beta
        # Beta of a series with itself should be ~1.0
        rng = np.random.default_rng(42)
        prices = pd.Series(100 + np.cumsum(rng.normal(0.1, 1, 100)),
                           index=pd.date_range("2024-01-01", periods=100))
        betas = rolling_beta(prices, prices, window=20)
        if len(betas) > 0:
            assert abs(betas.iloc[-1] - 1.0) < 0.01
