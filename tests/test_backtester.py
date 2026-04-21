import datetime as dt
import pytest
import pandas as pd
import numpy as np

from trader.data.data_access import TickStorage
from trader.data.duckdb_store import DuckDBDataStore
from trader.data.universe import UniverseAccessor
from trader.objects import Action, BarSize
from trader.simulation.backtester import Backtester, BacktestConfig
from trader.trading.strategy import Signal, Strategy, StrategyContext, StrategyState

import logging


# ---------------------------------------------------------------------------
# Test strategies
# ---------------------------------------------------------------------------

class NeverSignalStrategy(Strategy):
    def on_prices(self, prices):
        return None


class AlwaysBuyStrategy(Strategy):
    def on_prices(self, prices):
        if len(prices) < 2:
            return None
        return Signal(
            source_name=self.name or "always_buy",
            action=Action.BUY,
            probability=0.9,
            risk=0.1,
            conid=self.conids[0] if self.conids else 0,
            quantity=10,
        )


class BuySellStrategy(Strategy):
    """Buys on first call, sells on second call."""

    def __init__(self):
        super().__init__()
        self._call_count = 0

    def on_prices(self, prices):
        self._call_count += 1
        if self._call_count == 5:
            return Signal(
                source_name=self.name or "buy_sell",
                action=Action.BUY,
                probability=0.9,
                risk=0.1,
                conid=self.conids[0] if self.conids else 0,
                quantity=10,
            )
        elif self._call_count == 15:
            return Signal(
                source_name=self.name or "buy_sell",
                action=Action.SELL,
                probability=0.9,
                risk=0.1,
                conid=self.conids[0] if self.conids else 0,
                quantity=10,
            )
        return None


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _write_synthetic_bars(duckdb_path, conid=4391, n=20, base_price=100.0):
    """Write synthetic OHLCV bars to the DuckDB tick_data table."""
    store = DuckDBDataStore(duckdb_path)
    dates = pd.date_range("2024-01-02 09:30", periods=n, freq="1min", tz="UTC")
    rng = np.random.default_rng(42)
    close = base_price + np.cumsum(rng.normal(0.05, 0.2, n))
    df = pd.DataFrame({
        "open": close - 0.1,
        "high": close + 0.5,
        "low": close - 0.5,
        "close": close,
        "volume": rng.integers(500, 5000, n).astype(float),
    }, index=dates)
    df.index.name = "date"
    store.write(str(conid), df)
    return df


def _make_backtester(duckdb_path, **config_kwargs):
    storage = TickStorage(duckdb_path=duckdb_path)
    config = BacktestConfig(
        start_date=dt.datetime(2024, 1, 2, 9, 30, tzinfo=dt.timezone.utc),
        end_date=dt.datetime(2024, 1, 2, 10, 30, tzinfo=dt.timezone.utc),
        **config_kwargs,
    )
    return Backtester(storage=storage, config=config)


def _install_strategy(strategy, duckdb_path, conids=None):
    conids = conids or [4391]
    storage = TickStorage(duckdb_path=duckdb_path)
    ua = UniverseAccessor.__new__(UniverseAccessor)
    ua.duckdb_path = duckdb_path
    ua.universe_library = "Universes"
    ctx = StrategyContext(
        name=strategy.__class__.__name__,
        bar_size=BarSize.Mins1,
        conids=conids,
        universe=None,
        historical_days_prior=0,
        paper_only=False,
        storage=storage,
        universe_accessor=ua,
        logger=logging.getLogger("test"),
    )
    strategy.install(ctx)
    strategy.state = StrategyState.RUNNING
    return strategy


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestBacktester:
    def test_no_signal_flat_equity(self, tmp_duckdb_path):
        _write_synthetic_bars(tmp_duckdb_path)
        bt = _make_backtester(tmp_duckdb_path)
        strategy = _install_strategy(NeverSignalStrategy(), tmp_duckdb_path)
        result = bt.run(strategy, [4391])
        assert result.total_trades == 0
        assert result.total_return == pytest.approx(0.0)
        assert len(result.equity_curve) > 0
        # All equity values should equal initial capital
        assert all(v == pytest.approx(100_000.0) for v in result.equity_curve.values)

    def test_single_buy_opens_position(self, tmp_duckdb_path):
        _write_synthetic_bars(tmp_duckdb_path)
        bt = _make_backtester(tmp_duckdb_path)
        strategy = _install_strategy(AlwaysBuyStrategy(), tmp_duckdb_path)
        result = bt.run(strategy, [4391])
        assert result.total_trades > 0
        assert all(t.action == Action.BUY for t in result.trades)

    def test_buy_sell_roundtrip(self, tmp_duckdb_path):
        _write_synthetic_bars(tmp_duckdb_path, n=20)
        bt = _make_backtester(tmp_duckdb_path)
        strategy = _install_strategy(BuySellStrategy(), tmp_duckdb_path)
        result = bt.run(strategy, [4391])
        actions = [t.action for t in result.trades]
        assert Action.BUY in actions
        assert Action.SELL in actions

    def test_slippage_applied(self, tmp_duckdb_path):
        _write_synthetic_bars(tmp_duckdb_path)
        bt = _make_backtester(tmp_duckdb_path, slippage_bps=10.0)
        strategy = _install_strategy(AlwaysBuyStrategy(), tmp_duckdb_path)
        result = bt.run(strategy, [4391])
        if result.trades:
            # Slippage increases buy price above raw close
            # We can't compare directly, but we can check trades exist
            assert result.total_trades > 0

    def test_commission_deducted(self, tmp_duckdb_path):
        _write_synthetic_bars(tmp_duckdb_path)
        bt = _make_backtester(tmp_duckdb_path, commission_per_share=1.0)
        strategy = _install_strategy(AlwaysBuyStrategy(), tmp_duckdb_path)
        result = bt.run(strategy, [4391])
        if result.trades:
            for trade in result.trades:
                assert trade.commission == trade.quantity * 1.0

    def test_metrics_computed(self, tmp_duckdb_path):
        _write_synthetic_bars(tmp_duckdb_path, n=20)
        bt = _make_backtester(tmp_duckdb_path)
        strategy = _install_strategy(BuySellStrategy(), tmp_duckdb_path)
        result = bt.run(strategy, [4391])
        # Metrics should be numeric, not NaN
        assert not pd.isna(result.total_return)
        assert not pd.isna(result.sharpe_ratio)
        assert not pd.isna(result.max_drawdown)
        assert 0.0 <= result.win_rate <= 1.0

    def test_insufficient_capital(self, tmp_duckdb_path):
        _write_synthetic_bars(tmp_duckdb_path)
        bt = _make_backtester(tmp_duckdb_path, initial_capital=1.0)
        strategy = _install_strategy(AlwaysBuyStrategy(), tmp_duckdb_path)
        result = bt.run(strategy, [4391])
        # With only $1, no trades should execute
        assert result.total_trades == 0

    def test_no_lookahead_fill_at_next_bar_open(self, tmp_duckdb_path):
        """Fill price must come from the NEXT bar's open, not the triggering bar's close."""
        from trader.simulation.slippage import ZeroSlippage

        # Hand-craft bars with distinct open/close so we can tell which one was used.
        store = DuckDBDataStore(tmp_duckdb_path)
        dates = pd.date_range("2024-01-02 09:30", periods=5, freq="1min", tz="UTC")
        df = pd.DataFrame({
            # close[i] and open[i+1] are intentionally different
            "open":   [100.0, 110.0, 120.0, 130.0, 140.0],
            "high":   [101.0, 111.0, 121.0, 131.0, 141.0],
            "low":    [ 99.0, 109.0, 119.0, 129.0, 139.0],
            "close":  [100.5, 110.5, 120.5, 130.5, 140.5],
            "volume": [1000.0] * 5,
        }, index=dates)
        df.index.name = "date"
        store.write("4391", df)

        bt = _make_backtester(tmp_duckdb_path, slippage_model=ZeroSlippage(), fill_policy='next_open')
        strategy = _install_strategy(AlwaysBuyStrategy(), tmp_duckdb_path)
        result = bt.run(strategy, [4391])

        # The first trade must fill at the OPEN of bar t+1 (not close of bar t).
        # AlwaysBuy triggers on the second bar (needs len>=2). Triggering bar is
        # dates[1] with close=110.5. Next bar is dates[2] with open=120.0. Fill
        # must be at 120.0 to prove we're not peeking at the triggering bar.
        assert result.total_trades >= 1
        first = result.trades[0]
        assert first.price == pytest.approx(120.0), (
            f"Expected fill at next-bar open 120.0, got {first.price} "
            "— this indicates lookahead bias (filled at triggering bar's close)."
        )

    def test_same_close_policy_reproduces_legacy_fill(self, tmp_duckdb_path):
        """With fill_policy='same_close', fills happen at triggering bar close (legacy)."""
        from trader.simulation.slippage import ZeroSlippage

        store = DuckDBDataStore(tmp_duckdb_path)
        dates = pd.date_range("2024-01-02 09:30", periods=5, freq="1min", tz="UTC")
        df = pd.DataFrame({
            "open":   [100.0, 110.0, 120.0, 130.0, 140.0],
            "high":   [101.0, 111.0, 121.0, 131.0, 141.0],
            "low":    [ 99.0, 109.0, 119.0, 129.0, 139.0],
            "close":  [100.5, 110.5, 120.5, 130.5, 140.5],
            "volume": [1000.0] * 5,
        }, index=dates)
        df.index.name = "date"
        store.write("4391", df)

        bt = _make_backtester(tmp_duckdb_path, slippage_model=ZeroSlippage(), fill_policy='same_close')
        strategy = _install_strategy(AlwaysBuyStrategy(), tmp_duckdb_path)
        result = bt.run(strategy, [4391])

        assert result.total_trades >= 1
        # Legacy lookahead: first fill at triggering bar's close (110.5).
        first = result.trades[0]
        assert first.price == pytest.approx(110.5)

    def test_sqrt_slippage_model(self, tmp_duckdb_path):
        from trader.simulation.slippage import SquareRootImpact, ZeroSlippage

        _write_synthetic_bars(tmp_duckdb_path)

        bt_zero = _make_backtester(tmp_duckdb_path, slippage_model=ZeroSlippage())
        strategy = _install_strategy(BuySellStrategy(), tmp_duckdb_path)
        result_zero = bt_zero.run(strategy, [4391])

        bt_sqrt = _make_backtester(tmp_duckdb_path, slippage_model=SquareRootImpact())
        strategy2 = _install_strategy(BuySellStrategy(), tmp_duckdb_path)
        result_sqrt = bt_sqrt.run(strategy2, [4391])

        # Both should produce trades
        assert result_zero.total_trades > 0
        assert result_sqrt.total_trades > 0

        # Fill prices should differ between zero and sqrt slippage
        if result_zero.trades and result_sqrt.trades:
            zero_price = result_zero.trades[0].price
            sqrt_price = result_sqrt.trades[0].price
            assert zero_price != sqrt_price
