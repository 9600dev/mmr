"""Advanced backtester tests — multi-conid, load_strategy_class, run_from_module, edge cases."""

import datetime as dt
import logging
import os
import sys
import tempfile

import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from trader.data.data_access import TickStorage
from trader.data.duckdb_store import DuckDBDataStore
from trader.data.market_data import NORMALIZED_COLUMNS
from trader.data.universe import UniverseAccessor
from trader.objects import Action, BarSize
from trader.simulation.backtester import Backtester, BacktestConfig
from trader.trading.strategy import Signal, Strategy, StrategyContext, StrategyState


# ---------------------------------------------------------------------------
# Test strategies
# ---------------------------------------------------------------------------

class NeverSignalStrategy(Strategy):
    def on_prices(self, prices):
        return None


class AlternatingStrategy(Strategy):
    """Buys on odd calls, sells on even calls (after first buy)."""

    def __init__(self):
        super().__init__()
        self._count = 0
        self._bought = False

    def on_prices(self, prices):
        self._count += 1
        if self._count >= 3 and not self._bought:
            self._bought = True
            return Signal("alt", Action.BUY, 0.9, 0.1, quantity=10)
        if self._count >= 10 and self._bought:
            self._bought = False
            return Signal("alt", Action.SELL, 0.9, 0.1, quantity=10)
        return None


class MultiConidStrategy(Strategy):
    """Tracks which conids it receives data for."""

    def __init__(self):
        super().__init__()
        self.seen_conids = set()
        self.call_count = 0

    def on_prices(self, prices):
        self.call_count += 1
        return None


class ColumnCheckerStrategy(Strategy):
    """Verifies that on_prices receives normalized columns."""

    def __init__(self):
        super().__init__()
        self.column_lists = []

    def on_prices(self, prices):
        self.column_lists.append(list(prices.columns))
        return None


class ZeroStdStrategy(Strategy):
    """Returns a buy signal exactly once at bar 3, creating zero-variance returns."""

    def __init__(self):
        super().__init__()
        self._count = 0

    def on_prices(self, prices):
        self._count += 1
        if self._count == 3:
            return Signal("zero_std", Action.BUY, 0.5, 0.5, quantity=1)
        return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_bars(duckdb_path, conid=4391, n=30, base_price=100.0,
                start="2024-06-01 09:30", seed=42):
    store = DuckDBDataStore(duckdb_path)
    dates = pd.date_range(start, periods=n, freq="1min", tz="UTC")
    rng = np.random.default_rng(seed)
    close = base_price + np.cumsum(rng.normal(0.05, 0.2, n))
    df = pd.DataFrame({
        "open": close - 0.1,
        "high": close + 0.5,
        "low": close - 0.5,
        "close": close,
        "volume": rng.integers(500, 5000, n).astype(float),
        "average": close + 0.02,
        "bar_count": rng.integers(10, 100, n),
    }, index=dates)
    df.index.name = "date"
    store.write(str(conid), df)
    return df


def _make_backtester(duckdb_path, **config_kwargs):
    storage = TickStorage(duckdb_path=duckdb_path)
    config = BacktestConfig(
        start_date=dt.datetime(2024, 6, 1, 9, 30, tzinfo=dt.timezone.utc),
        end_date=dt.datetime(2024, 6, 1, 10, 30, tzinfo=dt.timezone.utc),
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
        paper=True,
        storage=storage,
        universe_accessor=ua,
        logger=logging.getLogger("test"),
    )
    strategy.install(ctx)
    strategy.state = StrategyState.RUNNING
    return strategy


# ---------------------------------------------------------------------------
# Multi-conid tests
# ---------------------------------------------------------------------------

class TestMultiConidBacktest:
    def test_two_conids_merged(self, tmp_duckdb_path):
        _write_bars(tmp_duckdb_path, conid=4391, n=20, base_price=100.0, seed=42)
        _write_bars(tmp_duckdb_path, conid=265598, n=20, base_price=175.0, seed=99)

        bt = _make_backtester(tmp_duckdb_path)
        strategy = _install_strategy(NeverSignalStrategy(), tmp_duckdb_path, conids=[4391, 265598])
        result = bt.run(strategy, [4391, 265598])

        assert result.total_trades == 0
        assert len(result.equity_curve) > 0
        # All equity should equal initial capital since no trades
        assert all(v == pytest.approx(100_000.0) for v in result.equity_curve.values)

    def test_multi_conid_strategy_receives_normalized_columns(self, tmp_duckdb_path):
        _write_bars(tmp_duckdb_path, conid=4391, n=20, seed=42)
        _write_bars(tmp_duckdb_path, conid=265598, n=20, seed=99)

        bt = _make_backtester(tmp_duckdb_path)
        checker = ColumnCheckerStrategy()
        strategy = _install_strategy(checker, tmp_duckdb_path, conids=[4391, 265598])
        bt.run(strategy, [4391, 265598])

        # Every on_prices call should have normalized columns
        for cols in checker.column_lists:
            assert cols == NORMALIZED_COLUMNS

    def test_multi_conid_with_trades(self, tmp_duckdb_path):
        _write_bars(tmp_duckdb_path, conid=4391, n=20, seed=42)
        _write_bars(tmp_duckdb_path, conid=265598, n=20, seed=99)

        bt = _make_backtester(tmp_duckdb_path)
        strategy = _install_strategy(AlternatingStrategy(), tmp_duckdb_path, conids=[4391, 265598])
        result = bt.run(strategy, [4391, 265598])

        assert result.total_trades > 0

    def test_one_conid_no_data(self, tmp_duckdb_path):
        """If only one of two conids has data, backtest should still run."""
        _write_bars(tmp_duckdb_path, conid=4391, n=20)
        # conid 99999 has no data

        bt = _make_backtester(tmp_duckdb_path)
        strategy = _install_strategy(NeverSignalStrategy(), tmp_duckdb_path, conids=[4391, 99999])
        result = bt.run(strategy, [4391, 99999])

        assert len(result.equity_curve) > 0


# ---------------------------------------------------------------------------
# No data edge case
# ---------------------------------------------------------------------------

class TestBacktesterNoData:
    def test_no_data_raises_value_error(self, tmp_duckdb_path):
        bt = _make_backtester(tmp_duckdb_path)
        strategy = _install_strategy(NeverSignalStrategy(), tmp_duckdb_path)

        with pytest.raises(ValueError, match="no historical data"):
            bt.run(strategy, [99999])


# ---------------------------------------------------------------------------
# _load_strategy_class tests
# ---------------------------------------------------------------------------

class TestLoadStrategyClass:
    def test_load_valid_strategy(self, tmp_duckdb_path, tmp_path):
        """Load a strategy class from a file."""
        strategy_code = '''
from trader.trading.strategy import Strategy, Signal

class TestStrat(Strategy):
    def on_prices(self, prices):
        return None
'''
        strategy_file = tmp_path / "test_strat.py"
        strategy_file.write_text(strategy_code)

        bt = _make_backtester(tmp_duckdb_path)
        cls = bt._load_strategy_class(str(strategy_file), "TestStrat")
        assert issubclass(cls, Strategy)

    def test_load_nonexistent_file(self, tmp_duckdb_path):
        bt = _make_backtester(tmp_duckdb_path)
        with pytest.raises(FileNotFoundError, match="not found"):
            bt._load_strategy_class("/tmp/nonexistent_strategy.py", "Foo")

    def test_load_wrong_class_name(self, tmp_duckdb_path, tmp_path):
        strategy_code = '''
from trader.trading.strategy import Strategy

class RealStrat(Strategy):
    def on_prices(self, prices):
        return None
'''
        strategy_file = tmp_path / "real_strat.py"
        strategy_file.write_text(strategy_code)

        bt = _make_backtester(tmp_duckdb_path)
        with pytest.raises(AttributeError):
            bt._load_strategy_class(str(strategy_file), "WrongName")

    def test_load_non_strategy_class(self, tmp_duckdb_path, tmp_path):
        code = '''
class NotAStrategy:
    pass
'''
        f = tmp_path / "not_strategy.py"
        f.write_text(code)

        bt = _make_backtester(tmp_duckdb_path)
        with pytest.raises(TypeError, match="not a subclass"):
            bt._load_strategy_class(str(f), "NotAStrategy")


# ---------------------------------------------------------------------------
# run_from_module tests
# ---------------------------------------------------------------------------

class TestRunFromModule:
    def test_run_from_module_end_to_end(self, tmp_duckdb_path, tmp_path):
        _write_bars(tmp_duckdb_path, conid=4391, n=20)

        strategy_code = '''
from trader.trading.strategy import Strategy, Signal
from trader.objects import Action

class FileStrat(Strategy):
    def on_prices(self, prices):
        if len(prices) == 5:
            return Signal("file_strat", Action.BUY, 0.9, 0.1, quantity=10)
        return None
'''
        strategy_file = tmp_path / "file_strat.py"
        strategy_file.write_text(strategy_code)

        bt = _make_backtester(tmp_duckdb_path)
        result = bt.run_from_module(str(strategy_file), "FileStrat", [4391])

        assert result.total_trades > 0
        assert len(result.equity_curve) > 0


# ---------------------------------------------------------------------------
# Sharpe ratio edge cases
# ---------------------------------------------------------------------------

class TestSharpeRatioEdgeCases:
    def test_zero_std_returns_zero_sharpe(self, tmp_duckdb_path):
        """When all returns are the same, std=0 and sharpe should be 0."""
        _write_bars(tmp_duckdb_path, conid=4391, n=20)

        bt = _make_backtester(tmp_duckdb_path)
        strategy = _install_strategy(NeverSignalStrategy(), tmp_duckdb_path)
        result = bt.run(strategy, [4391])

        # No trades = flat equity = zero returns = zero sharpe
        assert result.sharpe_ratio == pytest.approx(0.0)

    def test_single_bar_zero_sharpe(self, tmp_duckdb_path):
        """With only 1 bar, sharpe should be 0."""
        _write_bars(tmp_duckdb_path, conid=4391, n=1)

        bt = _make_backtester(tmp_duckdb_path)
        strategy = _install_strategy(NeverSignalStrategy(), tmp_duckdb_path)
        result = bt.run(strategy, [4391])

        assert result.sharpe_ratio == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# Win rate tests
# ---------------------------------------------------------------------------

class TestWinRate:
    def test_no_sell_trades_zero_win_rate(self, tmp_duckdb_path):
        """With only buy trades, win rate should be 0."""

        class OnlyBuyStrategy(Strategy):
            def __init__(self):
                super().__init__()
                self._count = 0

            def on_prices(self, prices):
                self._count += 1
                if self._count == 5:
                    return Signal("only_buy", Action.BUY, 0.9, 0.1, quantity=5)
                return None

        _write_bars(tmp_duckdb_path, conid=4391, n=20)
        bt = _make_backtester(tmp_duckdb_path)
        strategy = _install_strategy(OnlyBuyStrategy(), tmp_duckdb_path)
        result = bt.run(strategy, [4391])

        assert result.win_rate == pytest.approx(0.0)

    def test_no_trades_zero_win_rate(self, tmp_duckdb_path):
        _write_bars(tmp_duckdb_path, conid=4391, n=20)
        bt = _make_backtester(tmp_duckdb_path)
        strategy = _install_strategy(NeverSignalStrategy(), tmp_duckdb_path)
        result = bt.run(strategy, [4391])

        assert result.win_rate == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# Normalization integration
# ---------------------------------------------------------------------------

class TestNoShortSelling:
    """Verify the backtester doesn't allow short selling (selling without position)."""

    def test_sell_without_position_skipped(self, tmp_duckdb_path):
        """SELL signal when no position is held should be skipped."""

        class SellFirstStrategy(Strategy):
            def __init__(self):
                super().__init__()
                self._count = 0

            def on_prices(self, prices):
                self._count += 1
                if self._count == 3:
                    return Signal("sell_first", Action.SELL, 0.9, 0.1, quantity=10)
                return None

        _write_bars(tmp_duckdb_path, conid=4391, n=20)
        bt = _make_backtester(tmp_duckdb_path)
        strategy = _install_strategy(SellFirstStrategy(), tmp_duckdb_path)
        result = bt.run(strategy, [4391])

        # No trades should execute — SELL without position should be skipped
        assert result.total_trades == 0
        # Equity should remain at initial capital
        assert all(v == pytest.approx(100_000.0) for v in result.equity_curve.values)

    def test_sell_only_held_quantity(self, tmp_duckdb_path):
        """SELL should only sell what we actually hold, not more."""

        class BuyThenOversell(Strategy):
            def __init__(self):
                super().__init__()
                self._count = 0

            def on_prices(self, prices):
                self._count += 1
                if self._count == 3:
                    return Signal("bto", Action.BUY, 0.9, 0.1, quantity=5)
                if self._count == 10:
                    return Signal("bto", Action.SELL, 0.9, 0.1, quantity=100)  # Try to sell 100
                return None

        _write_bars(tmp_duckdb_path, conid=4391, n=20)
        bt = _make_backtester(tmp_duckdb_path)
        strategy = _install_strategy(BuyThenOversell(), tmp_duckdb_path)
        result = bt.run(strategy, [4391])

        assert result.total_trades == 2
        sell_trade = [t for t in result.trades if t.action == Action.SELL][0]
        assert sell_trade.quantity == 5  # Only sold what was held

    def test_buy_sell_buy_cycle(self, tmp_duckdb_path):
        """Full buy → sell → buy cycle should work correctly."""

        class CycleStrategy(Strategy):
            def __init__(self):
                super().__init__()
                self._count = 0

            def on_prices(self, prices):
                self._count += 1
                if self._count == 3:
                    return Signal("cycle", Action.BUY, 0.9, 0.1, quantity=10)
                if self._count == 8:
                    return Signal("cycle", Action.SELL, 0.9, 0.1, quantity=10)
                if self._count == 13:
                    return Signal("cycle", Action.BUY, 0.9, 0.1, quantity=10)
                return None

        _write_bars(tmp_duckdb_path, conid=4391, n=20)
        bt = _make_backtester(tmp_duckdb_path)
        strategy = _install_strategy(CycleStrategy(), tmp_duckdb_path)
        result = bt.run(strategy, [4391])

        assert result.total_trades == 3
        actions = [t.action for t in result.trades]
        assert actions == [Action.BUY, Action.SELL, Action.BUY]


class TestBacktesterNormalizationIntegration:
    def test_historical_data_is_normalized(self, tmp_duckdb_path):
        """Data from DuckDB should be normalized before reaching strategies."""
        checker = ColumnCheckerStrategy()
        _write_bars(tmp_duckdb_path, conid=4391, n=20)

        bt = _make_backtester(tmp_duckdb_path)
        strategy = _install_strategy(checker, tmp_duckdb_path)
        bt.run(strategy, [4391])

        assert len(checker.column_lists) == 20
        for cols in checker.column_lists:
            assert cols == NORMALIZED_COLUMNS

    def test_vwap_renamed_from_average(self, tmp_duckdb_path):
        """The 'average' column from DuckDB should appear as 'vwap' after normalization."""

        class VwapChecker(Strategy):
            def __init__(self):
                super().__init__()
                self.has_vwap = []
                self.has_average = []

            def on_prices(self, prices):
                self.has_vwap.append("vwap" in prices.columns)
                self.has_average.append("average" in prices.columns)
                return None

        _write_bars(tmp_duckdb_path, conid=4391, n=10)
        bt = _make_backtester(tmp_duckdb_path)
        checker = VwapChecker()
        strategy = _install_strategy(checker, tmp_duckdb_path)
        bt.run(strategy, [4391])

        assert all(checker.has_vwap), "vwap should be present in all on_prices calls"
        assert not any(checker.has_average), "average should NOT be present after normalization"

    def test_bar_size_dropped(self, tmp_duckdb_path):
        """bar_size and what_to_show should be dropped by normalization."""

        class MetadataChecker(Strategy):
            def __init__(self):
                super().__init__()
                self.columns_seen = []

            def on_prices(self, prices):
                self.columns_seen.append(set(prices.columns))
                return None

        _write_bars(tmp_duckdb_path, conid=4391, n=10)
        bt = _make_backtester(tmp_duckdb_path)
        checker = MetadataChecker()
        strategy = _install_strategy(checker, tmp_duckdb_path)
        bt.run(strategy, [4391])

        for cols in checker.columns_seen:
            assert "bar_size" not in cols
            assert "what_to_show" not in cols
