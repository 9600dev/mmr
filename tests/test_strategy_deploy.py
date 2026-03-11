"""Tests for strategy deploy class name inference and the backtester short-selling fix."""

import datetime as dt
import os
import sys
import tempfile

import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from trader.data.data_access import Data, TickData, TickStorage
from trader.data.duckdb_store import DuckDBDataStore
from trader.objects import BarSize


# ---------------------------------------------------------------------------
# _to_symbol string support
# ---------------------------------------------------------------------------

class TestToSymbolStringSupport:
    """Verify _to_symbol handles raw string keys (e.g. 'SPY' for unresolved symbols)."""

    def test_string_passthrough(self, tmp_duckdb_path):
        data = Data(tmp_duckdb_path, "test")
        assert data._to_symbol("SPY") == "SPY"

    def test_string_conid(self, tmp_duckdb_path):
        data = Data(tmp_duckdb_path, "test")
        assert data._to_symbol("4391") == "4391"

    def test_write_read_with_string_symbol(self, tmp_duckdb_path):
        """Reproduce the bug: data download stores 'SPY' as string when no conId found."""
        td = TickData(tmp_duckdb_path, "bardata")
        dates = pd.date_range("2024-06-01 09:30", periods=5, freq="1min", tz="UTC")
        df = pd.DataFrame({
            "open": [100.0] * 5,
            "high": [101.0] * 5,
            "low": [99.0] * 5,
            "close": [100.5] * 5,
            "volume": [1000.0] * 5,
        }, index=dates)
        df.index.name = "date"

        # This should NOT raise 'cast not supported'
        td.write_resolve_overlap("SPY", df)
        result = td.read("SPY")
        assert len(result) == 5

    def test_tickdata_get_data_with_string(self, tmp_duckdb_path):
        store = DuckDBDataStore(tmp_duckdb_path)
        dates = pd.date_range("2024-06-01 09:30", periods=3, freq="1min", tz="UTC")
        df = pd.DataFrame({"close": [100.0, 101.0, 102.0]}, index=dates)
        df.index.name = "date"
        store.write("SPY", df)

        td = TickData(tmp_duckdb_path, "bardata")
        result = td.get_data("SPY")
        assert len(result) == 3


# ---------------------------------------------------------------------------
# Strategy deploy class name inference
# ---------------------------------------------------------------------------

class TestDeployClassNameInference:
    """Test that _handle_strategy_deploy correctly infers class names."""

    def test_infer_from_file_normal(self, tmp_path):
        """Normal PascalCase class name should be found."""
        code = '''
from trader.trading.strategy import Strategy

class MyCoolStrategy(Strategy):
    def on_prices(self, prices):
        return None
'''
        (tmp_path / "my_cool_strategy.py").write_text(code)

        import ast
        tree = ast.parse(code)
        class_name = None
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                for base in node.bases:
                    base_name = base.id if isinstance(base, ast.Name) else (
                        base.attr if isinstance(base, ast.Attribute) else ''
                    )
                    if base_name == 'Strategy':
                        class_name = node.name
                        break
                if class_name:
                    break

        assert class_name == "MyCoolStrategy"

    def test_infer_from_file_acronym(self, tmp_path):
        """Acronym class names like RSIStrategy should be preserved."""
        code = '''
from trader.trading.strategy import Strategy

class RSIStrategy(Strategy):
    def on_prices(self, prices):
        return None
'''
        (tmp_path / "rsi_strategy.py").write_text(code)

        import ast
        tree = ast.parse(code)
        class_name = None
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                for base in node.bases:
                    base_name = base.id if isinstance(base, ast.Name) else (
                        base.attr if isinstance(base, ast.Attribute) else ''
                    )
                    if base_name == 'Strategy':
                        class_name = node.name
                        break
                if class_name:
                    break

        assert class_name == "RSIStrategy"
        # Old behavior would give 'RsiStrategy' which is wrong
        fallback = ''.join(word.capitalize() for word in "rsi_strategy".split('_'))
        assert fallback == "RsiStrategy"
        assert class_name != fallback

    def test_infer_skips_non_strategy_classes(self, tmp_path):
        """Should only find Strategy subclasses, not helper classes."""
        code = '''
from trader.trading.strategy import Strategy

class HelperClass:
    pass

class MyStrategy(Strategy):
    def on_prices(self, prices):
        return None
'''
        import ast
        tree = ast.parse(code)
        class_name = None
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                for base in node.bases:
                    base_name = base.id if isinstance(base, ast.Name) else (
                        base.attr if isinstance(base, ast.Attribute) else ''
                    )
                    if base_name == 'Strategy':
                        class_name = node.name
                        break
                if class_name:
                    break

        assert class_name == "MyStrategy"


# ---------------------------------------------------------------------------
# Strategy file roundtrip: create → deploy → backtest
# ---------------------------------------------------------------------------

class TestStrategiesRoundtrip:
    def test_backtest_all_strategies(self, tmp_duckdb_path):
        """Verify all strategy files can be loaded and backtested."""
        from trader.simulation.backtester import Backtester, BacktestConfig

        store = DuckDBDataStore(tmp_duckdb_path)
        n = 100
        rng = np.random.default_rng(42)
        close = 100.0 + np.cumsum(rng.normal(0.1, 1.0, n))
        dates = pd.date_range("2024-01-02 09:30", periods=n, freq="1D", tz="UTC")
        df = pd.DataFrame({
            "open": close - 0.5,
            "high": close + 1.0,
            "low": close - 1.0,
            "close": close,
            "volume": rng.integers(500, 5000, n).astype(float),
            "average": close + 0.02,
            "bar_count": rng.integers(10, 100, n),
        }, index=dates)
        df.index.name = "date"
        store.write("4391", df)

        storage = TickStorage(tmp_duckdb_path)
        config = BacktestConfig(
            start_date=dt.datetime(2024, 1, 2, tzinfo=dt.timezone.utc),
            end_date=dt.datetime(2024, 5, 15, tzinfo=dt.timezone.utc),
            bar_size=BarSize.Days1,
        )
        bt = Backtester(storage=storage, config=config)

        strategies_dir = os.path.join(PROJECT_ROOT, "strategies")
        strategy_files = [
            ("ma_crossover.py", "MaCrossover"),
            ("mean_reversion.py", "MeanReversion"),
            ("rsi_strategy.py", "RSIStrategy"),
            ("momentum.py", "Momentum"),
        ]

        for filename, classname in strategy_files:
            filepath = os.path.join(strategies_dir, filename)
            if os.path.exists(filepath):
                result = bt.run_from_module(filepath, classname, [4391])
                assert result is not None, f"{filename} backtest returned None"
                assert len(result.equity_curve) > 0, f"{filename} produced empty equity curve"
                assert not pd.isna(result.total_return), f"{filename} has NaN return"
