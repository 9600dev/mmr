import datetime as dt
import math
import sys

import numpy as np
import pandas as pd
import pytest
from ib_async.contract import Stock
from ib_async.ticker import Ticker

from trader.data.duckdb_store import DuckDBDataStore
from trader.data.market_data import (
    NORMALIZED_COLUMNS,
    _ib_nan,
    normalize_historical,
    normalize_ticker,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ticker(**kwargs) -> Ticker:
    """Create a Ticker with sensible defaults, overridden by kwargs."""
    t = Ticker()
    t.contract = Stock("TEST", "SMART", "USD")
    t.contract.conId = 9999
    t.time = dt.datetime(2026, 3, 10, 14, 30, 0, tzinfo=dt.timezone.utc)
    t.bid = float("nan")
    t.ask = float("nan")
    t.last = float("nan")
    t.lastSize = float("nan")
    t.volume = float("nan")
    t.vwap = float("nan")
    for k, v in kwargs.items():
        setattr(t, k, v)
    return t


def _make_historical_df(n=20, include_average=True, include_bar_size=True,
                        include_what_to_show=True, tz="UTC"):
    """Build a DataFrame matching the DuckDB tick_data schema."""
    rng = np.random.default_rng(42)
    dates = pd.date_range("2024-01-02 09:30", periods=n, freq="1min", tz=tz)
    close = 100.0 + np.cumsum(rng.normal(0.05, 0.2, n))
    data = {
        "open": close - 0.1,
        "high": close + 0.5,
        "low": close - 0.5,
        "close": close,
        "volume": rng.integers(500, 5000, n).astype(float),
    }
    if include_average:
        data["average"] = close - 0.02
    if include_bar_size:
        data["bar_size"] = "1 min"
    if include_what_to_show:
        data["what_to_show"] = 1
    df = pd.DataFrame(data, index=dates)
    df.index.name = "date"
    return df


# ===================================================================
# _ib_nan
# ===================================================================

class TestIbNan:
    def test_none_returns_nan(self):
        assert np.isnan(_ib_nan(None))

    def test_normal_float(self):
        assert _ib_nan(42.5) == 42.5

    def test_zero(self):
        assert _ib_nan(0.0) == 0.0

    def test_negative(self):
        assert _ib_nan(-10.5) == -10.5

    def test_inf_returns_nan(self):
        assert np.isnan(_ib_nan(float("inf")))

    def test_neg_inf_returns_nan(self):
        assert np.isnan(_ib_nan(float("-inf")))

    def test_nan_returns_nan(self):
        assert np.isnan(_ib_nan(float("nan")))

    def test_sys_float_max_returns_nan(self):
        assert np.isnan(_ib_nan(sys.float_info.max))

    def test_large_but_valid_passes(self):
        val = 9.99e307
        assert _ib_nan(val) == val

    def test_string_returns_nan(self):
        assert np.isnan(_ib_nan("not a number"))

    def test_int_coerced_to_float(self):
        assert _ib_nan(100) == 100.0

    def test_bool_coerced(self):
        # bool is a subclass of int; True -> 1.0
        assert _ib_nan(True) == 1.0

    def test_object_returns_nan(self):
        assert np.isnan(_ib_nan(object()))


# ===================================================================
# normalize_ticker
# ===================================================================

class TestNormalizeTicker:
    def test_columns_match_schema(self):
        t = _make_ticker(bid=100.0, ask=100.1, last=100.05, lastSize=50,
                         volume=1000, vwap=100.03)
        df = normalize_ticker(t)
        assert list(df.columns) == NORMALIZED_COLUMNS

    def test_single_row(self):
        t = _make_ticker(last=200.0)
        df = normalize_ticker(t)
        assert len(df) == 1

    def test_index_is_datetimeindex_named_date(self):
        t = _make_ticker(last=200.0)
        df = normalize_ticker(t)
        assert isinstance(df.index, pd.DatetimeIndex)
        assert df.index.name == "date"

    def test_index_is_tz_aware(self):
        t = _make_ticker(last=200.0)
        df = normalize_ticker(t)
        assert df.index.tz is not None

    def test_last_used_for_ohlc(self):
        t = _make_ticker(bid=99.0, ask=101.0, last=100.5)
        df = normalize_ticker(t)
        assert df["open"].iloc[0] == 100.5
        assert df["high"].iloc[0] == 100.5
        assert df["low"].iloc[0] == 100.5
        assert df["close"].iloc[0] == 100.5

    def test_midpoint_fallback_when_no_last(self):
        t = _make_ticker(bid=200.0, ask=200.10)
        df = normalize_ticker(t)
        expected = (200.0 + 200.10) / 2.0
        assert df["close"].iloc[0] == pytest.approx(expected)

    def test_all_nan_when_no_prices(self):
        t = _make_ticker()  # all NaN
        df = normalize_ticker(t)
        assert np.isnan(df["close"].iloc[0])
        assert np.isnan(df["bid"].iloc[0])
        assert np.isnan(df["ask"].iloc[0])
        assert np.isnan(df["last"].iloc[0])

    def test_bid_ask_preserved(self):
        t = _make_ticker(bid=150.0, ask=150.5, last=150.2)
        df = normalize_ticker(t)
        assert df["bid"].iloc[0] == 150.0
        assert df["ask"].iloc[0] == 150.5

    def test_volume_and_vwap_preserved(self):
        t = _make_ticker(last=100.0, volume=5_000_000, vwap=99.8)
        df = normalize_ticker(t)
        assert df["volume"].iloc[0] == 5_000_000
        assert df["vwap"].iloc[0] == 99.8

    def test_last_size_preserved(self):
        t = _make_ticker(last=100.0, lastSize=42)
        df = normalize_ticker(t)
        assert df["last_size"].iloc[0] == 42.0

    def test_bar_count_always_nan(self):
        t = _make_ticker(last=100.0)
        df = normalize_ticker(t)
        assert np.isnan(df["bar_count"].iloc[0])

    def test_ib_sentinel_bid_ask_become_nan(self):
        t = _make_ticker(bid=sys.float_info.max, ask=sys.float_info.max,
                         last=300.0)
        df = normalize_ticker(t)
        assert np.isnan(df["bid"].iloc[0])
        assert np.isnan(df["ask"].iloc[0])
        # last is valid, so OHLC should use it
        assert df["close"].iloc[0] == 300.0

    def test_inf_volume_becomes_nan(self):
        t = _make_ticker(last=100.0, volume=float("inf"))
        df = normalize_ticker(t)
        assert np.isnan(df["volume"].iloc[0])

    def test_none_vwap_becomes_nan(self):
        t = _make_ticker(last=100.0, vwap=None)
        df = normalize_ticker(t)
        assert np.isnan(df["vwap"].iloc[0])

    def test_naive_time_gets_utc(self):
        t = _make_ticker(last=100.0)
        t.time = dt.datetime(2026, 1, 1, 12, 0, 0)  # naive
        df = normalize_ticker(t)
        assert df.index[0].tzinfo is not None

    def test_none_time_defaults_to_now(self):
        t = _make_ticker(last=100.0)
        t.time = None
        df = normalize_ticker(t)
        assert df.index[0].tzinfo is not None
        # Should be close to now
        delta = abs((dt.datetime.now(dt.timezone.utc) - df.index[0]).total_seconds())
        assert delta < 5

    def test_tz_aware_time_preserved(self):
        eastern = dt.timezone(dt.timedelta(hours=-5))
        ts = dt.datetime(2026, 3, 10, 10, 0, 0, tzinfo=eastern)
        t = _make_ticker(last=100.0)
        t.time = ts
        df = normalize_ticker(t)
        assert df.index[0] == ts

    def test_accumulation_preserves_schema(self):
        t1 = _make_ticker(bid=100.0, ask=100.1, last=100.05)
        t1.time = dt.datetime(2026, 3, 10, 14, 30, 0, tzinfo=dt.timezone.utc)
        t2 = _make_ticker(bid=101.0, ask=101.2, last=101.1)
        t2.time = dt.datetime(2026, 3, 10, 14, 31, 0, tzinfo=dt.timezone.utc)

        combined = pd.concat([normalize_ticker(t1), normalize_ticker(t2)])
        assert list(combined.columns) == NORMALIZED_COLUMNS
        assert len(combined) == 2

    def test_accumulation_many_rows(self):
        base = dt.datetime(2026, 3, 10, 14, 0, 0, tzinfo=dt.timezone.utc)
        frames = []
        for i in range(100):
            t = _make_ticker(last=100.0 + i * 0.1)
            t.time = base + dt.timedelta(seconds=i)
            frames.append(normalize_ticker(t))
        combined = pd.concat(frames)
        assert len(combined) == 100
        assert list(combined.columns) == NORMALIZED_COLUMNS


# ===================================================================
# normalize_historical
# ===================================================================

class TestNormalizeHistorical:
    def test_columns_match_schema(self):
        df = _make_historical_df()
        result = normalize_historical(df)
        assert list(result.columns) == NORMALIZED_COLUMNS

    def test_row_count_preserved(self):
        df = _make_historical_df(n=50)
        result = normalize_historical(df)
        assert len(result) == 50

    def test_average_renamed_to_vwap(self):
        df = _make_historical_df()
        result = normalize_historical(df)
        assert "average" not in result.columns
        assert "vwap" in result.columns
        # Values should match the original average
        assert result["vwap"].iloc[0] == pytest.approx(df["average"].iloc[0])

    def test_bar_size_dropped(self):
        df = _make_historical_df()
        assert "bar_size" in df.columns
        result = normalize_historical(df)
        assert "bar_size" not in result.columns

    def test_what_to_show_dropped(self):
        df = _make_historical_df()
        assert "what_to_show" in df.columns
        result = normalize_historical(df)
        assert "what_to_show" not in result.columns

    def test_bid_ask_are_nan(self):
        df = _make_historical_df()
        result = normalize_historical(df)
        assert result["bid"].isna().all()
        assert result["ask"].isna().all()

    def test_last_equals_close(self):
        df = _make_historical_df()
        result = normalize_historical(df)
        pd.testing.assert_series_equal(
            result["last"], result["close"], check_names=False
        )

    def test_last_size_is_nan(self):
        df = _make_historical_df()
        result = normalize_historical(df)
        assert result["last_size"].isna().all()

    def test_ohlcv_values_unchanged(self):
        df = _make_historical_df()
        result = normalize_historical(df)
        for col in ("open", "high", "low", "close", "volume"):
            pd.testing.assert_series_equal(
                result[col], df[col], check_names=False
            )

    def test_index_name_is_date(self):
        df = _make_historical_df()
        result = normalize_historical(df)
        assert result.index.name == "date"

    def test_index_name_fixed_if_wrong(self):
        df = _make_historical_df()
        df.index.name = "timestamp"
        result = normalize_historical(df)
        assert result.index.name == "date"

    def test_empty_dataframe_passthrough(self):
        empty = pd.DataFrame()
        result = normalize_historical(empty)
        assert result.empty

    def test_no_average_column(self):
        df = _make_historical_df(include_average=False)
        result = normalize_historical(df)
        assert "vwap" in result.columns
        assert result["vwap"].isna().all()

    def test_no_bar_size_column(self):
        df = _make_historical_df(include_bar_size=False)
        result = normalize_historical(df)
        assert list(result.columns) == NORMALIZED_COLUMNS

    def test_no_what_to_show_column(self):
        df = _make_historical_df(include_what_to_show=False)
        result = normalize_historical(df)
        assert list(result.columns) == NORMALIZED_COLUMNS

    def test_minimal_df_only_ohlcv(self):
        """A DataFrame with only open/high/low/close/volume (no average, bar_size, etc.)."""
        df = _make_historical_df(include_average=False, include_bar_size=False,
                                 include_what_to_show=False)
        result = normalize_historical(df)
        assert list(result.columns) == NORMALIZED_COLUMNS
        # close should be copied to last
        pd.testing.assert_series_equal(
            result["last"], result["close"], check_names=False
        )

    def test_does_not_mutate_input(self):
        df = _make_historical_df()
        original_columns = list(df.columns)
        normalize_historical(df)
        assert list(df.columns) == original_columns
        assert "average" in df.columns

    def test_bar_count_preserved_when_present(self):
        df = _make_historical_df()
        df["bar_count"] = 999
        result = normalize_historical(df)
        assert (result["bar_count"] == 999).all()

    def test_bar_count_nan_when_absent(self):
        df = _make_historical_df()
        assert "bar_count" not in df.columns
        result = normalize_historical(df)
        assert result["bar_count"].isna().all()

    def test_timezone_preserved(self):
        df = _make_historical_df(tz="US/Eastern")
        result = normalize_historical(df)
        assert str(result.index.tz) == "US/Eastern"


# ===================================================================
# Backtester integration: normalized data flows through correctly
# ===================================================================

class TestBacktesterNormalization:
    """Test that the backtester feeds normalized DataFrames to strategies."""

    def _write_bars(self, duckdb_path, conid=4391, n=60):
        store = DuckDBDataStore(duckdb_path)
        rng = np.random.default_rng(42)
        dates = pd.date_range("2024-01-02 09:30", periods=n, freq="1min", tz="UTC")
        close = 100.0 + np.cumsum(rng.normal(0.05, 0.2, n))
        df = pd.DataFrame({
            "open": close - 0.1,
            "high": close + 0.5,
            "low": close - 0.5,
            "close": close,
            "volume": rng.integers(500, 5000, n).astype(float),
            "average": close - 0.02,
            "bar_count": 100,
            "bar_size": "1 min",
            "what_to_show": 1,
        }, index=dates)
        df.index.name = "date"
        store.write(str(conid), df)
        return df

    def test_strategy_receives_normalized_columns(self, tmp_duckdb_path):
        """The strategy's on_prices() must receive exactly NORMALIZED_COLUMNS."""
        from trader.data.data_access import TickStorage
        from trader.data.universe import UniverseAccessor
        from trader.objects import BarSize
        from trader.simulation.backtester import Backtester, BacktestConfig
        from trader.trading.strategy import Signal, Strategy, StrategyContext, StrategyState
        import logging

        self._write_bars(tmp_duckdb_path)

        received_columns = []

        class ColumnCapture(Strategy):
            def on_prices(self, prices):
                received_columns.append(list(prices.columns))
                return None

        storage = TickStorage(duckdb_path=tmp_duckdb_path)
        config = BacktestConfig(
            start_date=dt.datetime(2024, 1, 2, 9, 30, tzinfo=dt.timezone.utc),
            end_date=dt.datetime(2024, 1, 2, 10, 30, tzinfo=dt.timezone.utc),
        )
        bt = Backtester(storage=storage, config=config)

        strategy = ColumnCapture()
        ua = UniverseAccessor.__new__(UniverseAccessor)
        ua.duckdb_path = tmp_duckdb_path
        ua.universe_library = "Universes"
        ctx = StrategyContext(
            name="column_capture", bar_size=BarSize.Mins1, conids=[4391],
            universe=None, historical_days_prior=0, paper_only=False,
            storage=storage, universe_accessor=ua,
            logger=logging.getLogger("test"),
        )
        strategy.install(ctx)
        strategy.state = StrategyState.RUNNING

        bt.run(strategy, [4391])

        assert len(received_columns) > 0, "Strategy was never called"
        for cols in received_columns:
            assert cols == NORMALIZED_COLUMNS, f"Got columns: {cols}"

    def test_strategy_can_access_last_column(self, tmp_duckdb_path):
        """A strategy that reads prices['last'] should work in backtest."""
        from trader.data.data_access import TickStorage
        from trader.data.universe import UniverseAccessor
        from trader.objects import Action, BarSize
        from trader.simulation.backtester import Backtester, BacktestConfig
        from trader.trading.strategy import Signal, Strategy, StrategyContext, StrategyState
        import logging

        self._write_bars(tmp_duckdb_path)

        class LastPriceStrategy(Strategy):
            def on_prices(self, prices):
                if len(prices) < 5:
                    return None
                last_price = prices["last"].iloc[-1]
                if last_price > 101:
                    return Signal(
                        source_name=self.name or "test",
                        action=Action.BUY,
                        probability=0.8, risk=0.2,
                        quantity=10,
                    )
                return None

        storage = TickStorage(duckdb_path=tmp_duckdb_path)
        config = BacktestConfig(
            start_date=dt.datetime(2024, 1, 2, 9, 30, tzinfo=dt.timezone.utc),
            end_date=dt.datetime(2024, 1, 2, 10, 30, tzinfo=dt.timezone.utc),
        )
        bt = Backtester(storage=storage, config=config)

        strategy = LastPriceStrategy()
        ua = UniverseAccessor.__new__(UniverseAccessor)
        ua.duckdb_path = tmp_duckdb_path
        ua.universe_library = "Universes"
        ctx = StrategyContext(
            name="last_price", bar_size=BarSize.Mins1, conids=[4391],
            universe=None, historical_days_prior=0, paper_only=False,
            storage=storage, universe_accessor=ua,
            logger=logging.getLogger("test"),
        )
        strategy.install(ctx)
        strategy.state = StrategyState.RUNNING

        result = bt.run(strategy, [4391])
        # Should have at least one trade (prices trend upward past 101)
        assert result.total_trades > 0

    def test_strategy_can_access_vwap_column(self, tmp_duckdb_path):
        """A strategy that reads prices['vwap'] should work in backtest."""
        from trader.data.data_access import TickStorage
        from trader.data.universe import UniverseAccessor
        from trader.objects import BarSize
        from trader.simulation.backtester import Backtester, BacktestConfig
        from trader.trading.strategy import Signal, Strategy, StrategyContext, StrategyState
        import logging

        self._write_bars(tmp_duckdb_path)

        vwap_values = []

        class VwapReader(Strategy):
            def on_prices(self, prices):
                vwap_values.append(prices["vwap"].iloc[-1])
                return None

        storage = TickStorage(duckdb_path=tmp_duckdb_path)
        config = BacktestConfig(
            start_date=dt.datetime(2024, 1, 2, 9, 30, tzinfo=dt.timezone.utc),
            end_date=dt.datetime(2024, 1, 2, 10, 30, tzinfo=dt.timezone.utc),
        )
        bt = Backtester(storage=storage, config=config)

        strategy = VwapReader()
        ua = UniverseAccessor.__new__(UniverseAccessor)
        ua.duckdb_path = tmp_duckdb_path
        ua.universe_library = "Universes"
        ctx = StrategyContext(
            name="vwap_reader", bar_size=BarSize.Mins1, conids=[4391],
            universe=None, historical_days_prior=0, paper_only=False,
            storage=storage, universe_accessor=ua,
            logger=logging.getLogger("test"),
        )
        strategy.install(ctx)
        strategy.state = StrategyState.RUNNING

        bt.run(strategy, [4391])

        assert len(vwap_values) > 0
        # vwap should have actual values (not all NaN)
        assert not all(np.isnan(v) for v in vwap_values)

    def test_strategy_bid_ask_nan_in_backtest(self, tmp_duckdb_path):
        """bid and ask should be NaN in backtest mode."""
        from trader.data.data_access import TickStorage
        from trader.data.universe import UniverseAccessor
        from trader.objects import BarSize
        from trader.simulation.backtester import Backtester, BacktestConfig
        from trader.trading.strategy import Signal, Strategy, StrategyContext, StrategyState
        import logging

        self._write_bars(tmp_duckdb_path)

        bid_ask_checks = []

        class BidAskChecker(Strategy):
            def on_prices(self, prices):
                bid_ask_checks.append({
                    "bid_nan": prices["bid"].isna().all(),
                    "ask_nan": prices["ask"].isna().all(),
                    "last_size_nan": prices["last_size"].isna().all(),
                })
                return None

        storage = TickStorage(duckdb_path=tmp_duckdb_path)
        config = BacktestConfig(
            start_date=dt.datetime(2024, 1, 2, 9, 30, tzinfo=dt.timezone.utc),
            end_date=dt.datetime(2024, 1, 2, 10, 30, tzinfo=dt.timezone.utc),
        )
        bt = Backtester(storage=storage, config=config)

        strategy = BidAskChecker()
        ua = UniverseAccessor.__new__(UniverseAccessor)
        ua.duckdb_path = tmp_duckdb_path
        ua.universe_library = "Universes"
        ctx = StrategyContext(
            name="bid_ask_checker", bar_size=BarSize.Mins1, conids=[4391],
            universe=None, historical_days_prior=0, paper_only=False,
            storage=storage, universe_accessor=ua,
            logger=logging.getLogger("test"),
        )
        strategy.install(ctx)
        strategy.state = StrategyState.RUNNING

        bt.run(strategy, [4391])

        assert len(bid_ask_checks) > 0
        for check in bid_ask_checks:
            assert check["bid_nan"], "bid should be NaN in backtest"
            assert check["ask_nan"], "ask should be NaN in backtest"
            assert check["last_size_nan"], "last_size should be NaN in backtest"


# ===================================================================
# DuckDB roundtrip: write → read → normalize_historical
# ===================================================================

class TestDuckDBNormalizationRoundtrip:
    def test_write_read_normalize(self, tmp_duckdb_path):
        """Data written to DuckDB and read back normalizes correctly."""
        store = DuckDBDataStore(tmp_duckdb_path)
        rng = np.random.default_rng(7)
        n = 30
        dates = pd.date_range("2024-06-01 09:30", periods=n, freq="1min", tz="UTC")
        close = 150.0 + np.cumsum(rng.normal(0.0, 0.3, n))
        df = pd.DataFrame({
            "open": close - 0.05,
            "high": close + 0.3,
            "low": close - 0.3,
            "close": close,
            "volume": rng.integers(100, 3000, n).astype(float),
            "average": close + 0.01,
            "bar_count": 50,
        }, index=dates)
        df.index.name = "date"
        store.write("12345", df)

        raw = store.read("12345")
        assert "average" in raw.columns

        result = normalize_historical(raw)
        assert list(result.columns) == NORMALIZED_COLUMNS
        assert "average" not in result.columns
        assert result["vwap"].iloc[0] == pytest.approx(raw["average"].iloc[0])
        pd.testing.assert_series_equal(
            result["last"], result["close"], check_names=False
        )

    def test_multiple_symbols_normalize_independently(self, tmp_duckdb_path):
        store = DuckDBDataStore(tmp_duckdb_path)
        rng = np.random.default_rng(99)
        dates = pd.date_range("2024-06-01 09:30", periods=10, freq="1min", tz="UTC")

        for sym, base in [("1111", 50.0), ("2222", 200.0)]:
            close = base + np.cumsum(rng.normal(0, 0.1, 10))
            df = pd.DataFrame({
                "open": close, "high": close + 1, "low": close - 1,
                "close": close, "volume": 1000.0, "average": close,
            }, index=dates)
            df.index.name = "date"
            store.write(sym, df)

        for sym in ["1111", "2222"]:
            raw = store.read(sym)
            result = normalize_historical(raw)
            assert list(result.columns) == NORMALIZED_COLUMNS


# ===================================================================
# SMI crossover fix: prices['close'] instead of prices.ask
# ===================================================================

class TestSMICrossoverFix:
    """Verify the SMI crossover strategy uses prices['close'] correctly."""

    def test_smi_on_prices_uses_close(self, tmp_duckdb_path):
        """SMICrossOver.on_prices() should not crash on normalized backtest data."""
        from strategies.smi_crossover import SMICrossOver
        from trader.data.data_access import TickStorage
        from trader.data.universe import UniverseAccessor
        from trader.objects import BarSize
        from trader.trading.strategy import StrategyContext, StrategyState
        import logging

        # Build a 100-bar normalized DataFrame
        df = _make_historical_df(n=100)
        normalized = normalize_historical(df)

        strategy = SMICrossOver()
        storage = TickStorage(duckdb_path=tmp_duckdb_path)
        ua = UniverseAccessor.__new__(UniverseAccessor)
        ua.duckdb_path = tmp_duckdb_path
        ua.universe_library = "Universes"
        ctx = StrategyContext(
            name="smi_crossover", bar_size=BarSize.Mins1, conids=[4391],
            universe=None, historical_days_prior=5, paper_only=False,
            storage=storage, universe_accessor=ua,
            logger=logging.getLogger("test"),
        )
        strategy.install(ctx)
        strategy.state = StrategyState.RUNNING

        # This used to crash with KeyError: 'ask'
        result = strategy.on_prices(normalized)
        # Result is either None or a Signal — no crash is the key assertion
        assert result is None or hasattr(result, "action")

    def test_smi_on_prices_with_ask_column_present(self, tmp_duckdb_path):
        """SMICrossOver should still work if ask column has data (live-like)."""
        from strategies.smi_crossover import SMICrossOver
        from trader.data.data_access import TickStorage
        from trader.data.universe import UniverseAccessor
        from trader.objects import BarSize
        from trader.trading.strategy import StrategyContext, StrategyState
        import logging

        df = _make_historical_df(n=100)
        normalized = normalize_historical(df)
        # Simulate live-like data where ask has values
        normalized["ask"] = normalized["close"] + 0.05

        strategy = SMICrossOver()
        storage = TickStorage(duckdb_path=tmp_duckdb_path)
        ua = UniverseAccessor.__new__(UniverseAccessor)
        ua.duckdb_path = tmp_duckdb_path
        ua.universe_library = "Universes"
        ctx = StrategyContext(
            name="smi_crossover", bar_size=BarSize.Mins1, conids=[4391],
            universe=None, historical_days_prior=5, paper_only=False,
            storage=storage, universe_accessor=ua,
            logger=logging.getLogger("test"),
        )
        strategy.install(ctx)
        strategy.state = StrategyState.RUNNING

        # Should work fine — uses close, not ask
        result = strategy.on_prices(normalized)
        assert result is None or hasattr(result, "action")

    def test_smi_too_few_bars_returns_none(self, tmp_duckdb_path):
        """SMICrossOver returns None when insufficient data (< 50 bars)."""
        from strategies.smi_crossover import SMICrossOver
        from trader.data.data_access import TickStorage
        from trader.data.universe import UniverseAccessor
        from trader.objects import BarSize
        from trader.trading.strategy import StrategyContext, StrategyState
        import logging

        df = _make_historical_df(n=20)
        normalized = normalize_historical(df)

        strategy = SMICrossOver()
        storage = TickStorage(duckdb_path=tmp_duckdb_path)
        ua = UniverseAccessor.__new__(UniverseAccessor)
        ua.duckdb_path = tmp_duckdb_path
        ua.universe_library = "Universes"
        ctx = StrategyContext(
            name="smi_crossover", bar_size=BarSize.Mins1, conids=[4391],
            universe=None, historical_days_prior=5, paper_only=False,
            storage=storage, universe_accessor=ua,
            logger=logging.getLogger("test"),
        )
        strategy.install(ctx)
        strategy.state = StrategyState.RUNNING

        result = strategy.on_prices(normalized)
        assert result is None


# ===================================================================
# Edge cases and cross-source consistency
# ===================================================================

class TestCrossSourceConsistency:
    """Verify that normalize_ticker and normalize_historical produce
    DataFrames with identical schemas so strategies can run on both."""

    def test_same_columns(self):
        ticker_df = normalize_ticker(
            _make_ticker(bid=100.0, ask=100.1, last=100.05)
        )
        hist_df = normalize_historical(_make_historical_df(n=5))
        assert list(ticker_df.columns) == list(hist_df.columns)

    def test_concat_mixed_sources(self):
        """A DataFrame built by concatenating ticker rows and historical rows
        should have a consistent schema."""
        ticker_rows = normalize_ticker(
            _make_ticker(bid=100.0, ask=100.1, last=100.05)
        )
        hist_rows = normalize_historical(_make_historical_df(n=5))
        combined = pd.concat([hist_rows, ticker_rows])
        assert list(combined.columns) == NORMALIZED_COLUMNS
        assert len(combined) == 6

    def test_dtypes_compatible(self):
        """All columns should be float in both outputs."""
        ticker_df = normalize_ticker(
            _make_ticker(bid=100.0, ask=100.1, last=100.05, lastSize=50,
                         volume=1000, vwap=100.0)
        )
        hist_df = normalize_historical(_make_historical_df(n=5))
        for col in NORMALIZED_COLUMNS:
            assert pd.api.types.is_float_dtype(ticker_df[col]) or \
                   pd.api.types.is_numeric_dtype(ticker_df[col]), \
                f"ticker {col}: {ticker_df[col].dtype}"
            assert pd.api.types.is_float_dtype(hist_df[col]) or \
                   pd.api.types.is_numeric_dtype(hist_df[col]), \
                f"hist {col}: {hist_df[col].dtype}"

    def test_index_name_consistent(self):
        ticker_df = normalize_ticker(_make_ticker(last=100.0))
        hist_df = normalize_historical(_make_historical_df(n=5))
        assert ticker_df.index.name == hist_df.index.name == "date"
