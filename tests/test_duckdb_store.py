import datetime as dt
import pytest
import pandas as pd
import numpy as np

from unittest.mock import MagicMock, patch
from trader.data.duckdb_store import DuckDBDataStore, DuckDBObjectStore
from trader.sdk import MMR


@pytest.fixture
def data_store(tmp_duckdb_path):
    return DuckDBDataStore(tmp_duckdb_path)


@pytest.fixture
def object_store(tmp_duckdb_path):
    return DuckDBObjectStore(tmp_duckdb_path)


def _sample_df(n=10, start="2024-01-02 09:30"):
    dates = pd.date_range(start, periods=n, freq="1min", tz="UTC")
    rng = np.random.default_rng(42)
    return pd.DataFrame({
        "open": 100.0 + rng.normal(0, 1, n),
        "high": 101.0 + rng.normal(0, 1, n),
        "low": 99.0 + rng.normal(0, 1, n),
        "close": 100.0 + rng.normal(0, 1, n),
        "volume": rng.integers(100, 1000, n).astype(float),
    }, index=dates)


class TestDuckDBDataStore:
    def test_write_read_roundtrip(self, data_store):
        df = _sample_df()
        data_store.write("AMD", df)
        result = data_store.read("AMD")
        assert len(result) == 10
        assert "close" in result.columns
        assert "open" in result.columns

    def test_read_nonexistent_symbol(self, data_store):
        result = data_store.read("DOESNOTEXIST")
        assert result.empty

    def test_date_range_filter(self, data_store):
        df = _sample_df(n=60, start="2024-01-02 09:30")
        data_store.write("AAPL", df)
        start = dt.datetime(2024, 1, 2, 9, 40, tzinfo=dt.timezone.utc)
        end = dt.datetime(2024, 1, 2, 9, 50, tzinfo=dt.timezone.utc)
        result = data_store.read("AAPL", start=start, end=end)
        assert len(result) > 0
        assert len(result) <= 11  # 9:40 through 9:50 inclusive

    def test_list_symbols(self, data_store):
        data_store.write("AMD", _sample_df())
        data_store.write("NVDA", _sample_df())
        symbols = data_store.list_symbols()
        assert "AMD" in symbols
        assert "NVDA" in symbols


class TestDuckDBObjectStore:
    def test_write_read_roundtrip(self, object_store):
        data = {"key": "value", "nums": [1, 2, 3]}
        object_store.write("test_obj", data)
        result = object_store.read("test_obj")
        assert result == data

    def test_list_keys(self, object_store):
        object_store.write("alpha", {"a": 1})
        object_store.write("beta", {"b": 2})
        keys = object_store.list_symbols()
        assert "alpha" in keys
        assert "beta" in keys

    def test_delete(self, object_store):
        object_store.write("to_delete", "data")
        object_store.delete("to_delete")
        result = object_store.read("to_delete")
        assert result is None


def _sample_df_with_bar_size(n=10, start="2024-01-02 09:30", bar_size="1 day"):
    dates = pd.date_range(start, periods=n, freq="1min", tz="UTC")
    rng = np.random.default_rng(42)
    return pd.DataFrame({
        "open": 100.0 + rng.normal(0, 1, n),
        "high": 101.0 + rng.normal(0, 1, n),
        "low": 99.0 + rng.normal(0, 1, n),
        "close": 100.0 + rng.normal(0, 1, n),
        "volume": rng.integers(100, 1000, n).astype(float),
        "bar_size": bar_size,
    }, index=dates)


def _make_mmr_for_history(tmp_duckdb_path) -> MMR:
    """Create an MMR with a mock container pointing to the temp DuckDB."""
    mmr = MMR.__new__(MMR)
    mmr._client = None
    mmr._data_client = None
    mmr._massive_rest_client = None
    mmr._subscriptions = []
    mmr._position_map = {}
    mock_container = MagicMock()
    mock_container.config.return_value = {
        'duckdb_path': tmp_duckdb_path,
        'universe_library': 'Universes',
    }
    mmr._container = mock_container
    return mmr


class TestHistoryList:
    def test_empty_store(self, tmp_duckdb_path):
        # Ensure the table exists even if empty
        DuckDBDataStore(tmp_duckdb_path)
        mmr = _make_mmr_for_history(tmp_duckdb_path)
        df = mmr.history_list()
        assert df.empty
        assert list(df.columns) == ['symbol', 'name', 'bar_size', 'start', 'end', 'rows']

    def test_lists_symbols_and_bar_sizes(self, tmp_duckdb_path):
        store = DuckDBDataStore(tmp_duckdb_path)
        store.write("4391", _sample_df_with_bar_size(bar_size="1 day", start="2024-01-02 09:30"))
        store.write("4391", _sample_df_with_bar_size(bar_size="1 min", start="2024-02-01 09:30"))
        store.write("265598", _sample_df_with_bar_size(n=5, bar_size="1 day"))

        mmr = _make_mmr_for_history(tmp_duckdb_path)
        df = mmr.history_list()
        assert len(df) == 3
        assert set(df['symbol'].unique()) == {'4391', '265598'}

    def test_filter_by_bar_size(self, tmp_duckdb_path):
        store = DuckDBDataStore(tmp_duckdb_path)
        store.write("4391", _sample_df_with_bar_size(bar_size="1 day", start="2024-01-02 09:30"))
        store.write("4391", _sample_df_with_bar_size(bar_size="1 min", start="2024-02-01 09:30"))

        mmr = _make_mmr_for_history(tmp_duckdb_path)
        df = mmr.history_list(bar_size="1 day")
        assert len(df) == 1
        assert df.iloc[0]['bar_size'] == '1 day'

    def test_filter_by_symbol(self, tmp_duckdb_path):
        store = DuckDBDataStore(tmp_duckdb_path)
        store.write("4391", _sample_df_with_bar_size(bar_size="1 day"))
        store.write("265598", _sample_df_with_bar_size(bar_size="1 day"))

        mmr = _make_mmr_for_history(tmp_duckdb_path)
        df = mmr.history_list(symbol="4391")
        assert len(df) == 1
        assert df.iloc[0]['symbol'] == '4391'

    def test_resolves_conid_to_name(self, tmp_duckdb_path):
        from trader.data.data_access import SecurityDefinition
        from trader.data.universe import Universe, UniverseAccessor

        store = DuckDBDataStore(tmp_duckdb_path)
        store.write("4391", _sample_df_with_bar_size(bar_size="1 day"))

        # Write a universe with the SecurityDefinition
        obj_store = DuckDBObjectStore(tmp_duckdb_path)
        mock_def = SecurityDefinition(
            symbol='AMD', exchange='SMART', conId=4391, secType='STK',
            primaryExchange='NASDAQ', currency='USD', tradingClass='AMD',
            includeExpired=False, secIdType='', secId='', description='',
            minTick=0.01, orderTypes='', validExchanges='', priceMagnifier=1.0,
            longName='Advanced Micro Devices', category='', subcategory='',
            tradingHours='', timeZoneId='', liquidHours='', stockType='',
            minSize=1.0, sizeIncrement=1.0, suggestedSizeIncrement=1.0,
            bondType='', couponType='', callable=False, putable=False,
            coupon=0.0, convertable=False, maturity='', issueDate='',
            nextOptionDate='', nextOptionPartial=False, nextOptionType='',
            marketRuleIds='',
        )
        universe = Universe('test_universe', [mock_def])
        obj_store.write('test_universe', universe)

        mmr = _make_mmr_for_history(tmp_duckdb_path)
        df = mmr.history_list()
        assert len(df) == 1
        assert df.iloc[0]['name'] == 'AMD'

    def test_filter_by_ticker_name(self, tmp_duckdb_path):
        from trader.data.data_access import SecurityDefinition
        from trader.data.universe import Universe
        from trader.data.duckdb_store import DuckDBObjectStore

        store = DuckDBDataStore(tmp_duckdb_path)
        store.write("4391", _sample_df_with_bar_size(bar_size="1 day"))
        store.write("265598", _sample_df_with_bar_size(bar_size="1 day"))

        obj_store = DuckDBObjectStore(tmp_duckdb_path)
        amd_def = SecurityDefinition(
            symbol='AMD', exchange='SMART', conId=4391, secType='STK',
            primaryExchange='NASDAQ', currency='USD', tradingClass='AMD',
            includeExpired=False, secIdType='', secId='', description='',
            minTick=0.01, orderTypes='', validExchanges='', priceMagnifier=1.0,
            longName='Advanced Micro Devices', category='', subcategory='',
            tradingHours='', timeZoneId='', liquidHours='', stockType='',
            minSize=1.0, sizeIncrement=1.0, suggestedSizeIncrement=1.0,
            bondType='', couponType='', callable=False, putable=False,
            coupon=0.0, convertable=False, maturity='', issueDate='',
            nextOptionDate='', nextOptionPartial=False, nextOptionType='',
            marketRuleIds='',
        )
        universe = Universe('stocks', [amd_def])
        obj_store.write('stocks', universe)

        mmr = _make_mmr_for_history(tmp_duckdb_path)
        # Filter by ticker name (case insensitive)
        df = mmr.history_list(symbol="amd")
        assert len(df) == 1
        assert df.iloc[0]['name'] == 'AMD'

    def test_row_count(self, tmp_duckdb_path):
        store = DuckDBDataStore(tmp_duckdb_path)
        store.write("4391", _sample_df_with_bar_size(n=25, bar_size="1 day"))

        mmr = _make_mmr_for_history(tmp_duckdb_path)
        df = mmr.history_list()
        assert df.iloc[0]['rows'] == 25
