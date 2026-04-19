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


class TestConcurrentAccess:
    """Verify the lock in DuckDBConnection serializes same-process writers.

    Same-DB writes from multiple threads must not produce torn rows or
    duplicates. (Cross-process concurrency is enforced by DuckDB's own file
    lock.)
    """

    def test_concurrent_writes_no_duplicates(self, tmp_duckdb_path):
        import threading

        store = DuckDBDataStore(tmp_duckdb_path)
        # Each thread writes non-overlapping date ranges for distinct symbols
        # so we can verify row counts deterministically.
        def _write(sym: str, start_offset: int):
            dates = pd.date_range(
                f"2024-01-0{start_offset} 09:30", periods=10, freq="1min", tz="UTC",
            )
            df = pd.DataFrame({
                "open": [100.0] * 10,
                "high": [101.0] * 10,
                "low": [99.0] * 10,
                "close": [100.5] * 10,
                "volume": [1000.0] * 10,
            }, index=dates)
            df.index.name = "date"
            store.write(sym, df)

        threads = [
            threading.Thread(target=_write, args=(f'SYM{i}', i + 2))
            for i in range(5)
        ]
        for t in threads: t.start()
        for t in threads: t.join()

        # All 5 symbols should be present, each with 10 rows
        for i in range(5):
            df = store.read(f'SYM{i}')
            assert len(df) == 10, f'SYM{i} should have 10 rows'

    def test_tickstorage_list_libraries_finds_written_data(self, tmp_duckdb_path):
        """Regression: TickStorage.list_libraries() used to call
        store._db.execute(query).fetchall() — which broke silently after
        execute() was refactored to be atomic (returns rows, not a
        connection). Empty list_libraries() ⇒ data summary says "No data"
        even with gigabytes of rows on disk. Lock it in."""
        from trader.data.data_access import TickStorage
        from trader.objects import BarSize

        store = DuckDBDataStore(tmp_duckdb_path)
        # Write a row with an explicit bar_size column (matches what
        # `data download` writes after normalize_historical).
        dates = pd.date_range("2024-01-02 09:30", periods=3, freq="1min", tz="UTC")
        df = pd.DataFrame({
            "open": [100.0] * 3, "high": [101.0] * 3, "low": [99.0] * 3,
            "close": [100.5] * 3, "volume": [1000.0] * 3,
            "bar_size": ["1 min"] * 3,
        }, index=dates)
        df.index.name = "date"
        store.write("AAPL", df)

        storage = TickStorage(duckdb_path=tmp_duckdb_path)
        libs = storage.list_libraries()
        assert "1 min" in libs, (
            f"list_libraries returned {libs!r} — TickStorage lost the "
            "bar_size discovery pathway, which breaks `mmr data summary`"
        )

    def test_concurrent_writes_same_symbol_same_range_idempotent(self, tmp_duckdb_path):
        """Two threads writing the SAME symbol + same date range should not
        double-up rows (DELETE+INSERT is atomic under execute_atomic)."""
        import threading

        store = DuckDBDataStore(tmp_duckdb_path)
        dates = pd.date_range("2024-01-02 09:30", periods=8, freq="1min", tz="UTC")
        df = pd.DataFrame({
            "open": [100.0] * 8,
            "high": [101.0] * 8,
            "low": [99.0] * 8,
            "close": [100.5] * 8,
            "volume": [1000.0] * 8,
        }, index=dates)
        df.index.name = "date"

        errors = []

        def _write():
            try:
                store.write('AAPL', df)
            except Exception as ex:  # noqa: BLE001 — test expects none
                errors.append(ex)

        threads = [threading.Thread(target=_write) for _ in range(4)]
        for t in threads: t.start()
        for t in threads: t.join()

        assert errors == []
        # Only 8 rows regardless of how many times we wrote
        assert len(store.read('AAPL')) == 8
