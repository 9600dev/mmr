import datetime as dt
import pytest
import pandas as pd
import numpy as np

from unittest.mock import MagicMock, patch
from trader.data.duckdb_store import (
    SHADOWED_DB_MARKER,
    DuckDBConnection,
    DuckDBDataStore,
    DuckDBObjectStore,
    ShadowedDatabaseError,
)
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

    def test_write_does_not_clobber_other_bar_sizes(self, data_store):
        # Regression: write()'s DELETE used to filter only by symbol+date
        # range, so writing a wide 1-min window for AAPL silently wiped
        # any daily rows for AAPL in that range. The fix adds bar_size
        # to the DELETE predicate. This test would have failed before.
        daily = _sample_df(n=5, start="2024-01-02 09:30").copy()
        daily["bar_size"] = "1 day"
        data_store.write("AAPL", daily)

        # Now write 1-min data covering the SAME date range for the same
        # symbol. Pre-fix this DELETE'd all daily rows above.
        minute = _sample_df(n=60, start="2024-01-02 09:30").copy()
        minute["bar_size"] = "1 min"
        data_store.write("AAPL", minute)

        daily_after = data_store.read("AAPL", bar_size="1 day")
        minute_after = data_store.read("AAPL", bar_size="1 min")
        assert len(daily_after) == 5, "daily rows should survive a 1-min write"
        assert len(minute_after) == 60


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


class TestShadowedHostDatabase:
    """Host-vs-container DB shadowing must fail loudly, not return empty.

    docker-compose overlays ~/.local/share/mmr/data with the mmr_db_data volume,
    so trader.yaml's single duckdb_path resolves to different files on the host
    and in the container. DuckDB creates the host one on open, so host-side
    reads silently returned zero rows (`mmr backtests` -> {"data": []}) and
    `mmr status` reported pending_proposals from the wrong database.
    """

    def test_opens_normally_without_the_marker(self, tmp_path):
        conn = DuckDBConnection(str(tmp_path / 'ok.duckdb'))
        assert conn.db_path.endswith('ok.duckdb')

    def test_refuses_to_open_beside_the_marker(self, tmp_path):
        (tmp_path / SHADOWED_DB_MARKER).write_text('shadowed')
        with pytest.raises(ShadowedDatabaseError):
            DuckDBConnection(str(tmp_path / 'stub.duckdb'))

    def test_refusal_does_not_create_a_stub_file(self, tmp_path):
        """The whole failure mode is DuckDB creating an empty file on open."""
        (tmp_path / SHADOWED_DB_MARKER).write_text('shadowed')
        db = tmp_path / 'stub.duckdb'
        with pytest.raises(ShadowedDatabaseError):
            DuckDBConnection(str(db))
        assert not db.exists()

    def test_error_message_says_how_to_fix_it(self, tmp_path):
        (tmp_path / SHADOWED_DB_MARKER).write_text('shadowed')
        with pytest.raises(ShadowedDatabaseError) as exc:
            DuckDBConnection(str(tmp_path / 'stub.duckdb'))
        msg = str(exc.value)
        assert 'docker.sh -e' in msg
        assert 'MMR_ALLOW_HOST_DB=1' in msg

    def test_env_override_allows_deliberate_host_use(self, tmp_path, monkeypatch, capsys):
        (tmp_path / SHADOWED_DB_MARKER).write_text('shadowed')
        monkeypatch.setenv('MMR_ALLOW_HOST_DB', '1')
        conn = DuckDBConnection(str(tmp_path / 'deliberate.duckdb'))
        assert conn.db_path.endswith('deliberate.duckdb')
        assert 'MMR_ALLOW_HOST_DB=1' in capsys.readouterr().err

    def test_override_must_be_exactly_1(self, tmp_path, monkeypatch):
        """Fail closed on a fat-fingered value rather than honouring 'true'."""
        (tmp_path / SHADOWED_DB_MARKER).write_text('shadowed')
        monkeypatch.setenv('MMR_ALLOW_HOST_DB', 'true')
        with pytest.raises(ShadowedDatabaseError):
            DuckDBConnection(str(tmp_path / 'stub.duckdb'))

    def test_marker_in_a_different_directory_is_ignored(self, tmp_path):
        """Only the DB's own directory counts — no walking up the tree."""
        (tmp_path / SHADOWED_DB_MARKER).write_text('shadowed')
        sub = tmp_path / 'elsewhere'
        sub.mkdir()
        conn = DuckDBConnection(str(sub / 'fine.duckdb'))
        assert conn.db_path.endswith('fine.duckdb')


class TestWritesRefuseImpossibleBars:
    """The write path is the lowest chokepoint every bar passes through, so it
    is where a bar that cannot have happened gets stopped.

    Before 2026-07-28 nothing validated data on the way in, and 2,580 stored
    1-min bars had a "high" between their own open and close with bar_count=0:
    synthesised from bid/ask midpoints rather than trades. ORB's entire signal
    is the high and low of the opening range.

    Dropping rather than raising is deliberate. One bad row in a 20,000-row
    frame must not cost a day of data, and a frame-level refusal would make
    ingestion fail closed in a way that starves strategies of real bars.
    """

    def _tick_data(self, tmp_duckdb_path):
        from trader.data.data_access import TickData
        from trader.objects import BarSize
        return TickData(tmp_duckdb_path, BarSize.Mins1)

    def _frame(self, rows):
        import datetime as dt
        import pandas as pd
        idx = pd.DatetimeIndex(
            [dt.datetime(2026, 7, 28, 13, 30) + dt.timedelta(minutes=i)
             for i in range(len(rows))], name='date').tz_localize('UTC')
        return pd.DataFrame(rows, index=idx)

    def test_a_midpoint_high_is_refused(self, tmp_duckdb_path):
        """The exact shape found in the store."""
        td = self._tick_data(tmp_duckdb_path)
        df = self._frame([
            {'open': 76.81, 'high': 76.825, 'low': 76.80, 'close': 76.83,
             'volume': 5994.0},                                    # impossible
            {'open': 76.81, 'high': 76.90, 'low': 76.80, 'close': 76.83,
             'volume': 100.0},                                     # fine
        ])
        td.write(4391, df)
        stored = td.read(4391)
        assert len(stored) == 1, 'the impossible bar was persisted'
        assert float(stored['high'].iloc[0]) == 76.90

    def test_a_wholly_bad_frame_writes_nothing_rather_than_raising(self, tmp_duckdb_path):
        td = self._tick_data(tmp_duckdb_path)
        df = self._frame([
            {'open': 10.0, 'high': 9.0, 'low': 8.0, 'close': 9.5, 'volume': 1.0},
        ])
        td.write(4392, df)                    # must not raise
        assert len(td.read(4392)) == 0

    def test_good_bars_are_untouched(self, tmp_duckdb_path):
        td = self._tick_data(tmp_duckdb_path)
        df = self._frame([
            {'open': 10.0, 'high': 11.0, 'low': 9.0, 'close': 10.5, 'volume': 5.0},
            {'open': 10.5, 'high': 10.5, 'low': 10.5, 'close': 10.5, 'volume': 0.0},
        ])
        td.write(4393, df)
        assert len(td.read(4393)) == 2, 'a legitimate flat/zero-volume bar was dropped'

    def test_the_refusal_is_logged_not_silent(self, tmp_duckdb_path, caplog):
        """Silently dropping data is the failure mode this whole exercise
        exists to remove."""
        import logging as stdlib_logging
        td = self._tick_data(tmp_duckdb_path)
        df = self._frame([
            {'open': 76.81, 'high': 76.825, 'low': 76.80, 'close': 76.83,
             'volume': 5994.0},
        ])
        with caplog.at_level(stdlib_logging.WARNING):
            td.write(4394, df)
        assert any('REFUSED' in r.message for r in caplog.records)


class TestQuarantineKeepsTheEvidence:
    """Rejected bars are KEPT, not discarded.

    That a source SENT an impossible bar is evidence in its own right: it is
    how vendor quality gets measured over time, and how "130 impossible bars
    last month" becomes something you can substantiate rather than assert. The
    write guard originally dropped them, which destroyed exactly that record.

    A separate TABLE rather than a flag column, deliberately. A flag puts the
    burden on every reader to remember to filter, and a reader who forgets
    silently trains a strategy on fiction — which is the failure being removed,
    reintroduced one layer down. With a separate table, `tick_data` holds an
    invariant instead: everything in it passed validation.
    """

    def _tick_data(self, tmp_duckdb_path):
        from trader.data.data_access import TickData
        from trader.objects import BarSize
        return TickData(tmp_duckdb_path, BarSize.Mins1)

    def _frame(self, rows):
        import datetime as dt
        import pandas as pd
        idx = pd.DatetimeIndex(
            [dt.datetime(2026, 7, 28, 13, 30) + dt.timedelta(minutes=i)
             for i in range(len(rows))], name='date').tz_localize('UTC')
        return pd.DataFrame(rows, index=idx)

    def _quarantined(self, tmp_duckdb_path):
        from trader.data.duckdb_store import DuckDBConnection
        db = DuckDBConnection.get_instance(tmp_duckdb_path)
        return db.execute('SELECT symbol, high, close, reason '
                          'FROM tick_data_quarantine', fetch='all') or []

    def test_a_rejected_bar_is_recorded_not_lost(self, tmp_duckdb_path):
        td = self._tick_data(tmp_duckdb_path)
        td.write(4391, self._frame([
            {'open': 76.81, 'high': 76.825, 'low': 76.80, 'close': 76.83,
             'volume': 5994.0},
        ]))
        rows = self._quarantined(tmp_duckdb_path)
        assert len(rows) == 1, 'the evidence was destroyed'
        assert float(rows[0][1]) == 76.825
        assert 'impossible' in (rows[0][3] or '')

    def test_the_main_table_never_holds_it(self, tmp_duckdb_path):
        """The invariant that makes the separation worth having."""
        td = self._tick_data(tmp_duckdb_path)
        td.write(4391, self._frame([
            {'open': 76.81, 'high': 76.825, 'low': 76.80, 'close': 76.83,
             'volume': 5994.0},
        ]))
        assert len(td.read(4391)) == 0

    def test_good_bars_are_not_quarantined(self, tmp_duckdb_path):
        td = self._tick_data(tmp_duckdb_path)
        td.write(4392, self._frame([
            {'open': 10.0, 'high': 11.0, 'low': 9.0, 'close': 10.5, 'volume': 5.0},
        ]))
        assert self._quarantined(tmp_duckdb_path) == []
        assert len(td.read(4392)) == 1

    def test_a_quarantine_failure_does_not_lose_the_good_bars(self, tmp_duckdb_path):
        """Recording the evidence is secondary to not corrupting the store. If
        quarantine itself fails, the good bars must still land and the bad ones
        must still be refused."""
        from unittest.mock import patch
        td = self._tick_data(tmp_duckdb_path)
        df = self._frame([
            {'open': 76.81, 'high': 76.825, 'low': 76.80, 'close': 76.83,
             'volume': 5994.0},
            {'open': 10.0, 'high': 11.0, 'low': 9.0, 'close': 10.5, 'volume': 5.0},
        ])
        with patch.object(type(td.library), 'quarantine',
                          side_effect=RuntimeError('disk full')):
            td.write(4393, df)
        assert len(td.read(4393)) == 1


class TestSameInstantDuplicatesCannotBeWritten:
    """A single frame carrying two rows at the same instant must store one row.

    The upsert DELETE clears previously STORED rows in the frame's range — it
    cannot touch two rows that arrive together, so nothing downstream stopped
    a same-instant pair from landing as a duplicate key. Observed live as 14
    GLD/XLK 1-min pairs (2026-05/07, quarantined 2026-07-30): an all-NULL
    placeholder beside a real bar at the same minute. The tz representations
    differed upstream (one naive, one aware), so every pandas index dedup saw
    two distinct keys; the write path's utc=True normalization then collapsed
    them onto one instant at INSERT time. The store enforces the invariant at
    its own chokepoint rather than trusting every producer.
    """

    def _store(self, tmp_duckdb_path):
        from trader.data.duckdb_store import DuckDBDataStore
        return DuckDBDataStore(tmp_duckdb_path)

    def _row(self, when, close, bar_size='1 min'):
        import numpy as np
        empty = close is None
        return {
            'date': when,
            'open': np.nan if empty else close,
            'high': np.nan if empty else close + 0.5,
            'low': np.nan if empty else close - 0.5,
            'close': np.nan if empty else close,
            'volume': np.nan if empty else 100.0,
            'average': np.nan, 'bar_count': 0,
            'bar_size': bar_size, 'what_to_show': 1,
        }

    def _dup_keys(self, store):
        return store._db.execute(
            "SELECT symbol, bar_size, date, count(*) FROM tick_data "
            "GROUP BY 1, 2, 3 HAVING count(*) > 1", fetch='all') or []

    def test_the_live_shape_placeholder_beside_real_bar(self, tmp_duckdb_path):
        """The exact pair found in the store: naive-UTC placeholder, aware-ET
        real bar, same instant. One row must survive — the real one."""
        import datetime as dt
        import pandas as pd
        import pytz
        store = self._store(tmp_duckdb_path)
        aware = pytz.timezone('US/Eastern').localize(dt.datetime(2026, 5, 4, 4, 0))
        naive_same_instant = dt.datetime(2026, 5, 4, 8, 0)
        df = pd.DataFrame([
            self._row(naive_same_instant, close=None),   # placeholder
            self._row(aware, close=50.0),                # real bar
        ])
        store.write('51529211', df)
        assert self._dup_keys(store) == []
        rows = store._db.execute(
            "SELECT close FROM tick_data WHERE symbol = '51529211'",
            fetch='all')
        assert len(rows) == 1
        assert rows[0][0] == 50.0, 'the placeholder won over the real bar'

    def test_real_bar_wins_regardless_of_frame_order(self, tmp_duckdb_path):
        import datetime as dt
        import pandas as pd
        store = self._store(tmp_duckdb_path)
        when = dt.datetime(2026, 5, 4, 8, 0, tzinfo=dt.timezone.utc)
        df = pd.DataFrame([
            self._row(when, close=50.0),     # real first this time
            self._row(when, close=None),     # placeholder last
        ])
        store.write('4215230', df)
        rows = store._db.execute(
            "SELECT close FROM tick_data WHERE symbol = '4215230'",
            fetch='all')
        assert len(rows) == 1
        assert rows[0][0] == 50.0

    def test_same_instant_different_bar_size_is_not_a_duplicate(self, tmp_duckdb_path):
        """A 1-min bar and a daily bar can legitimately share a timestamp."""
        import datetime as dt
        import pandas as pd
        store = self._store(tmp_duckdb_path)
        when = dt.datetime(2026, 5, 4, 8, 0, tzinfo=dt.timezone.utc)
        df = pd.DataFrame([
            self._row(when, close=50.0, bar_size='1 min'),
            self._row(when, close=51.0, bar_size='1 day'),
        ])
        store.write('265598', df)
        rows = store._db.execute(
            "SELECT count(*) FROM tick_data WHERE symbol = '265598'",
            fetch='one')
        assert rows[0] == 2

    def test_the_drop_is_logged_not_silent(self, tmp_duckdb_path, caplog):
        import datetime as dt
        import logging as stdlib_logging
        import pandas as pd
        store = self._store(tmp_duckdb_path)
        when = dt.datetime(2026, 5, 4, 8, 0, tzinfo=dt.timezone.utc)
        df = pd.DataFrame([
            self._row(when, close=None),
            self._row(when, close=50.0),
        ])
        with caplog.at_level(stdlib_logging.WARNING):
            store.write('51529211', df)
        assert any('same-instant duplicate' in r.message for r in caplog.records)
