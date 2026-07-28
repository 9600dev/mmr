import datetime as dt
import dill
import duckdb
import os
import pandas as pd
import sys
import threading

from pathlib import Path
from typing import Any, Optional

from trader.data.store import DataStore, ObjectStore


# Marker file written by `docker.sh up()` into the HOST data directory. Inside
# the container that same directory IS the mmr_db_data named volume, so the
# marker is invisible there — which is precisely the discriminator we need:
# present  => this path is the host-side shadow, the live DB is in the container
# absent   => normal (in-container, or a non-Docker install where host IS live)
SHADOWED_DB_MARKER = '.db_in_container_volume'


class ShadowedDatabaseError(RuntimeError):
    """Opening a DuckDB that a container volume shadows.

    `trader.yaml` gives one path (`~/.local/share/mmr/data/mmr.duckdb`) which
    resolves to DIFFERENT files on the host and in the container, because
    docker-compose overlays `data/` with the mmr_db_data volume. Host-side reads
    therefore hit a near-empty stub and DuckDB happily creates it on open, so
    `mmr backtests` returned `{"data": []}` and `mmr status` reported a
    pending_proposals count from the wrong database — silent wrong answers,
    which this codebase treats as worse than no answer.
    """


_shadow_warned: set[str] = set()


def _assert_not_shadowed(db_path: str) -> None:
    marker = os.path.join(os.path.dirname(os.path.abspath(db_path)), SHADOWED_DB_MARKER)
    if not os.path.exists(marker):
        return
    if os.environ.get('MMR_ALLOW_HOST_DB') == '1':
        # Deliberate host-side use (e.g. backtesting against separately
        # downloaded data). Still say so once — loudly, on stderr, so it can't
        # be mistaken for the live DB.
        if db_path not in _shadow_warned:
            _shadow_warned.add(db_path)
            print(
                f'WARNING: MMR_ALLOW_HOST_DB=1 — using host-side {db_path}, which is '
                'NOT the live database the services read.',
                file=sys.stderr,
            )
        return
    raise ShadowedDatabaseError(
        f'{db_path} is not the live database.\n'
        f'This directory is shadowed by the Docker volume mmr_db_data, so the DuckDB '
        f'the services actually read lives INSIDE the container. Reading it here '
        f'returns empty results rather than an error, which is why this refuses.\n'
        f'  interactive:  ./docker.sh -e   then run mmr in the container\n'
        f'  one-shot:     docker exec -u trader -w /home/trader/mmr mmr-mmr-1 mmr <command>\n'
        f'  host DB anyway (separate data, not the live book): MMR_ALLOW_HOST_DB=1'
    )


class DuckDBConnection:
    """Manager for DuckDB database connections.

    DuckDB only allows one write connection per database file across all
    processes.  To support multiple services (trader_service, strategy_service)
    accessing the same database, we use short-lived connections: connect,
    execute, close.  DuckDB connect/disconnect is very fast (~1ms) so this
    is not a performance concern.
    """

    _instances: dict[str, 'DuckDBConnection'] = {}
    _class_lock = threading.Lock()

    def __init__(self, db_path: str):
        # Refuse the host-side shadow BEFORE creating directories or letting
        # DuckDB create a stub file on open — the earliest point at which a
        # wrong-database read can still be turned into a loud failure.
        _assert_not_shadowed(db_path)
        self.db_path = db_path
        self._lock = threading.Lock()
        # Ensure directory exists
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            Path(db_dir).mkdir(parents=True, exist_ok=True)

    @classmethod
    def get_instance(cls, db_path: str) -> 'DuckDBConnection':
        with cls._class_lock:
            if db_path not in cls._instances:
                cls._instances[db_path] = DuckDBConnection(db_path)
            return cls._instances[db_path]

    def execute(
        self,
        query: str,
        params: Optional[list] = None,
        fetch: str = 'none',
    ):
        """Execute a query atomically and return results (or None).

        fetch: 'none' returns None, 'all' returns list of tuples,
        'one' returns a single tuple (or None), 'df' returns a DataFrame.

        The connection is opened and closed inside the lock, so callers
        never hold a connection reference beyond this call. For multi-
        statement atomicity (register + insert + unregister), use
        execute_atomic().
        """
        def _run(conn):
            result = conn.execute(query, params) if params else conn.execute(query)
            if fetch == 'all':
                return result.fetchall()
            if fetch == 'one':
                return result.fetchone()
            if fetch == 'df':
                return result.fetchdf()
            return None

        return self.execute_atomic(_run)

    def execute_atomic(self, fn):
        """Execute a function with an exclusive connection.

        The function receives a DuckDBPyConnection and can perform
        multiple operations atomically.  The connection is closed
        after the function returns.

        **Multi-process contention**: DuckDB uses file-level locking — a
        second process trying to open the same DB while another has a
        R/W connection will fail with an IOException. This matters when
        running parallel backtest subprocesses (the helper's
        ``backtest_batch(concurrency=N)``). We retry with exponential
        backoff + jitter so short-lived connections (open → write one
        row → close, typical at backtest end) don't all fail the first
        time they collide.
        """
        import random
        import time as _time

        with self._lock:
            delay = 0.05
            last_err: Optional[Exception] = None
            # 32 attempts × growing-then-capped backoff gives ~45s of total
            # wait before giving up. Sized so 15+ concurrent backtest
            # subprocesses all land their final-row writes without a
            # single one getting starved out. The 8-attempt prior budget
            # (~6s) silently dropped most writes under sweeps with
            # concurrency >= 8 — see sweep b4v1od34x: 1080 "successful"
            # children, 0 actually persisted.
            for attempt in range(32):
                try:
                    conn = duckdb.connect(self.db_path)
                    break
                except duckdb.IOException as ex:
                    # Only retry on lock contention — not on malformed
                    # files or permission errors. The error string is
                    # the most portable signal across DuckDB versions.
                    msg = str(ex).lower()
                    if 'lock' not in msg and 'conflict' not in msg:
                        raise
                    last_err = ex
                    _time.sleep(delay + random.uniform(0, delay))
                    delay = min(delay * 2, 2.0)
            else:
                # Exhausted retries. Propagate so the caller (usually a
                # backtest subprocess) exits loudly rather than silently
                # dropping the write.
                raise last_err  # type: ignore[misc]
            try:
                return fn(conn)
            finally:
                conn.close()


def _default_db_path() -> str:
    return os.path.join(os.path.expanduser('~'), '.mmr', 'data.duckdb')


class DuckDBDataStore(DataStore):
    """Time-series data storage backed by DuckDB.

    All tick data is stored in a single table ``tick_data`` with a composite
    index on (symbol, date).  The ``date`` column is stored as TIMESTAMPTZ.
    """

    TABLE_NAME = 'tick_data'

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or _default_db_path()
        self._db = DuckDBConnection.get_instance(self.db_path)
        self._ensure_table()

    def _ensure_table(self):
        self._db.execute_atomic(lambda conn: self._create_table(conn))

    QUARANTINE_TABLE = 'tick_data_quarantine'

    def _create_table(self, conn):
        # The quarantine holds bars a vendor sent us that cannot describe a
        # real market. They are KEPT, deliberately: dropping them destroys the
        # evidence that the source shipped them, and vendor quality over time
        # is worth measuring. Nothing reads this table for trading or
        # backtesting — that is the whole point of it being a separate table
        # rather than a flag column, which every reader would have to remember
        # to filter on.
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.QUARANTINE_TABLE} (
                symbol VARCHAR NOT NULL,
                date TIMESTAMPTZ NOT NULL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                average DOUBLE,
                bar_count INTEGER,
                bar_size VARCHAR,
                what_to_show INTEGER,
                quarantined_at TIMESTAMPTZ NOT NULL,
                reason VARCHAR
            )
        """)
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
                symbol VARCHAR NOT NULL,
                date TIMESTAMPTZ NOT NULL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                average DOUBLE,
                bar_count INTEGER,
                bar_size VARCHAR,
                what_to_show INTEGER
            )
        """)
        # Create composite index if it does not exist.  DuckDB does not
        # support IF NOT EXISTS on CREATE INDEX, so we catch the error.
        try:
            conn.execute(f"""
                CREATE INDEX idx_{self.TABLE_NAME}_symbol_date
                ON {self.TABLE_NAME} (symbol, date)
            """)
        except duckdb.CatalogException:
            pass

    # --------------------------------------------------------------------- #
    # DataStore interface
    # --------------------------------------------------------------------- #

    def read(
        self,
        symbol: str,
        start: Optional[dt.datetime] = None,
        end: Optional[dt.datetime] = None,
        bar_size: Optional[str] = None,
    ) -> pd.DataFrame:
        conditions = ["symbol = ?"]
        params: list[Any] = [symbol]

        if start is not None:
            conditions.append("date >= ?")
            params.append(start)
        if end is not None:
            conditions.append("date <= ?")
            params.append(end)
        if bar_size is not None:
            conditions.append("(bar_size = ? OR bar_size IS NULL)")
            params.append(bar_size)

        where = " AND ".join(conditions)
        query = f"""
            SELECT date, open, high, low, close, volume, average,
                   bar_count, bar_size, what_to_show
            FROM {self.TABLE_NAME}
            WHERE {where}
            ORDER BY date
        """

        def _read(conn):
            result = conn.execute(query, params)
            return result.fetchdf()

        df = self._db.execute_atomic(_read)

        if df.empty:
            return pd.DataFrame()

        # Convert the date column to a DatetimeIndex with UTC timezone
        df['date'] = pd.to_datetime(df['date'], utc=True)
        df.set_index('date', inplace=True)
        df.index.name = 'date'

        return df


    def quarantine(self, symbol: str, df: pd.DataFrame, reason: str) -> int:
        """Persist rejected bars with the reason they were rejected.

        Append-only and never read by the trading or backtesting paths. Returns
        the number of rows recorded so the caller can log it.
        """
        if df is None or len(df) == 0:
            return 0
        frame = df.copy()
        frame['symbol'] = symbol
        frame['quarantined_at'] = pd.Timestamp.now(tz='UTC')
        frame['reason'] = reason
        if frame.index.name == 'date' or isinstance(frame.index, pd.DatetimeIndex):
            frame = frame.reset_index().rename(columns={'index': 'date'})
        cols = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume',
                'average', 'bar_count', 'bar_size', 'what_to_show',
                'quarantined_at', 'reason']
        for c in cols:
            if c not in frame.columns:
                frame[c] = None
        frame = frame[cols]

        def _run(conn):
            conn.register('quarantine_df', frame)
            conn.execute(
                f'INSERT INTO {self.QUARANTINE_TABLE} SELECT * FROM quarantine_df')
            conn.unregister('quarantine_df')
            return len(frame)

        return int(self._db.execute_atomic(_run))

    def write(self, symbol: str, df: pd.DataFrame) -> None:
        if df.empty:
            return

        # Work with a copy so we don't mutate the caller's DataFrame
        write_df = df.copy()

        # Ensure we have a 'date' column (may come from index)
        if 'date' not in write_df.columns:
            if write_df.index.name == 'date' or isinstance(write_df.index, pd.DatetimeIndex):
                write_df = write_df.reset_index()
                if write_df.columns[0] != 'date':
                    write_df = write_df.rename(columns={write_df.columns[0]: 'date'})

        # Make sure dates are timezone-aware (UTC)
        write_df['date'] = pd.to_datetime(write_df['date'], utc=True)

        # Add symbol column
        write_df['symbol'] = symbol

        # Ensure all expected columns exist with defaults
        for col, default in [
            ('open', None), ('high', None), ('low', None), ('close', None),
            ('volume', None), ('average', None), ('bar_count', None),
            ('bar_size', None), ('what_to_show', None),
        ]:
            if col not in write_df.columns:
                write_df[col] = default

        # Select only the columns we care about in the right order
        cols = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume',
                'average', 'bar_count', 'bar_size', 'what_to_show']
        write_df = write_df[[c for c in cols if c in write_df.columns]]

        # Upsert: delete existing rows for this symbol in the date range, then insert.
        # CRITICAL: filter the DELETE by bar_size too — otherwise writing a
        # 1-min window for AAPL clobbers any daily rows for AAPL in that same
        # date range. With write_resolve_overlap() merging in pre-existing rows,
        # the date span can grow to span years of history, deleting everything.
        # Observed: 10/20 NASDAQ daily downloads silently disappeared because
        # they were wiped by the subsequent 1-min write for the same conid.
        min_date = write_df['date'].min()
        max_date = write_df['date'].max()
        bar_sizes_being_written = list(write_df['bar_size'].dropna().unique())

        def _write(conn):
            # Wrap the DELETE + INSERT in a single transaction. Without this the
            # DELETE autocommits, so a crash (or an INSERT failure) between the
            # two leaves the old rows gone and the new rows never written —
            # permanent, silent data loss for that symbol/range.
            conn.execute("BEGIN TRANSACTION")
            try:
                if bar_sizes_being_written:
                    for bs in bar_sizes_being_written:
                        conn.execute(
                            f"DELETE FROM {self.TABLE_NAME} "
                            f"WHERE symbol = ? AND date >= ? AND date <= ? AND bar_size = ?",
                            [symbol, min_date, max_date, bs],
                        )
                else:
                    # Caller didn't set bar_size on any row — fall back to the old
                    # behavior (delete all bar_sizes in range). Should never happen
                    # for proper TickData writes; preserved for safety.
                    conn.execute(
                        f"DELETE FROM {self.TABLE_NAME} "
                        f"WHERE symbol = ? AND date >= ? AND date <= ?",
                        [symbol, min_date, max_date],
                    )
                conn.register('__write_df', write_df)
                try:
                    conn.execute(f"INSERT INTO {self.TABLE_NAME} SELECT * FROM __write_df")
                finally:
                    conn.unregister('__write_df')
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

        self._db.execute_atomic(_write)

    def delete(self, symbol: str) -> None:
        self._db.execute(
            f"DELETE FROM {self.TABLE_NAME} WHERE symbol = ?",
            [symbol],
        )

    def list_symbols(self) -> list[str]:
        def _list(conn):
            result = conn.execute(
                f"SELECT DISTINCT symbol FROM {self.TABLE_NAME} ORDER BY symbol"
            )
            return [row[0] for row in result.fetchall()]
        return self._db.execute_atomic(_list)

    def min_date(self, symbol: str, bar_size: Optional[str] = None) -> dt.datetime:
        def _min(conn):
            if bar_size is not None:
                result = conn.execute(
                    f"SELECT MIN(date) FROM {self.TABLE_NAME} WHERE symbol = ? AND bar_size = ?",
                    [symbol, bar_size],
                )
            else:
                result = conn.execute(
                    f"SELECT MIN(date) FROM {self.TABLE_NAME} WHERE symbol = ?",
                    [symbol],
                )
            return result.fetchone()
        row = self._db.execute_atomic(_min)
        if row is None or row[0] is None:
            raise ValueError(f"No data found for symbol: {symbol}")
        val = row[0]
        if isinstance(val, dt.datetime):
            return val
        return pd.Timestamp(val).to_pydatetime()

    def max_date(self, symbol: str, bar_size: Optional[str] = None) -> dt.datetime:
        def _max(conn):
            if bar_size is not None:
                result = conn.execute(
                    f"SELECT MAX(date) FROM {self.TABLE_NAME} WHERE symbol = ? AND bar_size = ?",
                    [symbol, bar_size],
                )
            else:
                result = conn.execute(
                    f"SELECT MAX(date) FROM {self.TABLE_NAME} WHERE symbol = ?",
                    [symbol],
                )
            return result.fetchone()
        row = self._db.execute_atomic(_max)
        if row is None or row[0] is None:
            raise ValueError(f"No data found for symbol: {symbol}")
        val = row[0]
        if isinstance(val, dt.datetime):
            return val
        return pd.Timestamp(val).to_pydatetime()


class DuckDBObjectStore(ObjectStore):
    """Key-value object storage backed by DuckDB.

    Objects are serialized with ``dill`` and stored as BLOBs in the
    ``object_store`` table.
    """

    TABLE_NAME = 'object_store'

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or _default_db_path()
        self._db = DuckDBConnection.get_instance(self.db_path)
        self._ensure_table()

    def _ensure_table(self):
        self._db.execute_atomic(lambda conn: conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
                key VARCHAR PRIMARY KEY,
                data BLOB NOT NULL
            )
        """))

    # --------------------------------------------------------------------- #
    # ObjectStore interface
    # --------------------------------------------------------------------- #

    def read(self, key: str) -> Any:
        def _read(conn):
            result = conn.execute(
                f"SELECT data FROM {self.TABLE_NAME} WHERE key = ?",
                [key],
            )
            return result.fetchone()
        row = self._db.execute_atomic(_read)
        if row is None:
            return None
        return dill.loads(row[0])

    def write(self, key: str, data: Any, **kwargs) -> None:
        blob = dill.dumps(data)
        self._db.execute_atomic(lambda conn: conn.execute(
            f"""
            INSERT OR REPLACE INTO {self.TABLE_NAME} (key, data)
            VALUES (?, ?)
            """,
            [key, blob],
        ))

    def delete(self, key: str) -> None:
        self._db.execute(
            f"DELETE FROM {self.TABLE_NAME} WHERE key = ?",
            [key],
        )

    def list_symbols(self) -> list[str]:
        def _list(conn):
            result = conn.execute(
                f"SELECT key FROM {self.TABLE_NAME} ORDER BY key"
            )
            return [row[0] for row in result.fetchall()]
        return self._db.execute_atomic(_list)
