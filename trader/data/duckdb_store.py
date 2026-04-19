import datetime as dt
import dill
import duckdb
import os
import pandas as pd
import threading

from pathlib import Path
from typing import Any, Optional

from trader.data.store import DataStore, ObjectStore


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
        """
        with self._lock:
            conn = duckdb.connect(self.db_path)
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

    def _create_table(self, conn):
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

        # Upsert: delete existing rows for this symbol in the date range, then insert
        min_date = write_df['date'].min()
        max_date = write_df['date'].max()

        def _write(conn):
            conn.execute(
                f"DELETE FROM {self.TABLE_NAME} WHERE symbol = ? AND date >= ? AND date <= ?",
                [symbol, min_date, max_date],
            )
            conn.register('__write_df', write_df)
            try:
                conn.execute(f"INSERT INTO {self.TABLE_NAME} SELECT * FROM __write_df")
            finally:
                conn.unregister('__write_df')

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
