"""Small operational journal, isolated from historical-data DuckDB locks.

SQLite's WAL and FULL synchronous commits provide a durable intent/outbox
boundary.  No network or broker call may occur inside ``transaction``.  The
sidecar belongs with the trading database and must be backed up with it.
"""
from __future__ import annotations

import contextlib
import os
import sqlite3
import threading
import weakref
from collections.abc import Iterator


class ExecutionJournal:
    def __init__(self, database_path: str):
        self.path = (os.path.realpath(os.path.expanduser(database_path))
                     + '.execution.sqlite3')
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(self.path, timeout=5.0, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute('PRAGMA journal_mode=WAL')
        self._connection.execute('PRAGMA synchronous=FULL')
        # A persistent WAL connection avoids a checkpoint/fsync on every
        # read-only intent lookup. Each actor serializes access through _lock;
        # separate processes use SQLite's own transaction locking.
        self._finalizer = weakref.finalize(self, self._connection.close)
        with self.transaction() as conn:
            conn.executescript('''
                CREATE TABLE IF NOT EXISTS execution_intents (
                    intent_id TEXT PRIMARY KEY,
                    strategy TEXT NOT NULL,
                    conid INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    updated REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS intent_owner
                    ON execution_intents(strategy, conid, kind, status);
                CREATE TABLE IF NOT EXISTS execution_bar_progress (
                    strategy TEXT NOT NULL,
                    conid INTEGER NOT NULL,
                    entry_bar_ts TEXT NOT NULL,
                    watermark TEXT NOT NULL,
                    bars_held INTEGER NOT NULL,
                    PRIMARY KEY(strategy, conid, entry_bar_ts)
                );
                CREATE TABLE IF NOT EXISTS broker_order_progress (
                    identity TEXT PRIMARY KEY,
                    cumulative REAL NOT NULL,
                    notional REAL NOT NULL,
                    terminal TEXT,
                    snapshot TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS broker_execution_receipts (
                    identity TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS broker_event_outbox (
                    identity TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    delivered INTEGER NOT NULL DEFAULT 0
                );
            ''')

    @contextlib.contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            conn = self._connection
            try:
                conn.execute('BEGIN IMMEDIATE')
                yield conn
                conn.commit()
            except BaseException:
                conn.rollback()
                raise

    def close(self):
        with self._lock:
            self._finalizer()
