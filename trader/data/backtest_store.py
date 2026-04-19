"""Backtest-run storage — DuckDB-backed history of every backtest executed.

Answers questions like:
  - "What parameters produced last week's best mean-reversion run?"
  - "Did the edge hold before I changed the threshold yesterday?"
  - "Show me every backtest of AAPL on 1-min bars."

Stores: inputs (strategy file + class + params, conids, date range, bar_size,
fill policy, etc.), summary metrics (return, sharpe, drawdown, win rate, trade
count), and a SHA-256 hash of the strategy source so you can tell whether two
runs used the same code or not. The full trade list and equity curve are
serialized as JSON and optional (off by default to keep the DB compact;
opt-in per run).
"""

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

import datetime as dt
import hashlib
import json
import os

from trader.data.duckdb_store import DuckDBConnection


@dataclass
class BacktestRecord:
    # Inputs
    strategy_path: str
    class_name: str
    conids: List[int]
    universe: str
    start_date: dt.datetime
    end_date: dt.datetime
    bar_size: str
    initial_capital: float
    fill_policy: str
    slippage_bps: float
    commission_per_share: float
    params: Dict[str, Any] = field(default_factory=dict)
    code_hash: str = ''

    # Outputs (populated after run)
    total_trades: int = 0
    total_return: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0
    win_rate: float = 0.0
    final_equity: float = 0.0

    # Extended practitioner metrics
    sortino_ratio: float = 0.0
    calmar_ratio: float = 0.0
    profit_factor: float = 0.0
    expectancy_bps: float = 0.0
    time_in_market_pct: float = 0.0

    # Optional full detail (large — stored as JSON when requested)
    trades_json: str = ''
    equity_curve_json: str = ''

    # Lifecycle
    id: Optional[int] = None
    created_at: Optional[dt.datetime] = None
    note: str = ''
    # Soft-delete flag. Archived runs are hidden from ``list()`` by default
    # but still stored — they can be restored via ``unarchive`` or shown
    # with ``list(include_archived=True)``. Preferred over ``delete`` when
    # you want to clean up the default history view without losing the
    # data (e.g. for later meta-analysis).
    archived: bool = False
    # Parent sweep id, if this run was produced by ``mmr sweep run``.
    # ``None`` for ad-hoc ``mmr backtest`` or ``bt-sweep`` runs. Lets
    # ``backtests list --sweep <id>`` filter and ``sweeps show <id>``
    # reconstruct the leaderboard of a nightly cron.
    sweep_id: Optional[int] = None


@dataclass
class SweepRecord:
    """Metadata for a ``mmr sweep run`` invocation — the parent entity
    that owns N ``BacktestRecord`` rows.

    Persisted so humans and LLMs can answer "what sweeps have we run?"
    without grep-guessing run notes. The full YAML manifest is stored
    verbatim so the sweep is reproducible; the config_hash lets the
    runner dedup against recent identical sweeps.
    """
    name: str
    manifest_yaml: str
    config_hash: str
    status: str = 'running'            # running | completed | failed | cancelled
    started_at: Optional[dt.datetime] = None
    finished_at: Optional[dt.datetime] = None
    n_runs_planned: int = 0
    n_runs_successful: int = 0
    n_runs_failed: int = 0
    concurrency: int = 1
    digest_path: str = ''              # filesystem path to the markdown digest
    note: str = ''
    id: Optional[int] = None


def compute_strategy_hash(strategy_path: str) -> str:
    """SHA-256 of the strategy source file. Empty string if unreadable —
    persistence still works, you just lose the reproducibility breadcrumb."""
    try:
        with open(strategy_path, 'rb') as f:
            return hashlib.sha256(f.read()).hexdigest()
    except (OSError, FileNotFoundError):
        return ''


class BacktestStore:
    _CREATE_TABLE = """
        CREATE TABLE IF NOT EXISTS backtest_runs (
            id INTEGER PRIMARY KEY,
            strategy_path VARCHAR NOT NULL,
            class_name VARCHAR NOT NULL,
            conids VARCHAR NOT NULL,              -- JSON [int]
            universe VARCHAR DEFAULT '',
            start_date TIMESTAMP NOT NULL,
            end_date TIMESTAMP NOT NULL,
            bar_size VARCHAR NOT NULL,
            initial_capital DOUBLE NOT NULL,
            fill_policy VARCHAR DEFAULT 'next_open',
            slippage_bps DOUBLE DEFAULT 0.0,
            commission_per_share DOUBLE DEFAULT 0.0,
            params VARCHAR DEFAULT '{}',          -- JSON
            code_hash VARCHAR DEFAULT '',

            total_trades INTEGER DEFAULT 0,
            total_return DOUBLE DEFAULT 0.0,
            sharpe_ratio DOUBLE DEFAULT 0.0,
            max_drawdown DOUBLE DEFAULT 0.0,
            win_rate DOUBLE DEFAULT 0.0,
            final_equity DOUBLE DEFAULT 0.0,

            sortino_ratio DOUBLE DEFAULT 0.0,
            calmar_ratio DOUBLE DEFAULT 0.0,
            profit_factor DOUBLE DEFAULT 0.0,
            expectancy_bps DOUBLE DEFAULT 0.0,
            time_in_market_pct DOUBLE DEFAULT 0.0,

            trades_json VARCHAR DEFAULT '',       -- optional
            equity_curve_json VARCHAR DEFAULT '', -- optional

            created_at TIMESTAMP NOT NULL,
            note VARCHAR DEFAULT '',
            archived BOOLEAN DEFAULT FALSE,       -- soft-delete flag
            sweep_id INTEGER DEFAULT NULL         -- FK into sweeps.id
        )
    """

    _CREATE_SWEEPS_TABLE = """
        CREATE TABLE IF NOT EXISTS sweeps (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            manifest_yaml VARCHAR NOT NULL,
            config_hash VARCHAR DEFAULT '',
            status VARCHAR DEFAULT 'running',
            started_at TIMESTAMP NOT NULL,
            finished_at TIMESTAMP,
            n_runs_planned INTEGER DEFAULT 0,
            n_runs_successful INTEGER DEFAULT 0,
            n_runs_failed INTEGER DEFAULT 0,
            concurrency INTEGER DEFAULT 1,
            digest_path VARCHAR DEFAULT '',
            note VARCHAR DEFAULT ''
        )
    """
    _CREATE_SWEEPS_SEQUENCE = """
        CREATE SEQUENCE IF NOT EXISTS sweeps_id_seq START 1
    """

    _CREATE_SEQUENCE = """
        CREATE SEQUENCE IF NOT EXISTS backtest_runs_id_seq START 1
    """

    _INSERT = """
        INSERT INTO backtest_runs (
            id, strategy_path, class_name, conids, universe,
            start_date, end_date, bar_size, initial_capital, fill_policy,
            slippage_bps, commission_per_share, params, code_hash,
            total_trades, total_return, sharpe_ratio, max_drawdown,
            win_rate, final_equity,
            sortino_ratio, calmar_ratio, profit_factor, expectancy_bps,
            time_in_market_pct,
            trades_json, equity_curve_json,
            created_at, note, sweep_id
        )
        VALUES (
            nextval('backtest_runs_id_seq'), ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?,
            ?, ?, ?, ?,
            ?,
            ?, ?,
            ?, ?, ?
        )
    """

    # Schema migration: columns added after the initial release. ALTER TABLE
    # ADD COLUMN is idempotent in DuckDB (raises on duplicate), so we try each
    # and swallow the "already exists" error. Keeps existing rows intact.
    _MIGRATE_COLUMNS = (
        ('sortino_ratio',      'DOUBLE DEFAULT 0.0'),
        ('calmar_ratio',       'DOUBLE DEFAULT 0.0'),
        ('profit_factor',      'DOUBLE DEFAULT 0.0'),
        ('expectancy_bps',     'DOUBLE DEFAULT 0.0'),
        ('time_in_market_pct', 'DOUBLE DEFAULT 0.0'),
        ('archived',           'BOOLEAN DEFAULT FALSE'),
        ('sweep_id',           'INTEGER DEFAULT NULL'),
    )

    # Explicit SELECT column list — we cannot rely on ``SELECT *`` because
    # ALTER TABLE ADD COLUMN appends to the END of the stored table, while
    # our CREATE_TABLE declares new metrics in the MIDDLE. A fresh DB and
    # a migrated DB therefore return columns in different orders, and the
    # positional unpacking in ``_rows_to_records`` would silently read the
    # wrong values (e.g. profit_factor becoming a TIMESTAMP). Naming them
    # forces a stable order regardless of storage path.
    _SELECT_COLUMNS = (
        'id', 'strategy_path', 'class_name', 'conids', 'universe',
        'start_date', 'end_date', 'bar_size', 'initial_capital',
        'fill_policy', 'slippage_bps', 'commission_per_share',
        'params', 'code_hash',
        'total_trades', 'total_return', 'sharpe_ratio', 'max_drawdown',
        'win_rate', 'final_equity',
        'sortino_ratio', 'calmar_ratio', 'profit_factor',
        'expectancy_bps', 'time_in_market_pct',
        'trades_json', 'equity_curve_json',
        'created_at', 'note', 'archived', 'sweep_id',
    )
    _SELECT_FIELDS = ', '.join(_SELECT_COLUMNS)

    def __init__(self, duckdb_path: str):
        self.duckdb_path = duckdb_path
        self.db = DuckDBConnection.get_instance(duckdb_path)
        self._ensure_table()

    def _ensure_table(self):
        def _init(conn):
            conn.execute(self._CREATE_TABLE)
            conn.execute(self._CREATE_SEQUENCE)
            conn.execute(self._CREATE_SWEEPS_TABLE)
            conn.execute(self._CREATE_SWEEPS_SEQUENCE)
            # Idempotent column adds for databases created before the
            # extended-metrics release. DuckDB raises on duplicate ADD
            # COLUMN; we swallow that specific error and move on.
            for name, decl in self._MIGRATE_COLUMNS:
                try:
                    conn.execute(f"ALTER TABLE backtest_runs ADD COLUMN {name} {decl}")
                except Exception as ex:
                    msg = str(ex).lower()
                    if 'already exists' in msg or 'duplicate' in msg:
                        continue
                    raise
        self.db.execute_atomic(_init)

    def add(self, record: BacktestRecord) -> int:
        """Persist a backtest run and return its assigned id."""
        now = dt.datetime.now()

        def _insert(conn):
            # ``inf`` is not representable in DuckDB DOUBLE — coerce to a
            # sentinel (1e18) so profit_factor on an all-winners run still
            # persists sensibly. ``nan`` becomes 0.
            def _safe(x):
                try:
                    fx = float(x)
                except (TypeError, ValueError):
                    return 0.0
                if fx != fx:     # nan
                    return 0.0
                if fx == float('inf'):
                    return 1e18
                if fx == float('-inf'):
                    return -1e18
                return fx

            conn.execute(self._INSERT, [
                record.strategy_path,
                record.class_name,
                json.dumps(record.conids),
                record.universe or '',
                record.start_date,
                record.end_date,
                record.bar_size,
                record.initial_capital,
                record.fill_policy,
                record.slippage_bps,
                record.commission_per_share,
                json.dumps(record.params),
                record.code_hash,
                record.total_trades,
                _safe(record.total_return),
                _safe(record.sharpe_ratio),
                _safe(record.max_drawdown),
                _safe(record.win_rate),
                _safe(record.final_equity),
                _safe(record.sortino_ratio),
                _safe(record.calmar_ratio),
                _safe(record.profit_factor),
                _safe(record.expectancy_bps),
                _safe(record.time_in_market_pct),
                record.trades_json,
                record.equity_curve_json,
                now,
                record.note,
                record.sweep_id,
            ])
            row = conn.execute("SELECT currval('backtest_runs_id_seq')").fetchone()
            return row[0]

        return self.db.execute_atomic(_insert)

    def get(self, id: int) -> Optional[BacktestRecord]:
        rows = self.db.execute(
            f"SELECT {self._SELECT_FIELDS} FROM backtest_runs WHERE id = ?",
            [id], fetch='all',
        ) or []
        records = self._rows_to_records(rows)
        return records[0] if records else None

    # Columns that ``list()`` is allowed to sort by. Restricting via a
    # whitelist rather than interpolating the user string prevents SQL
    # injection — we parameterize values but the sort column has to be
    # literal SQL, so validation happens here.
    _SORTABLE_COLUMNS = frozenset({
        'created_at', 'total_return', 'sharpe_ratio', 'sortino_ratio',
        'calmar_ratio', 'profit_factor', 'expectancy_bps', 'win_rate',
        'max_drawdown', 'total_trades', 'time_in_market_pct',
    })

    def list(
        self,
        strategy_class: Optional[str] = None,
        limit: int = 50,
        sort_by: str = 'created_at',
        descending: bool = True,
        include_archived: bool = False,
        archived_only: bool = False,
        sweep_id: Optional[int] = None,
    ) -> List[BacktestRecord]:
        """List stored runs, most-recent or best-first by the requested metric.

        ``sort_by`` accepts any column in ``_SORTABLE_COLUMNS`` (e.g.
        ``'sharpe_ratio'``). Unknown columns raise ValueError — we don't
        interpolate arbitrary strings into SQL.

        By default archived rows are **excluded** — they're the history
        equivalent of deleting, just reversible. Pass
        ``include_archived=True`` for "everything", or ``archived_only=True``
        for just the archived rows (e.g. to review before purging).

        ``sweep_id`` filters to runs produced by a specific sweep (see
        the ``sweeps`` table). Useful for "show me the leaderboard of
        last night's cron".
        """
        if sort_by not in self._SORTABLE_COLUMNS:
            raise ValueError(
                f'cannot sort by {sort_by!r}; allowed: '
                f'{sorted(self._SORTABLE_COLUMNS)}'
            )
        order = 'DESC' if descending else 'ASC'

        clauses: List[str] = []
        params: List = []
        if strategy_class:
            clauses.append('class_name = ?')
            params.append(strategy_class)
        if sweep_id is not None:
            clauses.append('sweep_id = ?')
            params.append(sweep_id)
        if archived_only:
            clauses.append('archived = TRUE')
        elif not include_archived:
            clauses.append('archived = FALSE')
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ''

        params.append(limit)
        rows = self.db.execute(
            f"SELECT {self._SELECT_FIELDS} FROM backtest_runs"
            f"{where} ORDER BY {sort_by} {order}, id DESC LIMIT ?",
            params,
            fetch='all',
        ) or []
        return self._rows_to_records(rows)

    # -- Sweep CRUD -----------------------------------------------------

    _SWEEP_COLUMNS = (
        'id', 'name', 'manifest_yaml', 'config_hash', 'status',
        'started_at', 'finished_at', 'n_runs_planned', 'n_runs_successful',
        'n_runs_failed', 'concurrency', 'digest_path', 'note',
    )
    _SWEEP_SELECT = ', '.join(_SWEEP_COLUMNS)

    def create_sweep(self, record: SweepRecord) -> int:
        """Persist a sweep row at the *start* of a sweep run. Returns the
        assigned id; child ``backtest_runs`` rows reference it via ``sweep_id``.

        ``status`` is set to ``'running'`` here and transitioned by
        ``finalize_sweep`` when the sweep ends (success, failure, or
        SIGINT cancellation)."""
        now = dt.datetime.now()

        def _insert(conn):
            conn.execute(
                """
                INSERT INTO sweeps (
                    id, name, manifest_yaml, config_hash, status,
                    started_at, n_runs_planned, concurrency, note
                ) VALUES (
                    nextval('sweeps_id_seq'), ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                [
                    record.name, record.manifest_yaml, record.config_hash,
                    record.status or 'running', now,
                    record.n_runs_planned, record.concurrency, record.note,
                ],
            )
            row = conn.execute("SELECT currval('sweeps_id_seq')").fetchone()
            return row[0]

        return self.db.execute_atomic(_insert)

    def finalize_sweep(
        self,
        sweep_id: int,
        *,
        status: str,
        n_runs_successful: int,
        n_runs_failed: int,
        digest_path: str = '',
    ) -> None:
        """Transition a sweep to its terminal state. ``status`` should be
        one of ``'completed'``, ``'failed'``, ``'cancelled'``.

        Safe to call multiple times; last call wins — useful when SIGINT
        handling tries to write 'cancelled' on its way out."""
        now = dt.datetime.now()
        self.db.execute(
            """
            UPDATE sweeps
               SET status = ?, finished_at = ?,
                   n_runs_successful = ?, n_runs_failed = ?,
                   digest_path = ?
             WHERE id = ?
            """,
            [status, now, n_runs_successful, n_runs_failed, digest_path, sweep_id],
            fetch='none',
        )

    def get_sweep(self, sweep_id: int) -> Optional[SweepRecord]:
        rows = self.db.execute(
            f"SELECT {self._SWEEP_SELECT} FROM sweeps WHERE id = ?",
            [sweep_id], fetch='all',
        ) or []
        return self._rows_to_sweeps(rows)[0] if rows else None

    def list_sweeps(self, limit: int = 25) -> List[SweepRecord]:
        """Most-recent sweeps first. Used by ``mmr sweep list`` to show
        what's run historically."""
        rows = self.db.execute(
            f"SELECT {self._SWEEP_SELECT} FROM sweeps "
            f"ORDER BY started_at DESC LIMIT ?",
            [limit], fetch='all',
        ) or []
        return self._rows_to_sweeps(rows)

    def _rows_to_sweeps(self, rows: list) -> List[SweepRecord]:
        out: List[SweepRecord] = []
        for row in rows:
            out.append(SweepRecord(
                id=row[0], name=row[1], manifest_yaml=row[2],
                config_hash=row[3], status=row[4],
                started_at=row[5], finished_at=row[6],
                n_runs_planned=row[7], n_runs_successful=row[8],
                n_runs_failed=row[9], concurrency=row[10],
                digest_path=row[11], note=row[12],
            ))
        return out

    def set_archived(self, ids: List[int], archived: bool) -> int:
        """Mark ``ids`` archived (or un-archived). Returns the number of
        rows whose state **actually changed** — ids that are missing or
        already in the target state are counted as 0.

        Bulk API so ``backtests archive 1 2 3 4`` is a single DB
        round-trip, and the CLI can report real vs no-op moves.
        """
        if not ids:
            return 0

        def _update(conn):
            placeholders = ','.join('?' for _ in ids)
            # Count the rows that will actually transition — everything else
            # in ``ids`` is either missing or already in target state and
            # shouldn't show up as "updated" in the CLI's report. DuckDB
            # doesn't give a reliable rowcount on the bound cursor, so we
            # pre-count in the same transaction.
            changed = conn.execute(
                f"SELECT COUNT(*) FROM backtest_runs "
                f"WHERE id IN ({placeholders}) AND archived != ?",
                [*ids, archived],
            ).fetchone()
            conn.execute(
                f"UPDATE backtest_runs SET archived = ? "
                f"WHERE id IN ({placeholders})",
                [archived, *ids],
            )
            return changed[0] if changed else 0

        return self.db.execute_atomic(_update)

    def delete(self, id: int) -> bool:
        def _delete(conn):
            conn.execute("DELETE FROM backtest_runs WHERE id = ?", [id])
            row = conn.execute(
                "SELECT COUNT(*) FROM backtest_runs WHERE id = ?", [id]
            ).fetchone()
            return row[0] == 0
        return self.db.execute_atomic(_delete)

    def _rows_to_records(self, rows: list) -> List[BacktestRecord]:
        out: List[BacktestRecord] = []
        for row in rows:
            # New columns were appended — older rows created before the
            # migration have 0.0 defaults (applied by DuckDB on ALTER).
            # Read positionally; column order matches _CREATE_TABLE.
            out.append(BacktestRecord(
                id=row[0],
                strategy_path=row[1],
                class_name=row[2],
                conids=json.loads(row[3]) if row[3] else [],
                universe=row[4],
                start_date=row[5],
                end_date=row[6],
                bar_size=row[7],
                initial_capital=row[8],
                fill_policy=row[9],
                slippage_bps=row[10],
                commission_per_share=row[11],
                params=json.loads(row[12]) if row[12] else {},
                code_hash=row[13],
                total_trades=row[14],
                total_return=row[15],
                sharpe_ratio=row[16],
                max_drawdown=row[17],
                win_rate=row[18],
                final_equity=row[19],
                sortino_ratio=row[20] if len(row) > 20 else 0.0,
                calmar_ratio=row[21] if len(row) > 21 else 0.0,
                profit_factor=row[22] if len(row) > 22 else 0.0,
                expectancy_bps=row[23] if len(row) > 23 else 0.0,
                time_in_market_pct=row[24] if len(row) > 24 else 0.0,
                trades_json=row[25] if len(row) > 25 else '',
                equity_curve_json=row[26] if len(row) > 26 else '',
                created_at=row[27] if len(row) > 27 else None,
                note=row[28] if len(row) > 28 else '',
                archived=bool(row[29]) if len(row) > 29 else False,
                sweep_id=row[30] if len(row) > 30 else None,
            ))
        return out
