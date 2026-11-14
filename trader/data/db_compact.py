"""Measure and reclaim dead space in a DuckDB file.

A DuckDB file never shrinks, and for most of this store's life it could not
even reuse what it freed: `tick_data` carried an ART index, and DuckDB 1.x does
not vacuum deleted rows out of an indexed table. Every upsert (DELETE the
incoming keys, INSERT the new rows) therefore left its dead rows in the file
for good. Measured 2026-11-14 on the live history database:

    62.4 M live rows stored in 1,292 M row slots  (95% dead)
    28.6 GiB on disk; 5.2 GB as a fresh copy, of which ~3.7 GB was the index

The index bought nothing — no query shape the store runs uses it (plans and
timings are identical with index scans switched off) — and every short-lived
connection paid for the dead weight: 131 ms per open/query/close against the
live file, 61 ms against a fresh copy of the same data.

`database_usage` reports that state read-only. `compact_database` rebuilds the
file and swaps it in:

  * the source is attached READ-ONLY for the whole operation. That shared lock
    keeps every writer out (they retry with backoff, exactly as they do against
    the nightly backup), so there is no moment at which a write can land in the
    file that is about to be replaced;
  * the rebuild is verified before anything is swapped: every table must have
    the same row count and the same order-independent content checksum, and
    the catalogue (tables, views, sequences, constraints, surviving indexes)
    must match;
  * the swap is one `rename(2)` performed while the source lock is still held.
    The old file is kept beside the new one unless the caller discards it, under
    a name the backup job does not pick up.

Nothing here is run automatically. It is an operator act (`mmr data compact
--apply`), best done while no market is in session: the lock is held for about
as long as the nightly backup holds it.
"""
import datetime as dt
import logging
import os
import random
import shutil
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import duckdb

logger = logging.getLogger(__name__)

#: Kept-file marker. Deliberately NOT a `.duckdb` suffix: the backup job
#: snapshots every `*.duckdb` in the data directory.
OLD_FILE_INFIX = '.pre_compact_'


class CompactionError(RuntimeError):
    """The rebuild was abandoned; the source database is untouched."""


def _quote_literal(text: str) -> str:
    return "'" + str(text).replace("'", "''") + "'"


def _quote_identifier(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _is_lock_error(ex: BaseException) -> bool:
    return 'lock' in str(ex).lower()


def _retry_on_lock(action, max_attempts: int):
    """Run ``action`` until it stops failing on the file lock (same pattern as
    the backup and ``execute_atomic``). Anything else raises immediately."""
    last: Optional[BaseException] = None
    for attempt in range(max(1, max_attempts)):
        try:
            return action()
        except duckdb.Error as ex:
            if not _is_lock_error(ex):
                raise
            last = ex
            time.sleep(min(2.0, 0.1 * (2 ** attempt)) + random.random() * 0.1)
    raise CompactionError(f'database stayed locked by another process: {last}')


def _wal_path(path: Path) -> Path:
    return path.with_name(path.name + '.wal')


def _tables(conn, database: str) -> List[str]:
    rows = conn.execute(
        'SELECT table_name FROM duckdb_tables() WHERE database_name = ? AND NOT internal '
        "AND schema_name = 'main' ORDER BY table_name", [database]).fetchall()
    return [row[0] for row in rows]


def _columns(conn, database: str, table: str) -> List[str]:
    rows = conn.execute(
        'SELECT column_name FROM duckdb_columns() WHERE database_name = ? AND table_name = ? '
        "AND schema_name = 'main' ORDER BY column_index", [database, table]).fetchall()
    return [row[0] for row in rows]


def _table_fingerprint(conn, database: str, table: str) -> tuple:
    """``(rows, xor of row hashes, sum of row hashes mod p)``.

    Order-independent, so a rebuild that re-sorts a table still matches. Two
    aggregates because XOR alone cancels a pair of identical rows.
    """
    columns = ', '.join(_quote_identifier(column) for column in _columns(conn, database, table))
    if not columns:
        return (0, None, None)
    qualified = f'{_quote_identifier(database)}.main.{_quote_identifier(table)}'
    row = conn.execute(
        f'SELECT count(*), bit_xor(h), sum(h % 2147483647) FROM (SELECT hash({columns}) AS h FROM {qualified})'
    ).fetchone()
    return (int(row[0]), row[1], row[2]) if row else (0, None, None)


def _catalogue(conn, database: str, ignore_indexes: Iterable[str] = ()) -> Dict[str, Any]:
    """What must survive a rebuild besides the rows."""
    ignored = set(ignore_indexes)
    parameters = [database]
    return {
        'tables': sorted(_tables(conn, database)),
        'views': sorted(row[0] for row in conn.execute(
            'SELECT view_name FROM duckdb_views() WHERE database_name = ? AND NOT internal', parameters).fetchall()),
        # (name, next value): a copy re-creates a sequence starting where the
        # original would continue, so compare what the NEXT id will be.
        'sequences': sorted(
            (row[0], (int(row[1]) + int(row[3])) if row[1] is not None else int(row[2]))
            for row in conn.execute(
                'SELECT sequence_name, last_value, start_value, increment_by FROM duckdb_sequences() '
                'WHERE database_name = ?', parameters).fetchall()),
        'constraints': sorted(
            (row[0], row[1], tuple(row[2] or ()))
            for row in conn.execute(
                'SELECT table_name, constraint_type, constraint_column_names FROM duckdb_constraints() '
                'WHERE database_name = ?', parameters).fetchall()),
        'indexes': sorted(
            (row[0], row[1]) for row in conn.execute(
                'SELECT index_name, table_name FROM duckdb_indexes() WHERE database_name = ?', parameters).fetchall()
            if row[0] not in ignored),
    }


def database_usage(path) -> Dict[str, Any]:
    """Read-only report of how much of a DuckDB file is live data.

    ``tables`` lists, largest waste first, each table's live rows, the row
    slots its row groups still store, and the blocks its data occupies.
    ``dead_fraction`` is over the whole file's stored rows. Raises
    ``CompactionError`` when the file stays locked.
    """
    source = Path(os.path.expanduser(str(path))).resolve(strict=True)
    conn = _retry_on_lock(lambda: duckdb.connect(str(source), read_only=True), 12)
    try:
        size = conn.execute('PRAGMA database_size').fetchone()
        block_size, total, used, free = int(size[2]), int(size[3]), int(size[4]), int(size[5])
        tables: List[Dict[str, Any]] = []
        for table in _tables(conn, conn.execute('SELECT current_database()').fetchone()[0]):
            live = int(conn.execute(f'SELECT count(*) FROM {_quote_identifier(table)}').fetchone()[0])
            stored_row = conn.execute(
                f"SELECT coalesce(sum(count), 0) FROM pragma_storage_info({_quote_literal(table)}) "
                "WHERE column_path = '[0]'").fetchone()
            block_row = conn.execute(
                f'SELECT count(DISTINCT block_id) FROM pragma_storage_info({_quote_literal(table)}) '
                'WHERE block_id >= 0').fetchone()
            stored = max(int(stored_row[0]), live)
            tables.append({
                'table': table, 'live_rows': live, 'stored_rows': stored, 'dead_rows': stored - live,
                'data_bytes': int(block_row[0]) * block_size,
            })
        indexes = [
            {'index': row[0], 'table': row[1]} for row in conn.execute(
                'SELECT index_name, table_name FROM duckdb_indexes() ORDER BY 1').fetchall()]
    finally:
        conn.close()
    tables.sort(key=lambda item: (-item['dead_rows'], item['table']))
    stored_total = sum(item['stored_rows'] for item in tables)
    live_total = sum(item['live_rows'] for item in tables)
    return {
        'path': str(source), 'file_bytes': source.stat().st_size, 'block_size': block_size,
        'total_blocks': total, 'used_blocks': used, 'free_blocks': free,
        'live_rows': live_total, 'stored_rows': stored_total,
        'dead_fraction': (1.0 - live_total / stored_total) if stored_total else 0.0,
        'tables': tables, 'indexes': indexes,
    }


def compact_database(
    path,
    *,
    order_by: Optional[Mapping[str, Sequence[str]]] = None,
    drop_indexes: Sequence[str] = (),
    keep_old: bool = True,
    max_attempts: int = 12,
    memory_limit: Optional[str] = None,
) -> Dict[str, Any]:
    """Rebuild the DuckDB at ``path`` into a compact file and swap it in.

    ``order_by`` maps a table to the columns its rows are written in (a table
    not named, or missing one of the columns, keeps its stored order).
    ``drop_indexes`` names indexes NOT to carry across. ``keep_old`` leaves the
    replaced file beside the new one as ``<name>.pre_compact_<utc stamp>``.

    Raises ``CompactionError`` — with the source untouched and every scratch
    file removed — when the file stays locked, a leftover write-ahead log is
    found, or the rebuild does not verify.
    """
    source = Path(os.path.expanduser(str(path))).resolve(strict=True)
    if source.suffix != '.duckdb':
        raise ValueError(f'not a DuckDB file: {source}')
    order_by = {table: list(columns) for table, columns in (order_by or {}).items()}
    token = uuid.uuid4().hex[:12]
    staged = source.with_name(f'.{source.name}.compact-{token}.tmp')
    scratch = source.with_name(f'.{source.name}.compact-{token}.spill')
    started = time.monotonic()
    bytes_before = source.stat().st_size

    # A clean open folds any write-ahead log into the file and removes it; a
    # log that survives that belongs to a crashed writer and must be looked at
    # by a person, because it would be replayed onto the REBUILT file.
    def _checkpoint():
        writer = duckdb.connect(str(source))
        try:
            writer.execute('CHECKPOINT')
        finally:
            writer.close()

    _retry_on_lock(_checkpoint, max_attempts)
    if _wal_path(source).exists():
        raise CompactionError(f'{_wal_path(source).name} is still present after a checkpoint; not compacting')

    conn = duckdb.connect()                     # in-memory: owns neither file
    attached_source = attached_target = False
    swapped = False
    try:
        scratch.mkdir(parents=True, exist_ok=False)
        conn.execute(f'SET temp_directory = {_quote_literal(str(scratch))}')
        if memory_limit:
            conn.execute(f'SET memory_limit = {_quote_literal(memory_limit)}')
        _retry_on_lock(
            lambda: conn.execute(f'ATTACH {_quote_literal(str(source))} AS _src (READ_ONLY)'), max_attempts)
        attached_source = True
        locked_at = time.monotonic()
        conn.execute(f'ATTACH {_quote_literal(str(staged))} AS _new')
        attached_target = True

        conn.execute('COPY FROM DATABASE _src TO _new (SCHEMA)')
        present = {row[0] for row in conn.execute(
            "SELECT index_name FROM duckdb_indexes() WHERE database_name = '_new'").fetchall()}
        dropped = [name for name in drop_indexes if name in present]
        for name in dropped:
            conn.execute(f'DROP INDEX _new.main.{_quote_identifier(name)}')

        tables = []
        for table in _tables(conn, '_src'):
            columns = _columns(conn, '_src', table)
            wanted = order_by.get(table) or []
            ordering = wanted if wanted and all(column in columns for column in wanted) else []
            clause = (' ORDER BY ' + ', '.join(_quote_identifier(column) for column in ordering)) if ordering else ''
            name = _quote_identifier(table)
            conn.execute(f'INSERT INTO _new.main.{name} SELECT * FROM _src.main.{name}{clause}')
            before, after = _table_fingerprint(conn, '_src', table), _table_fingerprint(conn, '_new', table)
            if before != after:
                raise CompactionError(
                    f'rebuilt table {table!r} does not match its source: {before[0]:,} rows before, '
                    f'{after[0]:,} after (content checksum {"matches" if before[1:] == after[1:] else "differs"})')
            tables.append({'table': table, 'rows': before[0], 'ordered_by': ordering})

        expected, rebuilt = _catalogue(conn, '_src', dropped), _catalogue(conn, '_new')
        if expected != rebuilt:
            changed = sorted(key for key in expected if expected[key] != rebuilt[key])
            raise CompactionError(f'rebuilt catalogue differs from the source in: {", ".join(changed)}')

        conn.execute('DETACH _new')             # flushes and closes the rebuilt file
        attached_target = False
        if _wal_path(staged).exists():
            raise CompactionError('the rebuilt file still has a write-ahead log after detach')
        if _wal_path(source).exists():
            raise CompactionError('a write-ahead log appeared beside the source during the rebuild')

        # The swap, with the source's shared lock still held: writers cannot
        # have opened the old file since `locked_at`, and the next open of this
        # path gets the rebuilt one.
        kept: Optional[Path] = None
        if keep_old:
            stamp = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
            kept = source.with_name(f'{source.name}{OLD_FILE_INFIX}{stamp}')
            os.link(source, kept)
        os.replace(staged, source)
        swapped = True
        handle = os.open(source.parent, os.O_RDONLY)
        try:
            os.fsync(handle)
        finally:
            os.close(handle)
        lock_seconds = time.monotonic() - locked_at
    finally:
        for attached, alias in ((attached_target, '_new'), (attached_source, '_src')):
            if attached:
                try:
                    conn.execute(f'DETACH {alias}')
                except duckdb.Error as ex:      # pragma: no cover - best effort on the way out
                    logger.warning('db-compact: detach %s failed: %s', alias, ex)
        conn.close()
        shutil.rmtree(scratch, ignore_errors=True)
        if not swapped:
            staged.unlink(missing_ok=True)
            _wal_path(staged).unlink(missing_ok=True)

    summary = {
        'path': str(source), 'bytes_before': bytes_before, 'bytes_after': source.stat().st_size,
        'tables': tables, 'dropped_indexes': dropped, 'old_file': str(kept) if kept else None,
        'lock_seconds': round(lock_seconds, 1), 'seconds': round(time.monotonic() - started, 1),
    }
    logger.info('db-compact: %s %.0f MB -> %.0f MB (%d tables, writers held out %.1fs)',
                source.name, bytes_before / 1e6, summary['bytes_after'] / 1e6, len(tables), lock_seconds)
    return summary
