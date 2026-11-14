"""`trader.data.db_compact`: measuring and reclaiming dead space in a DuckDB file.

The live history database was 28.6 GiB for 62 M live rows (2026-11-14): 95% of
its stored rows were dead, and an index no query used made up most of the rest.
The rebuild replaces the file the trading stack reads, so what is pinned here is
mostly what it must NOT do: lose a row, reuse an id, drop a constraint, or leave
the source changed when it gives up.
"""
from __future__ import annotations

import hashlib
import logging
import subprocess
import sys
import time
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from trader.data import db_compact
from trader.data.db_backup import _database_files
from trader.data.db_compact import CompactionError, compact_database, database_usage

INDEX = 'idx_tick_data_symbol_date'
ORDER = {'tick_data': ['symbol', 'bar_size', 'date']}


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def store(tmp_path):
    """A small database shaped like the real ones: an indexed tick table that
    has been upserted (so it holds dead rows), a keyed table fed by a sequence,
    and a view."""
    path = tmp_path / 'mmr_history.duckdb'
    con = duckdb.connect(str(path))
    con.execute("""CREATE TABLE tick_data(symbol VARCHAR NOT NULL, date TIMESTAMPTZ NOT NULL, open DOUBLE,
                   high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE, average DOUBLE, bar_count INTEGER,
                   bar_size VARCHAR, what_to_show INTEGER)""")
    con.execute(f'CREATE INDEX {INDEX} ON tick_data(symbol, date)')
    con.execute('CREATE SEQUENCE proposal_ids START 1')
    con.execute("""CREATE TABLE proposals(id INTEGER PRIMARY KEY DEFAULT nextval('proposal_ids'),
                   symbol VARCHAR NOT NULL, payload BLOB)""")
    con.execute('CREATE VIEW series AS SELECT symbol, bar_size, count(*) AS bars FROM tick_data GROUP BY 1, 2')
    # Arrival order interleaves symbols and bar sizes, as nightly increments do.
    con.execute("""INSERT INTO tick_data
                   SELECT CAST(9000 - (i % 7) * 13 AS VARCHAR),
                          TIMESTAMPTZ '2026-01-05 14:30:00+00' + INTERVAL (i // 7) MINUTE,
                          100.0, 101.0, 99.0,
                          CASE WHEN i % 97 = 0 THEN NULL ELSE 100 + (i % 500) / 10.0 END,
                          (i * 7919) % 100000, NULL, 0,
                          CASE WHEN i % 5 = 0 THEN '1 day' ELSE '1 min' END, 0
                   FROM range(21000) r(i)""")
    for symbol in ('AAA', 'BBB', 'CCC', 'DDD'):
        con.execute('INSERT INTO proposals(symbol, payload) VALUES (?, ?)', [symbol, symbol.encode() * 50])
    con.execute('DELETE FROM proposals WHERE id = 4')          # the highest id is gone; it must never come back
    con.execute("DELETE FROM tick_data WHERE symbol = '8961' AND bar_size = '1 min'")
    con.close()
    return path


def _rows(path: Path, table: str):
    con = duckdb.connect(str(path), read_only=True)
    try:
        columns = [row[0] for row in con.execute(
            'SELECT column_name FROM duckdb_columns() WHERE table_name = ? ORDER BY column_index', [table]).fetchall()]
        order = ', '.join(f'"{column}"' for column in columns)
        return con.execute(f'SELECT * FROM "{table}" ORDER BY {order}').fetchall()
    finally:
        con.close()


class TestUsageReport:
    def test_reports_live_and_stored_rows_without_changing_the_file(self, store):
        before = _sha(store)
        usage = database_usage(store)
        assert _sha(store) == before
        assert usage['file_bytes'] == store.stat().st_size
        assert usage['used_blocks'] + usage['free_blocks'] == usage['total_blocks']
        ticks = next(item for item in usage['tables'] if item['table'] == 'tick_data')
        assert ticks['live_rows'] == len(_rows(store, 'tick_data'))
        assert ticks['stored_rows'] >= ticks['live_rows']
        assert ticks['dead_rows'] == ticks['stored_rows'] - ticks['live_rows']
        assert usage['live_rows'] == sum(item['live_rows'] for item in usage['tables'])
        assert 0.0 <= usage['dead_fraction'] < 1.0
        assert usage['indexes'] and {'index': INDEX, 'table': 'tick_data'} in usage['indexes']

    def test_an_empty_database_is_not_a_division_by_zero(self, tmp_path):
        path = tmp_path / 'empty.duckdb'
        duckdb.connect(str(path)).close()
        usage = database_usage(path)
        assert usage['tables'] == [] and usage['dead_fraction'] == 0.0

    def test_a_missing_file_is_an_error_not_a_new_database(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            database_usage(tmp_path / 'nope.duckdb')
        assert not (tmp_path / 'nope.duckdb').exists()


class TestCompaction:
    def test_every_row_survives_and_the_file_is_replaced(self, store):
        ticks, proposals = _rows(store, 'tick_data'), _rows(store, 'proposals')
        summary = compact_database(store, order_by=ORDER, drop_indexes=[INDEX])
        assert _rows(store, 'tick_data') == ticks
        assert _rows(store, 'proposals') == proposals
        assert summary['bytes_after'] == store.stat().st_size
        assert {item['table']: item['rows'] for item in summary['tables']} == {
            'tick_data': len(ticks), 'proposals': len(proposals)}

    def test_ids_are_never_reused_and_keys_are_still_enforced(self, store):
        compact_database(store, order_by=ORDER, drop_indexes=[INDEX])
        con = duckdb.connect(str(store))
        try:
            con.execute("INSERT INTO proposals(symbol) VALUES ('NEW')")
            assert con.execute("SELECT id FROM proposals WHERE symbol = 'NEW'").fetchone() == (5,)
            with pytest.raises(duckdb.Error):
                con.execute("INSERT INTO proposals(id, symbol) VALUES (1, 'DUPLICATE')")
            with pytest.raises(duckdb.Error):
                con.execute("INSERT INTO tick_data(symbol, date) VALUES (NULL, now())")
            assert con.execute("SELECT count(*) FROM series").fetchone()[0] > 0      # the view came across
        finally:
            con.close()

    def test_only_the_named_index_is_dropped(self, store):
        con = duckdb.connect(str(store))
        con.execute('CREATE INDEX proposals_symbol ON proposals(symbol)')
        con.close()
        summary = compact_database(store, drop_indexes=[INDEX, 'never_existed'])
        assert summary['dropped_indexes'] == [INDEX]
        assert database_usage(store)['indexes'] == [{'index': 'proposals_symbol', 'table': 'proposals'}]

    def test_no_index_is_dropped_unless_named(self, store):
        compact_database(store)
        assert {'index': INDEX, 'table': 'tick_data'} in database_usage(store)['indexes']

    def test_rows_are_stored_in_the_requested_order(self, store):
        summary = compact_database(store, order_by=ORDER, drop_indexes=[INDEX])
        con = duckdb.connect(str(store), read_only=True)
        try:
            stored = con.execute('SELECT symbol, bar_size, date FROM tick_data ORDER BY rowid').fetchall()
        finally:
            con.close()
        assert stored == sorted(stored)
        assert next(item for item in summary['tables'] if item['table'] == 'tick_data')['ordered_by'] == ORDER['tick_data']

    def test_an_ordering_that_names_a_missing_column_is_not_applied(self, store):
        ticks = _rows(store, 'tick_data')
        summary = compact_database(store, order_by={'tick_data': ['symbol', 'no_such_column'], 'absent': ['x']})
        assert _rows(store, 'tick_data') == ticks
        assert all(item['ordered_by'] == [] for item in summary['tables'])

    def test_dead_rows_are_gone_afterwards(self, store):
        compact_database(store, order_by=ORDER, drop_indexes=[INDEX])
        usage = database_usage(store)
        assert usage['dead_fraction'] == 0.0
        assert all(item['dead_rows'] == 0 for item in usage['tables'])

    def test_the_store_works_on_the_rebuilt_file_and_does_not_recreate_the_index(self, store):
        from trader.data.duckdb_store import DuckDBDataStore

        compact_database(store, order_by=ORDER, drop_indexes=[INDEX])
        data = DuckDBDataStore(str(store))
        frame = pd.DataFrame({
            'date': pd.date_range('2026-11-13 14:30', periods=3, freq='min', tz='UTC'),
            'open': 1.0, 'high': 2.0, 'low': 0.5, 'close': [1.5, 1.6, 1.7], 'volume': 10.0, 'bar_size': '1 min'})
        data.write('424242', frame)
        data.write('424242', frame.assign(close=[2.5, 2.6, 2.7]))          # an upsert, not an append
        stored = data.read('424242', bar_size='1 min')
        assert list(stored['close']) == [2.5, 2.6, 2.7]
        assert database_usage(store)['indexes'] == []

    def test_only_duckdb_files_are_accepted(self, tmp_path):
        journal = tmp_path / 'mmr.duckdb.execution.sqlite3'
        journal.write_bytes(b'')
        with pytest.raises(ValueError):
            compact_database(journal)


class TestReplacedFile:
    def test_the_old_file_is_kept_under_a_name_the_backup_ignores(self, store):
        original = _sha(store)
        summary = compact_database(store, order_by=ORDER, drop_indexes=[INDEX])
        kept = Path(summary['old_file'])
        assert kept.parent == store.parent and kept.name.startswith(store.name + db_compact.OLD_FILE_INFIX)
        assert _sha(kept) == original
        assert _database_files(store.parent) == [store]            # one database, not two

    def test_the_old_file_can_be_discarded(self, store):
        summary = compact_database(store, keep_old=False)
        assert summary['old_file'] is None
        assert sorted(path.name for path in store.parent.iterdir()) == [store.name]

    def test_nothing_else_is_left_behind(self, store):
        summary = compact_database(store, order_by=ORDER, drop_indexes=[INDEX])
        assert sorted(path.name for path in store.parent.iterdir()) == sorted(
            [store.name, Path(summary['old_file']).name])


class TestGivingUpLeavesTheSourceAlone:
    def _assert_untouched(self, store, original):
        assert _sha(store) == original
        assert sorted(path.name for path in store.parent.iterdir()) == [store.name]

    def test_a_rebuild_that_does_not_verify_is_not_swapped_in(self, store, monkeypatch):
        compact_database(store, keep_old=False)                    # settle the file so its bytes are stable
        original = _sha(store)
        real = db_compact._table_fingerprint

        def lossy(conn, database, table):
            rows, xor, total = real(conn, database, table)
            return (rows - 1, xor, total) if database == '_new' and table == 'tick_data' else (rows, xor, total)

        monkeypatch.setattr(db_compact, '_table_fingerprint', lossy)
        with pytest.raises(CompactionError, match='tick_data'):
            compact_database(store, order_by=ORDER, drop_indexes=[INDEX])
        self._assert_untouched(store, original)

    def test_changed_content_with_the_same_row_count_is_caught(self, store, monkeypatch):
        compact_database(store, keep_old=False)
        original = _sha(store)
        real = db_compact._table_fingerprint

        def corrupt(conn, database, table):
            rows, xor, total = real(conn, database, table)
            return (rows, xor, (total or 0) + 1) if database == '_new' and table == 'proposals' else (rows, xor, total)

        monkeypatch.setattr(db_compact, '_table_fingerprint', corrupt)
        with pytest.raises(CompactionError, match='checksum differs'):
            compact_database(store)
        self._assert_untouched(store, original)

    def test_a_catalogue_that_lost_something_is_not_swapped_in(self, store, monkeypatch):
        compact_database(store, keep_old=False)
        original = _sha(store)
        real = db_compact._catalogue

        def forgetful(conn, database, ignore_indexes=()):
            found = real(conn, database, ignore_indexes)
            if database == '_new':
                found = dict(found, sequences=[])
            return found

        monkeypatch.setattr(db_compact, '_catalogue', forgetful)
        with pytest.raises(CompactionError, match='sequences'):
            compact_database(store)
        self._assert_untouched(store, original)

    def test_a_write_ahead_log_left_by_a_crashed_writer_stops_it(self, store, monkeypatch, tmp_path):
        compact_database(store, keep_old=False)
        original = _sha(store)
        leftover = tmp_path / 'elsewhere.wal'
        leftover.write_bytes(b'x')
        monkeypatch.setattr(db_compact, '_wal_path', lambda path: leftover)
        with pytest.raises(CompactionError, match='wal'):
            compact_database(store)
        assert _sha(store) == original

    def test_a_database_held_by_another_process_is_not_touched(self, store):
        compact_database(store, keep_old=False)
        original = _sha(store)
        holder = subprocess.Popen(
            [sys.executable, '-c',
             'import duckdb, sys, time\n'
             'con = duckdb.connect(sys.argv[1])\n'
             'print("held", flush=True)\n'
             'time.sleep(60)\n', str(store)],
            stdout=subprocess.PIPE, text=True)
        try:
            assert holder.stdout is not None and holder.stdout.readline().strip() == 'held'
            started = time.monotonic()
            with pytest.raises(CompactionError, match='locked'):
                compact_database(store, max_attempts=2)
            assert time.monotonic() - started < 10
        finally:
            holder.kill()
            holder.wait(timeout=10)
        self._assert_untouched(store, original)


# --------------------------------------------------------------------------
# `mmr data compact`
# --------------------------------------------------------------------------

def _run_compact(tmp_path, monkeypatch, capsys, **flags):
    """Run the real handler against ``tmp_path`` as the data directory."""
    import argparse
    import json
    from types import SimpleNamespace

    from trader import mmr_cli as cli
    from trader.container import Container

    monkeypatch.setattr(Container, 'instance', classmethod(
        lambda cls: SimpleNamespace(config=lambda: {'duckdb_path': str(tmp_path / 'mmr.duckdb')})))
    monkeypatch.setattr(cli, '_json_mode', True)
    monkeypatch.setattr(cli, '_exit_code', 0)
    arguments = {'apply': False, 'discard_old': False, 'db': None}
    arguments.update(flags)
    logging.disable(logging.CRITICAL)          # as the CLI entry point does: stdout is the JSON document only
    try:
        cli._handle_data_compact(argparse.Namespace(**arguments))
    finally:
        logging.disable(logging.NOTSET)
    return json.loads(capsys.readouterr().out), cli._exit_code


class TestCompactCommand:
    def test_the_default_is_a_report_that_changes_nothing(self, store, tmp_path, monkeypatch, capsys):
        before = _sha(store)
        document, code = _run_compact(tmp_path, monkeypatch, capsys)
        assert code == 0 and document['success'] is True and document['applied'] is False
        assert _sha(store) == before
        (row,) = document['data']
        assert row['db'] == store.name and row['legacy_indexes'] == [INDEX]
        assert row['worth_compacting'] is True and INDEX in row['reason']
        assert 'compacted' not in row

    def test_apply_rebuilds_drops_the_legacy_index_and_reports_what_it_kept(self, store, tmp_path, monkeypatch, capsys):
        ticks = _rows(store, 'tick_data')
        document, code = _run_compact(tmp_path, monkeypatch, capsys, apply=True)
        assert code == 0 and document['applied'] is True
        (row,) = document['data']
        assert row['compacted'] is True and row['dropped_indexes'] == [INDEX]
        assert row['rows_verified'] == len(ticks) + len(_rows(store, 'proposals'))
        assert document['kept_old_files'] == [row['old_file']] and Path(row['old_file']).exists()
        assert _rows(store, 'tick_data') == ticks
        assert database_usage(store)['indexes'] == []

    def test_a_file_with_nothing_to_reclaim_is_left_alone(self, store, tmp_path, monkeypatch, capsys):
        _run_compact(tmp_path, monkeypatch, capsys, apply=True, discard_old=True)
        settled = _sha(store)
        document, code = _run_compact(tmp_path, monkeypatch, capsys, apply=True)
        (row,) = document['data']
        assert code == 0 and row['worth_compacting'] is False and row['compacted'] is False
        assert _sha(store) == settled
        assert sorted(path.name for path in tmp_path.iterdir()) == [store.name]

    def test_discard_old_removes_the_replaced_file(self, store, tmp_path, monkeypatch, capsys):
        document, _ = _run_compact(tmp_path, monkeypatch, capsys, apply=True, discard_old=True)
        assert document['kept_old_files'] == []
        assert sorted(path.name for path in tmp_path.iterdir()) == [store.name]

    def test_an_unknown_database_name_is_refused_before_anything_runs(self, store, tmp_path, monkeypatch, capsys):
        before = _sha(store)
        document, code = _run_compact(tmp_path, monkeypatch, capsys, apply=True, db=['nope.duckdb'])
        assert document['success'] is False and 'nope.duckdb' in document['message'] and code == 1
        assert _sha(store) == before

    def test_a_failed_rebuild_is_reported_and_exits_non_zero(self, store, tmp_path, monkeypatch, capsys):
        before = _sha(store)

        def refuse(*args, **kwargs):
            raise CompactionError('database stayed locked by another process')

        monkeypatch.setattr(db_compact, 'compact_database', refuse)
        document, code = _run_compact(tmp_path, monkeypatch, capsys, apply=True)
        assert code == 1 and document['success'] is False
        assert 'locked' in document['errors'][0] and document['data'][0]['compacted'] is False
        assert _sha(store) == before

    def test_the_rebuild_plan_comes_from_the_store(self):
        from trader import mmr_cli as cli
        from trader.data.duckdb_store import DuckDBDataStore

        order_by, legacy = cli._compaction_plan()
        assert order_by == {'tick_data': ['symbol', 'bar_size', 'date']}
        assert legacy == [INDEX] == list(DuckDBDataStore.LEGACY_INDEXES)
