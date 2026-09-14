"""Complete recovery points include committed SQLite WAL and all DuckDBs."""
import json
from pathlib import Path
import sqlite3

import duckdb
import pytest

from trader.data.db_backup import (
    MANIFEST, RESTORE_MARKER, backup_database, backup_sqlite_database,
    restore_snapshot, run_backup,
)


def _make_db(path, rows):
    con = duckdb.connect(str(path))
    con.execute('CREATE TABLE t (id INTEGER, name VARCHAR)')
    con.executemany('INSERT INTO t VALUES (?, ?)', rows)
    con.close()


def test_backup_is_consistent_and_queryable(tmp_path):
    src = tmp_path / 'src.duckdb'
    _make_db(src, [(1, 'a'), (2, 'b'), (3, 'c')])
    dst = tmp_path / 'out' / 'src.duckdb'
    n = backup_database(str(src), str(dst))
    assert n > 0 and dst.exists()
    v = duckdb.connect(str(dst), read_only=True)
    assert v.execute('SELECT COUNT(*) FROM t').fetchone()[0] == 3
    assert v.execute('SELECT name FROM t WHERE id=2').fetchone()[0] == 'b'
    v.close()


def test_backup_retries_past_a_separate_process_writer(tmp_path):
    """The real production case: a SEPARATE process briefly holds the DB's file
    lock. backup_database must retry past the contention and still capture the
    data. (An in-process holder would hit DuckDB's same-process attach dedup,
    which is not the deployment scenario — the backup runs as its own process.)"""
    import subprocess
    import sys
    import selectors
    src = tmp_path / 'live.duckdb'
    _make_db(src, [(1, 'x')])
    # Separate process: open RW, insert, hold the lock ~0.8s, then release.
    holder = subprocess.Popen([
        sys.executable, '-c',
        "import duckdb,time,sys; c=duckdb.connect(sys.argv[1]); "
        "c.execute(\"INSERT INTO t VALUES (2, 'y')\"); "
        "print('LOCKED', flush=True); time.sleep(0.8); c.close()",
        str(src),
    ], stdout=subprocess.PIPE, text=True)
    try:
        # Import/process startup can exceed 200ms, especially on macOS.
        # Wait for the INSERT to commit while the child still holds the lock;
        # otherwise a correct backup can legitimately snapshot the first row.
        with selectors.DefaultSelector() as ready:
            ready.register(holder.stdout, selectors.EVENT_READ)
            assert ready.select(timeout=20), 'writer did not acquire its DB lock'
        assert holder.stdout.readline().strip() == 'LOCKED'
        dst = tmp_path / 'live_bak.duckdb'
        backup_database(str(src), str(dst))    # retries until the holder frees it
        v = duckdb.connect(str(dst), read_only=True)
        assert v.execute('SELECT COUNT(*) FROM t').fetchone()[0] == 2
        v.close()
    finally:
        try:
            holder.wait(timeout=10)
        except subprocess.TimeoutExpired:
            holder.kill()
            holder.wait(timeout=5)
        holder.stdout.close()


def test_run_backup_snapshots_all_dbs_and_symlink(tmp_path):
    data = tmp_path / 'data'; data.mkdir()
    backups = tmp_path / 'backups'
    _make_db(data / 'mmr.duckdb', [(1, 'a')])
    _make_db(data / 'mmr_history.duckdb', [(1, 'h')])
    (data / 'notes.txt').write_text('ignored')   # non-duckdb file is skipped

    r = run_backup(data, backups, keep=7, timestamp='2026-07-03T00-00-00')
    assert r['ok']
    assert {d['db'] for d in r['databases']} == {'mmr.duckdb', 'mmr_history.duckdb'}
    assert all(d['ok'] for d in r['databases'])
    snap = backups / '2026-07-03T00-00-00_clean'
    assert (snap / 'mmr.duckdb').exists() and (snap / 'mmr_history.duckdb').exists()
    # latest symlink points at the newest snapshot
    latest = backups / 'latest'
    assert latest.is_symlink() and (latest / 'mmr.duckdb').exists()


def test_keep_n_rotation(tmp_path):
    data = tmp_path / 'data'; data.mkdir()
    backups = tmp_path / 'backups'
    _make_db(data / 'mmr.duckdb', [(1, 'a')])
    for ts in ('2026-07-01T00-00-00', '2026-07-02T00-00-00',
               '2026-07-03T00-00-00', '2026-07-04T00-00-00'):
        run_backup(data, backups, keep=2, timestamp=ts)
    kept = sorted(p.name for p in backups.glob('*_clean'))
    assert kept == ['2026-07-03T00-00-00_clean', '2026-07-04T00-00-00_clean']
    assert (backups / 'latest' / 'mmr.duckdb').exists()   # symlink still valid


def test_manual_backups_not_pruned(tmp_path):
    data = tmp_path / 'data'; data.mkdir()
    backups = tmp_path / 'backups'; backups.mkdir()
    (backups / 'before_changes').mkdir()          # a manual backup (no _clean suffix)
    _make_db(data / 'mmr.duckdb', [(1, 'a')])
    for ts in ('2026-07-01T00-00-00', '2026-07-02T00-00-00', '2026-07-03T00-00-00'):
        run_backup(data, backups, keep=1, timestamp=ts)
    assert (backups / 'before_changes').exists()   # untouched
    assert len(list(backups.glob('*_clean'))) == 1


def test_best_effort_one_bad_db(tmp_path):
    data = tmp_path / 'data'; data.mkdir()
    backups = tmp_path / 'backups'
    _make_db(data / 'good.duckdb', [(1, 'a')])
    (data / 'corrupt.duckdb').write_bytes(b'not a duckdb file at all')
    r = run_backup(data, backups, keep=7, timestamp='2026-07-03T00-00-00')
    byname = {d['db']: d for d in r['databases']}
    assert byname['good.duckdb']['ok'] is True
    assert byname['corrupt.duckdb']['ok'] is False
    assert r['ok'] is True   # partial success — good.duckdb was captured
    assert r['complete'] is False
    assert not (backups / 'latest').exists()
    assert json.loads((backups / r['timestamp'] / MANIFEST).read_text())['complete'] is False


def test_named_manual_backup_not_rotated_and_keeps_latest(tmp_path):
    data = tmp_path / 'data'; data.mkdir()
    backups = tmp_path / 'backups'
    _make_db(data / 'mmr.duckdb', [(1, 'a')])
    # an auto snapshot first, so `latest` exists and points at it
    run_backup(data, backups, keep=7, timestamp='2026-07-03T00-00-00')
    auto_latest = (backups / 'latest').resolve().name
    # a named milestone backup
    r = run_backup(data, backups, name='before_upgrade')
    assert r['ok']
    assert (backups / 'before_upgrade' / 'mmr.duckdb').exists()
    # named dir has no _clean suffix → immune to rotation
    for ts in ('2026-07-04T00-00-00', '2026-07-05T00-00-00'):
        run_backup(data, backups, keep=1, timestamp=ts)
    assert (backups / 'before_upgrade').exists()
    # latest still tracks the newest AUTO snapshot, not the manual one
    assert (backups / 'latest').resolve().name == '2026-07-05T00-00-00_clean'


def test_missing_source_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        backup_database(str(tmp_path / 'nope.duckdb'), str(tmp_path / 'out.duckdb'))


def test_sqlite_backup_includes_committed_wal_without_checkpoint(tmp_path):
    source = tmp_path / 'mmr.duckdb.execution.sqlite3'
    writer = sqlite3.connect(source)
    try:
        writer.execute('PRAGMA journal_mode=WAL')
        writer.execute('PRAGMA wal_autocheckpoint=0')
        writer.execute('CREATE TABLE intents (id TEXT PRIMARY KEY)')
        writer.execute("INSERT INTO intents VALUES ('submitted-before-crash')")
        writer.commit()
        assert source.with_name(source.name + '-wal').stat().st_size > 0
        # This intentionally bypasses WAL replay to prove the main file alone
        # does not yet contain the transaction being tested.
        with sqlite3.connect(source.as_uri() + '?immutable=1', uri=True) as stale:
            assert stale.execute("SELECT name FROM sqlite_master WHERE name='intents'").fetchone() is None
        destination = tmp_path / 'snapshot.sqlite3'
        assert backup_sqlite_database(str(source), str(destination)) > 0
        with sqlite3.connect(destination) as restored:
            assert restored.execute('SELECT id FROM intents').fetchall() == [('submitted-before-crash',)]
        assert not destination.with_name(destination.name + '-wal').exists()
    finally:
        writer.close()


def test_snapshot_restore_carries_journal_and_reconciliation_marker(tmp_path):
    data = tmp_path / 'data'
    data.mkdir()
    _make_db(data / 'mmr.duckdb', [(1, 'a')])
    with sqlite3.connect(data / 'mmr.duckdb.execution.sqlite3') as writer:
        writer.execute('CREATE TABLE intents (id TEXT)')
        writer.execute("INSERT INTO intents VALUES ('unknown')")
    summary = run_backup(data, tmp_path / 'backups', name='recovery')
    assert summary['complete'] is True
    manifest = json.loads((Path(summary['dir']) / MANIFEST).read_text())
    assert {item['engine'] for item in manifest['databases']} == {'duckdb', 'sqlite'}
    assert manifest['consistency'] == 'per_database'
    restored = tmp_path / 'restored'
    marker = restore_snapshot(summary['dir'], restored)
    assert marker['status'] == 'BROKER_RECONCILIATION_REQUIRED'
    assert marker['snapshot_started_at'] == manifest['started_at']
    assert (restored / RESTORE_MARKER).exists()
    with sqlite3.connect(restored / 'mmr.duckdb.execution.sqlite3') as journal:
        assert journal.execute('SELECT id FROM intents').fetchall() == [('unknown',)]
    with pytest.raises(FileExistsError, match='overwrite'):
        restore_snapshot(summary['dir'], restored)


def test_partial_snapshot_never_evicts_last_complete_recovery_point(tmp_path):
    data = tmp_path / 'data'
    data.mkdir()
    backups = tmp_path / 'backups'
    _make_db(data / 'mmr.duckdb', [(1, 'a')])
    complete = run_backup(data, backups, keep=1, timestamp='2026-09-01')
    (data / 'journal.sqlite3').write_bytes(b'corrupt sqlite database')
    partial = run_backup(data, backups, keep=1, timestamp='2026-09-02')
    assert partial['ok'] is True and partial['complete'] is False
    assert (backups / 'latest').resolve() == Path(complete['dir'])
    assert Path(complete['dir']).is_dir()
    with pytest.raises(ValueError, match='complete'):
        restore_snapshot(partial['dir'], tmp_path / 'invalid_restore')


def test_restore_rejects_tampering_before_creating_any_database(tmp_path):
    data = tmp_path / 'data'
    data.mkdir()
    _make_db(data / 'mmr.duckdb', [(1, 'a')])
    summary = run_backup(data, tmp_path / 'backups', name='source')
    (Path(summary['dir']) / 'mmr.duckdb').write_bytes(b'corruption')
    destination = tmp_path / 'restored'
    with pytest.raises(ValueError, match='checksum'):
        restore_snapshot(summary['dir'], destination)
    assert not list(destination.iterdir())


def test_backup_refuses_to_overwrite_existing_recovery_point(tmp_path):
    data = tmp_path / 'data'
    data.mkdir()
    _make_db(data / 'mmr.duckdb', [(1, 'a')])
    run_backup(data, tmp_path / 'backups', name='protected')
    with pytest.raises(FileExistsError):
        run_backup(data, tmp_path / 'backups', name='protected')


def test_backup_refuses_to_overwrite_its_source(tmp_path):
    source = tmp_path / 'mmr.duckdb'
    _make_db(source, [(1, 'preserve')])
    with pytest.raises(ValueError, match='differ'):
        backup_database(str(source), str(source))
    connection = duckdb.connect(str(source), read_only=True)
    try:
        assert connection.execute('SELECT name FROM t').fetchone() == ('preserve',)
    finally:
        connection.close()


def test_failed_latest_update_preserves_previous_recovery_point(tmp_path, monkeypatch):
    from trader.data import db_backup
    data = tmp_path / 'data'
    data.mkdir()
    backups = tmp_path / 'backups'
    _make_db(data / 'mmr.duckdb', [(1, 'a')])
    first = run_backup(data, backups, keep=1, timestamp='2026-09-01')
    replace = db_backup.os.replace

    def fail_latest(source, destination):
        if Path(destination).name == 'latest':
            raise PermissionError('cannot replace latest')
        replace(source, destination)

    monkeypatch.setattr(db_backup.os, 'replace', fail_latest)
    second = run_backup(data, backups, keep=1, timestamp='2026-09-02')
    assert second['complete'] is True and second['latest_updated'] is False
    assert (backups / 'latest').resolve() == Path(first['dir'])
    assert Path(first['dir']).is_dir()


def test_manual_backup_with_auto_suffix_is_still_never_pruned(tmp_path):
    data = tmp_path / 'data'
    data.mkdir()
    backups = tmp_path / 'backups'
    _make_db(data / 'mmr.duckdb', [(1, 'a')])
    manual = run_backup(data, backups, name='1999_manual_clean')
    run_backup(data, backups, keep=1, timestamp='2026-09-01')
    run_backup(data, backups, keep=1, timestamp='2026-09-02')
    assert Path(manual['dir']).is_dir()


def test_backdated_snapshot_does_not_leave_latest_dangling(tmp_path):
    data = tmp_path / 'data'
    data.mkdir()
    backups = tmp_path / 'backups'
    _make_db(data / 'mmr.duckdb', [(1, 'a')])
    newest = run_backup(data, backups, keep=1, timestamp='2026-09-02')
    run_backup(data, backups, keep=1, timestamp='2026-09-01')
    assert (backups / 'latest').resolve() == Path(newest['dir'])
    assert (backups / 'latest' / 'mmr.duckdb').exists()
