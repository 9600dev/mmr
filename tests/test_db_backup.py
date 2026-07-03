"""Clean DuckDB backup: consistency, latest symlink, keep-N rotation, best-effort."""
import duckdb
import pytest

from trader.data.db_backup import backup_database, run_backup


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
    import time
    src = tmp_path / 'live.duckdb'
    _make_db(src, [(1, 'x')])
    # Separate process: open RW, insert, hold the lock ~0.8s, then release.
    holder = subprocess.Popen([
        sys.executable, '-c',
        "import duckdb,time,sys; c=duckdb.connect(sys.argv[1]); "
        "c.execute(\"INSERT INTO t VALUES (2, 'y')\"); time.sleep(0.8); c.close()",
        str(src),
    ])
    try:
        time.sleep(0.2)                        # ensure the holder has the lock
        dst = tmp_path / 'live_bak.duckdb'
        backup_database(str(src), str(dst))    # retries until the holder frees it
        v = duckdb.connect(str(dst), read_only=True)
        assert v.execute('SELECT COUNT(*) FROM t').fetchone()[0] == 2
        v.close()
    finally:
        holder.wait(timeout=10)


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


def test_missing_source_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        backup_database(str(tmp_path / 'nope.duckdb'), str(tmp_path / 'out.duckdb'))
