"""Clean, consistent DuckDB backups to the host-mounted backups directory.

DuckDB has no ``sqlite3.Connection.backup()`` equivalent, so we use an in-memory
connection that ATTACHes the source ``READ_ONLY`` — which coexists with the live
short-lived writers without lock contention — and ``COPY FROM DATABASE`` into a
fresh target file. That yields a transactionally-consistent, *compacted* snapshot
even while the services are writing (a plain ``cp`` of a live DuckDB file can copy
a torn WAL frame).

Mirrors the horserank ``daily_pipeline`` pattern: snapshot every DB into a
timestamped dir, update a ``latest`` symlink, and prune to the newest N. The
``data/`` dir is a Docker named volume (not host-visible), but ``backups/`` is a
host bind-mount — so a snapshot written here lands on the host and survives volume
loss / rebuilds.
"""
import datetime as dt
import logging
import os
import random
import time
from pathlib import Path
from typing import Optional

import duckdb

logger = logging.getLogger(__name__)

_SUFFIX = '_clean'   # only auto-snapshots carry this; manual backups are untouched by pruning


def _sql_quote(path: str) -> str:
    return path.replace("'", "''")


def backup_database(src_path: str, dst_path: str, max_attempts: int = 8) -> int:
    """Write a clean, compacted snapshot of the DuckDB at *src_path* to *dst_path*.

    Safe against a live DB: an in-memory connection ATTACHes the source read-only
    and COPY FROM DATABASE writes a fresh target. The services use short-lived
    connections, so the file is usually unlocked — but if a writer briefly holds
    the file lock, the read-only ATTACH fails; we retry with backoff (matching the
    execute_atomic contention pattern) until it frees. Non-lock errors (missing /
    corrupt source) raise immediately. Returns bytes written."""
    src = Path(src_path)
    if not src.exists():
        raise FileNotFoundError(f'source DB not found: {src_path}')
    dst = Path(dst_path)
    dst.parent.mkdir(parents=True, exist_ok=True)

    last_err: Optional[Exception] = None
    for attempt in range(max_attempts):
        if dst.exists():
            dst.unlink()
        con = duckdb.connect()   # in-memory: takes no lock on either file
        try:
            con.execute(f"ATTACH '{_sql_quote(str(src))}' AS _src (READ_ONLY)")
            con.execute(f"ATTACH '{_sql_quote(str(dst))}' AS _bak")
            con.execute('COPY FROM DATABASE _src TO _bak')
            con.execute('DETACH _bak')
            con.execute('DETACH _src')
            return dst.stat().st_size
        except duckdb.Error as ex:
            last_err = ex
            if 'lock' not in str(ex).lower():
                raise   # not contention (corrupt/other) — fail fast
            time.sleep(min(2.0, 0.1 * (2 ** attempt)) + random.random() * 0.1)
        finally:
            con.close()
    if dst.exists():
        dst.unlink()
    raise last_err if last_err else RuntimeError('backup failed')


def _update_latest(backup_dir: Path, target: Path) -> None:
    link = backup_dir / 'latest'
    try:
        if link.is_symlink() or link.exists():
            link.unlink()
        link.symlink_to(target.name)   # relative → resolves within backup_dir on host + container
    except Exception as ex:
        logger.warning('db-backup: latest symlink update failed: %s', ex)


def _prune(backup_dir: Path, keep: int) -> list:
    """Keep the newest *keep* auto-snapshots (``*_clean``); delete older ones.
    Manual backups (without the suffix) are never touched."""
    if keep <= 0:
        return []
    snaps = sorted(backup_dir.glob(f'*{_SUFFIX}'), key=lambda p: p.name)
    pruned = []
    for old in snaps[:-keep]:
        try:
            for f in old.glob('*'):
                f.unlink()
            old.rmdir()
            pruned.append(old.name)
            logger.info('db-backup: pruned old snapshot %s', old.name)
        except Exception as ex:
            logger.warning('db-backup: prune failed for %s: %s', old, ex)
    return pruned


def run_backup(data_dir, backup_dir, keep: int = 7,
               timestamp: Optional[str] = None) -> dict:
    """Snapshot every ``*.duckdb`` in *data_dir* into a timestamped subdir of
    *backup_dir*, update the ``latest`` symlink, and prune to the newest *keep*.

    Best-effort per database — one DB failing does not abort the others. Returns
    a summary dict."""
    data_dir = Path(os.path.expanduser(str(data_dir)))
    backup_dir = Path(os.path.expanduser(str(backup_dir)))
    ts = timestamp or dt.datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%S')
    dst_dir = backup_dir / f'{ts}{_SUFFIX}'
    dst_dir.mkdir(parents=True, exist_ok=True)

    results = []
    for db in sorted(data_dir.glob('*.duckdb')):
        try:
            n = backup_database(str(db), str(dst_dir / db.name))
            results.append({'db': db.name, 'bytes': n, 'ok': True})
            logger.info('db-backup: %s → %s (%.0f MB)', db.name, dst_dir / db.name, n / 1e6)
        except Exception as ex:
            results.append({'db': db.name, 'ok': False, 'error': str(ex)})
            logger.error('db-backup FAILED for %s: %s', db.name, ex)

    ok_any = any(r['ok'] for r in results)
    pruned = []
    if ok_any:
        _update_latest(backup_dir, dst_dir)
        pruned = _prune(backup_dir, keep)
    else:
        try:
            dst_dir.rmdir()   # nothing written — don't leave an empty dir
        except Exception:
            pass

    return {
        'timestamp': ts,
        'dir': str(dst_dir) if ok_any else None,
        'databases': results,
        'ok': ok_any,
        'kept': keep,
        'pruned': pruned,
    }
