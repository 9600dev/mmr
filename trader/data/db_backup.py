"""Consistent per-database snapshots, including the execution journal's WAL.

DuckDB COPY FROM DATABASE retries contention with short-lived writers; SQLite's
online backup API includes committed WAL transactions. A manifest records the
snapshot interval because separate databases do not share one transaction.
Only complete snapshots update latest or rotate old recovery points. Restore
requires broker reconciliation before opening authority can resume.

The container data directory is a named volume; backups are host-mounted so
these snapshots survive volume loss and image rebuilds.
"""
import datetime as dt
import hashlib
import json
import logging
import os
import random
import shutil
import sqlite3
import tempfile
import time
import uuid
from pathlib import Path
from typing import Optional

import duckdb

logger = logging.getLogger(__name__)

_SUFFIX = '_clean'   # only auto-snapshots carry this; manual backups are untouched by pruning
MANIFEST = 'manifest.json'
RESTORE_MARKER = 'BROKER_RECONCILIATION_REQUIRED.json'


def _utcnow() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _database_files(directory: Path) -> list[Path]:
    return sorted(path for path in directory.iterdir()
                  if path.is_file() and path.suffix in ('.duckdb', '.sqlite3'))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def _sync_directory(path: Path) -> None:
    handle = os.open(path, os.O_RDONLY)
    try:
        os.fsync(handle)
    finally:
        os.close(handle)


def _write_json(path: Path, value: dict) -> None:
    temporary = path.with_name(f'.{path.name}.{uuid.uuid4().hex}.tmp')
    try:
        with temporary.open('x') as destination:
            json.dump(value, destination, indent=2, sort_keys=True)
            destination.write('\n')
            destination.flush()
            os.fsync(destination.fileno())
        os.replace(temporary, path)
        _sync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def backup_sqlite_database(src_path: str, dst_path: str, timeout: float = 30.0) -> int:
    """Copy committed WAL transactions through SQLite's online backup API.

    Copying just the .sqlite3 file loses commits still in -wal. Copying the
    files independently can tear a checkpoint. The backup API yields one
    consistent database image and requires neither sidecar in the snapshot.
    """
    src = Path(src_path).resolve(strict=True)
    dst = Path(dst_path)
    if src == dst.resolve():
        raise ValueError('backup destination must differ from its source')
    dst.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + timeout

    def progress(_status, _remaining, _total):
        if time.monotonic() >= deadline:
            raise TimeoutError(f'SQLite backup did not finish within {timeout:g}s')

    with tempfile.TemporaryDirectory(prefix='.sqlite-backup-', dir=dst.parent) as staging:
        staged = Path(staging) / dst.name
        source = sqlite3.connect(src.as_uri() + '?mode=ro', uri=True, timeout=timeout)
        target = sqlite3.connect(str(staged), timeout=timeout)
        try:
            source.backup(target, pages=256, progress=progress, sleep=.05)
            # The source header may carry WAL mode. Make the artifact a
            # standalone file even when a read-only verifier opens it later;
            # ExecutionJournal explicitly enables WAL again after restore.
            target.execute('PRAGMA journal_mode=DELETE')
            if target.execute('PRAGMA quick_check').fetchone() != ('ok',):
                raise RuntimeError(f'SQLite snapshot failed quick_check: {src.name}')
        finally:
            target.close()
            source.close()
        os.replace(staged, dst)
    return dst.stat().st_size


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
    if src.resolve() == dst.resolve():
        raise ValueError('backup destination must differ from its source')
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


def _update_latest(backup_dir: Path, target: Path) -> bool:
    link = backup_dir / 'latest'
    temporary = backup_dir / f'.latest-{uuid.uuid4().hex}'
    try:
        temporary.symlink_to(target.name)
        os.replace(temporary, link)
        _sync_directory(backup_dir)
        return True
    except Exception as ex:
        logger.warning('db-backup: latest symlink update failed: %s', ex)
        return False
    finally:
        temporary.unlink(missing_ok=True)


def _is_complete_snapshot(directory: Path) -> bool:
    try:
        manifest = json.loads((directory / MANIFEST).read_text())
        return (manifest.get('complete') is True
                and bool(manifest.get('databases'))
                and all(item.get('ok') is True
                        and isinstance(item.get('db'), str)
                        and Path(item['db']).name == item['db']
                        and (directory / item['db']).is_file()
                        and (directory / item['db']).stat().st_size == item.get('bytes')
                        for item in manifest['databases']))
    except (OSError, ValueError, TypeError, AttributeError):
        return False


def _auto_snapshots(backup_dir: Path) -> list[Path]:
    return sorted((path for path in backup_dir.glob(f'*{_SUFFIX}')
                   if path.is_dir() and not path.is_symlink()
                   and _is_complete_snapshot(path)
                   and not json.loads((path / MANIFEST).read_text()).get('manual', False)),
                  key=lambda path: path.name)


def _prune(backup_dir: Path, keep: int) -> list:
    """Keep the newest *keep* auto-snapshots (``*_clean``); delete older ones.
    Manual backups (without the suffix) are never touched."""
    if keep <= 0:
        return []
    # Failed/legacy snapshots never count towards retention: a partial copy
    # must not evict the last complete recovery point.
    snaps = _auto_snapshots(backup_dir)
    pruned = []
    for old in snaps[:-keep]:
        try:
            shutil.rmtree(old)
            pruned.append(old.name)
            logger.info('db-backup: pruned old snapshot %s', old.name)
        except Exception as ex:
            logger.warning('db-backup: prune failed for %s: %s', old, ex)
    return pruned


def run_backup(data_dir, backup_dir, keep: int = 7,
               timestamp: Optional[str] = None, name: Optional[str] = None) -> dict:
    """Snapshot every DuckDB and SQLite execution journal into a new directory.

    Default (auto) mode: a timestamped ``<ts>_clean`` dir, updates the ``latest``
    symlink, and prunes to the newest *keep*. If *name* is given it's a MANUAL
    milestone backup: the dir is exactly *name* (no ``_clean`` suffix, so rotation
    never deletes it, and ``latest`` is left pointing at the newest auto-snapshot).

    Best-effort per database; ``ok`` means some data was captured, ``complete``
    means all expected databases were captured. Only complete snapshots update
    latest or rotate recovery points. Snapshots are consistent PER DATABASE,
    not one cross-engine transaction; every restore requires broker
    reconciliation of the manifest's snapshot interval and subsequent gap.
    """
    data_dir = Path(os.path.expanduser(str(data_dir)))
    backup_dir = Path(os.path.expanduser(str(backup_dir)))
    databases = _database_files(data_dir)
    started_at = _utcnow()
    manual = bool(name)
    if manual:
        if name in ('.', '..') or Path(name).name != name:
            raise ValueError('backup name must be a single directory name')
        dst_dir = backup_dir / name
    else:
        ts = timestamp or dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H-%M-%S-%f')
        if ts in ('.', '..') or Path(ts).name != ts:
            raise ValueError('backup timestamp must be a single directory name')
        dst_dir = backup_dir / f'{ts}{_SUFFIX}'
    dst_dir.mkdir(parents=True, exist_ok=False)

    results = []
    for db in databases:
        record = {'db': db.name, 'engine': 'sqlite' if db.suffix == '.sqlite3' else 'duckdb',
                  'started_at': _utcnow()}
        try:
            backup_fn = backup_sqlite_database if db.suffix == '.sqlite3' else backup_database
            n = backup_fn(str(db), str(dst_dir / db.name))
            record.update(bytes=n, ok=True, sha256=_sha256(dst_dir / db.name))
            logger.info('db-backup: %s → %s (%.0f MB)', db.name, dst_dir / db.name, n / 1e6)
        except Exception as ex:
            record.update(ok=False, error=str(ex))
            logger.error('db-backup FAILED for %s: %s', db.name, ex)
        record['completed_at'] = _utcnow()
        results.append(record)

    ok_any = any(r['ok'] for r in results)
    inventory_stable = [db.name for db in databases] == [db.name for db in _database_files(data_dir)]
    complete = bool(results) and all(r['ok'] for r in results) and inventory_stable
    manifest = {
        'version': 1, 'snapshot_id': dst_dir.name, 'started_at': started_at,
        'completed_at': _utcnow(), 'databases': results, 'complete': complete,
        'inventory_stable': inventory_stable, 'consistency': 'per_database', 'manual': manual,
        'broker_reconciliation_required': True,
    }
    _write_json(dst_dir / MANIFEST, manifest)
    pruned = []
    latest_updated = False
    if complete and not manual:
        # Timestamp overrides may be older than the existing recovery point.
        # Point latest at the newest complete snapshot before pruning anything.
        latest_updated = _update_latest(backup_dir, _auto_snapshots(backup_dir)[-1])
        if latest_updated:
            pruned = _prune(backup_dir, keep)

    return {
        'timestamp': dst_dir.name,
        'dir': str(dst_dir) if ok_any else None,
        'databases': results,
        'ok': ok_any,
        'complete': complete,
        'manifest': str(dst_dir / MANIFEST),
        'latest_updated': latest_updated,
        'kept': keep,
        'pruned': pruned,
    }


def restore_snapshot(snapshot_dir, data_dir) -> dict:
    """Restore a verified complete snapshot into an empty database directory.

    This does not authorize trading. The marker survives restore and service
    restarts until the operator/runtime reconciles against complete broker
    truth. Each source file and staged copy is checked before publication.
    """
    source = Path(snapshot_dir).resolve(strict=True)
    destination = Path(data_dir)
    manifest = json.loads((source / MANIFEST).read_text())
    if (manifest.get('complete') is not True or manifest.get('version') != 1
            or not manifest.get('databases')
            or not all(item.get('ok') is True for item in manifest['databases'])):
        raise ValueError('restore requires a complete, versioned backup manifest')
    destination.mkdir(parents=True, exist_ok=True)
    if _database_files(destination) or (destination / RESTORE_MARKER).exists():
        raise FileExistsError('restore refuses to overwrite an existing database or recovery marker')
    names = []
    for item in manifest['databases']:
        name = item['db']
        if (not isinstance(name, str) or Path(name).name != name
                or Path(name).suffix not in ('.duckdb', '.sqlite3') or name in names):
            raise ValueError('backup manifest contains an invalid database identity')
        if _sha256(source / name) != item['sha256']:
            raise ValueError(f'backup checksum mismatch: {name}')
        names.append(name)
    if set(names) != {path.name for path in _database_files(source)}:
        raise ValueError('backup database inventory does not match its manifest')
    marker = {
        'version': 1, 'snapshot_id': manifest['snapshot_id'], 'restored_at': _utcnow(),
        'snapshot_started_at': manifest['started_at'],
        'snapshot_completed_at': manifest['completed_at'],
        'status': 'BROKER_RECONCILIATION_REQUIRED',
        'reason': 'Per-database snapshot interval and post-snapshot broker activity must be reconciled.',
    }
    published = []
    with tempfile.TemporaryDirectory(prefix='.restore-', dir=destination) as staging:
        staging = Path(staging)
        for item in manifest['databases']:
            staged = staging / item['db']
            shutil.copyfile(source / item['db'], staged)
            if _sha256(staged) != item['sha256']:
                raise ValueError(f'restored checksum mismatch: {item["db"]}')
            with staged.open('rb') as handle:
                os.fsync(handle.fileno())
        try:
            _write_json(destination / RESTORE_MARKER, marker)
            for name in names:
                # link refuses a concurrent/pre-existing destination; replace
                # would silently overwrite a database created during restore.
                os.link(staging / name, destination / name)
                published.append(destination / name)
            _sync_directory(destination)
        except BaseException:
            for path in published:
                path.unlink(missing_ok=True)
            (destination / RESTORE_MARKER).unlink(missing_ok=True)
            raise
    return marker


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    backup = commands.add_parser('backup')
    backup.add_argument('data_dir')
    backup.add_argument('backup_dir')
    backup.add_argument('--name')
    backup.add_argument('--keep', type=int, default=7)
    restore = commands.add_parser('restore')
    restore.add_argument('snapshot_dir')
    restore.add_argument('data_dir')
    args = parser.parse_args()
    if args.command == 'backup':
        summary = run_backup(args.data_dir, args.backup_dir, name=args.name, keep=args.keep)
        print(json.dumps(summary, indent=2))
        raise SystemExit(0 if summary['complete'] else 1)
    print(json.dumps(restore_snapshot(args.snapshot_dir, args.data_dir), indent=2))
