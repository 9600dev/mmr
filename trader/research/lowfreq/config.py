"""Configuration and isolation for the low-frequency research harness (D03).

The harness reads ONLY the databases named by ``LOWFREQ_RESEARCH_CONFIG``
(default ``~/.local/share/mmr/research_dev/trader_research.yaml``), opens them
read-only, never writes ``backtest_runs`` and makes no network calls. Everything
that touches a database goes through this module so the isolation is one
decision rather than a convention every loader has to remember.

``LOWFREQ_RESEARCH_ROOT`` (default ``<repo>/research/lowfreq``) is where the
per-family registries, ledgers and results live (D02). Tests point it at a
temporary directory.
"""
from __future__ import annotations

import contextlib
import os
from pathlib import Path
from typing import Any, Dict, Iterator

import duckdb
import yaml

ENV_CONFIG = 'LOWFREQ_RESEARCH_CONFIG'
ENV_ROOT = 'LOWFREQ_RESEARCH_ROOT'
DEFAULT_CONFIG = '~/.local/share/mmr/research_dev/trader_research.yaml'


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def research_config_path() -> Path:
    return Path(os.path.expanduser(os.environ.get(ENV_CONFIG) or DEFAULT_CONFIG))


def load_research_config() -> Dict[str, Any]:
    """The YAML named by LOWFREQ_RESEARCH_CONFIG. Fails loudly when absent."""
    path = research_config_path()
    if not path.exists():
        raise FileNotFoundError(
            f'{ENV_CONFIG} points at {path}, which does not exist; the harness '
            f'reads only the databases named there (D03)')
    with open(path, 'r') as fh:
        cfg = yaml.safe_load(fh) or {}
    if not isinstance(cfg, dict):
        raise ValueError(f'{path} is not a mapping')
    return cfg


def research_db_paths() -> Dict[str, Path]:
    """``{'main': <duckdb_path>, 'history': <history_duckdb_path>}`` (D03)."""
    cfg = load_research_config()
    out: Dict[str, Path] = {}
    for key, name in (('duckdb_path', 'main'), ('history_duckdb_path', 'history')):
        raw = cfg.get(key)
        if not raw:
            raise ValueError(f'{research_config_path()} has no {key}')
        out[name] = Path(os.path.expanduser(str(raw)))
    return out


def research_root() -> Path:
    return Path(os.path.expanduser(os.environ.get(ENV_ROOT) or str(repo_root() / 'research' / 'lowfreq')))


def connect_read_only(path: Path) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB file read-only. A missing file is an error, never created."""
    if not path.exists():
        raise FileNotFoundError(f'research database {path} does not exist')
    return duckdb.connect(str(path), read_only=True)


@contextlib.contextmanager
def read_only_duckdb_store() -> Iterator[None]:
    """Force every ``DuckDBConnection`` opened inside the block to be read-only.

    The point-in-time and fundamentals loaders live in ``scripts/`` and go
    through ``trader.data.duckdb_store.DuckDBConnection``, which opens files
    read-write. Wrapping them rather than copying them keeps one membership
    rule in the codebase (D03 asks for read-only, not for a second loader).
    """
    from trader.data import duckdb_store

    original = duckdb_store.duckdb.connect

    def _ro_connect(path, *args, **kwargs):
        kwargs.pop('read_only', None)
        return original(path, *args, read_only=True, **kwargs)

    duckdb_store.duckdb.connect = _ro_connect  # type: ignore[assignment]
    try:
        yield
    finally:
        duckdb_store.duckdb.connect = original  # type: ignore[assignment]


@contextlib.contextmanager
def bound_container() -> Iterator[None]:
    """Point ``trader.container.Container`` at the research config for the block.

    ``scripts/pit_panel.py`` and ``scripts/fundamental_signals.py`` resolve
    their database paths through the Container singleton. Inside this block
    the singleton is (re)built from ``LOWFREQ_RESEARCH_CONFIG`` and its two
    database paths are asserted to be exactly the research ones; the previous
    singleton and ``TRADER_CONFIG`` are restored afterwards.
    """
    from trader.container import Container

    wanted = research_db_paths()
    cfg_path = str(research_config_path())
    prev_env = os.environ.get('TRADER_CONFIG')
    prev_instance = Container._instance
    os.environ['TRADER_CONFIG'] = cfg_path
    try:
        with Container._instance_lock:
            Container._instance = Container(cfg_path)
        cfg = Container.instance().config()
        for key, name in (('duckdb_path', 'main'), ('history_duckdb_path', 'history')):
            got = Path(os.path.expanduser(str(cfg.get(key, ''))))
            if got.resolve() != wanted[name].resolve():
                raise RuntimeError(
                    f'Container {key}={got} differs from research config '
                    f'{wanted[name]}; refusing (D03)')
        yield
    finally:
        if prev_env is None:
            os.environ.pop('TRADER_CONFIG', None)
        else:
            os.environ['TRADER_CONFIG'] = prev_env
        with Container._instance_lock:
            Container._instance = prev_instance
