"""Host-side DB-backed commands run inside the container (2026-11-05 finding).

On a Docker install the live DuckDB is in the container volume; from the host,
`portfolio-diff` never persisted its baseline, `portfolio-risk` silently lost
groups/correlations, `session`/`group` failed. The host CLI now re-executes
those commands inside the running container.
"""
import os
import sys
from types import SimpleNamespace

import pytest

import trader.mmr_cli as cli


@pytest.fixture
def shadowed(tmp_path, monkeypatch):
    data = tmp_path / 'data'
    data.mkdir()
    (data / '.db_in_container_volume').write_text('marker')
    cfg = {'duckdb_path': str(data / 'mmr.duckdb')}
    from trader import container as container_mod
    monkeypatch.setattr(container_mod.Container, 'instance',
                        classmethod(lambda cls: SimpleNamespace(config=lambda: cfg)))
    monkeypatch.delenv('MMR_ALLOW_HOST_DB', raising=False)
    monkeypatch.delenv('MMR_NO_CONTAINER_PROXY', raising=False)
    monkeypatch.setattr(os.path, 'exists',
                        lambda p, _orig=os.path.exists: False if p == '/.dockerenv' else _orig(p))
    import shutil, subprocess
    monkeypatch.setattr(shutil, 'which', lambda name: '/usr/bin/docker' if name == 'docker' else None)
    monkeypatch.setattr(subprocess, 'run', lambda *a, **k: SimpleNamespace(stdout='abc123\n'))
    calls = []
    monkeypatch.setattr(os, 'execvp', lambda f, argv: calls.append(argv))
    monkeypatch.setattr(sys, 'argv', ['mmr', '--json', 'portfolio-diff'])
    return calls


def test_db_backed_command_is_proxied(shadowed):
    cli._maybe_proxy_to_container('portfolio-diff')
    assert shadowed, 'expected exec into the container'
    argv = shadowed[0]
    assert argv[:2] == ['/usr/bin/docker', 'exec']
    assert argv[-3:] == ['mmr', '--json', 'portfolio-diff']
    assert 'mmr-mmr-1' in argv and '-u' in argv


def test_rpc_only_command_not_proxied(shadowed):
    cli._maybe_proxy_to_container('status')
    cli._maybe_proxy_to_container('market-hours')
    assert shadowed == []


def test_opt_out_env(shadowed, monkeypatch):
    monkeypatch.setenv('MMR_NO_CONTAINER_PROXY', '1')
    cli._maybe_proxy_to_container('session')
    assert shadowed == []


def test_container_not_running_falls_through(shadowed, monkeypatch):
    import subprocess
    monkeypatch.setattr(subprocess, 'run', lambda *a, **k: SimpleNamespace(stdout=''))
    cli._maybe_proxy_to_container('session')
    assert shadowed == []


def test_not_shadowed_falls_through(shadowed, tmp_path):
    (tmp_path / 'data' / '.db_in_container_volume').unlink()
    cli._maybe_proxy_to_container('session')
    assert shadowed == []
