"""The skill helpers' exec-mode decision must not go stale when the stack starts.

Regression (2026-09-29): `_exec_mode()` cached its first answer for the life of
the process. A long-lived session that called a helper while Docker was down
cached 'host', and after `./docker.sh -g` every DuckDB-backed helper still read
the empty host stub — exec_mode() reported container_running=True beside
mode='host'.
"""
import importlib.util
from pathlib import Path

import pytest

HELPERS = Path(__file__).resolve().parents[1] / 'skills' / 'mmr-skill' / 'scripts' / 'mmr_helpers.py'


@pytest.fixture
def helpers(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location('mmr_helpers_under_test', HELPERS)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    marker = tmp_path / '.db_in_container_volume'
    marker.write_text('x')
    monkeypatch.setattr(mod, '_DB_MARKER', marker)
    for name in ('MMR_SKILL_EXEC', 'MMR_ALLOW_HOST_DB'):
        monkeypatch.delenv(name, raising=False)
    clock = {'t': 1000.0}
    monkeypatch.setattr(mod.time, 'monotonic', lambda: clock['t'])
    mod._exec_mode_cache = None
    mod._exec_mode_host_fallback_at = None
    return mod, clock, marker


def test_host_fallback_is_reprobed_once_the_container_comes_up(helpers, monkeypatch):
    mod, clock, _ = helpers
    running = {'v': False}
    monkeypatch.setattr(mod, '_container_running', lambda: running['v'])
    assert mod._exec_mode() == 'host'
    running['v'] = True
    assert mod._exec_mode() == 'host'            # inside the TTL: still cached
    clock['t'] += mod._HOST_FALLBACK_TTL_S
    assert mod._exec_mode() == 'container'       # re-probed, stack is up


def test_container_decision_stays_cached(helpers, monkeypatch):
    mod, clock, _ = helpers
    calls = []
    monkeypatch.setattr(mod, '_container_running', lambda: calls.append(1) or True)
    assert mod._exec_mode() == 'container'
    clock['t'] += 10 * mod._HOST_FALLBACK_TTL_S
    assert mod._exec_mode() == 'container'
    assert len(calls) == 1                       # no docker inspect per call


@pytest.mark.parametrize('env', [('MMR_SKILL_EXEC', 'host'), ('MMR_ALLOW_HOST_DB', '1')])
def test_deliberate_host_mode_is_never_reprobed(helpers, monkeypatch, env):
    mod, clock, _ = helpers
    monkeypatch.setenv(*env)
    monkeypatch.setattr(mod, '_container_running', lambda: True)
    assert mod._exec_mode() == 'host'
    clock['t'] += 10 * mod._HOST_FALLBACK_TTL_S
    assert mod._exec_mode() == 'host'


def test_no_marker_means_a_stable_host_install(helpers, monkeypatch):
    mod, clock, marker = helpers
    marker.unlink()
    monkeypatch.setattr(mod, '_container_running', lambda: True)
    assert mod._exec_mode() == 'host'
    clock['t'] += 10 * mod._HOST_FALLBACK_TTL_S
    assert mod._exec_mode() == 'host'
