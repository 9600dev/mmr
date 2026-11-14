"""requirements.txt (what the image installs) must equal uv.lock (what is tested).

The Docker image installs from requirements.txt; the test suite runs from
uv.lock. They were maintained separately, and on 2026-11-14 the running
container was four MAJOR versions ahead of the tested set (duckdb 1.4 -> 2.0,
reactivex 4.1 -> 6.1, vectorbt 0.28 -> 1.2, pandas 3.0 -> 3.1) because the
file carried `>=` bounds and each image build took whatever was newest. Twice
before that, a dependency added to pyproject.toml but not to requirements.txt
produced an image that went HEALTHY and died on import.

requirements.txt is now generated (scripts/lock_requirements.sh). These tests
make a hand edit, a forgotten re-export, or an unpinned line fail loudly.
"""

from __future__ import annotations

import pathlib
import re
import tomllib

REPO = pathlib.Path(__file__).resolve().parent.parent
_PIN = re.compile(r'^([A-Za-z0-9][A-Za-z0-9_.-]*)==([^\s;\\]+)')


def _norm(name: str) -> str:
    return re.sub(r'[-_.]+', '-', name).lower()


def _requirement_pins() -> dict[str, str]:
    pins: dict[str, str] = {}
    for line in (REPO / 'requirements.txt').read_text().splitlines():
        match = _PIN.match(line)
        if match:
            pins[_norm(match.group(1))] = match.group(2)
    return pins


def _lock_versions() -> dict[str, str]:
    lock = tomllib.loads((REPO / 'uv.lock').read_text())
    return {_norm(p['name']): p['version'] for p in lock['package'] if 'version' in p}


def _runtime_dependencies() -> set[str]:
    project = tomllib.loads((REPO / 'pyproject.toml').read_text())['project']
    names = set()
    for spec in project['dependencies']:
        names.add(_norm(re.split(r'[\s\[<>=!~;]', spec, maxsplit=1)[0]))
    return names


def test_requirements_is_fully_pinned_with_hashes():
    text = (REPO / 'requirements.txt').read_text()
    assert _requirement_pins(), 'requirements.txt pins nothing'
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith('#') or stripped.startswith('--hash='):
            continue
        assert _PIN.match(stripped), (
            f'requirements.txt has an unpinned line: {stripped!r} — the file is an export of '
            f'uv.lock; regenerate it with scripts/lock_requirements.sh')
    assert text.count('--hash=sha256:') >= len(_requirement_pins())


def test_every_pin_matches_the_lock():
    lock = _lock_versions()
    drift = {name: (version, lock.get(name)) for name, version in _requirement_pins().items()
             if lock.get(name) != version}
    assert not drift, (
        f'requirements.txt disagrees with uv.lock (requirements, lock): {drift} — '
        f'run scripts/lock_requirements.sh')


def test_every_runtime_dependency_is_in_the_image():
    missing = sorted(_runtime_dependencies() - set(_requirement_pins()))
    assert not missing, (
        f'declared in pyproject.toml but absent from requirements.txt: {missing} — the image '
        f'would build and then fail on import; run scripts/lock_requirements.sh')


def test_the_legacy_eventkit_distribution_is_not_locked():
    """`aeventkit` and `eventkit` both install the `eventkit` package and
    overwrite each other; install order then decides the slot layout."""
    lock = _lock_versions()
    assert 'aeventkit' in lock
    assert 'eventkit' not in lock
