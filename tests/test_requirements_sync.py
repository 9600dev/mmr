"""requirements.txt must contain every runtime dependency pyproject.toml declares.

(requirements.txt is now generated from uv.lock — see scripts/lock_requirements.sh
and tests/test_requirements_lock.py; this file keeps the original guard.)

The container image installs from requirements.txt (Dockerfile line ~50, cached
as its own layer), while `uv`/`pip install -e .` resolve from pyproject.toml.
Two hand-maintained lists of the same thing, and the failure is silent in the
worst possible place: the image builds fine, the container reports HEALTHY, and
the service dies at first import of the missing module.

This has now happened twice — `deal` + `pydantic` (fixed in 233e074, after a
code sync ModuleNotFoundError'd at service start) and `pandas-market-calendars`
(2026-07-26, caught only because the deploy was verified by importing it inside
the running container). Twice is a pattern, so it becomes a test.
"""
from __future__ import annotations

import pathlib
import re
import tomllib

REPO = pathlib.Path(__file__).resolve().parent.parent


def _name(spec: str) -> str:
    """Bare distribution name: strip extras, version pins, and normalise the
    underscore/hyphen spelling PEP 503 treats as equivalent."""
    return re.split(r'[><=!~\[]', spec, maxsplit=1)[0].strip().lower().replace('_', '-')


def _pyproject_runtime_deps() -> set[str]:
    data = tomllib.loads((REPO / 'pyproject.toml').read_text())
    return {_name(d) for d in data['project']['dependencies']}


def _requirements() -> set[str]:
    out = set()
    for line in (REPO / 'requirements.txt').read_text().splitlines():
        line = line.strip()
        if not line or line.startswith('#') or line.startswith('-'):
            continue
        out.add(_name(line))
    return out


def test_every_runtime_dependency_is_in_the_image_requirements():
    """A pyproject dep absent here builds a HEALTHY container whose services
    crash on import. That is strictly worse than a build failure."""
    missing = _pyproject_runtime_deps() - _requirements()
    assert not missing, (
        'these runtime dependencies are in pyproject.toml but NOT in '
        'requirements.txt, so the container image will not have them: '
        f'{sorted(missing)}'
    )


# The converse ("nothing in requirements.txt that pyproject does not declare")
# was asserted here while requirements.txt was a hand-maintained copy of the
# dependency list. Since 2026-11-14 the file is an EXPORT of uv.lock — the full
# pinned closure, transitive dependencies included — so that comparison no
# longer means anything. What replaced it is stronger: every line must be a pin
# that matches the lock exactly (tests/test_requirements_lock.py), so nothing
# can be in the image that the tested lock does not contain.
