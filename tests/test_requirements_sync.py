"""requirements.txt must not drift from pyproject.toml.

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


def test_requirements_has_no_extras_pyproject_does_not_declare():
    """The converse: something installed into the image but unknown to the
    package is either a forgotten dep or dead weight in every build."""
    extra = _requirements() - _pyproject_runtime_deps()
    assert not extra, (
        'these are in requirements.txt but not declared in pyproject.toml — '
        f'add them to the project deps or drop them: {sorted(extra)}'
    )
