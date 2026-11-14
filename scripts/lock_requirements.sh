#!/usr/bin/env bash
# Regenerate requirements.txt from uv.lock.
#
# requirements.txt is what the Docker image installs. It used to be a
# hand-maintained copy of pyproject.toml's dependency list with `>=` bounds,
# which went wrong in two ways:
#
#   * a runtime dependency added to pyproject.toml but not here produced an
#     image that built, went HEALTHY, and died on import (twice — see
#     docs/OPERATIONAL_STATE.md);
#   * `>=` bounds meant every image build resolved to whatever was newest that
#     day, while the test suite ran against uv.lock. On 2026-11-14 the running
#     container had duckdb 2.0 / pandas 3.1 / reactivex 6.1 / vectorbt 1.2
#     against a tested set of duckdb 1.4 / pandas 3.0 / reactivex 4.1 /
#     vectorbt 0.28 — four major-version jumps the tests had never seen.
#
# Now the file is an export of the lock (exact versions + hashes), so the image
# installs precisely what `uv run pytest` validated. tests/test_requirements_lock.py
# fails if the two drift.
#
# Usage:
#   scripts/lock_requirements.sh            # re-export from the current uv.lock
#   scripts/lock_requirements.sh --upgrade  # upgrade every package first
set -euo pipefail
cd "$(dirname "$0")/.."

# A calling environment's VIRTUAL_ENV / conda prefix redirects uv at ITS
# interpreter (see CLAUDE.md, "Driving uv from another Python environment").
unset VIRTUAL_ENV UV_PROJECT_ENVIRONMENT UV_RUN_RECURSION_DEPTH CONDA_PREFIX

if [ "${1:-}" = "--upgrade" ]; then
    uv lock --upgrade
else
    uv lock
fi
uv export --frozen --no-dev --no-emit-project --format requirements-txt -o requirements.txt >/dev/null
echo "requirements.txt: $(grep -cE '^[A-Za-z0-9_.-]+==' requirements.txt) pinned packages exported from uv.lock"
