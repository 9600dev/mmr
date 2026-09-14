#!/usr/bin/env python3
"""Install the existing pre-commit gates and MMR's native credential push guard."""

from __future__ import annotations

import pathlib
import subprocess
import sys


PUSH_HOOK = """#!/bin/sh
# MMR dotenv guard; installed by scripts/install_git_hooks.py.
set -eu
repo_root=$(git rev-parse --show-toplevel)
exec python3 "$repo_root/scripts/env_guard.py" pre-push "$@"
"""


def install() -> None:
    configured = subprocess.run(
        ["git", "config", "--get", "core.hooksPath"], capture_output=True, check=False,
    )
    if configured.returncode != 1:
        raise RuntimeError("A custom core.hooksPath is configured or unreadable; integrate the guard there without replacing existing hooks.")
    root = pathlib.Path(subprocess.check_output(
        ["git", "rev-parse", "--show-toplevel"], text=True,
    ).strip())
    hooks = pathlib.Path(subprocess.check_output(
        ["git", "rev-parse", "--path-format=absolute", "--git-path", "hooks"], text=True,
    ).strip())
    push = hooks / "pre-push"
    if push.is_symlink() or (push.exists() and push.read_text() != PUSH_HOOK):
        raise RuntimeError("An unrelated pre-push hook exists; refusing to replace it.")
    subprocess.run(
        [sys.executable, "-m", "pre_commit", "install",
         "--hook-type", "pre-commit", "--hook-type", "commit-msg"],
        cwd=root, check=True,
    )
    hooks.mkdir(parents=True, exist_ok=True)
    push.write_text(PUSH_HOOK)
    push.chmod(0o755)
    print("Installed commit, commit-message and native credential push hooks.")


if __name__ == "__main__":
    try:
        install()
    except (OSError, RuntimeError, subprocess.CalledProcessError) as exc:
        print(f"Hook installation failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
