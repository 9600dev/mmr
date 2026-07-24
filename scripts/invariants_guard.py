#!/usr/bin/env python3
"""Spec-protection guard (the "protect the spec from the agent" rule).

tests/invariants/ is the human-owned executable spec. If an agent can edit the
properties AND the implementation in one change, it can quietly weaken a
property to make its code pass and everything stays green — self-consistent
garbage. This hook refuses a commit that stages BOTH an invariants file and an
implementation file, forcing spec changes into their own separately-reviewed
commit.

Allowed together: invariants + docs/tests-config. Refused: invariants + any
trader/** implementation. Override for a genuine paired change (rare, and it
should be a deliberate human act) with:  ALLOW_INVARIANTS_IMPL=1 git commit ...
"""
from __future__ import annotations

import os
import subprocess
import sys

SPEC_PREFIX = "tests/invariants/"
IMPL_PREFIX = "trader/"


def staged_files() -> list[str]:
    out = subprocess.run(
        ["git", "diff", "--cached", "--name-only", "--diff-filter=ACMR"],
        capture_output=True, text=True, check=True,
    ).stdout
    return [ln.strip() for ln in out.splitlines() if ln.strip()]


def main() -> int:
    if os.environ.get("ALLOW_INVARIANTS_IMPL") == "1":
        return 0
    files = staged_files()
    spec = [f for f in files if f.startswith(SPEC_PREFIX)]
    impl = [f for f in files if f.startswith(IMPL_PREFIX)]
    if spec and impl:
        print("spec-protection guard FAILED — this commit stages BOTH the "
              "human-owned invariants spec and implementation:\n", file=sys.stderr)
        for f in spec:
            print(f"  spec:  {f}", file=sys.stderr)
        for f in impl:
            print(f"  impl:  {f}", file=sys.stderr)
        print("\nSplit them: commit the implementation, then the spec change on "
              "its own so a property can't be silently weakened to pass code in "
              "the same breath. Deliberate paired change: ALLOW_INVARIANTS_IMPL=1 "
              "git commit ...", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
