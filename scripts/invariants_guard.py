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

SCOPE, honestly stated: this is review hygiene, not a security control. It
enforces exactly one thing — spec and implementation do not move in the SAME
commit — so the spec change is reviewable on its own. It does not stop two
sequential commits, it does not survive `git commit --no-verify`, and
ALLOW_INVARIANTS_IMPL is settable by anything that can run git. Its value is
that weakening a property becomes a separate, visible, reviewable act rather
than a line buried in an implementation diff.
"""
from __future__ import annotations

import os
import subprocess
import sys

SPEC_PREFIX = "tests/invariants/"
IMPL_PREFIX = "trader/"


def staged_files() -> list[str]:
    # ACMRD — the D matters. With the previous ACMR filter a staged DELETION was
    # invisible, so `git rm tests/invariants/test_x.py` plus an implementation
    # change sailed through: the single most direct way to weaken the spec was
    # the one the guard could not see, even though the policy names deletion
    # explicitly ("agents may not weaken, loosen, or DELETE a property").
    out = subprocess.run(
        ["git", "diff", "--cached", "--name-only", "--diff-filter=ACMRD"],
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
