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

The hook has a second job. A commit that REMOVES spec lines is a revision, not
an addition, and must carry the four-point protocol from
tests/invariants/README.md in its message: the counterexample, a falsification
the reviewer can run, what is still caught, and one sentence naming what is now
allowed. Pure additions never trip it. Override:  ALLOW_SPEC_REVISION=1.

Why the message rather than a separate document: the evidence then lives in the
history beside the change it justifies, and is still there when someone asks in
a year why a property looks weaker than it reads.

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
import pathlib
import subprocess
import sys

SPEC_PREFIX = "tests/invariants/"
IMPL_PREFIX = "trader/"

# The four-point protocol (tests/invariants/README.md). A commit that REMOVES
# spec lines must carry the evidence a reviewer needs, in the message, where it
# survives in the history next to the change it justifies.
PROTOCOL_MARKERS = ("Counterexample:", "Falsification:", "Still caught:",
                    "Newly allowed:")


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


def removed_spec_lines() -> dict[str, list[str]]:
    """Substantive lines deleted from spec files, per file.

    Comments, blank lines and docstring prose are ignored: rewording a rationale
    is not revising a property. What counts is losing an assertion, a test, a
    property decorator, or a generator bound.
    """
    out = subprocess.run(
        ["git", "diff", "--cached", "--unified=0", "--", SPEC_PREFIX],
        capture_output=True, text=True, check=True,
    ).stdout
    removed: dict[str, list[str]] = {}
    current = ""
    for line in out.splitlines():
        if line.startswith("+++ b/"):
            current = line[6:]
        elif line.startswith("-") and not line.startswith("---"):
            body = line[1:].strip()
            if not body or body.startswith("#"):
                continue
            if any(k in body for k in
                   ("assert", "def test", "@given", "@settings", "@_SETTINGS",
                    "st.", "assume(", "xfail")):
                removed.setdefault(current, []).append(body)
    return removed


def main() -> int:
    if os.environ.get("ALLOW_INVARIANTS_IMPL") == "1":
        return 0

    # A REVISION (spec lines removed) must carry its evidence. A pure ADDITION
    # never trips this: adding properties is always allowed and always cheap to
    # review. Only taking one away needs the argument, because that is the act
    # the guard exists to make visible.
    removed = removed_spec_lines()
    if removed and os.environ.get("ALLOW_SPEC_REVISION") != "1":
        message = ""
        for path in (".git/MERGE_MSG", ".git/COMMIT_EDITMSG"):
            try:
                message = pathlib.Path(path).read_text()
                break
            except OSError:
                continue
        missing = [m for m in PROTOCOL_MARKERS if m not in message]
        if missing:
            print("spec-protection guard FAILED — this commit REMOVES properties "
                  "from the human-owned spec:\n", file=sys.stderr)
            for path, lines in removed.items():
                for ln in lines[:4]:
                    print(f"  {path}: {ln}", file=sys.stderr)
            print("\nRevising a property requires the four-point protocol "
                  "(tests/invariants/README.md). The commit message is missing:",
                  file=sys.stderr)
            for m in missing:
                print(f"  {m}", file=sys.stderr)
            print("\nSupply all four. Point 4 is the one a reviewer judges: state "
                  "in one sentence what is now allowed that was not. If you cannot "
                  "produce point 2 (a falsification the reviewer can RUN), this is "
                  "not a revision, it is a weakening, and the answer is to fix the "
                  "implementation instead.\n"
                  "Deliberate override: ALLOW_SPEC_REVISION=1 git commit ...",
                  file=sys.stderr)
            return 1

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
