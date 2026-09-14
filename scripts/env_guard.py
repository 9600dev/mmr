#!/usr/bin/env python3
"""Refuse dotenv credentials in the index or history being pushed.

Only Git pathname metadata is read; file contents are never inspected or printed.
The native pre-push hook supplies every ref update on stdin. Checking full
reachable history also catches a credential file deleted before the branch tip.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys


def git(*args: str, data: bytes | None = None) -> bytes:
    result = subprocess.run(
        ["git", "--no-replace-objects", *args],
        input=data,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(f"Git {args[0]} failed; credential safety could not be checked.")
    return result.stdout


def forbidden_paths(raw: bytes) -> list[bytes]:
    return sorted({
        path
        for path in raw.split(b"\0")
        if any(
            part != b".env.example"
            and (part == b".env" or part.startswith(b".env."))
            for part in path.split(b"/")
        )
    })


def pushed_tips(raw: bytes) -> list[str]:
    tips = set()
    for line in raw.splitlines():
        fields = line.split()
        if len(fields) != 4:
            raise RuntimeError("Malformed pre-push ref update; refusing to push.")
        local_oid, remote_oid = fields[1], fields[3]
        if not all(re.fullmatch(rb"[0-9a-fA-F]{40}|[0-9a-fA-F]{64}", oid)
                   for oid in (local_oid, remote_oid)):
            raise RuntimeError("Malformed pre-push object ID; refusing to push.")
        if set(local_oid) == {ord("0")}:
            continue  # Deleting a remote ref sends no new history.
        # Peel annotated tags and refuse objects with no inspectable commit tree.
        commit = git("rev-parse", "--verify", local_oid.decode() + "^{commit}")
        tips.add(commit.strip().decode("ascii"))
    return sorted(tips)


def check(mode: str) -> int:
    if mode == "staged":
        paths = git("ls-files", "--cached", "-z")
    else:
        tips = pushed_tips(sys.stdin.buffer.read())
        if not tips:
            return 0
        if git("rev-parse", "--is-shallow-repository").strip() == b"true":
            raise RuntimeError("Full history is required for the credential guard; unshallow before pushing.")
        paths = git(
            "log", "--format=", "--name-only", "-z", "--root", "-m",
            "--full-history", "--no-renames", "--diff-filter=ACMT", "--stdin",
            data=("\n".join(tips) + "\n").encode("ascii"),
        )
    blocked = forbidden_paths(paths)
    if not blocked:
        return 0
    print("Blocked: .env credentials must not be committed or pushed.", file=sys.stderr)
    for path in blocked:
        print(f"  {os.fsdecode(path)!r}", file=sys.stderr)
    if mode == "staged":
        print("Remove these paths from the Git index while keeping your local files.", file=sys.stderr)
    else:
        print("Remove these paths from every affected commit; deleting them at HEAD is insufficient.", file=sys.stderr)
    print("Only .env.example is allowed, and it must contain placeholders.", file=sys.stderr)
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("staged", "pre-push"))
    parser.add_argument("remote_args", nargs="*")
    args = parser.parse_args()
    try:
        return check(args.mode)
    except (OSError, RuntimeError, UnicodeError) as exc:
        print(f"Credential guard failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
