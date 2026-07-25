#!/usr/bin/env python3
"""Type-check gate using `ty`, with per-scope baselines so an existing
codebase can adopt type-checking without a big-bang fix: the gate fails only
on diagnostics BEYOND the recorded baseline, so new code must be clean while
the pre-existing set is burned down per-file.

Two scopes, each gated against its own baseline file:

  * **kernel** (`trader/trading/`) — the enforced primary scope. The capability
    types that make "this order went through the gate" a type invariant live
    here; it is held at zero pre-existing diagnostics.
  * **advisory** (`trader/data/`, `trader/simulation/`) — widened coverage.
    Their current diagnostics are baselined (NOT mass-fixed); the gate still
    fails on any NEW diagnostic so the count can only ratchet down.

A new diagnostic ANYWHERE in either scope fails the gate.

Signature = "<relative-file>\t<severity>[<rule>]" (line/col dropped so moving
code within a file doesn't churn the baseline). A regression is any signature
whose count exceeds the baseline count, or any brand-new signature.

    python scripts/ty_gate.py            # check against baselines (CI/pre-commit)
    python scripts/ty_gate.py --update   # re-record the baselines (human-reviewed)

`ty` is a dev-only tool (never in the container); run via `uv run`.
"""
from __future__ import annotations

import argparse
import collections
import json
import pathlib
import re
import subprocess
import sys

REPO = pathlib.Path(__file__).resolve().parent.parent
LINE_RE = re.compile(r"^(?P<file>[^:]+):\d+:\d+: (?P<sev>error|warning)\[(?P<rule>[a-z-]+)\]")

# Each scope names its ty-checked dirs and a dedicated baseline file. The
# kernel is the primary enforced scope; advisory dirs are widened coverage
# whose existing diagnostics are baselined but never allowed to grow.
SCOPES: dict[str, dict[str, object]] = {
    "kernel": {
        "dirs": ["trader/trading/"],
        "baseline": REPO / "scripts" / "ty_baseline.json",
    },
    "advisory": {
        "dirs": ["trader/data/", "trader/simulation/"],
        "baseline": REPO / "scripts" / "ty_baseline_advisory.json",
    },
}


class GateError(RuntimeError):
    """The gate could not be evaluated. Never silently treated as a pass."""


# `ty` ends a concise run with one of these. Their ABSENCE means we are not
# looking at a completed scan, however parseable the rest of the output looked.
_SUMMARY_RE = re.compile(r"^(All checks passed!|Found \d+ diagnostics?)$")


def collect(dirs: list[str]) -> collections.Counter[str]:
    """Diagnostics per signature, or raise GateError if the scan didn't happen.

    FAIL CLOSED. This used to `return counts` unconditionally: because the
    parse simply finds nothing when `ty` is missing / crashes / renames a flag /
    changes its output format, a totally broken toolchain produced zero
    diagnostics, zero regressions, and a cheerful "ty gate OK" with exit 0 — on
    the repo's ONLY enforcement point. It also then reported the whole baseline
    as "fewer than baseline — consider --update", inviting a human to overwrite
    the baseline with nothing and destroy the ratchet permanently.
    """
    try:
        proc = subprocess.run(
            ["uv", "run", "ty", "check", *dirs, "--output-format", "concise", "--exit-zero"],
            cwd=REPO, capture_output=True, text=True,
        )
    except OSError as e:
        raise GateError(f"could not execute ty for {', '.join(dirs)}: {e}") from e

    out = proc.stdout + proc.stderr
    # --exit-zero means diagnostics do NOT set a non-zero status, so any
    # non-zero return is a tool/invocation failure, not a finding.
    if proc.returncode != 0:
        raise GateError(
            f"ty exited {proc.returncode} for {', '.join(dirs)} (with --exit-zero, "
            f"that means the tool failed, not that it found problems):\n{out.strip()}"
        )

    counts: collections.Counter[str] = collections.Counter()
    for line in out.splitlines():
        m = LINE_RE.match(line.strip())
        if m:
            counts[f"{m['file']}\t{m['sev']}[{m['rule']}]"] += 1

    # A completed scan always prints a summary line. Without one we cannot tell
    # "clean" from "never ran" — and those must not look the same.
    if not any(_SUMMARY_RE.match(ln.strip()) for ln in out.splitlines()):
        raise GateError(
            f"ty produced no recognisable summary line for {', '.join(dirs)} — "
            f"cannot distinguish a clean scan from a scan that never happened. "
            f"Output was:\n{out.strip() or '(empty)'}"
        )
    return counts


def load_baseline(path: pathlib.Path) -> collections.Counter[str]:
    if not path.exists():
        return collections.Counter()
    return collections.Counter(json.loads(path.read_text()))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--update", action="store_true", help="record current diagnostics as the baselines")
    args = ap.parse_args()

    failed = False
    total = 0
    for name, scope in SCOPES.items():
        dirs = scope["dirs"]  # type: ignore[assignment]
        baseline_path = scope["baseline"]  # type: ignore[assignment]
        try:
            current = collect(dirs)
        except GateError as e:
            # An unevaluable gate is a FAILED gate — including under --update,
            # where treating it as "zero diagnostics" would write an empty
            # baseline over a real one.
            print(f"ty gate FAILED [{name}] — could not evaluate:\n  {e}", file=sys.stderr)
            return 1
        total += sum(current.values())

        if args.update:
            baseline_path.write_text(json.dumps(dict(sorted(current.items())), indent=2) + "\n")
            print(f"ty baseline updated [{name}]: {sum(current.values())} diagnostics "
                  f"across {len(current)} signatures ({', '.join(dirs)})")
            continue

        baseline = load_baseline(baseline_path)
        regressions = {sig: (current[sig], baseline.get(sig, 0))
                       for sig in current if current[sig] > baseline.get(sig, 0)}
        if regressions:
            failed = True
            print(f"ty gate FAILED [{name}] — new type diagnostics beyond baseline:\n", file=sys.stderr)
            for sig, (now, was) in sorted(regressions.items()):
                f, rule = sig.split("\t")
                print(f"  {f}  {rule}  ({was} -> {now})", file=sys.stderr)
            print("\nFix the diagnostic, or if adopting cleanup run: python scripts/ty_gate.py --update",
                  file=sys.stderr)
            continue

        improved = sum(baseline.values()) - sum(min(current[s], baseline[s]) for s in baseline)
        msg = f"ty gate OK [{name}] — {sum(current.values())} diagnostics, none beyond baseline"
        if improved > 0:
            msg += f" ({improved} fewer than baseline — consider --update)"
        print(msg)

    if args.update:
        return 0
    if failed:
        return 1
    print(f"ty gate OK — {total} diagnostics total across all scopes, none beyond baseline")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
