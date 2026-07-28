#!/usr/bin/env python3
"""Symbolic-execution gate: run CrossHair over the deal-contracted pure kernel.

CrossHair reads the ``deal`` ``@pre`` / ``@post`` / ``@ensure`` / ``@raises``
contracts on a function and tries to find a concrete input that satisfies the
preconditions yet violates a postcondition (or raises an undeclared exception).
On these small, referentially-transparent kernel helpers it explores far more of
the input space than Hypothesis's random sampling: it walks execution paths
symbolically via the z3 SMT solver, so it reaches the pathological float corners
(denormal underflow, inf) that sampling almost never hits. This is how we found
the ``price * multiplier`` underflow → ``ZeroDivisionError`` in
``_floor_shares_for_notional``.

It is NOT a proof. Each condition runs under ``--per_condition_timeout``, so a
clean result means "no counterexample found within the budget", not "no
counterexample exists" — paths can be left unexplored when the solver runs out
of time. Stronger than sampling on this code; weaker than verification.

TARGETS are only the genuinely pure, contracted functions. The stateful
entry points — ``PositionSizer.compute`` (reads config + portfolio/liquidity
state), the ``ProposalStore`` methods (DuckDB I/O) — are deliberately NOT here;
only their extracted pure sub-parts are contracted and checked.

Usage:
    uv run python scripts/crosshair_check.py                  # all targets
    uv run python scripts/crosshair_check.py --timeout 30     # per-condition cap (s)
    uv run python scripts/crosshair_check.py order_math       # filter by substring

Exit code: 0 if every target is clean, 1 if any target is not clean
(counterexample found or tool error). Wired into .pre-commit-config.yaml as a
MANUAL-stage hook — CrossHair is too slow (tens of seconds/function) to run on
every commit, so it does not block ``git commit``. Invoke deliberately with:

    uv run pre-commit run crosshair-check --hook-stage manual

`crosshair` is dev-only (in the `dev` extra); run via `uv run`.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys

# The deal-contracted pure kernel functions, in kernel order. Keep in sync with
# the @deal decorators in the source modules (and the tests that exercise them).
#
# An entry may be a bare dotted name (uses --timeout) or a (name, seconds) pair
# capping that one target. The cap is a WALL-CLOCK decision, not a statement
# that the target is less important: CrossHair's per-condition timeout is a
# budget it will always spend, and a function containing a loop over a symbolic
# integer explores paths without bound. protective_stop_plan's cent step-down
# is exactly that shape — clean in 43s at 10s/condition, still running after 18
# MINUTES at 30. A gate that takes 20 minutes is a gate nobody runs.
TARGETS = [
    "trader.trading.exit_class.reduces_exposure",
    "trader.trading.order_structure.structural_rejection",
    ("trader.trading.protective_stop.protective_stop_plan", 10),
    "trader.trading.order_split.split_order",
    "trader.trading.order_math.reducible_quantity",
    "trader.trading.order_math.order_notional",
    "trader.trading.order_math.whole_shares_for_notional",
    "trader.trading.order_math._floor_shares_for_notional",
    "trader.trading.position_sizing._confidence_scale",
    "trader.trading.position_sizing._volatility_multiplier",
    "trader.trading.position_sizing.compute_atr",
    "trader.data.bar_quality.check_bar",
    ("trader.data.bar_quality.check_series", 10),
    ("trader.data.bar_quality.spacing_findings", 10),
    ("trader.data.bar_quality.unexplained_jumps", 10),
    "trader.data.proposal_transitions.is_known_status",
    "trader.data.proposal_transitions.is_valid_transition",
]


def check(target: str, timeout: int) -> tuple[int, str]:
    # PYTHONINTMAXSTRDIGITS: realizing a symbolic float can make z3 hand back an
    # exact rational whose numerator has thousands of digits, and CrossHair
    # stringifies it — which trips CPython's 4300-digit int/str guard and crashes
    # the checker with a ValueError. That reads as NOT-CLEAN (this gate fails
    # closed), i.e. a tooling limit would look like a contract violation. Raising
    # the limit only affects int-to-string conversion inside the checker; it does
    # not weaken any check. Hit by protective_stop.protective_stop_plan, whose
    # domain includes broker-supplied prices near the float maximum.
    env = {**os.environ, "PYTHONINTMAXSTRDIGITS": "1000000"}
    proc = subprocess.run(
        [sys.executable, "-m", "crosshair", "check", target,
         "--per_condition_timeout", str(timeout)],
        capture_output=True, text=True, env=env,
    )
    return proc.returncode, (proc.stdout + proc.stderr).strip()


def main() -> int:
    ap = argparse.ArgumentParser(description="Run CrossHair over the contracted kernel.")
    ap.add_argument("--timeout", type=int, default=20,
                    help="per-condition timeout in seconds (default 20)")
    ap.add_argument("filters", nargs="*",
                    help="only run targets whose dotted name contains one of these substrings")
    args = ap.parse_args()

    resolved = [(t, args.timeout) if isinstance(t, str) else (t[0], min(t[1], args.timeout))
                for t in TARGETS]
    targets = [(name, cap) for name, cap in resolved
               if not args.filters or any(f in name for f in args.filters)]
    if not targets:
        print("crosshair_check: no targets matched filters", file=sys.stderr)
        return 1

    not_clean = 0
    for t, cap in targets:
        rc, out = check(t, cap)
        if rc == 0:
            print(f"OK    {t}")
        else:
            not_clean += 1
            print(f"NOT-CLEAN  {t}  (crosshair exit {rc})")
            for line in out.splitlines():
                print(f"    {line}")

    print(f"\ncrosshair: {len(targets)} target(s) checked, {not_clean} not clean")
    return 1 if not_clean else 0


if __name__ == "__main__":
    raise SystemExit(main())
