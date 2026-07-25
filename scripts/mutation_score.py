#!/usr/bin/env python3
"""Tally mutmut results per source module — the mutation score readout.

mutmut's own ``mutmut results`` hides killed mutants by default and prints one
flat list; this reads the same persisted result data (``SourceFileMutationData``)
and prints a per-module killed/survived/timeout/suspicious/no-tests/not-checked
table plus the mutation score (killed / (killed+survived), i.e. of the mutants
actually exercised). Run from the repo root AFTER ``mutmut run`` (it reads the
``mutants/`` sandbox), with the canonical env interpreter:

    ~/miniforge3/envs/mmr/bin/python3 scripts/mutation_score.py
    ~/miniforge3/envs/mmr/bin/python3 scripts/mutation_score.py --survivors  # also list survivor keys

See scripts/run_mutation.sh for the full pass.
"""
from __future__ import annotations

import argparse
import json
import sys
import pathlib
from collections import defaultdict

from mutmut.__main__ import status_by_exit_code, walk_mutatable_files
from mutmut.mutation.data import SourceFileMutationData

# Ordered status columns for the table.
_COLS = ["killed", "survived", "timeout", "suspicious", "no tests", "skipped",
         "caught by type check", "not checked"]


def collect() -> tuple[dict[str, dict[str, int]], dict[str, list[str]]]:
    per_module: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    survivors: dict[str, list[str]] = defaultdict(list)
    for path in walk_mutatable_files():
        module = str(path)
        m = SourceFileMutationData(path=path)
        m.load()
        for key, exit_code in m.exit_code_by_key.items():
            status = status_by_exit_code[exit_code]
            per_module[module][status] += 1
            if status == "survived":
                survivors[module].append(key)
    return per_module, survivors


BASELINE = pathlib.Path(__file__).resolve().parent / "mutation_baseline.json"


def _score(counts: dict[str, int]) -> float | None:
    """(killed + timeout) / (killed + timeout + survived), or None if nothing ran.

    Timeouts count as CAUGHT, deliberately. A timed-out mutant is one whose
    change made the suite hang — a detected difference, not an undetected one —
    and mutmut's own classification is timing-dependent: order_math was observed
    moving 57 killed/1 timeout -> 56/2 across runs with NO code change, while
    killed+timeout stayed fixed at 58. Excluding timeouts therefore made the
    score wobble by ~0.1% run to run, which for a gate means spurious failures
    and, soon after, a gate nobody trusts. Including them makes it reproducible.

    Note this is the GATE score; the printed table still shows the raw
    killed/(killed+survived) so the timeout column stays visible.
    """
    killed = counts.get("killed", 0) + counts.get("timeout", 0)
    survived = counts.get("survived", 0)
    denom = killed + survived
    return (killed / denom) if denom else None


def _module_report(per_module: dict[str, dict[str, int]]) -> dict[str, dict]:
    return {
        module: {
            "killed": counts.get("killed", 0),
            "survived": counts.get("survived", 0),
            "timeout": counts.get("timeout", 0),
            "score": round(_score(counts) or 0.0, 6),
        }
        for module, counts in sorted(per_module.items())
    }


def check_against_baseline(per_module: dict[str, dict[str, int]]) -> int:
    """Fail if any baselined module's score dropped, or wasn't exercised.

    Baselines the SCORE, not the survivor keys. Mutant identifiers renumber
    whenever a mutated function changes (check_leverage's two equivalents moved
    from 12/14 to 13/15 when the function grew a tri-state record), so a
    key-based baseline would false-alarm on every edit. The score is normalised
    against what actually ran: adding well-tested code raises it, adding
    untested code lowers it, and for UNCHANGED code it is exactly reproducible
    (verified: three consecutive runs produced byte-identical results).

    FAILS CLOSED, the lesson from the ty gate: a missing baseline, absent
    mutation data, or a module that was generated but never exercised is a
    FAILED check, never a silent pass.
    """
    if not BASELINE.exists():
        print(f"mutation gate FAILED — no baseline at {BASELINE}. "
              f"Record one with: scripts/run_mutation.sh baseline", file=sys.stderr)
        return 1
    baseline = json.loads(BASELINE.read_text())
    current = _module_report(per_module)

    failed = False
    for module, base in sorted(baseline.get("modules", {}).items()):
        cur = current.get(module)
        if cur is None or (cur["killed"] + cur["survived"]) == 0:
            print(f"mutation gate FAILED [{module}] — baselined but NOT exercised in this "
                  f"run; a partial pass cannot satisfy the gate. Run `run_mutation.sh all`.",
                  file=sys.stderr)
            failed = True
            continue
        if cur["score"] + 1e-9 < base["score"]:
            print(f"mutation gate FAILED [{module}] — score {cur['score']:.1%} is below "
                  f"baseline {base['score']:.1%} "
                  f"(killed {base['killed']}->{cur['killed']}, "
                  f"survived {base['survived']}->{cur['survived']}). A mutant the tests "
                  f"used to catch now survives: add a test, or document it as an "
                  f"equivalent in the ledger and re-baseline.", file=sys.stderr)
            failed = True
    if failed:
        return 1

    improved = [m for m, b in baseline.get("modules", {}).items()
                if current.get(m) and current[m]["score"] > b["score"] + 1e-9]
    msg = f"mutation gate OK — {len(baseline.get('modules', {}))} module(s) at or above baseline"
    if improved:
        msg += f" ({len(improved)} improved: {', '.join(sorted(improved))} — consider --update)"
    print(msg)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--survivors", action="store_true", help="list survivor mutant keys")
    ap.add_argument("--check", action="store_true",
                    help="compare against scripts/mutation_baseline.json; non-zero on regression")
    ap.add_argument("--update", action="store_true",
                    help="record the current scores as the baseline (a human-reviewed act)")
    args = ap.parse_args()

    per_module, survivors = collect()
    if not per_module:
        print("no mutation data found — run `mutmut run` first", file=sys.stderr)
        return 1

    if args.update:
        BASELINE.write_text(json.dumps(
            {"note": "Per-module mutation SCORE floor. Regenerate with "
                     "scripts/run_mutation.sh baseline after a human-reviewed change. "
                     "Scores are keyed on the module, not mutant ids, which renumber.",
             "modules": _module_report(per_module)}, indent=2) + "\n")
        print(f"mutation baseline updated: {len(per_module)} module(s) -> {BASELINE}")
        return 0

    if args.check:
        return check_against_baseline(per_module)

    name_w = max(len(m) for m in per_module) + 2
    header = f"{'module':<{name_w}}" + "".join(f"{c[:9]:>11}" for c in _COLS) + f"{'score':>9}"
    print(header)
    print("-" * len(header))
    tot = defaultdict(int)
    for module in sorted(per_module):
        counts = per_module[module]
        row = f"{module:<{name_w}}"
        for c in _COLS:
            row += f"{counts.get(c, 0):>11}"
            tot[c] += counts.get(c, 0)
        killed, survived = counts.get("killed", 0), counts.get("survived", 0)
        denom = killed + survived
        score = f"{killed / denom * 100:.1f}%" if denom else "  n/a"
        row += f"{score:>9}"
        print(row)
    print("-" * len(header))
    trow = f"{'TOTAL':<{name_w}}"
    for c in _COLS:
        trow += f"{tot[c]:>11}"
    tk, ts = tot["killed"], tot["survived"]
    tscore = f"{tk / (tk + ts) * 100:.1f}%" if (tk + ts) else "  n/a"
    trow += f"{tscore:>9}"
    print(trow)

    # The score's denominator is killed+survived, so mutants that were never
    # EXERCISED silently vanish from it. A partial run (e.g. `run_mutation.sh
    # cores`) therefore printed a confident "TOTAL 95.5%" while 817 of 884
    # mutants had not been run — a number a reader would reasonably mistake for
    # full-kernel coverage. Say so explicitly rather than flattering the run.
    unchecked = tot.get("not checked", 0)
    if unchecked:
        exercised = tk + ts + tot.get("timeout", 0) + tot.get("suspicious", 0)
        print()
        print(f"INCOMPLETE RUN: {unchecked} mutant(s) were generated but NOT executed "
              f"({exercised} exercised). The score above covers only what ran —")
        print("it is NOT a whole-kernel figure. Modules showing 'n/a' were not measured "
              "at all. Use `run_mutation.sh all` for a full pass.")

    if args.survivors:
        print("\nsurvivors:")
        for module in sorted(survivors):
            for key in survivors[module]:
                print(f"    {key}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
