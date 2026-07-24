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
from collections import defaultdict

from mutmut.__main__ import status_by_exit_code, walk_mutatable_files
from mutmut.mutation.data import SourceFileMutationData

# Ordered status columns for the table.
_COLS = ["killed", "survived", "timeout", "suspicious", "no tests", "skipped",
         "caught by type check", "not checked"]


def collect() -> dict[str, dict[str, int]]:
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


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--survivors", action="store_true", help="list survivor mutant keys")
    args = ap.parse_args()

    per_module, survivors = collect()
    if not per_module:
        print("no mutation data found — run `mutmut run` first")
        return 1

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

    if args.survivors:
        print("\nsurvivors:")
        for module in sorted(survivors):
            for key in survivors[module]:
                print(f"    {key}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
