# Mutation ledger — the paper-sleeve order path (2026-09-29, IN PROGRESS)

Scope added to `[tool.mutmut] only_mutate` this round: `trader/rebalance/targets.py`,
`trader/rebalance/plan.py`, `trader/rebalance/propose.py`, `trader/rebalance/fills.py`,
`trader/lowfreq_live/targets.py`. Oracle added to `pytest_add_cli_args_test_selection`:
`tests/test_rebalance_targets.py`, `test_rebalance_plan.py`, `test_rebalance_propose.py`,
`test_rebalance_fills.py`, `test_rebalance_properties.py` (new, Hypothesis P1–P7),
`test_lowfreq_live_targets.py`.

Run command (one module at a time, never a bare `mutmut run`):

```
.venv/bin/python3 scripts/run_mutation.py run --max-children 8 'trader.rebalance.targets.*'
.venv/bin/python3 scripts/mutation_score.py --survivors
```

## Status

The first scoped pass was started in round 1 and **had not completed when the round
ended**; it is running in the background (per-module sentinels
`/tmp/mmr_mut/done_<module>`, scores in `/tmp/mmr_mut/score_<module>.txt`). Mutant
counts generated for the five modules:

| module | mutants generated | killed | survived | timeout | score | status |
|---|---|---|---|---|---|---|
| trader/rebalance/targets.py | 419 | — | — | — | — | running |
| trader/rebalance/propose.py | 168 | — | — | — | — | queued |
| trader/rebalance/plan.py | 481 | — | — | — | — | queued |
| trader/rebalance/fills.py | 520 | — | — | — | — | queued |
| trader/lowfreq_live/targets.py | 634 | — | — | — | — | queued |

The first attempt at the stats pass FAILED and the cause is recorded here so it is
not repeated: `tests/test_rebalance_offline.py::test_end_to_end_targets_on_the_dev_db_
then_offline_plan_stays_within_capital` reads the research registry by repo-relative
path (`research/lowfreq/equity/candidates.yaml`), which mutmut does not copy into its
`mutants/` sandbox — it passes under plain pytest and fails only there. The offline,
CLI, integration and static driver files were therefore left OUT of the oracle; the
six files above are the oracle.

## Survivor classification

To be filled from `mutation_score.py --survivors` once each module completes. Rule
(from `scripts/run_mutation.sh`): every survivor is either

* **equivalent** — a rewrite with identical behaviour on every reachable input, with
  the one-line reason (an unreachable boundary, `False`→`None` where only truthiness
  is read, a `<`→`<=` at a point both branches compute the same value), or
* **cosmetic text** — a change inside a log/refusal/warning string only, named by
  mutant id,

and any survivor that is neither is a **test gap**: a test is ADDED (production code
is never edited to raise the score).

_(no module has completed yet — nothing to classify)_

## Baseline floors — HUMAN REVIEW REQUIRED

No entries have been added to `scripts/mutation_baseline.json` yet: a floor must be
recorded from a number that has been seen, and none has. When the pass completes,
the five modules will be ADDED to the baseline (existing entries byte-identical) at
their measured `(killed+timeout)/(killed+timeout+survived)` score. **Those floors
need human review before they are treated as a gate:** `mutation_score.py` warns that
scores on identical code can move by ~1–2% between runs in larger modules, and
`run_mutation.sh baseline` explicitly requires a full pass — the scoped run here is
not one, so `run_mutation.sh check` will still report the other baselined modules as
"not exercised" (that is the gate failing closed, by design, not a regression).

## Bugs found while building the oracle (fixed, pinned)

* **fills.match_executions** journaled a receipt with NaN/inf `shares` (`<= 0` is
  False for NaN; every receipt-vs-broker comparison is False for NaN) and accepted a
  NaN "known" broker fill quantity as an exact match. Pinned:
  `test_regression_bug_a_nan_or_inf_shares_receipt_is_refused_not_journaled`,
  `test_regression_bug_a_non_finite_broker_fill_quantity_is_not_evidence`, and the
  property `test_p7_journaled_fills_are_finite_and_signed_by_side`.
* **plan.reconcile_holdings / make_plan** crashed with a bare `ValueError` on a
  non-finite journal quantity instead of the `JOURNAL_UNREADABLE` refusal. Pinned:
  `test_regression_bug_b_non_finite_journal_quantity_refuses_the_plan_instead_of_crashing`.
