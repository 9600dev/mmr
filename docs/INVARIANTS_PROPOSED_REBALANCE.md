# Invariants — the paper-sleeve order path (promoted 2026-10-01)

Drafted 2026-09-29 by the hardening climb; promoted into the spec as
`tests/invariants/test_rebalance_sleeve.py` on 2026-10-01 by the orchestrator under the owner's explicit delegation
("I don't need to review things — I trust your judgement"), in a commit of its own. Each names the harm it forbids and is
silent about mechanism, per `tests/invariants/README.md` ("name the harm, not
the mechanism"). Nothing under `tests/invariants/` was touched.

Scope: `trader/rebalance/` (targets loader, plan, propose, fills journal +
reconcile) and `trader/lowfreq_live/targets.py` (the writer). This code turns a
D08 weights file into PENDING proposals on the pinned paper account; its first
live-paper rebalance is 2026-10-01.

| # | Statement | Why it matters | Test today | Found |
|---|-----------|----------------|------------|-------|
| P1 | A plan never SELLs more shares of a symbol than the sleeve holds; the sleeve is never short after execution. | An oversell is an opening SHORT the server would have to classify and gate; the sleeve is long-only by design (D15). | `test_p1_plan_never_sells_more_than_the_sleeve_holds` | — |
| P2 | The plan's BUY notional never exceeds sleeve cash plus the plan's own SELL proceeds, at the prices the plan uses; post-execution gross exposure never exceeds capital. | Buying more than the sleeve can pay for is leverage the targets file forbids (weights sum ≤ 1). The plan may REFUSE rather than trim, never repair. | `test_p2_buy_notional_never_exceeds_cash_plus_own_sell_proceeds` | — |
| P3 | Every SELL leg satisfies `qty ≤ min(sleeve holding, broker position)`, so the account is never taken short by the sleeve and the server classifies the order exit-class. | The exit-class predicate is what exempts the SELL from the risk gate and approval tier; a SELL that crosses the live position would be split and its remainder gated (or refused). Holdings are the D13 journal CLAMPED to IB, never IB alone and never the journal alone. | `test_p3_every_sell_reduces_the_live_broker_position` | — |
| P4 | Shares held OUTSIDE the sleeve (in IB but not in the journal, or IB's excess over the journal) are never sold and never counted as sleeve holdings; every order sizes from the sleeve's own holding. | The pinned account also holds manual/other-strategy inventory. Selling it, or counting it toward the target so the sleeve under-buys, corrupts both books. | `test_p4_shares_outside_the_sleeve_are_never_sold_nor_counted` | — |
| P5 | For any valid (long-only, float-sum ≤ 1) weight vector, the file `trader.lowfreq_live` writes loads through `trader.rebalance.targets.load_targets` with every weight strictly positive and the EXACT decimal sum of the written strings ≤ 1. | The reader judges the bound on the decimal strings as written (no tolerance — 1.0000000001 is leverage). The writer must quantise so the round trip never refuses a valid instruction (the first end-to-end run was refused on 69 × repr(1/69)). **As worded the property says non-negative; the true rule is strictly positive** — the reader refuses a zero weight and the writer drops weights that floor to zero. | `test_p5_written_targets_load_through_the_reader_with_positive_weights_summing_at_most_one` | Gap: writer ticker regex is wider than the reader's (`test_p5_regression_writer_regex_is_wider_than_the_readers`) — fail-closed, not unsafe, but a valid strategy output could be refused at the reader. |
| P6 | `propose` creates exactly one PENDING proposal per planned order, in plan order (SELLs first), stamped with the sleeve group and batch source; when the sleeve has an open batch it creates NOTHING. | The rebalancer's only write. A second batch under an open one changes holdings under the earlier batch when it is approved; a partial or reordered batch makes the journal and the plan disagree. | `test_p6_propose_mirrors_the_plan_exactly_or_creates_nothing` | — |
| P7 | A journaled fill is always a finite, non-zero quantity signed by the order's side (`+` BUY, `−` SELL) whose magnitude equals the receipt's shares. Any receipt or broker observation that is not a finite number REFUSES the whole reconcile. | The D13 journal IS the sleeve's inventory. One NaN row makes `SUM(quantity)` NaN for that ticker forever. | `test_p7_journaled_fills_are_finite_and_signed_by_side` | **Bug A** (below) |

## Bugs the drafting found (fixed in this round, each pinned by a named regression)

**Bug A — `fills.match_executions` journaled NaN/inf receipts.**
`e.shares <= 0` is False for NaN, and every receipt-vs-broker inequality is False
for NaN, so a receipt with `shares=nan` (or `inf`) passed as "usable", matched a
`Filled 25` observation, and reached the journal as `Fill(quantity=nan)`. A
"known" broker fill quantity of NaN likewise matched any receipt count. Fix: a
receipt's `shares`/`cum_qty` and an observation's `filled` must be finite or the
order is a problem (`ExecutionsIncomplete`, nothing journaled). Regressions:
`test_regression_bug_a_nan_or_inf_shares_receipt_is_refused_not_journaled`,
`test_regression_bug_a_non_finite_broker_fill_quantity_is_not_evidence`.

**Bug B — `plan.reconcile_holdings` crashed on a non-finite journal quantity.**
`int(math.floor(min(nan, broker)))` raised a bare `ValueError: cannot convert
float NaN to integer`; `make_plan` propagated it, so once the journal held a NaN
(bug A) every later plan crashed the same way — the sleeve was bricked with no
named refusal. Fix: the pure function raises a `ValueError` naming the symbol;
`sleeve_state` converts it to the `JOURNAL_UNREADABLE` refusal it already uses
for an unreadable journal, before any price is fetched. Regression:
`test_regression_bug_b_non_finite_journal_quantity_refuses_the_plan_instead_of_crashing`.

## Notes for the promoting human

* P1/P2 are exercised through `size_orders` (pure); P3/P4 through `make_plan`
  with `tests/rebalance_fakes.FakeGateway` (journal + IB book + prices), which
  also asserts the plan touched only read-only gateway methods.
* The only refusal the generators admit is `GROSS_EXPOSURE_EXCEEDS_CAPITAL`
  (a banded overhang); the properties are vacuous on a refused plan, which is
  the documented behaviour (no repair mode).
* P5 assumes at least one written weight survives quantisation; the writer's
  `compute_targets` refuses an all-flat instruction upstream, so this mirrors a
  documented refusal rather than dodging a failure.
* Observation, not changed: `reconcile_holdings` treats an IB position that is
  NaN as "IB shows none — treated as flat" (with a warning), the same as a
  missing position. That is the pre-existing documented rule; whether a
  non-finite broker read should instead refuse the plan is a design call.
