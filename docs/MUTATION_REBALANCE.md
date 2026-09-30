# Mutation ledger — the paper-sleeve order path (2026-09-29/30)

Scope added to `[tool.mutmut] only_mutate`: `trader/rebalance/targets.py`, `plan.py`,
`propose.py`, `fills.py`, `trader/lowfreq_live/targets.py`. Oracle
(`pytest_add_cli_args_test_selection`): `tests/test_rebalance_{targets,plan,propose,fills}.py`,
`tests/test_rebalance_properties.py` (Hypothesis P1–P7), `tests/test_lowfreq_live_targets.py`,
and — added after the first pass — `tests/test_rebalance_mutation_kills.py` and
`tests/test_lowfreq_live_mutation_kills.py`, whose tests name the mutant ids they kill.
`test_rebalance_offline.py`/`_cli.py`/`_integration.py`/`_static.py` stay OUT: the offline
end-to-end test reads `research/lowfreq/equity/candidates.yaml` by repo-relative path,
which mutmut does not copy into `mutants/`, so it fails the stats pass there.

```
.venv/bin/python3 scripts/run_mutation.py run --max-children 8 'trader.rebalance.*' 'trader.lowfreq_live.targets.*'
.venv/bin/python3 scripts/mutation_score.py --survivors
```

## Results

All three passes ran on 2026-09-30 (pass 1 00:02–00:07, pass 2 00:23–00:29, pass 3 plan-only 00:30–00:34 PDT); `mutants/` was deleted afterwards.

Score = (killed+timeout)/(killed+timeout+survived), the gate formula. Pass 1 is the oracle
as first wired; pass 2 adds the two kill files (same source, same mutants).

| module | mutants | pass 1 killed/surv | pass 1 score | pass 2 killed/surv | pass 2 score |
|---|---|---|---|---|---|
| trader/rebalance/targets.py | 419 | 359 / 60 | 85.7% | 397 / 22 | **94.7%** |
| trader/rebalance/propose.py | 168 | 135 / 33 | 80.4% | 144 / 24 | 85.7% |
| trader/rebalance/plan.py | 481 | 407 / 74 | 84.5% | 441 / 37 → 442 / 36 (pass 3) | **92.5%** |
| trader/rebalance/fills.py | 520 | 420 / 100 | 80.8% | 460 / 60 | 88.5% |
| trader/lowfreq_live/targets.py | 634 | 418 / 206 (10 no-tests) | 67.0% | 518 / 112 (4 no-tests) | 82.2% |

Pass-2 survivors by class (every one is named below): targets 22 = 19 cosmetic + 3 equivalent;
propose 24 = 22 cosmetic + 2 equivalent; plan 37 = 20 cosmetic + 16 equivalent + 1 gap (`size_orders
#159`, KILLED in pass 3 by `test_a_one_share_buy_is_still_limit_flagged` — plan is now 442/36, 92.5%); fills 60 = 47 cosmetic +
13 equivalent; lowfreq 112 = 71 cosmetic + 12 equivalent + 29 residual (below). No behavioural
survivor is unclassified.

## How every pass-1 survivor was classified

Each survivor was diffed against `x_<fn>__mutmut_orig` in `mutants/` and sorted into:

* **cosmetic text** — the diff is confined to a string literal, an f-string, the message of a
  `raise`, a `warnings/problems.append(...)` payload or a log call (verified mechanically: the
  two ASTs are identical once string constants and those call arguments are normalised);
* **equivalent** — identical behaviour on every reachable input, one-line reason;
* **killed in pass 2** — a real test gap, closed by a named test (production code untouched);
* **residual** — behavioural, not killed, listed by name with why.

### trader/rebalance/targets.py — 60 survivors
* Cosmetic text (27): `_require #6,7,11`; `_kind_name #2,3`; `_parse_date #2`;
  `_parse_generated_at #2,9,10`; `_finite_positive #3,4`; `_jsonable #5,6,9`;
  `validate_targets #1,4,5,8,53,87,88,192,210,235,260`; `load_targets #8,9` (encoding-name
  case: `'UTF-8'` is the same codec).
* Killed in pass 2 (31): `_finite #3` (10**400 → `..._not_finite_not_crashed`); `_finite_positive #9`
  (`test_fractional_capital_is_accepted`); `_jsonable #1,2,3,4,7,8,10,11`
  (`test_decimal_params_become_json_native_numbers_with_exact_types`);
  `validate_targets #13,26,63,80,95,102,107,118,144,154,164,174` (where=None: the message must
  name the FILE — `test_missing_*_field_names_the_file_and_the_field`); `#72,73`
  (`test_bad_prices_as_of_names_file_and_field`); `#243` (`test_weight_above_one_is_refused_by_the_per_weight_rule`);
  `#255,257,268,270,280` (`test_notes_default_to_empty_and_fields_round_trip`).
* Equivalent (3): `exact_weight_sum #8` — `sum(decimals)` without a `Decimal(0)` start still
  returns a Decimal (`0 + Decimal` is Decimal); same value, same type. `_require #5` — no caller
  passes `kind=bool`, and a bool value is refused one line later by `_is_number`/`isinstance`
  (only the message differs — the bool-capital test kills neither, by design). `load_targets #6`
  — `encoding=None` is the locale codec, UTF-8 on every host and in the container.

### trader/rebalance/propose.py — 33 survivors
* Cosmetic text (23): `check_no_open_batch #11,26,33`; `reasoning_for #2,3`;
  `propose_plan #3,6,8,9,42-49,52,53,57,58,69,70` (the reasoning string and the
  metadata's descriptive keys are text the operator reads, never a decision input).
* Killed in pass 2 (8): `check_no_open_batch #24,28,30,31`
  (`test_sleeve_busy_lists_the_open_ids_in_id_order` — #28/#30 crash on two records, #31
  reorders); `propose_plan #21,64` (`test_proposals_carry_market_day_rth_spec_and_the_strategy_params`);
  `#94,95` (`test_midway_failure_reports_batch_identity_and_the_real_ids`).
* Equivalent (2): `propose_plan #22,23` — dropping `order_type='MARKET'` / `tif='DAY'` falls
  back to `ExecutionSpec`'s defaults, which are exactly those values.

### trader/rebalance/plan.py — 74 survivors
* Cosmetic text (21): `check_freshness #3,15`; `size_orders #8,11,32,95,220-223`;
  `fetch_prices #7-14` (the per-ticker warning); `sleeve_state #9,12,17`.
* Killed in pass 2 (40): `check_freshness #1`; `reconcile_holdings #16,17,18,19,21,32,34,42,43,46,47`
  (four `test_reconcile_*` tests); `size_orders #52,77` (band is % of capital, skip reports it);
  `#85,88,89` (sells-only one-share buy); `#97` (sells after a deferred buy); `#123,133`
  (limit flag values outside excess + resulting position); `#231,232,233,235` (plan identity);
  `_priced #1,6` (bool / text prices are unpriceable); `make_plan #6` (injected `today`);
  `make_plan #24,25,28,32,33,34,37` + `sleeve_state #36`
  (`test_make_plan_threads_limits_ib_book_band_and_sells_only_through`); `sleeve_state #37`.
* Killed in pass 3 (1): `size_orders #159` (`delta > 1`: a ONE-share BUY escaped the limit flag —
  `test_a_one_share_buy_is_still_limit_flagged`; the only pass-2 behavioural survivor that was a gap).
* Equivalent (16): `reconcile_holdings #44` (`<=` differs only when journal == IB + 1e-9 exactly);
  `size_orders #158` (`delta >= 0` in the flag guard: deltas of 0 never reach it); `sessions_after #3` (`today == as_of` → the range yields no session strictly
  after as_of either way); `#7,8,10,11,15,19` (the ±7-day calendar padding: any padding that
  covers [as_of, today], including the library default, yields the same sessions);
  `size_orders #36` (`prices[t] or 1.0` — every price already passed `_priced`, so the `or`
  branch is unreachable); `#84,108` (`delta >= 0`: deltas of 0 were `continue`d above);
  `#206` (`>=` at exactly capital + $0.005 — a half-cent boundary the cent-priced universe cannot
  land on); `#209,210` (`skipped` never holds a zero delta); `fetch_prices #6` (`''` vs `None`
  sentinel — the only reader is `_priced`, which refuses both).

### trader/rebalance/fills.py — 100 survivors
* Cosmetic text (50): `SleeveFillsStore._exists #7,8`, `record #17,18`, `holdings #8,9`,
  `list_fills #8,9` (SQL keyword/identifier CASE — DuckDB is case-insensitive; the `XX…XX`
  variants are killed); `submitted_quantity #6,7,12,13`; `match_executions #73,74,77,79,80,81,82,130,131,143,144,180` (#79 is the
  `statuses` text of the unknown-fill message);
  `reconcile_sleeve #24,26,27,35,36,38,39,47,49,50,58,59,61-64,68,83,88,105-107,126,133,148-150`.
* Killed in pass 2 (38): `trade_date_of #4` (host-tz dependent: the two instants in
  `test_trade_date_is_the_new_york_session_date_not_local_or_utc` differ from ET on any UTC or
  US-Pacific host; on an Eastern host the mutant is equivalent); `__init__ #1`, `record #20`,
  `list_fills #10,11,12,15,17,18,30,31,32,35,45,46` (`test_store_round_trips_every_fill_field`);
  `executed_order_ids #9`; `submitted_quantity #2,3,5,14,16,17`; `match_executions #3,40,48`;
  `#112,114,115` (two observations of one order); `#146` (a later order's MISMATCH still raises);
  `reconcile_sleeve #77,81,141,151,156,158,160,174`.
* Equivalent (12): `submitted_quantity #11` (`float(None)` raises TypeError inside the same
  `except`, returning None like the NaN path); `match_executions #56,59` (`default=` is only read
  when `usable` is empty, and the comparison is short-circuited by `usable and`); `#63,94,104,107`
  (`>` vs `>=` at exactly 1e-9 — receipts carry share counts, never a 1e-9 difference); `#132`
  (`or`: a non-empty `usable` with `broker_filled <= eps` has already appended a contradiction
  problem, so the whole match raises either way); `#134` (`< eps` at exactly 1e-9);
  `reconcile_sleeve #142,145` (boundaries at exactly ±1e-9); `#146` (`qty < +eps` differs only for
  a journal sum in [0, 1e-9) — the fake drops zeros, the real SUM of whole-share fills is never a
  sub-1e-9 positive).

### trader/lowfreq_live/targets.py — 206 survivors (+10 mutants with no covering test)
* Cosmetic text (109): `benchmark_sidecar_path #3,4`; `validate_targets_document` (30 message
  mutants: `#2,3,4,9-12,21,29,30,42,48-50,64,74-76,78-80,93,101-103,113,123,130-132`);
  `load_panel_for #7,8,18,19,34,35,37,38,46-48`; `latest_fundamentals_acceptance #8,9,24,27,28`
  (the SQL text, case variants); `check_freshness` (13 refusal-message mutants);
  `compute_targets` (31 refusal-message / notes-text mutants); `write_targets #13,14,16,17,20,32,33,36,37,40,41,46,47,58,62`
  (tmp-suffix and sidecar-name text — the `.json.tmp` staging names); `list_sleeve_targets #14,15`.
* Killed in pass 2 (58): `validate_targets_document #36,61,87,91,98,115,122`; `_positive #5`;
  `write_targets #7-12,15,22,24,26` (default path, existing dir); `#52,53,66,68,69,74,77,79,80`
  (sorted sidecar weights; the D08 file is indented with `schema` first); `list_sleeve_targets #11`;
  `compute_targets #9,10,17,20,21,22,23` (capital and params rules); `#57-63` (`check_fresh=False`
  still requires the panel to end on as_of); `#183,197,217,231,234,237` + `check_freshness #27,30`
  (diagnostics/provenance fields); `check_freshness #34` (lag limit inclusive);
  `load_panel_for #3,9,23,26,36` (entry warmup, unknown universe).
* Equivalent, added after pass 2 (4): `write_targets #75,78` (`sort_keys=None`/omitted is the
  default `False`); `compute_targets #59` (`== 1` in the `check_fresh=False` branch — same
  reasoning as `check_freshness #2`); `load_panel_for #30` (`entry.get(None, fam.top_n)` is the
  family value every registered entry uses).
* Equivalent (11): `written_weight_sum #4` (`sum` of Decimals without a Decimal start is still a
  Decimal); `latest_fundamentals_acceptance #10` (an aggregate `fetchone()` always returns a
  1-tuple, so `row` is never falsy); `#15` (`typing.cast` ignores its first argument at runtime);
  `#26` (the naive branch is unreachable: `fundamentals.accepted_at` is TIMESTAMPTZ, so the
  read is always tz-aware — and the mutant would have crashed `tz_convert(None)` had it been reached);
  `check_freshness #2` (`== 1`: every loader refuses an empty window — verified,
  `load_equity_panel` raises "pit_daily_bars returned no rows" — so the check is reached only
  with bars; a HAND-BUILT empty PricePanel would hit IndexError instead of the named refusal,
  a hardening gap off the loader path); `compute_targets #28`, `load_panel_for
  #14,30`, `#28` (`entry.get(None, fam.X)` returns the family default, which is what every
  registered entry uses — an entry-level override of `universe`/`top_n` does not exist in the
  registry); `compute_targets #81` (`make_strategy(entry, None, panel)` resolves None to the
  registered default cell, and a non-default cell is refused before this line unless registered;
  the parity test pins the default-cell weights); `#114` (`normalise_weights` default
  `max_gross` equals `base_costs('equity').max_gross`).
* **Residual (29, behavioural, NOT killed)** — all in code the fixture DB cannot reach:
  `load_panel_for #39,40,41,42,43,44,45` (the `etf` loader call: the fixture has no ETF panel);
  `load_panel_for #28` (dropping `top_n` falls to the loader's default, indistinguishable from the
  family's 500 on a 3-name fixture);
  `compute_targets #117,118,126,127,128,129,130,131,132,135,136,139,141,145,149,156,157,158,159,160,161`
  (benchmark coverage/weights plumbing: the fixture's only benchmark, `EqEwCovered`, holds
  (`None`) below 30 covered names, so on a 3-name panel the sidecar content cannot distinguish
  these). They affect the `.benchmark.json` sidecar (the ledger's virtual benchmark), never the
  D08 weights the rebalancer trades. Closing them needs a benchmark class with `MIN_NAMES=1` in
  the test registry and an ETF fixture — listed as the next test to add, not as acceptable.
* No covering test (4 after pass 2, 10 before): `TargetsRefused.__init__`-adjacent and `default_targets_path` mutants
  mutmut found no test for; `test_default_path_is_targets_dir_sleeve_as_of_...` now reaches
  `default_targets_path`.

## Baseline floors — HUMAN REVIEW REQUIRED

Five entries were ADDED to `scripts/mutation_baseline.json` (existing entries byte-identical)
at the pass-2 scores. **Treat them as provisional until a human has seen a second run:**
`mutation_score.py` records that identical code has moved 1–2% between runs in larger
modules, and `run_mutation.sh check` still requires a FULL pass (it will report the other
baselined modules as "not exercised" after a scoped run — that is the gate failing closed by
design). The lowfreq floor in particular sits below the 90% target because of the 28 residuals
above; raising it is a matter of adding the two fixtures, not of touching production code.

## Bugs found while building the oracle (fixed, pinned)

* **fills.match_executions** journaled a receipt with NaN/inf `shares` (`<= 0` is False for NaN;
  every receipt-vs-broker comparison is False for NaN) and accepted a NaN "known" broker fill
  quantity as an exact match. Pinned: `test_regression_bug_a_nan_or_inf_shares_receipt_is_refused_not_journaled`,
  `test_regression_bug_a_non_finite_broker_fill_quantity_is_not_evidence`, property
  `test_p7_journaled_fills_are_finite_and_signed_by_side`.
* **plan.reconcile_holdings / make_plan** crashed with a bare `ValueError` on a non-finite
  journal quantity instead of the `JOURNAL_UNREADABLE` refusal. Pinned:
  `test_regression_bug_b_non_finite_journal_quantity_refuses_the_plan_instead_of_crashing`.
