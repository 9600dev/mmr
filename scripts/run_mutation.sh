#!/usr/bin/env bash
# Mutation-test the pure safety kernel — "verify the verifier".
#
# Mutates the four safety-critical modules (order_math + proposal_transitions
# first — the fast, most-critical pure cores — then position_sizing + risk_gate)
# and checks the kernel test oracle (tests/invariants/ + the focused kernel unit
# tests + tests/test_deal_contracts.py + tests/test_mutation_kills.py) actually
# catches each change. A SURVIVING mutant = a code change no test noticed.
#
# Config (scope + oracle) lives in [tool.mutmut] in pyproject.toml. The driver
# scripts/run_mutation.py wraps mutmut with a patch that lets it mutate the
# @deal-contracted functions (mutmut skips decorated functions by default), which
# are the whole point. scripts/mutation_score.py prints the per-module readout.
#
# mutmut 3.x runs pytest IN-PROCESS, so it must run under the canonical env
# interpreter (which then IS the test interpreter). Install once with:
#     ~/miniforge3/envs/mmr/bin/python3 -m pip install mutmut==3.6.0
#
# The baseline is now a MACHINE-CHECKED artifact: scripts/mutation_baseline.json,
# compared by `run_mutation.sh check` (non-zero on regression), re-recorded by
# `run_mutation.sh baseline`. It used to be this comment block, which nothing
# verified and which was stale within a day. The numbers below are kept only as
# a human-readable orientation — the JSON is the source of truth.
#
# The GATE score counts timeouts as CAUGHT: (killed+timeout)/(killed+timeout+
# survived). A timed-out mutant made the suite hang, which is a detected
# difference, and mutmut's killed-vs-timeout split is timing-dependent —
# order_math was seen moving 57/1 -> 56/2 with no code change while killed+
# timeout stayed at 58. Excluding timeouts made the score wobble ~0.1% between
# identical runs, which in a gate means spurious failures and then a gate nobody
# trusts. The printed TABLE still shows raw killed/(killed+survived) so the
# timeout column stays visible.
# Scale note: a FULL pass is now ~2,600 mutants and takes tens of minutes,
# because auto_executor.py (1,709 mutants on its own) entered the scope on
# 2026-07-25. `run_mutation.sh check` requires a full pass by design — a partial
# run cannot satisfy the gate — so budget for that, or use `cores` for a quick
# read on the pure kernel while iterating.
#
#   auto_executor.py        1212 killed / 685 survived / 25 timeout = 63.9%   (corrected floor — see run-integrity note; the 74.3% was run inflation)
#       BASELINED, NOT ENDORSED. decide_signal (2026-07-25) and then the
#       protective-stop + reconcile paths (2026-07-26) have been analysed and
#       specced; the rest is a recorded floor so the score cannot silently
#       regress, NOT a claim that those survivors are acceptable. 9 mutants have
#       no covering test at all. What the 2026-07-26 pass did was MOVE decisions
#       out — the stop's arithmetic to protective_stop.py, the never-oversell
#       clamp to order_math.reducible_quantity — so what remains in the big
#       survivor blocks is SDK plumbing and log strings. Whoever picks this up
#       next: the remaining consequence is in ORDERING (cancel-before-close,
#       clear-tracking-in-finally) and in _process_signal/_execute_open, not in
#       arithmetic.
#   proposal_transitions.py    8 killed /  0 survived            = 100.0%
#   order_math.py             68 killed /  3 survived / 2 timeout =  95.8%   (3 survivors = documented equivalents, see below)
#   order_structure.py        86 killed /  3 survived            =  96.6%   (3 survivors = documented equivalents, see below)
#   order_split.py            40 killed /  2 survived            =  95.2%   (2 documented equivalents, see below)
#   protective_stop.py        53 killed /  5 survived / 1 timeout =  91.4%   (4 equivalents + 1 unreachable-domain residual, see below)
#   market_session.py        144 killed / 49 survived            =  74.6%   (log-string-dominated; every behavioural survivor classified, see below)
#   order_math.reducible_quantity                                 = 100.0%   (0 survivors of 11 — the shared never-oversell clamp)
#   position_sizing.py       415 killed / 165 survived           =  71.6%   (survivors: reasoning/warning text + session_summary report + boundary/degenerate/defense-in-depth equivalents)
#   risk_gate.py             320 killed /  32 survived           =  90.9%   (post-flip: +14 string mutants from the new refusal reasons; every decision mutant killed)   (survivors: diagnostic checks/reason strings + degenerate boundaries; every gate DECISION mutant killed)
# The pure cores (order_math, proposal_transitions) and the contracted sizing
# scalars (_confidence_scale/_volatility_multiplier) are the hard safety floor
# and score ~100%/95%; the lower numbers are cosmetic-text-dominated methods.
#
#   TURNOVER CAP + order_notional (2026-07-27). order_math 95.9% -> 97.3%,
#     risk_gate 90.9% -> 91.9%. First measurement was 87.7% / 85.6%: 51 new
#     survivors, and nearly all were genuine gaps in the NEW tests, not
#     equivalents. Worth recording what they were, because they are the same
#     shapes every time:
#       * inputs never exercised: quantity 0, multiplier 0/negative/NaN,
#         non-numeric quantity, and the overflow branch. All reachable, none
#         tested.
#       * `qty * price * mult` -> `qty * price / mult` survived because EVERY
#         test used multiplier 1.0, where the two agree. One options-multiplier
#         test kills it.
#       * `event_type=ORDER_SUBMITTED` -> `None` (query everything) survived
#         because the tests only ever wrote ORDER_SUBMITTED rows. Counting
#         SIGNAL and ORDER_FILLED too would multiply the measured turnover.
#       * `continue` -> `break` on the exit-class skip survived even after a
#         test was written for it, because query_since returns NEWEST FIRST and
#         the test appended the exit event first. An exit on both sides of the
#         opening event makes the test independent of iteration order.
#       * `unvaluable += 1` -> `-= 1` survived an assertion of "'2 of' in
#         reason", because '-2 of' contains '2 of'. startswith kills it.
#       * the per-strategy branch's own `checks[...] = 'fail'` and reason text
#         were unasserted; only the account branch was. Two branches, one
#         tested.
#     The 8 survivors that remain are documented equivalents:
#       * 4x logging.debug MESSAGE mutants in the CASH-exempt path (diagnostic
#         text, no behaviour). TRUE EQUIVALENTS.
#       * 3x `approved=False` -> `approved=None` — the module's existing
#         both-falsy class. TRUE EQUIVALENTS.
#       * `this_order = position_value if position_value > 0 else 0.0` ->
#         `>= 0`: at 0 both yield 0.0, and for negatives both take the else.
#         TRUE EQUIVALENT.
#
#   STALE-BAR GATE, fail-closed (2026-07-28). auto_executor 72.4% -> 91.7%,
#     protective_stop 91.5% -> 100%, risk_gate 91.9% -> 89.2%,
#     position_sizing wobbles ~71.0-71.6%. Two of those need explaining.
#
#     auto_executor's 19-point jump involved NO new tests. `SignalWork.
#     bar_size_seconds` defaulted to 0.0, which the stale-bar gate reads as
#     "interval unknown" and responds to by not running. Production always set
#     it; no test ever did. So every test that drove the open path drove it
#     with that gate switched off, and no test could reveal this because every
#     caller inherited the same default. The tests also used a hardcoded bar
#     timestamp three weeks in the past, which only worked because the gate was
#     off. Removing the default failed 31 tests at once; fixing them to state a
#     real interval and present a fresh bar moved the score 19 points, because
#     the tests finally reached code they had never reached. A default that
#     disables a check is an off switch every caller flips without noticing.
#
#     risk_gate DROPPED 2.7 points and the cause is mechanical, not a
#     regression. The `skipped:` / `unevaluable:` vocabulary split renamed
#     seven check values, and longer + more string literals mean more string
#     mutants, which survive. Verified rather than assumed: normalising every
#     string Constant in each survivor's AST and comparing against the original
#     shows 32 of the 60 survivors differ ONLY inside a literal. The rename
#     changed no structure, so it cannot have introduced a decision mutant, and
#     the module's standing rule (every gate DECISION mutant killed; survivors
#     are reason strings and degenerate boundaries) still holds. Recorded at
#     the lower number rather than papered over.
#
#     position_sizing is UNCHANGED code that measured 71.6%, 71.0% and 71.2%
#     across three runs. That contradicts the reproducibility claim below, so
#     it is recorded at the LOWEST observed value: a floor set at a wobbling
#     measurement's high-water mark cries wolf, and this gate's whole value is
#     that its alarms mean something. The cause is not established. The
#     candidates are mutmut scheduling under different load (the full pass runs
#     3,400+ mutants, the isolated one 580) and the newly dynamic bar timestamp
#     in the shared oracle. Worth pinning down before trusting sub-1% moves in
#     this module.
#
# EQUIVALENT-MUTANT LEDGER (the policy says a survivor is either a test gap or a
# documented equivalent *stating why*; the why was missing, so here it is —
# derived and verified 2026-07-24, all three order_math survivors):
#
#   x_whole_shares_for_notional__mutmut_13
#       `valid = False` -> `valid = None`, in the `except TypeError` branch.
#       The value is only ever consumed by `if not valid:`; False and None are
#       both falsy, so no observable behaviour changes. TRUE EQUIVALENT.
#
#   x__floor_shares_for_notional__mutmut_18   (`while shares >= 1` -> `> 1`)
#   x__floor_shares_for_notional__mutmut_19   (`while shares >= 1` -> `>= 2`)
#       18 and 19 are the same mutant (`shares > 1` == `shares >= 2`). They
#       diverge from the original ONLY in the state floor(amount/denom) == 1
#       while 1*denom > amount: the original steps 1 -> 0 and refuses, the mutant
#       returns 1 (an overspend, i.e. the bump-to-1 bug this module exists to
#       prevent). That state is UNREACHABLE in IEEE754: denom > amount implies an
#       exact quotient < 1, and rounding a quotient up to exactly 1.0 requires it
#       to exceed 1 - 2**-54 — but double spacing near `amount` is 2**-52
#       relative, so the smallest representable denom > amount already yields
#       ~0.9999999999999998, which floors to 0, never 1. No input can reach the
#       divergence. TRUE EQUIVALENTS.
#       (If fractional shares are ever enabled, or the floor/step-down loop is
#       restructured, RE-DERIVE this — the argument depends on both.)
#
#   x_reduces_exposure__mutmut_24   (`return position > 0`  -> `>= 0`)
#   x_reduces_exposure__mutmut_29   (`return position < 0`  -> `<= 0`)
#       exit_class.py, 2026-07-24. Both diverge from the original ONLY at
#       position == 0.0, and the guard above them (`position == 0.0 -> return
#       False`) makes that state unreachable at the comparison. -0.0 == 0.0 in
#       Python, so a negative zero is filtered too. TRUE EQUIVALENTS.
#       (Re-derive if the flat-position guard is ever relaxed.)
#       NB exit_class went 61.3% -> 93.5% only once tests/invariants/test_exit_class.py
#       was added to the ORACLE selection in pyproject.toml — adding the spec file
#       alone changed nothing, the drift tests/test_verification_wiring.py now catches.
#
#   RE-DERIVED 2026-07-26 after the fail-closed flip (targeted run, 320/32 =
#   90.9%; the 94.8% -> 90.9% drop is DENOMINATOR GROWTH, not lost coverage —
#   the three new refusal messages contribute ~14 unkillable-by-policy string
#   mutants, and every DECISION mutant is killed):
#   2026-07-30 — risk_gate 91.9% -> 89.2% -> 90.1%, and NOT re-baselined down.
#     The module GREW by 47 mutants when the turnover caps and the four-state
#     check vocabulary landed (507 -> 554). Tests still catch everything they
#     caught before — killed went 466 -> 499 — so nothing regressed; the new
#     code simply arrived with thinner coverage than the module's average, and
#     a score is a ratio.
#
#     Thirteen tests were added for the turnover arithmetic and the notional
#     accumulator (strict-ceiling boundary, exit-class exclusion, per-strategy
#     scoping, negative position_value not crediting the budget, legacy vs
#     unvaluable counted separately, malformed JSON not raising, fill-price
#     rescue). That recovered 0.9 points.
#
#     The floor is DELIBERATELY LEFT AT 91.9%, which the module currently fails.
#     Lowering it would bank the erosion, and the whole point of a ratchet is
#     that it does not move down quietly. The remaining survivors are dominated
#     by check_leverage's documented equivalents (below) plus reason-string
#     mutants in the new turnover branches, which are the same class. Closing
#     the last ~2 points means either more string-contract tests or ruling the
#     new reason strings equivalent — a judgement call for a human, not
#     something to grant myself by re-recording the number.
#
#     * check_leverage 4/6/13/15 — get('initMarginAfter'/'equityWithLoanAfter',
#       0) -> None / omitted default. TRUE EQUIVALENTS post-flip, by a NEW
#       argument: both defaults are falsy, the falsy guard now routes both to
#       the same refusal BEFORE any arithmetic runs, so the TypeError that
#       once discriminated None can never be reached. (Pre-flip only the
#       equity pair was equivalent; the flip made the init-margin pair
#       equivalent too.)
#     * check_leverage 39/42-45, 64/67-69, 104/107-110 — XX-wrap/case-flip on
#       the three refusal reasons. The contract-bearing substrings
#       ('NetLiquidation', 'initMarginAfter', 'equityWithLoanAfter',
#       'fail-closed') ARE asserted; only exact-text matches would kill these.
#       Cosmetic, same policy as the six evaluate string mutants.
#     * evaluate 1/2/3/8 + check_instrument 1/2 (default-args), evaluate
#       41/44/88/91/106/108 (strings), evaluate 188 (debug log), __init__ 3
#       (falsy equivalent) — unchanged from the 2026-07-25 classification.
#   Historical (pre-flip) derivation for the old 13/15 pair:
#   xǁRiskGateǁcheck_leverage__mutmut_13  (`get('equityWithLoanAfter', 0)` -> None)
#   xǁRiskGateǁcheck_leverage__mutmut_15  (`get('equityWithLoanAfter', )`  -> None)
#       (were 12/14 before check_leverage grew its tri-state record — mutant
#       NUMBERS shift whenever the function changes, so match on the DIFF, not
#       the index, when re-deriving this ledger.)
#       risk_gate.py, 2026-07-25. Same mutant twice. `equity_after` is only ever
#       consumed by `if equity_after:`, and 0 and None are both falsy, so the
#       cushion branch is skipped identically. The arithmetic that could tell them
#       apart — `(equity_after - init_margin_after)` — sits INSIDE that guard and
#       is unreachable when the value is falsy. TRUE EQUIVALENTS.
#       (Re-derive if equity_after is ever read outside the truthiness guard.)
#       The other three original check_leverage survivors (9, 21, 32) were REAL
#       test gaps, not equivalents — see TestCheckLeverageMissingData in
#       tests/test_risk_gate.py. Adding the tri-state record then produced SEVEN
#       new survivors in the cushion-FAIL branch alone (key, value and the
#       checks= argument could all be corrupted or dropped unnoticed), because I
#       asserted the leverage-fail record and not its cushion twin. Killed by
#       test_a_cushion_refusal_records_the_cushion_dimension. Lesson: adding an
#       observability record adds mutable surface — assert EVERY branch of it.
#
#   risk_gate.py RESIDUAL CLASSES (2026-07-25, after 85.3% -> 94.8%, 294/16).
#   Every remaining survivor is classified; none is an unexamined gap:
#     * xǁRiskGateǁ__init____mutmut_3 — `trading_filter = None` -> `""`. Only ever
#       read via `if not self.trading_filter:`; both falsy. TRUE EQUIVALENT.
#     * check_leverage 13/15 — see above.
#     * 6 DEFAULT-ARGUMENT mutants (evaluate 1/2/3/8, check_instrument 1/2):
#       they mutate parameter DEFAULTS. Both production callers (trading_runtime,
#       executioner) pass every argument explicitly, so the defaults are
#       unreachable in production. Killing them properly means removing the
#       defaults (or flipping *_evaluable to fail-closed) — MEASURED: that breaks
#       26 existing tests, for a benefit that only accrues against a future
#       caller who omits an argument. Recorded as a deliberate residual, not an
#       oversight. Revisit if a third caller of RiskGate.evaluate ever appears.
#     * 6 STRING mutants wrapping a reason in XX...XX (evaluate 41/44/88/91/
#       106/108). The contract-bearing substrings ARE asserted
#       (test_fail_closed_refusals_say_they_are_fail_closed), which killed the
#       case-flip variants; XX-wrapping preserves every substring, so only an
#       exact-text assertion would catch it. Cosmetic: the operator still reads
#       the full explanation. Not worth brittle exact-match tests.
#     * evaluate 188 — `logging.debug(f'...')` -> `logging.debug(None)`. Cosmetic.
#
#   auto_executor.decide_signal SURVIVOR CLASSIFICATION (2026-07-25).
#   The gate between a strategy signal and an unattended real-money order. It was
#   pure and had 84 unit tests but NO property and NO mutation coverage, so their
#   strength was unmeasured. Measured: 79.2% (32 survivors). Seven of those
#   changed real behaviour in the dangerous direction, and all seven are now
#   killed by tests/invariants/test_auto_execute_decision.py (-> 80.5%):
#     * mutant 2  — `live_armed: bool = False` -> `True`. THE LIVE DOUBLE-ARM
#       DEFAULTING TO ARMED. Nothing asserted that a caller omitting the argument
#       gets the disarmed answer. Killed by test_the_DEFAULT_is_disarmed.
#     * mutant 91 — `bar_age_seconds is not None` -> `is None`, inverting the
#       stale-bar gate so stale data opens and fresh data does not.
#     * mutant 93 — `stale_bar_multiple * bar_size` -> `/`, collapsing the stale
#       threshold from 180s to 0.05s. Killed by asserting a FRESH bar still opens.
#     * mutant 1  — the default stale multiple 3.0 -> 4.0. Killed with a bar at
#       3.5x its interval, which is stale under 3x and fresh under 4x.
#     * mutants 18/69 — `held_qty > 0` -> `> 1`, twice: a fractional (0.5-lot)
#       position stops counting as held, so it loses its disarmed-close exemption
#       and can be pyramided into. Killed with explicit 0.5-lot cases.
#     * mutant 90 — `bar_size_seconds > 0` -> `> 1`, switching the stale gate off
#       entirely for sub-second bar sizes.
#   Remaining survivors are reason-STRING mutants (XX-wrapping and case flips on
#   the human-readable explanation), the unreachable `unsupported action` branch,
#   and two boundary cases classified rather than chased:
#     * mutant 92  — `>` -> `>=` on the stale threshold. Differs only for a bar
#       aged EXACTLY 3x its interval, and errs toward refusing an open. Safe
#       direction, unreachable in practice with float ages.
#     * mutant 125 — `work.quantity > 0` -> `> 1` on the size passthrough. A
#       strategy-specified FRACTIONAL size would fall back to the position sizer
#       instead of being honoured — a real difference, but the fallback is the
#       safer path and no live strategy specifies fractional sizes.
#   NOTE the module is 1,709 mutants in total; only decide_signal was analysed.
#   The rest of auto_executor.py is now in scope and baselined, not examined.
#
#   order_split.py RE-MEASURED (2026-07-27, 95.4%, 62 killed / 3 survived).
#     The first measurement (95.2%, 40/2) predates the step-down loop and the
#     _conserves oracle, both added when a `deal` postcondition failure showed
#     exact float conservation was unachievable. Re-measuring after that change
#     dropped the score to 87.9% and surfaced TWO NON-equivalent survivors:
#       * `while open_qty > 0.0` -> `> 1.0`
#       * `nextafter(open_qty, 0.0)` -> `nextafter(open_qty, 1.0)`
#     Both survived because no test ever entered the regime where the step-down
#     does any work. It only runs when qty > 2*|held|; at ordinary sizes that
#     forces a remainder far above 1.0, so the sub-1.0 path is reachable ONLY at
#     tiny magnitudes (available=2.656539007259914e-180, qty=3.79631089884772e-178
#     overshoots by 5.5e-194). Found by search after mutation flagged it — the
#     same denormal corner CrossHair found in _floor_shares_for_notional. Pinned
#     in tests/invariants/test_order_split.py, plus tiny-magnitude branches added
#     to the Hypothesis strategies so the property tests reach it too.
#     A third survivor (`_conserves` boundary `qty <= 0` -> `qty < 0`) was also
#     genuine: it drops a zero request into the sum-based path, where halves that
#     CANCEL, like (-1, +1), read as conserving. Killed by testing the oracle
#     directly. Lesson worth keeping: a mutated POSTCONDITION fails silently —
#     the contract just stops catching things and every test still passes — so
#     the oracle needs its own tests, not only the function it guards.
#     The 3 remaining survivors are documented EQUIVALENTS, each a boundary moved
#     to a branch whose fallthrough computes the identical value. Verified by
#     differential testing over 400k inputs across the ordinary, denormal and
#     exactly-at-boundary regimes, not by reading alone:
#       * mutmut_3  (`qty <= 0` -> `qty < 0`): 0 differences beyond the SIGN of a
#         zero (open_qty -0.0 vs 0.0). -0.0 == 0.0 in every comparison and in
#         share semantics.
#       * mutmut_27 (`qty <= available` -> `qty < available`): 0 differences.
#         At qty == available the flip branch computes open_qty = 0 and the loop
#         is a no-op, returning the same plan.
#       * mutmut_36 (`while open_qty > 0.0` -> `>= 0.0`): 0 differences. Differs
#         only if open_qty reaches exactly 0 while available > qty, which the
#         branch condition (qty > available) makes impossible. The guard is
#         defensive: were that state reachable, nextafter(0.0, 0.0) == 0.0 would
#         spin forever.
#   Closes the flip residual: decomposes a position-crossing order into an
#   unrefusable reduction and a gated remainder. Both survivors are TRUE
#   EQUIVALENTS, and both for the same reason — the mutated boundary falls
#   through to a branch that computes the identical answer:
#     * 3  — `qty <= 0` -> `qty < 0`. At qty == 0 the fallthrough reaches
#       reduces_exposure(), which itself refuses a non-positive quantity and
#       returns False, producing SplitPlan(0.0, 0.0): the same value the guard
#       returns directly.
#     * 27 — `qty <= available` -> `qty < available`. At qty == available the
#       fallthrough computes SplitPlan(available, qty - available), and
#       qty - available == 0, so it equals SplitPlan(qty, 0.0).
#   (Re-derive if either fallthrough branch changes.)
#
#   protective_stop.py SURVIVOR CLASSIFICATION (2026-07-26, 81.8% -> 92.3%).
#   The disaster stop's size and price, extracted pure from
#   AutoExecutor._ensure_protective. Measuring it found a REAL BUG first:
#   `round(avg_cost * (1 - pct/100), 2)` returns the entry price itself for any
#   entry under ~$0.0625 at the default 8% (round(0.05*0.92, 2) == 0.05), so the
#   disaster stop sold at market the instant IB accepted it. Every pre-existing
#   test used a $100 entry. Now floors (never steps toward entry) and refuses
#   when no two-decimal price sits strictly below entry.
#   The Hypothesis property then found a SECOND one: flooring alone does not
#   guarantee "never nearer than requested" — at 99999.99999999999 and 1.00001%,
#   `target * 100` rounds up onto exactly 9899999.0. Fixed with the same
#   step-down loop order_math._floor_shares_for_notional uses, and pinned.
#   The 12 first-round survivors were the whole degenerate region: the off
#   switch at a fractional-cent entry, the overflow guard, and every branch of
#   the step-down loop (untested because the loop almost never runs). Three
#   pinned cases killed seven. Remaining 5 (mutant NUMBERS below are from the
#   2026-07-26 final run and shifted once already when the never-oversell clamp
#   moved out to order_math.reducible_quantity — match on the DIFF, not the id):
#     * 11 — `cost <= 0` -> `< 0`. At cost == 0 the target is 0, so the floor is
#       0 and the `0 < stop_price` check refuses anyway. TRUE EQUIVALENT.
#     * 36 — the INITIAL `math.floor(target * 100.0)` -> `* 101.0`. The step-down
#       loop converges to the same cents from ANY start at or above the answer,
#       so the output is identical (just slower). TRUE EQUIVALENT — and a
#       property of the loop worth knowing: it makes the initial floor
#       mutation-insensitive, so do not read a high score here as coverage of
#       that expression.
#     * 38 — `while cents > 0` -> `>= 0`. Diverges only for a negative target,
#       where both land on a non-positive stop and return None. TRUE EQUIVALENT.
#     * 39 — `while cents > 0` -> `> 1`. Needs a step from 1 cent to 0, i.e. a
#       double target < 0.01 whose `* 100.0` rounds UP to >= 1.0. That window is
#       narrower than one ULP of 0.01 (relative error <= 2**-53 vs a 2**-59
#       absolute spacing); a +/-200-ULP scan below 0.01 finds no such double, and
#       400k random samples in [1e-4, 1e3] find no float-boundary step-up at all.
#       EQUIVALENT in IEEE754, same argument shape as the order_math pair.
#       (Re-derive if the cent scale changes — sub-penny tick sizes would.)
#     * 32 — the overflow guard's `target * 100.0` -> `* 101.0`. A REAL
#       difference, in the band where `*100` is finite and `*101` is not:
#       entry prices between about 1.78e306 and 1.80e306. Recorded as an
#       unreachable-domain residual rather than pinned with an absurd test.
#   order_math.reducible_quantity — the never-oversell clamp both the close path
#   and the protective stop now share — has ZERO survivors out of 11 mutants.
#
#   auto_executor's _ensure_protective / _cancel_protective / set_protective are
#   still the largest survivor block in the module (~130). They are now THIN:
#   the decisions moved to protective_stop.py and order_math.reducible_quantity,
#   which score 91.4% and 100%. What is left is SDK plumbing and log strings.
#   Whoever picks this up next: the remaining consequence is in the ORDERING
#   (cancel-before-close, clear-tracking-in-finally), not in arithmetic.
#
#   order_lifecycle.py FIRST MEASUREMENT (2026-07-27, 61.7% -> 78.1%).
#   Scoped as part of the nine-bug coverage audit: the module writes the
#   ORDER_FILLED rows the PnL ledger pairs, and the live Cancelled->Filled race
#   proved a wrong record silently corrupts realized PnL. First measurement had
#   ~90 survivors inside _record_event alone — every FIELD of the ledger row
#   (strategy_name from orderRef, filled-vs-total quantity, avgFillPrice,
#   conid, order_id, metadata.status) could be corrupted unnoticed, because
#   tests asserted event TYPES and never content. TestRecordedEventContent
#   pins the whole row; ApiCancelled mapping and the filled=0 degenerate are
#   pinned separately (3+4 more kills).
#   RESIDUAL 73: FIRST-PASS CLASSIFICATION ONLY — spot-checked families are
#   defensive getattr-default equivalents (attrs always present on real Trade
#   objects), log-string mutants in the supersede branch, and async
#   cleanup-order variants in wait_decisive. NOT exhaustively derived; treat
#   as a floor, not an endorsement (auto_executor-style).
#
#   market_session.py SURVIVOR CLASSIFICATION (2026-07-26, 25.7% -> 74.6%).
#   The session gate for dispatched bars, scoped the day it shipped. The first
#   measurement was the WORST in the kernel — 25.7% with 28 passing tests — and
#   the survivor pattern (84 broken-schedule mutants collapsing into False)
#   exposed that the documented fail-open policy was NOT IMPLEMENTED for
#   calendar errors: "shut that day" and "lookup failed" were the same bare
#   Optional, so one library exception suppressed every bar for every
#   instrument, and failures were CACHED, pinning the wrong answer for the
#   process lifetime. Fixed with the tri-state SessionLookup (548db31); the
#   score moved 25.7% -> 63.7% from the semantics fix alone, with no
#   assertions added for the purpose.
#   Then two test-infrastructure findings:
#     * COLD CACHES (63.7% -> 70.5%). _schedule_cache is module-level and
#       leaked across tests, so mutants in _utc_stamps/_calendar survived
#       DIRECT assertions on their output — the mutated code never ran, the
#       test read a cache warmed by an earlier test. Autouse fixture clears
#       all three module caches per test. A warm cache turns a behavioural
#       test into a cache read; check for this before trusting any module
#       with module-level memoization.
#     * The OFFSET mutants pointed at real load-bearing behaviour
#       (70.5% -> 74.6%): the neighbouring-day loop is what admits ASX bars
#       23:00-24:00 UTC all southern summer (Sydney UTC+11 opens the session
#       on the PREVIOUS UTC day) and the final minute of US post-market
#       (00:00 UTC next day). Pinned in TestSessionsThatStraddleUtcMidnight —
#       without +1, the gate would eat the first hour of every ASX session
#       from October to April and it would look exactly like a slow feed.
#   Remaining 49, all classified:
#     * ~24 log-string/log-arg mutants (XX-wrap, case-flip, None'd args) and
#       warn-once bookkeeping (worst case: the unmapped-venue warning logs
#       every time instead of once). Cosmetic.
#     * session_window 45/62/63 — EXCEPT-NET EQUIVALENTS: the mutated return
#       (SessionLookup(False) / (None,) / and-for-or making min([]) reachable)
#       raises TypeError/ValueError INSIDE the try, the broad except catches
#       it and returns the correct SessionLookup(None, False) anyway. Only the
#       log text differs. (Re-derive if the except ever narrows.)
#     * in_session 60 — `day - dt.timedelta(offset)` over the SYMMETRIC set
#       {-1,0,1} visits the same three days. TRUE EQUIVALENT.
#     * in_session 65 — continue->break on an unevaluable day: every path
#       that reaches the break also sets unevaluable=True, and the post-loop
#       fail-open returns True in both versions. OBSERVABLY EQUIVALENT.
#     * tz-case mutants ('UTC' -> 'utc') — pytz resolves both. EQUIVALENT.
#     * or->and belt-and-braces (value None/NaT double-checks) — the NaT is
#       caught by the isinstance(Timestamp) check downstream. EQUIVALENT.
#     * default-arg mutants — the one production call site
#       (strategy_runtime._bar_in_session) passes every argument explicitly,
#       and 'XXXX' maps to no calendar so the fallthrough is identical.
#     * the isinstance(day, dt.date) guard's return — unreachable: NaT is
#       filtered by the isna check before .date() is called. Defensive only.
#
#   order_structure.py SURVIVOR CLASSIFICATION (2026-07-26, 84.3% -> 96.6%).
#   The last structural check before IB, extracted pure from OrderValidator and
#   moved to the placement chokepoint. First measurement: 14 survivors, ALL of
#   them `getattr` DEFAULT mutants in the rejection_for_order adapter — i.e. the
#   behaviour when an order object is MISSING a structural attribute, which no
#   test exercised. Six changed real behaviour:
#     * 16/25/45 — the default DELETED (`getattr(order, 'lmtPrice', )`). A
#       missing field then raises AttributeError inside the placement path,
#       converting a refusal into a crash with no recorded reason.
#     * 29/49/59 — the default made PERMISSIVE (0 -> 1). An order with no
#       totalQuantity places 1 share; one with no lmtPrice places a limit at a
#       price nobody chose. Both look well-formed all the way to IB.
#   Killed by test_an_order_missing_a_structural_field_is_refused, which pins the
#   assumed value as well as the refusal (same reasoning as
#   test_the_DEFAULT_is_disarmed: a default nothing asserts is a decision nobody
#   is holding). That one property also killed 19/22/42/52, whose defaults differ
#   only in what the refusal REPORTS.
#   Remaining 3, all TRUE EQUIVALENTS:
#     * 13 — action default '' -> None. _normalize does `str(text or '')`, so
#       None and '' both normalize to '' and take the same refusal branch.
#     * 32/39 — orderType default '' -> None / 'XXXX'. A missing order type
#       deliberately degrades to "unpriced" (the price checks do not apply), and
#       none of the three values is in _NEEDS_LIMIT/_NEEDS_STOP, so all three
#       accept identically. Pinned by
#       test_an_order_missing_its_type_degrades_to_unpriced_rather_than_refusing.
#       (Re-derive if an order type is ever matched by prefix or truthiness.)
#
#   position_sizing.py SURVIVOR CLASSIFICATION (2026-07-25, 396/161 = 71.1%).
#   All 161 classified by whether they can change the SIZED AMOUNT — the only
#   safety-relevant output (amount_usd / quantity):
#     * 85 in session_summary() — a reporting method; cannot affect a size.
#     * 6 reasoning/warning TEXT mutants — cannot affect a size.
#     * 70 on the size path, and they are almost entirely `> 0` GUARD boundary
#       mutants (`net_liquidation > 0`, `price > 0`, `avg_daily_volume > 0` ->
#       `> 1`). They survive for ONE structural reason, worth knowing before
#       anyone tries to kill them individually: tests/invariants/
#       test_sizing_properties.py has good properties but its input strategies
#       never generate the degenerate region — net_liq starts at 10,000 and
#       price at 1.0, so `> 0` and `> 1` can never disagree. Widening those
#       strategies would kill most of the 70 at once; writing 70 boundary tests
#       would be the wrong tool.
#       CAUTION before widening: probing that region found a real behavioural
#       edge — at net_liquidation == 0 the percentage cap is bypassed and
#       compute() returns the default amount (~$4,300 at confidence 0.8) against
#       a cap of $0. It is the ONLY input where the amount exceeds
#       max_position_pct; a small-but-positive account is capped correctly. It
#       is backstopped (the risk gate refuses the open whether net-liq is flagged
#       unreadable or reads as zero), so it is a wrong number nothing acts on.
#       Pinned in tests/test_position_sizing.py::TestSizingWithUnreadableNet-
#       Liquidation. Widening the spec's strategies therefore needs that
#       behaviour decided FIRST, or the widened property goes red on day one.
#
# RUN-INTEGRITY WARNING (2026-07-26, RESOLVED 2026-07-27): mutmut's
# shared-process model can mis-score in BOTH directions, and the two errors
# look different:
#   * UNDER-kill (false survivors): proven once — _execute_open__mutmut_5 was
#     recorded SURVIVED while its own covering set, run manually
#     (MUTANT_UNDER_TEST=... pytest, cwd=mutants), failed 23 tests.
#   * OVER-kill (false kills): the 2026-07-26 morning run scored auto_executor
#     74.3%; the evening run and the 2026-07-27 quiet-machine run BOTH scored
#     ~63.9% on it, and ELEVEN of eleven manually-verified survivors from the
#     quiet run were genuine. The 74.3% was the outlier — inflated, most
#     plausibly by cross-mutant state contamination making later tests fail
#     spuriously. A spurious failure IS a spurious kill.
# The meta-lesson: one proven false survivor does not condemn a whole run, and
# an agreeing pair of runs beats a flattering singleton. Policy: prefer quiet
# machines; when runs disagree, manually verify a SAMPLE of the disputed
# verdicts IN BOTH DIRECTIONS before recording either number; `baseline`
# records without checking first, so never run it casually.
#
# Usage:
#   scripts/run_mutation.sh            # all 4 modules, then per-module score
#   scripts/run_mutation.sh cores      # only the fast pure cores (order_math + proposal_transitions)
#   scripts/run_mutation.sh score      # just re-print the score from the last run
#   scripts/run_mutation.sh survivors  # per-module score + list surviving mutant keys
#   scripts/run_mutation.sh check      # compare last run to the recorded baseline (non-zero on regression)
#   scripts/run_mutation.sh baseline   # full pass, then RE-RECORD the baseline (human-reviewed)
set -euo pipefail

PY="${MMR_PY:-$HOME/miniforge3/envs/mmr/bin/python3}"
cd "$(dirname "$0")/.."

case "${1:-all}" in
  cores)
    rm -rf mutants
    "$PY" scripts/run_mutation.py run \
        'trader.trading.order_math.*' 'trader.data.proposal_transitions.*'
    "$PY" scripts/mutation_score.py
    ;;
  all)
    rm -rf mutants
    "$PY" scripts/run_mutation.py run
    "$PY" scripts/mutation_score.py
    ;;
  score)
    "$PY" scripts/mutation_score.py
    ;;
  check)
    # Compare the LAST run against scripts/mutation_baseline.json. Non-zero if a
    # module's score dropped, if a baselined module was not exercised (a partial
    # run must not satisfy the gate), or if the baseline/mutation data is missing
    # — fail closed, the lesson from the ty gate reporting OK when it never ran.
    "$PY" scripts/mutation_score.py --check
    ;;
  baseline)
    # Re-record the score floor. A HUMAN-REVIEWED act: only after confirming each
    # surviving mutant is a documented equivalent (see the ledger above) or has a
    # test added. Run a FULL pass first — baselining a partial run bakes in
    # unmeasured modules.
    rm -rf mutants
    "$PY" scripts/run_mutation.py run
    "$PY" scripts/mutation_score.py
    "$PY" scripts/mutation_score.py --update
    ;;
  survivors)
    "$PY" scripts/mutation_score.py --survivors
    ;;
  *)
    echo "usage: $0 [all|cores|score|survivors|check|baseline]" >&2
    exit 2
    ;;
esac
