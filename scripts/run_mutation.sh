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
#   proposal_transitions.py    8 killed /  0 survived            = 100.0%
#   order_math.py             57 killed /  3 survived / 1 timeout =  95.0%   (3 survivors = documented equivalents, see below)
#   position_sizing.py       395 killed / 162 survived           =  70.9%   (survivors: reasoning/warning text + session_summary report + boundary/degenerate/defense-in-depth equivalents)
#   risk_gate.py             216 killed /  43 survived           =  83.4%   (survivors: diagnostic checks/reason strings + degenerate boundaries; every gate DECISION mutant killed)
# The pure cores (order_math, proposal_transitions) and the contracted sizing
# scalars (_confidence_scale/_volatility_multiplier) are the hard safety floor
# and score ~100%/95%; the lower numbers are cosmetic-text-dominated methods.
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
