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
# Recorded baseline (2026-07-23, mutmut 3.6.0, canonical mmr env). Score is
# killed / (killed + survived); timeouts are detected (a hang the suite caught).
# NOTE this baseline is a COMMENT, not a machine-checked artifact (unlike the ty
# gate's JSON baselines) — nothing detects drift. It already drifted once: a
# 2026-07-24 re-run of `cores` gave order_math 56 killed / 3 survived / 2
# timeouts (94.9%), i.e. one mutant moved killed -> timeout in a day. Re-derive
# before trusting these numbers; treat them as an order-of-magnitude reference.
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
#   xǁRiskGateǁcheck_leverage__mutmut_12  (`get('equityWithLoanAfter', 0)` -> None)
#   xǁRiskGateǁcheck_leverage__mutmut_14  (`get('equityWithLoanAfter', )`  -> None)
#       risk_gate.py, 2026-07-25. Same mutant twice. `equity_after` is only ever
#       consumed by `if equity_after:`, and 0 and None are both falsy, so the
#       cushion branch is skipped identically. The arithmetic that could tell them
#       apart — `(equity_after - init_margin_after)` — sits INSIDE that guard and
#       is unreachable when the value is falsy. TRUE EQUIVALENTS.
#       (Re-derive if equity_after is ever read outside the truthiness guard.)
#       The other three check_leverage survivors (9, 21, 32) were REAL test gaps,
#       not equivalents — see TestCheckLeverageMissingData in tests/test_risk_gate.py.
#
# Usage:
#   scripts/run_mutation.sh            # all 4 modules, then per-module score
#   scripts/run_mutation.sh cores      # only the fast pure cores (order_math + proposal_transitions)
#   scripts/run_mutation.sh score      # just re-print the score from the last run
#   scripts/run_mutation.sh survivors  # per-module score + list surviving mutant keys
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
  survivors)
    "$PY" scripts/mutation_score.py --survivors
    ;;
  *)
    echo "usage: $0 [all|cores|score|survivors]" >&2
    exit 2
    ;;
esac
