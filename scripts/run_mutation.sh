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
#   proposal_transitions.py    8 killed /  0 survived            = 100.0%
#   order_math.py             57 killed /  3 survived / 1 timeout =  95.0%   (3 survivors = documented equivalents)
#   position_sizing.py       395 killed / 162 survived           =  70.9%   (survivors: reasoning/warning text + session_summary report + boundary/degenerate/defense-in-depth equivalents)
#   risk_gate.py             216 killed /  43 survived           =  83.4%   (survivors: diagnostic checks/reason strings + degenerate boundaries; every gate DECISION mutant killed)
# The pure cores (order_math, proposal_transitions) and the contracted sizing
# scalars (_confidence_scale/_volatility_multiplier) are the hard safety floor
# and score ~100%/95%; the lower numbers are cosmetic-text-dominated methods.
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
