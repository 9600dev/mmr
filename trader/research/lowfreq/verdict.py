"""D01 verdict: PASS / PROMISING / FAIL from the gate inputs. Pure.

PASS requires ALL of
  (a) OOS Sharpe >= decisive benchmark OOS Sharpe (every benchmark, D04)
  (b) OOS active annualised return > 0 with Newey-West t >= 2.0
  (c) DSR of the active-return series >= 0.90 (deflated by family trial count)
  (d) OOS max drawdown no worse than the benchmark's by more than 5 pp
  (e) active return positive in >= 60% of OOS calendar years
  (f) active return still > 0 under 3x the base cost
  (g) grid >= 4 cells only: PBO < 0.35 and selection stability >= 0.5
PROMISING = (a), (b with t >= 1.0), (d), (f) hold but not all of PASS.
Otherwise FAIL. A gate whose input could not be computed FAILS (fail closed).
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional

T_PASS, T_PROMISING, DSR_MIN, DD_TOL, YEARS_MIN, PBO_MAX, STAB_MIN = 2.0, 1.0, 0.90, 0.05, 0.60, 0.35, 0.5


@dataclass(frozen=True)
class GateInputs:
    oos_sharpe: Optional[float]
    benchmark_oos_sharpe_max: Optional[float]
    active_ann_return: Optional[float]
    active_t_nw: Optional[float]
    active_dsr: Optional[float]
    oos_max_drawdown: Optional[float]
    benchmark_max_drawdown: Optional[float]
    fraction_positive_years: Optional[float]
    active_ann_return_stressed: Optional[float]
    n_cells: int
    pbo: Optional[float] = None
    selection_stability: Optional[float] = None


def _ok(v: Optional[float]) -> bool:
    return v is not None and v == v


def _ge(a: Optional[float], b: Optional[float]) -> bool:
    return a is not None and b is not None and a == a and b == b and a >= b


def _gt0(a: Optional[float]) -> bool:
    return a is not None and a == a and a > 0


def _lt(a: Optional[float], b: float) -> bool:
    return a is not None and a == a and a < b


def decide(g: GateInputs) -> Dict[str, Any]:
    a = _ge(g.oos_sharpe, g.benchmark_oos_sharpe_max)
    b_pass = _gt0(g.active_ann_return) and _ge(g.active_t_nw, T_PASS)
    b_prom = _gt0(g.active_ann_return) and _ge(g.active_t_nw, T_PROMISING)
    c = _ge(g.active_dsr, DSR_MIN)
    d = (g.benchmark_max_drawdown is not None and _ok(g.benchmark_max_drawdown)
         and _ge(g.oos_max_drawdown, g.benchmark_max_drawdown - DD_TOL))
    e = _ge(g.fraction_positive_years, YEARS_MIN)
    f = _gt0(g.active_ann_return_stressed)
    gg: Optional[bool]
    if g.n_cells >= 4:
        gg = _lt(g.pbo, PBO_MAX) and _ge(g.selection_stability, STAB_MIN)
    else:
        gg = None                                   # not applicable
    gates = {'a_sharpe_vs_benchmark': a, 'b_active_t_ge_2': b_pass, 'b_active_t_ge_1': b_prom,
             'c_dsr_ge_0_90': c, 'd_drawdown_within_5pp': d, 'e_positive_years_ge_60pct': e,
             'f_active_positive_at_3x_cost': f, 'g_pbo_and_stability': gg}
    if a and b_pass and c and d and e and f and (gg is None or gg):
        verdict = 'PASS'
    elif a and b_prom and d and f:
        verdict = 'PROMISING'
    else:
        verdict = 'FAIL'
    return {'verdict': verdict, 'gates': gates, 'inputs': asdict(g)}
