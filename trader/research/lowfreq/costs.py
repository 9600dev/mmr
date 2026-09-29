"""Cost model (D01): ETFs 3 bps + $0.005/share, equities 5 bps + $0.005/share,
shorts 50 bps/yr borrow, no leverage (gross <= 1.0). Stress = every component
multiplied."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Costs:
    slippage_bps: float
    commission_per_share: float
    short_borrow_bps_annual: float = 50.0
    max_gross: float = 1.0           # D01: no leverage

    def scaled(self, k: float) -> 'Costs':
        return Costs(self.slippage_bps * k, self.commission_per_share * k,
                     self.short_borrow_bps_annual * k, self.max_gross)

    def as_dict(self) -> dict:
        return {'slippage_bps': self.slippage_bps, 'commission_per_share': self.commission_per_share,
                'short_borrow_bps_annual': self.short_borrow_bps_annual, 'max_gross': self.max_gross}


BASE = {'etf': Costs(3.0, 0.005), 'equity': Costs(5.0, 0.005)}
STRESS_MULTIPLIER = 3.0


def base_costs(universe: str) -> Costs:
    if universe not in BASE:
        raise ValueError(f'no cost model for universe {universe!r} (D01 names etf and equity)')
    return BASE[universe]
