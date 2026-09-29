"""``status``: what the sleeve holds, what is in flight, and drift vs the latest targets."""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Mapping, Optional, Sequence

from trader.rebalance.plan import (
    Refusal, fetch_prices, sleeve_state, target_shares,
)
from trader.rebalance.ports import Gateway, ProposalRecord
from trader.rebalance.targets import Targets, latest_targets_path, load_targets, sleeve_group


@dataclass(frozen=True)
class HoldingLine:
    symbol: str
    shares: int
    price: Optional[float]
    target_weight: float
    target_shares: Optional[int]

    @property
    def value(self) -> Optional[float]:
        return None if self.price is None else self.shares * self.price

    @property
    def drift_shares(self) -> Optional[int]:
        return None if self.target_shares is None else self.shares - self.target_shares


@dataclass(frozen=True)
class SleeveStatus:
    sleeve: str
    group: str
    holdings: Sequence[HoldingLine]
    market_value: Optional[float]            # None when any holding is unpriceable
    open_proposals: Sequence[ProposalRecord]
    targets: Optional[Targets]
    targets_path: Optional[str]
    warnings: Sequence[str] = field(default_factory=tuple)
    as_of_today: Optional[dt.date] = None

    @property
    def gross_drift_value(self) -> Optional[float]:
        total = 0.0
        for h in self.holdings:
            if h.price is None or h.drift_shares is None:
                return None
            total += abs(h.drift_shares) * h.price
        return total


def sleeve_status(sleeve: str, gateway: Gateway, *, targets_dir: str | None = None,
                  targets_file: str | None = None) -> SleeveStatus:
    group = sleeve_group(sleeve)
    state = sleeve_state(sleeve, gateway)
    holdings, warnings, in_flight = state.holdings, list(state.warnings), list(state.open_proposals)

    targets: Optional[Targets] = None
    path: Optional[str] = targets_file
    if path is None:
        found = latest_targets_path(sleeve, targets_dir)
        path = str(found) if found else None
    if path:
        targets = load_targets(path)
        if targets.sleeve != sleeve:
            raise Refusal('WRONG_SLEEVE', f'{path} is for sleeve {targets.sleeve!r}, not {sleeve!r}')

    symbols = sorted(set(holdings) | (set(targets.weights) if targets else set()))
    prices = fetch_prices(gateway, symbols)
    lines: list[HoldingLine] = []
    for sym in symbols:
        px = prices.get(sym)
        px = float(px) if px is not None else None
        w = targets.weights.get(sym, 0.0) if targets else 0.0
        tgt = target_shares(w, targets.capital_usd, px) if (targets and px) else (0 if targets else None)
        lines.append(HoldingLine(symbol=sym, shares=holdings.get(sym, 0), price=px,
                                 target_weight=w, target_shares=tgt))
    values = [line.value for line in lines if line.shares]
    mv: Optional[float] = None if any(v is None for v in values) else float(sum(v for v in values if v is not None))
    return SleeveStatus(sleeve=sleeve, group=group, holdings=tuple(lines), market_value=mv,
                        open_proposals=tuple(in_flight), targets=targets, targets_path=path,
                        warnings=tuple(warnings))
