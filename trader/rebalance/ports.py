"""The rebalancer's seams to the outside world.

Everything the rebalancer needs from IB, the proposal store and the price feed
comes through these four narrow protocols, so tests drive the whole package with
fakes and the production adapter (:mod:`trader.rebalance.sdk_gateway`) is the
only module that touches :class:`trader.sdk.MMR`.

By design (D09) there is NO protocol method that approves, places, cancels or
closes anything: the rebalancer cannot express those actions.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Protocol, Sequence


@dataclass(frozen=True)
class ProposalRecord:
    """The slice of a stored proposal the rebalancer reasons about."""

    id: int
    symbol: str
    action: str            # 'BUY' | 'SELL'
    quantity: Optional[float]
    status: str            # PENDING | APPROVED | EXECUTED | FAILED | REJECTED | EXPIRED
    source: str
    group: str
    created_at: Optional[dt.datetime] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @property
    def executed_quantity(self) -> float:
        """Whole quantity submitted for an EXECUTED proposal, signed by action.

        ``submission_quantity`` is the exact payload the SDK persisted before
        transport (an amount-sized proposal has no ``quantity`` of its own).
        Broker acceptance is not inventory: the caller still reconciles this
        against IB (D09).
        """
        if self.status != 'EXECUTED':
            return 0.0
        qty = self.metadata.get('submission_quantity', self.quantity)
        if qty is None:
            return 0.0
        signed = float(qty)
        return signed if self.action.upper() == 'BUY' else -signed


class ProposalReader(Protocol):
    def proposals_in_group(self, group: str) -> Sequence[ProposalRecord]:
        """Every stored proposal tagged with ``group``, any status."""
        ...


class Broker(Protocol):
    def stock_positions(self) -> Mapping[str, float]:
        """Live IB positions, symbol -> signed quantity, stocks only.

        Must raise (not return empty) when the read fails: an unreadable book
        cannot be reconciled and the plan must refuse.
        """
        ...


class Pricer(Protocol):
    def price(self, ticker: str) -> Optional[float]:
        """A finite positive price for ``ticker`` or None when it cannot be priced."""
        ...


class Proposer(Protocol):
    def ensure_group(self, group: str, description: str) -> None:
        """Create the sleeve's position group if it does not exist (idempotent)."""
        ...

    def propose(self, *, symbol: str, action: str, quantity: float, execution: Any,
                reasoning: str, source: str, group: str, metadata: Mapping[str, Any]) -> int:
        """Create ONE PENDING proposal through the existing pipeline; return its id."""
        ...


class Gateway(ProposalReader, Broker, Pricer, Proposer, Protocol):
    """Everything at once — what the CLI is handed."""
