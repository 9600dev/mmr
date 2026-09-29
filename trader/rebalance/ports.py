"""The rebalancer's seams to the outside world.

Everything the rebalancer needs from IB, the proposal store, the price feed and
the fills journal comes through these narrow protocols, so tests drive the
whole package with fakes and the production adapter
(:mod:`trader.rebalance.sdk_gateway`) is the only module that touches
:class:`trader.sdk.MMR`.

By design (D12) there is NO protocol method that approves, places, cancels or
closes anything: the rebalancer cannot express those actions. The only writes
it can express are ``propose`` (a PENDING proposal) and ``record_fills`` (the
D13 ``sleeve_fills`` journal it owns).
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
    order_ids: Sequence[int] = field(default_factory=tuple)


@dataclass(frozen=True)
class Execution:
    """One broker execution as journaled by trader_service (an ORDER_FILLED event)."""

    exec_id: str
    order_id: int
    symbol: str
    action: str            # 'BUY' | 'SELL'
    quantity: float        # unsigned shares filled by this execution
    price: Optional[float] # None when the broker gave quantity without a usable price
    time: dt.datetime
    commission: Optional[float] = None
    event_id: Optional[int] = None


@dataclass(frozen=True)
class Fill:
    """One row of the D13 ``sleeve_fills`` journal."""

    sleeve: str
    ticker: str
    trade_date: dt.date
    quantity: float        # signed: + buy, - sell
    price: Optional[float]
    commission: Optional[float]
    exec_id: str           # primary key: idempotency
    order_id: str
    proposal_id: int
    recorded_at: dt.datetime


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

    def account_net_liquidation(self) -> Optional[float]:
        """The account's NetLiquidation, or None when it cannot be read (never a guess)."""
        ...

    def executions(self) -> Sequence[Execution]:
        """Every journaled broker execution. Must raise when they cannot be read."""
        ...


class Pricer(Protocol):
    def price(self, ticker: str) -> Optional[float]:
        """A finite positive price for ``ticker`` or None when it cannot be priced."""
        ...


class FillsJournal(Protocol):
    def sleeve_holdings(self, sleeve: str) -> Mapping[str, float]:
        """Per-ticker signed quantity sums of ``sleeve_fills`` for the sleeve (D13)."""
        ...

    def record_fills(self, fills: Sequence[Fill]) -> int:
        """Insert fills not yet journaled (by exec_id); return how many were new."""
        ...


class Proposer(Protocol):
    def ensure_group(self, group: str, description: str) -> None:
        """Create the sleeve's position group if it does not exist (idempotent)."""
        ...

    def propose(self, *, symbol: str, action: str, quantity: float, execution: Any,
                reasoning: str, source: str, group: str, metadata: Mapping[str, Any]) -> int:
        """Create ONE PENDING proposal through the existing pipeline; return its id."""
        ...


class Gateway(ProposalReader, Broker, Pricer, FillsJournal, Proposer, Protocol):
    """Everything at once — what the CLI is handed."""
