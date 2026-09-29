"""The rebalancer's seams to the outside world.

Everything the rebalancer needs from IB, the proposal store, the price feed and
the fills journal comes through these narrow protocols, so tests drive the
whole package with fakes and the production adapter
(:mod:`trader.rebalance.sdk_gateway`) is the only module that touches
:class:`trader.sdk.MMR`.

By design (D15) there is NO protocol method that approves, places, cancels or
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
    """One IB execution receipt (``execDetails``), as replayed by trader_service.

    This is the broker's own record of one execution — never a cumulative fill
    delta the local order tracker derived from an ``orderStatus`` message.
    ``time`` is IB's execution time (not the time trader_service observed it).
    """

    exec_id: str
    order_id: int
    shares: float                  # unsigned shares of THIS execution
    cum_qty: float                 # cumulative shares of the order after it (0 = not reported)
    price: Optional[float]         # this execution's price; None when the receipt carried none
    time: Optional[dt.datetime]    # aware; None when the receipt carried no time
    account: str = ''
    perm_id: int = 0


@dataclass(frozen=True)
class OrderObservation:
    """The order tracker's view of one broker order (its status and cumulative fill)."""

    order_id: int
    symbol: str
    action: str                    # 'BUY' | 'SELL' | '' when the broker never said
    status: str                    # IB order status ('Filled', 'Cancelled', 'Submitted', 'Unknown', ...)
    filled: float                  # cumulative filled quantity the broker reported
    fill_quantity_known: bool      # False when the tracker could not establish the fill quantity
    con_id: int = 0
    identity_ambiguous: bool = False   # the same numeric id was seen from another broker client
    total_quantity: float = 0.0        # the order's totalQuantity as the broker reported it (0 = not reported)

    @property
    def terminal(self) -> bool:
        """The broker will not fill this order any further (Filled, or dead)."""
        return self.status in TERMINAL_ORDER_STATUSES


# IB order statuses after which no further execution can arrive (order_lifecycle's _FILLED | _DEAD).
TERMINAL_ORDER_STATUSES = frozenset({'Filled', 'Cancelled', 'ApiCancelled', 'Inactive'})


@dataclass(frozen=True)
class ExecutionEvidence:
    """What trader_service's read-only ``execution_snapshot`` says about a set of orders.

    ``receipts_available`` is False when the durable receipt journal could not
    be read; ``history_complete`` is False when IB's execution-history replay
    has not completed; ``orders_complete`` is False when the open-orders refresh
    failed or IB was disconnected. Any of them means the executions cannot be
    read and ``reconcile`` must refuse (D13). ``complete`` is the runtime's own
    overall verdict (all of the above, plus every requested order id known to
    its tracker and no ambiguous identity) — copied, never inferred, so a reason
    the per-order matching does not recognise still refuses. ``replay_scope``
    names IB's bounded window: absence of a receipt is evidence only inside it.
    """

    account: str
    account_confirmed: bool
    receipts_available: bool
    history_complete: bool
    orders_complete: bool
    observed_at: str
    orders: Sequence[OrderObservation]
    executions: Sequence[Execution]
    complete: bool
    replay_scope: str = ''


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

    def execution_evidence(self, order_ids: Sequence[int]) -> ExecutionEvidence:
        """IB execution receipts + order observations for ``order_ids`` (read-only).

        Must raise when the snapshot cannot be taken at all; completeness of
        what came back is reported on the evidence, not assumed.
        """
        ...


class Pricer(Protocol):
    def price(self, ticker: str) -> Optional[float]:
        """A finite positive price for ``ticker`` or None when it cannot be priced."""
        ...


class FillsJournal(Protocol):
    def sleeve_holdings(self, sleeve: str) -> Mapping[str, float]:
        """Per-ticker signed quantity sums of ``sleeve_fills`` for the sleeve (D13).

        A journal that does not exist yet is an empty journal; reading it must
        not create it.
        """
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
