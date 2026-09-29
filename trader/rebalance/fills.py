"""The D13 ``sleeve_fills`` journal and the ``reconcile`` that fills it.

The proposal store records submissions, not fills, so a sleeve's holdings are
the per-ticker sums of ``sleeve_fills`` (in the MAIN DuckDB, ``duckdb_path``),
reconciled against IB. C01 owns writing the table; C02 reads it and creates it
in its tests from :data:`SLEEVE_FILLS_DDL`.

``reconcile`` matches journaled broker executions (trader_service's
``ORDER_FILLED`` events) to the order ids of the sleeve's EXECUTED proposals and
inserts each execution once (``exec_id`` is the primary key). If executions
cannot be read it refuses rather than guessing.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Iterable, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

from trader.rebalance.ports import Execution, Fill, Gateway, ProposalRecord
from trader.rebalance.targets import sleeve_group

# D13 schema. exec_id is the idempotency key: recording the same execution twice is a no-op.
SLEEVE_FILLS_DDL = """
CREATE TABLE IF NOT EXISTS sleeve_fills (
    sleeve      VARCHAR NOT NULL,
    ticker      VARCHAR NOT NULL,
    trade_date  DATE NOT NULL,
    quantity    DOUBLE NOT NULL,
    price       DOUBLE,
    commission  DOUBLE,
    exec_id     VARCHAR PRIMARY KEY,
    order_id    VARCHAR NOT NULL,
    proposal_id BIGINT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL
)
"""

_SESSION_TZ = ZoneInfo('America/New_York')


def trade_date_of(when: dt.datetime) -> dt.date:
    """The US session date of an execution timestamp.

    A naive timestamp is LOCAL time: the event store persists aware UTC
    timestamps as naive local wall time (14:30Z reads back as 07:30 on a PDT
    host), and ``astimezone`` on a naive value applies the system zone — the
    same zone the writing process used.
    """
    return when.astimezone(_SESSION_TZ).date()


class SleeveFillsStore:
    """Read/write ``sleeve_fills`` through the project's atomic DuckDB wrapper."""

    def __init__(self, duckdb_path: str):
        from trader.data.duckdb_store import DuckDBConnection
        self.duckdb_path = duckdb_path
        self.db = DuckDBConnection.get_instance(duckdb_path)
        self.db.execute_atomic(lambda conn: conn.execute(SLEEVE_FILLS_DDL))

    def record(self, fills: Sequence[Fill]) -> int:
        """Insert fills whose exec_id is not yet journaled. Returns the number inserted."""
        if not fills:
            return 0

        def _insert(conn) -> int:
            inserted = 0
            for f in fills:
                exists = conn.execute('SELECT 1 FROM sleeve_fills WHERE exec_id = ?', [f.exec_id]).fetchone()
                if exists:
                    continue
                conn.execute(
                    'INSERT INTO sleeve_fills (sleeve, ticker, trade_date, quantity, price, commission, '
                    'exec_id, order_id, proposal_id, recorded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    [f.sleeve, f.ticker, f.trade_date, float(f.quantity), f.price, f.commission,
                     f.exec_id, str(f.order_id), int(f.proposal_id), f.recorded_at])
                inserted += 1
            return inserted

        return int(self.db.execute_atomic(_insert))

    def holdings(self, sleeve: str) -> dict[str, float]:
        rows = self.db.execute(
            'SELECT ticker, SUM(quantity) FROM sleeve_fills WHERE sleeve = ? GROUP BY ticker ORDER BY ticker',
            [sleeve], fetch='all') or []
        return {str(t): float(q) for t, q in rows}

    def list_fills(self, sleeve: str) -> list[Fill]:
        rows = self.db.execute(
            'SELECT sleeve, ticker, trade_date, quantity, price, commission, exec_id, order_id, proposal_id, '
            'recorded_at FROM sleeve_fills WHERE sleeve = ? ORDER BY trade_date, recorded_at, exec_id',
            [sleeve], fetch='all') or []
        return [Fill(sleeve=r[0], ticker=r[1], trade_date=r[2], quantity=float(r[3]),
                     price=None if r[4] is None else float(r[4]),
                     commission=None if r[5] is None else float(r[5]),
                     exec_id=str(r[6]), order_id=str(r[7]), proposal_id=int(r[8]), recorded_at=r[9])
                for r in rows]


@dataclass(frozen=True)
class ReconcileReport:
    sleeve: str
    matched: Sequence[Fill]                       # executions belonging to the sleeve's EXECUTED proposals
    inserted: int                                 # how many were new (0 on --dry-run)
    dry_run: bool
    unfilled_proposals: Sequence[int]             # EXECUTED proposals with no execution journaled yet
    proposals_without_order_ids: Sequence[int]    # EXECUTED proposals that carry no broker order id
    holdings: Mapping[str, float]                 # journal sums after the run (before, on --dry-run)
    ib_positions: Mapping[str, float]
    warnings: Sequence[str] = field(default_factory=tuple)

    @property
    def matched_by_symbol(self) -> dict[str, float]:
        out: dict[str, float] = {}
        for f in self.matched:
            out[f.ticker] = out.get(f.ticker, 0.0) + f.quantity
        return out


def match_executions(sleeve: str, records: Iterable[ProposalRecord], executions: Iterable[Execution],
                     now: Optional[dt.datetime] = None) -> tuple[list[Fill], list[int], list[int]]:
    """Pair executions with the sleeve's EXECUTED proposals by broker order id.

    Returns (fills, unfilled proposal ids, proposal ids carrying no order id).
    Only EXECUTED proposals count (D13); an APPROVED-but-unconfirmed submission
    is not inventory and a FAILED one placed nothing the sleeve may keep.
    """
    now = now or dt.datetime.now(dt.timezone.utc)
    order_to_proposal: dict[int, ProposalRecord] = {}
    without_ids: list[int] = []
    for rec in records:
        if rec.status != 'EXECUTED':
            continue
        if not rec.order_ids:
            without_ids.append(rec.id)
            continue
        for oid in rec.order_ids:
            order_to_proposal[int(oid)] = rec
    fills: list[Fill] = []
    seen_proposals: set[int] = set()
    for ex in executions:
        rec = order_to_proposal.get(int(ex.order_id))
        if rec is None:
            continue
        if ex.symbol.upper() != rec.symbol.upper():
            raise ValueError(f'execution {ex.exec_id} for order {ex.order_id} is {ex.symbol}, '
                             f'but proposal #{rec.id} is {rec.symbol} — refusing to journal a mismatch')
        signed = float(ex.quantity) if ex.action.upper() == 'BUY' else -float(ex.quantity)
        if ex.action.upper() != rec.action.upper():
            raise ValueError(f'execution {ex.exec_id} is a {ex.action} but proposal #{rec.id} is a {rec.action}')
        fills.append(Fill(sleeve=sleeve, ticker=rec.symbol.upper(), trade_date=trade_date_of(ex.time),
                          quantity=signed, price=ex.price, commission=ex.commission, exec_id=ex.exec_id,
                          order_id=str(ex.order_id), proposal_id=rec.id, recorded_at=now))
        seen_proposals.add(rec.id)
    unfilled = sorted({r.id for r in order_to_proposal.values()} - seen_proposals)
    return fills, unfilled, sorted(without_ids)


def reconcile_sleeve(sleeve: str, gateway: Gateway, *, dry_run: bool = False,
                     now: Optional[dt.datetime] = None) -> ReconcileReport:
    """Journal every execution of the sleeve's EXECUTED proposals exactly once."""
    from trader.rebalance.plan import Refusal
    records = list(gateway.proposals_in_group(sleeve_group(sleeve)))
    try:
        executions = list(gateway.executions())
    except Exception as exc:
        raise Refusal('EXECUTIONS_UNREADABLE', f'cannot read broker executions: {exc}') from exc
    try:
        ib_positions = dict(gateway.stock_positions())
    except Exception as exc:
        raise Refusal('IB_UNREADABLE', f'cannot read IB positions: {exc}') from exc
    try:
        fills, unfilled, without_ids = match_executions(sleeve, records, executions, now=now)
    except ValueError as exc:
        raise Refusal('EXECUTION_MISMATCH', str(exc)) from exc
    inserted = 0 if dry_run else gateway.record_fills(fills)
    holdings = dict(gateway.sleeve_holdings(sleeve))
    warnings: list[str] = []
    for sym, qty in sorted(holdings.items()):
        ib_qty = float(ib_positions.get(sym, 0.0))
        if qty > ib_qty + 1e-9:
            warnings.append(f'{sym}: journal holds {qty:g} but IB shows {ib_qty:g} — the sleeve is clamped to IB')
        if qty < -1e-9:
            warnings.append(f'{sym}: journal is net short {qty:g} — a long-only sleeve should never be')
    if without_ids:
        warnings.append(f'EXECUTED proposals without broker order ids (cannot be matched): '
                        f'{", ".join(f"#{i}" for i in without_ids)}')
    return ReconcileReport(sleeve=sleeve, matched=tuple(fills), inserted=inserted, dry_run=dry_run,
                           unfilled_proposals=tuple(unfilled), proposals_without_order_ids=tuple(without_ids),
                           holdings=holdings, ib_positions=ib_positions, warnings=tuple(warnings))
