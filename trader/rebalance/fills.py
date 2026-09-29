"""The D13 ``sleeve_fills`` journal and the ``reconcile`` that fills it.

The proposal store records submissions, not fills, so a sleeve's holdings are
the per-ticker sums of ``sleeve_fills`` (in the MAIN DuckDB, ``duckdb_path``),
reconciled against IB. C01 owns writing the table; C02 reads it and creates it
in its tests from :data:`SLEEVE_FILLS_DDL`.

``reconcile`` matches IB's execution receipts — read through trader_service's
read-only ``execution_snapshot`` — to the broker order ids of the sleeve's
EXECUTED proposals and inserts each execution once (``exec_id`` is the primary
key). It refuses rather than guesses (D13): when the receipts cannot be read,
when IB's history replay is incomplete, and when the receipts do not cover
what the broker says an order filled (an ``orderStatus`` seen before its
``execDetails``, or a fill outside IB's bounded history window).

Reads never create the table: ``plan``, ``status`` and ``reconcile --dry-run``
leave a database without ``sleeve_fills`` exactly as they found it.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Iterable, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

from trader.rebalance.ports import Execution, ExecutionEvidence, Fill, Gateway, ProposalRecord
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
TABLE = 'sleeve_fills'
_SESSION_TZ = ZoneInfo('America/New_York')
_EPS = 1e-9


class ExecutionMismatch(ValueError):
    """A broker observation contradicts the proposal it is matched to."""


class ExecutionsIncomplete(ValueError):
    """The receipts do not account for everything the broker says filled."""


def trade_date_of(when: dt.datetime) -> dt.date:
    """The US session date of an IB execution time (a naive value is UTC)."""
    if when.tzinfo is None:
        when = when.replace(tzinfo=dt.timezone.utc)
    return when.astimezone(_SESSION_TZ).date()


class SleeveFillsStore:
    """Read/write ``sleeve_fills`` through the project's atomic DuckDB wrapper.

    Construction and reads run no DDL: a missing table is an empty journal.
    Only :meth:`record` (the reconcile write path) creates the schema.
    """

    def __init__(self, duckdb_path: str):
        from trader.data.duckdb_store import DuckDBConnection
        self.duckdb_path = duckdb_path
        self.db = DuckDBConnection.get_instance(duckdb_path)

    @staticmethod
    def _exists(conn) -> bool:
        row = conn.execute('SELECT count(*) FROM duckdb_tables() WHERE table_name = ?', [TABLE]).fetchone()
        return bool(row and row[0])

    def exists(self) -> bool:
        return bool(self.db.execute_atomic(self._exists))

    def record(self, fills: Sequence[Fill]) -> int:
        """Insert fills whose exec_id is not yet journaled. Returns the number inserted.

        The schema is created here, in the same transaction as the first insert;
        an empty batch writes nothing and creates nothing.
        """
        if not fills:
            return 0

        def _insert(conn) -> int:
            conn.execute(SLEEVE_FILLS_DDL)
            inserted = 0
            for f in fills:
                exists = conn.execute(f'SELECT 1 FROM {TABLE} WHERE exec_id = ?', [f.exec_id]).fetchone()
                if exists:
                    continue
                conn.execute(
                    f'INSERT INTO {TABLE} (sleeve, ticker, trade_date, quantity, price, commission, '
                    'exec_id, order_id, proposal_id, recorded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    [f.sleeve, f.ticker, f.trade_date, float(f.quantity), f.price, f.commission,
                     f.exec_id, str(f.order_id), int(f.proposal_id), f.recorded_at])
                inserted += 1
            return inserted

        return int(self.db.execute_atomic(_insert))

    def holdings(self, sleeve: str) -> dict[str, float]:
        def _read(conn) -> list:
            if not self._exists(conn):
                return []
            return conn.execute(f'SELECT ticker, SUM(quantity) FROM {TABLE} WHERE sleeve = ? '
                                'GROUP BY ticker ORDER BY ticker', [sleeve]).fetchall()
        return {str(t): float(q) for t, q in self.db.execute_atomic(_read)}

    def list_fills(self, sleeve: str) -> list[Fill]:
        def _read(conn) -> list:
            if not self._exists(conn):
                return []
            return conn.execute(
                'SELECT sleeve, ticker, trade_date, quantity, price, commission, exec_id, order_id, proposal_id, '
                f'recorded_at FROM {TABLE} WHERE sleeve = ? ORDER BY trade_date, recorded_at, exec_id',
                [sleeve]).fetchall()
        return [Fill(sleeve=r[0], ticker=r[1], trade_date=r[2], quantity=float(r[3]),
                     price=None if r[4] is None else float(r[4]),
                     commission=None if r[5] is None else float(r[5]),
                     exec_id=str(r[6]), order_id=str(r[7]), proposal_id=int(r[8]), recorded_at=r[9])
                for r in self.db.execute_atomic(_read)]


@dataclass(frozen=True)
class UnfilledOrder:
    proposal_id: int
    order_id: int
    broker_status: str        # the tracker's status for the order, or 'unknown' when it never saw it


@dataclass(frozen=True)
class MatchResult:
    fills: Sequence[Fill]
    unfilled: Sequence[UnfilledOrder]                 # EXECUTED proposals whose orders have no receipt
    proposals_without_order_ids: Sequence[int]
    warnings: Sequence[str] = field(default_factory=tuple)

    @property
    def unfilled_proposal_ids(self) -> list[int]:
        return sorted({u.proposal_id for u in self.unfilled})


@dataclass(frozen=True)
class ReconcileReport:
    sleeve: str
    matched: Sequence[Fill]                       # executions belonging to the sleeve's EXECUTED proposals
    inserted: int                                 # how many were new (0 on --dry-run)
    dry_run: bool
    unfilled_proposals: Sequence[int]             # EXECUTED proposals with no execution receipt yet
    proposals_without_order_ids: Sequence[int]    # EXECUTED proposals that carry no broker order id
    holdings: Mapping[str, float]                 # journal sums after the run (before, on --dry-run)
    ib_positions: Mapping[str, float]
    warnings: Sequence[str] = field(default_factory=tuple)
    unfilled: Sequence[UnfilledOrder] = field(default_factory=tuple)
    evidence_observed_at: str = ''
    replay_scope: str = ''

    @property
    def matched_by_symbol(self) -> dict[str, float]:
        out: dict[str, float] = {}
        for f in self.matched:
            out[f.ticker] = out.get(f.ticker, 0.0) + f.quantity
        return out


def executed_order_ids(records: Iterable[ProposalRecord]) -> tuple[dict[int, ProposalRecord], list[int]]:
    """Broker order id -> EXECUTED proposal, plus the EXECUTED proposals carrying no order id.

    Only EXECUTED proposals count (D13): an APPROVED-but-unconfirmed submission
    is not inventory and a FAILED one placed nothing the sleeve may keep.
    """
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
    return order_to_proposal, sorted(without_ids)


def match_executions(sleeve: str, records: Iterable[ProposalRecord], evidence: ExecutionEvidence,
                     now: Optional[dt.datetime] = None) -> MatchResult:
    """Pair IB execution receipts with the sleeve's EXECUTED proposals by broker order id.

    One journal row per receipt, dated by IB's execution time. Raises
    :class:`ExecutionMismatch` when the broker's order observation contradicts
    the proposal (symbol or side), and :class:`ExecutionsIncomplete` when the
    receipts of an order do not add up to what the broker says filled — that is
    the ``orderStatus``-before-``execDetails`` race, or a fill outside IB's
    bounded history window; either way there is nothing to journal yet.
    """
    now = now or dt.datetime.now(dt.timezone.utc)
    order_to_proposal, without_ids = executed_order_ids(records)
    fills: list[Fill] = []
    unfilled: list[UnfilledOrder] = []
    problems: list[str] = []
    warnings: list[str] = []
    for oid in sorted(order_to_proposal):
        rec = order_to_proposal[oid]
        observations = [o for o in evidence.orders if o.order_id == oid]
        receipts = sorted((e for e in evidence.executions if e.order_id == oid), key=lambda e: (e.cum_qty, e.exec_id))
        for obs in observations:
            if obs.symbol and obs.symbol != rec.symbol.upper():
                raise ExecutionMismatch(f'broker order {oid} is {obs.symbol}, but proposal #{rec.id} is '
                                        f'{rec.symbol} — refusing to journal a mismatch')
            if obs.action and obs.action != rec.action.upper():
                raise ExecutionMismatch(f'broker order {oid} is a {obs.action} but proposal #{rec.id} is a {rec.action}')
            if obs.identity_ambiguous:
                problems.append(f'order {oid} (proposal #{rec.id}): broker identity is ambiguous')
        covered = sum(e.shares for e in receipts)
        cum_max = max((e.cum_qty for e in receipts), default=0.0)
        if receipts and cum_max > 0 and abs(covered - cum_max) > _EPS:
            problems.append(f'order {oid} (proposal #{rec.id}): receipts cover {covered:g} of {cum_max:g} '
                            f'cumulative shares — an execution is missing')
        known = [o.filled for o in observations if o.fill_quantity_known]
        if known and max(known) > covered + _EPS:
            problems.append(f'order {oid} (proposal #{rec.id}): broker reports {max(known):g} filled but the '
                            f'execution receipts cover {covered:g} (execDetails not yet replayed, or the fill '
                            f'lies outside IB\'s history window)')
        if not observations and receipts:
            warnings.append(f'order {oid} (proposal #{rec.id}): no broker order observation; symbol and side '
                            f'taken from the proposal, receipts matched by order id')
        for e in receipts:
            if e.shares <= 0:
                problems.append(f'order {oid}: receipt {e.exec_id} reports {e.shares:g} shares')
                continue
            if e.time is None:
                problems.append(f'order {oid}: receipt {e.exec_id} carries no execution time')
                continue
            signed = float(e.shares) if rec.action.upper() == 'BUY' else -float(e.shares)
            fills.append(Fill(sleeve=sleeve, ticker=rec.symbol.upper(), trade_date=trade_date_of(e.time),
                              quantity=signed, price=e.price, commission=None, exec_id=e.exec_id,
                              order_id=str(oid), proposal_id=rec.id, recorded_at=now))
        if not receipts:
            status = next((o.status for o in observations if o.status), 'unknown')
            unfilled.append(UnfilledOrder(proposal_id=rec.id, order_id=oid, broker_status=status))
    if problems:
        raise ExecutionsIncomplete('; '.join(problems))
    return MatchResult(fills=tuple(fills), unfilled=tuple(unfilled), proposals_without_order_ids=tuple(without_ids),
                       warnings=tuple(warnings))


def reconcile_sleeve(sleeve: str, gateway: Gateway, *, dry_run: bool = False,
                     now: Optional[dt.datetime] = None) -> ReconcileReport:
    """Journal every IB execution of the sleeve's EXECUTED proposals exactly once — or refuse."""
    from trader.rebalance.plan import Refusal
    records = list(gateway.proposals_in_group(sleeve_group(sleeve)))
    order_to_proposal, _ = executed_order_ids(records)
    try:
        evidence = gateway.execution_evidence(sorted(order_to_proposal))
    except Exception as exc:
        raise Refusal('EXECUTIONS_UNREADABLE', f'cannot read broker executions: {exc}') from exc
    if order_to_proposal:
        if not evidence.receipts_available:
            raise Refusal('EXECUTIONS_UNREADABLE', 'trader_service could not read its execution receipts '
                                                   '(journal degraded); nothing journaled')
        if not evidence.history_complete:
            raise Refusal('EXECUTIONS_UNREADABLE', "IB's execution-history replay is incomplete; "
                                                   'receipts may be missing — nothing journaled')
        if not evidence.account_confirmed:
            raise Refusal('EXECUTIONS_UNREADABLE', 'the broker has not confirmed the trading account; '
                                                   'nothing journaled')
    try:
        ib_positions = dict(gateway.stock_positions())
    except Exception as exc:
        raise Refusal('IB_UNREADABLE', f'cannot read IB positions: {exc}') from exc
    try:
        match = match_executions(sleeve, records, evidence, now=now)
    except ExecutionMismatch as exc:
        raise Refusal('EXECUTION_MISMATCH', str(exc)) from exc
    except ExecutionsIncomplete as exc:
        raise Refusal('EXECUTIONS_INCOMPLETE', f'{exc}; run reconcile again once the receipts are in') from exc
    inserted = 0 if dry_run else gateway.record_fills(match.fills)
    holdings = dict(gateway.sleeve_holdings(sleeve))
    warnings: list[str] = list(match.warnings)
    for sym, qty in sorted(holdings.items()):
        ib_qty = float(ib_positions.get(sym, 0.0))
        if qty > ib_qty + _EPS:
            warnings.append(f'{sym}: journal holds {qty:g} but IB shows {ib_qty:g} — the sleeve is clamped to IB')
        if qty < -_EPS:
            warnings.append(f'{sym}: journal is net short {qty:g} — a long-only sleeve should never be')
    if match.proposals_without_order_ids:
        warnings.append(f'EXECUTED proposals without broker order ids (cannot be matched): '
                        f'{", ".join(f"#{i}" for i in match.proposals_without_order_ids)}')
    return ReconcileReport(sleeve=sleeve, matched=tuple(match.fills), inserted=inserted, dry_run=dry_run,
                           unfilled_proposals=tuple(match.unfilled_proposal_ids),
                           proposals_without_order_ids=tuple(match.proposals_without_order_ids),
                           holdings=holdings, ib_positions=ib_positions, warnings=tuple(warnings),
                           unfilled=tuple(match.unfilled), evidence_observed_at=evidence.observed_at,
                           replay_scope=evidence.replay_scope)
