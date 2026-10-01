"""Production adapter: the ports implemented over :class:`trader.sdk.MMR`.

This is the ONLY module in the package that touches the SDK, and it uses just
these members: ``connect``/``close``, ``positions`` (IB book, read-only),
``snapshot`` (prices, read-only), ``_get_portfolio_state`` (NetLiquidation,
read-only), ``execution_snapshot`` (IB execution receipts + order observations,
read-only), ``propose`` (creates a PENDING proposal) and the proposal/group
stores for reads plus group creation. Sleeve fills live in the D13
``sleeve_fills`` table in the main DuckDB. Nothing here can approve, place,
cancel or close — see ``tests/test_rebalance_static.py``.

Broker executions are deliberately NOT read from the local event store: its
``ORDER_FILLED`` rows are cumulative fill deltas the order tracker derives from
``orderStatus`` messages (possibly without an execution id, stamped with the
observation time), not IB's execution receipts.
"""
from __future__ import annotations

import datetime as dt
import math
import sys
from typing import Any, Iterable, Mapping, Optional, Sequence

from trader.rebalance.ports import Execution, ExecutionEvidence, Fill, OrderObservation, ProposalRecord


class SdkGateway:
    """Read IB + the proposal store through the SDK; write only via ``propose`` and the journal."""

    def __init__(self, mmr: Any, *, connected: bool = False):
        self._mmr = mmr
        self._connected = connected

    @classmethod
    def connect(cls, config_file: str = '', timeout: int = 30) -> 'SdkGateway':
        from trader.sdk import MMR
        mmr = MMR(config_file=config_file, timeout=timeout)
        mmr.connect()
        return cls(mmr, connected=True)

    def close(self) -> None:
        if self._connected:
            self._mmr.close()
            self._connected = False

    def _duckdb_path(self) -> str:
        path = str(self._mmr._container.config().get('duckdb_path', '') or '')
        if not path:
            raise ValueError('duckdb_path not configured')
        return path

    # -- ProposalReader ----------------------------------------------------
    def proposals_in_group(self, group: str) -> Sequence[ProposalRecord]:
        store = self._mmr._proposal_store()
        out: list[ProposalRecord] = []
        for p in store.query(status=None, limit=1_000_000):
            if (p.group or '') != group:
                continue
            out.append(ProposalRecord(
                id=int(p.id or 0), symbol=str(p.symbol).upper(), action=str(p.action).upper(),
                quantity=p.quantity, status=str(p.status), source=str(p.source or ''),
                group=str(p.group or ''), created_at=p.created_at, metadata=dict(p.metadata or {}),
                order_ids=tuple(int(o) for o in (p.order_ids or [])),
            ))
        return out

    # -- Broker ------------------------------------------------------------
    def stock_positions(self) -> Mapping[str, float]:
        df = self._mmr.positions()
        if df is None:
            raise ConnectionError('positions() returned nothing')
        out: dict[str, float] = {}
        if df.empty:
            return out
        for _, row in df.iterrows():
            if str(row.get('secType', 'STK')) != 'STK':
                continue
            sym = str(row['symbol']).upper()
            out[sym] = out.get(sym, 0.0) + float(row['position'])
        return out

    def account_net_liquidation(self) -> Optional[float]:
        state = self._mmr._get_portfolio_state()
        if not state.net_liquidation_evaluable:
            return None
        value = float(state.net_liquidation)
        return value if math.isfinite(value) and value > 0 else None

    def execution_evidence(self, order_ids: Sequence[int]) -> ExecutionEvidence:
        """trader_service's read-only ``execution_snapshot`` scoped to the sleeve's broker order ids."""
        ids = sorted({int(o) for o in order_ids})
        if not ids:
            return ExecutionEvidence(account='', account_confirmed=True, receipts_available=True,
                                     history_complete=True, orders_complete=True, observed_at='',
                                     orders=(), executions=(), complete=True, replay_scope='nothing requested')
        snap = self._mmr.execution_snapshot(order_ids=ids)
        if not isinstance(snap, Mapping):
            raise ConnectionError(f'execution_snapshot returned {type(snap).__name__}, not a snapshot')
        return evidence_from_snapshot(snap, ids)

    # -- FillsJournal -------------------------------------------------------
    def sleeve_holdings(self, sleeve: str) -> Mapping[str, float]:
        from trader.rebalance.fills import SleeveFillsStore
        return SleeveFillsStore(self._duckdb_path()).holdings(sleeve)

    def record_fills(self, fills: Sequence[Fill]) -> int:
        from trader.rebalance.fills import SleeveFillsStore
        return SleeveFillsStore(self._duckdb_path()).record(fills)

    # -- Pricer ------------------------------------------------------------
    def price(self, ticker: str) -> Optional[float]:
        snap = self._mmr.snapshot(ticker, exchange=US_EXCHANGE, currency=US_CURRENCY)
        return price_from_snapshot(snap)

    # -- Proposer ----------------------------------------------------------
    def ensure_group(self, group: str, description: str) -> None:
        gs = self._mmr._group_store()
        if gs.get_group(group) is None:
            gs.create_group(group, description=description)

    def propose(self, *, symbol: str, action: str, quantity: float, execution: Any,
                reasoning: str, source: str, group: str, metadata: Mapping[str, Any]) -> int:
        proposal_id, _leverage, _snap = self._mmr.propose(
            symbol=symbol, action=action, quantity=float(quantity), execution=execution,
            reasoning=reasoning, source=source, group=group, metadata=dict(metadata),
            exchange=US_EXCHANGE, currency=US_CURRENCY,
        )
        return int(proposal_id)


# The sleeve trades the US point-in-time universe only. A bare symbol is ambiguous at IB for
# many US names (EIX also lists on FWB in EUR, LEN is also Lenovo on EBS, SE also a CAD name on
# PURE, SYY also FWB/MEXI): unpinned, pricing refused the first live plan and an approved
# proposal would have failed at resolution. Pin every price and proposal to the US listing.
US_EXCHANGE = 'SMART'
US_CURRENCY = 'USD'


def _num(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return number if math.isfinite(number) else 0.0


def _price(value: Any) -> Optional[float]:
    number = _num(value)
    # ib_async's UNSET_DOUBLE (sys.float_info.max) is finite and positive: not a price.
    return number if 0 < number < sys.float_info.max else None


def _time(value: Any) -> Optional[dt.datetime]:
    """An IB execution time from its receipt. Naive values are UTC (ib_async's convention)."""
    if isinstance(value, dt.datetime):
        parsed = value
    elif isinstance(value, str) and value:
        try:
            parsed = dt.datetime.fromisoformat(value[:-1] + '+00:00' if value.endswith('Z') else value)
        except ValueError:
            return None
    else:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=dt.timezone.utc)


def evidence_from_snapshot(snap: Mapping[str, Any], order_ids: Iterable[int]) -> ExecutionEvidence:
    """Map the ``execution_snapshot`` RPC reply onto :class:`ExecutionEvidence`, scoped to ``order_ids``.

    Completeness flags — including the runtime's overall ``complete`` verdict —
    are copied, never inferred: a missing key reads as False. Receipts and
    observations for other order ids or another account are dropped.
    """
    wanted = {int(o) for o in order_ids}
    account = str(snap.get('account') or '')
    replay = snap.get('execution_replay') or {}
    orders: list[OrderObservation] = []
    for row in snap.get('orders') or []:
        if not isinstance(row, Mapping):
            continue
        oid = int(row.get('orderId') or 0)
        if oid not in wanted:
            continue
        row_account = str(row.get('account') or '')
        if account and row_account and row_account != account:
            continue
        orders.append(OrderObservation(
            order_id=oid, symbol=str(row.get('symbol') or '').upper(), action=str(row.get('action') or '').upper(),
            status=str(row.get('status') or ''), filled=_num(row.get('filled')),
            fill_quantity_known=bool(row.get('fillQuantityKnown', False)), con_id=int(row.get('conId') or 0),
            identity_ambiguous=bool(row.get('identityAmbiguous', False)),
            total_quantity=_num(row.get('totalQuantity'))))
    executions: list[Execution] = []
    for rec in snap.get('executions') or []:
        if not isinstance(rec, Mapping):
            continue
        oid = int(rec.get('orderId') or 0)
        exec_id = str(rec.get('execId') or '')
        if oid not in wanted or not exec_id:
            continue
        rec_account = str(rec.get('account') or '')
        if account and rec_account and rec_account != account:
            continue
        executions.append(Execution(
            exec_id=exec_id, order_id=oid, shares=_num(rec.get('shares')), cum_qty=_num(rec.get('cumQty')),
            price=_price(rec.get('price')), time=_time(rec.get('time')), account=rec_account,
            perm_id=int(rec.get('permId') or 0)))
    return ExecutionEvidence(
        account=account, account_confirmed=bool(snap.get('account_confirmed', False)),
        receipts_available=bool(snap.get('execution_receipts_available', False)),
        history_complete=bool(snap.get('executions_complete', False)),
        orders_complete=bool(snap.get('orders_complete', False)),
        observed_at=str(snap.get('observed_at') or ''),
        orders=tuple(orders), executions=tuple(sorted(executions, key=lambda e: (e.order_id, e.cum_qty, e.exec_id))),
        complete=bool(snap.get('complete', False)),
        replay_scope=str(replay.get('scope') or '') if isinstance(replay, Mapping) else '')


def price_from_snapshot(snap: Mapping[str, Any] | None) -> Optional[float]:
    """last, else bid/ask midpoint, else close — or None. Never a guess."""
    if not snap:
        return None

    def _f(key: str) -> float:
        try:
            v = float(snap.get(key, float('nan')))
        except (TypeError, ValueError):
            return float('nan')
        return v if math.isfinite(v) and v > 0 else float('nan')

    last, bid, ask, close = _f('last'), _f('bid'), _f('ask'), _f('close')
    if math.isfinite(last):
        return last
    if math.isfinite(bid) and math.isfinite(ask) and ask >= bid:
        return (bid + ask) / 2.0
    if math.isfinite(close):
        return close
    return None
