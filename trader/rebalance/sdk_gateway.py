"""Production adapter: the four ports implemented over :class:`trader.sdk.MMR`.

This is the ONLY module in the package that touches the SDK, and it uses just
these members: ``connect``/``close``, ``positions`` (IB book, read-only),
``snapshot`` (prices, read-only), ``_get_portfolio_state`` (NetLiquidation,
read-only), ``propose`` (creates a PENDING proposal) and the proposal/group
stores for reads plus group creation. Broker executions come from the event
store's ORDER_FILLED journal and sleeve fills from the D13 ``sleeve_fills``
table, both in the main DuckDB. Nothing here can approve, place, cancel or
close — see ``tests/test_rebalance_static.py``.
"""
from __future__ import annotations

import math
from typing import Any, Mapping, Optional, Sequence

from trader.rebalance.ports import Execution, Fill, ProposalRecord


class SdkGateway:
    """Read IB + the proposal store through the SDK; write only via ``propose``."""

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

    def executions(self) -> Sequence[Execution]:
        """ORDER_FILLED events: trader_service's durable journal of broker executions."""
        from trader.data.event_store import EventStore
        store = EventStore(self._duckdb_path())
        out: list[Execution] = []
        for ev in store.list_fills(limit=1_000_000):
            meta = dict(ev.metadata or {})
            if meta.get('excluded_from_ledger'):
                continue
            exec_id = str(meta.get('execId') or '') or f'event:{ev.id}'
            price: Optional[float] = float(ev.price) if ev.price and math.isfinite(float(ev.price)) else None
            if meta.get('price_evaluable') is False:
                price = None
            out.append(Execution(exec_id=exec_id, order_id=int(ev.order_id), symbol=str(ev.symbol).upper(),
                                 action=str(ev.action).upper(), quantity=float(ev.quantity), price=price,
                                 time=ev.timestamp, commission=None, event_id=ev.id))
        return out

    # -- FillsJournal -------------------------------------------------------
    def sleeve_holdings(self, sleeve: str) -> Mapping[str, float]:
        from trader.rebalance.fills import SleeveFillsStore
        return SleeveFillsStore(self._duckdb_path()).holdings(sleeve)

    def record_fills(self, fills: Sequence[Fill]) -> int:
        from trader.rebalance.fills import SleeveFillsStore
        return SleeveFillsStore(self._duckdb_path()).record(fills)

    # -- Pricer ------------------------------------------------------------
    def price(self, ticker: str) -> Optional[float]:
        snap = self._mmr.snapshot(ticker)
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
        )
        return int(proposal_id)


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
