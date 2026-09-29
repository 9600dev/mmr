"""Fakes shared by tests/test_rebalance_*.py (not collected: no test_ prefix)."""
from __future__ import annotations

import copy
import datetime as dt
import json
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from trader.rebalance.ports import Execution, Fill, ProposalRecord

SHA = 'a' * 64
TODAY = dt.date(2026, 9, 29)          # a Tuesday; 2026-09-28 was the previous session
AS_OF = '2026-09-28'


def make_doc(**over: Any) -> dict:
    doc = {
        'schema': 'mmr.targets.v1',
        'sleeve': 's2p',
        'as_of': AS_OF,
        'generated_at': '2026-09-28T21:05:00Z',
        'strategy': {'family': 'equity', 'id': 's2p_top_quintile', 'module': 'strategies.lowfreq_s2p',
                     'class': 'S2PTopQuintile', 'params': {'quantile': 0.2}, 'source_sha256': SHA},
        'capital_usd': 10_000.0,
        'prices_as_of': AS_OF,
        'weights': {'AAPL': 0.5, 'MSFT': 0.3},
        'notes': 'test',
    }
    doc.update(copy.deepcopy(over))
    return doc


def write_targets(tmp_path: Path, doc: Mapping[str, Any] | None = None, name: str = f'{AS_OF}.json',
                  sleeve_dir: bool = False) -> Path:
    doc = dict(doc or make_doc())
    folder = tmp_path / doc['sleeve'] if sleeve_dir else tmp_path
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / name
    path.write_text(json.dumps(doc), encoding='utf-8')
    return path


def executed(pid: int, symbol: str, action: str, qty: float, group: str = 'sleeve_s2p',
             source: str = 'rebalance:s2p:2026-08-31', status: str = 'EXECUTED',
             order_ids: Sequence[int] | None = None) -> ProposalRecord:
    """A proposal record; EXECUTED ones carry a broker order id (900 + pid) unless told otherwise."""
    if order_ids is None:
        order_ids = (900 + pid,) if status == 'EXECUTED' else ()
    return ProposalRecord(id=pid, symbol=symbol, action=action, quantity=qty, status=status,
                          source=source, group=group, created_at=dt.datetime(2026, 9, 1, 10, 0),
                          metadata={'submission_quantity': float(qty), 'con_id': 1}, order_ids=tuple(order_ids))


def execution(exec_id: str, order_id: int, symbol: str, action: str, qty: float, price: float | None = 100.0,
              when: dt.datetime | None = None) -> Execution:
    return Execution(exec_id=exec_id, order_id=order_id, symbol=symbol, action=action, quantity=qty,
                     price=price, time=when or dt.datetime(2026, 9, 1, 14, 30, tzinfo=dt.timezone.utc))


class FakeGateway:
    """Implements the ports over in-memory state and records every call.

    ``fills`` seeds the sleeve_fills journal (sleeve -> ticker -> signed sum);
    ``record_fills`` appends to it exactly like the real store (exec_id idempotent).
    """

    FORBIDDEN = ('approve', 'buy', 'sell', 'cancel', 'cancel_all', 'close_position', 'reject',
                 'place_order', 'place_expressive_order', 'place_standalone_order',
                 'place_protective_order', 'emergency_close_position')

    def __init__(self, proposals: Sequence[ProposalRecord] = (), positions: Optional[Mapping[str, float]] = None,
                 prices: Optional[Mapping[str, Optional[float]]] = None, fail_positions: bool = False,
                 fills: Optional[Mapping[str, Mapping[str, float]]] = None,
                 executions: Sequence[Execution] = (), fail_executions: bool = False,
                 net_liquidation: Optional[float] = None):
        self.proposals = list(proposals)
        self.positions = dict(positions or {})
        self.prices = dict(prices or {})
        self.fail_positions = fail_positions
        self.fail_executions = fail_executions
        self.journal: dict[str, dict[str, float]] = {s: dict(t) for s, t in (fills or {}).items()}
        self.recorded: list[Fill] = []
        self.executions_ = list(executions)
        self.net_liquidation = net_liquidation
        self.calls: list[tuple[str, dict]] = []
        self.groups: dict[str, str] = {}
        self.closed = False
        self._next_id = 100

    def __getattr__(self, name: str):
        if name in self.FORBIDDEN:
            raise AssertionError(f'rebalancer invoked forbidden gateway method {name!r}')
        raise AttributeError(name)

    def proposals_in_group(self, group: str) -> Sequence[ProposalRecord]:
        self.calls.append(('proposals_in_group', {'group': group}))
        return [p for p in self.proposals if p.group == group]

    def stock_positions(self) -> Mapping[str, float]:
        self.calls.append(('stock_positions', {}))
        if self.fail_positions:
            raise ConnectionError('trader_service unreachable')
        return dict(self.positions)

    def account_net_liquidation(self) -> Optional[float]:
        self.calls.append(('account_net_liquidation', {}))
        return self.net_liquidation

    def executions(self) -> Sequence[Execution]:
        self.calls.append(('executions', {}))
        if self.fail_executions:
            raise ConnectionError('event store unreadable')
        return list(self.executions_)

    def price(self, ticker: str) -> Optional[float]:
        self.calls.append(('price', {'ticker': ticker}))
        return self.prices.get(ticker)

    def sleeve_holdings(self, sleeve: str) -> Mapping[str, float]:
        self.calls.append(('sleeve_holdings', {'sleeve': sleeve}))
        return {t: q for t, q in self.journal.get(sleeve, {}).items() if q != 0}

    def record_fills(self, fills: Sequence[Fill]) -> int:
        self.calls.append(('record_fills', {'n': len(fills)}))
        seen = {f.exec_id for f in self.recorded}
        inserted = 0
        for f in fills:
            if f.exec_id in seen:
                continue
            seen.add(f.exec_id)
            self.recorded.append(f)
            book = self.journal.setdefault(f.sleeve, {})
            book[f.ticker] = book.get(f.ticker, 0.0) + f.quantity
            inserted += 1
        return inserted

    def ensure_group(self, group: str, description: str) -> None:
        self.calls.append(('ensure_group', {'group': group, 'description': description}))
        self.groups.setdefault(group, description)

    def propose(self, **kw: Any) -> int:
        self.calls.append(('propose', dict(kw)))
        self._next_id += 1
        self.proposals.append(ProposalRecord(
            id=self._next_id, symbol=kw['symbol'], action=kw['action'], quantity=kw['quantity'],
            status='PENDING', source=kw['source'], group=kw['group'], metadata=dict(kw['metadata'])))
        return self._next_id

    def close(self) -> None:
        self.closed = True

    @property
    def propose_calls(self) -> list[dict]:
        return [kw for name, kw in self.calls if name == 'propose']

    @property
    def method_names(self) -> set[str]:
        return {name for name, _ in self.calls}


def held(sleeve: str = 's2p', **tickers: float) -> dict[str, dict[str, float]]:
    """Journal seed helper: held(AAPL=30, MSFT=5) -> {'s2p': {'AAPL': 30.0, 'MSFT': 5.0}}."""
    return {sleeve: {t: float(q) for t, q in tickers.items()}}
