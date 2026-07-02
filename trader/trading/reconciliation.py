"""Broker-truth reconciliation.

After a trader_service restart (or a crash mid-approve), internal state — the
proposal store and the in-memory book — can diverge from what IB actually did:
a proposal marked EXECUTED whose order never filled, an APPROVED proposal whose
order is quietly live (an approve() that timed out), or a position left without a
protective order.

This module cross-checks recent non-terminal / recently-executed proposals and
current positions against live IB open-orders + executions + positions and
produces a report of divergences.

REPORT-ONLY: it never places or cancels orders and does not mutate proposal
status. It surfaces divergence for a human / the LLM loop to act on. (Auto-repair
of unambiguous proposal-status divergences is a deliberate future extension,
gated behind an explicit flag.)

The core ``reconcile()`` is pure — it takes already-normalised dicts + proposals
and returns a report — so it is unit-testable without IB. The live wrapper on the
Trader normalises IB objects into those dicts.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

# IB order statuses that mean the order is still working at the broker.
_WORKING = {'PreSubmitted', 'Submitted', 'PendingSubmit', 'ApiPending', 'PendingCancel'}
# Order types that protect an existing position (stop / trailing stop / take-profit).
_PROTECTIVE_TYPES = {'STP', 'STP LMT', 'TRAIL', 'TRAIL LIMIT', 'LMT'}

# Only reconcile proposals touched within this window — IB's reqExecutions only
# returns the recent (same-session) fills, so an old EXECUTED proposal legitimately
# has no execution record and must not be flagged as divergent.
_RECENT_DAYS = 2


@dataclass
class ReconciliationFinding:
    kind: str            # 'executed_not_filled' | 'approved_working' | ...
    severity: str        # 'critical' | 'warning' | 'info'
    symbol: str
    detail: str
    proposal_id: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ReconciliationReport:
    findings: List[ReconciliationFinding] = field(default_factory=list)
    checked_proposals: int = 0
    checked_positions: int = 0
    ib_open_orders: int = 0
    ib_executions: int = 0

    @property
    def diverged(self) -> bool:
        return any(f.severity in ('critical', 'warning') for f in self.findings)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'diverged': self.diverged,
            'checked_proposals': self.checked_proposals,
            'checked_positions': self.checked_positions,
            'ib_open_orders': self.ib_open_orders,
            'ib_executions': self.ib_executions,
            'findings': [f.to_dict() for f in self.findings],
        }


def _is_recent(when: Optional[dt.datetime], now: dt.datetime) -> bool:
    if when is None:
        return True  # unknown age → don't skip (conservative: check it)
    try:
        return (now - when) <= dt.timedelta(days=_RECENT_DAYS)
    except Exception:
        return True


def reconcile(
    proposals: List[Any],
    open_orders: List[Dict[str, Any]],
    executions: List[Dict[str, Any]],
    positions: List[Dict[str, Any]],
    now: Optional[dt.datetime] = None,
) -> ReconciliationReport:
    """Pure reconciliation core.

    ``proposals`` are TradeProposal-like objects (need: status, order_ids, symbol,
    action, quantity, id, updated_at). ``open_orders`` / ``executions`` /
    ``positions`` are normalised dicts (see the live wrapper for the shape).
    """
    now = now or dt.datetime.now()
    report = ReconciliationReport(
        ib_open_orders=len(open_orders),
        ib_executions=len(executions),
    )

    open_by_id = {o['order_id']: o for o in open_orders if o.get('order_id')}
    open_ids = set(open_by_id)
    filled_ids = {e['order_id'] for e in executions if e.get('order_id')}

    # Recent executions/open orders indexed by (symbol, action) for proposals that
    # have no reliable order_id link (APPROVED that never reached EXECUTED).
    def _norm_action(a: str) -> str:
        a = (a or '').upper()
        if a in ('BOT', 'BUY'):
            return 'BUY'
        if a in ('SLD', 'SELL'):
            return 'SELL'
        return a

    fills_by_key: Dict[tuple, int] = {}
    for e in executions:
        key = (e.get('symbol', ''), _norm_action(e.get('side', '')))
        fills_by_key[key] = fills_by_key.get(key, 0) + 1
    open_by_key: Dict[tuple, int] = {}
    for o in open_orders:
        key = (o.get('symbol', ''), _norm_action(o.get('action', '')))
        open_by_key[key] = open_by_key.get(key, 0) + 1

    for p in proposals:
        status = getattr(p, 'status', '')
        if status not in ('EXECUTED', 'APPROVED'):
            continue
        if not _is_recent(getattr(p, 'updated_at', None), now):
            continue
        report.checked_proposals += 1
        pid = getattr(p, 'id', None)
        symbol = getattr(p, 'symbol', '') or ''
        action = _norm_action(getattr(p, 'action', ''))
        order_ids = list(getattr(p, 'order_ids', []) or [])

        if status == 'EXECUTED':
            if order_ids:
                if any(oid in filled_ids for oid in order_ids):
                    continue  # confirmed filled — no finding
                if any(oid in open_ids for oid in order_ids):
                    report.findings.append(ReconciliationFinding(
                        kind='executed_still_working', severity='warning',
                        symbol=symbol, proposal_id=pid,
                        detail=(f'proposal marked EXECUTED but its order(s) {order_ids} are '
                                f'still WORKING at IB (not filled)')))
                else:
                    report.findings.append(ReconciliationFinding(
                        kind='executed_no_broker_record', severity='warning',
                        symbol=symbol, proposal_id=pid,
                        detail=(f'proposal marked EXECUTED but IB has no open order or '
                                f'recent fill for order(s) {order_ids} — verify manually')))
            # EXECUTED without order_ids: nothing reliable to check.
        elif status == 'APPROVED':
            # An APPROVED proposal that never advanced to EXECUTED: the approve()
            # may have timed out with the order actually live. Match heuristically.
            key = (symbol, action)
            if open_by_key.get(key):
                report.findings.append(ReconciliationFinding(
                    kind='approved_order_working', severity='critical',
                    symbol=symbol, proposal_id=pid,
                    detail=(f'proposal still APPROVED but a matching {action} {symbol} order '
                            f'is WORKING at IB — the approve likely timed out with a live '
                            f'order; reconcile before re-approving')))
            elif fills_by_key.get(key):
                report.findings.append(ReconciliationFinding(
                    kind='approved_likely_filled', severity='critical',
                    symbol=symbol, proposal_id=pid,
                    detail=(f'proposal still APPROVED but a matching {action} {symbol} fill '
                            f'exists at IB — the order likely executed; do NOT re-approve')))
            else:
                report.findings.append(ReconciliationFinding(
                    kind='approved_no_broker_record', severity='warning',
                    symbol=symbol, proposal_id=pid,
                    detail=(f'proposal APPROVED but no matching IB order/fill found — the '
                            f'order may have failed; safe to re-evaluate')))

    # Positions without a protective order.
    for pos in positions:
        qty = float(pos.get('position', 0) or 0)
        if qty == 0:
            continue
        report.checked_positions += 1
        con_id = pos.get('conId')
        symbol = pos.get('symbol', '') or ''
        exit_action = 'SELL' if qty > 0 else 'BUY'
        protective = [
            o for o in open_orders
            if o.get('conId') == con_id
            and _norm_action(o.get('action', '')) == exit_action
            and str(o.get('orderType', '')).upper() in _PROTECTIVE_TYPES
        ]
        if not protective:
            report.findings.append(ReconciliationFinding(
                kind='unprotected_position', severity='warning',
                symbol=symbol,
                detail=(f'position {qty:g} {symbol} has no protective '
                        f'({exit_action}) stop/limit order at IB')))

    return report
