"""Derived fill outcome for EXECUTED proposals.

EXECUTED is terminal in the proposal state machine (tests/invariants pin it:
``'EXECUTED': set()``) and means "orders were submitted", not "filled". An
order that was later cancelled with zero fill still leaves its proposal
EXECUTED, so the ledger alone can't tell a filled trade from a dud.

Rather than mutate the audit trail, the outcome is DERIVED at read time from
the order events trader_service persists in ``trading_events``
(ORDER_FILLED rows are fill deltas; ORDER_CANCELLED / ORDER_REJECTED carry
the broker status). Pure function — no I/O.
"""
from typing import Any, Iterable, Optional

FILLED = 'filled'
PARTIAL = 'partially_filled'
CANCELLED_UNFILLED = 'cancelled_unfilled'
REJECTED = 'rejected'
WORKING = 'working'
UNKNOWN = 'unknown'

_EPS = 1e-9


def _etype(ev: Any) -> str:
    t = getattr(ev, 'event_type', '')
    return str(getattr(t, 'value', t))


def fill_outcome(events: Iterable[Any], order_ids: Iterable[int],
                 target_quantity: Optional[float] = None) -> dict:
    """Summarise what actually happened to a proposal's orders.

    Args:
        events: TradingEvent-like objects (event_type, order_id, quantity,
            price, timestamp, metadata) for the proposal's orders.
        order_ids: the proposal's recorded order ids.
        target_quantity: the proposal's share quantity, if it had one.

    Returns:
        dict with ``outcome`` (filled | partially_filled | cancelled_unfilled |
        rejected | working | unknown), ``filled_quantity``, ``avg_fill_price``
        (None when nothing filled) and per-order ``orders`` detail.
    """
    ids = [int(o) for o in (order_ids or [])]
    per: dict = {oid: {'filled': 0.0, 'cum': 0.0, 'notional': 0.0, 'terminal': None,
                       'remaining': None, 'events': 0} for oid in ids}
    for ev in sorted(events, key=lambda e: (getattr(e, 'timestamp', None) is None,
                                            getattr(e, 'timestamp', None) or 0,
                                            getattr(e, 'id', 0) or 0)):
        oid = int(getattr(ev, 'order_id', 0) or 0)
        if oid not in per:
            continue
        o = per[oid]
        o['events'] += 1
        md = getattr(ev, 'metadata', None) or {}
        kind = _etype(ev)
        if kind == 'ORDER_FILLED':
            q = float(getattr(ev, 'quantity', 0) or 0)
            px = float(getattr(ev, 'price', 0) or 0)
            if q > 0:
                o['filled'] += q
                if px > 0:
                    o['notional'] += q * px
            cum = md.get('cumulative_filled')
            if isinstance(cum, (int, float)):
                o['cum'] = max(o['cum'], float(cum))
        elif kind in ('ORDER_CANCELLED', 'ORDER_REJECTED'):
            o['terminal'] = 'rejected' if kind == 'ORDER_REJECTED' else 'cancelled'
        if isinstance(md.get('remaining'), (int, float)):
            o['remaining'] = float(md['remaining'])

    orders = {}
    total = 0.0
    notional = 0.0
    for oid, o in per.items():
        filled = max(o['filled'], o['cum'])
        total += filled
        notional += o['notional']
        done = (o['remaining'] is not None and o['remaining'] <= _EPS and filled > _EPS)
        state = ('filled' if done else o['terminal'] if o['terminal'] else
                 'working' if o['events'] else 'no_events')
        orders[oid] = {'filled_quantity': filled, 'state': state}

    if not ids or all(o['events'] == 0 for o in per.values()):
        outcome = UNKNOWN
    elif total > _EPS:
        if target_quantity is not None and target_quantity == target_quantity and target_quantity > 0:
            complete = total + _EPS >= float(target_quantity)
        else:
            complete = all(v['state'] == 'filled' for v in orders.values())
        outcome = FILLED if complete else PARTIAL
    else:
        states = {v['state'] for v in orders.values()}
        if states <= {'cancelled', 'rejected'}:
            outcome = REJECTED if states == {'rejected'} else CANCELLED_UNFILLED
        else:
            outcome = WORKING
    filled_px = sum(per[o]['filled'] for o in per if per[o]['notional'] > 0)
    return {
        'outcome': outcome,
        'filled_quantity': total,
        'avg_fill_price': (notional / filled_px) if filled_px > _EPS else None,
        'orders': orders,
    }
