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


def _side(value: Any) -> str:
    """``'BUY'`` / ``'SELL'``, or ``''`` when ``value`` names neither."""
    side = str(value or '').strip().upper()
    return side if side in ('BUY', 'SELL') else ''


def fill_outcome(events: Iterable[Any], order_ids: Iterable[int],
                 target_quantity: Optional[float] = None,
                 action: Optional[str] = None) -> dict:
    """Summarise what actually happened to a proposal's ENTRY.

    A proposal with an exit spec records every order it placed: the entry and
    its take-profit / stop / trailing legs, which trade the OPPOSITE side. The
    outcome, ``filled_quantity`` and ``avg_fill_price`` describe the entry
    side only; exit-leg fills are reported apart. Summing both sides read a
    BUY 100 whose take-profit sold as "filled 200" at a blend of the two
    prices, a half-filled entry closed by its stop as fully filled, and an
    amount-sized bracket as never more than partially filled (one leg of a
    bracket always ends cancelled).

    An order's side is the ``action`` on its journal events. An order whose
    events name no side counts with the entry, so a single-order proposal and
    the same-side halves of a position-crossing order (reduction + opening
    remainder) are summarised exactly as before.

    Args:
        events: TradingEvent-like objects (event_type, order_id, action,
            quantity, price, timestamp, metadata) for the proposal's orders.
        order_ids: the proposal's recorded order ids, entry first (every
            placement path lists it first).
        target_quantity: the proposal's share quantity, if it had one.
        action: the proposal's action (BUY / SELL). When omitted, the entry
            side is the side of the FIRST order id.

    Returns:
        dict with ``outcome`` (filled | partially_filled | cancelled_unfilled |
        rejected | working | unknown), ``filled_quantity``, ``avg_fill_price``
        (None when nothing filled), ``exit_filled_quantity``,
        ``exit_avg_fill_price`` (None when no exit leg filled) and per-order
        ``orders`` detail (``filled_quantity``, ``state``, ``role``:
        entry | exit).
    """
    ids = [int(o) for o in (order_ids or [])]
    per: dict = {oid: {'filled': 0.0, 'cum': 0.0, 'notional': 0.0, 'terminal': None,
                       'remaining': None, 'events': 0, 'side': ''} for oid in ids}
    for ev in sorted(events, key=lambda e: (getattr(e, 'timestamp', None) is None,
                                            getattr(e, 'timestamp', None) or 0,
                                            getattr(e, 'id', 0) or 0)):
        oid = int(getattr(ev, 'order_id', 0) or 0)
        if oid not in per:
            continue
        o = per[oid]
        o['events'] += 1
        o['side'] = o['side'] or _side(getattr(ev, 'action', ''))
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

    entry_side = _side(action) or (per[ids[0]]['side'] if ids else '')

    orders = {}
    entry_ids = []
    total = 0.0
    exit_total = 0.0
    for oid, o in per.items():
        filled = max(o['filled'], o['cum'])
        is_exit = bool(entry_side and o['side'] and o['side'] != entry_side)
        if is_exit:
            exit_total += filled
        else:
            entry_ids.append(oid)
            total += filled
        done = (o['remaining'] is not None and o['remaining'] <= _EPS and filled > _EPS)
        state = ('filled' if done else o['terminal'] if o['terminal'] else
                 'working' if o['events'] else 'no_events')
        orders[oid] = {'filled_quantity': filled, 'state': state,
                       'role': 'exit' if is_exit else 'entry'}

    if not entry_ids or all(per[oid]['events'] == 0 for oid in entry_ids):
        outcome = UNKNOWN
    elif total > _EPS:
        if target_quantity is not None and target_quantity == target_quantity and target_quantity > 0:
            complete = total + _EPS >= float(target_quantity)
        else:
            complete = all(orders[oid]['state'] == 'filled' for oid in entry_ids)
        outcome = FILLED if complete else PARTIAL
    else:
        states = {orders[oid]['state'] for oid in entry_ids}
        if states <= {'cancelled', 'rejected'}:
            outcome = REJECTED if states == {'rejected'} else CANCELLED_UNFILLED
        else:
            outcome = WORKING

    def average_price(oids: list) -> Optional[float]:
        priced = [per[oid] for oid in oids if per[oid]['notional'] > 0]
        quantity = sum(o['filled'] for o in priced)
        return (sum(o['notional'] for o in priced) / quantity) if quantity > _EPS else None

    return {
        'outcome': outcome,
        'filled_quantity': total,
        'avg_fill_price': average_price(entry_ids),
        'exit_filled_quantity': exit_total,
        'exit_avg_fill_price': average_price([oid for oid in per if orders[oid]['role'] == 'exit']),
        'orders': orders,
    }
