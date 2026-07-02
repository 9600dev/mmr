"""OrderLifecycleTracker — the single sink for IB order-status truth.

Subscribes to the ``orderStatus`` stream (``Trade`` objects, one per status
change) and does two things the rest of the system was missing:

1. **Records outcome events** (``ORDER_FILLED`` / ``ORDER_CANCELLED`` /
   ``ORDER_REJECTED``) in the event store — previously only ``ORDER_SUBMITTED``
   was ever written, so the risk-gate lookback and the audit trail were blind to
   what actually happened to an order.

2. **Lets callers await a decisive status** for an order id (accepted vs rejected
   vs filled), so order placement can confirm IB actually took the order rather
   than trusting the local ``placeOrder`` echo (which is returned even for an
   order IB goes on to reject).

The tracker is created once per session and its subscription is re-established on
every reconnect (so it keeps working after an IB drop). All callbacks run on the
trader_service event loop (ib_async fires eventkit callbacks there), so resolving
asyncio Futures from ``on_trade`` is loop-safe.
"""
from __future__ import annotations

import asyncio
import datetime as dt
from typing import Dict, List, Optional

from trader.common.logging_helper import setup_logging
from trader.data.event_store import EventStore, EventType, TradingEvent

logging = setup_logging(module_name='order_lifecycle')

# IB order-status buckets.
_ACCEPTED = {'PreSubmitted', 'Submitted'}   # live and working at IB
_FILLED = {'Filled'}
_DEAD = {'Cancelled', 'ApiCancelled', 'Inactive'}  # never worked / rejected / killed
# PendingSubmit, ApiPending, PendingCancel → not yet decisive; keep waiting.


class OrderLifecycleTracker:
    def __init__(self, event_store: Optional[EventStore] = None):
        self._event_store = event_store
        self._status: Dict[int, str] = {}        # order_id -> latest status seen
        self._terminal_logged: set = set()       # order_ids already outcome-logged
        self._waiters: Dict[int, List[asyncio.Future]] = {}

    def set_event_store(self, event_store: EventStore) -> None:
        """Attach the event store after construction (it is built later than the
        tracker, which is created in __init__ so it exists before the first
        connected_event/setup_subscriptions runs)."""
        self._event_store = event_store

    # ------------------------------------------------------------------
    # Stream sink
    # ------------------------------------------------------------------
    def on_trade(self, trade) -> None:
        """Handle one orderStatus update (an ib_async Trade)."""
        try:
            order = getattr(trade, 'order', None)
            st = getattr(trade, 'orderStatus', None)
            order_id = int(getattr(order, 'orderId', 0) or 0)
            status = str(getattr(st, 'status', '') or '')
            if order_id == 0 or not status:
                return

            self._status[order_id] = status

            decisive = self._decisive(status)
            if decisive is not None:
                self._resolve_waiters(order_id, decisive)

            # Record the terminal outcome exactly once per order.
            if (status in _FILLED or status in _DEAD) and order_id not in self._terminal_logged:
                self._terminal_logged.add(order_id)
                self._record_event(trade, status)
        except Exception as ex:  # never let a status update crash the stream
            logging.warning('order lifecycle on_trade error: %s', ex)

    # ------------------------------------------------------------------
    # Decisive-status waiting (used to confirm IB acceptance)
    # ------------------------------------------------------------------
    @staticmethod
    def _decisive(status: str) -> Optional[str]:
        if status in _FILLED:
            return 'filled'
        if status in _ACCEPTED:
            return 'accepted'
        if status in _DEAD:
            return 'rejected'
        return None  # pending — not decisive yet

    def latest_status(self, order_id: int) -> Optional[str]:
        return self._status.get(order_id)

    async def wait_decisive(self, order_id: int, timeout: float = 10.0) -> str:
        """Await a decisive status for *order_id*.

        Returns 'filled' | 'accepted' | 'rejected' | 'timeout'. 'accepted' means
        IB has the order working; 'rejected' means it was cancelled/inactive
        without ever working; 'timeout' means no decisive status arrived in time
        (the order may still be live — the caller must treat this as UNKNOWN, not
        failure).
        """
        cur = self._status.get(order_id)
        d = self._decisive(cur) if cur else None
        if d is not None:
            return d

        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        self._waiters.setdefault(order_id, []).append(fut)
        try:
            return await asyncio.wait_for(fut, timeout)
        except asyncio.TimeoutError:
            return 'timeout'
        finally:
            waiters = self._waiters.get(order_id)
            if waiters and fut in waiters:
                waiters.remove(fut)
            if waiters is not None and not waiters:
                self._waiters.pop(order_id, None)

    def _resolve_waiters(self, order_id: int, decisive: str) -> None:
        for fut in self._waiters.get(order_id, []):
            if not fut.done():
                fut.set_result(decisive)

    # ------------------------------------------------------------------
    # Event recording
    # ------------------------------------------------------------------
    def _record_event(self, trade, status: str) -> None:
        if self._event_store is None:
            return
        if status in _FILLED:
            event_type = EventType.ORDER_FILLED
        elif status in ('Cancelled', 'ApiCancelled'):
            event_type = EventType.ORDER_CANCELLED
        else:  # Inactive → treated as a rejection
            event_type = EventType.ORDER_REJECTED
        try:
            contract = getattr(trade, 'contract', None)
            order = getattr(trade, 'order', None)
            st = getattr(trade, 'orderStatus', None)
            if event_type == EventType.ORDER_FILLED:
                qty = float(getattr(st, 'filled', 0.0) or 0.0)
            else:
                qty = float(getattr(order, 'totalQuantity', 0.0) or 0.0)
            self._event_store.append(TradingEvent(
                event_type=event_type,
                timestamp=dt.datetime.now(),
                strategy_name=str(getattr(order, 'orderRef', '') or 'order'),
                conid=int(getattr(contract, 'conId', 0) or 0),
                symbol=str(getattr(contract, 'symbol', '') or ''),
                action=str(getattr(order, 'action', '') or ''),
                quantity=qty,
                price=float(getattr(st, 'avgFillPrice', 0.0) or 0.0),
                order_id=int(getattr(order, 'orderId', 0) or 0),
                metadata={'status': status},
            ))
        except Exception as ex:
            logging.warning('failed to record %s event: %s', event_type, ex)
