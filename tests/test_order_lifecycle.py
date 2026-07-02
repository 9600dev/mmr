"""Tests for OrderLifecycleTracker — outcome events + decisive-status waiting."""
import asyncio
import types

import pytest

from trader.data.event_store import EventStore, EventType
from trader.trading.order_lifecycle import OrderLifecycleTracker


def _trade(order_id, status, *, symbol='AMD', conid=4391, action='BUY',
           total_qty=100.0, filled=0.0, avg_price=0.0, order_ref='proposal'):
    order = types.SimpleNamespace(orderId=order_id, action=action,
                                  totalQuantity=total_qty, orderRef=order_ref)
    st = types.SimpleNamespace(status=status, filled=filled, avgFillPrice=avg_price)
    contract = types.SimpleNamespace(conId=conid, symbol=symbol)
    return types.SimpleNamespace(order=order, orderStatus=st, contract=contract)


class TestOutcomeEvents:
    def test_fill_writes_order_filled_once(self, tmp_duckdb_path):
        es = EventStore(tmp_duckdb_path)
        t = OrderLifecycleTracker(es)
        t.on_trade(_trade(1, 'Submitted'))
        t.on_trade(_trade(1, 'Filled', filled=100.0, avg_price=42.5))
        # A duplicate terminal update must not double-log.
        t.on_trade(_trade(1, 'Filled', filled=100.0, avg_price=42.5))

        filled = es.query_since(_epoch(), EventType.ORDER_FILLED)
        assert len(filled) == 1
        assert filled[0].order_id == 1
        assert filled[0].quantity == 100.0
        assert filled[0].price == 42.5

    def test_cancel_and_reject_events(self, tmp_duckdb_path):
        es = EventStore(tmp_duckdb_path)
        t = OrderLifecycleTracker(es)
        t.on_trade(_trade(2, 'Cancelled'))
        t.on_trade(_trade(3, 'Inactive'))
        assert len(es.query_since(_epoch(), EventType.ORDER_CANCELLED)) == 1
        assert len(es.query_since(_epoch(), EventType.ORDER_REJECTED)) == 1

    def test_pending_status_writes_nothing(self, tmp_duckdb_path):
        es = EventStore(tmp_duckdb_path)
        t = OrderLifecycleTracker(es)
        t.on_trade(_trade(4, 'PendingSubmit'))
        t.on_trade(_trade(4, 'PreSubmitted'))
        assert es.query_since(_epoch()) == []


class TestDecisiveWait:
    def test_wait_returns_accepted(self):
        t = OrderLifecycleTracker(None)

        async def run():
            fut = asyncio.ensure_future(t.wait_decisive(10, timeout=2))
            await asyncio.sleep(0)
            t.on_trade(_trade(10, 'Submitted'))
            return await fut

        assert asyncio.run(run()) == 'accepted'

    def test_wait_returns_rejected(self):
        t = OrderLifecycleTracker(None)

        async def run():
            fut = asyncio.ensure_future(t.wait_decisive(11, timeout=2))
            await asyncio.sleep(0)
            t.on_trade(_trade(11, 'Cancelled'))
            return await fut

        assert asyncio.run(run()) == 'rejected'

    def test_wait_times_out(self):
        t = OrderLifecycleTracker(None)
        assert asyncio.run(t.wait_decisive(12, timeout=0.1)) == 'timeout'

    def test_wait_returns_immediately_if_already_known(self):
        t = OrderLifecycleTracker(None)
        t.on_trade(_trade(13, 'Filled', filled=1.0))
        assert asyncio.run(t.wait_decisive(13, timeout=0.1)) == 'filled'


def _epoch():
    import datetime as dt
    return dt.datetime(1970, 1, 1)
