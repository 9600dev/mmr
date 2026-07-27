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


class TestFilledSupersedesCancelled:
    """IB's status stream can lie transiently. Observed live 2026-07-27 (QBTS
    order 320): Cancelled at +25ms, then the SAME order filled — broker truth
    'Filled', event store stuck on ORDER_CANCELLED forever, no fill recorded.
    The PnL ledger pairs ORDER_FILLED events, so on a strategy exit this race
    silently drops the sell from realized PnL.
    """

    def _trade(self, order_id, status, filled=0.0, ref='s1'):
        from types import SimpleNamespace
        return SimpleNamespace(
            order=SimpleNamespace(orderId=order_id, orderRef=ref,
                                  action='SELL', totalQuantity=50.0),
            orderStatus=SimpleNamespace(status=status, filled=filled,
                                        avgFillPrice=18.9),
            contract=SimpleNamespace(conId=1, symbol='QBTS'),
        )

    def test_cancelled_then_filled_records_the_fill(self, tmp_path):
        from trader.data.event_store import EventStore, EventType
        from trader.trading.order_lifecycle import OrderLifecycleTracker
        es = EventStore(str(tmp_path / 'lc.duckdb'))
        t = OrderLifecycleTracker(es)
        t.on_trade(self._trade(320, 'Cancelled'))
        t.on_trade(self._trade(320, 'Filled', filled=50.0))
        events = [e.event_type for e in es.query_since(
            since=__import__('datetime').datetime(2000, 1, 1))]
        assert EventType.ORDER_CANCELLED in events
        assert EventType.ORDER_FILLED in events, (
            'the fill was suppressed by the earlier transient Cancelled')
        fills = [e for e in es.query_since(
            since=__import__('datetime').datetime(2000, 1, 1))
            if e.event_type == EventType.ORDER_FILLED]
        assert (fills[0].metadata or {}).get('superseded') == 'Cancelled'

    def test_filled_then_cancelled_does_not_record_the_cancel(self, tmp_path):
        """The reverse direction stays terminal-once: money moved, a later
        Cancelled is stream noise and must not shadow the fill."""
        from trader.data.event_store import EventStore, EventType
        from trader.trading.order_lifecycle import OrderLifecycleTracker
        es = EventStore(str(tmp_path / 'lc2.duckdb'))
        t = OrderLifecycleTracker(es)
        t.on_trade(self._trade(321, 'Filled', filled=50.0))
        t.on_trade(self._trade(321, 'Cancelled'))
        events = [e.event_type for e in es.query_since(
            since=__import__('datetime').datetime(2000, 1, 1))]
        assert events.count(EventType.ORDER_FILLED) == 1
        assert EventType.ORDER_CANCELLED not in events

    def test_duplicate_filled_records_once(self, tmp_path):
        from trader.data.event_store import EventStore, EventType
        from trader.trading.order_lifecycle import OrderLifecycleTracker
        es = EventStore(str(tmp_path / 'lc3.duckdb'))
        t = OrderLifecycleTracker(es)
        t.on_trade(self._trade(322, 'Filled', filled=50.0))
        t.on_trade(self._trade(322, 'Filled', filled=50.0))
        events = [e.event_type for e in es.query_since(
            since=__import__('datetime').datetime(2000, 1, 1))]
        assert events.count(EventType.ORDER_FILLED) == 1


class TestRecordedEventContent:
    """Mutation scored this module 61.7% on first measurement, with ~90
    survivors inside _record_event — every FIELD of the ledger row could be
    corrupted unnoticed because tests asserted event types, never content.
    The PnL ledger pairs these rows by strategy_name and quantity; a wrong
    field is a silently wrong realized PnL, which is the same failure class
    as the live Cancelled->Filled bug that put this module in the mutation
    scope in the first place."""

    def _trade(self, status='Filled', filled=37.0, total=50.0):
        from types import SimpleNamespace
        return SimpleNamespace(
            order=SimpleNamespace(orderId=414, orderRef='orb_googl',
                                  action='SELL', totalQuantity=total),
            orderStatus=SimpleNamespace(status=status, filled=filled,
                                        avgFillPrice=296.5),
            contract=SimpleNamespace(conId=208813719, symbol='GOOGL'),
        )

    def _one_event(self, tmp_path, trade):
        import datetime as dt
        from trader.data.event_store import EventStore
        from trader.trading.order_lifecycle import OrderLifecycleTracker
        es = EventStore(str(tmp_path / 'content.duckdb'))
        t = OrderLifecycleTracker(es)
        t.on_trade(trade)
        events = list(es.query_since(since=dt.datetime(2000, 1, 1)))
        assert len(events) == 1
        return events[0]

    def test_fill_row_fields_round_trip_from_the_trade(self, tmp_path):
        from trader.data.event_store import EventType
        e = self._one_event(tmp_path, self._trade())
        assert e.event_type == EventType.ORDER_FILLED
        assert e.strategy_name == 'orb_googl', 'ledger attribution comes from orderRef'
        assert e.conid == 208813719
        assert e.symbol == 'GOOGL'
        assert e.action == 'SELL'
        assert e.quantity == 37.0, 'a FILL records the FILLED quantity, not totalQuantity'
        assert e.price == 296.5, 'fill price is avgFillPrice'
        assert e.order_id == 414
        assert (e.metadata or {}).get('status') == 'Filled'

    def test_cancel_row_records_total_quantity(self, tmp_path):
        """A cancel has no fill; its size is what was ASKED (totalQuantity),
        and filled=0 must not zero it."""
        from trader.data.event_store import EventType
        e = self._one_event(tmp_path, self._trade(status='Cancelled', filled=0.0))
        assert e.event_type == EventType.ORDER_CANCELLED
        assert e.quantity == 50.0
        assert e.strategy_name == 'orb_googl'
        assert (e.metadata or {}).get('status') == 'Cancelled'

    def test_inactive_maps_to_rejected(self, tmp_path):
        from trader.data.event_store import EventType
        e = self._one_event(tmp_path, self._trade(status='Inactive', filled=0.0))
        assert e.event_type == EventType.ORDER_REJECTED

    def test_blank_orderref_falls_back_to_order_not_empty(self, tmp_path):
        from types import SimpleNamespace
        tr = self._trade()
        tr.order.orderRef = ''
        e = self._one_event(tmp_path, tr)
        assert e.strategy_name == 'order', 'blank ref must not write an empty strategy_name'


class TestOnTradeGuards:
    def _tracker(self):
        from trader.trading.order_lifecycle import OrderLifecycleTracker
        return OrderLifecycleTracker(None)

    def _trade(self, oid, status):
        from types import SimpleNamespace
        return SimpleNamespace(
            order=SimpleNamespace(orderId=oid, orderRef='', action='SELL',
                                  totalQuantity=1.0),
            orderStatus=SimpleNamespace(status=status, filled=0.0, avgFillPrice=0.0),
            contract=SimpleNamespace(conId=1, symbol='X'),
        )

    def test_order_id_zero_is_ignored(self):
        t = self._tracker()
        t.on_trade(self._trade(0, 'Filled'))
        assert t.latest_status(0) is None

    def test_empty_status_is_ignored(self):
        t = self._tracker()
        t.on_trade(self._trade(5, ''))
        assert t.latest_status(5) is None

    def test_latest_status_tracks_progression(self):
        t = self._tracker()
        for st in ('PendingSubmit', 'PreSubmitted', 'Submitted', 'Filled'):
            t.on_trade(self._trade(6, st))
        assert t.latest_status(6) == 'Filled'


class TestWaitDecisive:
    def _tracker(self):
        from trader.trading.order_lifecycle import OrderLifecycleTracker
        return OrderLifecycleTracker(None)

    def _trade(self, oid, status):
        from types import SimpleNamespace
        return SimpleNamespace(
            order=SimpleNamespace(orderId=oid, orderRef='', action='BUY',
                                  totalQuantity=1.0),
            orderStatus=SimpleNamespace(status=status, filled=1.0, avgFillPrice=1.0),
            contract=SimpleNamespace(conId=1, symbol='X'),
        )

    def test_known_status_resolves_immediately(self):
        import asyncio
        t = self._tracker()
        t.on_trade(self._trade(7, 'Submitted'))
        out = asyncio.new_event_loop().run_until_complete(t.wait_decisive(7, timeout=0.2))
        assert out == 'accepted'

    def test_later_status_resolves_a_waiter(self):
        import asyncio
        t = self._tracker()

        async def drive():
            fut = asyncio.ensure_future(t.wait_decisive(8, timeout=2.0))
            await asyncio.sleep(0.01)
            t.on_trade(self._trade(8, 'Cancelled'))
            return await fut

        assert asyncio.new_event_loop().run_until_complete(drive()) == 'rejected'

    def test_timeout_returns_timeout_and_cleans_up(self):
        import asyncio
        t = self._tracker()
        out = asyncio.new_event_loop().run_until_complete(t.wait_decisive(9, timeout=0.05))
        assert out == 'timeout'
        assert 9 not in t._waiters, 'timed-out waiter must not leak'

    def test_filled_wins_over_accepted_for_new_waiters(self):
        import asyncio
        t = self._tracker()
        t.on_trade(self._trade(10, 'Submitted'))
        t.on_trade(self._trade(10, 'Filled'))
        out = asyncio.new_event_loop().run_until_complete(t.wait_decisive(10, timeout=0.2))
        assert out == 'filled'


class TestApiCancelledAndDegenerateFill:
    def test_apicancelled_maps_to_cancelled(self, tmp_path):
        """ApiCancelled is OUR cancel (vs a desk/exchange cancel) and must
        record identically — three surviving mutants proved nothing pinned
        the second member of the status pair."""
        import datetime as dt
        from types import SimpleNamespace
        from trader.data.event_store import EventStore, EventType
        from trader.trading.order_lifecycle import OrderLifecycleTracker
        es = EventStore(str(tmp_path / 'api.duckdb'))
        t = OrderLifecycleTracker(es)
        t.on_trade(SimpleNamespace(
            order=SimpleNamespace(orderId=20, orderRef='s', action='SELL',
                                  totalQuantity=5.0),
            orderStatus=SimpleNamespace(status='ApiCancelled', filled=0.0,
                                        avgFillPrice=0.0),
            contract=SimpleNamespace(conId=1, symbol='X')))
        events = list(es.query_since(since=dt.datetime(2000, 1, 1)))
        assert events[0].event_type == EventType.ORDER_CANCELLED

    def test_fill_with_zero_filled_records_zero_not_a_fabricated_quantity(self, tmp_path):
        """Degenerate but pinned: a Filled status carrying filled=0 must record
        0, never a fabricated default — a wrong nonzero here would enter the
        PnL pairing as a phantom lot."""
        import datetime as dt
        from types import SimpleNamespace
        from trader.data.event_store import EventStore
        from trader.trading.order_lifecycle import OrderLifecycleTracker
        es = EventStore(str(tmp_path / 'zf.duckdb'))
        t = OrderLifecycleTracker(es)
        t.on_trade(SimpleNamespace(
            order=SimpleNamespace(orderId=21, orderRef='s', action='BUY',
                                  totalQuantity=5.0),
            orderStatus=SimpleNamespace(status='Filled', filled=0.0,
                                        avgFillPrice=0.0),
            contract=SimpleNamespace(conId=1, symbol='X')))
        events = list(es.query_since(since=dt.datetime(2000, 1, 1)))
        assert events[0].quantity == 0.0
