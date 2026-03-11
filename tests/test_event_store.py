import datetime as dt
import pytest
from trader.data.event_store import EventStore, EventType, TradingEvent


def _make_event(
    event_type=EventType.SIGNAL,
    strategy_name="test_strat",
    conid=4391,
    timestamp=None,
):
    return TradingEvent(
        event_type=event_type,
        timestamp=timestamp or dt.datetime.now(),
        strategy_name=strategy_name,
        conid=conid,
        symbol="AMD",
        action="BUY",
        quantity=100.0,
        price=150.0,
    )


class TestEventStore:
    def test_table_creation(self, event_store):
        # Just verify the store can be created without error
        assert event_store is not None

    def test_append_and_query_all(self, event_store):
        event_store.append(_make_event())
        events = event_store.query_all()
        assert len(events) == 1
        assert events[0].strategy_name == "test_strat"
        assert events[0].conid == 4391

    def test_query_by_strategy(self, event_store):
        event_store.append(_make_event(strategy_name="alpha"))
        event_store.append(_make_event(strategy_name="beta"))
        results = event_store.query_by_strategy("alpha")
        assert len(results) == 1
        assert results[0].strategy_name == "alpha"

    def test_query_by_conid(self, event_store):
        event_store.append(_make_event(conid=1111))
        event_store.append(_make_event(conid=2222))
        results = event_store.query_by_conid(2222)
        assert len(results) == 1
        assert results[0].conid == 2222

    def test_query_signals(self, event_store):
        event_store.append(_make_event(event_type=EventType.SIGNAL))
        event_store.append(_make_event(event_type=EventType.ORDER_FILLED))
        results = event_store.query_signals()
        assert len(results) == 1
        assert results[0].event_type == EventType.SIGNAL

    def test_query_since(self, event_store):
        old = dt.datetime(2020, 1, 1)
        recent = dt.datetime.now() - dt.timedelta(minutes=5)
        event_store.append(_make_event(timestamp=old))
        event_store.append(_make_event(timestamp=recent))
        one_hour_ago = dt.datetime.now() - dt.timedelta(hours=1)
        results = event_store.query_since(one_hour_ago)
        assert len(results) == 1

    def test_count_since(self, event_store):
        now = dt.datetime.now()
        for i in range(5):
            event_store.append(_make_event(timestamp=now - dt.timedelta(minutes=i)))
        one_hour_ago = now - dt.timedelta(hours=1)
        count = event_store.count_since(one_hour_ago, event_type=EventType.SIGNAL)
        assert count == 5

    def test_multiple_events_ordering(self, event_store):
        t1 = dt.datetime(2024, 1, 1, 10, 0, 0)
        t2 = dt.datetime(2024, 1, 1, 11, 0, 0)
        t3 = dt.datetime(2024, 1, 1, 12, 0, 0)
        event_store.append(_make_event(timestamp=t1))
        event_store.append(_make_event(timestamp=t2))
        event_store.append(_make_event(timestamp=t3))
        events = event_store.query_all()
        # Results ordered DESC by timestamp
        assert events[0].timestamp >= events[1].timestamp >= events[2].timestamp
