"""EXECUTED proposals report what actually happened to their orders."""
import datetime as dt
from types import SimpleNamespace

from trader.trading.proposal_outcome import fill_outcome

T0 = dt.datetime(2026, 11, 5, 10, 0)


def ev(kind, oid, q=0.0, px=0.0, secs=0, **md):
    return SimpleNamespace(event_type=kind, order_id=oid, quantity=q, price=px,
                           timestamp=T0 + dt.timedelta(seconds=secs), id=secs, metadata=md)


def test_cancelled_with_zero_fill():
    r = fill_outcome([ev('ORDER_SUBMITTED', 1), ev('ORDER_CANCELLED', 1, 5, secs=1,
                                                     cumulative_filled=0.0, remaining=5.0)], [1], 5)
    assert r['outcome'] == 'cancelled_unfilled' and r['filled_quantity'] == 0
    assert r['avg_fill_price'] is None


def test_full_fill_with_vwap():
    r = fill_outcome([ev('ORDER_FILLED', 1, 2, 10.0, 1, cumulative_filled=2.0, remaining=1.0),
                      ev('ORDER_FILLED', 1, 1, 13.0, 2, cumulative_filled=3.0, remaining=0.0)], [1], 3)
    assert r['outcome'] == 'filled' and r['filled_quantity'] == 3
    assert abs(r['avg_fill_price'] - 11.0) < 1e-9


def test_partial_then_cancel():
    r = fill_outcome([ev('ORDER_FILLED', 1, 2, 10.0, 1, cumulative_filled=2.0, remaining=3.0),
                      ev('ORDER_CANCELLED', 1, 5, secs=2, remaining=3.0)], [1], 5)
    assert r['outcome'] == 'partially_filled' and r['filled_quantity'] == 2


def test_cancel_race_superseded_by_fill():
    # Observed live: Cancelled event, then the fill that beat the cancel.
    r = fill_outcome([ev('ORDER_CANCELLED', 1082, 1, secs=0, cumulative_filled=0.0, remaining=1.0),
                      ev('ORDER_FILLED', 1082, 1, 12.42, 1, cumulative_filled=1.0, remaining=0.0)],
                     [1082], 1)
    assert r['outcome'] == 'filled'


def test_rejected_working_unknown():
    assert fill_outcome([ev('ORDER_REJECTED', 1)], [1], 1)['outcome'] == 'rejected'
    assert fill_outcome([ev('ORDER_SUBMITTED', 1)], [1], 1)['outcome'] == 'working'
    assert fill_outcome([], [1], 1)['outcome'] == 'unknown'
    assert fill_outcome([ev('ORDER_FILLED', 99, 1, 1.0)], [1], 1)['outcome'] == 'unknown'


def test_amount_based_uses_remaining():
    r = fill_outcome([ev('ORDER_FILLED', 1, 40, 25.0, 1, cumulative_filled=40.0, remaining=0.0)], [1], None)
    assert r['outcome'] == 'filled'


def test_multi_order_one_cancelled():
    r = fill_outcome([ev('ORDER_FILLED', 1, 3, 10.0, 1, cumulative_filled=3.0, remaining=0.0),
                      ev('ORDER_CANCELLED', 2, 2, secs=2, remaining=2.0)], [1, 2], 5)
    assert r['outcome'] == 'partially_filled'
    assert r['orders'][1]['state'] == 'filled' and r['orders'][2]['state'] == 'cancelled'


def test_sdk_listing_and_detail(tmp_path):
    """End to end on a tmp DuckDB: store + event store -> proposals() outcome column."""
    from trader.data.event_store import EventStore, EventType, TradingEvent
    from trader.data.proposal_store import ProposalStore
    from trader.sdk import MMR
    from trader.trading.proposal import ExecutionSpec, TradeProposal

    db = str(tmp_path / 'mmr.duckdb')
    ps = ProposalStore(db)
    pid = ps.add(TradeProposal(symbol='F', action='BUY', quantity=5.0,
                               execution=ExecutionSpec(order_type='LIMIT', limit_price=10.0)))
    ps.try_transition(pid, 'PENDING', 'APPROVED')
    ps.try_transition(pid, 'APPROVED', 'EXECUTED', order_ids=[1061])
    es = EventStore(db)
    now = dt.datetime.now()
    es.append(TradingEvent(event_type=EventType.ORDER_SUBMITTED, timestamp=now, strategy_name='manual', order_id=1061,
                           symbol='F', action='BUY', quantity=5.0))
    es.append(TradingEvent(event_type=EventType.ORDER_CANCELLED, timestamp=now, strategy_name='manual', order_id=1061,
                           symbol='F', action='BUY', quantity=5.0,
                           metadata={'cumulative_filled': 0.0, 'remaining': 5.0}))

    mmr = object.__new__(MMR)
    mmr._container = SimpleNamespace(config=lambda: {'duckdb_path': db})
    mmr._prop_store = ps
    mmr._proposal_ttl = lambda: None
    df = mmr.proposals(status='EXECUTED')
    assert list(df['outcome']) == ['cancelled_unfilled']
    assert mmr.proposal_detail(pid)['fill']['outcome'] == 'cancelled_unfilled'
