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


# ---------------------------------------------------------------------------
# Brackets: the proposal's order ids are [entry, take-profit, stop-loss]. The
# exit legs trade the OPPOSITE side; their fills are not fills of the proposal
# (docs/AUDIT_2026-11-14.md B1.15). Journal events carry the order's action.
# ---------------------------------------------------------------------------

def leg(kind, oid, action, q=0.0, px=0.0, secs=0, **md):
    event = ev(kind, oid, q, px, secs, **md)
    event.action = action
    return event


def _bracket(entry_fill=100.0, entry_remaining=0.0):
    """BUY 100 bracket: entry 1, take-profit 2, stop-loss 3, all submitted."""
    events = [leg('ORDER_SUBMITTED', 1, 'BUY', 100, secs=0),
              leg('ORDER_SUBMITTED', 2, 'SELL', 100, secs=0),
              leg('ORDER_SUBMITTED', 3, 'SELL', 100, secs=0)]
    if entry_fill:
        events.append(leg('ORDER_FILLED', 1, 'BUY', entry_fill, 10.0, 1,
                          cumulative_filled=entry_fill, remaining=entry_remaining))
    return events


def test_bracket_exit_fill_is_not_counted_as_an_entry_fill():
    events = _bracket() + [
        leg('ORDER_FILLED', 2, 'SELL', 100, 12.0, 60, cumulative_filled=100.0, remaining=0.0),
        leg('ORDER_CANCELLED', 3, 'SELL', 100, secs=61, cumulative_filled=0.0, remaining=100.0)]
    r = fill_outcome(events, [1, 2, 3], 100, action='BUY')
    assert r['outcome'] == 'filled'
    assert r['filled_quantity'] == 100, 'the take-profit sale was added to the entry fill'
    assert r['avg_fill_price'] == 10.0, 'the exit price was blended into the entry price'
    assert r['exit_filled_quantity'] == 100 and r['exit_avg_fill_price'] == 12.0
    assert [r['orders'][oid]['role'] for oid in (1, 2, 3)] == ['entry', 'exit', 'exit']
    assert r['orders'][2]['state'] == 'filled' and r['orders'][3]['state'] == 'cancelled'


def test_half_filled_entry_is_not_completed_by_its_stop():
    """BUY 100 fills 50 and the rest is cancelled; the stop then sells the 50.
    Summed across sides that read 100 of 100: 'filled'."""
    events = _bracket(entry_fill=50.0, entry_remaining=50.0) + [
        leg('ORDER_CANCELLED', 1, 'BUY', 100, secs=2, cumulative_filled=50.0, remaining=50.0),
        leg('ORDER_FILLED', 3, 'SELL', 50, 9.0, 60, cumulative_filled=50.0, remaining=0.0),
        leg('ORDER_CANCELLED', 2, 'SELL', 100, secs=61, cumulative_filled=0.0, remaining=100.0)]
    r = fill_outcome(events, [1, 2, 3], 100, action='BUY')
    assert r['outcome'] == 'partially_filled'
    assert r['filled_quantity'] == 50 and r['avg_fill_price'] == 10.0
    assert r['exit_filled_quantity'] == 50 and r['exit_avg_fill_price'] == 9.0


def test_amount_sized_bracket_with_a_filled_entry_is_filled():
    """No share target (amount-sized): completeness is the entry's, not "every
    leg filled" — a bracket always ends with one leg cancelled."""
    working_legs = fill_outcome(_bracket(), [1, 2, 3], None, action='BUY')
    assert working_legs['outcome'] == 'filled' and working_legs['filled_quantity'] == 100
    exited = fill_outcome(_bracket() + [
        leg('ORDER_FILLED', 2, 'SELL', 100, 12.0, 60, cumulative_filled=100.0, remaining=0.0),
        leg('ORDER_CANCELLED', 3, 'SELL', 100, secs=61, cumulative_filled=0.0, remaining=100.0)],
        [1, 2, 3], None, action='BUY')
    assert exited['outcome'] == 'filled' and exited['filled_quantity'] == 100


def test_entry_side_is_read_from_the_first_order_when_no_action_is_given():
    events = _bracket() + [
        leg('ORDER_FILLED', 2, 'SELL', 100, 12.0, 60, cumulative_filled=100.0, remaining=0.0)]
    r = fill_outcome(events, [1, 2, 3], 100)
    assert r['filled_quantity'] == 100 and r['avg_fill_price'] == 10.0
    assert r['exit_filled_quantity'] == 100


def test_unfilled_entry_outcome_is_the_entrys_not_its_legs():
    rejected = fill_outcome([leg('ORDER_REJECTED', 1, 'BUY', 100, secs=1),
                             leg('ORDER_CANCELLED', 2, 'SELL', 100, secs=2),
                             leg('ORDER_CANCELLED', 3, 'SELL', 100, secs=2)], [1, 2, 3], 100, action='BUY')
    assert rejected['outcome'] == 'rejected'
    cancelled = fill_outcome([leg('ORDER_SUBMITTED', 1, 'BUY', 100),
                              leg('ORDER_SUBMITTED', 2, 'SELL', 100),
                              leg('ORDER_CANCELLED', 1, 'BUY', 100, secs=5, cumulative_filled=0.0, remaining=100.0)],
                             [1, 2], 100, action='BUY')
    assert cancelled['outcome'] == 'cancelled_unfilled'


def test_same_side_orders_of_a_split_are_still_summed():
    """A position-crossing SELL is placed as a reduction and an opening
    remainder — two orders on the proposal's own side."""
    r = fill_outcome([leg('ORDER_FILLED', 1, 'SELL', 3, 10.0, 1, cumulative_filled=3.0, remaining=0.0),
                      leg('ORDER_FILLED', 2, 'SELL', 2, 11.0, 2, cumulative_filled=2.0, remaining=0.0)],
                     [1, 2], 5, action='SELL')
    assert r['outcome'] == 'filled' and r['filled_quantity'] == 5
    assert abs(r['avg_fill_price'] - 10.4) < 1e-9
    assert r['exit_filled_quantity'] == 0 and r['exit_avg_fill_price'] is None
    assert [r['orders'][oid]['role'] for oid in (1, 2)] == ['entry', 'entry']


def test_single_leg_results_are_unchanged_by_the_action():
    events = [leg('ORDER_FILLED', 1, 'BUY', 2, 10.0, 1, cumulative_filled=2.0, remaining=3.0),
              leg('ORDER_CANCELLED', 1, 'BUY', 5, secs=2, remaining=3.0)]
    with_action = fill_outcome(events, [1], 5, action='BUY')
    without = fill_outcome(events, [1], 5)
    assert with_action == without
    assert with_action['outcome'] == 'partially_filled' and with_action['filled_quantity'] == 2


def test_no_entry_side_order_is_unknown_not_cancelled():
    """Every recorded order is on the other side of the proposal: nothing here
    describes the entry."""
    r = fill_outcome([leg('ORDER_CANCELLED', 2, 'SELL', 100, secs=1)], [2], 100, action='BUY')
    assert r['outcome'] == 'unknown' and r['filled_quantity'] == 0


def test_sdk_bracket_detail_reports_the_entry_fill(tmp_path):
    """End to end on a tmp DuckDB: an EXECUTED bracket whose take-profit sold."""
    from trader.data.event_store import EventStore, EventType, TradingEvent
    from trader.data.proposal_store import ProposalStore
    from trader.sdk import MMR
    from trader.trading.proposal import ExecutionSpec, TradeProposal

    db = str(tmp_path / 'mmr.duckdb')
    ps = ProposalStore(db)
    pid = ps.add(TradeProposal(symbol='F', action='BUY', quantity=5.0,
                               execution=ExecutionSpec(order_type='LIMIT', limit_price=10.0,
                                                       exit_type='BRACKET', take_profit_price=12.0,
                                                       stop_loss_price=9.0)))
    ps.try_transition(pid, 'PENDING', 'APPROVED')
    ps.try_transition(pid, 'APPROVED', 'EXECUTED', order_ids=[1061, 1062, 1063])
    es = EventStore(db)
    now = dt.datetime.now()

    def record(kind, oid, action, qty, price=0.0, secs=0, **md):
        es.append(TradingEvent(event_type=kind, timestamp=now + dt.timedelta(seconds=secs),
                               strategy_name='manual', order_id=oid, symbol='F', action=action,
                               quantity=qty, price=price, metadata=md))

    record(EventType.ORDER_SUBMITTED, 1061, 'BUY', 5.0)
    record(EventType.ORDER_SUBMITTED, 1062, 'SELL', 5.0)
    record(EventType.ORDER_SUBMITTED, 1063, 'SELL', 5.0)
    record(EventType.ORDER_FILLED, 1061, 'BUY', 5.0, 10.0, 1, cumulative_filled=5.0, remaining=0.0)
    record(EventType.ORDER_FILLED, 1062, 'SELL', 5.0, 12.0, 60, cumulative_filled=5.0, remaining=0.0)
    record(EventType.ORDER_CANCELLED, 1063, 'SELL', 5.0, 0.0, 61, cumulative_filled=0.0, remaining=5.0)

    mmr = object.__new__(MMR)
    mmr._container = SimpleNamespace(config=lambda: {'duckdb_path': db})
    mmr._prop_store = ps
    mmr._proposal_ttl = lambda: None
    fill = mmr.proposal_detail(pid)['fill']
    assert fill['outcome'] == 'filled'
    assert fill['filled_quantity'] == 5.0 and fill['avg_fill_price'] == 10.0
    assert fill['exit_filled_quantity'] == 5.0 and fill['exit_avg_fill_price'] == 12.0
    assert list(mmr.proposals(status='EXECUTED')['outcome']) == ['filled']


def test_cli_show_prints_entry_and_exit_fills_apart(capsys, monkeypatch):
    import argparse

    from trader import mmr_cli

    monkeypatch.setattr(mmr_cli, '_json_mode', False)
    detail = {'trusted': {}, 'untrusted': {}, 'order_ids': [1, 2, 3], 'created_at': T0,
              'fill': {'outcome': 'filled', 'filled_quantity': 100.0, 'avg_fill_price': 10.0,
                       'exit_filled_quantity': 100.0, 'exit_avg_fill_price': 12.0, 'orders': {}}}
    mmr = SimpleNamespace(proposal_detail=lambda proposal_id: detail)
    mmr_cli._handle_proposals(mmr, argparse.Namespace(proposals_action='show', proposal_id=7))
    out = capsys.readouterr().out
    assert '(filled 100 @ 10)' in out
    assert 'exit legs filled: 100 @ 12' in out

    detail['fill'].update(exit_filled_quantity=0.0, exit_avg_fill_price=None)
    mmr_cli._handle_proposals(mmr, argparse.Namespace(proposals_action='show', proposal_id=7))
    assert 'exit legs' not in capsys.readouterr().out


def test_reconcile_names_the_entrys_outcome_for_a_bracket(tmp_path):
    """Trader.reconcile_with_broker resolves an EXECUTED proposal IB no longer
    reports from the journal. BUY 100 filled 50 and was cancelled; its stop
    sold the 50. Summed across sides the journal said 'filled'."""
    import asyncio
    from unittest.mock import AsyncMock

    from trader.data.event_store import EventStore, EventType, TradingEvent
    from trader.data.proposal_store import ProposalStore
    from trader.trading.proposal import ExecutionSpec, TradeProposal
    from trader.trading.trading_runtime import Trader

    db = str(tmp_path / 'mmr.duckdb')
    ps = ProposalStore(db)
    pid = ps.add(TradeProposal(symbol='F', action='BUY', quantity=100.0,
                               execution=ExecutionSpec(order_type='LIMIT', limit_price=10.0,
                                                       exit_type='STOP_LOSS', stop_loss_price=9.0)))
    ps.try_transition(pid, 'PENDING', 'APPROVED')
    ps.try_transition(pid, 'APPROVED', 'EXECUTED', order_ids=[2001, 2002])
    es = EventStore(db)
    now = dt.datetime.now()

    def record(kind, oid, action, qty, price=0.0, secs=0, **md):
        es.append(TradingEvent(event_type=kind, timestamp=now + dt.timedelta(seconds=secs),
                               strategy_name='manual', order_id=oid, symbol='F', action=action,
                               quantity=qty, price=price, metadata=md))

    record(EventType.ORDER_SUBMITTED, 2001, 'BUY', 100.0)
    record(EventType.ORDER_SUBMITTED, 2002, 'SELL', 100.0)
    record(EventType.ORDER_FILLED, 2001, 'BUY', 50.0, 10.0, 1, cumulative_filled=50.0, remaining=50.0)
    record(EventType.ORDER_CANCELLED, 2001, 'BUY', 100.0, 0.0, 2, cumulative_filled=50.0, remaining=50.0)
    record(EventType.ORDER_FILLED, 2002, 'SELL', 50.0, 9.0, 60, cumulative_filled=50.0, remaining=0.0)

    trader = object.__new__(Trader)
    trader.duckdb_path = db
    trader.client = SimpleNamespace(get_open_orders=AsyncMock(return_value=[]),
                                    get_executions=AsyncMock(return_value=[]))
    trader.get_positions = lambda: []

    report = asyncio.run(trader.reconcile_with_broker())

    finding, = [f for f in report['findings'] if f['proposal_id'] == pid]
    assert finding['kind'] == 'executed_outcome_known'
    assert finding['detail'].endswith('the outcome: partially_filled'), finding['detail']


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
