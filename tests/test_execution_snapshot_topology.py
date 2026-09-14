"""One client intent may contain more than one scoped physical broker order."""
import asyncio
import json
from types import SimpleNamespace

import pandas as pd
import pytest
from ib_async.order import Order, OrderStatus, Trade

from review.test_review_order_contract import _coordinated_trader, _stock
from review.test_review_strategy_contract import TS, make_work
from test_execution_recovery import recovery
from trader.objects import Action
from trader.data.event_store import EventStore
from trader.data.execution_journal import ExecutionJournal
from trader.strategy.auto_executor import AutoExecutor
from trader.trading.order_lifecycle import OrderLifecycleTracker


INTENT = 'topology-recovery'


def observed(trader, *, order_id=17, client_id=7, perm_id=1717, reference=INTENT):
    return Trade(_stock(), Order(orderId=order_id, clientId=client_id, permId=perm_id,
                                 account=trader.ib_account, totalQuantity=40, filledQuantity=40,
                                 action='BUY', orderRef='owner|mmr:' + reference),
                 OrderStatus(status='Filled', filled=40, avgFillPrice=10))


def reserve(trader, ids):
    journal = trader.server_order_journal()
    journal.claim(INTENT, 'same-wire-intent', trader.ib_account)
    for order_id in ids:
        journal.reserve_order(INTENT, order_id, 7)
    journal.finish(INTENT, 'SUBMITTED')
    return journal


@pytest.mark.asyncio
async def test_missing_split_leg_cannot_be_hidden_by_one_terminal_order(tmp_path):
    trader = _coordinated_trader(tmp_path)
    try:
        reserve(trader, [17, 18])
        trader.order_tracker.on_trade(observed(trader))
        snapshot = await trader.execution_snapshot(intent_id=INTENT, order_ids=[17])
        assert not snapshot['complete']
        assert snapshot['orders'][0]['orderId'] == 17
    finally:
        trader.order_tracker.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('wrong_identity', [{'client_id': 99}, {'reference': 'another-intent'}])
async def test_same_numeric_id_with_wrong_scoped_identity_does_not_complete_intent(tmp_path, wrong_identity):
    trader = _coordinated_trader(tmp_path)
    try:
        reserve(trader, [17])
        trader.order_tracker.on_trade(observed(trader, **wrong_identity))
        snapshot = await trader.execution_snapshot(intent_id=INTENT, order_ids=[17])
        assert not snapshot['complete']
        assert not snapshot['orders']
    finally:
        trader.order_tracker.close()


@pytest.mark.asyncio
async def test_single_permanent_identity_can_complete_its_only_durable_broker_order(tmp_path):
    trader = _coordinated_trader(tmp_path)
    try:
        reserve(trader, [17])
        trader.order_tracker.on_trade(observed(trader, order_id=0, client_id=0), completed=True)
        snapshot = await trader.execution_snapshot(intent_id=INTENT, order_ids=[17])
        assert snapshot['complete']
        assert len(snapshot['orders']) == 1
        assert snapshot['orders'][0]['permId'] == 1717
        assert snapshot['orders'][0]['orderId'] == 0
        assert snapshot['orders'][0]['clientIntentId'] == INTENT
    finally:
        trader.order_tracker.close()


@pytest.mark.asyncio
async def test_one_permanent_identity_cannot_stand_in_for_two_missing_split_legs(tmp_path):
    trader = _coordinated_trader(tmp_path)
    try:
        reserve(trader, [17, 18])
        trader.order_tracker.on_trade(observed(trader, order_id=0, client_id=0), completed=True)
        snapshot = await trader.execution_snapshot(intent_id=INTENT)
        assert not snapshot['complete']
        assert len(snapshot['orders']) == 1
    finally:
        trader.order_tracker.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('known_perm_id', [0, 1717])
async def test_permanent_only_leg_does_not_infer_a_multi_leg_numeric_mapping(tmp_path, known_perm_id):
    trader = _coordinated_trader(tmp_path)
    try:
        reserve(trader, [17, 18])
        trader.order_tracker.on_trade(observed(trader, perm_id=known_perm_id))
        trader.order_tracker.on_trade(observed(trader, order_id=0, client_id=0, perm_id=1818), completed=True)
        snapshot = await trader.execution_snapshot(intent_id=INTENT)
        assert not snapshot['complete']
        assert len(snapshot['orders']) == 2
    finally:
        trader.order_tracker.close()


@pytest.mark.asyncio
async def test_intent_snapshot_waits_for_parent_to_finish_reserving_split_legs(tmp_path):
    trader = _coordinated_trader(tmp_path)
    first_reserved = asyncio.Event()
    finish_split = asyncio.Event()
    lookup_started = asyncio.Event()
    journal = trader.server_order_journal()
    journal.claim(INTENT, 'same-wire-intent', trader.ib_account)

    async def parent_order():
        async with trader.serialized_orders():
            journal.reserve_order(INTENT, 17, 7)
            trader.order_tracker.on_trade(observed(trader))
            first_reserved.set()
            await finish_split.wait()
            journal.reserve_order(INTENT, 18, 7)
            journal.finish(INTENT, 'SUBMITTED')

    async def read_intent():
        lookup_started.set()
        return await trader.execution_snapshot(intent_id=INTENT, order_ids=[17])

    parent = asyncio.create_task(parent_order())
    snapshot_task = None
    try:
        await asyncio.wait_for(first_reserved.wait(), 2)
        snapshot_task = asyncio.create_task(read_intent())
        await asyncio.wait_for(lookup_started.wait(), 2)
        assert not snapshot_task.done(), 'a terminal first leg is not a finished split topology'
        finish_split.set()
        await asyncio.wait_for(parent, 2)
        snapshot = await asyncio.wait_for(snapshot_task, 2)
        assert not snapshot['complete']
    finally:
        finish_split.set()
        await parent
        if snapshot_task is not None:
            await snapshot_task
        trader.order_tracker.close()


def test_executor_snapshot_cache_is_scoped_by_intent_and_expected_order_ids():
    calls = []

    def read(**kwargs):
        calls.append(kwargs)
        return dict(complete=kwargs['order_ids'] != [17, 18], orders=[])

    executor = object.__new__(AutoExecutor)
    executor._snapshot_cache = None
    executor._sdk = SimpleNamespace(execution_snapshot=read)
    intent = dict(intent_id=INTENT, payload=dict(order_ids=[17, 18]))
    assert executor._execution_snapshot()['complete']
    assert not executor._execution_snapshot(intent)['complete']
    assert not executor._execution_snapshot(intent)['complete']
    assert executor._execution_snapshot()['complete']
    assert calls == [dict(intent_id='', order_ids=None), dict(intent_id=INTENT, order_ids=[17, 18])]
    executor._snapshot_cache = None
    assert not executor._execution_snapshot(intent)['complete']
    assert len(calls) == 3


def test_incomplete_split_replay_preserves_known_inventory_and_unknown_reservation(recovery, monkeypatch):
    executor, sdk, _ = recovery
    monkeypatch.setenv('MMR_PROTECTIVE_STOP_PCT', '0')
    sdk.broker[1111] = 100
    sdk.fill_next = 40
    executor._process_signal(make_work(quantity=140))
    opening = executor.intents.all(kind='OPEN')[0]
    first_id = sdk.accepted[0]['orderId']
    missing_id = first_id + 1
    sdk.accepted[0].update(status='Filled', totalQuantity=40)
    executor.intents.update(opening, order_ids=[first_id, missing_id])

    def snapshot(intent_id='', order_ids=None):
        rows = sdk.trades().to_dict('records')
        known = {row['orderId'] for row in rows}
        return dict(complete=all(oid in known for oid in order_ids or []), orders=rows,
                    positions_complete=True, positions=sdk.positions().to_dict('records'))

    monkeypatch.setattr(sdk, 'execution_snapshot', snapshot, raising=False)
    executor._snapshot_cache = None
    executor._reconcile_intent(opening)
    assert opening['status'] == 'UNKNOWN'
    assert executor.state.open_position('orb_test', 1111)['quantity'] == 40
    executor._process_signal(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))
    assert len(sdk.approve_calls) == 1
    assert sdk.broker[1111] == 140
    assert executor.intents.all(kind='CLOSE', active=True)
    sdk.accepted.append(dict(sdk.accepted[0], orderId=missing_id, status='Cancelled', filled=0))
    sdk.fill_next = None
    executor.manage_positions()
    assert sdk.broker[1111] == 100
    assert sdk.propose_calls[-1]['quantity'] == 40
    assert executor.state.open_position('orb_test', 1111) is None


@pytest.mark.asyncio
async def test_legacy_duplicate_snapshot_cannot_add_alias_fills_to_owned_inventory(tmp_path):
    trader = _coordinated_trader(tmp_path, held=160)
    trader.order_tracker.close()
    store = EventStore(str(tmp_path / 'broker-events.duckdb'))
    trader.order_tracker = OrderLifecycleTracker(store)
    sdk = SimpleNamespace()
    executor = AutoExecutor(str(tmp_path / 'owned.duckdb'), paper_trading=True,
                            sdk_factory=lambda: sdk)
    broken = executor.intents.create('old-owner', 100, 'OPEN',
                                     dict(bar_ts=TS.isoformat(), order_ids=[17]), status='UNKNOWN')
    healthy = executor.intents.create('valid-owner', 100, 'OPEN',
                                      dict(bar_ts=TS.isoformat(), order_ids=[18]), status='UNKNOWN')
    try:
        journal = trader.server_order_journal()
        for intent, order_id in ((broken, 17), (healthy, 18)):
            journal.claim(intent['intent_id'], intent['intent_id'], trader.ib_account)
            journal.reserve_order(intent['intent_id'], order_id, 7)
            journal.finish(intent['intent_id'], 'SUBMITTED')
        trader.order_tracker.on_trade(observed(trader, perm_id=0, reference=broken['intent_id']))
        assert await asyncio.to_thread(trader.order_tracker.flush)
        original, = trader.order_tracker.snapshot([17])
        trader.order_tracker.close()
        duplicate = dict(original, identity=f'{trader.ib_account}:perm:1717', permId=1717)
        persisted = ExecutionJournal(store.duckdb_path)
        try:
            with persisted.transaction() as conn:
                conn.execute('INSERT INTO broker_order_progress VALUES (?, ?, ?, ?, ?)',
                             [duplicate['identity'], 40, 400, 'filled', json.dumps(duplicate)])
        finally:
            persisted.close()
        trader.order_tracker = OrderLifecycleTracker(store)
        known = observed(trader, order_id=18, perm_id=1818, reference=healthy['intent_id'])
        known.order.totalQuantity = 20
        known.orderStatus.filled = 20
        trader.order_tracker.on_trade(known)
        assert await asyncio.to_thread(trader.order_tracker.flush)
        snapshots = {intent['intent_id']: await trader.execution_snapshot(
            intent_id=intent['intent_id'], order_ids=intent['payload']['order_ids'])
            for intent in (broken, healthy)}
        bad = snapshots[broken['intent_id']]
        assert not bad['complete']
        assert len(bad['orders']) == 2
        assert all(row.get('identityAmbiguous') is True for row in bad['orders'])
        sdk.execution_snapshot = lambda intent_id='', **kwargs: snapshots[intent_id]
        executor._reconcile_intent(broken)
        executor._reconcile_intent(healthy)
        assert broken['status'] == 'UNKNOWN'
        assert executor.state.open_position('old-owner', 100) is None
        assert executor.state.open_position('valid-owner', 100)['quantity'] == 20
        assert not executor._opening_is_broker_terminal(broken)
    finally:
        trader.order_tracker.close()
