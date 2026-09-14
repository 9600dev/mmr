"""Protective adoption uses native broker identity without recounting old fills.

The actual runtime intent/snapshot and lifecycle tracker run against the existing
in-memory broker. All stores are temporary; no configured service is contacted.
"""
import asyncio
import datetime as dt
import hashlib
import json
import time
from types import SimpleNamespace

import pytest

from review.test_review_order_contract import _coordinated_trader, _stock
from review.test_review_strategy_contract import FakeResult
from trader.strategy.auto_executor import AutoExecutor
from trader.strategy.execution_intents import timestamp_text


OWNER = "native_owner"
CONID = 100
BROKER_INTENT = "protective:native-replacement"


@pytest.fixture
def native_adoption(tmp_path):
    trader = _coordinated_trader(tmp_path, held=140)
    path = str(tmp_path / "executor.duckdb")
    sdk = SimpleNamespace(execution_snapshot=lambda **kwargs: asyncio.run(
        trader.execution_snapshot(**kwargs)))
    executor = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
    entry = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5)
    executor.state.record_open(OWNER, CONID, 40, entry, 501, None, None)
    try:
        yield SimpleNamespace(trader=trader, executor=executor, sdk=sdk,
                              path=path, entry=entry)
    finally:
        trader.order_tracker.close(timeout=1)


def place_external_protection(ctx):
    result = asyncio.run(ctx.trader.place_standalone_order(
        _stock(), "SELL", 40, "STP", aux_price=8, order_ref=OWNER,
        client_intent_id=BROKER_INTENT))
    assert result.is_success(), result.error
    trade, = ctx.trader.placed
    return trade


def observe_fill(ctx, trade, filled, status, *, inventory=None):
    trade.orderStatus.status = status
    trade.orderStatus.filled = filled
    trade.orderStatus.remaining = 40 - filled
    trade.orderStatus.avgFillPrice = 8
    # An explicit inventory is useful for a delayed historical execution
    # receipt: the current broker holding may already be a different entry.
    ctx.trader.inventory = 140 - filled if inventory is None else inventory
    ctx.trader.order_tracker.on_trade(trade)
    assert ctx.trader.order_tracker.flush(timeout=1)
    snapshot = asyncio.run(ctx.trader.execution_snapshot())
    row, = snapshot["orders"]
    assert snapshot["complete"] and snapshot["positions_complete"]
    assert row["clientIntentId"] == BROKER_INTENT
    assert row["orderRef"] == OWNER
    assert row["filled"] == filled and row["fillQuantityKnown"] is True
    return row


def owned_quantity(executor):
    position = executor.state.open_position(OWNER, CONID)
    return position["quantity"] if position else 0


@pytest.mark.parametrize("filled,status", [
    (10, "Submitted"), (10, "Cancelled"), (40, "Filled"),
])
def test_native_replacement_fill_reconciles_owned_inventory(native_adoption, filled, status):
    ctx = native_adoption
    trade = place_external_protection(ctx)
    observe_fill(ctx, trade, filled, status)

    ctx.executor._reconcile_intents(OWNER, CONID)

    assert owned_quantity(ctx.executor) == 40 - filled
    assert ctx.trader.inventory == 140 - filled
    assert len(ctx.trader.placed) == 1, "adoption must not submit another physical order"
    restarted = AutoExecutor(ctx.path, paper_trading=True, sdk_factory=lambda: ctx.sdk)
    restarted._reconcile_intents(OWNER, CONID)
    restarted._snapshot_cache = None
    restarted._reconcile_intents(OWNER, CONID)
    assert owned_quantity(restarted) == 40 - filled


def persist_legacy_adoption(ctx, row):
    # Historical saved state: a numeric-only adapter had already attributed
    # ten fills under its local adopt-* key, before the native ref was exposed.
    legacy_identity = [OWNER, CONID, row["account"], row["clientId"],
                       row["permId"], row["orderId"], timestamp_text(ctx.entry)]
    legacy_id = "adopt-" + hashlib.sha256(json.dumps(legacy_identity).encode()).hexdigest()
    payload = dict(bar_ts=timestamp_text(ctx.entry), order_ids=[row["orderId"]],
                   quantity=40, adopted=True)
    with ctx.executor.intents.journal.transaction() as conn:
        conn.execute("INSERT INTO execution_intents VALUES (?, ?, ?, ?, ?, ?, ?)",
                     [legacy_id, OWNER, CONID, "PROTECTIVE", "WORKING",
                      json.dumps(payload), time.time()])
    # These are pre-upgrade committed bytes, not a request for the new code to
    # authorize an attribution without the new ownership proof.
    def seed_checkpoint(conn):
        conn.execute("UPDATE auto_exec_positions SET quantity=30 "
                     "WHERE strategy=? AND conid=? AND status='OPEN'", [OWNER, CONID])
        conn.execute("INSERT INTO auto_exec_fill_progress VALUES (?, ?)", [legacy_id, 10])

    ctx.executor.state.db.execute_atomic(seed_checkpoint)
    legacy, = ctx.executor.intents.all(kind="PROTECTIVE")
    ctx.executor.intents.update(legacy, cumulative_filled=10)
    assert owned_quantity(ctx.executor) == 30
    return legacy_id


def test_persisted_synthetic_adoption_keeps_its_applied_fill_checkpoint(native_adoption):
    ctx = native_adoption
    trade = place_external_protection(ctx)
    row = observe_fill(ctx, trade, 10, "Submitted")
    legacy_id = persist_legacy_adoption(ctx, row)
    observe_fill(ctx, trade, 20, "Submitted")

    restarted = AutoExecutor(ctx.path, paper_trading=True, sdk_factory=lambda: ctx.sdk)
    restarted._reconcile_intents(OWNER, CONID)

    assert owned_quantity(restarted) == 20, "only the ten newly reported fills may apply"
    saved, = restarted.intents.all(kind="PROTECTIVE")
    assert saved["intent_id"] == legacy_id, "do not fork the already applied identity"
    _positions, checkpoints = restarted.state.ownership_snapshot(restarted.intents.all())
    assert checkpoints == {legacy_id: 20}
    restarted_again = AutoExecutor(ctx.path, paper_trading=True, sdk_factory=lambda: ctx.sdk)
    restarted_again._reconcile_intents(OWNER, CONID)
    assert owned_quantity(restarted_again) == 20
    assert len(restarted_again.intents.all(kind="PROTECTIVE")) == 1
    assert ctx.trader.inventory == 120


def test_cached_adoption_alias_uses_local_checkpoint_during_journal_outage(
        native_adoption, monkeypatch):
    ctx = native_adoption
    trade = place_external_protection(ctx)
    row = observe_fill(ctx, trade, 10, "Submitted")
    legacy_id = persist_legacy_adoption(ctx, row)
    ctx.executor._reconcile_intents(OWNER, CONID)
    ctx.executor._load_open_view()
    cached, = ctx.executor.managed_positions()
    assert cached["quantity"] == 30
    assert cached["fill_checkpoints"][legacy_id] == 10
    observe_fill(ctx, trade, 20, "Submitted")
    ctx.executor._snapshot_cache = None
    requests = []
    faults = []

    def record_emergency_request(**kwargs):
        requests.append(kwargs)
        return FakeResult(ok=False, error="UNKNOWN: diagnostic sink records request only")

    def unavailable_journal():
        raise OSError("local adoption journal unavailable")

    ctx.sdk.emergency_close_position = record_emergency_request
    with monkeypatch.context() as patch:
        patch.setattr(ctx.executor.intents.journal, "transaction", unavailable_journal)
        patch.setattr("trader.strategy.auto_executor.logging.exception",
                      lambda message, *args, **kwargs: faults.append(message))
        ctx.executor._remember_emergency_exit(
            OWNER, CONID, ctx.entry, "exit during local storage outage",
            entry_bar_ts=ctx.entry, request_entry_bar_ts=ctx.entry, capture_explicit=False)
        ctx.executor._retry_emergency_exits()

    assert [request["quantity"] for request in requests] == [20]
    assert faults == [], "fresh native fill matching must not require the local journal"
    assert ctx.trader.inventory == 120, "the diagnostic sink places no physical order"
    assert len(ctx.trader.placed) == 1


def test_terminal_stop_from_previous_entry_cannot_reduce_new_owned_inventory(native_adoption):
    ctx = native_adoption
    old_stop = place_external_protection(ctx)
    observe_fill(ctx, old_stop, 40, "Filled")
    ctx.executor.state.record_close(OWNER, CONID, "CLOSED", "previous entry finished")
    newer_entry = dt.datetime.now(dt.timezone.utc)
    ctx.executor.state.record_open(OWNER, CONID, 60, newer_entry, 502, None, None)
    ctx.trader.inventory = 160
    # An old broker order is observed AGAIN after the new entry. Fresh replay
    # observation time must not become proof of fresh submission/ownership.
    ctx.trader.order_tracker.on_trade(old_stop)
    assert ctx.trader.order_tracker.flush(timeout=1)

    ctx.executor._reconcile_intents(OWNER, CONID)
    restarted = AutoExecutor(ctx.path, paper_trading=True, sdk_factory=lambda: ctx.sdk)
    restarted._reconcile_intents(OWNER, CONID)

    assert owned_quantity(restarted) == 60
    assert ctx.trader.inventory == 160
    assert len(ctx.trader.placed) == 1


@pytest.mark.parametrize("old_status", ["Submitted", "Cancelled"])
def test_adopted_receipt_does_not_debit_a_new_same_bar_ownership_epoch(
        native_adoption, monkeypatch, old_status):
    ctx = native_adoption
    old_stop = place_external_protection(ctx)
    observe_fill(ctx, old_stop, 10, old_status)
    ctx.executor._reconcile_intents(OWNER, CONID)
    assert owned_quantity(ctx.executor) == 30
    old_position = ctx.executor.state.open_position(OWNER, CONID)
    adopted, = ctx.executor.intents.all(kind="PROTECTIVE")
    assert adopted["payload"]["ownership_epoch"] == old_position["ownership_epoch"]

    ctx.executor.state.record_close(OWNER, CONID, "CLOSED", "old residual closed externally")
    # Neither a repeated bar label/proposal nor equal wall-clock resolution
    # proves that a later owned holding is the same holding.
    with monkeypatch.context() as patch:
        patch.setattr("trader.strategy.auto_executor.time.time",
                      lambda: old_position["ownership_started_at"])
        ctx.executor.state.record_open(OWNER, CONID, 60, ctx.entry, 501, None, None)
    new_position = ctx.executor.state.open_position(OWNER, CONID)
    assert new_position["ownership_started_at"] == old_position["ownership_started_at"]
    assert new_position["ownership_epoch"] != old_position["ownership_epoch"]
    # The old broker execution is learned late; it already belongs to the
    # completed old holding. Current inventory is the manual100 plus new60.
    observe_fill(ctx, old_stop, 20, "Cancelled", inventory=160)

    for _ in range(2):
        restarted = AutoExecutor(ctx.path, paper_trading=True, sdk_factory=lambda: ctx.sdk)
        restarted._reconcile_intents(OWNER, CONID)
        assert owned_quantity(restarted) == 60
        saved, = restarted.intents.all(kind="PROTECTIVE")
        assert saved["intent_id"] == adopted["intent_id"]
        assert saved["payload"]["ownership_epoch"] == old_position["ownership_epoch"]
        _positions, checkpoints = restarted.state.ownership_snapshot(restarted.intents.all())
        assert checkpoints[adopted["intent_id"]] == 20
    assert ctx.trader.inventory == 160
    assert len(ctx.trader.placed) == 1


def test_legacy_owned_row_without_creation_proof_keeps_adoption_unresolved(native_adoption):
    ctx = native_adoption
    ctx.executor.state.db.execute(
        "UPDATE auto_exec_positions SET ownership_epoch=NULL, ownership_started_at=NULL "
        "WHERE strategy=? AND conid=? AND status='OPEN'", [OWNER, CONID])
    stop = place_external_protection(ctx)
    observe_fill(ctx, stop, 10, "Submitted")

    for _ in range(2):
        restarted = AutoExecutor(ctx.path, paper_trading=True, sdk_factory=lambda: ctx.sdk)
        restarted._reconcile_intents(OWNER, CONID)
        assert owned_quantity(restarted) == 40, "missing epoch proof cannot authorize a debit"
        adopted, = restarted.intents.all(kind="PROTECTIVE")
        assert adopted["payload"]["attribution_unresolved"] is True
        assert adopted["payload"].get("ownership_epoch") is None
    assert ctx.trader.inventory == 130
    assert len(ctx.trader.placed) == 1


def test_stop_claim_before_delayed_open_fill_application_uses_open_origin(
        native_adoption, monkeypatch):
    ctx = native_adoption
    ctx.executor.state.record_close(OWNER, CONID, "CLOSED", "fixture holding awaits durable OPEN receipt")
    opening = ctx.executor.intents.create(
        OWNER, CONID, "OPEN", dict(bar_ts=timestamp_text(ctx.entry), quantity=40, proposal_id=501),
        status="WORKING")
    # The broker already has the opening fill, and an authorized independent
    # workflow has placed and partially executed its stop before the executor
    # finishes applying that OPEN receipt locally.
    stop = place_external_protection(ctx)
    row = observe_fill(ctx, stop, 10, "Cancelled")
    claim_time = row["brokerIntentCreatedAt"]
    with monkeypatch.context() as patch:
        patch.setattr("trader.strategy.auto_executor.time.time", lambda: claim_time + 100)
        assert ctx.executor.state.apply_fill(opening, 40) == 40
    ctx.executor.intents.update(opening, status="FILLED", cumulative_filled=40)
    owned = ctx.executor.state.open_position(OWNER, CONID)
    assert owned["ownership_started_at"] <= claim_time

    for _ in range(2):
        restarted = AutoExecutor(ctx.path, paper_trading=True, sdk_factory=lambda: ctx.sdk)
        restarted._reconcile_intents(OWNER, CONID)
        assert owned_quantity(restarted) == 30
        adopted, = restarted.intents.all(kind="PROTECTIVE")
        assert not adopted["payload"].get("attribution_unresolved")
    assert ctx.trader.inventory == 130
    assert len(ctx.trader.placed) == 1


def test_live_legacy_server_claim_cannot_prove_partial_fill_ownership(
        native_adoption, monkeypatch):
    ctx = native_adoption
    stop = place_external_protection(ctx)
    journal = ctx.trader.server_order_journal()
    with journal.journal.transaction() as conn:
        conn.execute("UPDATE server_order_intents SET created_at=NULL WHERE intent_id=?", [BROKER_INTENT])
    row = observe_fill(ctx, stop, 10, "Submitted")
    assert row.get("brokerIntentCreatedAt") is None
    cancelled, replacements, faults = [], [], []

    def cancel(order_id):
        cancelled.append(order_id)
        stop.orderStatus.status = "Cancelled"
        ctx.trader.order_tracker.on_trade(stop)
        assert ctx.trader.order_tracker.flush(timeout=1)
        return FakeResult()

    ctx.sdk.cancel = cancel
    ctx.sdk.place_protective_order = lambda **kwargs: replacements.append(kwargs)
    monkeypatch.setattr("trader.strategy.auto_executor.logging.exception",
                        lambda message, *args, **kwargs: faults.append(message))
    for _ in range(2):
        restarted = AutoExecutor(ctx.path, paper_trading=True, sdk_factory=lambda: ctx.sdk)
        restarted._reconcile_intents(OWNER, CONID)
        restarted._ensure_protective(OWNER, CONID)
        assert owned_quantity(restarted) == 40
        adopted, = restarted.intents.all(kind="PROTECTIVE")
        assert adopted["payload"]["attribution_unresolved"] is True
        assert adopted["status"] == "UNKNOWN"
        assert stop.orderStatus.status == "Submitted"
    assert ctx.trader.inventory == 130
    assert cancelled == replacements == faults == []
    assert len(ctx.trader.placed) == 1


@pytest.fixture
def tail_adoption_context(tmp_path):
    """Native local coordinator/tracker with isolated durable recovery stores."""
    from trader.data.event_store import EventStore
    from trader.trading.order_lifecycle import OrderLifecycleTracker

    trader = _coordinated_trader(tmp_path, held=140)
    # The coordinator's memory event adapter cannot query aware fill receipts
    # against naive risk cutoffs. Replace its unused tracker before any work.
    previous_tracker = trader.order_tracker
    previous_tracker.close(timeout=1)
    if previous_tracker._journal is not None:
        previous_tracker._journal.close()
    if previous_tracker._temporary is not None:
        previous_tracker._temporary.cleanup()
    trader.event_store = EventStore(str(tmp_path / 'tail_broker_events.duckdb'))
    trader.order_tracker = OrderLifecycleTracker(trader.event_store)
    path = str(tmp_path / 'tail_adoption_executor.duckdb')
    sdk = SimpleNamespace(execution_snapshot=lambda **kwargs: asyncio.run(
        trader.execution_snapshot(**kwargs)))
    executor = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
    entry = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5)
    executor.state.record_open(OWNER, CONID, 40, entry, 501, None, None)
    ctx = SimpleNamespace(trader=trader, executor=executor, sdk=sdk, path=path,
                          entry=entry, executors=[executor],
                          trackers=[trader.order_tracker], native_history=[],
                          broker_event_stores=[trader.event_store],
                          new_history_event_path=str(
                              tmp_path / 'tail_broker_events_new_history.duckdb'))
    try:
        yield ctx
    finally:
        for current in ctx.executors:
            current.intents.journal.close()
        for tracker in ctx.trackers:
            tracker.close(timeout=1)
            if tracker._journal is not None:
                tracker._journal.close()
            if tracker._temporary is not None:
                tracker._temporary.cleanup()
        journal = getattr(trader, '_server_order_journal', None)
        if journal is not None:
            journal.journal.close()


def _tail_reopen(ctx):
    executor = AutoExecutor(ctx.path, paper_trading=True, sdk_factory=lambda: ctx.sdk)
    ctx.executors.append(executor)
    ctx.executor = executor
    return executor


def _tail_snapshot(ctx):
    snapshot = ctx.sdk.execution_snapshot()
    assert snapshot['complete'] and snapshot['positions_complete']
    ctx.executor._snapshot_cache = None
    return snapshot


def _tail_row(ctx, reference):
    rows = [row for row in _tail_snapshot(ctx)['orders']
            if row['clientIntentId'] == reference]
    assert len(rows) == 1
    return rows[0]


def _tail_observe(ctx, trade, filled=0, status='Submitted', *, unknown=False):
    # These are external broker observations on native Trade objects. No
    # acceptance/snapshot dictionary is supplied to the code under test.
    trade.orderStatus.status = status
    trade.orderStatus.filled = float('nan') if unknown else filled
    trade.order.filledQuantity = float('nan') if unknown else filled
    trade.orderStatus.remaining = (float('nan') if unknown else
                                   max(0, trade.order.totalQuantity - filled))
    trade.orderStatus.avgFillPrice = 8
    ctx.trader.order_tracker.on_trade(trade, completed=unknown)
    assert ctx.trader.order_tracker.flush(timeout=1)
    ctx.executor._snapshot_cache = None


def _tail_historical_trade(ctx, reference, *, oid=1, permanent=8001,
                           client=8, instrument=CONID, owner=OWNER,
                           action='SELL', order_type='STP', status='Cancelled'):
    from ib_async import Contract, Order, OrderStatus, Trade

    contract = Contract(conId=instrument, secType='STK', symbol='TAIL',
                        exchange='SMART', currency='USD')
    order = Order(orderId=oid, clientId=client, permId=permanent,
                  account=ctx.trader.ib_account, action=action,
                  orderType=order_type, totalQuantity=40,
                  orderRef=owner + '|mmr:' + reference)
    trade = Trade(contract, order, OrderStatus(orderId=oid, clientId=client,
                  permId=permanent, status=status, filled=0, remaining=40))
    ctx.native_history.append(trade)
    _tail_observe(ctx, trade, status=status)
    return trade


def _tail_place_stop(ctx, reference, *, quantity=40):
    before = len(ctx.trader.placed)
    result = asyncio.run(ctx.trader.place_standalone_order(
        _stock(), 'SELL', quantity, 'STP', aux_price=8, order_ref=OWNER,
        client_intent_id=reference))
    assert result.is_success(), result.error
    assert len(ctx.trader.placed) == before + 1
    return ctx.trader.placed[-1]


def _tail_seed_aliasless_legacy(ctx, row, *, entry=None):
    # Dated pre-ownership-proof checkpoint format, before broker aliases were
    # saved. The historical physical key is computed independently here.
    bar = timestamp_text(ctx.entry if entry is None else entry)
    identity = [OWNER, CONID, row['account'], row['clientId'], row['permId'],
                row['orderId'], bar]
    legacy_id = 'adopt-' + hashlib.sha256(json.dumps(identity).encode()).hexdigest()
    payload = dict(bar_ts=bar, order_ids=[row['orderId']], quantity=40, adopted=True)
    with ctx.executor.intents.journal.transaction() as conn:
        conn.execute('INSERT INTO execution_intents '
                     '(intent_id,strategy,conid,kind,status,payload,updated) '
                     'VALUES (?,?,?,?,?,?,?)',
                     [legacy_id, OWNER, CONID, 'PROTECTIVE', 'WORKING',
                      json.dumps(payload), time.time()])
    return legacy_id


def _tail_complete_add(ctx, quantity, entry):
    from trader.trading.risk_gate import RiskGate, RiskLimits

    ctx.trader.risk_gate = RiskGate(RiskLimits(), ctx.trader.event_store)
    opening = ctx.executor.intents.create(OWNER, CONID, 'OPEN',
        dict(bar_ts=timestamp_text(entry), quantity=quantity), status='SUBMITTING')
    before = len(ctx.trader.placed)
    result = asyncio.run(ctx.trader.place_expressive_order(
        _stock(), 'BUY', quantity, {'order_type': 'MARKET'}, algo_name=OWNER,
        client_intent_id=opening['intent_id']))
    assert result.is_success(), result.error
    assert len(ctx.trader.placed) == before + 1
    trade = ctx.trader.placed[-1]
    ctx.executor.intents.update(opening, status='WORKING', order_ids=[trade.order.orderId])
    ctx.trader.inventory += quantity
    _tail_observe(ctx, trade, quantity, 'Filled')
    ctx.executor._reconcile_intent(opening)
    assert opening['status'] == 'FILLED'
    return opening


@pytest.mark.parametrize('case', [
    'zero_order', 'foreign_owner', 'buy_action', 'unknown_instrument', 'foreign_instrument',
])
def test_tracked_pointer_cannot_adopt_an_unproven_native_order(tail_adoption_context, case):
    from ib_async import Position

    ctx = tail_adoption_context
    instrument = 1 if case == 'unknown_instrument' else CONID
    if instrument != CONID:
        ctx.executor.state.record_close(OWNER, CONID, 'CLOSED', 'fixture uses exact conId1')
        ctx.executor.state.record_open(OWNER, instrument, 40, ctx.entry, 501, None, None)
        contract = _stock()
        contract.conId = instrument
        ctx.trader.get_positions = lambda: [Position(
            ctx.trader.ib_account, contract, ctx.trader.inventory, 10)]
    ctx.executor.state.set_protective(OWNER, instrument, 1)
    _tail_historical_trade(ctx, 'history:tracked-scope',
        oid=0 if case == 'zero_order' else 1,
        instrument=(0 if case == 'unknown_instrument' else
                    200 if case == 'foreign_instrument' else instrument),
        owner='another_owner' if case == 'foreign_owner' else OWNER,
        action='BUY' if case == 'buy_action' else 'SELL',
        order_type='LMT')

    ctx.executor._adopt_observed_protectives(OWNER, instrument)

    assert ctx.executor.intents.all(kind='PROTECTIVE') == []
    assert ctx.executor.state.open_position(OWNER, instrument)['quantity'] == 40
    assert ctx.trader.placed == []


@pytest.mark.parametrize('first_kind', ['external', 'own'])
def test_adoption_visits_later_physical_stops_after_an_existing_stop(
        tail_adoption_context, first_kind):
    ctx = tail_adoption_context
    local = None
    if first_kind == 'own':
        position = ctx.executor.state.open_position(OWNER, CONID)
        local = ctx.executor.intents.create(OWNER, CONID, 'PROTECTIVE',
            dict(bar_ts=timestamp_text(ctx.entry), quantity=40,
                 ownership_epoch=position['ownership_epoch'],
                 ownership_started_at=position['ownership_started_at']))
    first_reference = local['intent_id'] if local is not None else 'protective:tail-first'
    first = _tail_place_stop(ctx, first_reference)
    if local is not None:
        ctx.executor.intents.update(local, status='WORKING', order_ids=[first.order.orderId])
    ctx.executor.state.set_protective(OWNER, CONID, first.order.orderId)
    later_reference = 'protective:tail-later'
    later = _tail_place_stop(ctx, later_reference)
    _tail_observe(ctx, later, 0, 'Cancelled')
    physical = _tail_row(ctx, later_reference)

    ctx.executor._adopt_observed_protectives(OWNER, CONID)

    saved = ctx.executor.intents.all(kind='PROTECTIVE')
    assert len(saved) == 2
    assert {ctx.executor._broker_intent_id(row) for row in saved} == {first_reference, later_reference}
    recovered = [row for row in saved if row['payload'].get('broker_intent_id') == later_reference]
    assert len(recovered) == 1
    saved_later = recovered[0]
    expected = [OWNER, CONID, physical['account'], physical['clientId'], physical['permId'],
                physical['orderId'], timestamp_text(ctx.entry)]
    assert saved_later['payload']['bar_ts'] == timestamp_text(ctx.entry)
    assert saved_later['intent_id'] == 'adopt-' + hashlib.sha256(json.dumps(expected).encode()).hexdigest()
    assert owned_quantity(ctx.executor) == 40 and len(ctx.trader.placed) == 2


def test_zero_numeric_legacy_checkpoint_does_not_match_a_new_permanent_order(tail_adoption_context):
    from trader.data.event_store import EventStore
    from trader.trading.order_lifecycle import OrderLifecycleTracker

    ctx = tail_adoption_context
    _tail_historical_trade(ctx, 'history:zero-old', oid=0, permanent=8101)
    prior_row = _tail_row(ctx, 'history:zero-old')
    legacy_id = _tail_seed_aliasless_legacy(ctx, prior_row)
    # Historical recovery premise: a pre-upgrade executor checkpoint
    # survives, but the replacement broker event database and its native
    # journal are NEW and empty. Only the newer physical row is replayed.
    # Retain the old stores unchanged; this is not an intact-journal restart.
    ctx.trader.order_tracker.close(timeout=1)
    ctx.trader.event_store = EventStore(ctx.new_history_event_path)
    ctx.broker_event_stores.append(ctx.trader.event_store)
    ctx.trader.order_tracker = OrderLifecycleTracker(ctx.trader.event_store)
    ctx.trackers.append(ctx.trader.order_tracker)
    _tail_historical_trade(ctx, 'history:zero-new', oid=0, permanent=8102)
    _tail_reopen(ctx)._reconcile_intents(OWNER, CONID)

    saved = ctx.executor.intents.all(kind='PROTECTIVE')
    assert len(saved) == 2
    old = [row for row in saved if row['intent_id'] == legacy_id]
    new = [row for row in saved if row['payload'].get('broker_intent_id') == 'history:zero-new']
    assert len(old) == len(new) == 1
    assert not old[0]['payload'].get('broker_intent_id')
    assert all(row['status'] == 'UNKNOWN' and row['payload']['attribution_unresolved'] for row in saved)
    assert new[0]['payload']['order_ids'] == [0]
    assert new[0]['status'] == 'UNKNOWN' and new[0]['payload']['attribution_unresolved']
    assert owned_quantity(ctx.executor) == 40 and ctx.trader.placed == []


def test_created_protective_without_wire_ids_does_not_block_external_recovery(tail_adoption_context):
    ctx = tail_adoption_context
    pending = ctx.executor.intents.create(OWNER, CONID, 'PROTECTIVE',
        dict(bar_ts=timestamp_text(ctx.entry), quantity=40))
    reference = 'protective:external-after-created'
    _tail_place_stop(ctx, reference)
    _tail_reopen(ctx)._adopt_observed_protectives(OWNER, CONID)

    saved = ctx.executor.intents.all(kind='PROTECTIVE')
    assert len(saved) == 2
    created = [row for row in saved if row['intent_id'] == pending['intent_id']]
    assert len(created) == 1
    assert created[0]['status'] == 'CREATED' and 'order_ids' not in created[0]['payload']
    assert any(row['payload'].get('broker_intent_id') == reference for row in saved)
    assert owned_quantity(ctx.executor) == 40 and len(ctx.trader.placed) == 1


def test_two_aliasless_local_matches_refuse_recovery_with_identity_context(tail_adoption_context):
    from trader.strategy.auto_executor import AutoExecutionError

    ctx = tail_adoption_context
    _tail_historical_trade(ctx, 'history:ambiguous-a', oid=17, permanent=8201, client=7)
    a = _tail_row(ctx, 'history:ambiguous-a')
    first_id = _tail_seed_aliasless_legacy(ctx, a)
    _tail_historical_trade(ctx, 'history:ambiguous-b', oid=17, permanent=8202, client=8)
    b = _tail_row(ctx, 'history:ambiguous-b')
    second_id = _tail_seed_aliasless_legacy(ctx, b)
    assert first_id != second_id
    failure = None
    try:
        _tail_reopen(ctx)._adopt_observed_protectives(OWNER, CONID)
    except AutoExecutionError as exc:
        failure = exc

    assert failure is not None, 'two persisted physical checkpoints require reconciliation'
    message = str(failure).lower()
    assert 'protective' in message and 'multiple' in message and 'intent' in message
    saved = ctx.executor.intents.all(kind='PROTECTIVE')
    assert {row['intent_id'] for row in saved} == {first_id, second_id}
    assert all(not row['payload'].get('broker_intent_id') for row in saved)
    assert owned_quantity(ctx.executor) == 40 and ctx.trader.placed == []


@pytest.mark.parametrize('old_status,unknown', [
    pytest.param('Filled', False, id='filled'),
    pytest.param('ApiCancelled', False, id='api-cancelled'),
    pytest.param('Inactive', False, id='inactive'),
    pytest.param('Filled', True, id='unknown-final-quantity'),
])
def test_old_terminal_claim_is_skipped_without_hiding_current_protection(
        tail_adoption_context, monkeypatch, old_status, unknown):
    ctx = tail_adoption_context
    old_reference, new_reference = 'protective:old-terminal', 'protective:current-terminal'
    old = _tail_place_stop(ctx, old_reference)
    old_created = _tail_row(ctx, old_reference)['brokerIntentCreatedAt']
    _tail_observe(ctx, old, 40 if old_status == 'Filled' and not unknown else 0,
                  old_status, unknown=unknown)
    ctx.executor.state.record_close(OWNER, CONID, 'CLOSED', 'prior holding finished externally')
    with monkeypatch.context() as patch:
        patch.setattr('trader.strategy.auto_executor.time.time', lambda: old_created + 1)
        ctx.executor.state.record_open(OWNER, CONID, 60, ctx.entry + dt.timedelta(minutes=1),
                                       502, None, None)
    ctx.trader.inventory = 160
    with monkeypatch.context() as patch:
        patch.setattr('trader.strategy.auto_executor.time.time', lambda: old_created + 2)
        current = _tail_place_stop(ctx, new_reference)
    _tail_observe(ctx, current, 10, 'Cancelled')
    ctx.trader.inventory = 150
    old_row = _tail_row(ctx, old_reference)
    if unknown:
        assert old_row['status'] == 'Unknown' and old_row['brokerStatus'] == 'Filled'
        assert old_row['fillQuantityKnown'] is False
    else:
        assert old_row['status'] == old_status

    _tail_reopen(ctx)._reconcile_intents(OWNER, CONID)

    saved = ctx.executor.intents.all(kind='PROTECTIVE')
    assert len(saved) == 1
    assert saved[0]['payload']['broker_intent_id'] == new_reference
    assert saved[0]['payload']['cumulative_filled'] == 10
    assert owned_quantity(ctx.executor) == 50
    assert ctx.trader.inventory == 150 and len(ctx.trader.placed) == 2


def test_protective_claim_equal_to_ownership_origin_is_reconciled(tail_adoption_context, monkeypatch):
    ctx = tail_adoption_context
    origin = ctx.executor.state.open_position(OWNER, CONID)['ownership_started_at']
    reference = 'protective:equal-origin'
    with monkeypatch.context() as patch:
        patch.setattr('trader.strategy.auto_executor.time.time', lambda: origin)
        stop = _tail_place_stop(ctx, reference)
    _tail_observe(ctx, stop, 10, 'Cancelled')
    ctx.trader.inventory = 130
    assert _tail_row(ctx, reference)['brokerIntentCreatedAt'] == origin

    ctx.executor._reconcile_intents(OWNER, CONID)

    saved = ctx.executor.intents.all(kind='PROTECTIVE')
    assert len(saved) == 1
    assert saved[0]['payload']['cumulative_filled'] == 10
    assert not saved[0]['payload']['attribution_unresolved']
    assert owned_quantity(ctx.executor) == 30 and ctx.trader.inventory == 130


def test_bound_adoption_resolves_after_add_when_native_provenance_returns(
        tail_adoption_context, monkeypatch):
    ctx = tail_adoption_context
    reference = 'protective:provenance-recovery'
    stop = _tail_place_stop(ctx, reference)
    _tail_observe(ctx, stop, 10)
    ctx.trader.inventory = 130
    journal = ctx.trader.server_order_journal()

    def unavailable_provenance(*args, **kwargs):
        raise OSError('creation provenance temporarily unavailable')

    with monkeypatch.context() as patch:
        patch.setattr(journal, 'get_many', unavailable_provenance)
        ctx.executor._reconcile_intents(OWNER, CONID)
    initial = ctx.executor.intents.all(kind='PROTECTIVE')
    assert len(initial) == 1 and initial[0]['status'] == 'UNKNOWN'
    old_epoch = initial[0]['payload']['ownership_epoch']
    assert old_epoch is not None and initial[0]['payload']['attribution_unresolved']
    _tail_complete_add(ctx, 20, ctx.entry + dt.timedelta(minutes=1))
    position = ctx.executor.state.open_position(OWNER, CONID)
    assert position['quantity'] == 60 and position['ownership_epoch'] == old_epoch
    assert timestamp_text(position['entry_bar_ts']) != initial[0]['payload']['bar_ts']

    _tail_reopen(ctx)._reconcile_intents(OWNER, CONID)

    saved = ctx.executor.intents.all(kind='PROTECTIVE')
    assert len(saved) == 1 and saved[0]['intent_id'] == initial[0]['intent_id']
    assert saved[0]['payload']['ownership_epoch'] == old_epoch
    assert not saved[0]['payload']['attribution_unresolved']
    assert saved[0]['payload']['cumulative_filled'] == 10
    assert owned_quantity(ctx.executor) == 50 and ctx.trader.inventory == 150
    assert len(ctx.trader.placed) == 2


@pytest.mark.parametrize('missing_proof', ['origin', 'entry'])
def test_aliasless_legacy_adoption_requires_creation_and_concrete_entry_proof(
        tail_adoption_context, missing_proof):
    ctx = tail_adoption_context
    reference = 'protective:legacy-proof'
    stop = _tail_place_stop(ctx, reference)
    _tail_observe(ctx, stop, 10)
    ctx.trader.inventory = 130
    legacy_id = _tail_seed_aliasless_legacy(ctx, _tail_row(ctx, reference))
    if missing_proof == 'origin':
        # Explicit old server-journal format: the nullable creation column
        # cannot establish when this native physical order was submitted.
        journal = ctx.trader.server_order_journal()
        with journal.journal.transaction() as conn:
            conn.execute('UPDATE server_order_intents SET created_at=NULL WHERE intent_id=?', [reference])
        assert _tail_row(ctx, reference).get('brokerIntentCreatedAt') is None
        expected_quantity = 40
    else:
        _tail_complete_add(ctx, 20, ctx.entry + dt.timedelta(minutes=1))
        expected_quantity = 60

    _tail_reopen(ctx)._reconcile_intents(OWNER, CONID)

    saved = ctx.executor.intents.all(kind='PROTECTIVE')
    assert len(saved) == 1 and saved[0]['intent_id'] == legacy_id
    assert saved[0]['status'] == 'UNKNOWN'
    assert saved[0]['payload'].get('ownership_epoch') is None
    assert saved[0]['payload']['attribution_unresolved'] is True
    assert owned_quantity(ctx.executor) == expected_quantity
    assert ctx.trader.inventory == expected_quantity + 90
