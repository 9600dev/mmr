"""Explicit exit authority follows identified inventory, including degraded reads."""
import hashlib
import pandas as pd
import pytest

from test_execution_recovery import recovery
from review.test_review_strategy_contract import FakeResult, TS, make_work
from trader.objects import Action
from trader.strategy.auto_executor import AutoExecutor
from trader.strategy.execution_intents import timestamp_text
from trader.trading.order_reference import split_order_reference


def _pending_add(executor, sdk, quantity=20):
    executor.cooldown_seconds = 0
    sdk.broker[1111] = 100
    executor._process_signal(make_work(quantity=40))
    sdk.fill_next = 0
    executor._process_signal(make_work(quantity=quantity, pyramid_max_adds=1,
                                       bar_ts=TS + pd.Timedelta(seconds=10)))
    return sdk.accepted[-1]


@pytest.mark.parametrize('previously_owned', [0, 40])
def test_explicit_sell_captures_committed_add_when_ownership_cache_refresh_failed(
        recovery, monkeypatch, previously_owned):
    executor, sdk, _ = recovery
    if previously_owned:
        addition = _pending_add(executor, sdk)
    else:
        sdk.broker[1111] = 100
        sdk.fill_next = 0
        executor._process_signal(make_work(quantity=20, bar_ts=TS + pd.Timedelta(seconds=10)))
        addition = sdk.accepted[-1]
    addition.update(filled=20, status='Filled')
    sdk.broker[1111] = 120 + previously_owned
    submitted = []
    faults = []
    unavailable = False
    read_position = executor.state.open_position

    def lose_view_after_commit():
        nonlocal unavailable
        unavailable = True
        raise OSError('ownership view unavailable after committed add')

    def current_position(*args):
        if unavailable:
            raise OSError('ownership storage temporarily unavailable')
        return read_position(*args)

    def emergency(**kwargs):
        submitted.append(kwargs['quantity'])
        for order_id in list(sdk.active_stops):
            sdk.cancel(order_id)
        sdk.accepted.append(dict(
            orderId=1999, orderRef='orb_test|mmr:' + kwargs['client_intent_id'],
            clientIntentId=kwargs['client_intent_id'], conId=1111, action='SELL',
            orderType='LMT', totalQuantity=kwargs['quantity'], filled=kwargs['quantity'],
            avgFillPrice=100, status='Filled'))
        sdk.broker[1111] -= kwargs['quantity']
        return FakeResult(obj=[1999])

    monkeypatch.setattr(sdk, 'emergency_close_position', emergency, raising=False)
    with monkeypatch.context() as patch:
        patch.setattr(executor, '_load_open_view', lose_view_after_commit)
        patch.setattr(executor.state, 'open_position', current_position)
        patch.setattr('trader.strategy.auto_executor.logging.exception',
                      lambda message, *args, **kwargs: faults.append(message))
        executor._process_signal(make_work(action=Action.SELL,
                                           bar_ts=TS + pd.Timedelta(seconds=20)))
    assert unavailable, 'the add must commit before the cached-view outage'
    assert all(item['status'] == 'FILLED' for item in executor.intents.all(kind='OPEN'))
    for _ in range(3):
        executor.manage_positions()
    assert submitted == [previously_owned + 20], 'the committed opening belongs to the explicit SELL'
    assert sdk.broker[1111] == 100
    assert executor.state.open_position('orb_test', 1111) is None


def test_captured_open_identity_can_gain_proposal_provenance_without_expanding_scope(recovery):
    executor, sdk, _ = recovery
    executor._process_signal(make_work(quantity=40))
    entry = TS + pd.Timedelta(seconds=10)
    captured = executor.intents.create('orb_test', 1111, 'OPEN',
                                       dict(bar_ts=timestamp_text(entry)), status='CREATED')
    scope = executor._capture_exit_scope('orb_test', 1111)
    assert scope['openings'][captured['intent_id']]['proposal_id'] is None
    executor.intents.update(captured, proposal_id=888)
    executor.state.apply_fill(captured, 20)
    assert executor._scope_matches_position(scope, executor.state.open_position('orb_test', 1111))
    # The same timestamp does not authorize a different proposal's inventory.
    executor.state.record_close('orb_test', 1111, 'CLOSED', 'captured inventory ended')
    executor.state.record_open('orb_test', 1111, 80, entry, 889, None, None)
    assert not executor._scope_matches_position(scope, executor.state.open_position('orb_test', 1111))


@pytest.mark.parametrize('parent_id', ['auto-parent', 'auto-' + 'f' * 150])
def test_broker_emergency_parent_reference_restores_exact_captured_open_authority(recovery, parent_id):
    executor, sdk, path = recovery
    addition = _pending_add(executor, sdk)
    addition.update(status='Cancelled', fillQuantityKnown=False)
    scope = executor._capture_exit_scope('orb_test', 1111)
    parent = executor.intents.restore_exit(parent_id, 'orb_test', 1111,
        dict(bar_ts=timestamp_text(TS), reason='explicit SELL before local outage',
             explicit_exit_scope=scope, policy_entry_bar_ts=None, exit_request_active=True))
    executor.intents.update(parent, status='WAITING')
    emergency_id = executor._emergency_identity(
        parent_id, executor.state.open_position('orb_test', 1111)['ownership_epoch'])
    assert len(emergency_id.encode()) <= 160
    assert split_order_reference('orb_test|mmr:' + emergency_id) == ('orb_test', emergency_id)
    for order_id in list(sdk.active_stops):
        sdk.cancel(order_id)
    sdk.accepted.append(dict(
        orderId=1999, orderRef='orb_test|mmr:' + emergency_id,
        clientIntentId=emergency_id, conId=1111, action='SELL', orderType='LMT',
        totalQuantity=40, filled=40, avgFillPrice=100, status='Filled'))
    sdk.broker[1111] = 100
    restarted = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
    restarted.manage_positions()
    recovered = next(item for item in restarted.intents.all(kind='CLOSE')
                     if item['intent_id'] == emergency_id)
    assert recovered['payload']['parent_intent_id'] == parent_id
    assert recovered['payload']['explicit_exit_scope'] == scope
    addition.update(filled=20, fillQuantityKnown=True)
    sdk.broker[1111] = 120
    sdk.fill_next = None
    for _ in range(3):
        restarted.manage_positions()
    assert sdk.broker[1111] == 100
    assert restarted.state.open_position('orb_test', 1111) is None
    assert [row['filled'] for row in sdk.accepted if row['action'] == 'SELL'] == [40, 20]


@pytest.mark.parametrize('parent_problem', ['missing', 'owner', 'instrument', 'kind', 'ambiguous', 'raw'])
def test_unproven_emergency_parent_records_fill_without_continuing_authority(
        recovery, monkeypatch, parent_problem):
    executor, sdk, _ = recovery
    addition = _pending_add(executor, sdk)
    addition.update(status='Cancelled', fillQuantityKnown=False)
    payload = dict(bar_ts=timestamp_text(TS), reason='historical explicit SELL',
                   explicit_exit_scope=executor._capture_exit_scope('orb_test', 1111),
                   exit_request_active=False)
    parent_id = 'absent-parent'
    if parent_problem not in ('missing', 'raw'):
        parent = executor.intents.create(
            'different-owner' if parent_problem == 'owner' else 'orb_test',
            2222 if parent_problem == 'instrument' else 1111,
            'PROTECTIVE' if parent_problem == 'kind' else 'CLOSE', payload, status='RESOLVED')
        parent_id = parent['intent_id']
    emergency_id = executor._emergency_identity(
        None if parent_problem == 'raw' else parent_id,
        executor.state.open_position('orb_test', 1111)['ownership_epoch'])
    for order_id in list(sdk.active_stops):
        sdk.cancel(order_id)
    sdk.accepted.append(dict(
        orderId=1999, orderRef='orb_test|mmr:' + emergency_id,
        clientIntentId=emergency_id, conId=1111, action='SELL', orderType='LMT',
        totalQuantity=40, filled=40, avgFillPrice=100, status='Filled'))
    sdk.broker[1111] = 100
    if parent_problem == 'ambiguous':
        executor.intents.create('orb_test', 1111, 'CLOSE', payload, status='RESOLVED')
        digest = emergency_id.rsplit('-parent-', 1)[1].split('-epoch-', 1)[0]

        class SameLocator:
            def hexdigest(self):
                return digest

        # A locator must resolve uniquely even if saved state is ambiguous.
        with monkeypatch.context() as patch:
            patch.setattr('trader.strategy.auto_executor.hashlib.sha256', lambda value: SameLocator())
            executor._adopt_observed_protectives()
    for _ in range(2):
        executor.manage_positions()
    recovered = next(item for item in executor.intents.all(kind='CLOSE')
                     if item['intent_id'] == emergency_id)
    if parent_problem == 'raw':
        assert recovered['payload']['request_entry_bar_ts'] == timestamp_text(TS)
        assert recovered['payload'].get('explicit_exit_scope') is None
    else:
        assert recovered['payload']['recovery_authority_unknown'] is True
        assert recovered['payload']['exit_request_active'] is False
    assert executor.state.open_position('orb_test', 1111) is None
    addition.update(filled=20, fillQuantityKnown=True)
    sdk.broker[1111] = 120
    for _ in range(2):
        executor.manage_positions()
    assert executor.state.open_position('orb_test', 1111)['quantity'] == 20
    assert sdk.broker[1111] == 120
    assert [row['filled'] for row in sdk.accepted if row['action'] == 'SELL'] == [40]


def test_outer_close_failure_keeps_committed_parent_reference_and_scope(recovery, monkeypatch):
    executor, sdk, path = recovery
    addition = _pending_add(executor, sdk)
    addition.update(status='Cancelled', fillQuantityKnown=False)
    submitted = []
    faults = []

    def unavailable_before_inner_try(intent):
        assert executor.intents.all(kind='CLOSE')[0]['intent_id'] == intent['intent_id']
        raise OSError('reconciliation failed after the close claim committed')

    def emergency(**kwargs):
        submitted.append(kwargs)
        for order_id in list(sdk.active_stops):
            sdk.cancel(order_id)
        sdk.accepted.append(dict(
            orderId=1999, orderRef='orb_test|mmr:' + kwargs['client_intent_id'],
            clientIntentId=kwargs['client_intent_id'], conId=1111, action='SELL',
            orderType='LMT', totalQuantity=40, filled=40, avgFillPrice=100, status='Filled'))
        sdk.broker[1111] -= 40
        return FakeResult(obj=[1999])

    monkeypatch.setattr(sdk, 'emergency_close_position', emergency, raising=False)
    with monkeypatch.context() as patch:
        patch.setattr(executor, '_advance_close', unavailable_before_inner_try)
        patch.setattr('trader.strategy.auto_executor.logging.exception',
                      lambda message, *args, **kwargs: faults.append(message))
        executor._process_signal(make_work(action=Action.SELL,
                                           bar_ts=TS + pd.Timedelta(seconds=20)))
    assert faults == ['auto-executor: durable exit journal unavailable; retaining emergency reduction']
    parent = executor.intents.all(kind='CLOSE')[0]
    assert len(submitted) == 1 and submitted[0]['quantity'] == 40
    assert submitted[0]['client_intent_id'].endswith(
        '-parent-' + hashlib.sha256(parent['intent_id'].encode()).hexdigest()
        + '-epoch-' + executor.state.open_position('orb_test', 1111)['ownership_epoch'])
    restarted = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
    restarted.manage_positions()
    addition.update(filled=20, fillQuantityKnown=True)
    sdk.broker[1111] = 120
    sdk.fill_next = None
    for _ in range(3):
        restarted.manage_positions()
    assert sdk.broker[1111] == 100
    assert restarted.state.open_position('orb_test', 1111) is None
    assert [row['filled'] for row in sdk.accepted if row['action'] == 'SELL'] == [40, 20]
