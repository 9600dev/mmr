"""Cancellation coordination needs positive, scoped evidence of a completed BUY."""
from types import SimpleNamespace

import pandas as pd
import pytest

from trader.strategy.auto_executor import AutoExecutor


INTENT_ID = 'test-terminal-opening'


def opening(*, order_ids=(17,), conid=1111):
    return dict(intent_id=INTENT_ID, conid=conid,
                payload=dict(order_ids=list(order_ids)))


def order(**overrides):
    return dict(dict(orderId=17, conId=1111, action='BUY',
                     clientIntentId=INTENT_ID, status='Unknown',
                     brokerStatus='Filled', filled=40, fillQuantityKnown=False),
                **overrides)


def reader(snapshot):
    # This query is deliberately exercised without journal/proposal setup:
    # its contract is entirely the broker snapshot and durable order identity.
    executor = object.__new__(AutoExecutor)
    executor._snapshot_cache = None
    executor._sdk = SimpleNamespace(execution_snapshot=lambda **kwargs: snapshot)
    return executor


@pytest.mark.parametrize('terminal', ['Filled', 'Cancelled', 'ApiCancelled', 'Inactive'])
@pytest.mark.parametrize('container', [list, pd.DataFrame])
@pytest.mark.parametrize('source', ['brokerStatus', 'status'])
def test_completed_buy_recognition_accepts_supported_status_and_snapshot_forms(
        terminal, container, source):
    row = order(**{source: terminal})
    if source == 'status':
        row.pop('brokerStatus')
    executor = reader(dict(complete=True, orders=container([row])))
    assert executor._opening_is_broker_terminal(opening()) is True


@pytest.mark.parametrize('complete', [False, None])
def test_incomplete_or_unlabelled_snapshot_cannot_bypass_buy_cancellation(complete):
    snapshot = dict(orders=[order()])
    if complete is not None:
        snapshot['complete'] = complete
    assert reader(snapshot)._opening_is_broker_terminal(opening()) is False


@pytest.mark.parametrize('unrelated', [
    {'clientIntentId': 'another-intent'},
    {'conId': 2222},
    {'action': 'SELL'},
    {'clientIntentId': '', 'orderId': 18},
    {'action': None},
    {'conId': None},
])
def test_unrelated_terminal_order_cannot_authorize_owned_exit(unrelated):
    snapshot = dict(complete=True, orders=[order(**unrelated)])
    assert reader(snapshot)._opening_is_broker_terminal(opening()) is False


@pytest.mark.parametrize('unrelated', [
    {'clientIntentId': 'another-intent'},
    {'conId': 2222},
    {'action': 'SELL'},
])
def test_unrelated_working_order_cannot_strand_confirmed_owned_exit(unrelated):
    other = order(**dict(unrelated, brokerStatus='Submitted', status='Submitted'))
    snapshot = dict(complete=True, orders=[order(), other])
    assert reader(snapshot)._opening_is_broker_terminal(opening()) is True


def test_every_matched_buy_must_be_completed_before_cancellation_is_skipped():
    working = order(orderId=18, brokerStatus='PendingCancel', status='PendingCancel')
    snapshot = dict(complete=True, orders=[order(), working])
    assert reader(snapshot)._opening_is_broker_terminal(opening(order_ids=(17, 18))) is False


def test_numeric_order_identity_fallback_requires_the_known_scoped_id():
    snapshot = dict(complete=True, orders=[order(clientIntentId='')])
    assert reader(snapshot)._opening_is_broker_terminal(opening()) is True
    assert reader(snapshot)._opening_is_broker_terminal(opening(order_ids=(18,))) is False


@pytest.mark.parametrize('snapshot', [dict(complete=True), dict(complete=True, orders=[])])
def test_absence_of_orders_is_not_evidence_of_buy_completion(snapshot):
    assert reader(snapshot)._opening_is_broker_terminal(opening()) is False


def test_missing_contract_id_cannot_match_an_integer_contract_identity():
    row = order()
    row.pop('conId')
    snapshot = dict(complete=True, orders=[row, order(conId=0)])
    assert reader(snapshot)._opening_is_broker_terminal(opening(conid=1)) is False


def test_missing_numeric_identity_cannot_match_an_unreferenced_order():
    intent = opening()
    intent['payload'].pop('order_ids')
    snapshot = dict(complete=True, orders=[order(clientIntentId='')])
    assert reader(snapshot)._opening_is_broker_terminal(intent) is False


def test_unknown_numeric_id_uses_intent_scoped_broker_lookup():
    queries = []

    def snapshot(**kwargs):
        queries.append(kwargs)
        rows = [order(orderId=0)] if kwargs.get('intent_id') == INTENT_ID else []
        return dict(complete=True, orders=rows)

    executor = reader(None)
    executor._sdk.execution_snapshot = snapshot
    assert executor._opening_is_broker_terminal(opening(order_ids=())) is True
    assert queries == [dict(intent_id=INTENT_ID, order_ids=None)]


def test_missing_broker_status_cell_uses_the_observed_status_in_dataframe_adapter():
    rows = pd.DataFrame([
        order(orderId=18, clientIntentId='another-intent'),
        dict(orderId=17, conId=1111, action='BUY', clientIntentId=INTENT_ID,
             status='Cancelled', filled=40, fillQuantityKnown=False),
    ])
    snapshot = dict(complete=True, orders=rows)
    assert reader(snapshot)._opening_is_broker_terminal(opening()) is True


def test_completed_buy_without_numeric_id_cannot_strand_confirmed_owned_close(
        tmp_path, monkeypatch):
    from review.test_review_strategy_contract import LifecycleSDK, FakeResult, TS, make_work
    from trader.objects import Action

    monkeypatch.delenv('MMR_AUTO_EXECUTE_DISABLED', raising=False)
    monkeypatch.setenv('MMR_PROTECTIVE_STOP_PCT', '8')
    monkeypatch.setattr(AutoExecutor, '_now_utc', lambda self:
                        (TS.tz_localize('UTC') + pd.Timedelta(seconds=70)).to_pydatetime())
    sdk = LifecycleSDK()
    executor = AutoExecutor(str(tmp_path / 'state.duckdb'), paper_trading=True,
                            sdk_factory=lambda: sdk)
    sdk.broker[1111] = 100  # unrelated manual inventory
    sdk.fill_next = 40
    executor._process_signal(make_work(quantity=140))
    intent = executor.intents.all(kind='OPEN')[0]
    entry_id = sdk.accepted[0]['orderId']
    sdk.accepted[0].update(status='Cancelled', brokerStatus=float('nan'),
                           fillQuantityKnown=False, orderId=0)
    executor.intents.update(intent, order_ids=[])
    cancel = sdk.cancel
    monkeypatch.setattr(sdk, 'cancel', lambda oid:
                        FakeResult(ok=False, error='completed order has no numeric cancellation target')
                        if oid in (0, entry_id) else cancel(oid))
    sdk.fill_next = None
    executor._process_signal(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))
    assert sdk.propose_calls[-1]['action'] == 'SELL'
    assert sdk.propose_calls[-1]['quantity'] == 40
    assert sdk.broker[1111] == 100
    assert executor.state.open_position('orb_test', 1111) is None
    assert executor.intents.all(kind='OPEN', active=True)[0]['status'] == 'UNKNOWN'
