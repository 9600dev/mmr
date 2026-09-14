"""An attested legacy adoption also adopts the holding's working disaster stop.

Reproduction of the gap: a legacy attributed row (ownership_epoch NULL) with a
working STP tracked via ``protective_order_id``. Observation adopts the stop as
UNKNOWN/attribution_unresolved because its broker claim predates the holding's
origin, and after ``adopt_legacy_holding`` the origin IS the adoption instant,
which a pre-existing stop can never satisfy. Every close/repair path then
refused. The attestation covers the recorded stop; a stop that is not the
recorded identity keeps its refusal.
"""
import datetime as dt
import logging
from types import SimpleNamespace

import pytest

from review.test_review_strategy_contract import FakeResult
from test_intent_support import build_executor, inline_worker

OWNER, CONID, STOP = 'legacy_owner', 4391, 900
ENTRY = dt.datetime(2026, 9, 1, 15, tzinfo=dt.timezone.utc)


class LegacyStopSDK:
    """RPC-style fake: one legacy working stop the previous build placed."""

    def __init__(self, *, stop_ids=(STOP,)):
        self.orders = [dict(orderId=oid, orderRef=OWNER, conId=CONID, status='Submitted', action='SELL',
                            orderType='STP', totalQuantity=10.0, filled=0.0, clientIntentId='')
                       for oid in stop_ids]
        self.position_rows = [dict(conId=CONID, position=10.0, avgCost=25.5)]
        self.cancel_calls, self.propose_calls, self.approve_calls, self.protective_calls = [], [], [], []
        self.next_id = 300
        self.secdef = SimpleNamespace(symbol='LEG', exchange='ASX', primaryExchange='ASX',
                                      currency='AUD', secType='STK', conId=CONID)

    def execution_snapshot(self, intent_id='', order_ids=None):
        return dict(complete=True, orders=[dict(row) for row in self.orders], positions_complete=True,
                    positions=[dict(row) for row in self.position_rows], retry_safe=False)

    def resolve(self, symbol, **kwargs):
        return [self.secdef] if symbol in (CONID, 'LEG') else []

    def propose(self, **kwargs):
        pid = self.next_id
        self.next_id += 1
        self.propose_calls.append(kwargs)
        return pid, None, None

    def approve(self, pid, **kwargs):
        self.approve_calls.append(pid)
        call = self.propose_calls[-1]
        quantity = float(call['quantity'])
        remaining = self.position_rows[0]['position'] - quantity
        self.position_rows = [dict(conId=CONID, position=remaining, avgCost=25.5)] if remaining else []
        self.orders.append(dict(orderId=pid * 10, orderRef=OWNER, conId=CONID, status='Filled', action='SELL',
                                orderType='MKT', totalQuantity=quantity, filled=quantity, avgFillPrice=26.0,
                                clientIntentId=call['metadata']['client_intent_id']))
        return FakeResult(ok=True, obj=[pid * 10])

    def cancel(self, order_id):
        self.cancel_calls.append(order_id)
        for row in self.orders:
            if row['orderId'] == order_id and row['status'] != 'Filled':
                row['status'] = 'Cancelled'
        return FakeResult(ok=True)

    def place_protective_order(self, **kwargs):
        self.protective_calls.append(kwargs)
        oid = self.next_id
        self.next_id += 1
        self.orders.append(dict(orderId=oid, orderRef=OWNER, conId=CONID, status='Submitted', action='SELL',
                                orderType='STP', totalQuantity=kwargs['quantity'], filled=0.0,
                                clientIntentId=kwargs.get('client_intent_id', '')))
        return FakeResult(ok=True, obj=SimpleNamespace(order=SimpleNamespace(orderId=oid)))


def _legacy_holding(tmp_path, monkeypatch, sdk):
    executor, _ = build_executor(tmp_path, sdk, monkeypatch, name='legacy')
    inline_worker(executor)
    executor.state.record_open(OWNER, CONID, 10, ENTRY, 51, None, None)
    executor.state.db.execute(
        'UPDATE auto_exec_positions SET ownership_epoch=NULL, ownership_started_at=NULL '
        'WHERE strategy=? AND conid=?', [OWNER, CONID])
    executor.state.set_protective(OWNER, CONID, STOP)
    executor._load_open_view()
    return executor


def _protective(executor, order_id):
    return next(intent for intent in executor.intents.all(strategy=OWNER, conid=CONID, kind='PROTECTIVE')
                if order_id in intent['payload'].get('order_ids', []))


def test_attested_adoption_makes_the_holding_closable_under_its_live_stop(tmp_path, monkeypatch):
    sdk = LegacyStopSDK()
    executor = _legacy_holding(tmp_path, monkeypatch, sdk)

    executor.manage_positions()  # observation adopts the stop but cannot attribute it
    stop = _protective(executor, STOP)
    assert stop['status'] == 'UNKNOWN' and stop['payload']['attribution_unresolved'] is True

    # Reproduction: the close is refused while the recorded stop is unresolved.
    executor._execute_close(OWNER, CONID, ENTRY + dt.timedelta(minutes=1), 10, 'SELL signal')
    close, = executor.intents.all(kind='CLOSE', active=True)
    assert close['status'] == 'WAITING' and sdk.approve_calls == [] and sdk.cancel_calls == []

    adopted = executor.adopt_legacy_holding(OWNER, CONID)
    assert adopted['adopted_protective_intents'] == [stop['intent_id']]
    stop = executor.intents.get(stop['intent_id'])
    assert stop['status'] == 'WORKING' and stop['payload']['attribution_unresolved'] is False
    assert stop['payload']['ownership_epoch'] == adopted['ownership_epoch']
    assert stop['payload']['ownership_started_at'] == adopted['ownership_started_at']
    assert stop['payload']['adopted_by_attestation'].startswith('legacy-adoption:')

    executor.manage_positions()  # the pending close now proceeds: cancel own stop, sell, fill
    assert sdk.cancel_calls == [STOP]
    assert sdk.approve_calls == [300] and sdk.propose_calls[-1]['quantity'] == 10
    assert executor.state.open_position(OWNER, CONID) is None
    assert executor.intents.get(close['intent_id'])['status'] == 'FILLED'
    assert executor.intents.get(stop['intent_id'])['status'] == 'CANCELLED'


def test_observation_after_attestation_keeps_the_adopted_stop_resolved(tmp_path, monkeypatch):
    sdk = LegacyStopSDK()
    executor = _legacy_holding(tmp_path, monkeypatch, sdk)
    executor.manage_positions()
    executor.adopt_legacy_holding(OWNER, CONID)
    for _ in range(3):
        executor.manage_positions()
        stop = _protective(executor, STOP)
        assert stop['status'] == 'WORKING' and stop['payload']['attribution_unresolved'] is False
    assert sdk.cancel_calls == [] and sdk.protective_calls == [], 'the live stop covers the holding as-is'


def test_a_stop_that_is_not_the_recorded_identity_keeps_its_refusal(tmp_path, monkeypatch):
    other = 901
    sdk = LegacyStopSDK(stop_ids=(STOP, other))
    executor = _legacy_holding(tmp_path, monkeypatch, sdk)
    executor.manage_positions()
    assert _protective(executor, other)['payload']['attribution_unresolved'] is True

    adopted = executor.adopt_legacy_holding(OWNER, CONID)
    assert adopted['adopted_protective_intents'] == [_protective(executor, STOP)['intent_id']]
    assert _protective(executor, STOP)['payload']['attribution_unresolved'] is False
    unresolved = _protective(executor, other)
    assert unresolved['status'] == 'UNKNOWN' and unresolved['payload']['attribution_unresolved'] is True
    assert unresolved['payload'].get('ownership_epoch') is None

    executor._execute_close(OWNER, CONID, ENTRY + dt.timedelta(minutes=1), 10, 'SELL signal')
    executor.manage_positions()
    assert sdk.approve_calls == [] and sdk.cancel_calls == [], \
        'an unresolved stop of unknown ownership still reserves the holding'
    assert executor.state.open_position(OWNER, CONID)['quantity'] == 10


def test_an_unobserved_tracked_stop_changes_no_attribution(tmp_path, monkeypatch, caplog):
    sdk = LegacyStopSDK(stop_ids=())
    executor = _legacy_holding(tmp_path, monkeypatch, sdk)
    executor.manage_positions()
    with caplog.at_level(logging.WARNING, logger='auto_executor'):
        adopted = executor.adopt_legacy_holding(OWNER, CONID)
    assert 'adopted_protective_intents' not in adopted
    assert any(f'tracked protective {STOP} is not observed' in record.getMessage() for record in caplog.records)
    assert executor.state.open_position(OWNER, CONID)['ownership_epoch'] == adopted['ownership_epoch']
