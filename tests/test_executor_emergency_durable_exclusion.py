"""The emergency and durable close paths never both submit for one holding.

Before this fix an exception inside ``_advance_close`` remembered an emergency
exit while the CLOSE stayed WAITING; on the next cycle ``_retry_emergency_exits``
sent the emergency reduction AND ``_advance_close`` submitted the durable close.
The durable path now waits while an emergency ATTEMPT is unreconciled (bounded
by EMERGENCY_HANDOFF_SECONDS) or another active close already holds broker
orders, and a never-attempted emergency entry is released once the holding is
durably gone, so the key does not stay reserved against new opens.
"""
import logging

import pandas as pd
import pytest

from review.test_review_strategy_contract import FakeResult, TS, make_work
from test_intent_support import CONID, OWNER, SnapshotSDK, build_executor, clock, open_owned  # noqa: F401
from trader.objects import Action
from trader.strategy import auto_executor as auto_executor_module


def _accepting_endpoint(sdk, calls, *, fill=False, ok=True):
    """The trader accepts the reduction; whether it fills is the test's choice."""
    def submit(**kwargs):
        calls.append(kwargs)
        if not ok:
            return FakeResult(ok=False, error='UNKNOWN: acknowledgement lost')
        for oid in list(sdk.active_stops):
            sdk.cancelled_stops[oid] = sdk.active_stops.pop(oid)  # the coordinator displaced own protection
        row = dict(orderId=8800 + len(calls), orderRef=OWNER, clientIntentId=kwargs['client_intent_id'],
                   conId=CONID, action='SELL', orderType='MKT', status='Submitted',
                   totalQuantity=kwargs['quantity'], filled=0.0)
        if fill:
            row.update(status='Filled', filled=kwargs['quantity'], avgFillPrice=99.0)
            sdk.broker[CONID] -= kwargs['quantity']
        sdk.accepted.append(row)
        return FakeResult(ok=True, obj=[row['orderId']])
    return submit


def _fail_positions_once(executor, monkeypatch):
    original = executor._position_snapshot
    state = dict(fail=True)

    def flaky():
        if state['fail']:
            state['fail'] = False
            raise RuntimeError('positions read failed mid-close')
        return original()
    monkeypatch.setattr(executor, '_position_snapshot', flaky)


def _sell(executor):
    executor._process_signal(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))


def test_durable_close_waits_for_an_attempted_emergency_until_it_is_reconciled(tmp_path, monkeypatch, caplog):
    sdk = SnapshotSDK()
    executor, _ = build_executor(tmp_path, sdk, monkeypatch)
    open_owned(executor)
    calls = []
    monkeypatch.setattr(sdk, 'emergency_close_position', _accepting_endpoint(sdk, calls), raising=False)
    _fail_positions_once(executor, monkeypatch)

    _sell(executor)  # the durable path faults, the emergency attempt is sent and accepted
    assert len(calls) == 1 and calls[0]['quantity'] == 140
    parent, = executor.intents.all(kind='CLOSE')
    assert parent['status'] == 'WAITING' and sdk.approve_calls == [100]

    with caplog.at_level(logging.WARNING, logger='auto_executor'):
        executor.manage_positions()
    assert sdk.approve_calls == [100], 'the durable close must not submit a second reduction'
    assert len(calls) == 1
    restored = next(intent for intent in executor.intents.all(kind='CLOSE')
                    if intent['intent_id'] != parent['intent_id'])
    assert restored['status'] == 'WORKING' and restored['payload']['emergency'] is True
    assert any('waits:' in record.getMessage() and parent['intent_id'] in record.getMessage()
               for record in caplog.records)

    sdk.accepted[-1].update(status='Filled', filled=140.0, avgFillPrice=99.0)
    sdk.broker[CONID] = 0
    executor.manage_positions()
    assert executor.state.open_position(OWNER, CONID) is None
    assert executor._emergency_exits == {}
    assert {intent['status'] for intent in executor.intents.all(kind='CLOSE')} == {'FILLED', 'RESOLVED'}
    assert sdk.approve_calls == [100] and len(calls) == 1


def test_durable_close_resumes_after_the_handoff_window_when_the_attempt_has_no_evidence(
        tmp_path, monkeypatch, clock):
    sdk = SnapshotSDK()
    executor, _ = build_executor(tmp_path, sdk, monkeypatch)
    open_owned(executor)
    calls = []
    monkeypatch.setattr(sdk, 'emergency_close_position', _accepting_endpoint(sdk, calls, ok=False), raising=False)
    _fail_positions_once(executor, monkeypatch)

    _sell(executor)
    assert len(calls) == 1
    emergency, = executor._emergency_exits.values()
    assert emergency['attempted'] is True and emergency['attempted_at'] == clock.now

    executor.manage_positions()
    assert sdk.approve_calls == [100], 'inside the handoff window the durable path waits'

    clock.advance(auto_executor_module.EMERGENCY_HANDOFF_SECONDS + 1)
    executor.manage_positions()
    assert sdk.approve_calls == [100, 101], 'the trader ledger arbitrates after the window'
    assert executor.state.open_position(OWNER, CONID) is None
    assert len(calls) == 1


def test_emergency_does_not_send_while_a_durable_close_is_unresolved_and_is_released_when_it_wins(
        tmp_path, monkeypatch):
    sdk = SnapshotSDK()
    executor, _ = build_executor(tmp_path, sdk, monkeypatch)
    open_owned(executor)
    original = sdk.approve

    def approve(pid, **kwargs):
        if sdk.propose_calls[-1]['action'] == 'SELL' and not kwargs.get('resume'):
            sdk.approve_calls.append(pid)
            return FakeResult(ok=False, error='UNKNOWN: order submission timed out; may be live')
        return original(pid)

    monkeypatch.setattr(sdk, 'approve', approve)
    _sell(executor)
    close, = executor.intents.all(kind='CLOSE')
    assert close['status'] == 'UNKNOWN' and close['payload']['proposal_id'] == 101

    calls = []
    monkeypatch.setattr(sdk, 'emergency_close_position', _accepting_endpoint(sdk, calls, fill=True), raising=False)
    executor._remember_emergency_exit(OWNER, CONID, TS + pd.Timedelta(minutes=2), 'degraded time exit',
                                      entry_bar_ts=TS)
    executor._retry_emergency_exits()
    assert calls == [], 'an unresolved durable close reserves the reduction'
    assert (OWNER, CONID) in executor._emergency_exits

    sdk.retry_safe = True  # the trader proves the earlier attempt never sent; the durable close resumes
    executor.manage_positions()
    assert executor.intents.get(close['intent_id'])['status'] == 'FILLED'
    assert executor.state.open_position(OWNER, CONID) is None
    assert calls == []
    executor.manage_positions()  # the emergency retry runs first in a cycle; the next pass sees the holding gone
    assert executor._emergency_exits == {}, 'a never-attempted request has nothing left to reduce'

    executor.cooldown_seconds = 0
    executor._process_signal(make_work(quantity=140, bar_ts=TS + pd.Timedelta(minutes=3)))
    assert executor.state.open_position(OWNER, CONID)['quantity'] == 140, 'the key is not reserved by a ghost'
