"""A trader DEFERRED verdict keeps ONE close pending and retries it with backoff.

The verdict arrives either as a failed result ``DEFERRED: ...`` or behind the
server wrapper ``REJECTED: intent <id>: DEFERRED: ...``. Before this fix the
first was classified UNKNOWN (never retry_safe, exit hung) and the second
REJECTED (a fresh CLOSE minted every management cycle: a 1 Hz propose/approve
storm). Both now defer the same request, log the reason once per change, retry
on DEFERRED_RETRY_SECONDS only once the named foreign orders stop working, and
leave protective repair enabled meanwhile. When the foreign reservation is
already visible the executor predicts the verdict BEFORE cancelling its own
stop, so the holding never sits protected only by the foreign order.
"""
import logging

import pandas as pd
import pytest

from review.test_review_strategy_contract import FakeResult, TS, make_work
from test_intent_support import CONID, OWNER, SnapshotSDK, build_executor, clock, open_owned  # noqa: F401
from trader.objects import Action
from trader.strategy import auto_executor as auto_executor_module
from trader.strategy.auto_executor import AutoExecutor

FOREIGN = 555
VERDICT = ('DEFERRED: SELL 140 not placed: all 140 held are reserved by working reductions owned '
           f'elsewhere [{FOREIGN} (owner proposal)]; that owner must retire them (or cancel them '
           'explicitly) before this close')


def _foreign_reduction(sdk, quantity):
    row = dict(orderId=FOREIGN, orderRef='proposal', conId=CONID, status='Submitted', action='SELL',
               orderType='STP', totalQuantity=float(quantity), filled=0.0, clientIntentId='proposal:12')
    sdk.accepted.append(row)
    return row


def _deferring_approve(sdk, monkeypatch, wrap):
    original = sdk.approve
    state = dict(defer=True)

    def approve(pid, **kwargs):
        if sdk.propose_calls[-1]['action'] == 'SELL' and state['defer']:
            sdk.approve_calls.append(pid)
            verdict = VERDICT
            if wrap:
                verdict = f"REJECTED: intent {sdk.proposals[pid].metadata['client_intent_id']}: {VERDICT}"
            return FakeResult(ok=False, error=verdict)
        return original(pid)

    monkeypatch.setattr(sdk, 'approve', approve)
    return state


def _deferred_warnings(caplog):
    return [record.getMessage() for record in caplog.records
            if record.levelno >= logging.WARNING and 'DEFERRED by the trader' in record.getMessage()]


@pytest.mark.parametrize('wrapped', [False, True], ids=['bare_result', 'server_wrapper'])
def test_deferred_close_stays_one_request_and_retries_once_the_foreign_order_is_gone(
        tmp_path, monkeypatch, caplog, clock, wrapped):
    sdk = SnapshotSDK()
    executor, _ = build_executor(tmp_path, sdk, monkeypatch)
    open_owned(executor)
    own_stop = executor.state.open_position(OWNER, CONID)['protective_order_id']
    foreign = _foreign_reduction(sdk, 100)  # a partial reservation: the trader, not the executor, decides
    state = _deferring_approve(sdk, monkeypatch, wrapped)

    with caplog.at_level(logging.WARNING, logger='auto_executor'):
        executor._process_signal(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))
    close, = executor.intents.all(kind='CLOSE')
    payload = close['payload']
    assert close['status'] == 'WAITING'
    assert payload['deferred_reason'].startswith('DEFERRED:') and payload['deferred_count'] == 1
    assert payload['proposal_id'] is None and payload['deferred_proposal_ids'] == [101]
    assert sdk.approve_calls == [100, 101]
    assert own_stop not in sdk.active_stops, 'the own stop had been cancelled before the verdict arrived'
    assert len(_deferred_warnings(caplog)) == 1 and 'mmr cancel' in _deferred_warnings(caplog)[0]

    with caplog.at_level(logging.WARNING, logger='auto_executor'):
        executor.manage_positions()  # inside the backoff: no storm, protection repaired
    assert len(executor.intents.all(kind='CLOSE')) == 1 and sdk.approve_calls == [100, 101]
    assert len(sdk.active_stops) == 1, 'a deferred close does not block protective repair'
    repaired, = sdk.active_stops
    assert len(_deferred_warnings(caplog)) == 1, 'the same reason is not logged again'

    clock.advance(auto_executor_module.DEFERRED_RETRY_SECONDS[0] + 1)
    executor.manage_positions()  # due, but the named foreign order is still working: wait
    assert sdk.approve_calls == [100, 101] and repaired in sdk.active_stops

    foreign['status'] = 'Cancelled'
    state['defer'] = False
    executor.manage_positions()
    assert sdk.approve_calls == [100, 101, 102]
    assert [call['action'] for call in sdk.propose_calls] == ['BUY', 'SELL', 'SELL']
    final, = executor.intents.all(kind='CLOSE')
    assert final['intent_id'] == close['intent_id'] and final['status'] == 'FILLED'
    assert final['payload']['deferred_at'] is None and final['payload']['deferred_count'] == 1
    assert executor.state.open_position(OWNER, CONID) is None and sdk.broker[CONID] == 0
    assert not sdk.active_stops


def test_a_visible_foreign_reservation_defers_before_cancelling_own_protection(tmp_path, monkeypatch, clock):
    sdk = SnapshotSDK()
    executor, _ = build_executor(tmp_path, sdk, monkeypatch)
    open_owned(executor)
    own_stop = executor.state.open_position(OWNER, CONID)['protective_order_id']
    foreign = _foreign_reduction(sdk, 140)  # the whole position: the trader would certainly defer

    executor._process_signal(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))
    close, = executor.intents.all(kind='CLOSE')
    assert close['status'] == 'WAITING' and 'predicted' in close['payload']['deferred_reason']
    assert f'{FOREIGN} (owner proposal)' in close['payload']['deferred_reason']
    assert sdk.approve_calls == [100], 'nothing was proposed or approved'
    assert own_stop in sdk.active_stops and sdk.cancel_calls == [], 'own protection was left in place'

    clock.advance(auto_executor_module.DEFERRED_RETRY_SECONDS[0] + 1)
    executor.manage_positions()
    assert sdk.approve_calls == [100] and own_stop in sdk.active_stops

    foreign['status'] = 'Cancelled'
    executor.manage_positions()
    assert sdk.approve_calls == [100, 101]
    assert executor.state.open_position(OWNER, CONID) is None
    assert len(executor.intents.all(kind='CLOSE')) == 1


def test_a_fresh_explicit_sell_retries_without_waiting_out_the_backoff(tmp_path, monkeypatch, clock):
    sdk = SnapshotSDK()
    executor, _ = build_executor(tmp_path, sdk, monkeypatch)
    open_owned(executor)
    foreign = _foreign_reduction(sdk, 100)
    state = _deferring_approve(sdk, monkeypatch, wrap=False)
    executor._process_signal(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))
    assert sdk.approve_calls == [100, 101]

    foreign['status'] = 'Cancelled'
    state['defer'] = False
    executor._process_signal(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=2)))
    assert sdk.approve_calls == [100, 101, 102]
    assert executor.state.open_position(OWNER, CONID) is None
    assert len(executor.intents.all(kind='CLOSE')) == 1


def test_deferral_is_logged_once_per_distinct_reason(tmp_path, monkeypatch, caplog, clock):
    sdk = SnapshotSDK()
    executor, _ = build_executor(tmp_path, sdk, monkeypatch)
    open_owned(executor)
    _foreign_reduction(sdk, 100)
    _deferring_approve(sdk, monkeypatch, wrap=False)
    executor._process_signal(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))
    close, = executor.intents.all(kind='CLOSE')
    with caplog.at_level(logging.WARNING, logger='auto_executor'):
        executor._defer_close(close, VERDICT)
        executor._defer_close(close, VERDICT)
        executor._defer_close(close, VERDICT.replace(str(FOREIGN), '556'))
    assert len(_deferred_warnings(caplog)) == 2
    assert executor.intents.get(close['intent_id'])['payload']['deferred_count'] == 4


@pytest.mark.parametrize('message, expected', [
    ('DEFERRED: all held are reserved [1 (owner x)]', 'DEFERRED: all held are reserved [1 (owner x)]'),
    ('REJECTED: intent auto-1: DEFERRED: reserved', 'DEFERRED: reserved'),
    ('UNKNOWN: proposal #3: transport lost; DEFERRED: not really', None),
    ('Risk gate: refused', None),
    ('', None),
])
def test_deferral_text_recognises_both_arrival_forms_but_never_a_lost_reply(message, expected):
    assert AutoExecutor._deferral_text(message) == expected
