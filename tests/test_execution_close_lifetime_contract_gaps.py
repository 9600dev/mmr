"""Durable exit-request handoffs, independent of a new broker submission.

A paused _advance_close is a crash/worker-boundary seam: the assertions observe
committed request state before the already-tested execution coordinator runs.
All ownership and intent records use the real temporary stores.
"""
from copy import deepcopy

import pandas as pd
import pytest

from review.test_review_strategy_contract import TS
from test_execution_recovery import recovery
from trader.strategy.auto_executor import AutoExecutor, AutoExecutionError
from trader.strategy.execution_intents import timestamp_text


OWNER = 'orb_test'
CONID = 1111
OLD = TS - pd.Timedelta(minutes=1)
DUE = TS + pd.Timedelta(minutes=1)
_MISSING = object()


def _owned(recovery):
    executor, sdk, path = recovery
    executor.state.record_open(OWNER, CONID, 40.0, TS, 77, None, 3)
    sdk.broker[CONID] = 140.0  # the other 100 are manual inventory
    executor._load_open_view()
    return executor, sdk, path


def _terminal(executor, *, status='CANCELLED', active=True, policy=TS,
              request_entry=None, scope=None, reason='max_hold_bars=3', successor=None):
    position = executor.state.open_position(OWNER, CONID)
    payload = dict(bar_ts=timestamp_text(DUE), quantity=140.0,
                   policy_entry_bar_ts=timestamp_text(policy) if policy is not None else None,
                   request_entry_bar_ts=timestamp_text(request_entry) if request_entry is not None else None,
                   order_ids=[701], cumulative_filled=0.0,
                   ownership_epoch=position['ownership_epoch'],
                   ownership_started_at=position['ownership_started_at'])
    if active is not _MISSING:
        payload['exit_request_active'] = active
    if reason is not _MISSING:
        payload['reason'] = reason
    if scope is not None:
        payload['explicit_exit_scope'] = deepcopy(scope)
    if successor is not None:
        payload['successor_exit'] = deepcopy(successor)
    return executor.intents.create(OWNER, CONID, 'CLOSE', payload, status=status)


def _request(*, entry=TS, policy=TS):
    return dict(request_id='retained-current-request', bar_ts=timestamp_text(DUE),
                reason='max_hold_bars=3', entry_bar_ts=timestamp_text(entry),
                policy_entry_bar_ts=timestamp_text(policy) if policy is not None else None)


def _pause_submission(executor, monkeypatch):
    advanced = []
    monkeypatch.setattr(executor, '_advance_close', lambda intent: advanced.append(deepcopy(intent)))
    return advanced


def _read(executor, intent):
    return next(row for row in executor.intents.all(kind='CLOSE')
                if row['intent_id'] == intent['intent_id'])


def _children(executor, parent):
    return [row for row in executor.intents.all(strategy=OWNER, conid=CONID, kind='CLOSE')
            if row['intent_id'] != parent['intent_id']]


@pytest.mark.parametrize('terminal', ['CANCELLED', 'REJECTED'])
def test_known_terminal_legacy_request_default_retains_its_residual(recovery, monkeypatch, terminal):
    executor, _sdk, path = _owned(recovery)
    # Absence of the older request-active flag is isolated here. The physical
    # holding binding is known; an unbound legacy reducer is separately UNKNOWN
    # under current reconciliation and is not made executable by this fixture.
    parent = _terminal(executor, status=terminal, active=_MISSING)
    advanced = _pause_submission(executor, monkeypatch)
    executor._finish_close_request(parent)
    child, = _children(executor, parent)
    assert executor._unsubmitted_close(child)
    assert len(advanced) == 1
    assert _read(executor, parent)['payload']['exit_request_active'] is False
    restarted = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: _sdk)
    assert len(_children(restarted, parent)) == 1
    restarted.manage_positions()
    assert _sdk.propose_calls[-1]['quantity'] == 40.0
    assert _sdk.broker[CONID] == 100.0
    assert restarted.state.open_position(OWNER, CONID) is None


@pytest.mark.parametrize('terminal', ['FILLED', 'RESOLVED'])
def test_historical_completed_request_without_active_flag_cannot_revive(recovery, monkeypatch, terminal):
    executor, _sdk, _ = _owned(recovery)
    parent = _terminal(executor, status=terminal, active=_MISSING, policy=None)
    advanced = _pause_submission(executor, monkeypatch)
    executor._finish_close_request(parent)
    assert advanced == []
    assert _children(executor, parent) == []
    assert executor.state.open_position(OWNER, CONID)['quantity'] == 40.0


def test_residual_request_keeps_actual_reason_resolved_contract_and_unique_identity(recovery, monkeypatch):
    executor, _sdk, path = _owned(recovery)
    parent = _terminal(executor, reason='max_hold_bars=3')
    _pause_submission(executor, monkeypatch)
    executor._finish_close_request(parent)
    child, = _children(executor, parent)
    payload = child['payload']
    assert payload['reason'] == 'max_hold_bars=3'
    assert isinstance(payload['exit_request_id'], str) and payload['exit_request_id']
    assert payload['exit_request_id'] != parent['payload'].get('exit_request_id')
    assert payload['ident'] == dict(symbol='WDS', exchange='ASX', currency='AUD', sec_type='STK')
    assert payload['policy_entry_bar_ts'] == timestamp_text(TS)
    assert executor._unsubmitted_close(child)
    # Request completion must not rewrite broker status, scoped IDs or epoch.
    saved = _read(executor, parent)
    assert saved['status'] == 'CANCELLED'
    assert saved['payload']['order_ids'] == [701]
    assert saved['payload']['ownership_epoch'] == parent['payload']['ownership_epoch']
    assert saved['payload']['exit_request_active'] is False
    restarted = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: _sdk)
    restored, = _children(restarted, parent)
    assert restored['payload']['exit_request_id'] == payload['exit_request_id']


def test_missing_legacy_reason_still_describes_an_exit(recovery, monkeypatch):
    executor, _sdk, _ = _owned(recovery)
    parent = _terminal(executor, reason=_MISSING)
    _pause_submission(executor, monkeypatch)
    executor._finish_close_request(parent)
    child, = _children(executor, parent)
    reason = child['payload']['reason']
    assert isinstance(reason, str) and 'exit' in reason.casefold()


@pytest.mark.parametrize('flat', [True, False])
def test_fulfilled_or_obsolete_successor_is_consumed_without_changing_broker_history(recovery, monkeypatch, flat):
    executor, _sdk, _ = _owned(recovery)
    request = _request(entry=TS if flat else OLD, policy=TS if flat else None)
    parent = _terminal(executor, successor=request, policy=TS if flat else OLD)
    if flat:
        executor.state.record_close(OWNER, CONID, 'CLOSED', 'independently confirmed flat')
    advanced = _pause_submission(executor, monkeypatch)
    for _ in range(2):
        executor._finish_close_request(parent)
    saved = _read(executor, parent)
    assert saved['status'] == 'CANCELLED'
    assert saved['payload']['order_ids'] == [701]
    assert saved['payload']['exit_request_active'] is False
    assert saved['payload']['successor_exit'] is None
    assert _children(executor, parent) == []
    assert advanced == []


def test_transferred_successor_is_consumed_once_and_preserves_its_bound_reason(recovery, monkeypatch):
    executor, _sdk, path = _owned(recovery)
    request = _request()
    parent = _terminal(executor, successor=request, reason='older cancelled attempt')
    advanced = _pause_submission(executor, monkeypatch)
    executor._finish_close_request(parent)
    child, = _children(executor, parent)
    assert child['payload']['exit_request_id'] == request['request_id']
    assert child['payload']['reason'] == 'max_hold_bars=3'
    assert child['payload']['request_entry_bar_ts'] == timestamp_text(TS)
    assert child['payload']['policy_entry_bar_ts'] == timestamp_text(TS)
    assert executor._unsubmitted_close(child)
    assert _read(executor, parent)['payload']['successor_exit'] is None
    assert _read(executor, parent)['payload']['exit_request_active'] is False
    executor._finish_close_request(parent)
    assert len(advanced) == 1
    restarted = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: _sdk)
    assert _read(restarted, parent)['payload']['successor_exit'] is None


@pytest.mark.parametrize('unrelated_strategy,unrelated_conid', [('other-owner', CONID), (OWNER, 2222)])
def test_an_unrelated_active_close_does_not_block_this_owners_residual(
        recovery, monkeypatch, unrelated_strategy, unrelated_conid):
    executor, _sdk, _ = _owned(recovery)
    parent = _terminal(executor)
    unrelated = executor.intents.create(unrelated_strategy, unrelated_conid, 'CLOSE',
                                        dict(bar_ts=timestamp_text(DUE), quantity=15.0), status='WAITING')
    advanced = _pause_submission(executor, monkeypatch)
    executor._finish_close_request(parent)
    child, = _children(executor, parent)
    assert (child['strategy'], child['conid']) == (OWNER, CONID)
    assert len(advanced) == 1
    assert _read(executor, unrelated)['status'] == 'WAITING'


def test_already_claimed_successor_keeps_parent_until_that_attempt_is_managed(recovery, monkeypatch):
    executor, _sdk, _ = _owned(recovery)
    request = _request()
    parent = _terminal(executor, successor=request)
    # Crash after child creation but before compare-and-consume is a supported
    # single-worker state, without introducing overlapping executors.
    claimed = executor.intents.create(OWNER, CONID, 'CLOSE',
        dict(bar_ts=request['bar_ts'], quantity=40.0, reason=request['reason'],
             request_entry_bar_ts=request['entry_bar_ts'],
             policy_entry_bar_ts=request['policy_entry_bar_ts'],
             exit_request_id=request['request_id'], exit_request_active=True), status='WAITING')
    advanced = _pause_submission(executor, monkeypatch)
    executor._finish_close_request(parent)
    assert advanced == []
    assert [row['intent_id'] for row in _children(executor, parent)] == [claimed['intent_id']]
    assert _read(executor, parent)['payload']['successor_exit']['request_id'] == request['request_id']
    assert _read(executor, parent)['payload']['exit_request_active'] is True


@pytest.mark.parametrize('request_entry,policy', [(OLD, None), (None, OLD)])
def test_obsolete_binding_does_not_create_another_durable_attempt(recovery, monkeypatch, request_entry, policy):
    executor, _sdk, _ = _owned(recovery)
    parent = _terminal(executor, request_entry=request_entry, policy=policy)
    advanced = _pause_submission(executor, monkeypatch)
    executor._finish_close_request(parent)
    assert _children(executor, parent) == []
    assert advanced == []
    assert _read(executor, parent)['payload']['exit_request_active'] is False
    assert executor.state.open_position(OWNER, CONID)['quantity'] == 40.0


@pytest.mark.parametrize('captured_pending', [False, True])
def test_unmatched_scope_waits_only_for_its_exact_captured_opening(recovery, monkeypatch, captured_pending):
    executor, _sdk, _ = _owned(recovery)
    captured = executor.intents.create(OWNER, CONID, 'OPEN',
        dict(bar_ts=timestamp_text(DUE), quantity=20.0, proposal_id=88),
        status='UNKNOWN' if captured_pending else 'REJECTED')
    scope = dict(positions=[dict(entry_bar_ts=timestamp_text(OLD), proposal_id=66)],
                 openings={captured['intent_id']: dict(entry_bar_ts=timestamp_text(DUE), proposal_id=88)})
    parent = _terminal(executor, policy=None, scope=scope)
    advanced = _pause_submission(executor, monkeypatch)
    executor._finish_close_request(parent)
    assert _children(executor, parent) == []
    assert advanced == []
    assert _read(executor, parent)['payload']['exit_request_active'] is captured_pending


@pytest.mark.parametrize('current_timer', [False, True])
def test_independent_current_timer_can_authorize_its_own_entry_outside_old_explicit_scope(
        recovery, monkeypatch, current_timer):
    executor, _sdk, _ = _owned(recovery)
    scope = dict(positions=[dict(entry_bar_ts=timestamp_text(OLD), proposal_id=66)], openings={})
    request = _request(policy=None) if current_timer else None
    parent = _terminal(executor, policy=None, scope=scope, successor=request)
    advanced = _pause_submission(executor, monkeypatch)
    executor._finish_close_request(parent)
    children = _children(executor, parent)
    assert len(children) == int(current_timer)
    assert len(advanced) == int(current_timer)
    if current_timer:
        assert children[0]['payload']['request_entry_bar_ts'] == timestamp_text(TS)
        assert children[0]['payload']['explicit_exit_scope'] == scope


def test_resolution_failure_leaves_original_demand_unconsumed(recovery, monkeypatch):
    executor, sdk, _ = _owned(recovery)
    parent = _terminal(executor)
    advanced = _pause_submission(executor, monkeypatch)
    monkeypatch.setattr(sdk, 'resolve', lambda *args, **kwargs: [])
    with pytest.raises(AutoExecutionError):
        executor._finish_close_request(parent)
    assert _children(executor, parent) == []
    assert advanced == []
    assert _read(executor, parent)['payload']['exit_request_active'] is True


def test_policy_only_timer_keeps_latest_buy_wins_after_handoff(recovery, monkeypatch):
    executor, sdk, _ = _owned(recovery)
    parent = _terminal(executor, policy=TS, request_entry=None)
    advance = executor._advance_close
    _pause_submission(executor, monkeypatch)
    executor._finish_close_request(parent)
    child, = _children(executor, parent)
    # A pending add can commit while a claimed child waits for management.
    # The real atomic owner row now reflects its later BUY bar and quantity.
    executor.state.record_add(OWNER, CONID, 20.0, DUE, 88, None, 3)
    sdk.broker[CONID] = 160.0
    executor._load_open_view()
    advance(child)
    assert sdk.propose_calls == []
    assert executor.state.open_position(OWNER, CONID)['quantity'] == 60.0
    assert sdk.broker[CONID] == 160.0
    assert _read(executor, child)['status'] == 'RESOLVED'
