"""Repeated emergency demand preserves authority and stable consume tokens."""
import copy
import datetime as dt

import pytest

from test_emergency_request_contract_gaps import ENTRY, EPOCH, LATER, OWNER, owner
from trader.strategy import auto_executor as execution
from trader.strategy.auto_executor import AutoExecutor


def _mark_existing_attempt(executor, pending):
    # These tests isolate request merging after an already attempted send.
    # Actual native send/receipt/epoch attribution is covered separately.
    identity = AutoExecutor._emergency_identity('parent-close', EPOCH)
    pending.update(attempted=True, intent_id=identity, ownership_epoch=EPOCH,
                   ownership_started_at=123.0)
    return identity


def test_retry_can_retain_a_timer_while_the_managed_view_is_flat(owner):
    owner._remember_emergency_exit(*OWNER, ENTRY, 'timer', entry_bar_ts=ENTRY,
                                   capture_explicit=False)
    pending = owner._emergency_exits[OWNER]
    identity = pending['intent_id']
    owner._managed_view = []
    owner._remember_emergency_exit(*OWNER, LATER, 'timer retry', entry_bar_ts=ENTRY,
                                   capture_explicit=False)
    assert pending['intent_id'] == identity
    assert pending['policy_entry_bar_ts'] == ENTRY.isoformat()
    assert pending['reason'] == 'timer retry'
    assert pending['attempted'] is False


def test_separate_request_binding_advances_and_rejects_a_stale_retry(owner):
    owner.intents.create(*OWNER, 'OPEN',
                         {'bar_ts': LATER.isoformat(), 'proposal_id': 22}, status='UNKNOWN')
    owner._remember_emergency_exit(*OWNER, ENTRY, 'request A', request_entry_bar_ts=ENTRY)
    pending = owner._emergency_exits[OWNER]
    identity = pending['intent_id']
    captured = copy.deepcopy(pending['explicit_exit_scope'])
    # A validated request can be ahead of the cached managed view while an
    # already captured OPEN is being reconciled. Its explicit binding wins.
    owner._remember_emergency_exit(*OWNER, LATER, 'request B',
                                   request_entry_bar_ts=LATER, explicit_exit_scope=captured,
                                   capture_explicit=False)
    owner._remember_emergency_exit(*OWNER, ENTRY, 'stale A',
                                   request_entry_bar_ts=ENTRY, capture_explicit=False)
    assert pending['request_entry_bar_ts'] == LATER.isoformat()
    assert pending['reason'] == 'request B'
    assert pending['bar_ts'] == LATER.isoformat()
    assert pending['intent_id'] == identity
    assert pending['explicit_exit_scope'] == captured


@pytest.mark.parametrize('attempted', [False, True])
def test_explicit_same_entry_demand_survives_a_repeated_timer(owner, attempted):
    owner._remember_emergency_exit(*OWNER, ENTRY, 'timer', entry_bar_ts=ENTRY,
                                   capture_explicit=False)
    pending = owner._emergency_exits[OWNER]
    identity = _mark_existing_attempt(owner, pending) if attempted else pending['intent_id']
    scope = owner._capture_exit_scope(*OWNER)
    owner._remember_emergency_exit(*OWNER, ENTRY, 'explicit SELL',
                                   explicit_exit_scope=scope, capture_explicit=False)
    retained = copy.deepcopy(pending.get('successor_exit'))
    assert pending['reason'] == 'explicit SELL'
    if attempted:
        assert retained['entry_bar_ts'] == ENTRY.isoformat()
        assert retained['request_id']
        assert retained['reason'] == 'explicit SELL'
    owner._remember_emergency_exit(*OWNER, ENTRY, 'repeated timer', entry_bar_ts=ENTRY,
                                   capture_explicit=False)
    assert pending['reason'] == 'explicit SELL'
    assert pending.get('successor_exit') == retained
    assert pending['explicit_exit_scope'] == scope
    assert pending['intent_id'] == identity


def test_scoped_request_does_not_gain_an_uncaptured_same_bar_fallback(owner):
    owner._remember_emergency_exit(*OWNER, ENTRY, 'explicit SELL')
    pending = owner._emergency_exits[OWNER]
    owner._remember_emergency_exit(*OWNER, ENTRY, 'timer retry', entry_bar_ts=ENTRY,
                                   capture_explicit=False)
    # A distinct proposal at the same bar is not in the captured position set.
    # Adding a redundant-looking request binding would enable its fallback.
    unrelated = {'entry_bar_ts': ENTRY, 'proposal_id': 999}
    assert owner._close_entry_matches({'payload': pending}, unrelated) is False
    assert pending['request_entry_bar_ts'] is None


def test_repeated_timer_successor_keeps_its_consume_token_and_attempt(owner):
    owner._remember_emergency_exit(*OWNER, ENTRY, 'timer A', entry_bar_ts=ENTRY,
                                   capture_explicit=False)
    pending = owner._emergency_exits[OWNER]
    identity = _mark_existing_attempt(owner, pending)
    owner._remember_emergency_exit(*OWNER, LATER, 'timer B', entry_bar_ts=LATER,
                                   capture_explicit=False)
    successor = copy.deepcopy(pending['successor_exit'])
    assert successor['request_id']
    assert successor['reason'] == 'timer B'
    owner._remember_emergency_exit(*OWNER, LATER, 'timer B', entry_bar_ts=LATER,
                                   capture_explicit=False)
    assert pending['successor_exit'] == successor
    assert pending['intent_id'] == identity
    # Exercise the actual consumer instead of requiring a redundant policy
    # field: the successor's concrete request binding excludes another entry.
    child = {'payload': {'request_entry_bar_ts': successor['entry_bar_ts'],
                         'policy_entry_bar_ts': successor.get('policy_entry_bar_ts')}}
    assert owner._close_entry_matches(child, {'entry_bar_ts': LATER}) is True
    assert owner._close_entry_matches(child, {'entry_bar_ts': ENTRY}) is False


def test_newer_explicit_successor_changes_token_once_without_rebinding_attempt(owner):
    newest = LATER + dt.timedelta(minutes=1)
    owner.intents.create(*OWNER, 'OPEN',
                         {'bar_ts': LATER.isoformat(), 'proposal_id': 22}, status='UNKNOWN')
    owner._remember_emergency_exit(*OWNER, ENTRY, 'explicit SELL')
    pending = owner._emergency_exits[OWNER]
    identity = _mark_existing_attempt(owner, pending)
    # B and C are separately validated timer-demand inputs to this request
    # layer. This fixture has one captured pending OPEN (B); it does not claim
    # to execute a complete admitted opening/fill sequence through entry C.
    owner._remember_emergency_exit(*OWNER, LATER, 'timer B', entry_bar_ts=LATER,
                                   capture_explicit=False)
    first = copy.deepcopy(pending['successor_exit'])
    owner._remember_emergency_exit(*OWNER, newest, 'timer C', entry_bar_ts=newest,
                                   capture_explicit=False)
    latest = copy.deepcopy(pending['successor_exit'])
    assert latest['entry_bar_ts'] == newest.isoformat()
    assert latest['request_id'] and latest['request_id'] != first['request_id']
    assert latest['reason'] == 'timer C'
    owner._remember_emergency_exit(*OWNER, newest, 'timer C', entry_bar_ts=newest,
                                   capture_explicit=False)
    owner._remember_emergency_exit(*OWNER, LATER, 'stale B', entry_bar_ts=LATER,
                                   capture_explicit=False)
    assert pending['successor_exit'] == latest
    assert pending['intent_id'] == identity
    assert pending['ownership_epoch'] == EPOCH


def test_legacy_waiting_explicit_request_is_not_narrowed_to_a_later_timer(owner):
    # Historical WAITING requests can predate the separate scope/binding
    # fields. The real request and matching helpers still support that shape.
    legacy = owner.intents.create(*OWNER, 'CLOSE',
                                  {'bar_ts': ENTRY.isoformat(), 'reason': 'legacy explicit SELL'},
                                  status='WAITING')
    assert owner._unsubmitted_close(legacy) is True
    owner._remember_emergency_exit(*OWNER, ENTRY, legacy['payload']['reason'],
                                   capture_explicit=False, parent_intent_id=legacy['intent_id'])
    pending = owner._emergency_exits[OWNER]
    owner._remember_emergency_exit(*OWNER, LATER, 'later timer', entry_bar_ts=LATER,
                                   capture_explicit=False)
    # The current entry is still the one the explicit request can close.
    assert owner._close_entry_matches({'payload': pending}, {'entry_bar_ts': ENTRY}) is True
    assert pending['reason'] == 'legacy explicit SELL'
    assert pending['parent_intent_id'] == legacy['intent_id']


def test_emergency_degradation_notice_identifies_the_affected_owner(owner, monkeypatch):
    messages = []
    monkeypatch.setattr(execution.logging, 'critical',
                        lambda message, *args: messages.append(message % args))
    owner._remember_emergency_exit(*OWNER, ENTRY, 'timer', entry_bar_ts=ENTRY,
                                   capture_explicit=False)
    assert len(messages) == 1
    assert OWNER[0] in messages[0]
    assert str(OWNER[1]) in messages[0]
    assert 'durability is degraded' in messages[0].casefold()
