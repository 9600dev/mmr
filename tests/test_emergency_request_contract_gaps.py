"""Emergency request authority stays separate from immutable attempts."""
import datetime as dt
import threading
from types import SimpleNamespace

import pytest

from trader.strategy import auto_executor as execution
from trader.strategy.auto_executor import AutoExecutor, AutoExecutionError
from trader.strategy.execution_intents import IntentStore


OWNER = ('emergency_contract', 1111)
ENTRY = dt.datetime(2026, 9, 9, 14, 0, tzinfo=dt.timezone.utc)
LATER = ENTRY + dt.timedelta(minutes=1)
EPOCH = 'a' * 32


@pytest.fixture
def owner(tmp_path, monkeypatch):
    executor = AutoExecutor.__new__(AutoExecutor)
    executor.intents = IntentStore(str(tmp_path / 'emergency-contract.duckdb'))
    executor._view_lock = threading.Lock()
    executor._emergency_exits = {}
    executor._unpublished_open_fills = set()
    position = {'entry_bar_ts': ENTRY, 'proposal_id': 11}
    executor.state = SimpleNamespace(open_position=lambda *args: position)
    executor._managed_view = [dict(position, strategy_name=OWNER[0], conid=OWNER[1])]
    monkeypatch.setattr(execution.logging, 'critical', lambda *args, **kwargs: None)
    return executor


def test_emergency_retry_preserves_captured_scope_without_recapturing(owner, monkeypatch):
    pending = owner.intents.create(*OWNER, 'OPEN',
                                    {'bar_ts': LATER.isoformat(), 'proposal_id': 22}, status='UNKNOWN')
    owner._remember_emergency_exit(*OWNER, LATER, 'SELL request')
    first = owner._emergency_exits[OWNER]
    identity = first['intent_id']
    assert first['explicit_exit_scope'] == {
        'positions': [{'entry_bar_ts': ENTRY.isoformat(), 'proposal_id': 11}],
        'openings': {pending['intent_id']: {'entry_bar_ts': LATER.isoformat(), 'proposal_id': 22}},
    }

    def forbidden_capture(*args):
        pytest.fail('an exception retry cannot acquire a fresh opening set')

    monkeypatch.setattr(owner, '_capture_exit_scope', forbidden_capture)
    owner._remember_emergency_exit(*OWNER, LATER, 'retry', capture_explicit=False)
    retained = owner._emergency_exits[OWNER]
    assert retained['intent_id'] == identity
    assert set(retained['explicit_exit_scope']['openings']) == {pending['intent_id']}
    assert retained['attempted'] is False
    assert AutoExecutor._emergency_epoch(identity) is None


def test_timer_successor_and_stale_retry_cannot_rebind_an_attempted_explicit_exit(owner):
    scope = {'positions': [{'entry_bar_ts': ENTRY.isoformat(), 'proposal_id': 11}], 'openings': {}}
    owner._remember_emergency_exit(*OWNER, ENTRY, 'explicit SELL',
                                   explicit_exit_scope=scope, capture_explicit=False,
                                   parent_intent_id='durable-parent')
    pending = owner._emergency_exits[OWNER]
    identity = AutoExecutor._emergency_identity('durable-parent', EPOCH)
    pending.update(attempted=True, intent_id=identity, ownership_epoch=EPOCH,
                   ownership_started_at=123.0)
    owner._remember_emergency_exit(*OWNER, LATER, 'new timer', entry_bar_ts=LATER,
                                   capture_explicit=False)
    successor = dict(pending['successor_exit'])
    assert successor['entry_bar_ts'] == LATER.isoformat()
    assert successor['policy_entry_bar_ts'] is None
    owner._remember_emergency_exit(*OWNER, ENTRY, 'old retry', entry_bar_ts=ENTRY,
                                   capture_explicit=False)
    assert pending['successor_exit'] == successor
    assert pending['intent_id'] == identity
    assert pending['ownership_epoch'] == EPOCH
    assert pending['ownership_started_at'] == 123.0
    assert pending['explicit_exit_scope'] == scope


def test_unattempted_timer_keeps_the_newer_policy_when_an_older_retry_arrives(owner):
    owner._remember_emergency_exit(*OWNER, ENTRY, 'timer A', entry_bar_ts=ENTRY,
                                   capture_explicit=False)
    pending = owner._emergency_exits[OWNER]
    identity = pending['intent_id']
    owner._remember_emergency_exit(*OWNER, LATER, 'timer B', entry_bar_ts=LATER,
                                   capture_explicit=False)
    owner._remember_emergency_exit(*OWNER, ENTRY, 'retry A', entry_bar_ts=ENTRY,
                                   capture_explicit=False)
    assert pending['intent_id'] == identity
    assert pending['attempted'] is False
    assert pending['policy_entry_bar_ts'] == LATER.isoformat()
    assert pending['bar_ts'] == LATER.isoformat()
    assert pending['explicit_exit_scope'] is None


def test_fresh_explicit_sell_can_expand_request_scope_without_changing_physical_attempt(owner):
    owner._remember_emergency_exit(*OWNER, ENTRY, 'timer A', entry_bar_ts=ENTRY,
                                   capture_explicit=False)
    pending = owner._emergency_exits[OWNER]
    identity = AutoExecutor._emergency_identity(ownership_epoch=EPOCH)
    pending.update(attempted=True, intent_id=identity, ownership_epoch=EPOCH,
                   ownership_started_at=123.0)
    owner._managed_view[0].update(entry_bar_ts=LATER, proposal_id=22)
    new_scope = {'positions': [{'entry_bar_ts': LATER.isoformat(), 'proposal_id': 22}], 'openings': {}}
    owner._remember_emergency_exit(*OWNER, LATER, 'explicit SELL B',
                                   explicit_exit_scope=new_scope, capture_explicit=False)
    assert pending['policy_entry_bar_ts'] is None
    assert pending['successor_exit']['entry_bar_ts'] == LATER.isoformat()
    assert pending['successor_exit']['policy_entry_bar_ts'] is None
    assert pending['explicit_exit_scope'] == new_scope
    assert pending['intent_id'] == identity
    assert pending['ownership_epoch'] == EPOCH


@pytest.mark.parametrize('epoch', [1, '', 'a' * 31, 'a' * 33, 'a' * 31 + 'X', 'A' * 32])
def test_emergency_writer_refuses_unprovable_physical_epochs(epoch):
    with pytest.raises(AutoExecutionError):
        AutoExecutor._emergency_identity(ownership_epoch=epoch)


@pytest.mark.parametrize('change', [
    {'kind': 'OPEN'}, {'kind': 'PROTECTIVE'}, {'status': 'CREATED'},
    {'payload': {'proposal_id': 71}}, {'payload': {'order_ids': [91]}},
    {'payload': {'submitted_at': 123.0}},
])
def test_request_without_physical_attempt_cannot_be_inferred_after_submission_evidence(change):
    request = {'kind': 'CLOSE', 'status': 'WAITING', 'payload': {}}
    assert AutoExecutor._unsubmitted_close(request) is True
    request.update(change)
    assert AutoExecutor._unsubmitted_close(request) is False


@pytest.mark.parametrize(('alias', 'expected'), [
    (None, 'adopt-local'), ('', 'adopt-local'), ('native-protective-id', 'native-protective-id'),
])
def test_broker_alias_does_not_replace_the_local_checkpoint_identity(alias, expected):
    intent = {'intent_id': 'adopt-local', 'payload': {'broker_intent_id': alias}}
    assert AutoExecutor._broker_intent_id(intent) == expected
    assert intent['intent_id'] == 'adopt-local'


@pytest.mark.parametrize(('row', 'expected'), [
    ({'orderId': 91, 'clientIntentId': 'expected'}, True),
    ({'orderId': 91, 'clientIntentId': 'foreign'}, False),
    ({'orderId': 0, 'clientIntentId': 'expected'}, True),
    ({'orderId': 91}, True),
    ({'orderId': 92}, False),
    ({'orderId': 91, 'clientIntentId': float('nan'), 'brokerOrderRef': 'owner|mmr:foreign'}, False),
    ({'orderId': 91, 'clientIntentId': float('nan'), 'brokerOrderRef': 'owner|mmr:expected'}, True),
])
def test_explicit_broker_reference_wins_over_reused_numeric_order_id(row, expected):
    assert AutoExecutor._matches_intent_order(row, 'expected', [91]) is expected
