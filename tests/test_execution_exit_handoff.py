"""A newly due owned exit survives an older close attempt and its replay."""
import pandas as pd
import pytest

from test_execution_recovery import recovery
from review.test_review_strategy_contract import FakeResult, TS, make_work
from trader.strategy.auto_executor import AutoExecutor, BarWork
from trader.strategy.execution_queue import ExecutionWorkQueue
from trader.strategy.execution_intents import timestamp_text


OLD_ENTRY = TS - pd.Timedelta(minutes=1)
DUE_BAR = TS + pd.Timedelta(minutes=1)


def _owned(recovery):
    executor, sdk, path = recovery
    sdk.broker[1111] = 100  # unrelated manual shares must survive every close
    executor._process_signal(make_work(quantity=140, max_hold_bars=1))
    return executor, sdk, path


def _prior_close(executor, sdk, *, state='WAITING', explicit=False):
    payload = dict(bar_ts=timestamp_text(OLD_ENTRY), quantity=40,
                   reason='older exit request',
                   policy_entry_bar_ts=None if explicit else timestamp_text(OLD_ENTRY))
    if state != 'WAITING':
        owned = executor.state.open_position('orb_test', 1111)
        payload.update(proposal_id=777, order_ids=[] if state == 'SUBMITTING' else [7770],
                       ownership_epoch=owned['ownership_epoch'],
                       ownership_started_at=owned['ownership_started_at'])
    intent = executor.intents.create('orb_test', 1111, 'CLOSE', payload, status=state)
    row = dict(orderId=7770, orderRef='orb_test', clientIntentId=intent['intent_id'],
               conId=1111, action='SELL', totalQuantity=40, filled=0,
               status='Unknown' if state == 'UNKNOWN' else 'Submitted',
               fillQuantityKnown=state != 'UNKNOWN')
    if state not in ('WAITING', 'SUBMITTING'):
        sdk.accepted.append(row)
    return intent, row


def _due(executor):
    executor._process_bar(BarWork('orb_test', 1111, DUE_BAR, 1, entry_bar_ts=TS,
                                  observed_bar_timestamps=(DUE_BAR,)))


@pytest.mark.parametrize('overflow', [False, True])
def test_due_current_timer_survives_unsent_obsolete_close_after_restart(recovery, monkeypatch, overflow):
    executor, sdk, path = _owned(recovery)
    _prior_close(executor, sdk)
    restarted = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
    if overflow:
        restarted._queue = ExecutionWorkQueue(opening_capacity=0, exit_capacity=0)
        monkeypatch.setattr(restarted, 'start', lambda: None)
        monkeypatch.setenv('MMR_AUTO_EXECUTE_DISABLED', '1')
        assert restarted.submit_bar('orb_test', 1111, DUE_BAR, 1,
                                     entry_bar_ts=TS, observed_bar_timestamps=(DUE_BAR,))
        restarted.manage_positions()
    else:
        _due(restarted)
    assert sdk.broker[1111] == 100
    assert restarted.state.open_position('orb_test', 1111) is None
    assert len(sdk.approve_calls) == 2
    restarted.manage_positions()
    assert len(sdk.approve_calls) == 2


@pytest.mark.parametrize('state', ['WORKING', 'UNKNOWN', 'SUBMITTING'])
@pytest.mark.parametrize(('terminal', 'filled'), [('Filled', 40), ('Cancelled', 20)])
def test_due_timer_waits_for_old_attempt_then_closes_residual_after_restart(
        recovery, state, terminal, filled):
    executor, sdk, path = _owned(recovery)
    previous, row = _prior_close(executor, sdk, state=state)
    _due(executor)
    assert len(sdk.approve_calls) == 1, 'an unresolved SELL must not be submitted twice'
    retained = executor.intents.all(kind='CLOSE', active=True)[0]
    assert retained['intent_id'] == previous['intent_id']
    assert retained['payload'].get('proposal_id') == 777
    if state != 'SUBMITTING':
        assert retained['payload']['order_ids'] == [7770]
    restarted = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
    if state == 'SUBMITTING':
        sdk.accepted.append(row)
    row.update(status=terminal, fillQuantityKnown=True, filled=filled)
    sdk.broker[1111] -= filled
    restarted.manage_positions()
    assert sdk.broker[1111] == 100
    assert restarted.state.open_position('orb_test', 1111) is None
    assert sdk.propose_calls[-1]['quantity'] == 140 - filled
    assert len(sdk.approve_calls) == 2
    restarted.manage_positions()
    assert len(sdk.approve_calls) == 2


def test_timer_handoff_preserves_an_existing_explicit_sell_policy(recovery):
    executor, sdk, path = _owned(recovery)
    old, row = _prior_close(executor, sdk, state='WORKING', explicit=True)
    _due(executor)
    assert executor.intents.all(kind='CLOSE')[0]['payload']['policy_entry_bar_ts'] is None
    row.update(status='Filled', filled=40)
    sdk.broker[1111] -= 40
    restarted = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
    restarted.manage_positions()
    assert sdk.broker[1111] == 100
    successor = next(intent for intent in restarted.intents.all(kind='CLOSE')
                     if intent['intent_id'] != old['intent_id'])
    assert successor['payload']['policy_entry_bar_ts'] is None


def test_stale_timer_cannot_replace_current_handoff_or_close_later_entry(recovery):
    executor, sdk, path = _owned(recovery)
    old, row = _prior_close(executor, sdk, state='WORKING')
    _due(executor)
    executor._execute_close('orb_test', 1111, DUE_BAR, 140, 'stale retry', entry_bar_ts=OLD_ENTRY)
    row.update(status='Filled', filled=40)
    sdk.broker[1111] -= 40
    executor._snapshot_cache = None
    executor._reconcile_intent(old)
    executor.state.record_close('orb_test', 1111, 'CLOSED_EXTERNALLY', 'owned entry B ended')
    executor.state.record_open('orb_test', 1111, 80, DUE_BAR, 778, None, 1)
    sdk.broker[1111] = 180
    restarted = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
    restarted.manage_positions()
    restarted.manage_positions()
    assert restarted.state.open_position('orb_test', 1111)['quantity'] == 80
    assert sdk.broker[1111] == 180
    assert len(sdk.approve_calls) == 1


def test_terminal_attempt_with_current_successor_still_reserves_new_pyramid_buy(recovery):
    executor, sdk, _ = _owned(recovery)
    old, row = _prior_close(executor, sdk, state='WORKING')
    _due(executor)
    row.update(status='Filled', filled=40)
    sdk.broker[1111] -= 40
    executor._snapshot_cache = None
    executor._reconcile_intent(old)
    executor.cooldown_seconds = 0
    executor._process_signal(make_work(quantity=30, pyramid_max_adds=1, bar_ts=DUE_BAR))
    assert not any(call['action'] == 'BUY' for call in sdk.propose_calls[1:])
    executor.manage_positions()
    assert sdk.broker[1111] == 100


def test_crash_after_successor_claim_before_old_request_acknowledgment_is_idempotent(recovery, monkeypatch):
    executor, sdk, path = _owned(recovery)
    old, row = _prior_close(executor, sdk, state='WORKING')
    _due(executor)
    row.update(status='Filled', filled=40)
    sdk.broker[1111] -= 40
    finish = executor.intents.finish_exit_request

    def crash_after_claim(intent, token):
        if intent['intent_id'] == old['intent_id']:
            assert len(executor.intents.all(kind='CLOSE', active=True)) == 1
            raise SystemExit('process dies after successor was committed')
        return finish(intent, token)

    monkeypatch.setattr(executor.intents, 'finish_exit_request', crash_after_claim)
    with pytest.raises(SystemExit, match='successor was committed'):
        executor.manage_positions()
    assert len(sdk.approve_calls) == 1
    restarted = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
    restarted.manage_positions()
    restarted.manage_positions()
    assert sdk.broker[1111] == 100
    assert len(sdk.approve_calls) == 2


def test_successor_of_explicit_sell_cannot_execute_against_unrelated_later_entry(recovery):
    executor, sdk, path = _owned(recovery)
    _old, row = _prior_close(executor, sdk, state='WORKING', explicit=True)
    _due(executor)
    row.update(status='Filled', filled=40)
    sdk.broker[1111] -= 40
    sdk.cancel_fails = True
    executor.manage_positions()  # successor retained, protective cancellation unresolved
    assert len(sdk.approve_calls) == 1
    executor.state.record_close('orb_test', 1111, 'CLOSED_EXTERNALLY', 'entry B ended')
    executor.state.record_open('orb_test', 1111, 80, DUE_BAR, 778, None, 1)
    sdk.broker[1111] = 180
    sdk.cancel_fails = False
    restarted = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
    restarted.manage_positions()
    restarted.manage_positions()
    assert sdk.broker[1111] == 180
    assert restarted.state.open_position('orb_test', 1111)['quantity'] == 80
    assert len(sdk.approve_calls) == 1


def test_stale_timer_retry_does_not_erase_current_successor(recovery):
    executor, sdk, _ = _owned(recovery)
    _old, row = _prior_close(executor, sdk, state='WORKING')
    _due(executor)
    executor._execute_close('orb_test', 1111, DUE_BAR, 140, 'stale retry', entry_bar_ts=OLD_ENTRY)
    row.update(status='Filled', filled=40)
    sdk.broker[1111] -= 40
    executor.manage_positions()
    assert sdk.broker[1111] == 100
    assert len(sdk.approve_calls) == 2


def test_close_claim_returning_a_newly_existing_attempt_still_retains_due_request(recovery, monkeypatch):
    executor, sdk, _ = _owned(recovery)
    _prior_close(executor, sdk)
    read = executor.intents.all
    hidden_once = False

    def before_concurrent_claim(**kwargs):
        nonlocal hidden_once
        if kwargs.get('kind') == 'CLOSE' and kwargs.get('active') and not hidden_once:
            hidden_once = True
            return []  # another claimant wins after this read, before create
        return read(**kwargs)

    monkeypatch.setattr(executor.intents, 'all', before_concurrent_claim)
    executor._execute_close('orb_test', 1111, DUE_BAR, 140, 'due current timer', entry_bar_ts=TS)
    assert sdk.broker[1111] == 100
    assert len(sdk.approve_calls) == 2


def test_stale_intent_copies_cannot_overwrite_or_acknowledge_newer_exit_authority(recovery):
    executor, sdk, path = _owned(recovery)
    old, _row = _prior_close(executor, sdk)
    stale_a = executor.intents.all(kind='CLOSE')[0]
    stale_b = executor.intents.all(kind='CLOSE')[0]
    due = dict(request_id='due-B', entry_bar_ts=timestamp_text(TS),
               policy_entry_bar_ts=timestamp_text(TS), bar_ts=timestamp_text(DUE_BAR), reason='B is due')
    executor.intents.retain_exit_successor(old, due)
    executor.intents.retain_exit_successor(stale_a, dict(due, request_id='obsolete-A',
        entry_bar_ts=timestamp_text(OLD_ENTRY), policy_entry_bar_ts=timestamp_text(OLD_ENTRY)))
    assert executor.intents.all(kind='CLOSE')[0]['payload']['successor_exit']['request_id'] == 'due-B'
    executor.intents.retain_exit_successor(stale_b, dict(due, request_id='explicit-B', policy_entry_bar_ts=None))
    assert not executor.intents.finish_exit_request(old, 'due-B')
    executor.intents.retain_exit_successor(stale_a, dict(due, request_id='timer-cannot-narrow'))
    restarted = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
    retained = restarted.intents.all(kind='CLOSE')[0]['payload']['successor_exit']
    assert retained['request_id'] == 'explicit-B'
    assert retained['policy_entry_bar_ts'] is None
    restarted.manage_positions()
    assert sdk.broker[1111] == 100


def _emergency_endpoint(sdk, calls):
    def submit(**kwargs):
        calls.append(kwargs)
        sdk.active_stops.clear()  # reduction coordinator confirmed cancellation
        sdk.broker[1111] -= kwargs['quantity']
        sdk.accepted.append(dict(orderId=8880 + len(calls), orderRef='orb_test',
                                 clientIntentId=kwargs['client_intent_id'], conId=1111,
                                 action='SELL', status='Filled', totalQuantity=kwargs['quantity'],
                                 filled=kwargs['quantity']))
        return FakeResult(obj=[8880 + len(calls)])
    return submit


def _mark_emergency_attempted(executor, previous):
    """A post-send fixture retains the holding selected before its wire call."""
    owned = executor.state.open_position('orb_test', 1111)
    previous.update(attempted=True, ownership_epoch=owned['ownership_epoch'],
                    ownership_started_at=owned['ownership_started_at'],
                    intent_id=executor._emergency_identity(
                        previous.get('parent_intent_id'), owned['ownership_epoch']))


def test_emergency_new_due_epoch_supersedes_unsent_old_timer_without_downgrade(recovery, monkeypatch):
    executor, sdk, _ = _owned(recovery)
    calls = []
    monkeypatch.setattr(sdk, 'emergency_close_position', _emergency_endpoint(sdk, calls), raising=False)
    executor._remember_emergency_exit('orb_test', 1111, OLD_ENTRY, 'old timer', entry_bar_ts=OLD_ENTRY)
    executor._remember_emergency_exit('orb_test', 1111, DUE_BAR, 'current due timer', entry_bar_ts=TS)
    executor._remember_emergency_exit('orb_test', 1111, DUE_BAR, 'stale timer retry', entry_bar_ts=OLD_ENTRY)
    executor._retry_emergency_exits()
    assert len(calls) == 1
    assert calls[0]['quantity'] == 140
    assert sdk.broker[1111] == 100
    executor.manage_positions()
    assert len(calls) == 1


@pytest.mark.parametrize('retry_safe', [False, True])
def test_emergency_successor_uses_fresh_identity_only_after_conclusive_no_send(recovery, monkeypatch, retry_safe):
    executor, sdk, _ = _owned(recovery)
    calls = []
    monkeypatch.setattr(sdk, 'emergency_close_position', _emergency_endpoint(sdk, calls), raising=False)
    executor._remember_emergency_exit('orb_test', 1111, OLD_ENTRY, 'old timer', entry_bar_ts=OLD_ENTRY)
    previous = executor._emergency_exits[('orb_test', 1111)]
    _mark_emergency_attempted(executor, previous)
    executor._remember_emergency_exit('orb_test', 1111, DUE_BAR, 'current due timer', entry_bar_ts=TS)

    def snapshot(intent_id='', order_ids=None):
        orders = sdk.trades().to_dict('records')
        if intent_id == previous['intent_id']:
            return dict(complete=True, orders=[], retry_safe=retry_safe)
        return dict(complete=True, orders=orders, positions_complete=True,
                    positions=sdk.positions().to_dict('records'))

    monkeypatch.setattr(sdk, 'execution_snapshot', snapshot, raising=False)
    executor._snapshot_cache = None
    executor._retry_emergency_exits()
    if retry_safe:
        assert len(calls) == 1
        assert calls[0]['client_intent_id'] != previous['intent_id']
        assert calls[0]['quantity'] == 140
        assert sdk.broker[1111] == 100
        executor.manage_positions()
        assert len(calls) == 1
    else:
        assert calls == []
        assert executor._emergency_exits[('orb_test', 1111)]['intent_id'] == previous['intent_id']
        assert sdk.broker[1111] == 240


@pytest.mark.parametrize('quantity_known', [True, False])
@pytest.mark.parametrize(('terminal', 'filled'), [('Filled', 40), ('Cancelled', 20)])
def test_attempted_emergency_retains_new_due_timer_until_terminal_recovery(recovery, terminal, filled, quantity_known):
    executor, sdk, path = _owned(recovery)
    executor._remember_emergency_exit('orb_test', 1111, OLD_ENTRY, 'old timer', entry_bar_ts=OLD_ENTRY)
    previous = executor._emergency_exits[('orb_test', 1111)]
    _mark_emergency_attempted(executor, previous)
    row = dict(orderId=7770, orderRef='orb_test', clientIntentId=previous['intent_id'],
               conId=1111, action='SELL', totalQuantity=40, filled=0,
               status='Submitted' if quantity_known else 'Unknown',
               brokerStatus='Submitted' if quantity_known else 'Filled', fillQuantityKnown=quantity_known)
    sdk.accepted.append(row)
    executor._remember_emergency_exit('orb_test', 1111, DUE_BAR, 'current due timer', entry_bar_ts=TS)
    executor._retry_emergency_exits()
    assert previous['attempted']
    assert executor._emergency_exits[('orb_test', 1111)]['intent_id'] == previous['intent_id']
    assert len(sdk.approve_calls) == 1
    # The broker attempt and its successor must now survive loss of the
    # volatile mailbox, while UNKNOWN/working inventory remains reserved.
    restarted = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
    row.update(status=terminal, filled=filled, fillQuantityKnown=True)
    sdk.broker[1111] -= filled
    restarted.manage_positions()
    assert sdk.broker[1111] == 100
    assert restarted.state.open_position('orb_test', 1111) is None
    assert len(sdk.approve_calls) == 2
    restarted.manage_positions()
    assert len(sdk.approve_calls) == 2
