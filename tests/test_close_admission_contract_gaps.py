"""Exit-request lifetime reserves BUY admission independently of order status.

Private draft: not collected or executed during the frozen mutation run.
The small predicate cases use persisted terminal-request metadata and real
matching/query helpers. They do not fabricate broker fills. The final two
cases also exercise the real signal admission path with the existing fake SDK.
"""
import pandas as pd
import pytest

from test_execution_recovery import recovery
from review.test_review_strategy_contract import TS, make_work
from trader.strategy.auto_executor import AutoExecutor
from trader.strategy.execution_intents import timestamp_text


OLDER = TS - pd.Timedelta(minutes=1)
LATER = TS + pd.Timedelta(minutes=1)
ADD_BAR = TS + pd.Timedelta(minutes=2)
_ABSENT = object()


@pytest.fixture
def admission(recovery, monkeypatch):
    executor, sdk, path = recovery
    monkeypatch.setenv('MMR_PROTECTIVE_STOP_PCT', '0')
    sdk.broker[1111] = 100
    executor._process_signal(make_work(quantity=40))
    executor.cooldown_seconds = 0
    assert executor.state.open_position('orb_test', 1111)['quantity'] == 40
    return executor, sdk, path


def _saved_request(executor, *, status='FILLED', active=True, scope=None,
                   policy=TS, entry=TS, successor=None):
    """Terminal request metadata; no claim of a new physical order or fill."""
    payload = dict(bar_ts=timestamp_text(TS), reason='retained reduction request',
                   policy_entry_bar_ts=timestamp_text(policy) if policy is not None else None,
                   request_entry_bar_ts=timestamp_text(entry) if entry is not None else None,
                   explicit_exit_scope=scope, successor_exit=successor)
    if active is not _ABSENT:
        payload['exit_request_active'] = active
    return executor.intents.create('orb_test', 1111, 'CLOSE', payload, status=status)


def _position(executor):
    return executor.state.open_position('orb_test', 1111)


def _successor(entry):
    return dict(request_id='saved-timer-request', bar_ts=timestamp_text(LATER),
                reason='due timer', entry_bar_ts=timestamp_text(entry),
                policy_entry_bar_ts=timestamp_text(entry))


@pytest.mark.parametrize('status', ['FILLED', 'CANCELLED', 'REJECTED', 'RESOLVED'])
def test_active_terminal_request_reserves_its_holding_without_successor(admission, status):
    executor, _, path = admission
    request = _saved_request(executor, status=status)
    assert executor._close_request_pending(request, _position(executor)) is True
    restarted = AutoExecutor(path, paper_trading=True)
    saved = restarted.intents.all(kind='CLOSE')[0]
    assert restarted._close_request_pending(saved, _position(restarted)) is True


@pytest.mark.parametrize(('status', 'pending'), [
    ('CANCELLED', True), ('REJECTED', True), ('FILLED', False), ('RESOLVED', False),
])
def test_legacy_terminal_request_default_does_not_equate_all_terminal_states(
        admission, status, pending):
    executor, _, _ = admission
    request = _saved_request(executor, status=status, active=_ABSENT)
    assert 'exit_request_active' not in request['payload']
    assert executor._close_request_pending(request, _position(executor)) is pending


@pytest.mark.parametrize('status', ['CANCELLED', 'REJECTED'])
def test_retirement_removes_reservation_even_while_scope_is_retained_for_audit(admission, status):
    executor, _, path = admission
    position = _position(executor)
    scope = dict(positions=[dict(entry_bar_ts=timestamp_text(TS),
                                 proposal_id=position['proposal_id'])], openings={})
    request = _saved_request(executor, status=status, scope=scope)
    assert executor.intents.finish_exit_request(request, None)
    assert request['payload']['explicit_exit_scope'] == scope
    assert executor._close_request_pending(request, position) is False
    restarted = AutoExecutor(path, paper_trading=True)
    saved = restarted.intents.all(kind='CLOSE')[0]
    assert saved['payload']['explicit_exit_scope'] == scope
    assert restarted._close_request_pending(saved, _position(restarted)) is False


@pytest.mark.parametrize('proposal_was_known_when_captured', [False, True])
def test_captured_completed_add_reserves_buy_despite_an_older_timer_successor(
        admission, proposal_was_known_when_captured):
    executor, sdk, path = admission
    executor._process_signal(make_work(quantity=20, pyramid_max_adds=1, bar_ts=LATER))
    opening = next(item for item in executor.intents.all(kind='OPEN')
                   if item['payload']['bar_ts'] == timestamp_text(LATER))
    assert opening['status'] == 'FILLED'
    scope = dict(positions=[], openings={opening['intent_id']: dict(
        entry_bar_ts=timestamp_text(LATER),
        proposal_id=opening['payload']['proposal_id'] if proposal_was_known_when_captured else None)})
    request = _saved_request(executor, scope=scope, policy=None, successor=_successor(TS))
    assert not executor.intents.all(kind='OPEN', active=True)
    assert executor._close_request_pending(request, _position(executor)) is True

    restarted = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
    restarted.cooldown_seconds = 0
    saved = restarted.intents.all(kind='CLOSE')[0]
    assert restarted._close_request_pending(saved, _position(restarted)) is True
    restarted._process_signal(make_work(quantity=10, pyramid_max_adds=2, bar_ts=ADD_BAR))
    assert len(sdk.propose_calls) == 2
    assert len(sdk.approve_calls) == 2
    assert _position(restarted)['quantity'] == 60
    assert sdk.broker[1111] == 160


def test_captured_unresolved_open_keeps_demand_until_its_final_outcome(admission):
    executor, _, _ = admission
    opening = executor.intents.create('orb_test', 1111, 'OPEN',
        dict(bar_ts=timestamp_text(LATER), quantity=20, proposal_id=777), status='UNKNOWN')
    scope = dict(positions=[], openings={opening['intent_id']: dict(
        entry_bar_ts=timestamp_text(LATER), proposal_id=777)})
    request = _saved_request(executor, scope=scope, policy=None, entry=None)
    # This is the request-lifetime decision while no owned row is available;
    # an active OPEN separately blocks BUY admission in the full pipeline.
    assert executor._close_request_pending(request, None) is True
    executor.intents.update(opening, status='REJECTED')
    assert executor._close_request_pending(request, None) is False


def test_obsolete_timer_successor_does_not_reserve_an_unrelated_holding(admission):
    executor, sdk, _ = admission
    request = _saved_request(executor, policy=OLDER, entry=OLDER,
                             successor=_successor(OLDER))
    assert executor._close_request_pending(request, _position(executor)) is False
    executor._process_signal(make_work(quantity=20, pyramid_max_adds=1, bar_ts=LATER))
    assert len(sdk.approve_calls) == 2
    assert sdk.propose_calls[-1]['action'] == 'BUY'
    assert _position(executor)['quantity'] == 60
    assert sdk.broker[1111] == 160


@pytest.mark.parametrize(('policy', 'entry'), [(OLDER, TS), (TS, OLDER)])
def test_unscoped_request_must_match_both_saved_entry_bindings(admission, policy, entry):
    executor, _, _ = admission
    request = _saved_request(executor, policy=policy, entry=entry)
    assert executor._close_request_pending(request, _position(executor)) is False


def test_unscoped_request_without_owned_inventory_has_no_admission_reservation(admission):
    executor, _, _ = admission
    request = _saved_request(executor, policy=None, entry=None)
    assert executor._close_request_pending(request, None) is False


def test_real_terminal_partial_timer_close_reserves_remaining_shares_before_management(admission):
    executor, sdk, _ = admission
    sdk.fill_next = 20
    executor._execute_close('orb_test', 1111, LATER, 40, 'timer due', entry_bar_ts=TS)
    closing = executor.intents.all(kind='CLOSE')[0]
    assert sdk.accepted[-1]['action'] == 'SELL'
    assert sdk.accepted[-1]['filled'] == 20
    sdk.accepted[-1]['status'] = 'Cancelled'
    executor._snapshot_cache = None
    executor._reconcile_intent(closing)
    assert closing['status'] == 'CANCELLED'
    assert closing['payload'].get('successor_exit') is None
    assert closing['payload']['explicit_exit_scope'] is None
    assert _position(executor)['quantity'] == 20
    assert executor._close_request_pending(closing, _position(executor)) is True
    sdk.fill_next = None
    executor._process_signal(make_work(quantity=10, pyramid_max_adds=1, bar_ts=ADD_BAR))
    assert len(sdk.propose_calls) == 2
    assert len(sdk.approve_calls) == 2
    assert sdk.broker[1111] == 120
    executor.manage_positions()
    assert sdk.propose_calls[-1]['action'] == 'SELL'
    assert sdk.propose_calls[-1]['quantity'] == 20
    assert _position(executor) is None
    assert sdk.broker[1111] == 100
    assert len(sdk.approve_calls) == 3
