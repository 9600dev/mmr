"""Execution admission preserves owner and entry policy under overload."""
from types import SimpleNamespace

import pandas as pd
import pytest

from test_execution_recovery import recovery
from review.test_review_strategy_contract import FakeResult, TS, make_work
from trader.objects import Action
from trader.strategy.auto_executor import AutoExecutionError, AutoExecutor
from trader.strategy.execution_queue import ExecutionWorkQueue


def hold_worker(executor, monkeypatch, *, opening_capacity=0, exit_capacity=0):
    monkeypatch.setattr(executor, 'start', lambda: None)
    executor._queue = ExecutionWorkQueue(opening_capacity=opening_capacity,
                                         exit_capacity=exit_capacity)


def test_signal_admission_reports_queue_acceptance_and_opening_refusal(recovery, monkeypatch):
    executor, _, _ = recovery
    hold_worker(executor, monkeypatch, opening_capacity=1)
    work = make_work()
    assert executor.submit_signal(work) is True
    refused = make_work(bar_ts=TS + pd.Timedelta(minutes=1))
    assert executor.submit_signal(refused) is False
    assert executor._queue.qsize() == 1
    assert executor._queue.get(timeout=0) is work


def test_flat_sell_overflow_is_acknowledged_without_reserving_inventory(recovery, monkeypatch):
    executor, sdk, _ = recovery
    hold_worker(executor, monkeypatch)
    assert executor.submit_signal(make_work(action=Action.SELL)) is True
    assert executor.status_metrics()['overflow_exit_intents'] == 0
    executor.manage_positions()
    assert not executor.intents.all(kind='CLOSE')
    assert not sdk.propose_calls


def test_zero_fill_pending_open_sell_survives_saturated_exit_queue(recovery, monkeypatch):
    executor, sdk, _ = recovery
    sdk.fill_next = 0
    executor._process_signal(make_work(quantity=140))
    entry_id = sdk.accepted[0]['orderId']
    hold_worker(executor, monkeypatch)
    assert executor.submit_signal(make_work(action=Action.SELL,
                                             bar_ts=TS + pd.Timedelta(minutes=1))) is True
    executor.manage_positions()
    assert entry_id in sdk.cancel_calls
    assert sdk.accepted[0]['status'] == 'Cancelled'
    assert len(sdk.approve_calls) == 1
    assert not executor.intents.all(kind='OPEN', active=True)


def test_overflow_sell_keeps_separate_owners_for_the_same_contract(recovery, monkeypatch):
    executor, sdk, _ = recovery
    sdk.broker[1111] = 100
    executor._process_signal(make_work(strategy_name='alpha', quantity=40))
    executor._process_signal(make_work(strategy_name='beta', quantity=60))
    hold_worker(executor, monkeypatch)
    for owner in ('alpha', 'beta'):
        assert executor.submit_signal(make_work(strategy_name=owner, action=Action.SELL,
                                                 bar_ts=TS + pd.Timedelta(minutes=1))) is True
    assert executor.status_metrics()['overflow_exit_intents'] == 2
    executor.manage_positions()
    assert sdk.broker[1111] == 100
    assert executor.state.open_position('alpha', 1111) is None
    assert executor.state.open_position('beta', 1111) is None
    assert [call['quantity'] for call in sdk.propose_calls if call['action'] == 'SELL'] == [40, 60]


def timer(executor, entry, minute, observed):
    return executor.submit_bar('orb_test', 1111, TS + pd.Timedelta(minutes=minute), len(observed),
                               entry_bar_ts=entry, observed_bar_timestamps=tuple(observed))


def test_obsolete_epoch_cannot_replace_newer_due_timer_in_overflow(recovery, monkeypatch):
    executor, sdk, _ = recovery
    executor.cooldown_seconds = 0
    sdk.broker[1111] = 100
    executor._process_signal(make_work(quantity=40, pyramid_max_adds=1, max_hold_bars=10))
    entry = TS + pd.Timedelta(minutes=1)
    executor._process_signal(make_work(quantity=40, pyramid_max_adds=1, max_hold_bars=3, bar_ts=entry))
    hold_worker(executor, monkeypatch)
    assert timer(executor, entry, 4, [TS + pd.Timedelta(minutes=i) for i in (2, 3, 4)]) is True
    assert timer(executor, TS, 5, [TS + pd.Timedelta(minutes=5)]) is True
    executor.manage_positions()
    assert sdk.broker[1111] == 100
    assert sdk.propose_calls[-1]['action'] == 'SELL'
    assert sdk.propose_calls[-1]['quantity'] == 80


def test_new_entry_cannot_inherit_old_epoch_staged_bar_count(recovery, monkeypatch):
    executor, sdk, _ = recovery
    executor.cooldown_seconds = 0
    executor._process_signal(make_work(quantity=40, pyramid_max_adds=1, max_hold_bars=10))
    hold_worker(executor, monkeypatch)
    assert timer(executor, TS, 4, [TS + pd.Timedelta(minutes=i) for i in (1, 2, 3, 4)]) is True
    entry = TS + pd.Timedelta(minutes=5)
    monkeypatch.setattr(executor, '_now_utc', lambda: (entry.tz_localize('UTC') + pd.Timedelta(seconds=30)).to_pydatetime())
    executor._process_signal(make_work(quantity=40, pyramid_max_adds=1, max_hold_bars=3, bar_ts=entry))
    assert timer(executor, entry, 6, [TS + pd.Timedelta(minutes=6)]) is True
    executor.manage_positions()
    assert sdk.broker[1111] == 80
    assert len(sdk.approve_calls) == 2
    assert executor.state.open_position('orb_test', 1111)['quantity'] == 80


def test_older_bar_is_acknowledged_without_replacing_latest_queued_policy(recovery, monkeypatch):
    executor, sdk, _ = recovery
    executor._process_signal(make_work(quantity=40, max_hold_bars=3))
    hold_worker(executor, monkeypatch)
    assert timer(executor, TS, 3, [TS + pd.Timedelta(minutes=i) for i in (1, 2, 3)]) is True
    assert timer(executor, TS, 2, [TS + pd.Timedelta(minutes=2)]) is True
    executor.manage_positions()
    assert sdk.broker[1111] == 0


@pytest.mark.parametrize('container', [list, pd.DataFrame])
def test_live_protective_lookup_preserves_owner_contract_side_and_order_type(container):
    def row(order_id, **changes):
        return dict(dict(orderId=order_id, orderRef='alpha|mmr:owned-stop', conId=1111,
                         action='SELL', orderType='STP', status='Submitted'), **changes)
    rows = [row(1), row(2, orderRef='beta|mmr:other-stop'), row(3, conId=2222),
            row(4, action='BUY'), row(5, orderType='LMT'), row(6, status='Filled'),
            row(7, orderType='TRAIL', status='PendingCancel')]
    executor = object.__new__(AutoExecutor)
    executor._snapshot_cache = None
    executor._sdk = SimpleNamespace(execution_snapshot=lambda **kwargs:
                                   dict(complete=True, orders=container(rows)))
    assert executor._own_live_protectives(executor._sdk, 'alpha', 1111) == [1, 7]


@pytest.mark.parametrize('snapshot', [dict(complete=False, orders=[]), dict(orders=[])])
def test_protective_cancellation_refuses_incomplete_order_book(snapshot):
    executor = object.__new__(AutoExecutor)
    executor._snapshot_cache = None
    executor._sdk = SimpleNamespace(execution_snapshot=lambda **kwargs: snapshot)
    with pytest.raises(AutoExecutionError, match='snapshot incomplete'):
        executor._own_live_protectives(executor._sdk, 'alpha', 1111)


@pytest.mark.parametrize('pending_status', ['UNKNOWN', 'SUBMITTING'])
def test_pending_close_retries_same_proposal_only_after_explicit_server_permission(
        recovery, monkeypatch, pending_status):
    executor, sdk, _ = recovery
    monkeypatch.setenv('MMR_PROTECTIVE_STOP_PCT', '0')
    sdk.broker[1111] = 100
    executor._process_signal(make_work(quantity=40))
    approve = sdk.approve
    attempts = []
    permitted = False

    def coordinated_approve(proposal_id, **kwargs):
        attempts.append((proposal_id, kwargs))
        if len(attempts) == 1:
            return FakeResult(ok=False, error='UNKNOWN: cancellation coordination pending; no order submitted')
        return approve(proposal_id)

    def snapshot(intent_id='', order_ids=None):
        rows = sdk.trades().to_dict('records')
        return dict(complete=True, orders=rows, positions_complete=True,
                    positions=sdk.positions().to_dict('records'),
                    retry_safe=permitted and bool(intent_id))

    monkeypatch.setattr(sdk, 'approve', coordinated_approve)
    monkeypatch.setattr(sdk, 'execution_snapshot', snapshot, raising=False)
    executor._process_signal(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))
    pending = executor.intents.all(kind='CLOSE', active=True)[0]
    executor.intents.update(pending, status=pending_status)
    executor.manage_positions()
    assert len(attempts) == 1
    assert sdk.broker[1111] == 140
    permitted = True
    executor.manage_positions()
    assert attempts == [(attempts[0][0], {}), (attempts[0][0], {'resume': True})]
    assert len(sdk.propose_calls) == 2
    assert sdk.broker[1111] == 100
    assert executor.state.open_position('orb_test', 1111) is None


def test_closing_one_owner_does_not_cancel_another_owners_pending_buy(recovery):
    executor, sdk, _ = recovery
    sdk.fill_next = 0
    executor._process_signal(make_work(strategy_name='alpha', quantity=40))
    other_id = sdk.accepted[0]['orderId']
    sdk.fill_next = None
    executor._process_signal(make_work(strategy_name='beta', quantity=60))
    executor._process_signal(make_work(strategy_name='beta', action=Action.SELL,
                                      bar_ts=TS + pd.Timedelta(minutes=1)))
    assert other_id not in sdk.cancel_calls
    assert sdk.accepted[0]['status'] == 'Submitted'
    assert executor.intents.all(strategy='alpha', kind='OPEN', active=True)
    assert sdk.propose_calls[-1]['quantity'] == 60


def test_single_owned_share_remains_a_real_exit(recovery):
    executor, sdk, _ = recovery
    sdk.broker[1111] = 100
    executor._process_signal(make_work(quantity=1))
    executor._process_signal(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))
    assert sdk.propose_calls[-1]['action'] == 'SELL'
    assert sdk.propose_calls[-1]['quantity'] == 1
    assert sdk.broker[1111] == 100


def test_storage_failure_after_close_claim_preserves_emergency_owner_and_timer_epoch(recovery, monkeypatch):
    executor, sdk, _ = recovery
    sdk.broker[1111] = 100
    executor._process_signal(make_work(quantity=40, max_hold_bars=1))
    update = executor.intents.update
    attempts = []

    def fail_final_quantity(intent, **changes):
        if intent['kind'] == 'CLOSE' and 'quantity' in changes:
            raise OSError('journal becomes unavailable after cancellation')
        return update(intent, **changes)

    def emergency_close_position(**kwargs):
        attempts.append(kwargs)
        sdk.broker[1111] -= kwargs['quantity']
        sdk.accepted.append(dict(orderId=1999, orderRef='orb_test',
                                 clientIntentId=kwargs['client_intent_id'], conId=1111,
                                 action='SELL', status='Filled', totalQuantity=kwargs['quantity'],
                                 filled=kwargs['quantity']))
        return FakeResult(obj=[1999])

    monkeypatch.setattr(executor.intents, 'update', fail_final_quantity)
    monkeypatch.setattr(sdk, 'emergency_close_position', emergency_close_position, raising=False)
    executor._execute_close('orb_test', 1111, TS + pd.Timedelta(minutes=1), 40,
                            reason='max_hold_bars', entry_bar_ts=TS)
    assert len(attempts) == 1
    assert attempts[0]['strategy_name'] == 'orb_test'
    assert attempts[0]['con_id'] == 1111
    assert attempts[0]['quantity'] == 40
    assert sdk.broker[1111] == 100
    emergency = executor._emergency_exits[('orb_test', 1111)]
    assert emergency['policy_entry_bar_ts'] == TS.tz_localize('UTC').isoformat()
    assert emergency['reason'] == 'max_hold_bars'
    executor.manage_positions()
    assert len(attempts) == 1


@pytest.mark.parametrize('container', [list, pd.DataFrame])
@pytest.mark.parametrize('status, expected', [('Filled', True), ('Cancelled', True),
                                            ('ApiCancelled', True), ('Inactive', True),
                                            ('Submitted', False), ('PendingCancel', False),
                                            ('Unknown', False)])
def test_generic_cancellation_requires_terminal_quantity_evidence_from_scoped_snapshot(
        container, status, expected):
    queries = []

    def snapshot(**kwargs):
        queries.append(kwargs)
        return dict(complete=True, orders=container([dict(orderId=17, status=status,
                                                          brokerStatus='Filled')]))

    def legacy_trades():
        pytest.fail('explicit broker completeness protocol must be used when available')

    executor = object.__new__(AutoExecutor)
    executor._snapshot_cache = None
    executor._sdk = SimpleNamespace(execution_snapshot=snapshot, trades=legacy_trades)
    assert executor._order_is_terminal(17) is expected
    assert queries == [dict(order_ids=[17])]


@pytest.mark.parametrize('snapshot', [dict(complete=False, orders=[dict(orderId=1, status='Filled')]),
                                    dict(orders=[dict(orderId=1, status='Filled')]),
                                    dict(complete=True, orders=[]),
                                    dict(complete=True, orders=[dict(status='Filled')]),
                                    dict(complete=True, orders=[dict(orderId=0, status='Filled')])])
def test_missing_or_incomplete_order_identity_never_confirms_cancellation(snapshot):
    executor = object.__new__(AutoExecutor)
    executor._snapshot_cache = None
    executor._sdk = SimpleNamespace(execution_snapshot=lambda **kwargs: snapshot)
    assert executor._order_is_terminal(1) is False


def test_raised_protective_cancel_error_keeps_order_and_close_pending(recovery, monkeypatch):
    executor, sdk, _ = recovery
    errors = []
    monkeypatch.setattr('trader.strategy.auto_executor.logging.exception',
                        lambda *args, **kwargs: errors.append(args))
    executor._process_signal(make_work(quantity=40))
    protective_id = executor.state.open_position('orb_test', 1111)['protective_order_id']
    cancel = sdk.cancel

    def disconnected_cancel(order_id):
        raise ConnectionError('cancel reply unavailable')

    monkeypatch.setattr(sdk, 'cancel', disconnected_cancel)
    executor._process_signal(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))
    assert len(sdk.approve_calls) == 1
    assert sdk.broker[1111] == 40
    assert executor.state.open_position('orb_test', 1111)['protective_order_id'] == protective_id
    assert protective_id in sdk.active_stops
    assert errors and errors[0][-1] == protective_id
    monkeypatch.setattr(sdk, 'cancel', cancel)
    executor.manage_positions()
    assert sdk.broker[1111] == 0
    assert len(sdk.approve_calls) == 2
