"""Durable execution and recovery tests: no broker or operational state used."""
import datetime as dt

import pandas as pd
import pytest

from review.test_review_strategy_contract import LifecycleSDK, FakeResult, TS, make_work
from trader.objects import Action
from trader.strategy.auto_executor import AutoExecutor, BarWork
from trader.strategy.execution_queue import ExecutionWorkQueue


@pytest.fixture
def recovery(tmp_path, monkeypatch):
    monkeypatch.delenv('MMR_AUTO_EXECUTE_DISABLED', raising=False)
    monkeypatch.setenv('MMR_PROTECTIVE_STOP_PCT', '8')
    monkeypatch.setattr(AutoExecutor, '_now_utc', lambda self:
                        (TS.tz_localize('UTC') + pd.Timedelta(seconds=70)).to_pydatetime())
    sdk = LifecycleSDK()
    path = str(tmp_path / 'execution.duckdb')
    executor = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
    return executor, sdk, path


def test_open_reservation_is_durable_before_approve(recovery, monkeypatch):
    executor, sdk, path = recovery
    approve = sdk.approve

    def inspect_durable_reservation(pid):
        restarted = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
        intents = restarted.intents.all(kind='OPEN', active=True)
        assert len(intents) == 1
        assert intents[0]['status'] == 'SUBMITTING'
        assert intents[0]['payload']['proposal_id'] == pid
        assert intents[0]['intent_id'] == sdk.propose_calls[-1]['metadata']['client_intent_id']
        return approve(pid)

    monkeypatch.setattr(sdk, 'approve', inspect_durable_reservation)
    executor._process_signal(make_work(quantity=140))
    assert executor.state.open_position('orb_test', 1111)['quantity'] == 140


def test_unfilled_open_reserves_without_fabricating_owned_shares(recovery):
    executor, sdk, _ = recovery
    sdk.fill_next = 0
    executor._process_signal(make_work(quantity=140))
    executor._process_signal(make_work(quantity=140, bar_ts=TS + pd.Timedelta(minutes=1)))
    assert executor.state.open_position('orb_test', 1111) is None
    assert len(sdk.approve_calls) == 1
    assert not sdk.active_stops
    assert executor.intents.all(kind='OPEN', active=True)[0]['status'] == 'WORKING'


def test_unknown_reply_recovers_fill_ownership_and_stop_after_restart(recovery):
    executor, sdk, path = recovery
    sdk.timeout_after_submit = True
    executor._process_signal(make_work(quantity=140))
    restarted = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
    restarted.manage_positions()
    assert restarted.state.open_position('orb_test', 1111)['quantity'] == 140
    assert len(sdk.approve_calls) == 1
    assert sum(row['quantity'] for row in sdk.active_stops.values()) == 140


def test_partial_entry_progress_is_not_reapplied_after_restart(recovery):
    executor, sdk, path = recovery
    sdk.fill_next = 40
    executor._process_signal(make_work(quantity=140))
    sdk.accepted[0].update(filled=100)
    sdk.broker[1111] = 100
    executor.manage_positions()
    restarted = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
    restarted.manage_positions()
    restarted.manage_positions()
    position = restarted.state.open_position('orb_test', 1111)
    assert position['quantity'] == 100
    assert position['lots'] == 1
    assert sum(row['quantity'] for row in sdk.active_stops.values()) == 100


def test_replayed_order_id_from_another_intent_cannot_claim_manual_fills(recovery):
    executor, sdk, _ = recovery
    sdk.broker[1111] = 100
    sdk.fill_next = 40
    executor._process_signal(make_work(quantity=140))
    sdk.accepted.append(dict(sdk.accepted[0], clientIntentId='another-client-intent',
                             filled=100, totalQuantity=100, status='Filled', clientId=99))
    executor.manage_positions()
    assert executor.state.open_position('orb_test', 1111)['quantity'] == 40
    assert sum(row['quantity'] for row in sdk.active_stops.values()) == 40


def test_crash_after_attribution_commit_does_not_apply_fill_twice(recovery, monkeypatch):
    executor, sdk, path = recovery
    update = executor.intents.update

    def die_before_checkpoint_ack(intent, **kwargs):
        if 'cumulative_filled' in kwargs:
            raise RuntimeError('process ends after attribution transaction commits')
        return update(intent, **kwargs)

    monkeypatch.setattr(executor.intents, 'update', die_before_checkpoint_ack)
    with pytest.raises(RuntimeError):
        executor._process_signal(make_work(quantity=140))
    restarted = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
    restarted.manage_positions()
    assert restarted.state.open_position('orb_test', 1111)['quantity'] == 140
    assert len(sdk.approve_calls) == 1


def test_pending_cancel_exit_retries_without_another_signal(recovery):
    executor, sdk, _ = recovery
    executor._process_signal(make_work(quantity=140))
    sdk.cancel_fails = True
    executor._process_signal(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))
    assert len(sdk.approve_calls) == 1
    assert executor.intents.all(kind='CLOSE', active=True)
    sdk.cancel_fails = False
    executor.manage_positions()
    assert sdk.broker[1111] == 0
    assert len(sdk.approve_calls) == 2
    assert not sdk.active_stops


def test_terminal_partial_close_retries_only_unfilled_owned_residual(recovery):
    executor, sdk, _ = recovery
    sdk.broker[1111] = 100  # manual shares survive both exit attempts
    executor._process_signal(make_work(quantity=40))
    sdk.fill_next = 20
    executor._process_signal(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))
    assert executor.state.open_position('orb_test', 1111)['quantity'] == 20
    sdk.accepted[-1]['status'] = 'Cancelled'
    sdk.fill_next = None
    executor.manage_positions()
    assert sdk.propose_calls[-1]['quantity'] == 20
    assert sdk.broker[1111] == 100
    assert executor.state.open_position('orb_test', 1111) is None


def test_ambiguous_protective_placement_is_recovered_without_duplicate(recovery, monkeypatch):
    executor, sdk, path = recovery
    place = sdk.place_protective_order

    def lose_reply(**kwargs):
        place(**kwargs)
        return FakeResult(ok=False, error='UNKNOWN: reply lost')

    monkeypatch.setattr(sdk, 'place_protective_order', lose_reply)
    executor._process_signal(make_work(quantity=140))
    restarted = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
    restarted.manage_positions()
    assert len(sdk.protective_calls) == 1
    assert restarted.state.open_position('orb_test', 1111)['protective_order_id'] == 900


def test_authority_is_rechecked_after_slow_proposal_creation(recovery, monkeypatch):
    executor, sdk, _ = recovery
    enabled = True
    executor.authority_check = lambda name, generation: enabled
    propose = sdk.propose

    def revoke_during_propose(**kwargs):
        nonlocal enabled
        result = propose(**kwargs)
        enabled = False
        return result

    monkeypatch.setattr(sdk, 'propose', revoke_during_propose)
    executor._process_signal(make_work(quantity=140, deployment_generation='generation-1'))
    assert sdk.approve_calls == []
    assert not executor.intents.all(kind='OPEN', active=True)


def test_external_replacement_stop_fill_reduces_owned_quantity(recovery):
    executor, sdk, _ = recovery
    sdk.broker[1111] = 100
    executor._process_signal(make_work(quantity=140))
    # The replacing workflow confirms the prior stop's terminal zero-fill
    # history; absence from active orders alone cannot release attribution.
    for order_id in list(sdk.active_stops):
        sdk.cancel(order_id)
    # Current native snapshot metadata proves when this independent workflow
    # originally claimed the stop; the timestamp remains unchanged on replay.
    sdk.accepted.append(dict(orderId=991, orderRef='orb_test', conId=1111,
                             status='Submitted', action='SELL', orderType='STP',
                             totalQuantity=140, filled=40, avgFillPrice=95,
                             clientIntentId='protective:external-991',
                             brokerIntentCreatedAt=dt.datetime.now(dt.timezone.utc).timestamp()))
    sdk.broker[1111] = 200  # replacement sold 40 of strategy's 140 shares
    executor.manage_positions()
    assert executor.state.open_position('orb_test', 1111)['quantity'] == 100
    executor._process_signal(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))
    assert sdk.propose_calls[-1]['quantity'] == 100
    assert sdk.broker[1111] == 100


def test_sell_cancels_an_accepted_entry_before_it_can_fill(recovery):
    executor, sdk, _ = recovery
    sdk.fill_next = 0
    executor._process_signal(make_work(quantity=140))
    executor._process_signal(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))
    assert sdk.accepted[0]['status'] == 'Cancelled'
    assert len(sdk.approve_calls) == 1
    assert not executor.intents.all(kind='OPEN', active=True)


def test_local_journal_failure_keeps_a_broker_facing_exit(recovery, monkeypatch):
    executor, sdk, _ = recovery
    executor._process_signal(make_work(quantity=140))
    calls = []

    def emergency_close_position(**kwargs):
        calls.append(kwargs)
        sdk.active_stops.clear()  # server coordinator confirms cancellation first
        sdk.broker[1111] -= kwargs['quantity']
        sdk.accepted.append(dict(orderId=1999, orderRef='orb_test|mmr:' + kwargs['client_intent_id'],
                                 clientIntentId=kwargs['client_intent_id'], conId=1111,
                                 action='SELL', status='Filled', totalQuantity=kwargs['quantity'],
                                 filled=kwargs['quantity']))
        return FakeResult(obj=[1999])

    monkeypatch.setattr(sdk, 'emergency_close_position', emergency_close_position, raising=False)
    monkeypatch.setattr(executor.intents, 'create', lambda *a, **k: (_ for _ in ()).throw(OSError('journal unavailable')))
    executor._process_signal(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))
    assert len(calls) == 1
    assert calls[0]['quantity'] == 140
    assert sdk.broker[1111] == 0
    executor.manage_positions()
    assert len(calls) == 1, 'emergency reply replay must not submit another physical order'
    assert executor.state.open_position('orb_test', 1111) is None


def test_both_journal_and_broker_unavailable_leave_exit_pending(recovery, monkeypatch):
    executor, sdk, _ = recovery
    executor._process_signal(make_work(quantity=140))
    monkeypatch.setattr(executor.intents, 'create', lambda *a, **k: (_ for _ in ()).throw(OSError('journal unavailable')))
    monkeypatch.setattr(sdk, 'emergency_close_position', lambda **k: (_ for _ in ()).throw(ConnectionError('broker unavailable')), raising=False)
    executor._process_signal(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))
    assert executor._emergency_exits
    assert executor.status_metrics()['emergency_exit_intents'] == 1
    assert executor.status_metrics()['durability_degraded']
    assert sdk.broker[1111] == 140
    assert executor.state.open_position('orb_test', 1111) is not None


def test_storage_outage_does_not_bypass_global_signal_kill_switch(recovery, monkeypatch):
    executor, sdk, _ = recovery
    executor._process_signal(make_work(quantity=140))
    monkeypatch.setenv('MMR_AUTO_EXECUTE_DISABLED', '1')
    monkeypatch.setattr(executor.intents, 'all', lambda **kwargs:
                        (_ for _ in ()).throw(OSError('journal unavailable')))
    executor._process_signal(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))
    assert not executor._emergency_exits
    assert sdk.broker[1111] == 140
    assert len(sdk.approve_calls) == 1


@pytest.mark.parametrize('work_kind', ['signal', 'time_exit'])
def test_worker_still_dispatches_reductions_during_local_storage_outage(recovery, monkeypatch, work_kind):
    executor, sdk, _ = recovery
    sdk.broker[1111] = 100  # unrelated manual ownership must survive
    executor._process_signal(make_work(quantity=140, max_hold_bars=1))
    calls = []

    def emergency_close_position(**kwargs):
        calls.append(kwargs)
        sdk.active_stops.clear()  # server coordinator confirms cancellation
        sdk.broker[1111] -= kwargs['quantity']
        return FakeResult(obj=[1999])

    def unavailable(*args, **kwargs):
        raise OSError('local execution storage unavailable')

    monkeypatch.setattr(sdk, 'emergency_close_position', emergency_close_position, raising=False)
    monkeypatch.setattr(executor.intents, 'all', unavailable)
    monkeypatch.setattr(executor.state, 'open_position', unavailable)
    executor._reconciled = False
    bar_ts = TS + pd.Timedelta(minutes=1)
    item = (make_work(action=Action.SELL, bar_ts=bar_ts) if work_kind == 'signal'
            else BarWork('orb_test', 1111, bar_ts, 1))
    executor._queue.put(item)
    executor._queue.put(None)
    executor._run()
    assert len(calls) == 1, 'storage failure must not discard an already-owned reduction'
    assert calls[0]['quantity'] == 140
    assert sdk.broker[1111] == 100


def test_exit_overflow_never_writes_a_journal_on_the_runtime_loop(recovery, monkeypatch):
    executor, sdk, _ = recovery
    executor._process_signal(make_work(quantity=140))
    executor._queue = ExecutionWorkQueue(exit_capacity=0)
    monkeypatch.setattr(executor, 'start', lambda: None)
    create = executor.intents.create
    calls = []

    def observe_worker_claim(*args, **kwargs):
        calls.append(args)
        return create(*args, **kwargs)

    monkeypatch.setattr(executor.intents, 'create', observe_worker_claim)
    work = make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1))
    assert executor.submit_signal(work)
    assert executor.submit_signal(work)
    assert calls == [], 'queue overflow must not block market data on SQLite'
    assert executor.status_metrics()['overflow_exit_intents'] == 1
    executor.manage_positions()
    assert calls
    assert sdk.broker[1111] == 0
    assert executor.status_metrics()['overflow_exit_intents'] == 0


@pytest.mark.parametrize('queued_signal_first', [False, True])
def test_due_time_exit_survives_saturated_queue_with_global_signal_kill_switch(recovery, monkeypatch,
                                                                           queued_signal_first):
    executor, sdk, _ = recovery
    sdk.broker[1111] = 100  # manual shares remain outside strategy ownership
    executor._process_signal(make_work(quantity=140, max_hold_bars=1))
    executor._queue = ExecutionWorkQueue(opening_capacity=0, exit_capacity=0)
    monkeypatch.setattr(executor, 'start', lambda: None)
    bar_ts = TS + pd.Timedelta(minutes=1)
    if queued_signal_first:
        assert executor.submit_signal(make_work(action=Action.SELL, bar_ts=bar_ts))
    monkeypatch.setenv('MMR_AUTO_EXECUTE_DISABLED', '1')
    assert not executor.submit_signal(make_work(action=Action.BUY, bar_ts=bar_ts))
    assert not executor.submit_signal(make_work(action=Action.SELL, bar_ts=bar_ts))
    assert executor.submit_bar('orb_test', 1111, bar_ts, 1)
    executor.manage_positions()
    assert sdk.propose_calls[-1]['action'] == 'SELL'
    assert sdk.propose_calls[-1]['quantity'] == 140
    assert sdk.broker[1111] == 100
    assert executor.state.open_position('orb_test', 1111) is None


def test_due_timer_replacing_a_killed_overflow_signal_is_not_removed(recovery, monkeypatch):
    executor, sdk, _ = recovery
    executor._process_signal(make_work(quantity=140, max_hold_bars=1))
    executor._queue = ExecutionWorkQueue(opening_capacity=0, exit_capacity=0)
    monkeypatch.setattr(executor, 'start', lambda: None)
    bar_ts = TS + pd.Timedelta(minutes=1)
    assert executor.submit_signal(make_work(action=Action.SELL, bar_ts=bar_ts))
    injected = False

    def kill_switch_read(owner):
        nonlocal injected
        if not injected:
            injected = True
            # The runtime submits a due timer after the worker takes its
            # mailbox snapshot but before it discards the killed signal.
            assert owner.submit_bar('orb_test', 1111, bar_ts, 1)
        return True

    monkeypatch.setattr(AutoExecutor, 'kill_switch', property(kill_switch_read))
    executor._claim_overflow_exits()
    assert sdk.broker[1111] == 0, 'new independent timer must survive removal of the killed signal'
    executor.manage_positions()
    assert sdk.broker[1111] == 0
    assert executor.state.open_position('orb_test', 1111) is None


def test_persisted_bar_count_drives_due_exit_when_queue_is_saturated(recovery, monkeypatch):
    executor, sdk, path = recovery
    executor._process_signal(make_work(quantity=140, max_hold_bars=3))
    bars = tuple(TS + pd.Timedelta(minutes=i) for i in (1, 2, 3))
    executor._process_bar(BarWork('orb_test', 1111, bars[1], 2,
                                  entry_bar_ts=TS, observed_bar_timestamps=bars[:2]))
    restarted = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
    restarted._queue = ExecutionWorkQueue(opening_capacity=0, exit_capacity=0)
    monkeypatch.setattr(restarted, 'start', lambda: None)
    monkeypatch.setenv('MMR_AUTO_EXECUTE_DISABLED', '1')
    # A due timer sees one retained row, but two older rows were committed.
    with monkeypatch.context() as patch:
        patch.setattr(restarted.intents.journal, 'transaction', lambda:
                      (_ for _ in ()).throw(AssertionError('SQLite on runtime loop')))
        assert restarted.submit_bar('orb_test', 1111, bars[2], 1,
                                    entry_bar_ts=TS, observed_bar_timestamps=bars[2:])
    restarted.manage_positions()
    assert sdk.broker[1111] == 0
    assert restarted.state.open_position('orb_test', 1111) is None


def test_queued_timer_for_old_entry_cannot_close_later_pyramid_entry(recovery):
    executor, sdk, _ = recovery
    executor._process_signal(make_work(quantity=140, max_hold_bars=1))
    later = TS + pd.Timedelta(minutes=1)
    executor.cooldown_seconds = 0
    executor._process_signal(make_work(quantity=140, max_hold_bars=5,
                                       pyramid_max_adds=1, bar_ts=later))
    executor._process_bar(BarWork('orb_test', 1111, later, 1,
                                  entry_bar_ts=TS, observed_bar_timestamps=(later,)))
    assert sdk.broker[1111] == 280
    assert len(sdk.approve_calls) == 2
    assert executor.state.open_position('orb_test', 1111)['entry_bar_ts'] == later


@pytest.mark.parametrize('storage_unavailable', [False, True])
def test_old_timer_does_not_close_after_newer_buy_fill_is_discovered(recovery, monkeypatch,
                                                                  storage_unavailable):
    executor, sdk, _ = recovery
    executor._process_signal(make_work(quantity=140, max_hold_bars=1))
    later = TS + pd.Timedelta(minutes=1)
    executor.cooldown_seconds = 0
    sdk.fill_next = 0
    executor._process_signal(make_work(quantity=140, max_hold_bars=5,
                                       pyramid_max_adds=1, bar_ts=later))
    sdk.accepted[-1].update(filled=40)
    sdk.broker[1111] += 40
    emergency_calls = []
    monkeypatch.setattr(sdk, 'emergency_close_position',
                        lambda **kwargs: emergency_calls.append(kwargs), raising=False)
    if storage_unavailable:
        def unavailable(*args, **kwargs):
            raise OSError('local execution storage unavailable')
        monkeypatch.setattr(executor.state, 'open_position', unavailable)
        monkeypatch.setattr(executor.intents, 'all', unavailable)
    executor._process_bar(BarWork('orb_test', 1111, later, 1,
                                  entry_bar_ts=TS, observed_bar_timestamps=(later,)))
    assert sdk.broker[1111] == 180
    assert len(sdk.approve_calls) == 2
    assert emergency_calls == []


def test_unknown_replay_fill_total_keeps_reservation_and_confirmed_lower_bound(recovery):
    executor, sdk, _ = recovery
    sdk.fill_next = 40
    executor._process_signal(make_work(quantity=140))
    opening = executor.intents.all(kind='OPEN')[0]
    proven_ids = opening['payload']['order_ids'][:]
    sdk.accepted[0].update(status='Unknown', filled=40, fillQuantityKnown=False, orderId=0)
    executor._reconcile_intent(opening)
    assert opening['status'] == 'UNKNOWN'
    assert 0 not in opening['payload']['order_ids']
    assert opening['payload']['order_ids'] == proven_ids
    assert executor.state.open_position('orb_test', 1111)['quantity'] == 40
    assert executor.intents.all(kind='OPEN', active=True)


def test_prolonged_queue_saturation_preserves_each_observed_bar_without_retaining_frames(recovery, monkeypatch):
    executor, sdk, _ = recovery
    executor._process_signal(make_work(quantity=140, max_hold_bars=4))
    executor._queue = ExecutionWorkQueue(opening_capacity=0, exit_capacity=0)
    monkeypatch.setattr(executor, 'start', lambda: None)
    monkeypatch.setenv('MMR_AUTO_EXECUTE_DISABLED', '1')
    # Each retained frame has already lost the prior bar. Management is
    # stalled until the fourth notification; coalescing must keep its count.
    with monkeypatch.context() as patch:
        patch.setattr(executor.intents.journal, 'transaction', lambda:
                      (_ for _ in ()).throw(AssertionError('SQLite on runtime loop')))
        for index in range(1, 5):
            bar = TS + pd.Timedelta(days=index)
            assert executor.submit_bar('orb_test', 1111, bar, 1,
                                       entry_bar_ts=TS, observed_bar_timestamps=(bar,))
    assert executor.status_metrics()['overflow_bar_items'] == 1
    assert executor._bar_overflow[('orb_test', 1111)].observed_bar_timestamps == (bar,)
    executor.manage_positions()
    assert sdk.broker[1111] == 0
    assert executor.state.open_position('orb_test', 1111) is None


def test_unresolved_emergency_exit_reserves_new_pyramid_open(recovery):
    executor, sdk, _ = recovery
    executor._process_signal(make_work(quantity=140))
    executor._remember_emergency_exit('orb_test', 1111, TS, 'SELL awaiting broker observation')
    executor.cooldown_seconds = 0
    executor._process_signal(make_work(quantity=140, pyramid_max_adds=1,
                                       bar_ts=TS + pd.Timedelta(minutes=1)))
    assert len(sdk.approve_calls) == 1
    assert sdk.broker[1111] == 140


@pytest.mark.parametrize('overflow', [False, True])
def test_explicit_sell_supersedes_an_unsent_obsolete_timer(recovery, monkeypatch, overflow):
    executor, sdk, _ = recovery
    executor._process_signal(make_work(quantity=140))
    executor.intents.create('orb_test', 1111, 'CLOSE',
                           dict(bar_ts=TS.isoformat(), quantity=140, reason='old time exit',
                                policy_entry_bar_ts=(TS - pd.Timedelta(minutes=1)).isoformat()),
                           status='WAITING')
    work = make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1))
    if overflow:
        executor._queue = ExecutionWorkQueue(opening_capacity=0, exit_capacity=0)
        monkeypatch.setattr(executor, 'start', lambda: None)
        assert executor.submit_signal(work)
        executor.manage_positions()
    else:
        executor._process_signal(work)
    assert sdk.broker[1111] == 0
    assert executor.state.open_position('orb_test', 1111) is None


@pytest.mark.parametrize('scoped_order_id_known', [False, True])
def test_completed_buy_with_unknown_final_quantity_does_not_block_known_owned_exit(
        recovery, monkeypatch, scoped_order_id_known):
    executor, sdk, _ = recovery
    sdk.broker[1111] = 100  # unrelated manual inventory must survive
    sdk.fill_next = 40
    executor._process_signal(make_work(quantity=140))
    opening = executor.intents.all(kind='OPEN')[0]
    entry_id = sdk.accepted[0]['orderId']
    sdk.accepted[0].update(status='Unknown', brokerStatus='Filled', fillQuantityKnown=False)
    if not scoped_order_id_known:
        sdk.accepted[0]['orderId'] = 0
        executor.intents.update(opening, order_ids=[])
    cancel = sdk.cancel
    monkeypatch.setattr(sdk, 'cancel', lambda oid:
                        FakeResult(ok=False, error='cannot cancel completed order')
                        if oid in (0, entry_id) else cancel(oid))
    sdk.fill_next = None
    executor._process_signal(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))
    assert sdk.propose_calls[-1]['quantity'] == 40
    assert sdk.broker[1111] == 100
    assert executor.state.open_position('orb_test', 1111) is None
    assert executor.intents.all(kind='OPEN', active=True)[0]['status'] == 'UNKNOWN'
