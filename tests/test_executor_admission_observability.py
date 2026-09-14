"""Queue, rejection-audit and unresolved-ownership operator contracts."""
import datetime as dt
import queue
import threading

import pytest

from trader.data.event_store import EventStore, EventType
from trader.objects import Action
from trader.strategy import auto_executor as execution
from trader.strategy.auto_executor import AutoExecutor, ManagementWork, SignalWork
from trader.strategy.execution_intents import IntentStore
from trader.strategy.execution_queue import ExecutionWorkQueue


ENTRY = dt.datetime(2026, 9, 9, 14, 0, tzinfo=dt.timezone.utc)
OWNER = ('queue_contract', 1111)


def _loop_controller():
    # These loop-side contracts do not need a broker or an executing worker.
    # Use the real work queue and invoke the worker explicitly where needed.
    executor = AutoExecutor.__new__(AutoExecutor)
    executor._started = True
    executor._view_lock = threading.Lock()
    executor._queue = ExecutionWorkQueue(opening_capacity=0, exit_capacity=0)
    executor._management_queued = False
    executor._bar_overflow = {}
    executor._managed_view = [dict(strategy_name=OWNER[0], conid=OWNER[1],
                                   entry_bar_ts=ENTRY)]
    return executor


@pytest.fixture
def staged_bars(tmp_path):
    executor = _loop_controller()
    executor.intents = IntentStore(str(tmp_path / 'bar-observations.duckdb'))
    return executor


def test_late_older_bar_cannot_replace_a_newer_saturated_timer(staged_bars):
    first = ENTRY + dt.timedelta(minutes=1)
    latest = ENTRY + dt.timedelta(minutes=2)
    assert staged_bars.submit_bar(*OWNER, latest, 2, entry_bar_ts=ENTRY,
                                  observed_bar_timestamps=(first, latest))
    assert staged_bars.submit_bar(*OWNER, first, 1, entry_bar_ts=ENTRY,
                                  observed_bar_timestamps=(first,))

    # A due time-of-day timer must still receive the latest completed bar,
    # even when an older frame arrives before the worker drains overflow.
    work = staged_bars._bar_overflow[OWNER]
    assert work.bar_ts == latest
    assert work.bars_held == 2
    assert work.observed_progress == (latest.isoformat(), 2)


@pytest.mark.parametrize('optional_fields', ['neither', 'entry_only', 'observations_only'])
def test_partial_optional_bar_metadata_keeps_compatibility_count(staged_bars, optional_fields):
    bar = ENTRY + dt.timedelta(minutes=1)
    kwargs = {}
    if optional_fields == 'entry_only':
        kwargs['entry_bar_ts'] = ENTRY
    elif optional_fields == 'observations_only':
        kwargs['observed_bar_timestamps'] = (bar,)
    assert staged_bars.submit_bar(*OWNER, bar, 7, **kwargs)
    work = staged_bars._bar_overflow[OWNER]
    assert work.bar_ts == bar
    assert work.bars_held == 7
    assert work.observed_progress is None


def test_management_request_reaches_worker_before_explicit_shutdown():
    executor = _loop_controller()
    managed = []
    executor.manage_positions = lambda: managed.append('managed')
    executor.submit_management()
    executor.stop()
    executor._run()
    assert managed == ['managed']


def test_management_is_coalesced_until_dequeued_work_begins():
    executor = _loop_controller()
    executor.submit_management()
    item = executor._queue.get(timeout=0)
    assert isinstance(item, ManagementWork)
    # Pin the actual scheduling window after queue removal but before the
    # worker starts management. Another loop-side pulse is still coalesced.
    executor.submit_management()
    with pytest.raises(queue.Empty):
        executor._queue.get(timeout=0)


def test_saturated_queue_reports_exact_owner_for_open_refusal_and_exit_overflow(staged_bars, monkeypatch):
    monkeypatch.delenv('MMR_AUTO_EXECUTE_DISABLED', raising=False)
    staged_bars._open_view = {OWNER: ENTRY}
    staged_bars._exit_overflow = {}
    messages = []
    monkeypatch.setattr(execution.logging, 'error',
                        lambda message, *args: messages.append(message % args))
    opening = SignalWork(*OWNER, Action.BUY, ENTRY, bar_size_seconds=60)
    closing = SignalWork(*OWNER, Action.SELL, ENTRY, bar_size_seconds=60)
    assert staged_bars.submit_signal(opening) is False
    assert staged_bars.submit_signal(closing) is True
    assert staged_bars._exit_overflow[OWNER] is closing
    assert len(messages) == 2
    for message in messages:
        assert OWNER[0] in message
        assert str(OWNER[1]) in message
    assert 'opening queue full' in messages[0].casefold()
    assert 'overflow' in messages[1].casefold()


def test_kill_switch_notice_names_the_setting_that_suppressed_the_signal(monkeypatch):
    executor = _loop_controller()
    monkeypatch.setenv('MMR_AUTO_EXECUTE_DISABLED', '1')
    messages = []
    monkeypatch.setattr(execution.logging, 'info',
                        lambda message, *args: messages.append(message % args))
    signal = SignalWork(*OWNER, Action.BUY, ENTRY, bar_size_seconds=60)
    assert executor.submit_signal(signal) is False
    assert executor._queue.qsize() == 0
    assert len(messages) == 1
    assert AutoExecutor.KILL_SWITCH_ENV in messages[0]


def test_rejection_audit_is_readable_with_exact_identity_and_reason(tmp_path, monkeypatch):
    executor = AutoExecutor.__new__(AutoExecutor)
    path = str(tmp_path / 'rejection-audit.duckdb')
    executor.event_store = EventStore(path)
    failures = []
    monkeypatch.setattr(execution.logging, 'exception', lambda *args, **kwargs: failures.append(args))
    before = dt.datetime.now()
    executor._append_event(EventType.ORDER_REJECTED, 'audit_contract', 9876,
                           'BUY', 17.5, 'risk gate refused opening exposure')
    after = dt.datetime.now()

    # Reopen the real store; capturing a helper call alone would miss a
    # malformed event that the persistent audit writer refuses.
    events = EventStore(path).query_all()
    assert failures == []
    assert len(events) == 1
    event = events[0]
    assert event.event_type == EventType.ORDER_REJECTED
    assert before <= event.timestamp <= after
    assert (event.strategy_name, event.conid, event.action, event.quantity) == (
        'audit_contract', 9876, 'BUY', 17.5)
    assert event.metadata == {'note': 'risk gate refused opening exposure'}


def test_unresolved_ownership_warning_is_exact_and_deduplicated_per_identity(monkeypatch):
    executor = AutoExecutor.__new__(AutoExecutor)
    executor._ownership_warnings = set()
    messages = []

    def warning(message, *args, **kwargs):
        messages.append(message % args)

    monkeypatch.setattr(execution.logging, 'warning', warning)
    identities = [
        ('first_owner', 1111, 'first-intent'),
        ('second_owner', 1111, 'first-intent'),
        ('first_owner', 2222, 'first-intent'),
        ('first_owner', 1111, 'second-intent'),
    ]
    for identity in identities:
        executor._warn_ownership_pending(*identity)
        executor._warn_ownership_pending(*identity)
    assert len(messages) == len(identities)
    for (strategy, conid, identity), message in zip(identities, messages):
        assert strategy in message
        assert str(conid) in message
        assert identity in message
        assert 'ownership proof unavailable' in message.casefold()
        assert 'existing working protection is retained' in message.casefold()


@pytest.mark.parametrize('suffix', ['', 'a' * 31, 'a' * 33, 'a' * 31 + 'X', 'A' * 32])
def test_recovered_emergency_identity_requires_exact_hex_epoch(suffix):
    assert AutoExecutor._emergency_epoch('emergency-recovery-epoch-' + suffix) is None


def test_native_emergency_epoch_roundtrips_with_bounded_long_parent_reference():
    epoch = '0123456789abcdef' * 2
    identity = AutoExecutor._emergency_identity('parent-' + 'a' * 512, epoch)
    assert len(identity.encode()) <= 160
    assert AutoExecutor._emergency_epoch(identity) == epoch


from types import SimpleNamespace

import pandas as pd

from review.test_review_strategy_contract import TS, make_work, runtime_and_strategy
from test_execution_recovery import recovery


@pytest.fixture
def signal_admission(recovery, monkeypatch):
    manager, sdk, path = recovery
    manager.cooldown_seconds = 0
    contexts = []
    ctx = SimpleNamespace(manager=manager, sdk=sdk, path=path,
                          monkeypatch=monkeypatch, contexts=contexts)
    try:
        yield ctx
    finally:
        for runtime in contexts:
            for strategy in runtime.strategy_implementations:
                runtime._stop_callback_worker(strategy)
        manager.intents.journal.close()


def _admission_second_instrument(ctx):
    second = SimpleNamespace(**dict(vars(ctx.sdk.secdef), symbol='SECOND', conId=2222))
    original = ctx.sdk.resolve

    def resolve(symbol, **kwargs):
        if symbol in (2222, 'SECOND'):
            return [second]
        return original(symbol, **kwargs)

    ctx.monkeypatch.setattr(ctx.sdk, 'resolve', resolve)


def _admission_stop_after_proposal_request(ctx):
    # Observe the actual SDK proposal request without inventing an acceptance
    # or a fill for a second instrument/owner in the historical SDK adapter.
    propose = ctx.sdk.propose

    def unavailable_reply(**kwargs):
        propose(**kwargs)
        raise ConnectionError('proposal reply unavailable at the admission observation boundary')

    ctx.monkeypatch.setattr(ctx.sdk, 'propose', unavailable_reply)


def _admission_audit(ctx, work):
    return ctx.manager.state.db.execute(
        'SELECT strategy, conid, bar_ts, action, decision, reason '
        'FROM auto_exec_bar_log WHERE strategy=? AND conid=? AND bar_ts=?',
        [work.strategy_name, work.conid, work.bar_ts.to_pydatetime()], fetch='all')


@pytest.mark.parametrize('authorized', [True, False], ids=['current-generation', 'revoked-generation'])
def test_native_deployment_authority_precedes_proposal_creation(
        signal_admission, tmp_path, authorized):
    ctx = signal_admission
    runtime, _ = runtime_and_strategy(tmp_path)
    ctx.contexts.append(runtime)
    strategy = runtime.get_strategy('review')
    strategy.ctx.auto_execute = True
    strategy.enable()
    runtime._grant_generation(strategy)
    generation = strategy.ctx.deployment_generation
    assert runtime._opening_authorized('review', generation)
    if not authorized:
        runtime._revoke_opening(strategy)
    ctx.manager.authority_check = runtime._opening_authorized
    _admission_stop_after_proposal_request(ctx)
    work = make_work(strategy_name='review', quantity=10,
                     deployment_generation=generation)

    ctx.manager._process_signal(work)

    assert len(ctx.sdk.propose_calls) == int(authorized)
    assert ctx.sdk.approve_calls == []
    if authorized:
        call, = ctx.sdk.propose_calls
        assert call['action'] == 'BUY'
        assert call['metadata']['strategy'] == 'review'
        assert call['metadata']['conid'] == 1111
    else:
        assert ctx.manager.intents.all(strategy='review', kind='OPEN') == []
        row, = _admission_audit(ctx, work)
        assert row[:5] == ('review', 1111, work.bar_ts.to_pydatetime(), 'BUY', 'refused')
        assert isinstance(row[5], str)
        assert all(fact in row[5].casefold() for fact in ('deployment', 'authority', 'revoked'))


def test_unfilled_open_reserves_only_its_own_instrument(signal_admission):
    ctx = signal_admission
    ctx.sdk.fill_next = 0
    ctx.manager._process_signal(make_work(quantity=40))
    first, = ctx.manager.intents.all(kind='OPEN')
    assert first['status'] == 'WORKING'
    assert ctx.manager.state.open_position('orb_test', 1111) is None
    _admission_second_instrument(ctx)
    _admission_stop_after_proposal_request(ctx)
    work = make_work(conid=2222, quantity=10, bar_ts=TS + pd.Timedelta(seconds=1))

    ctx.manager._process_signal(work)

    assert len(ctx.sdk.propose_calls) == 2
    assert len(ctx.sdk.approve_calls) == 1
    assert ctx.sdk.propose_calls[-1]['metadata']['conid'] == 2222
    assert ctx.sdk.propose_calls[-1]['symbol'] == 'SECOND'
    assert ctx.sdk.broker[1111] == 0
    still, = ctx.manager.intents.all(strategy='orb_test', conid=1111, kind='OPEN')
    assert still['intent_id'] == first['intent_id']
    assert still['status'] == 'WORKING'


@pytest.mark.parametrize(('owner', 'conid'), [('independent_owner', 1111), ('orb_test', 2222)],
                         ids=['different-owner', 'different-instrument'])
def test_terminal_close_with_unknown_opening_does_not_reserve_another_namespace(
        signal_admission, owner, conid):
    ctx = signal_admission
    ctx.sdk.broker[1111] = 100
    ctx.sdk.fill_next = 40
    ctx.manager._process_signal(make_work(quantity=140))
    opening = ctx.sdk.accepted[0]
    opening.update(status='Cancelled', fillQuantityKnown=False)
    ctx.sdk.fill_next = None
    ctx.manager._process_signal(make_work(action=Action.SELL,
                                          bar_ts=TS + pd.Timedelta(seconds=1)))
    ctx.manager.manage_positions()
    assert ctx.manager.state.open_position('orb_test', 1111) is None
    pending_open, = ctx.manager.intents.all(strategy='orb_test', conid=1111, kind='OPEN', active=True)
    assert pending_open['status'] == 'UNKNOWN'
    closing, = ctx.manager.intents.all(strategy='orb_test', conid=1111, kind='CLOSE')
    assert closing['status'] == 'FILLED'
    assert pending_open['intent_id'] in closing['payload']['explicit_exit_scope']['openings']
    assert ctx.sdk.broker[1111] == 100
    _admission_second_instrument(ctx)
    _admission_stop_after_proposal_request(ctx)
    work = make_work(strategy_name=owner, conid=conid, quantity=10,
                     bar_ts=TS + pd.Timedelta(seconds=2))

    ctx.manager._process_signal(work)

    assert len(ctx.sdk.propose_calls) == 3
    assert len(ctx.sdk.approve_calls) == 2
    assert ctx.sdk.propose_calls[-1]['metadata']['strategy'] == owner
    assert ctx.sdk.propose_calls[-1]['metadata']['conid'] == conid
    assert ctx.sdk.broker[1111] == 100
    retained, = ctx.manager.intents.all(strategy='orb_test', conid=1111, kind='CLOSE')
    assert retained['intent_id'] == closing['intent_id']
    assert retained['payload']['exit_request_active'] is True


def test_working_close_blocks_a_pyramid_and_explains_the_reservation(signal_admission):
    ctx = signal_admission
    ctx.sdk.broker[1111] = 100
    ctx.manager._process_signal(make_work(quantity=40))
    ctx.sdk.fill_next = 0
    ctx.manager._process_signal(make_work(action=Action.SELL,
                                          bar_ts=TS + pd.Timedelta(seconds=1)))
    closing, = ctx.manager.intents.all(kind='CLOSE', active=True)
    assert closing['status'] == 'WORKING'
    assert ctx.manager.state.open_position('orb_test', 1111)['quantity'] == 40
    _admission_stop_after_proposal_request(ctx)
    work = make_work(quantity=10, pyramid_max_adds=1,
                     bar_ts=TS + pd.Timedelta(seconds=2))

    ctx.manager._process_signal(work)

    assert len(ctx.sdk.propose_calls) == 2
    assert len(ctx.sdk.approve_calls) == 2
    assert ctx.sdk.broker[1111] == 140
    assert ctx.manager.state.open_position('orb_test', 1111)['quantity'] == 40
    row, = _admission_audit(ctx, work)
    assert row[:5] == ('orb_test', 1111, work.bar_ts.to_pydatetime(), 'BUY', 'skip')
    assert isinstance(row[5], str)
    assert all(fact in row[5].casefold() for fact in ('unresolved', 'intent', 'reserves', 'exposure'))


def test_runtime_interval_conversion_failure_is_refused_by_the_real_worker(
        signal_admission, tmp_path):
    from trader.objects import BarSize
    from trader.trading.strategy import Signal

    ctx = signal_admission
    runtime, _ = runtime_and_strategy(tmp_path)
    ctx.contexts.append(runtime)
    strategy = runtime.get_strategy('review')
    strategy.ctx.auto_execute = True
    strategy.enable()
    runtime._grant_generation(strategy)
    ctx.manager.authority_check = runtime._opening_authorized
    captured = []
    runtime.auto_executor = SimpleNamespace(submit_signal=captured.append)
    _admission_stop_after_proposal_request(ctx)

    def conversion_unavailable(value):
        assert value == BarSize.Mins1
        raise ValueError('bar interval conversion unavailable')

    # Exercise the runtime's actual conversion-failure fallback. Ordinary
    # supported enums convert successfully; this does not claim otherwise.
    with ctx.monkeypatch.context() as patch:
        patch.setattr(BarSize, 'to_pandas_freq', conversion_unavailable)
        signal = Signal(source_name='review', action=Action.BUY,
                        probability=0.6, risk=0.4, quantity=10)
        runtime._submit_auto_execution(strategy, 1111, signal, TS)
    work, = captured
    assert work.bar_size_seconds == 0.0
    assert work.auto_execute and work.state_running

    ctx.manager._process_signal(work)

    assert ctx.sdk.propose_calls == []
    assert ctx.sdk.approve_calls == []
    assert ctx.manager.intents.all(kind='OPEN') == []
    # The runtime emits an aware session timestamp; the store normalizes it
    # to UTC-naive TIMESTAMP. Query the sole owner row without a timezone-
    # dependent aware/naive SQL comparison, then verify its actual instant.
    row, = ctx.manager.state.db.execute(
        'SELECT strategy, conid, bar_ts, action, decision, reason '
        'FROM auto_exec_bar_log WHERE strategy=? AND conid=?',
        [work.strategy_name, work.conid], fetch='all')
    assert row[:5] == ('review', 1111, TS.to_pydatetime(), 'BUY', 'skip')
    assert 'interval' in row[5] and 'fail-closed' in row[5]


@pytest.mark.parametrize('value', ['nan', 'inf', '1e308'],
                         ids=['nan', 'infinity', 'finite-threshold-overflow'])
def test_nonfinite_freshness_configuration_cannot_admit_a_stale_worker_open(
        signal_admission, value):
    ctx = signal_admission
    ctx.monkeypatch.setenv('MMR_STALE_BAR_MULTIPLE', value)
    _admission_stop_after_proposal_request(ctx)
    work = make_work(quantity=10, bar_ts=TS - pd.Timedelta(minutes=10))

    ctx.manager._process_signal(work)

    assert ctx.sdk.propose_calls == []
    assert ctx.sdk.approve_calls == []
    assert ctx.manager.intents.all(kind='OPEN') == []
    row, = _admission_audit(ctx, work)
    assert row[:5] == ('orb_test', 1111, work.bar_ts.to_pydatetime(), 'BUY', 'skip')
    assert 'stale_bar' in row[5]


@pytest.mark.parametrize('interval, setting', [(0.0, 'nan'), (float('inf'), 'inf')],
                         ids=['unknown-interval', 'nonfinite-interval'])
def test_invalid_freshness_still_closes_owned_shares_and_preserves_manual_inventory(
        signal_admission, interval, setting):
    ctx = signal_admission
    ctx.monkeypatch.setenv('MMR_STALE_BAR_MULTIPLE', '3')
    ctx.sdk.broker[1111] = 100.0
    ctx.manager._process_signal(make_work(quantity=10))
    assert ctx.manager.state.open_position('orb_test', 1111)['quantity'] == 10
    assert ctx.sdk.broker[1111] == 110.0
    ctx.monkeypatch.setenv('MMR_STALE_BAR_MULTIPLE', setting)

    ctx.manager._process_signal(make_work(
        action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1),
        bar_size_seconds=interval))

    assert [call['action'] for call in ctx.sdk.propose_calls] == ['BUY', 'SELL']
    assert ctx.sdk.propose_calls[-1]['quantity'] == 10.0
    assert ctx.sdk.broker[1111] == 100.0
    assert ctx.manager.state.open_position('orb_test', 1111) is None
    assert ctx.sdk.active_stops == {}
