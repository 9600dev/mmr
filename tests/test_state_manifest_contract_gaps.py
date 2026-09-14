"""Persist the latest add's bar and return the documented manifest decision.

Private draft only; not executed during the frozen canonical mutation run.
"""
import datetime as dt

import pytest

from review.test_review_strategy_contract import make_work
from trader.strategy.auto_executor import AutoExecState, Directive, check_manifest


@pytest.mark.parametrize('add_bar', [
    dt.datetime(2026, 9, 9, 10, 0),
    dt.datetime(2026, 9, 9, 15, 30, tzinfo=dt.timezone(dt.timedelta(hours=5, minutes=30))),
])
def test_compatibility_add_retains_its_actual_utc_bar_across_restart(tmp_path, add_bar):
    path = str(tmp_path / 'state.duckdb')
    state = AutoExecState(path)
    state.record_open('clock_test', 1111, 10, dt.datetime(2026, 9, 8, 10), 7, None, None)
    original = state.open_position('clock_test', 1111)
    state.record_add('clock_test', 1111, 12, add_bar, 8, dt.time(15, 45), 60)

    position = AutoExecState(path).open_position('clock_test', 1111)

    assert position['entry_bar_ts'] == dt.datetime(2026, 9, 9, 10, 0)
    assert position['quantity'] == 22
    assert position['lots'] == 2
    assert position['proposal_id'] == 8
    assert position['close_by_time'] == dt.time(15, 45)
    assert position['max_hold_bars'] == 60
    assert position['ownership_epoch'] == original['ownership_epoch']
    assert position['ownership_started_at'] == original['ownership_started_at']


@pytest.mark.parametrize(('past_opens', 'allowed'), [(1, True), (2, False), (3, False)])
def test_hourly_manifest_limit_returns_an_explicit_refusal_at_and_above_limit(past_opens, allowed):
    work = make_work(manifest_max_opens_per_hour=2)
    opening = Directive('open', 'eligible long entry', quantity=10)

    result = check_manifest(work, opening, opens_today=past_opens, opens_hour=past_opens)

    if allowed:
        assert result is None
    else:
        assert result is not None
        assert result.kind == 'refused'
        assert result.quantity is None


# Real local execution stores and submission producers, with the existing
# isolated SDK adapter. These controls do not contact a broker or claim native
# IB timing. Only the runtime's wall clocks move; timeout/monotonic clocks do not.
import time as _manifest_real_time
from types import SimpleNamespace

import duckdb
import pandas as pd

import trader.strategy.auto_executor as _manifest_auto
import trader.strategy.execution_intents as _manifest_intents
from review.test_review_strategy_contract import TS
from test_execution_recovery import recovery
from trader.objects import Action
from trader.strategy.auto_executor import AutoExecutor


@pytest.fixture
def manifest_workflow(recovery, monkeypatch):
    executor, sdk, path = recovery
    executor.cooldown_seconds = 0
    monkeypatch.setenv('MMR_PROTECTIVE_STOP_PCT', '0')
    clock = SimpleNamespace(now=dt.datetime(2026, 9, 10, 12).timestamp())
    real_datetime = dt.datetime

    class RuntimeDateTime(real_datetime):
        @classmethod
        def now(cls, tz=None):
            return real_datetime.fromtimestamp(clock.now, tz)

    datetime_api = SimpleNamespace(**{
        name: getattr(dt, name) for name in dir(dt) if not name.startswith('__')})
    datetime_api.datetime = RuntimeDateTime
    time_api = SimpleNamespace(**{
        name: getattr(_manifest_real_time, name) for name in dir(_manifest_real_time)
        if not name.startswith('__')})
    time_api.time = lambda: clock.now
    monkeypatch.setattr(_manifest_auto, 'dt', datetime_api)
    monkeypatch.setattr(_manifest_auto, 'time', time_api)
    monkeypatch.setattr(_manifest_intents, 'time', time_api)
    managers = [executor]
    ctx = SimpleNamespace(executor=executor, sdk=sdk, path=path, clock=clock,
                          managers=managers, monkeypatch=monkeypatch, next_bar=0)
    try:
        yield ctx
    finally:
        for manager in managers:
            manager.intents.journal.close()


def _manifest_actual_open(ctx, *, submitted_at, acknowledged_at=None,
                          lost_reply=False, owner='orb_test', add=False):
    """Use real signal/create/submit/approve handling; never insert an intent."""
    ctx.clock.now = submitted_at
    before = {item['intent_id'] for item in ctx.executor.intents.all(kind='OPEN')}
    approve = ctx.sdk.approve
    previous_fill = ctx.sdk.fill_next

    def timed_approval(proposal_id):
        if acknowledged_at is not None:
            ctx.clock.now = acknowledged_at
        if lost_reply:
            ctx.sdk.fill_next = 0
        result = approve(proposal_id)
        if lost_reply:
            raise ConnectionError('opening approval reply lost after server acceptance')
        return result

    work = make_work(strategy_name=owner, quantity=10,
                     pyramid_max_adds=1 if add else 0,
                     bar_ts=TS + pd.Timedelta(seconds=ctx.next_bar * 10))
    ctx.next_bar += 1
    try:
        with ctx.monkeypatch.context() as fault:
            fault.setattr(ctx.sdk, 'approve', timed_approval)
            ctx.executor._process_signal(work)
    finally:
        ctx.sdk.fill_next = previous_fill
    created = [item for item in ctx.executor.intents.all(kind='OPEN')
               if item['intent_id'] not in before]
    assert len(created) == 1
    return created[0]


def _manifest_restart(ctx):
    ctx.executor.intents.journal.close()
    manager = AutoExecutor(ctx.path, paper_trading=True, cooldown_seconds=0,
                           sdk_factory=lambda: ctx.sdk)
    ctx.managers.append(manager)
    ctx.executor = manager
    return manager


def _manifest_candidate_gate(ctx, **manifest):
    # The unresolved first instrument must not mask this strategy-wide gate.
    # No third proposal or broker fill is manufactured for the second conId.
    work = make_work(conid=2222, quantity=10, **manifest)
    return ctx.executor._manifest_gate(work, Directive('open', 'eligible long entry', quantity=10))


@pytest.mark.parametrize(('field', 'window_seconds'), [
    ('manifest_max_opens_per_day', 86400),
    ('manifest_max_opens_per_hour', 3600),
])
def test_manifest_counts_distinct_recent_acknowledgement_and_unknown_submission_after_restart(
        manifest_workflow, field, window_seconds):
    ctx = manifest_workflow
    gate_at = ctx.clock.now
    cutoff = gate_at - window_seconds
    # A was submitted before the window and acknowledged inside it. The
    # documented created-time bar log therefore still charges this open.
    first = _manifest_actual_open(ctx, submitted_at=cutoff - 1, acknowledged_at=cutoff + 1)
    # B is a real pyramid submission whose accepted, unfilled reply is lost.
    # Its durable submission reserves turnover without an acknowledged-open log.
    second = _manifest_actual_open(ctx, submitted_at=gate_at - 5, lost_reply=True, add=True)
    ctx.clock.now = gate_at

    assert first['status'] == 'FILLED'
    assert first['payload']['submitted_at'] < cutoff
    assert second['status'] == 'UNKNOWN'
    assert second['payload']['submitted_at'] >= cutoff
    assert first['intent_id'] != second['intent_id']
    assert len(ctx.sdk.propose_calls) == len(ctx.sdk.approve_calls) == 2
    assert ctx.sdk.broker[1111] == 10
    recent_logs = ctx.executor.state.db.execute(
        "SELECT created FROM auto_exec_bar_log WHERE strategy=? AND decision='open' AND created>=?",
        ['orb_test', dt.datetime.fromtimestamp(cutoff)], fetch='all')
    assert len(recent_logs) == 1

    _manifest_restart(ctx)
    durable = {item['intent_id']: item for item in ctx.executor.intents.all(kind='OPEN')}
    assert set(durable) == {first['intent_id'], second['intent_id']}
    assert durable[second['intent_id']]['status'] == 'UNKNOWN'
    before_calls = (len(ctx.sdk.propose_calls), len(ctx.sdk.approve_calls))

    result = _manifest_candidate_gate(ctx, **{field: 2})

    assert result is not None, 'two distinct past opening orders must exhaust a cap of two'
    assert result.kind == 'refused'
    assert result.quantity is None
    assert (len(ctx.sdk.propose_calls), len(ctx.sdk.approve_calls)) == before_calls


@pytest.mark.parametrize(("field", "window_seconds"), [
    pytest.param("manifest_max_opens_per_hour", 3600, id="hourly"),
    pytest.param("manifest_max_opens_per_day", 86400, id="daily"),
])
def test_manifest_retains_late_acknowledgement_when_success_log_write_fails(
        manifest_workflow, field, window_seconds):
    ctx = manifest_workflow
    gate_at = ctx.clock.now
    cutoff = gate_at - window_seconds
    failure = OSError('successful opening audit write unavailable')
    log_decision = ctx.executor.state.log_decision

    def fail_only_success_log(strategy, conid, bar_ts, action, decision, reason, **kwargs):
        if decision == 'open':
            raise failure
        return log_decision(strategy, conid, bar_ts, action, decision, reason, **kwargs)

    with ctx.monkeypatch.context() as fault:
        fault.setattr(ctx.executor.state, 'log_decision', fail_only_success_log)
        with pytest.raises(OSError) as raised:
            _manifest_actual_open(ctx, submitted_at=cutoff - 1, acknowledged_at=cutoff + 1)
    ctx.clock.now = gate_at
    original, = ctx.executor.intents.all(kind='OPEN')
    assert raised.value is failure
    assert original['status'] == 'WORKING'
    assert original['payload']['order_ids']
    assert original['payload']['submitted_at'] < cutoff
    assert ctx.sdk.broker[1111] == 10
    assert ctx.executor.state.count_opens_since('orb_test', dt.datetime.fromtimestamp(cutoff)) == 0

    _manifest_restart(ctx)
    restored, = ctx.executor.intents.all(kind='OPEN')
    assert restored['intent_id'] == original['intent_id']
    assert restored['status'] == 'WORKING'

    result = _manifest_candidate_gate(ctx, **{field: 1})

    assert result is not None, 'durable successful acknowledgement must survive its missing audit row'
    assert result.kind == 'refused'
    assert result.quantity is None
    assert len(ctx.sdk.propose_calls) == len(ctx.sdk.approve_calls) == 1


def test_manifest_absence_does_not_require_an_execution_history_read(manifest_workflow):
    ctx = manifest_workflow

    def unavailable_history(*args, **kwargs):
        raise OSError('unneeded manifest history read')

    with ctx.monkeypatch.context() as fault:
        fault.setattr(ctx.executor.intents, 'all', unavailable_history)
        fault.setattr(ctx.executor.state, 'count_opens_since', unavailable_history)
        result = _manifest_candidate_gate(ctx)

    assert result is None
    assert ctx.sdk.propose_calls == []


def test_manifest_long_declaration_accepts_an_ordinary_buy(manifest_workflow):
    ctx = manifest_workflow

    result = _manifest_candidate_gate(ctx, manifest_direction='long')

    assert result is None
    assert ctx.sdk.propose_calls == []


@pytest.mark.parametrize(('field', 'window_seconds'), [
    ('manifest_max_opens_per_day', 86400),
    ('manifest_max_opens_per_hour', 3600),
])
@pytest.mark.parametrize('age_beyond_window', [-30.0, 0.0, 0.5])
def test_manifest_unknown_submission_uses_inclusive_exact_rolling_window(
        manifest_workflow, field, window_seconds, age_beyond_window):
    ctx = manifest_workflow
    gate_at = ctx.clock.now
    submitted_at = gate_at - window_seconds - age_beyond_window
    pending = _manifest_actual_open(ctx, submitted_at=submitted_at, lost_reply=True)
    ctx.clock.now = gate_at
    assert pending['status'] == 'UNKNOWN'
    assert pending['payload']['submitted_at'] == submitted_at
    assert ctx.sdk.broker.get(1111, 0) == 0
    assert ctx.executor.state.count_opens_since('orb_test', dt.datetime.fromtimestamp(0)) == 0

    result = _manifest_candidate_gate(ctx, **{field: 1})

    if age_beyond_window > 0:
        assert result is None
    else:
        assert result is not None
        assert result.kind == 'refused'
        assert result.quantity is None
    assert len(ctx.sdk.propose_calls) == len(ctx.sdk.approve_calls) == 1


@pytest.mark.parametrize(('field', 'window_seconds'), [
    ('manifest_max_opens_per_day', 86400),
    ('manifest_max_opens_per_hour', 3600),
])
@pytest.mark.parametrize('ack_age_beyond_window', [-30.0, 30.0])
def test_manifest_acknowledged_open_uses_its_persisted_rolling_window(
        manifest_workflow, field, window_seconds, ack_age_beyond_window):
    ctx = manifest_workflow
    gate_at = ctx.clock.now
    first = _manifest_actual_open(
        ctx, submitted_at=gate_at - window_seconds - 120,
        acknowledged_at=gate_at - window_seconds - ack_age_beyond_window)
    ctx.clock.now = gate_at
    assert first['status'] == 'FILLED'
    assert first['payload']['submitted_at'] < gate_at - window_seconds
    assert ctx.sdk.broker[1111] == 10

    result = _manifest_candidate_gate(ctx, **{field: 1})

    if ack_age_beyond_window > 0:
        assert result is None
    else:
        assert result is not None
        assert result.kind == 'refused'
        assert result.quantity is None
    assert len(ctx.sdk.propose_calls) == len(ctx.sdk.approve_calls) == 1


def test_manifest_created_before_submission_has_no_turnover_stamp(manifest_workflow):
    ctx = manifest_workflow
    crash = RuntimeError('process interrupted before submitting the durable opening')

    def before_submission(intent):
        raise crash

    with ctx.monkeypatch.context() as fault:
        fault.setattr(ctx.executor, '_submit_intent', before_submission)
        with pytest.raises(RuntimeError) as raised:
            ctx.executor._process_signal(make_work(quantity=10))
    pending, = ctx.executor.intents.all(kind='OPEN')
    assert raised.value is crash
    assert pending['status'] == 'CREATED'
    assert 'submitted_at' not in pending['payload']
    assert ctx.sdk.propose_calls == []
    assert ctx.sdk.approve_calls == []

    result = _manifest_candidate_gate(ctx, manifest_max_opens_per_day=1,
                                      manifest_max_opens_per_hour=1)

    assert result is None


def test_manifest_other_strategy_open_does_not_spend_this_strategy_budget(manifest_workflow):
    ctx = manifest_workflow
    other = _manifest_actual_open(ctx, submitted_at=ctx.clock.now, owner='independent_owner')
    assert other['strategy'] == 'independent_owner'
    assert other['status'] == 'FILLED'
    assert ctx.executor.state.open_position('independent_owner', 1111)['quantity'] == 10

    result = _manifest_candidate_gate(ctx, manifest_max_opens_per_day=1,
                                      manifest_max_opens_per_hour=1)

    assert result is None
    assert len(ctx.sdk.propose_calls) == len(ctx.sdk.approve_calls) == 1


def test_manifest_native_close_never_spends_a_second_opening_unit(manifest_workflow):
    ctx = manifest_workflow
    gate_at = ctx.clock.now
    _manifest_actual_open(ctx, submitted_at=gate_at - 30)
    ctx.clock.now = gate_at - 20
    ctx.executor._process_signal(make_work(action=Action.SELL,
        bar_ts=TS + pd.Timedelta(seconds=ctx.next_bar * 10)))
    ctx.clock.now = gate_at
    closing, = ctx.executor.intents.all(kind='CLOSE')
    assert closing['status'] == 'FILLED'
    assert closing['payload']['submitted_at'] >= gate_at - 3600
    assert ctx.sdk.broker[1111] == 0
    assert ctx.executor.state.open_position('orb_test', 1111) is None

    result = _manifest_candidate_gate(ctx, manifest_max_opens_per_day=2,
                                      manifest_max_opens_per_hour=2)

    assert result is None
    assert [call['action'] for call in ctx.sdk.propose_calls] == ['BUY', 'SELL']


def test_manifest_preapproval_refusal_never_spends_opening_budget(manifest_workflow):
    ctx = manifest_workflow
    failure = ValueError('proposal input refused before broker approval')

    def reject_proposal(**kwargs):
        raise failure

    with ctx.monkeypatch.context() as fault:
        fault.setattr(ctx.sdk, 'propose', reject_proposal)
        rejected = _manifest_actual_open(ctx, submitted_at=ctx.clock.now)
    assert rejected['status'] == 'REJECTED'
    assert rejected['payload']['submitted_at'] == ctx.clock.now
    assert ctx.sdk.approve_calls == []
    assert ctx.executor.state.count_opens_since('orb_test', dt.datetime.fromtimestamp(0)) == 0

    result = _manifest_candidate_gate(ctx, manifest_max_opens_per_day=1,
                                      manifest_max_opens_per_hour=1)

    assert result is None


def test_manifest_acknowledged_identity_counts_once_before_and_after_restart(manifest_workflow):
    ctx = manifest_workflow
    original = _manifest_actual_open(ctx, submitted_at=ctx.clock.now - 30)
    assert original['status'] == 'FILLED'

    for after_restart in (False, True):
        if after_restart:
            _manifest_restart(ctx)
        assert _manifest_candidate_gate(ctx, manifest_max_opens_per_day=2,
                                         manifest_max_opens_per_hour=2) is None
        refused = _manifest_candidate_gate(ctx, manifest_max_opens_per_day=1,
                                           manifest_max_opens_per_hour=1)
        assert refused is not None and refused.kind == 'refused'
        assert refused.quantity is None
        saved, = ctx.executor.intents.all(kind='OPEN')
        assert saved['intent_id'] == original['intent_id']
    assert len(ctx.sdk.propose_calls) == len(ctx.sdk.approve_calls) == 1


@pytest.mark.parametrize("manifest_limits", [
    pytest.param(("manifest_max_opens_per_day", "manifest_max_opens_per_hour"), id="both"),
    pytest.param(("manifest_max_opens_per_day",), id="daily"),
    pytest.param(("manifest_max_opens_per_hour",), id="hourly"),
])
def test_manifest_migrated_unkeyed_logs_are_not_guessed_to_be_one_order(
        manifest_workflow, tmp_path, manifest_limits):
    ctx = manifest_workflow
    path = str(tmp_path / 'legacy_manifest.duckdb')
    # Exact historical seven-column schema. Identical presentation/bar fields
    # are not a durable order identity, so neither legacy row may be discarded.
    with duckdb.connect(path) as connection:
        connection.execute('''CREATE TABLE auto_exec_bar_log (
            strategy VARCHAR NOT NULL, conid BIGINT NOT NULL,
            bar_ts TIMESTAMP NOT NULL, action VARCHAR NOT NULL,
            decision VARCHAR NOT NULL, reason VARCHAR, created TIMESTAMP NOT NULL)''')
        row = ['orb_test', 1111, TS.to_pydatetime(), 'BUY', 'open',
               'legacy acknowledged opening', dt.datetime.fromtimestamp(ctx.clock.now - 10)]
        connection.executemany('INSERT INTO auto_exec_bar_log VALUES (?, ?, ?, ?, ?, ?, ?)', [row, row])
    manager = AutoExecutor(path, paper_trading=True, cooldown_seconds=0, sdk_factory=lambda: ctx.sdk)
    ctx.managers.append(manager)
    ctx.executor, ctx.path = manager, path
    assert ctx.executor.intents.all(kind='OPEN') == []

    for after_restart in (False, True):
        if after_restart:
            _manifest_restart(ctx)
        if len(manifest_limits) == 2:
            refused = _manifest_candidate_gate(ctx, manifest_max_opens_per_day=2,
                                               manifest_max_opens_per_hour=2)
        else:
            refused = _manifest_candidate_gate(ctx, **{field: 2 for field in manifest_limits})
        assert refused is not None and refused.kind == 'refused'
        assert refused.quantity is None
        if len(manifest_limits) == 2:
            assert _manifest_candidate_gate(ctx, manifest_max_opens_per_day=3,
                                             manifest_max_opens_per_hour=3) is None
        else:
            assert _manifest_candidate_gate(ctx, **{field: 3 for field in manifest_limits}) is None
    assert ctx.sdk.propose_calls == []
    assert ctx.sdk.approve_calls == []
