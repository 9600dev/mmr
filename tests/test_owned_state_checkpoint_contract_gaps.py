"""Owned inventory, durable fill progress, and restart views stay consistent.

Private source-driven draft. These function verdicts are still pending; no
survivor or mutant-kill claim accompanies these tests. Every database is local
to tmp_path and no SDK or broker endpoint is used.
"""
import datetime as dt
import queue
import threading
from types import SimpleNamespace

import pytest

from trader.strategy.auto_executor import AutoExecState, AutoExecutor
from trader.strategy.execution_intents import timestamp_text


BAR = dt.datetime(2026, 9, 9, 10, tzinfo=dt.timezone.utc)
ORIGIN = BAR.timestamp() - 30


@pytest.fixture
def owned_store(tmp_path):
    path = str(tmp_path / 'owned.duckdb')
    return SimpleNamespace(path=path, state=AutoExecState(path))


def _opening(identity='open-a', *, strategy='alpha', conid=1111, bar=BAR, **payload):
    return dict(intent_id=identity, strategy=strategy, conid=conid, kind='OPEN',
                payload=dict(bar_ts=timestamp_text(bar), quantity=999,
                             proposal_id=701, intent_created_at=ORIGIN, **payload))


def _reduction(state, identity='close-a', *, kind='CLOSE', strategy='alpha', conid=1111, **payload):
    position = state.open_position(strategy, conid)
    return dict(intent_id=identity, strategy=strategy, conid=conid, kind=kind,
                payload=dict(bar_ts=timestamp_text(BAR), quantity=999,
                             proposal_id=702, reason='confirmed reduction',
                             ownership_epoch=position['ownership_epoch'],
                             ownership_started_at=position['ownership_started_at'], **payload))


def _progress(state, identity):
    row = state.db.execute('SELECT quantity FROM auto_exec_fill_progress WHERE intent_id=?',
                           [identity], fetch='one')
    return None if row is None else row[0]


def test_open_progress_ignores_requested_quantity_and_never_moves_backwards(owned_store):
    ctx = owned_store
    intent = _opening()
    assert ctx.state.apply_fill(intent, 0) == 0
    assert ctx.state.open_position('alpha', 1111) is None
    assert _progress(ctx.state, intent['intent_id']) is None
    assert ctx.state.apply_fill(intent, 12) == 12
    first = ctx.state.open_position('alpha', 1111)
    assert first['quantity'] == 12
    assert first['lots'] == 1
    restarted = AutoExecState(ctx.path)
    assert restarted.apply_fill(intent, 7) == 0
    assert restarted.apply_fill(intent, 12) == 0
    assert restarted.open_position('alpha', 1111) == first
    assert _progress(restarted, intent['intent_id']) == 12
    assert restarted.apply_fill(intent, 20) == 8
    assert restarted.open_position('alpha', 1111)['quantity'] == 20
    assert restarted.open_position('alpha', 1111)['lots'] == 1
    assert _progress(restarted, intent['intent_id']) == 20


@pytest.mark.parametrize('kind', ['CLOSE', 'PROTECTIVE'])
def test_reducing_progress_is_exactly_scoped_and_replayed_once(owned_store, kind):
    ctx = owned_store
    a = _opening()
    b = _opening('other-instrument', conid=2222)
    c = _opening('other-owner', strategy='beta')
    for opening, quantity in [(a, 40), (b, 70), (c, 90)]:
        ctx.state.apply_fill(opening, quantity)
    intent = _reduction(ctx.state, kind=kind)
    assert ctx.state.apply_fill(intent, 10) == 10
    assert ctx.state.apply_fill(intent, 7) == 0
    restarted = AutoExecState(ctx.path)
    assert restarted.apply_fill(intent, 25) == 15
    assert restarted.apply_fill(intent, 25) == 0
    assert restarted.open_position('alpha', 1111)['quantity'] == 15
    assert restarted.open_position('alpha', 2222)['quantity'] == 70
    assert restarted.open_position('beta', 1111)['quantity'] == 90
    assert _progress(restarted, intent['intent_id']) == 25


@pytest.mark.parametrize('kind', ['CLOSE', 'PROTECTIVE'])
@pytest.mark.parametrize('missing_proof', ['epoch', 'unresolved'])
def test_unproven_reduction_does_not_consume_fill_evidence(owned_store, kind, missing_proof):
    ctx = owned_store
    ctx.state.apply_fill(_opening(), 40)
    before = ctx.state.open_position('alpha', 1111)
    intent = _reduction(ctx.state, kind=kind)
    if missing_proof == 'epoch':
        intent['payload'].pop('ownership_epoch')
    else:
        intent['payload']['attribution_unresolved'] = True
    assert ctx.state.apply_fill(intent, 10) == 0
    assert ctx.state.open_position('alpha', 1111) == before
    assert _progress(ctx.state, intent['intent_id']) is None


@pytest.mark.parametrize('kind', ['CLOSE', 'PROTECTIVE'])
def test_known_old_epoch_receipt_advances_only_its_checkpoint(owned_store, kind):
    ctx = owned_store
    ctx.state.apply_fill(_opening(), 40)
    old = _reduction(ctx.state, kind=kind)
    ctx.state.record_close('alpha', 1111, 'CLOSED_EXTERNALLY', 'earlier holding ended')
    ctx.state.apply_fill(_opening('fresh-entry'), 60)
    current = ctx.state.open_position('alpha', 1111)
    assert current['ownership_epoch'] != old['payload']['ownership_epoch']
    assert ctx.state.apply_fill(old, 10) == 0
    assert _progress(ctx.state, old['intent_id']) == 10
    assert ctx.state.open_position('alpha', 1111) == current
    restarted = AutoExecState(ctx.path)
    assert restarted.apply_fill(old, 15) == 0
    assert restarted.apply_fill(old, 10) == 0
    assert _progress(restarted, old['intent_id']) == 15
    assert restarted.open_position('alpha', 1111) == current


def test_fully_filled_reduction_keeps_terminal_audit_facts_after_open_row_disappears(owned_store):
    ctx = owned_store
    ctx.state.apply_fill(_opening(), 40)
    closing = _reduction(ctx.state)
    assert ctx.state.apply_fill(closing, 40) == 40
    assert ctx.state.open_position('alpha', 1111) is None
    saved = ctx.state.db.execute(
        'SELECT quantity, status, closed_reason, close_proposal_id FROM auto_exec_positions '
        'WHERE strategy=? AND conid=?', ['alpha', 1111], fetch='one')
    assert saved == (0, 'CLOSED', 'confirmed reduction', 702)
    assert _progress(ctx.state, closing['intent_id']) == 40
    assert AutoExecState(ctx.path).apply_fill(closing, 40) == 0


def test_checkpoint_write_failure_rolls_back_the_inventory_change(owned_store, monkeypatch):
    ctx = owned_store
    intent = _opening()
    atomic = ctx.state.db.execute_atomic

    class FailingCheckpoint:
        def __init__(self, connection):
            self.connection = connection

        def execute(self, statement, *args):
            if statement.startswith('INSERT INTO auto_exec_fill_progress'):
                raise RuntimeError('injected checkpoint write failure')
            return self.connection.execute(statement, *args)

    with monkeypatch.context() as fault:
        fault.setattr(ctx.state.db, 'execute_atomic', lambda fn: atomic(
            lambda connection: fn(FailingCheckpoint(connection))))
        with pytest.raises(RuntimeError, match='injected checkpoint write failure'):
            ctx.state.apply_fill(intent, 12)
    assert ctx.state.open_position('alpha', 1111) is None
    assert _progress(ctx.state, intent['intent_id']) is None
    assert ctx.state.apply_fill(intent, 12) == 12
    assert ctx.state.open_position('alpha', 1111)['quantity'] == 12
    assert _progress(ctx.state, intent['intent_id']) == 12


def test_ownership_snapshot_filters_closed_owners_but_includes_all_intent_kinds(owned_store):
    ctx = owned_store
    a = _opening(close_by_time='15:45:00', max_hold_bars=60)
    ctx.state.apply_fill(a, 40)
    close = _reduction(ctx.state)
    ctx.state.apply_fill(close, 10)
    stop = _reduction(ctx.state, 'protect-a', kind='PROTECTIVE')
    ctx.state.apply_fill(stop, 3)
    ctx.state.set_protective('alpha', 1111, 901)
    closed = _opening('closed-owner', strategy='retired', conid=3333)
    ctx.state.apply_fill(closed, 8)
    ctx.state.record_close('retired', 3333, 'CLOSED_EXTERNALLY', 'flat')
    unapplied = _opening('unapplied')

    positions, checkpoints = ctx.state.ownership_snapshot([a, close, stop, closed, unapplied])

    assert len(positions) == 1
    row = positions[0]
    assert (row['strategy'], row['conid'], row['quantity']) == ('alpha', 1111, 27)
    assert row['entry_bar_ts'] == BAR.replace(tzinfo=None)
    assert row['close_by_time'] == dt.time(15, 45)
    assert row['max_hold_bars'] == 60
    assert row['proposal_id'] == 701
    assert row['protective_order_id'] == 901
    assert row['ownership_epoch'] == close['payload']['ownership_epoch']
    assert row['ownership_started_at'] == ORIGIN
    assert checkpoints == {'open-a': 40, 'close-a': 10, 'protect-a': 3}


def test_ownership_snapshot_reads_positions_and_checkpoints_from_one_database_snapshot(owned_store, monkeypatch):
    ctx = owned_store
    intent = _opening()
    ctx.state.apply_fill(intent, 40)
    writer_state = AutoExecState(ctx.path)
    positions_read = threading.Event()
    writer_finished = threading.Event()
    outcome = queue.Queue()
    atomic = ctx.state.db.execute_atomic

    class ObserveFirstRows:
        def __init__(self, cursor):
            self.cursor = cursor

        def fetchall(self):
            rows = self.cursor.fetchall()
            positions_read.set()
            assert writer_finished.wait(5), 'concurrent writer did not finish'
            return rows

    class ReadBarrier:
        def __init__(self, connection):
            self.connection = connection
            self.first = True

        def execute(self, *args):
            result = self.connection.execute(*args)
            if self.first:
                self.first = False
                return ObserveFirstRows(result)
            return result

    def writer():
        try:
            assert positions_read.wait(5), 'snapshot did not reach its first read'
            outcome.put(writer_state.apply_fill(intent, 60))
        except BaseException as exc:
            outcome.put(exc)
        finally:
            writer_finished.set()

    monkeypatch.setattr(ctx.state.db, 'execute_atomic', lambda fn: atomic(
        lambda connection: fn(ReadBarrier(connection))))
    thread = threading.Thread(target=writer)
    thread.start()
    try:
        positions, checkpoints = ctx.state.ownership_snapshot([intent])
    finally:
        positions_read.set()
        thread.join(5)
        assert not thread.is_alive(), 'writer teardown incomplete'
    result = outcome.get_nowait()
    if isinstance(result, BaseException):
        raise result
    assert result == 20
    assert positions[0]['quantity'] == 40
    assert checkpoints == {'open-a': 40}
    assert writer_state.open_position('alpha', 1111)['quantity'] == 60
    assert _progress(writer_state, 'open-a') == 60


@pytest.mark.parametrize(('decision', 'reserved'), [
    ('open', True), ('close', True), ('skip', False), ('refused', False),
])
def test_bar_dedup_survives_restart_and_is_exactly_owner_instrument_and_instant(
        owned_store, decision, reserved):
    ctx = owned_store
    shifted = dt.datetime(2026, 9, 9, 15, 30,
                          tzinfo=dt.timezone(dt.timedelta(hours=5, minutes=30)))
    ctx.state.log_decision('alpha', 1111, shifted, 'BUY', decision, 'observed decision')
    restarted = AutoExecState(ctx.path)
    assert restarted.executed_for_bar('alpha', 1111, BAR) is reserved
    assert restarted.executed_for_bar('beta', 1111, BAR) is False
    assert restarted.executed_for_bar('alpha', 2222, BAR) is False
    assert restarted.executed_for_bar('alpha', 1111, BAR + dt.timedelta(minutes=1)) is False
    row = restarted.db.execute(
        'SELECT strategy, conid, bar_ts, action, decision, reason FROM auto_exec_bar_log', fetch='one')
    assert row == ('alpha', 1111, BAR.replace(tzinfo=None), 'BUY', decision, 'observed decision')


def test_protective_link_updates_only_the_current_exact_owned_row(owned_store):
    ctx = owned_store
    for identity, strategy, conid in [('old', 'alpha', 1111), ('other-instrument', 'alpha', 2222),
                                     ('other-owner', 'beta', 1111)]:
        ctx.state.apply_fill(_opening(identity, strategy=strategy, conid=conid), 10)
    ctx.state.set_protective('alpha', 1111, 900)
    ctx.state.record_close('alpha', 1111, 'CLOSED_EXTERNALLY', 'prior holding')
    ctx.state.apply_fill(_opening('fresh'), 20)
    ctx.state.set_protective('alpha', 1111, 901)
    assert ctx.state.open_position('alpha', 1111)['protective_order_id'] == 901
    assert ctx.state.open_position('alpha', 2222)['protective_order_id'] is None
    assert ctx.state.open_position('beta', 1111)['protective_order_id'] is None
    closed = ctx.state.db.execute(
        "SELECT protective_order_id FROM auto_exec_positions WHERE strategy='alpha' AND conid=1111 "
        "AND status='CLOSED_EXTERNALLY'", fetch='one')
    assert closed == (900,)
    ctx.state.set_protective('alpha', 1111, None)
    assert AutoExecState(ctx.path).open_position('alpha', 1111)['protective_order_id'] is None
    assert {(row[0], row[1], row[2]) for row in ctx.state.all_open()} == {
        ('alpha', 1111, 20), ('alpha', 2222, 10), ('beta', 1111, 10)}


def test_managed_restart_view_uses_inventory_checkpoints_not_journal_acknowledgments(tmp_path, monkeypatch):
    monkeypatch.setattr('trader.strategy.auto_executor.time.time', lambda: ORIGIN + 100)
    executor = AutoExecutor(str(tmp_path / 'restart.duckdb'), paper_trading=True)
    journal = executor.intents
    try:
        opening = journal.create('alpha', 1111, 'OPEN', dict(
            bar_ts=timestamp_text(BAR), quantity=40, proposal_id=701,
            close_by_time='15:45:00', max_hold_bars=60,
            bar_size_seconds=300, session_tz='America/New_York',
            cumulative_filled=0, submitted_at=ORIGIN), status='SUBMITTING')
        executor.state.apply_fill(opening, 40)
        position = executor.state.open_position('alpha', 1111)
        rejected = journal.create('alpha', 1111, 'CLOSE', dict(
            bar_ts=timestamp_text(BAR), quantity=1), status='REJECTED')
        closing = journal.create('alpha', 1111, 'CLOSE', dict(
            bar_ts=timestamp_text(BAR), quantity=40, cumulative_filled=0,
            ownership_epoch=position['ownership_epoch'], ownership_started_at=position['ownership_started_at'],
            submitted_at=ORIGIN + 20, bar_size_seconds=999, session_tz='not-open-metadata'), status='UNKNOWN')
        stop = journal.create('alpha', 1111, 'PROTECTIVE', dict(
            bar_ts=timestamp_text(BAR), quantity=40, cumulative_filled=0,
            ownership_epoch=position['ownership_epoch'], ownership_started_at=position['ownership_started_at'],
            submitted_at=ORIGIN + 50), status='WORKING')
        executor.state.apply_fill(closing, 10)
        executor.state.apply_fill(stop, 3)
        executor.state.set_protective('alpha', 1111, 901)
        executor.state.record_open('legacy-declaration', 2222, 5, BAR, 777, None, None)
        journal.create('unrelated', 3333, 'CLOSE', dict(bar_ts=timestamp_text(BAR)), status='REJECTED')
        executor._unpublished_open_fills.add(opening['intent_id'])

        executor._load_open_view()

        rows = {(r['strategy_name'], r['conid']): r for r in executor.managed_positions()}
        assert set(rows) == {('alpha', 1111), ('legacy-declaration', 2222)}
        owned = rows[('alpha', 1111)]
        assert owned['quantity'] == 27
        assert owned['proposal_id'] == 701
        assert owned['entry_bar_ts'] == owned['entry_bar'] == BAR.replace(tzinfo=None)
        assert owned['ownership_epoch'] == position['ownership_epoch']
        assert owned['ownership_started_at'] == position['ownership_started_at']
        assert owned['protective_order_id'] == 901
        assert owned['close_by_time'] == dt.time(15, 45)
        assert owned['max_hold_bars'] == 60
        assert owned['bar_size_seconds'] == 300
        assert owned['session_tz'] == 'America/New_York'
        assert owned['fill_checkpoints'] == {
            opening['intent_id']: 40, rejected['intent_id']: 0, closing['intent_id']: 10, stop['intent_id']: 3}
        legacy = rows[('legacy-declaration', 2222)]
        # A holding with no OPEN intent payload has no recorded exit policy.
        # It used to be reported as 60-second bars in UTC, and the runtime ran
        # that fabricated policy (review 2026-09-11): unknown is None, and the
        # runtime falls back to the loaded strategy's interval/session or defers.
        assert legacy['bar_size_seconds'] is None
        assert legacy['session_tz'] is None
        assert legacy['fill_checkpoints'] == {}
        assert executor._unpublished_open_fills == set()
        metrics = executor.status_metrics()
        assert metrics['pending_intents'] == 3
        assert metrics['unknown_intents'] == 2
        assert metrics['oldest_pending_seconds'] == 100
        assert metrics['managed_positions'] == 2
        assert metrics['unprotected_positions'] == 1
    finally:
        journal.journal.close()


def test_failed_view_refresh_retains_previous_view_and_unpublished_evidence(tmp_path, monkeypatch):
    executor = AutoExecutor(str(tmp_path / 'view.duckdb'), paper_trading=True)
    try:
        executor.state.record_open('alpha', 1111, 40, BAR, 701, None, None)
        executor._load_open_view()
        previous = executor.managed_positions()
        previous_open = dict(executor._open_view)
        executor._unpublished_open_fills.add('not-yet-published')

        def unavailable(intents):
            raise OSError('injected ownership read outage')

        with monkeypatch.context() as outage:
            outage.setattr(executor.state, 'ownership_snapshot', unavailable)
            with pytest.raises(OSError, match='injected ownership read outage'):
                executor._load_open_view()
        assert executor.managed_positions() == previous
        assert executor._open_view == previous_open
        assert executor._unpublished_open_fills == {'not-yet-published'}
        executor.state.record_close('alpha', 1111, 'CLOSED_EXTERNALLY', 'later flat observation')
        executor._load_open_view()
        assert executor.managed_positions() == []
        assert executor._open_view == {}
        assert executor._unpublished_open_fills == set()
        metrics = executor.status_metrics()
        assert metrics['pending_intents'] == metrics['unknown_intents'] == 0
        assert metrics['managed_positions'] == metrics['unprotected_positions'] == 0
    finally:
        executor.intents.journal.close()


@pytest.fixture
def journaled_fill_store(owned_store):
    """Real local intent rows and atomic ownership state; fills are input facts.

    These state-boundary controls do not submit orders or claim broker receipt
    production. The payloads follow the native OPEN/PROTECTIVE/CLOSE writers.
    """
    from trader.strategy.execution_intents import IntentStore

    intents = IntentStore(owned_store.path)
    try:
        yield SimpleNamespace(path=owned_store.path, state=owned_store.state,
                              intents=intents)
    finally:
        intents.journal.close()


def _journaled_fill_open(ctx, quantity, *, bar=BAR, close_by_time=None):
    return ctx.intents.create('alpha', 1111, 'OPEN', dict(
        bar_ts=timestamp_text(bar), quantity=quantity, proposal_id=701,
        close_by_time=close_by_time.isoformat() if close_by_time else None,
        max_hold_bars=None), status='WORKING')


def _journaled_fill_protective(ctx, position, quantity):
    # Native _ensure_protective stores no reason; its observed fill supplies
    # the durable position-history fallback rather than an invented cause.
    return ctx.intents.create('alpha', 1111, 'PROTECTIVE', dict(
        bar_ts=timestamp_text(position['entry_bar_ts']), quantity=quantity,
        stop_price=9.2, ownership_epoch=position['ownership_epoch'],
        ownership_started_at=position['ownership_started_at']), status='WORKING')


def test_later_open_fill_retains_latest_time_exit_policy_after_restart(journaled_fill_store):
    from trader.strategy.auto_executor import check_time_exit

    ctx = journaled_fill_store
    first = _journaled_fill_open(ctx, 4, close_by_time=dt.time(15))
    assert ctx.state.apply_fill(first, 4) == 4
    ctx.intents.update(first, status='FILLED', cumulative_filled=4)
    original = ctx.state.open_position('alpha', 1111)
    add_bar = BAR + dt.timedelta(minutes=1)
    add = _journaled_fill_open(ctx, 2, bar=add_bar, close_by_time=dt.time(15, 45))
    assert ctx.state.apply_fill(add, 1) == 1
    ctx.intents.update(add, status='WORKING', cumulative_filled=1)

    restarted = AutoExecState(ctx.path)
    partial = restarted.open_position('alpha', 1111)
    assert partial['quantity'] == 5 and partial['lots'] == 2
    assert partial['ownership_epoch'] == original['ownership_epoch']
    assert timestamp_text(partial['entry_bar_ts']) == timestamp_text(add_bar)
    assert partial['close_by_time'] == dt.time(15, 45)
    assert check_time_exit(BAR.replace(hour=15, minute=30), 0,
                           partial['close_by_time'], partial['max_hold_bars']) is None
    assert check_time_exit(BAR.replace(hour=15, minute=45), 0,
                           partial['close_by_time'], partial['max_hold_bars']) is not None

    assert restarted.apply_fill(add, 2) == 1
    assert restarted.apply_fill(add, 2) == 0
    final = AutoExecState(ctx.path).open_position('alpha', 1111)
    assert final['quantity'] == 6 and final['lots'] == 2
    assert final['close_by_time'] == dt.time(15, 45)
    assert _progress(restarted, add['intent_id']) == 2


def test_native_shaped_protective_fill_keeps_durable_execution_cause(journaled_fill_store):
    ctx = journaled_fill_store
    opening = _journaled_fill_open(ctx, 4)
    assert ctx.state.apply_fill(opening, 4) == 4
    ctx.intents.update(opening, status='FILLED', cumulative_filled=4)
    stop = _journaled_fill_protective(ctx, ctx.state.open_position('alpha', 1111), 4)

    assert ctx.state.apply_fill(stop, 4) == 4
    assert ctx.state.open_position('alpha', 1111) is None
    restarted = AutoExecState(ctx.path)
    quantity, status, cause = restarted.db.execute(
        'SELECT quantity, status, closed_reason FROM auto_exec_positions '
        'WHERE strategy=? AND conid=?', ['alpha', 1111], fetch='one')
    assert quantity == 0 and status == 'CLOSED'
    assert isinstance(cause, str) and 'broker' in cause.lower() and 'execution' in cause.lower()
    assert _progress(restarted, stop['intent_id']) == 4
    assert restarted.apply_fill(stop, 4) == 0


def test_late_protective_receipt_after_flat_commits_only_its_checkpoint(journaled_fill_store):
    from trader.strategy.execution_intents import IntentStore

    ctx = journaled_fill_store
    opening = _journaled_fill_open(ctx, 4)
    assert ctx.state.apply_fill(opening, 4) == 4
    ctx.intents.update(opening, status='FILLED', cumulative_filled=4)
    position = ctx.state.open_position('alpha', 1111)
    stop = _journaled_fill_protective(ctx, position, 4)
    assert ctx.state.apply_fill(stop, 1) == 1
    ctx.intents.update(stop, status='CANCELLED', cumulative_filled=1)
    closing = ctx.intents.create('alpha', 1111, 'CLOSE', dict(
        bar_ts=timestamp_text(BAR + dt.timedelta(minutes=1)), quantity=3,
        reason='SELL signal', ownership_epoch=position['ownership_epoch'],
        ownership_started_at=position['ownership_started_at']), status='WORKING')
    assert ctx.state.apply_fill(closing, 3) == 3
    ctx.intents.update(closing, status='FILLED', cumulative_filled=3)
    assert ctx.state.open_position('alpha', 1111) is None
    ctx.intents.journal.close()

    reopened = IntentStore(ctx.path)
    try:
        restored, = reopened.all(strategy='alpha', conid=1111, kind='PROTECTIVE')
        restarted = AutoExecState(ctx.path)
        assert restored['status'] == 'CANCELLED'
        assert restored['payload']['ownership_epoch'] == position['ownership_epoch']
        assert restarted.apply_fill(restored, 2) == 0
        assert _progress(restarted, restored['intent_id']) == 2
        assert restarted.open_position('alpha', 1111) is None
        assert restarted.apply_fill(restored, 2) == 0
        assert _progress(AutoExecState(ctx.path), restored['intent_id']) == 2
    finally:
        reopened.journal.close()


def test_pending_age_reports_subsecond_elapsed_time_and_resets_when_idle(tmp_path, monkeypatch):
    clock = {'now': ORIGIN}
    monkeypatch.setattr('trader.strategy.auto_executor.time.time', lambda: clock['now'])
    executor = AutoExecutor(str(tmp_path / 'pending-age.duckdb'), paper_trading=True)
    try:
        # The real journal supplies the creation/update timestamp. This is a
        # pending proposal-stage intent, with no claim of broker submission.
        intent = executor.intents.create('recent-owner', 1111, 'OPEN', dict(
            bar_ts=timestamp_text(BAR), quantity=1, bar_size_seconds=60, session_tz='UTC'))
        clock['now'] += 0.25
        executor._load_open_view()
        metrics = executor.status_metrics()
        assert metrics['pending_intents'] == 1
        assert metrics['oldest_pending_seconds'] == pytest.approx(0.25)

        executor.intents.update(intent, status='REJECTED', error='opening authority revoked before approval')
        executor._load_open_view()
        idle = executor.status_metrics()
        assert idle['pending_intents'] == 0
        assert idle['oldest_pending_seconds'] == 0.0
        assert executor.managed_positions() == []
    finally:
        executor.intents.journal.close()


def test_owned_cost_weights_partial_fills_and_adds_after_a_reduction(owned_store):
    state = owned_store.state
    opening = _opening()
    assert state.apply_fill(opening, 10, cumulative_quote_notional=100) == 10
    assert state.apply_fill(opening, 20, cumulative_quote_notional=300) == 10
    assert state.open_position('alpha', 1111)['avg_cost'] == 15
    closing = _reduction(state)
    assert state.apply_fill(closing, 15) == 15
    assert state.open_position('alpha', 1111)['avg_cost'] == 15
    addition = _opening('add-a')
    assert state.apply_fill(addition, 5, cumulative_quote_notional=125) == 5
    position = AutoExecState(owned_store.path).open_position('alpha', 1111)
    assert position['quantity'] == 10
    assert position['avg_cost'] == 20
    assert position['cost_evaluable'] is True
    assert state.apply_fill(addition, 5, cumulative_quote_notional=125) == 0
    assert state.open_position('alpha', 1111) == position


def test_owned_cost_price_only_recovery_replays_an_intervening_reduction(owned_store):
    state = owned_store.state
    opening = _opening()
    state.apply_fill(opening, 10)
    assert state.open_position('alpha', 1111)['avg_cost'] is None
    assert state.unpriced_open_intents() == {opening['intent_id']}
    closing = _reduction(state)
    state.apply_fill(closing, 4)
    restarted = AutoExecState(owned_store.path)
    assert restarted.open_position('alpha', 1111)['cost_evaluable'] is False
    assert restarted.apply_fill(opening, 10, cumulative_quote_notional=200) == 0
    recovered = restarted.open_position('alpha', 1111)
    assert recovered['quantity'] == 6
    assert recovered['avg_cost'] == 20
    assert recovered['cost_evaluable'] is True
    assert restarted.unpriced_open_intents() == set()
    assert _progress(restarted, opening['intent_id']) == 10
    assert _progress(restarted, closing['intent_id']) == 4


def test_owned_cost_missing_intermediate_price_is_not_distributed_by_guess(owned_store):
    state = owned_store.state
    opening = _opening()
    state.apply_fill(opening, 10)
    state.apply_fill(_reduction(state), 5)
    state.apply_fill(opening, 20, cumulative_quote_notional=300)
    # The first ten and second ten may have different prices. A total $300
    # does not establish what cost was removed by the intervening five-share
    # reduction, even though every currently held share has known quantity.
    position = AutoExecState(owned_store.path).open_position('alpha', 1111)
    assert position['quantity'] == 15
    assert position['avg_cost'] is None
    assert position['cost_evaluable'] is False
    assert state.apply_fill(opening, 20, cumulative_quote_notional=300) == 0
    assert state.open_position('alpha', 1111) == position


def test_old_epoch_cost_and_reduction_receipts_cannot_reprice_a_new_holding(owned_store):
    state = owned_store.state
    old = _opening('old-entry')
    state.apply_fill(old, 10, cumulative_quote_notional=100)
    closing = _reduction(state)
    prior_epoch = state.open_position('alpha', 1111)['ownership_epoch']
    state.apply_fill(closing, 10)
    fresh = _opening('new-entry')
    state.apply_fill(fresh, 4, cumulative_quote_notional=80)
    position = state.open_position('alpha', 1111)
    assert position['ownership_epoch'] != prior_epoch
    assert position['avg_cost'] == 20
    assert state.apply_fill(old, 10, cumulative_quote_notional=200) == 0
    assert state.apply_fill(closing, 15) == 0
    assert AutoExecState(owned_store.path).open_position('alpha', 1111) == position


def test_cost_history_records_only_the_attributed_part_of_an_oversized_reduction(owned_store):
    state = owned_store.state
    state.apply_fill(_opening(), 4, cumulative_quote_notional=40)
    closing = _reduction(state)
    assert state.apply_fill(closing, 10) == 10  # existing progress API
    assert state.open_position('alpha', 1111) is None
    row = state.db.execute(
        'SELECT attributed_delta FROM auto_exec_cost_events WHERE intent_id=?',
        [closing['intent_id']], fetch='one')
    assert row == (-4.0,)
    state.apply_fill(_opening('next'), 2, cumulative_quote_notional=40)
    assert state.open_position('alpha', 1111)['avg_cost'] == 20


@pytest.mark.parametrize('notional', [None, 0, -1, float('nan'), float('inf')])
def test_unpriced_owned_fills_keep_quantity_and_reductions_available(owned_store, notional):
    state = owned_store.state
    state.apply_fill(_opening(), 4, cumulative_quote_notional=notional)
    position = state.open_position('alpha', 1111)
    assert position['quantity'] == 4
    assert position['avg_cost'] is None
    assert position['cost_evaluable'] is False
    assert state.apply_fill(_reduction(state), 2) == 2
    position = state.open_position('alpha', 1111)
    assert position['quantity'] == 2
    assert position['avg_cost'] is None


def test_conflicting_price_at_the_same_endpoint_is_explicitly_unevaluable(owned_store):
    state = owned_store.state
    opening = _opening()
    state.apply_fill(opening, 4, cumulative_quote_notional=40)
    assert state.open_position('alpha', 1111)['avg_cost'] == 10
    assert state.apply_fill(opening, 4, cumulative_quote_notional=44) == 0
    assert state.open_position('alpha', 1111)['cost_evaluable'] is False
    assert state.apply_fill(opening, 4, cumulative_quote_notional=40) == 0
    position = AutoExecState(owned_store.path).open_position('alpha', 1111)
    assert position['quantity'] == 4
    assert position['avg_cost'] is None


def test_pre_cost_migration_holding_has_no_invented_entry_price(owned_store):
    state = owned_store.state
    state.record_open('alpha', 1111, 4, BAR, 701, None, None)
    position = AutoExecState(owned_store.path).open_position('alpha', 1111)
    assert position['quantity'] == 4
    assert position['avg_cost'] is None
    assert position['cost_evaluable'] is False
    assert 'history' in position['cost_unavailable_reason']


def test_late_open_delta_keeps_its_proven_endpoint_across_holding_epochs(owned_store):
    state = owned_store.state
    opening = _opening()
    state.apply_fill(opening, 4, cumulative_quote_notional=40)
    old_epoch = state.open_position('alpha', 1111)['ownership_epoch']
    state.apply_fill(_reduction(state), 4)
    assert state.open_position('alpha', 1111) is None
    assert state.apply_fill(opening, 6, cumulative_quote_notional=80) == 2
    current = AutoExecState(owned_store.path).open_position('alpha', 1111)
    assert current['ownership_epoch'] != old_epoch
    assert current['quantity'] == 2
    assert current['avg_cost'] == 20
    assert current['cost_evaluable'] is True


def test_unpriced_recovery_respects_owner_and_instrument_scope(owned_store):
    state = owned_store.state
    state.apply_fill(_opening('a'), 4)
    state.apply_fill(_opening('b', strategy='beta'), 4)
    state.apply_fill(_opening('c', conid=2222), 4)
    assert state.unpriced_open_intents() == {'a', 'b', 'c'}
    assert state.unpriced_open_intents(strategy='alpha') == {'a', 'c'}
    assert state.unpriced_open_intents(conid=1111) == {'a', 'b'}
    assert state.unpriced_open_intents(strategy='alpha', conid=1111) == {'a'}
    assert state.unpriced_open_intents(strategy='missing', conid=1111) == set()


def test_pre_cost_open_checkpoint_replay_preserves_unknown_basis(tmp_path):
    """Replay a real old-schema quantity checkpoint without inventing cost."""
    import uuid
    from trader.data.duckdb_store import DuckDBConnection
    from trader.strategy.execution_intents import IntentStore

    path = str(tmp_path / 'pre-cost-checkpoint.duckdb')
    intents = IntentStore(path)
    try:
        opening = intents.create('alpha', 1111, 'OPEN', dict(
            bar_ts=timestamp_text(BAR), quantity=8, proposal_id=701,
            bar_size_seconds=60, session_tz='UTC'), status='WORKING')
        epoch = uuid.uuid4().hex
        db = DuckDBConnection(path)

        def write_pre_cost_checkpoint(conn):
            # The pre-cost native state schema and first OPEN-fill write.
            # This is an explicit persisted-history fixture, not a claim of
            # new broker placement and not deletion of current cost evidence.
            conn.execute('CREATE TABLE auto_exec_positions ('
                         'strategy VARCHAR NOT NULL, conid BIGINT NOT NULL, '
                         'quantity DOUBLE NOT NULL, entry_bar_ts TIMESTAMP, '
                         'entry_time TIMESTAMP NOT NULL, proposal_id BIGINT, '
                         'close_by_time VARCHAR, max_hold_bars BIGINT, '
                         'status VARCHAR NOT NULL, closed_reason VARCHAR, '
                         'close_proposal_id BIGINT, updated TIMESTAMP NOT NULL, '
                         'protective_order_id BIGINT, lots BIGINT, '
                         'ownership_epoch VARCHAR, ownership_started_at DOUBLE)')
            conn.execute('CREATE TABLE auto_exec_fill_progress '
                         '(intent_id VARCHAR PRIMARY KEY, quantity DOUBLE NOT NULL)')
            now = dt.datetime.now()
            conn.execute(
                'INSERT INTO auto_exec_positions '
                '(strategy,conid,quantity,entry_bar_ts,entry_time,proposal_id,close_by_time,'
                'max_hold_bars,status,updated,lots,ownership_epoch,ownership_started_at) '
                "VALUES (?,?,?,?,?,?,?,?,'OPEN',?,1,?,?)",
                ['alpha', 1111, 4, BAR.replace(tzinfo=None), now, 701,
                 None, None, now, epoch, opening['payload']['intent_created_at']])
            conn.execute('INSERT INTO auto_exec_fill_progress VALUES (?, ?) '
                         'ON CONFLICT(intent_id) DO UPDATE SET quantity=excluded.quantity',
                         [opening['intent_id'], 4])

        db.execute_atomic(write_pre_cost_checkpoint)
        intents.update(opening, status='WORKING', cumulative_filled=4)
        assert db.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='auto_exec_cost_events'",
            fetch='one') == (0,)

        # Current initialization performs the actual migration. A cumulative
        # price can price no missing earlier event merely by arriving now.
        state = AutoExecState(path)
        before = state.open_position('alpha', 1111)
        assert before is not None and before['quantity'] == 4
        assert before['ownership_epoch'] == epoch
        assert before['cost_evaluable'] is False and before['avg_cost'] is None
        assert _progress(state, opening['intent_id']) == 4
        assert state.db.execute('SELECT COUNT(*) FROM auto_exec_cost_events', fetch='one') == (0,)

        assert state.apply_fill(opening, 4, cumulative_quote_notional=40) == 0
        after = state.open_position('alpha', 1111)
        assert after == before
        assert after['quantity'] == 4 and after['ownership_epoch'] == epoch
        assert after['cost_evaluable'] is False and after['avg_cost'] is None
        assert _progress(state, opening['intent_id']) == 4
        assert state.db.execute('SELECT COUNT(*) FROM auto_exec_cost_events', fetch='one') == (0,)
        restarted = AutoExecState(path)
        assert restarted.apply_fill(opening, 4, cumulative_quote_notional=40) == 0
        assert restarted.open_position('alpha', 1111) == before
        assert _progress(restarted, opening['intent_id']) == 4
    finally:
        intents.journal.close()


def test_migrated_null_epoch_holding_accepts_add_checkpoint_without_inventing_cost(tmp_path):
    from trader.data.duckdb_store import DuckDBConnection
    from trader.strategy.execution_intents import IntentStore

    path = str(tmp_path / 'pre-epoch-owned.duckdb')
    legacy = DuckDBConnection(path)
    # This is the original position table before the nullable ownership
    # migration, not a current holding with its provenance edited away.
    legacy.execute('CREATE TABLE auto_exec_positions ('
                   'strategy VARCHAR NOT NULL, conid BIGINT NOT NULL, '
                   'quantity DOUBLE NOT NULL, entry_bar_ts TIMESTAMP, '
                   'entry_time TIMESTAMP NOT NULL, proposal_id BIGINT, '
                   'close_by_time VARCHAR, max_hold_bars BIGINT, '
                   'status VARCHAR NOT NULL, closed_reason VARCHAR, '
                   'close_proposal_id BIGINT, updated TIMESTAMP NOT NULL)')
    legacy.execute('INSERT INTO auto_exec_positions '
                   '(strategy, conid, quantity, entry_bar_ts, entry_time, proposal_id, status, updated) '
                   "VALUES (?, ?, ?, ?, ?, ?, 'OPEN', ?)",
                   ['alpha', 1111, 4, BAR.replace(tzinfo=None), BAR.replace(tzinfo=None),
                    701, BAR.replace(tzinfo=None)])
    state = AutoExecState(path)
    before = state.open_position('alpha', 1111)
    assert before is not None and before['quantity'] == 4
    assert before['ownership_epoch'] is None and before['avg_cost'] is None
    journal = IntentStore(path)
    try:
        # Exercise the durable state boundary for a confirmed OPEN/ADD
        # receipt. This does not claim a new proposal or broker placement.
        addition = journal.create('alpha', 1111, 'OPEN',
                                  dict(bar_ts=timestamp_text(BAR + dt.timedelta(minutes=1)),
                                       bar_size_seconds=60, quantity=1), status='WORKING')
        assert state.apply_fill(addition, 1, cumulative_quote_notional=10) == 1
        assert _progress(state, addition['intent_id']) == 1
        restarted = AutoExecState(path)
        after = restarted.open_position('alpha', 1111)
        assert after is not None and after['quantity'] == 5
        assert after['ownership_epoch'] is None
        assert after['avg_cost'] is None and after['cost_evaluable'] is False
        assert restarted.db.execute('SELECT COUNT(*) FROM auto_exec_cost_events', fetch='one') == (0,)
        assert restarted.apply_fill(addition, 1, cumulative_quote_notional=10) == 0
        assert _progress(restarted, addition['intent_id']) == 1
        assert restarted.open_position('alpha', 1111) == after
    finally:
        journal.journal.close()


def test_priced_add_and_reduction_cannot_price_the_pre_cost_remainder(tmp_path):
    """A new priced slice cannot provide the missing cost of older owned shares."""
    import uuid
    from trader.data.duckdb_store import DuckDBConnection
    from trader.strategy.execution_intents import IntentStore

    path = str(tmp_path / 'pre-cost-remainder.duckdb')
    intents = IntentStore(path)
    try:
        opening = intents.create('alpha', 1111, 'OPEN', dict(
            bar_ts=timestamp_text(BAR), quantity=4, proposal_id=701,
            bar_size_seconds=60, session_tz='UTC'), status='WORKING')
        epoch = uuid.uuid4().hex
        db = DuckDBConnection(path)

        def write_pre_cost_checkpoint(conn):
            # The pre-cost native state schema and first OPEN-fill write.
            # This is an explicit persisted-history fixture, not a claim of
            # new broker placement and not deletion of current cost evidence.
            conn.execute('CREATE TABLE auto_exec_positions ('
                         'strategy VARCHAR NOT NULL, conid BIGINT NOT NULL, '
                         'quantity DOUBLE NOT NULL, entry_bar_ts TIMESTAMP, '
                         'entry_time TIMESTAMP NOT NULL, proposal_id BIGINT, '
                         'close_by_time VARCHAR, max_hold_bars BIGINT, '
                         'status VARCHAR NOT NULL, closed_reason VARCHAR, '
                         'close_proposal_id BIGINT, updated TIMESTAMP NOT NULL, '
                         'protective_order_id BIGINT, lots BIGINT, '
                         'ownership_epoch VARCHAR, ownership_started_at DOUBLE)')
            conn.execute('CREATE TABLE auto_exec_fill_progress '
                         '(intent_id VARCHAR PRIMARY KEY, quantity DOUBLE NOT NULL)')
            now = dt.datetime.now()
            conn.execute(
                'INSERT INTO auto_exec_positions '
                '(strategy,conid,quantity,entry_bar_ts,entry_time,proposal_id,close_by_time,'
                'max_hold_bars,status,updated,lots,ownership_epoch,ownership_started_at) '
                "VALUES (?,?,?,?,?,?,?,?,'OPEN',?,1,?,?)",
                ['alpha', 1111, 4, BAR.replace(tzinfo=None), now, 701,
                 None, None, now, epoch, opening['payload']['intent_created_at']])
            conn.execute('INSERT INTO auto_exec_fill_progress VALUES (?, ?) '
                         'ON CONFLICT(intent_id) DO UPDATE SET quantity=excluded.quantity',
                         [opening['intent_id'], 4])

        db.execute_atomic(write_pre_cost_checkpoint)
        intents.update(opening, status='FILLED', cumulative_filled=4)
        assert db.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='auto_exec_cost_events'",
            fetch='one') == (0,)
        state = AutoExecState(path)
        before = state.open_position('alpha', 1111)
        assert before is not None and before['quantity'] == 4
        assert before['ownership_epoch'] == epoch and epoch is not None
        assert before['cost_evaluable'] is False and before['avg_cost'] is None
        assert state.db.execute('SELECT COUNT(*) FROM auto_exec_cost_events', fetch='one') == (0,)

        # These are confirmed fill inputs to the real durable state boundary.
        # They do not assert that this fixture submitted a new broker order.
        addition = intents.create('alpha', 1111, 'OPEN', dict(
            bar_ts=timestamp_text(BAR + dt.timedelta(minutes=1)), quantity=1,
            bar_size_seconds=60, session_tz='UTC'), status='WORKING')
        assert state.apply_fill(addition, 1, cumulative_quote_notional=10) == 1
        added = state.open_position('alpha', 1111)
        assert added is not None and added['quantity'] == 5
        assert added['ownership_epoch'] == epoch
        assert added['cost_evaluable'] is False and added['avg_cost'] is None
        assert _progress(state, addition['intent_id']) == 1
        reduction = intents.create('alpha', 1111, 'CLOSE', dict(
            bar_ts=timestamp_text(BAR + dt.timedelta(minutes=2)), quantity=1,
            ownership_epoch=epoch, ownership_started_at=before['ownership_started_at'],
            reason='confirmed reduction after priced add'), status='WORKING')
        assert state.apply_fill(reduction, 1) == 1
        remaining = state.open_position('alpha', 1111)
        assert remaining is not None and remaining['quantity'] == 4
        assert remaining['ownership_epoch'] == epoch
        assert remaining['ownership_started_at'] == before['ownership_started_at']
        assert remaining['cost_evaluable'] is False and remaining['avg_cost'] is None
        assert 'history' in remaining['cost_unavailable_reason']
        assert _progress(state, opening['intent_id']) == 4
        assert _progress(state, addition['intent_id']) == 1
        assert _progress(state, reduction['intent_id']) == 1
        events = state.db.execute(
            'SELECT intent_id,attributed_delta,cumulative_quote_notional '
            'FROM auto_exec_cost_events ORDER BY sequence', fetch='all')
        assert events == [(addition['intent_id'], 1.0, 10.0),
                          (reduction['intent_id'], -1.0, None)]

        restarted = AutoExecState(path)
        assert restarted.apply_fill(addition, 1, cumulative_quote_notional=10) == 0
        assert restarted.apply_fill(reduction, 1) == 0
        assert restarted.open_position('alpha', 1111) == remaining
        assert _progress(restarted, opening['intent_id']) == 4
        assert _progress(restarted, addition['intent_id']) == 1
        assert _progress(restarted, reduction['intent_id']) == 1
    finally:
        intents.journal.close()
