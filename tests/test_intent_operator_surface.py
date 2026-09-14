"""Operator surface for execution intents: listing, resolution, worker routing.

`mmr strategies intents` needs JSON-safe rows that say what each intent blocks;
`mmr strategies resolve-intent` is an explicit human act that must never hide
broker evidence and must not mutate worker-owned state from the RPC thread.
"""
import asyncio
import json
import logging
import threading
import time
from types import SimpleNamespace

import pytest

from review.test_review_strategy_contract import TS
from test_intent_support import CONID, OWNER, SnapshotSDK, build_executor, inline_worker, make_work, open_owned
from trader.messaging.strategy_service_api import StrategyServiceApi
from trader.objects import Action
from trader.strategy.auto_executor import AutoExecutionError
from trader.strategy.execution_intents import timestamp_text
from trader.strategy.execution_queue import ExecutionWorkQueue, ManagementWork, OperatorWork


def _insert_intent(executor, intent_id, kind, status, payload, *, strategy=OWNER, conid=CONID):
    with executor.intents.journal.transaction() as conn:
        conn.execute('INSERT INTO execution_intents VALUES (?, ?, ?, ?, ?, ?, ?)',
                     [intent_id, strategy, conid, kind, status, json.dumps(payload), time.time()])


def _stuck_open(executor, intent_id='auto-stuck-open', **extra):
    payload = dict(bar_ts=timestamp_text(TS), quantity=140.0, submitted_at=time.time() - 3600,
                   intent_created_at=time.time() - 3600)
    payload.update(extra)
    _insert_intent(executor, intent_id, 'OPEN', 'SUBMITTING', payload)
    return intent_id


def test_listing_rows_are_json_safe_and_explain_what_blocks(tmp_path, monkeypatch):
    sdk = SnapshotSDK()
    executor, _ = build_executor(tmp_path, sdk, monkeypatch)
    inline_worker(executor)
    open_owned(executor)  # leaves an OPEN FILLED and a PROTECTIVE WORKING
    _stuck_open(executor)
    _insert_intent(executor, 'auto-old-close', 'CLOSE', 'CANCELLED',
                   dict(bar_ts=timestamp_text(TS), quantity=140.0, order_ids=[7000], exit_request_active=True))

    rows = executor.list_execution_intents()
    json.dumps(rows)  # every value is a JSON primitive
    by_id = {row['intent_id']: row for row in rows}
    assert {'auto-stuck-open', 'auto-old-close'} <= set(by_id)
    assert not any(row['kind'] == 'OPEN' and row['status'] == 'FILLED' for row in rows), \
        'a terminal intent with no pending request is not "active"'
    assert by_id['auto-stuck-open']['blocking'] == (
        'blocks opens; blocks exits (no broker order evidence; exit sizing waits for it)')
    assert 'exit request still pending' in by_id['auto-old-close']['blocking']
    assert by_id['auto-old-close']['flags'] == {'exit_request_active': True}
    working, = [row for row in rows if row['kind'] == 'PROTECTIVE']
    assert working['status'] == 'WORKING' and working['blocking'] == 'none'
    assert set(by_id['auto-stuck-open']) >= {'intent_id', 'kind', 'status', 'strategy', 'conid', 'created',
                                             'updated', 'order_ids', 'proposal_id', 'flags', 'blocking'}
    assert by_id['auto-stuck-open']['created'].endswith('+00:00')

    everything = executor.list_execution_intents(active_only=False)
    assert any(row['kind'] == 'OPEN' and row['status'] == 'FILLED' for row in everything)
    assert executor.list_execution_intents(strategy='someone_else') == []
    assert {row['conid'] for row in executor.list_execution_intents(conid=CONID)} == {CONID}


def test_resolution_requires_a_reason_and_a_known_non_terminal_intent(tmp_path, monkeypatch):
    sdk = SnapshotSDK()
    executor, _ = build_executor(tmp_path, sdk, monkeypatch)
    inline_worker(executor)
    open_owned(executor)
    with pytest.raises(AutoExecutionError, match='non-empty reason'):
        executor.resolve_execution_intent('auto-anything', '   ')
    with pytest.raises(AutoExecutionError, match='unknown to this executor'):
        executor.resolve_execution_intent('auto-missing', 'why')
    filled, = executor.intents.all(kind='OPEN')
    with pytest.raises(AutoExecutionError, match='already FILLED'):
        executor.resolve_execution_intent(filled['intent_id'], 'why')


def test_resolution_refuses_intents_with_broker_evidence(tmp_path, monkeypatch):
    sdk = SnapshotSDK()
    executor, _ = build_executor(tmp_path, sdk, monkeypatch)
    inline_worker(executor)
    open_owned(executor)
    protective, = executor.intents.all(kind='PROTECTIVE', active=True)
    stop_id = protective['payload']['order_ids'][0]
    with pytest.raises(AutoExecutionError) as refused:
        executor.resolve_execution_intent(protective['intent_id'], 'operator says it is gone')
    assert f'{stop_id}:Submitted' in str(refused.value) and 'matching orders' in str(refused.value)
    assert executor.intents.get(protective['intent_id'])['status'] == 'WORKING'

    _insert_intent(executor, 'auto-lost-ack', 'OPEN', 'UNKNOWN',
                   dict(bar_ts=timestamp_text(TS), quantity=10.0, order_ids=[7777]), conid=2222)
    with pytest.raises(AutoExecutionError, match='order 7777 is not proven terminal'):
        executor.resolve_execution_intent('auto-lost-ack', 'why')
    sdk.complete = False
    with pytest.raises(AutoExecutionError, match='snapshot is incomplete'):
        executor.resolve_execution_intent('auto-lost-ack', 'why')
    assert executor.intents.get('auto-lost-ack')['status'] == 'UNKNOWN'


def test_resolution_marks_the_intent_and_leaves_an_audit_row(tmp_path, monkeypatch, caplog):
    sdk = SnapshotSDK()
    executor, _ = build_executor(tmp_path, sdk, monkeypatch)
    inline_worker(executor)
    intent_id = _stuck_open(executor)
    requested = []
    monkeypatch.setattr(executor, 'submit_management', lambda: requested.append(True))
    reason = 'confirmed at IB: no order carries this id'
    with caplog.at_level(logging.WARNING, logger='auto_executor'):
        result = executor.resolve_execution_intent(intent_id, reason)
    assert result == dict(intent_id=intent_id, kind='OPEN', strategy=OWNER, conid=CONID,
                          status='RESOLVED', previous_status='SUBMITTING', reason=reason)
    saved = executor.intents.get(intent_id)
    assert saved['status'] == 'RESOLVED'
    assert saved['payload']['operator_resolved'] is True and saved['payload']['operator_reason'] == reason
    assert saved['payload'].get('never_submitted') is None, 'attested by an operator, not proven by the system'
    decision, note, logged_id = executor.state.db.execute(
        'SELECT decision, reason, intent_id FROM auto_exec_bar_log WHERE intent_id=?', [intent_id], fetch='one')
    assert (decision, logged_id) == ('operator_resolved', intent_id) and 'SUBMITTING -> RESOLVED' in note
    assert any(f'OPERATOR RESOLVED intent {intent_id}' in record.getMessage() for record in caplog.records)
    assert requested == [True]
    with pytest.raises(AutoExecutionError, match='already RESOLVED'):
        executor.resolve_execution_intent(intent_id, reason)
    # The key no longer reserves exposure.
    executor._process_signal(make_work(quantity=140))
    assert executor.state.open_position(OWNER, CONID)['quantity'] == 140


def test_operator_acts_run_on_the_worker_thread(tmp_path, monkeypatch):
    sdk = SnapshotSDK()
    executor, _ = build_executor(tmp_path, sdk, monkeypatch)
    threads = []
    monkeypatch.setattr(executor, '_resolve_execution_intent_on_worker',
                        lambda intent_id, reason: threads.append(threading.current_thread().name) or {'id': intent_id})
    monkeypatch.setattr(executor, '_adopt_legacy_holding_on_worker',
                        lambda strategy, conid, avg_cost: threads.append(threading.current_thread().name) or {'s': strategy})
    try:
        assert executor.resolve_execution_intent('auto-x', 'why') == {'id': 'auto-x'}
        assert executor.adopt_legacy_holding(OWNER, CONID) == {'s': OWNER}
    finally:
        executor.stop()
        executor._worker.join(timeout=10)
    assert threads == ['auto-executor', 'auto-executor']
    assert threading.current_thread().name not in threads


def test_operator_act_times_out_instead_of_hanging_when_the_worker_is_stuck(tmp_path, monkeypatch):
    sdk = SnapshotSDK()
    executor, _ = build_executor(tmp_path, sdk, monkeypatch)
    executor._started = True  # a worker exists but never drains, as during a blocked broker call
    with pytest.raises(TimeoutError, match='retry'):
        executor._run_on_worker(lambda: 'never', 'a test act', timeout=0.05)


def test_rpc_methods_wrap_the_executor(tmp_path, monkeypatch):
    sdk = SnapshotSDK()
    executor, _ = build_executor(tmp_path, sdk, monkeypatch)
    intent_id = _stuck_open(executor)
    api = StrategyServiceApi(SimpleNamespace(auto_executor=executor))
    try:
        assert asyncio.run(api.list_execution_intents(None, None, True)) == executor.list_execution_intents()
        assert asyncio.run(api.list_execution_intents(OWNER, CONID, False)) == \
            executor.list_execution_intents(OWNER, CONID, active_only=False)
        refused = asyncio.run(api.resolve_execution_intent(intent_id, ''))
        assert not refused.is_success() and refused.error == 'operator resolution requires a non-empty reason'
        missing = asyncio.run(api.resolve_execution_intent('auto-nope', 'why'))
        assert not missing.is_success() and 'unknown to this executor' in missing.error
        done = asyncio.run(api.resolve_execution_intent(intent_id, 'why'))
        assert done.is_success() and done.obj['status'] == 'RESOLVED'
    finally:
        executor.stop()
        executor._worker.join(timeout=10)
    absent = StrategyServiceApi(SimpleNamespace(auto_executor=None))
    assert asyncio.run(absent.list_execution_intents()) == []
    assert not asyncio.run(absent.resolve_execution_intent('x', 'y')).is_success()


def test_operator_work_is_served_after_exits_and_before_management():
    queue = ExecutionWorkQueue(opening_capacity=4, exit_capacity=4, operator_capacity=1)
    act = OperatorWork(lambda: 'done', 'act')
    assert queue.put(make_work(conid=1, action=Action.BUY))
    assert queue.put(ManagementWork())
    assert queue.put(act)
    assert queue.put(make_work(conid=2, action=Action.SELL))
    assert queue.admit(OperatorWork(lambda: None)) == 'operator queue full (1 items)'
    assert queue.qsize() == 4 and queue.metrics()['queue_depth'] == 4
    assert queue.get().action == Action.SELL
    assert queue.get() is act
    assert isinstance(queue.get(), ManagementWork)
    assert queue.get().action == Action.BUY
    act.run()
    assert act.wait(0) == 'done'


def test_operator_work_propagates_the_workers_exception():
    def fails():
        raise ValueError('boom')
    act = OperatorWork(fails, 'act')
    act.run()
    with pytest.raises(ValueError, match='boom'):
        act.wait(0)
