"""Small silent-failure modes: a typo must not disable protection, a refusal
must name its real cause, and queue admission must not match a class by name."""
import logging

import pytest

from review.test_review_strategy_contract import make_work
from test_intent_support import CONID, OWNER, SnapshotSDK, build_executor
from trader.objects import Action
from trader.strategy import auto_executor as auto_executor_module
from trader.strategy import execution_queue
from trader.strategy.auto_executor import AutoExecutor
from trader.strategy.execution_queue import ExecutionWorkQueue


@pytest.mark.parametrize('raw, expected, warns', [
    ('', 8.0, False), ('8', 8.0, False), ('5.5', 5.5, False), ('0', 0.0, False),
    ('abc', 8.0, True), ('nan', 8.0, True), ('inf', 8.0, True), ('-3', 8.0, True),
])
def test_protective_stop_pct_falls_back_to_the_default_with_one_warning(monkeypatch, caplog, raw, expected, warns):
    monkeypatch.setenv('MMR_PROTECTIVE_STOP_PCT', raw)
    executor = AutoExecutor.__new__(AutoExecutor)
    with caplog.at_level(logging.WARNING, logger='auto_executor'):
        assert executor.protective_stop_pct == expected
        assert executor.protective_stop_pct == expected
    warnings = [record.getMessage() for record in caplog.records if 'MMR_PROTECTIVE_STOP_PCT' in record.getMessage()]
    assert len(warnings) == (1 if warns else 0), warnings
    if warns:
        assert repr(raw) in warnings[0] and 'default 8%' in warnings[0]


def test_refused_opening_names_the_pending_exit_not_a_full_queue(tmp_path, monkeypatch):
    sdk = SnapshotSDK()
    executor, _ = build_executor(tmp_path, sdk, monkeypatch)
    executor._queue = ExecutionWorkQueue(opening_capacity=8, exit_capacity=8)
    monkeypatch.setattr(executor, 'start', lambda: None)
    messages = []
    monkeypatch.setattr(auto_executor_module.logging, 'error', lambda message, *args: messages.append(message % args))
    assert executor._queue.put(make_work(action=Action.SELL))
    assert executor.submit_signal(make_work(action=Action.BUY)) is False
    assert len(messages) == 1
    assert f'exit pending for {OWNER}/{CONID}' in messages[0] and 'opening queue full' not in messages[0]

    executor._queue = ExecutionWorkQueue(opening_capacity=0, exit_capacity=8)
    assert executor.submit_signal(make_work(action=Action.BUY)) is False
    assert 'opening queue full (0 items)' in messages[1]


def test_management_admission_uses_isinstance_not_the_class_name():
    class ManagementWork:  # noqa: N801 - an impostor sharing only the name
        pass

    class Pulse(auto_executor_module.ManagementWork):
        pass

    queue = ExecutionWorkQueue()
    assert queue.put(ManagementWork()) and queue.put(ManagementWork())
    assert queue.qsize() == 2, 'name-alikes are ordinary work, not coalesced management'
    queue = ExecutionWorkQueue()
    assert queue.put(Pulse()) and queue.put(Pulse())
    assert queue.qsize() == 1, 'a real ManagementWork subclass coalesces into the single slot'
    assert isinstance(queue.get(), auto_executor_module.ManagementWork)
    assert auto_executor_module.ManagementWork is execution_queue.ManagementWork, 're-exported, not redefined'
