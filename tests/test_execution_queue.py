"""Bounded admission preserves flatten intent while shedding opening work."""
import pandas as pd

from trader.objects import Action
from trader.strategy.auto_executor import SignalWork, ManagementWork
from trader.strategy.execution_queue import ExecutionWorkQueue


def work(conid, action):
    return SignalWork('strategy', conid, action, pd.Timestamp('2026-09-08T16:00Z'), 60)


def test_close_has_reserved_capacity_and_supersedes_queued_entry():
    queue = ExecutionWorkQueue(opening_capacity=1, exit_capacity=1)
    assert queue.put(work(1, Action.BUY))
    assert not queue.put(work(2, Action.BUY))
    assert queue.put(work(1, Action.SELL))
    assert queue.put(ManagementWork())
    assert queue.put(ManagementWork())
    assert queue.qsize() == 2
    assert queue.get().action == Action.SELL
    assert isinstance(queue.get(), ManagementWork)
    assert queue.qsize() == 0


def test_repeated_exit_coalesces_and_exit_overflow_is_explicit():
    queue = ExecutionWorkQueue(opening_capacity=1, exit_capacity=1)
    assert queue.put(work(1, Action.SELL))
    assert queue.put(work(1, Action.SELL))
    assert not queue.put(work(2, Action.SELL))
    assert queue.metrics()['queue_depth'] == 1
    assert queue.metrics()['rejected_queue_items'] == 1
    assert queue.metrics()['coalesced_queue_items'] == 1
