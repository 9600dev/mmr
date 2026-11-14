"""Bounded admission preserves flatten intent while shedding opening work."""
from types import SimpleNamespace

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


def _bar(name='s', conid=1):
    return SimpleNamespace(strategy_name=name, conid=conid, action=None)


def test_a_waiting_bar_is_not_starved_by_the_next_management_pass():
    """Management is re-armed every second. When a pass takes a second or more
    the next request is already queued by the time it ends; served ahead of
    everything else, it starved queued bars (time-exit evaluation) and opens
    indefinitely — 25 s without processing a due time-exit bar in the
    2026-11-14 measurement."""
    q = ExecutionWorkQueue()
    q.put(ManagementWork())                 # pass 1 requested
    assert isinstance(q.get(), ManagementWork)
    bar = _bar()
    q.put(bar)                              # arrives while pass 1 is running
    q.put(ManagementWork())                 # pass 2 requested after the bar
    assert q.get() is bar                   # the bar goes first
    assert isinstance(q.get(), ManagementWork)


def test_management_requested_first_is_served_first():
    q = ExecutionWorkQueue()
    q.put(ManagementWork())
    bar = _bar()
    q.put(bar)
    assert isinstance(q.get(), ManagementWork)
    assert q.get() is bar


def test_exits_still_precede_both():
    q = ExecutionWorkQueue()
    bar = _bar()
    q.put(bar)
    q.put(ManagementWork())
    exit_work = SimpleNamespace(strategy_name='s', conid=2, action=Action.SELL)
    q.put(exit_work)
    assert q.get() is exit_work
    assert q.get() is bar
    assert isinstance(q.get(), ManagementWork)
