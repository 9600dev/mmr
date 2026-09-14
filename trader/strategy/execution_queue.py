"""Bounded execution admission with coalesced, priority reduction work."""
from __future__ import annotations

import collections
import queue
import threading
import time
from dataclasses import dataclass
from typing import Callable, Optional

from trader.objects import Action


@dataclass
class ManagementWork:
    """Independent broker reconciliation; survives inactive strategy code.

    Defined here rather than in ``auto_executor`` so the queue can use
    ``isinstance`` instead of matching a class *name* (any class called
    ``ManagementWork`` used to be coalesced into the management slot).
    """


class OperatorWork:
    """An explicit human act that must run on the executor worker thread.

    Worker-owned state (the snapshot cache, ownership warnings, the intent
    journal's mutation path) is only ever touched by the worker. An RPC
    thread enqueues one of these and waits for its result instead of
    mutating that state itself.
    """

    def __init__(self, fn: Callable[[], object], description: str = ''):
        self.fn = fn
        self.description = description
        self.result: object = None
        self.error: Optional[BaseException] = None
        self._done = threading.Event()

    def run(self) -> None:
        try:
            self.result = self.fn()
        except BaseException as exc:  # the caller re-raises on its own thread
            self.error = exc
        finally:
            self._done.set()

    def wait(self, timeout: float) -> object:
        if not self._done.wait(timeout):
            raise TimeoutError(
                f'executor worker did not finish {self.description or "operator work"} '
                f'within {timeout:g}s; it may be blocked on a broker call — retry')
        if self.error is not None:
            raise self.error
        return self.result


class ExecutionWorkQueue:
    def __init__(self, opening_capacity=512, exit_capacity=1024, operator_capacity=64):
        self.opening_capacity = opening_capacity
        self.exit_capacity = exit_capacity
        self.operator_capacity = operator_capacity
        self._normal = collections.deque()
        self._exits = collections.OrderedDict()
        self._operator = collections.deque()
        self._management = None
        self._stopping = False
        self._condition = threading.Condition()
        self._rejected = 0
        self._coalesced = 0

    def put(self, item) -> bool:
        """True means admitted. Exit overflow must use durable admission."""
        return self.admit(item) is None

    def admit(self, item) -> Optional[str]:
        """Admit ``item``; return None, or the reason it was refused.

        The reason is what the caller logs. Refusing an opening because an
        exit for the same key is queued is not the same failure as a full
        opening queue, and reporting the latter for the former sent an
        operator looking at capacity when the real cause was a pending exit.
        """
        with self._condition:
            now = time.monotonic()
            if item is None:
                self._stopping = True
            elif isinstance(item, ManagementWork):
                if self._management is None:
                    self._management = (now, item)
            elif isinstance(item, OperatorWork):
                if len(self._operator) >= self.operator_capacity:
                    self._rejected += 1
                    return f'operator queue full ({self.operator_capacity} items)'
                self._operator.append((now, item))
            elif getattr(item, 'action', None) == Action.SELL:
                key = (item.strategy_name, item.conid)
                # A later flatten request supersedes queued entries. Merely
                # giving SELL priority could otherwise sell-flat, then BUY.
                before = len(self._normal)
                self._normal = collections.deque((created, work) for created, work in self._normal
                    if (getattr(work, 'strategy_name', None), getattr(work, 'conid', None)) != key
                    or getattr(work, 'action', None) != Action.BUY)
                self._coalesced += before - len(self._normal)
                if key in self._exits:
                    self._coalesced += 1
                elif len(self._exits) >= self.exit_capacity:
                    self._rejected += 1
                    return f'exit queue full ({self.exit_capacity} items)'
                else:
                    self._exits[key] = (now, item)
            else:
                key = (getattr(item, 'strategy_name', None), getattr(item, 'conid', None))
                if key in self._exits:
                    self._rejected += 1
                    return f'exit pending for {key[0]}/{key[1]}; opening work is not admitted behind it'
                if len(self._normal) >= self.opening_capacity:
                    self._rejected += 1
                    return f'opening queue full ({self.opening_capacity} items)'
                self._normal.append((now, item))
            self._condition.notify()
            return None

    def get(self, timeout=None):
        with self._condition:
            deadline = None if timeout is None else time.monotonic() + timeout
            while True:
                if self._exits:
                    return self._exits.popitem(last=False)[1][1]
                if self._operator:
                    return self._operator.popleft()[1]
                if self._management is not None:
                    _, item = self._management
                    self._management = None
                    return item
                if self._normal:
                    return self._normal.popleft()[1]
                if self._stopping:
                    return None
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise queue.Empty
                self._condition.wait(remaining)

    def qsize(self):
        with self._condition:
            return (len(self._normal) + len(self._exits) + len(self._operator)
                    + int(self._management is not None))

    def metrics(self):
        with self._condition:
            times = ([row[0] for row in self._normal] + [row[0] for row in self._exits.values()]
                     + [row[0] for row in self._operator])
            if self._management is not None:
                times.append(self._management[0])
            return dict(queue_depth=len(times), queue_oldest_seconds=time.monotonic() - min(times) if times else 0.0,
                        rejected_queue_items=self._rejected, coalesced_queue_items=self._coalesced,
                        opening_queue_capacity=self.opening_capacity, exit_queue_capacity=self.exit_capacity)
