"""Bounded execution admission with coalesced, priority reduction work."""
from __future__ import annotations

import collections
import queue
import threading
import time

from trader.objects import Action


class ExecutionWorkQueue:
    def __init__(self, opening_capacity=512, exit_capacity=1024):
        self.opening_capacity = opening_capacity
        self.exit_capacity = exit_capacity
        self._normal = collections.deque()
        self._exits = collections.OrderedDict()
        self._management = None
        self._stopping = False
        self._condition = threading.Condition()
        self._rejected = 0
        self._coalesced = 0

    def put(self, item) -> bool:
        """True means admitted. Exit overflow must use durable admission."""
        with self._condition:
            now = time.monotonic()
            if item is None:
                self._stopping = True
            elif type(item).__name__ == 'ManagementWork':
                if self._management is None:
                    self._management = (now, item)
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
                    return False
                else:
                    self._exits[key] = (now, item)
            else:
                key = (getattr(item, 'strategy_name', None), getattr(item, 'conid', None))
                if len(self._normal) >= self.opening_capacity or key in self._exits:
                    self._rejected += 1
                    return False
                self._normal.append((now, item))
            self._condition.notify()
            return True

    def get(self, timeout=None):
        with self._condition:
            deadline = None if timeout is None else time.monotonic() + timeout
            while True:
                if self._exits:
                    return self._exits.popitem(last=False)[1][1]
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
            return len(self._normal) + len(self._exits) + int(self._management is not None)

    def metrics(self):
        with self._condition:
            times = [row[0] for row in self._normal] + [row[0] for row in self._exits.values()]
            if self._management is not None:
                times.append(self._management[0])
            return dict(queue_depth=len(times), queue_oldest_seconds=time.monotonic() - min(times) if times else 0.0,
                        rejected_queue_items=self._rejected, coalesced_queue_items=self._coalesced,
                        opening_queue_capacity=self.opening_capacity, exit_queue_capacity=self.exit_capacity)
