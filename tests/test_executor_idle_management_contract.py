"""Worker liveness without relying on wall-clock scheduling or broker calls."""
import math

from test_executor_admission_observability import _loop_controller
from trader.strategy.execution_queue import ExecutionWorkQueue


class _ExpiredWaitQueue(ExecutionWorkQueue):
    """Advance a requested finite wait to its deadline immediately."""

    def __init__(self):
        super().__init__(opening_capacity=0, exit_capacity=0)
        self.requested_waits = []

    def get(self, timeout=None):
        # get() runs outside the worker's broad processing-error handler.
        # A missing management callback or ineffective stop must fail the
        # test after a few queue reads instead of spinning indefinitely.
        assert len(self.requested_waits) < 6, 'worker failed to finish bounded management cycles'
        # Periodic management needs a finite wait when no bar/signal arrives.
        # There is no asserted one-second service deadline: two seconds also
        # satisfies the finite positive wait contract.
        assert isinstance(timeout, (int, float)) and not isinstance(timeout, bool)
        assert math.isfinite(timeout) and timeout > 0
        self.requested_waits.append(timeout)
        return super().get(timeout=0)


def test_idle_worker_keeps_managing_without_new_market_work():
    executor = _loop_controller()
    executor._queue = _ExpiredWaitQueue()
    managed = []

    def manage():
        managed.append('managed')
        if len(managed) == 2:
            executor.stop()

    executor.manage_positions = manage
    executor._run()

    assert managed == ['managed', 'managed']
    assert len(executor._queue.requested_waits) == 3


def test_started_management_accepts_a_fresh_pulse_before_shutdown():
    executor = _loop_controller()
    executor._queue = _ExpiredWaitQueue()
    managed = []

    def manage():
        managed.append('managed')
        if len(managed) == 1:
            # Removal from the queue must release coalescing before management
            # begins, so a new pulse in this window is retained for the next
            # cycle. The real queue drains it before its shutdown sentinel.
            executor.submit_management()
            executor.stop()

    executor.manage_positions = manage
    executor.submit_management()
    executor._run()

    assert managed == ['managed', 'managed']
    assert executor._queue.qsize() == 0
