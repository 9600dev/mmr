"""Bounded waits, retry evidence and lifecycle workers; no broker calls."""

import asyncio
import datetime as dt
import math
import threading
from types import SimpleNamespace

import pytest

from trader.data.event_store import EventStore, EventType, TradingEvent
from trader.data.execution_journal import ExecutionJournal
from trader.trading import order_lifecycle
from trader.trading.order_lifecycle import OrderLifecycleTracker


class _Clock:
    def __init__(self):
        self.elapsed = 0.0

    def monotonic(self):
        return self.elapsed

    def sleep(self, duration):
        assert duration > 0
        self.elapsed += duration


class _Wake:
    def set(self):
        pass

    def clear(self):
        pass

    def wait(self, timeout=None):
        return False


def test_default_flush_budget_is_five_seconds_under_a_blocked_outbox(monkeypatch):
    tracker = OrderLifecycleTracker()
    clock = _Clock()
    monkeypatch.setattr(order_lifecycle, "time", clock)
    monkeypatch.setattr(tracker, "_pending_outbox", lambda: True)
    assert tracker.flush() is False
    assert 5.0 <= clock.elapsed <= 5.01
    tracker.close(timeout=0.0)


def test_flush_at_its_exact_deadline_does_not_start_another_storage_probe(monkeypatch):
    tracker = OrderLifecycleTracker()
    times = iter([0.0, 5.0])
    monkeypatch.setattr(order_lifecycle, "time", SimpleNamespace(monotonic=lambda: next(times)))

    def expired_probe():
        pytest.fail("an expired flush must not begin another potentially blocking journal probe")

    monkeypatch.setattr(tracker, "_pending_outbox", expired_probe)
    assert tracker.flush() is False


@pytest.mark.parametrize("explicit_timeout", [None, 0.01])
def test_close_joins_both_workers_with_a_finite_requested_grace(explicit_timeout):
    tracker = OrderLifecycleTracker()
    bound = 5.0 if explicit_timeout is None else explicit_timeout

    class HeldWorker:
        joined = False

        def join(self, timeout=None):
            assert timeout is not None and math.isfinite(timeout)
            assert 0 <= timeout <= bound, "shutdown cannot turn a bounded join into an unbounded wait"
            self.joined = True

    first, second = HeldWorker(), HeldWorker()
    tracker._worker, tracker._delivery_worker = first, second
    if explicit_timeout is None:
        tracker.close()
    else:
        tracker.close(timeout=explicit_timeout)
    assert first.joined and second.joined


def test_broker_workers_do_not_keep_the_process_alive_after_owner_exit(tmp_path, monkeypatch):
    workers = []

    class Worker:
        def __init__(self, *, target, name=None, daemon=None):
            self.daemon = bool(daemon)
            self.started = False
            workers.append(self)

        def start(self):
            self.started = True

        def join(self, timeout=None):
            assert timeout is not None

    # Isolate only lifecycle's threading namespace; do not replace Python's
    # global Thread class used by unrelated fixtures or logging.
    monkeypatch.setattr(order_lifecycle, "threading", SimpleNamespace(
        RLock=threading.RLock, Lock=threading.Lock, Event=threading.Event, Thread=Worker,
    ))
    tracker = OrderLifecycleTracker(EventStore(str(tmp_path / "workers.duckdb")))
    try:
        assert len(workers) == 2
        assert all(worker.started and worker.daemon for worker in workers)
    finally:
        tracker.close(timeout=0.0)


def _pending_submission(tmp_path):
    # Model an initialized durable journal whose delivery worker is unavailable.
    tracker = OrderLifecycleTracker()
    tracker._journal = ExecutionJournal(str(tmp_path / "pending-events"))
    event = TradingEvent(
        event_type=EventType.ORDER_SUBMITTED,
        timestamp=dt.datetime(2026, 9, 9, 12), strategy_name="owner", conid=123,
        order_id=17, quantity=1.0, price=10.0,
        metadata={"submission_identity": "pending-attempt"},
    )
    return tracker, event


def test_unavailable_submission_journal_preserves_the_refusal_cause():
    tracker = OrderLifecycleTracker()
    event = TradingEvent(
        event_type=EventType.ORDER_SUBMITTED,
        timestamp=dt.datetime(2026, 9, 9, 12), strategy_name="owner",
        metadata={"submission_identity": "not-yet-durable"},
    )
    with pytest.raises(RuntimeError) as error:
        tracker.record_submission(event)
    assert "journal" in str(error.value).casefold()
    assert "unavailable" in str(error.value).casefold()
    assert tracker.health["pending_submissions"] == 0


def test_default_submission_timeout_keeps_its_durable_reservation_and_never_waits_at_zero(
    tmp_path, monkeypatch,
):
    tracker, event = _pending_submission(tmp_path)
    clock = _Clock()
    monkeypatch.setattr(order_lifecycle, "time", clock)

    class SubmissionWake(_Wake):
        def wait(self, timeout=None):
            assert timeout is not None and timeout > 0, "the deadline must be checked before waiting"
            clock.sleep(timeout)
            return False

    tracker._submission_wake = SubmissionWake()
    try:
        with pytest.raises(TimeoutError):
            tracker.record_submission(event)
        assert 5.0 <= clock.elapsed <= 5.01
        assert tracker.health["pending_submissions"] == 1
        assert tracker.health["healthy"] is False
        assert tracker._pending_outbox() is True
    finally:
        tracker.close(timeout=0.0)


def test_submission_wait_recognizes_closed_owner_within_a_quarter_second(tmp_path, monkeypatch):
    tracker, event = _pending_submission(tmp_path)
    clock = _Clock()
    monkeypatch.setattr(order_lifecycle, "time", clock)

    class ClosingWake(_Wake):
        def wait(self, timeout=None):
            clock.sleep(timeout)
            tracker._closed.set()
            return False

    tracker._submission_wake = ClosingWake()
    try:
        with pytest.raises(TimeoutError):
            tracker.record_submission(event)
        assert clock.elapsed <= 0.25
        assert tracker.health["pending_submissions"] == 1
    finally:
        tracker.close(timeout=0.0)


def test_default_decisive_wait_passes_a_bounded_ten_second_deadline(monkeypatch):
    tracker = OrderLifecycleTracker()

    async def wait_for(future, timeout):
        assert 0 < timeout <= 10.0
        future.cancel()
        raise asyncio.TimeoutError

    monkeypatch.setattr(order_lifecycle, "asyncio", SimpleNamespace(
        get_event_loop=asyncio.get_event_loop, wait_for=wait_for, TimeoutError=asyncio.TimeoutError,
    ))
    assert asyncio.run(tracker.wait_decisive(17)) == "timeout"


@pytest.mark.parametrize("loop_name", ["_run", "_delivery_loop"])
def test_retries_preserve_changed_error_facts_deduplicate_repeats_and_recover(loop_name, monkeypatch):
    tracker = OrderLifecycleTracker()
    errors = ["E17 storage lease unavailable", "E17 storage lease unavailable", "E19 storage write denied"]
    attempts = []
    warnings = []
    health_at_wait = []

    def warning(message, *args):
        warnings.append(str(message) % args if args else str(message))

    def attempt(*args):
        attempts.append(args)
        if len(attempts) <= len(errors):
            raise OSError(errors[len(attempts) - 1])
        tracker._closed.set()

    class RetryWake(_Wake):
        def wait(self, timeout=None):
            health_at_wait.append(tracker.health["error"])
            return False

    tracker._wake = tracker._delivery_wake = RetryWake()
    monkeypatch.setattr(order_lifecycle.logging, "warning", warning)
    if loop_name == "_run":
        tracker._queue.put_nowait(({}, {"identity": "retained-observation"}))
        monkeypatch.setattr(tracker, "_persist_observation", attempt)
    else:
        monkeypatch.setattr(tracker, "_deliver", attempt)
    getattr(tracker, loop_name)()
    assert len(attempts) == 4
    assert len(warnings) == 2
    assert "E17" in warnings[0] and "E19" in warnings[1]
    # Recovery diagnostics must identify the failing stage and its retry,
    # alongside the changing cause already asserted above.
    stage = "journal" if loop_name == "_run" else "outbox"
    assert all(stage in message.casefold() for message in warnings)
    assert all("retry" in message.casefold() for message in warnings)
    assert ["E17" in value for value in health_at_wait[:2]] == [True, True]
    assert "E19" in health_at_wait[2]
    assert tracker.health["error"] == ""
    assert tracker.health["pending"] == 0


def test_selected_callback_buffer_capacity_fails_closed_at_the_next_observation(monkeypatch):
    tracker = OrderLifecycleTracker()
    # Occupy the selected 8,192-slot buffer without thousands of broker-object
    # constructions. The next callback cannot become unrecorded authority.
    for _ in range(8192):
        tracker._queue.put_nowait(None)
    critical_messages = []

    def critical(message, *args, **kwargs):
        critical_messages.append(str(message) % args if args else str(message))

    monkeypatch.setattr(order_lifecycle.logging, "critical", critical)
    trade = SimpleNamespace(
        order=SimpleNamespace(orderId=17, clientId=7, permId=1717, account="paper-A",
                              orderRef="owner", action="BUY", orderType="MKT", totalQuantity=1.0),
        orderStatus=SimpleNamespace(status="Submitted", filled=0.0, avgFillPrice=0.0),
        contract=SimpleNamespace(conId=123, symbol="OWN"),
    )
    tracker.on_trade(trade)
    assert tracker.health["replay_required"] is True
    assert tracker.health["healthy"] is False
    # The operator needs the loss condition and the recovery action; exact
    # punctuation, capitalization and the rest of the sentence are irrelevant.
    assert any("queue" in message.casefold() and "full" in message.casefold()
               for message in critical_messages)
    assert any("replay" in message.casefold() and "required" in message.casefold()
               for message in critical_messages)
    tracker.close(timeout=0.0)
