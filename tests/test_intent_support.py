"""Shared fakes for the intent/executor regression files. No tests live here.

``SnapshotSDK`` extends the review ``LifecycleSDK`` with the
completeness-bearing ``execution_snapshot`` RPC the production executor uses
(the frame adapter is the compatibility path), recording every call so a test
can count broker round-trips. ``clock`` replaces the executor module's ``time``
so backoff schedules can be driven without sleeping. ``build_executor`` builds a
real ``AutoExecutor`` over a temporary DuckDB with the same environment the
recovery fixtures use.
"""
import threading
import time as _time

import pandas as pd
import pytest

from review.test_review_strategy_contract import FakeResult, LifecycleSDK, TS, make_work  # noqa: F401
from trader.strategy import auto_executor as auto_executor_module
from trader.strategy.auto_executor import AutoExecutor

OWNER, CONID = 'orb_test', 1111


class SnapshotSDK(LifecycleSDK):
    """LifecycleSDK plus the RPC snapshot; ``snapshot_calls`` counts round-trips."""

    def __init__(self):
        super().__init__()
        self.snapshot_calls = []      # (intent_id, order_ids) per execution_snapshot call
        self.retry_safe = False       # the trader's RETRYABLE-claim proof for scoped reads
        self.complete = True

    def execution_snapshot(self, intent_id='', order_ids=None):
        self.snapshot_calls.append((intent_id, tuple(order_ids or ())))
        frame = self.trades()
        positions = self.positions()
        return dict(complete=self.complete,
                    orders=frame.to_dict('records') if not frame.empty else [],
                    positions_complete=True,
                    positions=positions.to_dict('records') if not positions.empty else [],
                    retry_safe=bool(self.retry_safe and intent_id), intent_outcome=None,
                    orders_complete=self.complete, executions_complete=self.complete)

    def scoped_calls(self):
        return [call for call in self.snapshot_calls if call[0]]


class FakeClock:
    def __init__(self):
        self.now = _time.time()

    def time(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


@pytest.fixture
def clock(monkeypatch):
    """Controllable wall clock for the executor module (it only uses time.time())."""
    fake = FakeClock()
    monkeypatch.setattr(auto_executor_module, 'time', fake)
    return fake


def build_executor(tmp_path, sdk, monkeypatch, *, name='executor', protective_pct='8'):
    monkeypatch.delenv('MMR_AUTO_EXECUTE_DISABLED', raising=False)
    monkeypatch.setenv('MMR_PROTECTIVE_STOP_PCT', protective_pct)
    monkeypatch.setattr(AutoExecutor, '_now_utc',
                        lambda self: (TS.tz_localize('UTC') + pd.Timedelta(seconds=70)).to_pydatetime())
    path = str(tmp_path / f'{name}.duckdb')
    return AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk), path


def inline_worker(executor):
    """Let the calling thread stand in for the worker so operator acts run inline.

    The stand-in is already running, so ``start()`` must be a no-op too.
    """
    executor._worker = threading.current_thread()
    executor._started = True
    return executor


def open_owned(executor, quantity=140):
    executor._process_signal(make_work(quantity=quantity))
    position = executor.state.open_position(OWNER, CONID)
    assert position is not None and position['quantity'] == quantity
    return position
