"""A failed startup replay must not disable opens until something else happens.

``replay_execution_history`` sets ``begin_replay()`` then awaits two bounded
broker requests; on a timeout the tracker's ``replay_required`` stays True and
``margin_checks`` refuses every open. ``_replay_broker_executions`` used to be
called only from ``setup_subscriptions`` and ``execution_snapshot``, so a 5s
IB hiccup at connect was a silent, indefinite opening outage. It now retries
on the pulse scheduler while replay is required, logging each failure and the
recovery.
"""
import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from ib_async import Contract, Order

from trader.trading.order_lifecycle import OrderLifecycleTracker
from trader.trading.trading_runtime import Trader


def _trader(completed_side_effect):
    t = object.__new__(Trader)
    t.ib_account = 'DU1'
    t.order_tracker = OrderLifecycleTracker(None)
    t._execution_history_ready = False
    t._execution_replay_lock = asyncio.Lock()
    t._execution_replay_completed_epoch = None
    t._execution_replay_failed = False
    t._main_loop = None
    t.disposables = []
    t.scheduler = None
    t.client = SimpleNamespace(ib=SimpleNamespace(
        isConnected=lambda: True,
        reqCompletedOrdersAsync=AsyncMock(side_effect=completed_side_effect),
        reqExecutionsAsync=AsyncMock(return_value=[]),
        trades=lambda: []))
    return t


@pytest.mark.asyncio
async def test_failed_replay_is_retried_and_recovery_is_logged(caplog):
    trader = _trader([asyncio.TimeoutError(), []])
    try:
        with caplog.at_level(logging.INFO):
            assert await trader._replay_broker_executions() is False
            assert trader.order_tracker.health['replay_required'] is True
            assert trader._execution_history_ready is False
            assert trader._execution_replay_completed_epoch is None
            failure = [r.getMessage() for r in caplog.records if 'replay incomplete' in r.getMessage()]
            assert failure and 'retrying every 30s' in failure[0], failure

            task = trader._retry_execution_replay_if_required()
            assert task is not None, 'replay is still required, so the tick must run an attempt'
            assert await task is True

        assert trader.order_tracker.health['replay_required'] is False
        assert trader._execution_history_ready is True
        assert trader._execution_replay_completed_epoch is not None
        assert any('replay RECOVERED' in r.getMessage() for r in caplog.records)
        assert trader._retry_execution_replay_if_required() is None, 'nothing to do once complete'
    finally:
        trader.order_tracker.close(timeout=1)


@pytest.mark.asyncio
async def test_retry_tick_is_a_no_op_while_disconnected_or_in_flight():
    trader = _trader([asyncio.TimeoutError()])
    try:
        assert await trader._replay_broker_executions() is False
        trader.client.ib.isConnected = lambda: False
        assert trader._retry_execution_replay_if_required() is None, 'the reconnect path replays'
        trader.client.ib.isConnected = lambda: True
        async with trader._execution_replay_lock:
            assert trader._retry_execution_replay_if_required() is None, 'an attempt is already running'
    finally:
        trader.order_tracker.close(timeout=1)


def test_retry_is_scheduled_next_to_the_pulse():
    trader = _trader([[]])
    scheduled = []

    class _Scheduler:
        def schedule_periodic(self, period, action):
            scheduled.append((period, action))
            return SimpleNamespace(dispose=lambda: None)

    trader.scheduler = _Scheduler()
    try:
        trader._start_execution_replay_retry()
        (period, action), = scheduled
        assert period == Trader._EXECUTION_REPLAY_RETRY_S == 30
        assert len(trader.disposables) == 1, 'torn down with the other subscriptions on reconnect'
        # The scheduled action never raises off the loop (no running loop here).
        assert action(None) is None
    finally:
        trader.order_tracker.close(timeout=1)


@pytest.mark.asyncio
async def test_margin_checks_refuse_opens_until_the_retry_recovers():
    """The observable consequence: an open is refused while replay is
    required and admitted once the retried replay completes."""
    trader = _trader([asyncio.TimeoutError(), []])
    trader.duckdb_path = None
    trader._journal_degraded = ''
    trader.risk_gate = None
    try:
        assert await trader._replay_broker_executions() is False
        refused = await trader.margin_checks(Contract(conId=1, secType='STK'), Order())
        assert not refused.approved and 'execution journal degraded' in refused.reason
        assert await trader._retry_execution_replay_if_required() is True
        # Past the replay gate; the next refusal (no risk gate) is a different one.
        later = await trader.margin_checks(Contract(conId=1, secType='CASH'), Order())
        assert later.approved
    finally:
        trader.order_tracker.close(timeout=1)
