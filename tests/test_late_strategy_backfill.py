"""A strategy deployed AFTER startup is backfilled and held like one loaded at
startup (audit 2026-11-14, B7).

The per-instrument history backfill (pending -> fetch -> release, three
attempts) ran for the strategies loaded when the service started. One added
later — a YAML entry picked up by the reconcile loop, ``mmr strategies deploy``
/ ``reload`` — was subscribed and dispatched bars straight away, with whatever
the store already held: no IB backfill, no hold. For an intraday strategy that
is a session missing everything before the deploy.

Uses the runtime harness of ``tests/test_strategy_runtime_b3.py``.
"""
import asyncio
import os
import sys
import time
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent))

from test_strategy_runtime_b3 import (  # noqa: E402
    FARM_NOTICE, M1, Recorder, _contract, _feed, _fetcher, _history, _install, _runtime, _serviceable,
    _start, _stop, runtime_log)  # noqa: F401
from trader.listeners.ib_history_worker import IBConnectivityError  # noqa: E402
from trader.strategy.strategy_runtime import _MAX_BACKFILL_ATTEMPTS  # noqa: E402
from trader.trading.strategy import StrategyState  # noqa: E402

LATE_SOURCE = (
    'from trader.trading.strategy import Strategy\n'
    'class Late(Strategy):\n'
    '    def __init__(self):\n'
    '        super().__init__()\n'
    '        self.calls = []\n'
    '    def on_prices(self, prices):\n'
    '        self.calls.append(prices)\n'
    '        return None\n')


def _deploy(rt, tmp_path, name='late', conid=7, days=5):
    """Add a strategy to the YAML the way ``mmr strategies deploy`` does."""
    directory = tmp_path / 'strategies'
    directory.mkdir(exist_ok=True)
    (directory / 'late.py').write_text(LATE_SOURCE)
    import yaml
    config = yaml.safe_load(Path(rt.strategy_config_file).read_text()) or {'strategies': []}
    config['strategies'].append({'name': name, 'module': 'late.py', 'class_name': 'Late',
                                 'bar_size': '1 min', 'conids': [conid], 'historical_days_prior': days})
    Path(rt.strategy_config_file).write_text(yaml.safe_dump(config))
    # A changed mtime whatever the filesystem's timestamp resolution.
    stamp = os.path.getmtime(rt.strategy_config_file) + 5 + len(config['strategies'])
    os.utime(rt.strategy_config_file, (stamp, stamp))


def _loadable(rt):
    """What ``connect()`` would have provided for ``load_strategy``; every
    strategy is enabled as it loads."""
    rt.storage = None
    rt.universe_accessor = None
    rt._load_enabled = lambda name: True
    return rt


async def _until(condition, real_sleep, task=None, seconds=20):
    deadline = time.monotonic() + seconds
    while not condition() and time.monotonic() < deadline and (task is None or not task.done()):
        await real_sleep(0.01)
    return condition()


def _running_service(rt, monkeypatch):
    """``run()`` with the network faked, strategies enabled as they load, and a
    counter of reconcile-loop ticks."""
    real_sleep = _serviceable(rt, monkeypatch)
    _loadable(rt)
    ticks = {'n': 0}
    rt._log_pulse = lambda: ticks.__setitem__('n', ticks['n'] + 1)
    return real_sleep, ticks


class TestLateDeploymentThroughTheService:
    @pytest.mark.asyncio
    @pytest.mark.timeout(60)
    async def test_late_strategy_is_backfilled_and_held_until_it_is(self, tmp_path, monkeypatch):
        rt = _runtime(tmp_path)
        real_sleep, ticks = _running_service(rt, monkeypatch)
        gate = asyncio.Event()
        attempted = []

        async def fetch(security, bar_size, historical_days, strategy_name):
            attempted.append((security.conId, strategy_name, historical_days))
            await gate.wait()                                    # a slow IB history request
        rt._fetch_history_with_resume = fetch

        task = asyncio.ensure_future(rt.run())
        try:
            assert await _until(lambda: ticks['n'] >= 1, real_sleep, task), 'the reconcile loop is running'
            assert attempted == [], 'empty roster at startup: nothing to backfill'

            _deploy(rt, tmp_path)                                # ... and the reconcile loop picks it up
            assert await _until(lambda: rt.get_strategy('late') is not None and 7 in rt.strategies,
                                real_sleep, task), 'loaded and subscribed by the reconcile loop'
            strategy = rt.get_strategy('late')
            assert strategy.state == StrategyState.RUNNING

            assert await _until(lambda: attempted, real_sleep, task, seconds=5), \
                'a strategy deployed after startup never had its history backfilled'
            assert attempted == [(7, 'late', 5)]

            # While the backfill is in flight the strategy is held, not fed.
            start = _start()
            rt._hist_bars[(7, M1)] = _history(start)
            sent = _feed(rt, _contract(7, 'TEST'), start, minutes=3)
            assert strategy.calls == [], 'dispatched bars before its history arrived'
            assert rt._backfill_due(7, M1)
            assert (7, 'late') not in rt._last_dispatched_bar, 'held, not consumed'

            gate.set()
            assert await _until(lambda: (7, M1) in rt._backfill_done, real_sleep, task)
            assert not rt._backfill_pending and not rt._backfill_failures
            _feed(rt, _contract(7, 'TEST'), start, minutes=1, first_tick=sent)
            assert len(strategy.calls) == 1, 'released once the instrument is primed'
            assert len(strategy.calls[0]) >= 30 + 3
        finally:
            gate.set()
            await _stop(task)
        assert attempted == [(7, 'late', 5)], 'fetched once'

    @pytest.mark.asyncio
    @pytest.mark.timeout(60)
    async def test_late_backfill_that_keeps_failing_is_bounded_like_startup(self, tmp_path, monkeypatch,
                                                                            runtime_log):
        rt = _runtime(tmp_path)
        real_sleep, ticks = _running_service(rt, monkeypatch)
        attempted = _fetcher(rt, lambda conid, call: Exception(FARM_NOTICE))

        task = asyncio.ensure_future(rt.run())
        try:
            assert await _until(lambda: ticks['n'] >= 1, real_sleep, task)
            _deploy(rt, tmp_path)
            assert await _until(
                lambda: rt._backfill_failures.get((7, M1), {}).get('attempts', 0) >= _MAX_BACKFILL_ATTEMPTS,
                real_sleep, task), 'the late backfill was never attempted, or never gave up'
            settled = ticks['n']
            assert await _until(lambda: ticks['n'] >= settled + 3, real_sleep, task)
        finally:
            await _stop(task)
        assert [conid for conid, _, _ in attempted] == [7] * _MAX_BACKFILL_ATTEMPTS, 'three attempts, then left alone'
        assert not rt._backfill_due(7, M1), 'the hold ends when the retries are exhausted'
        assert not rt._backfill_pending
        assert sum('giving up' in message for message in runtime_log['error']) == 1

    @pytest.mark.asyncio
    @pytest.mark.timeout(60)
    async def test_gateway_down_during_a_late_backfill_leaves_nothing_pending(self, tmp_path, monkeypatch):
        rt = _runtime(tmp_path)
        real_sleep, ticks = _running_service(rt, monkeypatch)
        calls = []

        async def fetch(security, bar_size, historical_days, strategy_name):
            calls.append((security.conId, set(rt._backfill_pending)))
            if len(calls) == 1:
                raise IBConnectivityError('error_code: 1100')
        rt._fetch_history_with_resume = fetch
        from unittest.mock import AsyncMock
        rt._reconnect_historical_client = AsyncMock()

        task = asyncio.ensure_future(rt.run())
        try:
            assert await _until(lambda: ticks['n'] >= 1, real_sleep, task)
            _deploy(rt, tmp_path)
            assert await _until(lambda: (7, M1) in rt._backfill_done, real_sleep, task), \
                'not retried after a connectivity error'
        finally:
            await _stop(task)
        assert [conid for conid, _ in calls] == [7, 7]
        assert calls[1][1] == set(), 'after the failed pass it is a recorded failure, not pending for ever'
        rt._reconnect_historical_client.assert_awaited()
        assert not rt._backfill_failures and not rt._backfill_pending

    @pytest.mark.asyncio
    @pytest.mark.timeout(60)
    async def test_reload_rpc_starts_the_backfill_without_waiting_for_the_next_tick(self, tmp_path, monkeypatch):
        """``mmr strategies deploy`` / ``reload``: ``reload_strategies`` loads
        the config and reconciles on the service loop."""
        import trader.strategy.strategy_runtime as runtime_module
        from trader.messaging.strategy_service_api import StrategyServiceApi
        rt = _runtime(tmp_path)
        real_sleep, ticks = _running_service(rt, monkeypatch)
        attempted = _fetcher(rt)
        # Once ``stalled`` is set the reconcile LOOP sleeps for good at its next
        # 30 s wait, so only the RPC can plan or start anything.
        stalled = {'on': False}
        fast_sleep = runtime_module.asyncio.sleep

        async def sleep(delay, *args, **kwargs):
            if stalled['on'] and delay >= 30:
                await real_sleep(3600)
            else:
                await fast_sleep(delay, *args, **kwargs)
        monkeypatch.setattr(runtime_module.asyncio, 'sleep', sleep)

        task = asyncio.ensure_future(rt.run())
        try:
            assert await _until(lambda: ticks['n'] >= 1, real_sleep, task)
            stalled['on'] = True
            seen = -1
            while seen != ticks['n']:                            # the loop has gone to sleep
                seen = ticks['n']
                await real_sleep(0.1)

            _deploy(rt, tmp_path)
            reply = await StrategyServiceApi(rt).reload_strategies()
            assert reply.is_success(), reply.error
            assert (7, M1) in rt._backfill_pending or (7, M1) in rt._backfill_done, \
                'the reload returned with the new instrument neither pending nor primed'
            assert await _until(lambda: (7, M1) in rt._backfill_done, real_sleep, task, seconds=5), \
                'the backfill waited for a reconcile tick that is not coming'
            assert ticks['n'] == seen, 'the reconcile loop stayed asleep throughout'
        finally:
            await _stop(task)
        assert attempted == [(7, 'late', 5)]


class TestLatePlanning:
    """The planning step on its own (what ``_reconcile_sync`` does between
    loading the config and subscribing), on a runtime marked as serving."""

    def _serving(self, tmp_path):
        rt = _loadable(_runtime(tmp_path))
        rt._backfill_serving = True
        return rt

    def test_instrument_already_primed_is_neither_refetched_nor_held(self, tmp_path):
        rt = self._serving(tmp_path)
        _install(rt, Recorder(), 1, name='first', days=5)
        attempted = _fetcher(rt)
        asyncio.run(rt.get_historical_data())                    # startup
        _install(rt, Recorder(), 1, name='second', days=5)       # deployed later, same instrument
        rt._plan_late_backfills()
        assert not rt._backfill_pending and not rt._backfill_due(1, M1, days=5)
        asyncio.run(rt._retry_backfills())
        assert attempted == [(1, 'first', 5)]

    def test_wider_window_on_a_primed_instrument_is_fetched_and_holds_only_the_late_strategy(self, tmp_path):
        rt = self._serving(tmp_path)
        _install(rt, Recorder(), 1, name='short', days=5)
        attempted = _fetcher(rt)
        asyncio.run(rt.get_historical_data())
        _install(rt, Recorder(), 1, name='long', days=90)
        rt._plan_late_backfills()

        assert rt._backfill_pending == {(1, M1)}
        assert rt._backfill_due(1, M1, days=90), 'the late strategy waits for the 90 days it declared'
        assert not rt._backfill_due(1, M1, days=5), 'a running strategy whose window is primed is not interrupted'

        asyncio.run(rt._retry_backfills())
        assert attempted == [(1, 'short', 5), (1, 'short', 90)], 'one fetch, with the widest window'
        assert not rt._backfill_pending and (1, M1) in rt._backfill_done
        assert not rt._backfill_due(1, M1, days=90)

    def test_running_strategy_keeps_its_bars_while_a_late_one_is_held(self, tmp_path):
        rt = self._serving(tmp_path)
        short = _install(rt, Recorder(), 7, name='short', days=5)
        _fetcher(rt)
        asyncio.run(rt.get_historical_data())
        long = _install(rt, Recorder(), 7, name='long', days=90)
        rt._plan_late_backfills()
        start = _start()
        rt._hist_bars[(7, M1)] = _history(start)
        sent = _feed(rt, _contract(7, 'TEST'), start, minutes=2)
        assert len(short.calls) >= 1 and long.calls == []
        asyncio.run(rt._retry_backfills())
        _feed(rt, _contract(7, 'TEST'), start, minutes=1, first_tick=sent)
        assert len(long.calls) == 1

    def test_a_strategy_is_planned_once(self, tmp_path):
        rt = self._serving(tmp_path)
        _install(rt, Recorder(), 1, name='late', days=5)
        _fetcher(rt, lambda conid, call: Exception(FARM_NOTICE))
        rt._plan_late_backfills()
        for _ in range(_MAX_BACKFILL_ATTEMPTS + 2):
            asyncio.run(rt._retry_backfills())
            rt._plan_late_backfills()                            # every reconcile tick
        assert rt._backfill_failures[(1, M1)]['attempts'] == _MAX_BACKFILL_ATTEMPTS
        assert not rt._backfill_pending and not rt._backfill_due(1, M1)

    @pytest.mark.asyncio
    async def test_reconcile_does_not_spend_the_retries_of_a_recorded_failure(self, tmp_path):
        """A reload RPC starts FIRST attempts at once; the bounded retry of an
        instrument that already failed keeps the reconcile loop's cadence."""
        rt = self._serving(tmp_path)
        rt._backfill_startup_done = True
        # The strategy is installed directly, not through the YAML: no reload.
        rt._config_mtime = os.path.getmtime(rt.strategy_config_file)
        _install(rt, Recorder(), 1, name='failing', days=5)
        attempted = _fetcher(rt, lambda conid, call: Exception(FARM_NOTICE))
        await rt.get_historical_data()                           # startup: attempt 1
        for _ in range(4):                                       # reload, reload, reload ...
            await rt._reconcile()
            task = rt.__dict__.get('_backfill_task')
            if task is not None:
                await task
        assert rt._backfill_failures[(1, M1)]['attempts'] == 1 and len(attempted) == 1

        rt._schedule_backfill_retry()                            # the loop's own tick
        await rt._backfill_task
        assert rt._backfill_failures[(1, M1)]['attempts'] == 2

    def test_a_strategy_that_declares_no_history_is_not_planned_into_a_hold(self, tmp_path):
        rt = self._serving(tmp_path)
        strategy = _install(rt, Recorder(), 7, name='nohist', days=0)
        rt._plan_late_backfills()
        start = _start()
        rt._hist_bars[(7, M1)] = _history(start)
        _feed(rt, _contract(7, 'TEST'), start, minutes=2)
        assert len(strategy.calls) >= 1

    def test_reconcile_without_a_running_service_plans_nothing(self, tmp_path):
        """Nothing would ever run the pass, so nothing may be held for it."""
        rt = _loadable(_runtime(tmp_path))
        _deploy(rt, tmp_path)
        rt._reconcile_sync()
        assert rt.get_strategy('late') is not None
        assert not rt._backfill_pending and not rt._backfill_due(7, M1)

    def test_reconcile_on_a_serving_runtime_holds_the_new_instrument_before_it_subscribes(self, tmp_path):
        rt = self._serving(tmp_path)
        _deploy(rt, tmp_path)
        seen = {}
        subscribe = rt.subscribe

        def spy(strategy, contract):
            seen[contract.conId] = rt._backfill_due(contract.conId, strategy.bar_size)
            return subscribe(strategy, contract)
        rt.subscribe = spy
        rt._reconcile_sync()
        assert seen == {7: True}, 'pending from before its first possible bar'
