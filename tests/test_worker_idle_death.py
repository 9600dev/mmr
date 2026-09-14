"""A callback worker that dies IDLE is reported to the runtime (2026-09-14).

The supervisor noticed the child was gone (``strategy process exited while
idle``) and failed the worker, but ``_fail`` only notified the runtime through
the in-flight work's ``on_error`` — and an idle death has none. The strategy
stayed RUNNING with ``_opening_authorized`` True and the pulse read N/N. The
worker now fires ``on_fatal`` once on its first recorded failure regardless of
in-flight work, and the runtime moves the strategy to ERROR and revokes its
opening authority.
"""
import asyncio
import hashlib
import logging
import queue
import textwrap
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from trader.objects import BarSize
from trader.strategy.callback_worker import StrategyCallbackWorker
from trader.trading.strategy import StrategyContext, StrategyState

pytestmark = pytest.mark.timeout(120)

DIES_IDLE = '''
    import os, threading
    from trader.trading.strategy import Strategy
    class Isolated(Strategy):
        def __init__(self):
            super().__init__()
            # Exit shortly AFTER readiness, with no callback in flight.
            threading.Timer(0.6, os._exit, [3]).start()
        def on_prices(self, prices):
            return None
'''


def _worker(tmp_path, monkeypatch, on_fatal):
    import trader.container as container
    monkeypatch.setattr(container, 'MMR_CONFIG_DIR', tmp_path / 'config')
    path = tmp_path / 'dies_idle.py'
    path.write_text(textwrap.dedent(DIES_IDLE))
    context = StrategyContext(
        name='Isolated', bar_size=BarSize.Mins1, conids=[123], universe=None,
        historical_days_prior=0, paper_only=True,
        storage=SimpleNamespace(duckdb_path=str(tmp_path / 'history.duckdb')),
        universe_accessor=None, logger=logging.getLogger('callback-test'),
        params={}, deployment_generation='generation-1')
    return StrategyCallbackWorker(str(path), hashlib.sha256(path.read_bytes()).hexdigest(),
                                  'Isolated', context, on_fatal=on_fatal)


def test_idle_child_exit_fires_on_fatal_once_with_the_generation(tmp_path, monkeypatch):
    fatal = queue.Queue()
    worker = _worker(tmp_path, monkeypatch, fatal.put)
    try:
        worker.wait_ready()
        failure = fatal.get(timeout=15)
        assert failure.error_type == 'CallbackWorkerError'
        assert 'exited while idle' in failure.message
        assert failure.conid is None and failure.bar_ts is None
        assert failure.generation == 'generation-1', 'the runtime can match it to the deployment'
        assert worker.failed
        with pytest.raises(queue.Empty):
            fatal.get(timeout=0.5)                      # exactly once
    finally:
        worker.stop()


def test_on_fatal_is_not_fired_for_a_deliberate_stop(tmp_path, monkeypatch):
    fatal = queue.Queue()
    import trader.container as container
    monkeypatch.setattr(container, 'MMR_CONFIG_DIR', tmp_path / 'config')
    path = tmp_path / 'healthy.py'
    path.write_text(textwrap.dedent('''
        from trader.trading.strategy import Strategy
        class Isolated(Strategy):
            def on_prices(self, prices):
                return None
    '''))
    context = StrategyContext(
        name='Isolated', bar_size=BarSize.Mins1, conids=[123], universe=None,
        historical_days_prior=0, paper_only=True,
        storage=SimpleNamespace(duckdb_path=str(tmp_path / 'history.duckdb')),
        universe_accessor=None, logger=logging.getLogger('callback-test'),
        params={}, deployment_generation='generation-1')
    worker = StrategyCallbackWorker(str(path), hashlib.sha256(path.read_bytes()).hexdigest(),
                                    'Isolated', context, on_fatal=fatal.put)
    worker.wait_ready()
    worker.stop()
    assert not worker.failed
    assert fatal.empty(), 'an operator stop is not a worker death'


def test_runtime_reports_idle_worker_death_without_a_service_loop(tmp_path):
    """The runtime-side handler: ERROR, authority revoked, pulse reflects it."""
    from trader.strategy.callback_worker import CallbackFailure
    from trader.strategy.strategy_runtime import StrategyRuntime
    from trader.trading.strategy import Strategy
    rt = StrategyRuntime.__new__(StrategyRuntime)
    rt.strategy_implementations, rt.strategies, rt.streams = [], {}, {}
    rt._last_dispatched_bar, rt._oos_bars = {}, {}
    strategy = Strategy()
    strategy.install(StrategyContext(
        name='idle', bar_size=BarSize.Mins1, conids=[1], universe=None, historical_days_prior=0,
        paper_only=False, storage=None, universe_accessor=None, logger=logging, params={},
        auto_execute=True))
    strategy.enable()
    rt.strategy_implementations.append(strategy)
    rt._grant_generation(strategy)
    generation = strategy.ctx.deployment_generation
    rt._callback_workers = {'idle': SimpleNamespace(pid=None, failed=True)}
    assert rt._opening_authorized('idle', generation)
    assert rt.runtime_status()['strategies_running'] == 1

    rt._post_callback(rt._callback_error, strategy,
                      CallbackFailure(None, None, generation, 'CallbackWorkerError',
                                      'strategy process exited while idle'))

    assert strategy.state == StrategyState.ERROR
    assert not rt._opening_authorized('idle', generation)
    status = rt.runtime_status()
    assert status['strategies_running'] == 0 and status['strategies_total'] == 1
    assert status['callback_workers']['idle']['failed'] is True


@pytest.mark.asyncio
async def test_runtime_moves_strategy_to_error_when_its_worker_dies_idle(tmp_path):
    from review.test_review_strategy_contract import _make_runtime
    directory = tmp_path / 'strategies'
    directory.mkdir()
    source = directory / 'dies_idle.py'
    source.write_text(textwrap.dedent(DIES_IDLE))
    runtime = _make_runtime(tmp_path, directory)
    runtime._isolate_callbacks = True
    runtime._loop = asyncio.get_running_loop()
    runtime._load_enabled = lambda name: True
    runtime._last_dispatched_bar, runtime._oos_bars = {}, {}
    runtime.event_store = MagicMock()
    runtime.zmq_messagebus_client = MagicMock()
    runtime.auto_executor = MagicMock()
    await asyncio.to_thread(runtime.load_strategy, 'dies_idle', '1 min', [1], None, 0,
                            str(source), 'Isolated', '', auto_execute=True)
    try:
        strategy = runtime.get_strategy('dies_idle')
        assert strategy is not None and strategy.state == StrategyState.RUNNING
        generation = strategy.ctx.deployment_generation
        assert runtime._opening_authorized('dies_idle', generation)
        deadline = asyncio.get_running_loop().time() + 20
        while strategy.state != StrategyState.ERROR:
            assert asyncio.get_running_loop().time() < deadline, 'idle worker death never reached the runtime'
            await asyncio.sleep(0.05)
        assert not runtime._opening_authorized('dies_idle', generation)
        status = runtime.runtime_status()
        assert status['strategies_running'] == 0
        assert status['callback_workers']['dies_idle']['failed'] is True
    finally:
        for worker in list(runtime._callback_workers.values()):
            await asyncio.to_thread(worker.stop)
