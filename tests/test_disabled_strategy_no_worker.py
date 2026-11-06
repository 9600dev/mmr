"""A strategy that loads DISABLED gets no callback-worker process; enable starts one."""
import asyncio
from unittest.mock import MagicMock

import pytest

from review.test_review_strategy_contract import _make_runtime
from trader.trading.strategy import StrategyState

SOURCE = """
from trader.trading.strategy import Strategy, Signal
class Quiet(Strategy):
    THRESHOLD = 20
    def on_prices(self, frame):
        return None
"""


def _runtime(tmp_path, enabled):
    directory = tmp_path / 'strategies'
    directory.mkdir()
    (directory / 'quiet.py').write_text(SOURCE)
    runtime = _make_runtime(tmp_path, directory)
    runtime._isolate_callbacks = True
    runtime._load_enabled = lambda name: enabled
    runtime._persist_enabled = lambda name, value: None
    runtime.event_store = MagicMock()
    runtime.zmq_messagebus_client = MagicMock()
    runtime.auto_executor = MagicMock()
    return runtime, str(directory / 'quiet.py')


@pytest.mark.asyncio
@pytest.mark.timeout(120)
async def test_disabled_strategy_has_no_worker_until_enabled(tmp_path):
    runtime, source = _runtime(tmp_path, enabled=False)
    runtime._loop = asyncio.get_running_loop()
    await asyncio.to_thread(runtime.load_strategy, 'quiet', '1 min', [1], None, 0,
                            source, 'Quiet', '', params={'THRESHOLD': '7'})
    try:
        strategy = runtime.get_strategy('quiet')
        assert strategy is not None and strategy.state == StrategyState.DISABLED
        assert 'quiet' not in getattr(runtime, '_callback_workers', {})
        assert len(strategy.ctx.effective_config_hash) == 64

        state = await asyncio.to_thread(runtime.enable_strategy, 'quiet')
        assert state == StrategyState.RUNNING
        worker = runtime._callback_workers['quiet']
        assert worker.pid
        assert strategy.ctx.params['THRESHOLD'] == 7          # worker applied the override
        assert len(strategy.ctx.effective_config_hash) == 64

        await asyncio.to_thread(runtime.disable_strategy, 'quiet')
        assert 'quiet' not in runtime._callback_workers
    finally:
        for worker in list(getattr(runtime, '_callback_workers', {}).values()):
            await asyncio.to_thread(worker.stop)


@pytest.mark.asyncio
@pytest.mark.timeout(120)
async def test_enabled_strategy_still_gets_worker_at_load(tmp_path):
    runtime, source = _runtime(tmp_path, enabled=True)
    runtime._loop = asyncio.get_running_loop()
    await asyncio.to_thread(runtime.load_strategy, 'quiet', '1 min', [1], None, 0,
                            source, 'Quiet', '', params={})
    try:
        assert 'quiet' in runtime._callback_workers
        assert runtime.get_strategy('quiet').state == StrategyState.RUNNING
    finally:
        for worker in list(getattr(runtime, '_callback_workers', {}).values()):
            await asyncio.to_thread(worker.stop)
