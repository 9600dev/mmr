"""Real runtime/child integration, without a broker or network sockets."""
import asyncio
import os
from unittest.mock import MagicMock

import pandas as pd
import pytest
from ib_async import Contract, Ticker

from review.test_review_strategy_contract import _make_runtime
from trader.trading.strategy import StrategyState


@pytest.mark.asyncio
@pytest.mark.timeout(120)
async def test_import_constructor_and_stateful_callbacks_stay_in_child(tmp_path):
    directory = tmp_path / 'strategies'
    directory.mkdir()
    marker = tmp_path / 'constructor_pid'
    source = directory / 'isolated.py'
    source.write_text(f'''
import os
from trader.trading.strategy import Strategy, Signal
from trader.objects import Action
class Isolated(Strategy):
    THRESHOLD = 20
    def __init__(self):
        super().__init__()
        with open({str(marker)!r}, 'w') as file:
            file.write(str(os.getpid()))
        self.calls = 0
    def on_prices(self, frame):
        assert len(self.ctx.deployment_generation) == 32
        assert len(self.ctx.effective_config_hash) == 64
        self.calls += 1
        return Signal('family-name', Action.BUY, .6, .4, quantity=self.calls)
''')
    runtime = _make_runtime(tmp_path, directory)
    runtime._isolate_callbacks = True
    runtime._loop = asyncio.get_running_loop()
    runtime._load_enabled = lambda name: True
    runtime._last_dispatched_bar = {}
    runtime._oos_bars = {}
    runtime._oos_last = {}
    runtime._oos_logged = set()
    runtime._tick_retention_days = 2
    runtime.event_store = MagicMock()
    runtime.zmq_messagebus_client = MagicMock()
    runtime.auto_executor = MagicMock()
    runtime.auto_executor.open_entry_bar.return_value = None
    await asyncio.to_thread(runtime.load_strategy, 'isolated', '1 min', [1], None, 0,
                            str(source), 'Isolated', '', params={'THRESHOLD': '7'})
    try:
        strategy = runtime.get_strategy('isolated')
        assert strategy is not None
        worker = runtime._callback_workers['isolated']
        assert int(marker.read_text()) == worker.pid != os.getpid()
        assert strategy.ctx.params['THRESHOLD'] == 7
        assert strategy.state == StrategyState.RUNNING
        runtime.strategies[1] = [strategy]
        now = pd.Timestamp.now(tz='UTC').floor('min') - pd.Timedelta(minutes=2)
        for n in range(2):
            stamp = now + pd.Timedelta(minutes=n)
            runtime._strategy_frame = lambda *args, stamp=stamp: pd.DataFrame(
                {'close': [100.]}, index=pd.DatetimeIndex([stamp]))
            ticker = Ticker(contract=Contract(conId=1), time=(stamp + pd.Timedelta(minutes=1)).to_pydatetime())
            ticker.last = 100
            ticker.volume = 100 + n
            runtime.on_ticker_next(ticker)
            deadline = asyncio.get_running_loop().time() + 10
            while runtime.auto_executor.submit_signal.call_count < n + 1:
                assert asyncio.get_running_loop().time() < deadline
                await asyncio.sleep(.01)
        work = [call.args[0] for call in runtime.auto_executor.submit_signal.call_args_list]
        assert [item.quantity for item in work] == [1, 2]
        assert all(item.strategy_name == 'isolated' and not item.auto_execute for item in work)
        await asyncio.gather(*runtime._signal_audit_tasks)
        assert runtime.event_store.append.call_count == 2
        assert all(call.args[0].strategy_name == 'isolated' for call in runtime.event_store.append.call_args_list)
    finally:
        for worker in getattr(runtime, '_callback_workers', {}).values():
            await asyncio.to_thread(worker.stop)


def test_old_worker_error_does_not_revoke_replacement(tmp_path):
    from review.test_review_strategy_contract import runtime_and_strategy
    from types import SimpleNamespace
    runtime, module = runtime_and_strategy(tmp_path)
    old = runtime.get_strategy('review')
    old_generation = old.ctx.deployment_generation
    module.write_text(module.read_text().replace('THRESHOLD = 20', 'THRESHOLD = 21'))
    runtime.load_strategy('review', '1 min', [], None, 0, str(module), 'V1', '', auto_execute=True)
    current = runtime.get_strategy('review')
    current.enable()
    runtime._callback_error(old, SimpleNamespace(generation=old_generation,
                                               error_type='TimeoutError', message='old process'))
    assert current.state == StrategyState.RUNNING
    assert runtime._opening_authorized('review', current.ctx.deployment_generation)


@pytest.mark.asyncio
async def test_signal_audit_lock_does_not_stall_execution_or_event_loop(tmp_path):
    import threading
    from trader.trading.strategy import Strategy, StrategyContext, Signal
    from trader.objects import Action, BarSize
    runtime = _make_runtime(tmp_path, tmp_path)
    runtime._loop = asyncio.get_running_loop()
    entered, release = threading.Event(), threading.Event()
    runtime.event_store = MagicMock()
    def append(event):
        entered.set()
        assert release.wait(5)
    runtime.event_store.append.side_effect = append
    runtime.zmq_messagebus_client = MagicMock()
    runtime.auto_executor = MagicMock()
    strategy = Strategy()
    strategy.install(StrategyContext('audit', BarSize.Mins1, [1], None, 0, False,
                                     None, None, None, auto_execute=True))
    strategy.enable()
    try:
        runtime._handle_signal(strategy, 1, Signal('audit', Action.BUY, .5, .5), pd.Timestamp.now(tz='UTC'))
        runtime.auto_executor.submit_signal.assert_called_once()
        assert await asyncio.to_thread(entered.wait, 2)
        await asyncio.wait_for(asyncio.sleep(.01), timeout=.2)
        assert len(runtime._signal_audit_tasks) == 1
    finally:
        release.set()
        await asyncio.gather(*runtime._signal_audit_tasks)
