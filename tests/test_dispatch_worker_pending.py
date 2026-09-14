"""A RUNNING strategy with no registered callback worker defers bars (review 2026-09-11).

``enable_strategy`` runs on a worker thread: it flipped the strategy to RUNNING
and granted a generation seconds before ``_callback_workers`` held its worker.
A bar dispatched in that window raised, set ERROR and revoked the strategy's
opening authority while the enable RPC reported success. The dispatcher now
defers the bar (watermark untouched) and dispatches once the worker exists.
"""
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pandas as pd
from ib_async import Contract

from review.test_review_market_data_contract import _frame, _runtime, _ticker
from trader.objects import BarSize
from trader.trading.strategy import Strategy, StrategyContext, StrategyState

CONID, NAME = 1, 'isolated'


def _dispatch_runtime(tmp_path):
    runtime = _runtime(tmp_path)
    runtime._isolate_callbacks = True
    runtime._callback_workers = {}
    runtime._contracts = {}
    runtime._live_bar_buffers = {}
    runtime._managed_contexts = {}
    runtime.auto_executor = MagicMock()
    runtime.auto_executor.open_entry_bar.return_value = None
    runtime._bar_in_session = lambda *args: True
    strategy = Strategy()
    strategy.install(StrategyContext(
        name=NAME, bar_size=BarSize.Mins1, conids=[CONID], universe=None, historical_days_prior=0,
        paper_only=False, storage=None, universe_accessor=None, logger=logging, params={}))
    strategy.enable()
    runtime.strategies = {CONID: [strategy]}
    runtime.strategy_implementations = [strategy]
    return runtime, strategy


def _tick(runtime, stamp):
    runtime._strategy_frame = lambda *args: _frame([stamp])
    runtime.on_ticker_next(_ticker(Contract(conId=CONID), stamp + pd.Timedelta(minutes=1), 100.0, 100))


def test_bar_before_worker_registration_is_deferred_not_fatal(tmp_path, caplog):
    runtime, strategy = _dispatch_runtime(tmp_path)
    stamp = pd.Timestamp('2026-09-10 14:30', tz='UTC')

    with caplog.at_level(logging.WARNING):
        _tick(runtime, stamp)
        _tick(runtime, stamp)

    assert strategy.state == StrategyState.RUNNING, 'not ERROR: the enable is still completing'
    assert (CONID, NAME) not in runtime._last_dispatched_bar, 'the bar is retried, not consumed'
    pending = [r for r in caplog.records if 'callback worker is not registered yet' in r.getMessage()]
    assert len(pending) == 1, 'logged once per strategy, not per bar'

    worker = SimpleNamespace(submit=Mock(return_value=True), pid=123, failed=False)
    runtime._callback_workers[NAME] = worker
    _tick(runtime, stamp)

    worker.submit.assert_called_once()
    assert worker.submit.call_args.args[0] == CONID
    assert runtime._last_dispatched_bar[(CONID, NAME)] == stamp


def test_runtime_status_snapshots_the_worker_table(tmp_path):
    runtime, _strategy = _dispatch_runtime(tmp_path)
    runtime._last_dispatched_bar = {}
    runtime._oos_bars = {}
    runtime._callback_workers = {NAME: SimpleNamespace(pid=7, failed=False)}
    runtime._deployment_generations = {}
    workers = runtime._callback_workers

    class _Mutating(dict):
        def items(self):
            # A concurrent enable/disable changes the table mid-iteration.
            workers['late'] = SimpleNamespace(pid=8, failed=False)
            return super().items()

    runtime._callback_workers = _Mutating(workers)
    status = runtime.runtime_status()
    assert status['callback_workers'][NAME] == {'pid': 7, 'failed': False}
