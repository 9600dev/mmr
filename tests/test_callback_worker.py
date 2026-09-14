"""Process-isolated callback contract; no broker or operational state is used."""

import hashlib
import logging
import queue
import subprocess
import sys
import textwrap
import threading
import time
from types import SimpleNamespace

import pandas as pd
import pytest

from trader.objects import BarSize
from trader.strategy.callback_worker import CallbackWorkerError, StrategyCallbackWorker
from trader.trading.strategy import StrategyContext, StrategyState


pytestmark = pytest.mark.timeout(120)


@pytest.fixture
def worker_factory(tmp_path, monkeypatch):
    import trader.container as container
    monkeypatch.setattr(container, 'MMR_CONFIG_DIR', tmp_path / 'config')
    workers = []

    def create(source, *, name='Isolated', params=None, **limits):
        path = tmp_path / f'{len(workers)}_strategy.py'
        path.write_text(textwrap.dedent(source))
        context = StrategyContext(
            name=name, bar_size=BarSize.Mins1, conids=[123], universe=None,
            historical_days_prior=0, paper_only=True,
            storage=SimpleNamespace(duckdb_path=str(tmp_path / 'history.duckdb')),
            universe_accessor=None, logger=logging.getLogger('callback-test'),
            params=params or {}, deployment_generation='generation-1',
        )
        worker = StrategyCallbackWorker(
            str(path), hashlib.sha256(path.read_bytes()).hexdigest(), name,
            context, **limits)
        workers.append(worker)
        return worker, context

    yield create
    for worker in workers:
        worker.stop()
        assert worker._process is None or not worker._process.is_alive()


def _frame():
    return pd.DataFrame(
        {'open': [100.0], 'high': [101.0], 'low': [99.0], 'close': [100.0], 'volume': [5.0]},
        index=pd.DatetimeIndex(['2026-09-08T16:00:00Z'], name='date'),
    )


def _submit(worker, frame=None, generation='generation-1'):
    results, errors = queue.Queue(), queue.Queue()
    frame = _frame() if frame is None else frame
    accepted = worker.submit(123, frame, frame.index[-1], generation, results.put, errors.put)
    return accepted, results, errors


def test_child_preserves_state_typed_params_and_parent_frame(worker_factory):
    worker, context = worker_factory('''
        from trader.trading.strategy import Strategy, Signal
        from trader.objects import Action
        class Isolated(Strategy):
            PERIOD = 2
            def __init__(self):
                super().__init__()
                self.seen = 0
                self.enable_count = 0
            def enable(self):
                self.enable_count += 1
                return super().enable()
            def on_prices(self, prices):
                self.seen += 1
                prices.iloc[0, prices.columns.get_loc('close')] = -999
                return Signal(self.name, Action.BUY, .6, .2,
                              quantity=self.seen, metadata={
                                  'period': self.PERIOD, 'typed': type(self.PERIOD).__name__,
                                  'enabled': self.enable_count, 'state': int(self.state)})
    ''', params={'PERIOD': '5'}, callback_timeout_s=5)
    metadata = worker.wait_ready()
    assert metadata['effective_params'] == {'PERIOD': 5}
    assert context.params == {'PERIOD': '5'}
    assert metadata['capabilities']['on_prices'] is True
    original = _frame()
    for expected in (1, 2):
        accepted, results, errors = _submit(worker, original)
        assert accepted
        result = results.get(timeout=8)
        assert errors.empty()
        assert result.signal.quantity == expected
        assert result.signal.metadata == {'period': 5, 'typed': 'int', 'enabled': 1, 'state': int(StrategyState.RUNNING)}
        assert result.conid == 123 and result.generation == 'generation-1'
        assert result.bar_ts == original.index[-1]
        assert original.iloc[0]['close'] == 100


def test_completed_result_releases_capacity_before_next_submission(worker_factory):
    worker, _ = worker_factory('''
        from trader.trading.strategy import Strategy, Signal
        from trader.objects import Action
        class Isolated(Strategy):
            def __init__(self):
                super().__init__()
                self.seen = 0
            def on_prices(self, prices):
                self.seen += 1
                return Signal(self.name, Action.BUY, .5, .2, quantity=self.seen)
    ''', max_pending=1)
    worker.wait_ready()
    finished = threading.Event()
    results, errors, accepted = [], [], []
    frame = _frame()

    def on_error(error):
        errors.append(error)
        finished.set()

    def on_result(result):
        results.append(result)
        if len(results) == 1:
            accepted.append(worker.submit(
                123, frame, frame.index[-1], 'generation-1', on_result, on_error))
        else:
            finished.set()

    assert worker.submit(123, frame, frame.index[-1], 'generation-1', on_result, on_error)
    assert finished.wait(8)
    assert not errors, errors
    assert accepted == [True]
    assert [result.signal.quantity for result in results] == [1, 2]


def test_hung_callback_times_out_and_cannot_block_another_worker(worker_factory):
    hung, _ = worker_factory('''
        import time
        from trader.trading.strategy import Strategy
        class Isolated(Strategy):
            def on_prices(self, prices):
                while True:
                    time.sleep(.01)
    ''', callback_timeout_s=.4)
    healthy, _ = worker_factory('''
        from trader.trading.strategy import Strategy, Signal
        from trader.objects import Action
        class Isolated(Strategy):
            def on_prices(self, prices):
                return Signal(self.name, Action.SELL, .5, .2)
    ''')
    hung.start()
    healthy.start()
    hung.wait_ready()
    healthy.wait_ready()
    started = time.monotonic()
    accepted, results, errors = _submit(hung)
    accepted_healthy, healthy_results, healthy_errors = _submit(healthy)
    assert accepted and accepted_healthy
    assert healthy_results.get(timeout=5).signal.action.name == 'SELL'
    error = errors.get(timeout=5)
    assert error.error_type == 'TimeoutError'
    assert 'callback exceeded' in error.message
    assert time.monotonic() - started < 5
    assert results.empty() and healthy_errors.empty()
    hung.stop()
    assert hung.failed and not hung._process.is_alive()


def test_saturation_fails_loudly_instead_of_skipping_a_stateful_bar(worker_factory):
    worker, _ = worker_factory('''
        import time
        from trader.trading.strategy import Strategy
        class Isolated(Strategy):
            def on_prices(self, prices):
                time.sleep(30)
    ''', callback_timeout_s=10, max_pending=1)
    worker.wait_ready()
    accepted, results, errors = _submit(worker)
    assert accepted
    accepted_second, second_results, second_errors = _submit(worker)
    assert not accepted_second
    assert second_errors.get(timeout=2).error_type == 'CallbackQueueFull'
    worker.stop()
    assert worker.failed
    assert results.empty() and second_results.empty()


@pytest.mark.parametrize('expression, message', [
    ('self.strategy_runtime.subscribe(self, None)', 'strategy_runtime.subscribe'),
    ('self.storage.write(123, prices)', 'storage.write'),
    ('self.storage.get_tickdata(self.bar_size).delete(123)', 'tick_data.delete'),
])
def test_unsupported_runtime_and_mutation_apis_are_explicit(worker_factory, expression, message):
    source = '''
        from trader.trading.strategy import Strategy
        class Isolated(Strategy):
            def on_prices(self, prices):
                EXPRESSION
    '''.replace('EXPRESSION', expression)
    worker, _ = worker_factory(source)
    worker.wait_ready()
    accepted, results, errors = _submit(worker)
    assert accepted
    error = errors.get(timeout=5)
    assert 'UnsupportedStrategyAPI' in error.message and message in error.message
    assert results.empty()


@pytest.mark.parametrize('source, expected', [
    ('''
        from trader.trading.strategy import Strategy
        class Isolated(Strategy):
            def __init__(self):
                raise ValueError('constructor failed in child')
            def on_prices(self, prices):
                return None
    ''', 'constructor failed in child'),
    ('''
        from trader.trading.strategy import Strategy
        class Isolated(Strategy):
            def on_bar(self, prices, state, index):
                return None
    ''', 'must implement on_prices'),
])
def test_initialization_failure_is_reported_before_ready(worker_factory, source, expected):
    worker, _ = worker_factory(source)
    with pytest.raises(CallbackWorkerError, match=expected):
        worker.wait_ready()
    assert worker.failed


def test_worker_checks_source_hash_in_child(worker_factory):
    worker, _ = worker_factory('''
        from trader.trading.strategy import Strategy
        class Isolated(Strategy):
            def on_prices(self, prices):
                return None
    ''')
    from pathlib import Path
    path = Path(worker._args[0])
    path.write_text(path.read_text() + '\n# changed after deployment was checked\n')
    with pytest.raises(CallbackWorkerError, match='source changed'):
        worker.wait_ready()


def test_startup_deadline_and_cleanup_are_bounded(worker_factory):
    worker, _ = worker_factory('''
        from trader.trading.strategy import Strategy
        class Isolated(Strategy):
            def on_prices(self, prices):
                return None
    ''', startup_timeout_s=.001)
    started = time.monotonic()
    with pytest.raises(CallbackWorkerError, match='initialization exceeded'):
        worker.wait_ready()
    assert time.monotonic() - started < 4
    assert worker._process is not None and not worker._process.is_alive()


def test_frame_byte_limit_rejects_before_delivery(worker_factory):
    worker, _ = worker_factory('''
        from trader.trading.strategy import Strategy
        class Isolated(Strategy):
            def on_prices(self, prices):
                return None
    ''', max_frame_bytes=8)
    worker.wait_ready()
    accepted, results, errors = _submit(worker)
    assert not accepted and results.empty()
    assert 'byte limit' in errors.get(timeout=2).message


def test_non_signal_result_refuses_and_stops_worker(worker_factory):
    worker, _ = worker_factory('''
        from trader.trading.strategy import Strategy
        class Isolated(Strategy):
            def on_prices(self, prices):
                return {'action': 'BUY'}
    ''')
    worker.wait_ready()
    accepted, results, errors = _submit(worker)
    assert accepted and results.empty()
    assert 'must return Signal or None' in errors.get(timeout=5).message


def test_explicit_memory_limit_is_not_silently_ignored_on_other_platforms(monkeypatch):
    from trader.strategy.callback_worker import _apply_memory_limit, UnsupportedStrategyAPI
    monkeypatch.setattr('trader.strategy.callback_worker.sys.platform', 'unsupported')
    with pytest.raises(UnsupportedStrategyAPI, match='requires Linux'):
        _apply_memory_limit(128 * 1024 * 1024)


@pytest.mark.skipif(sys.platform != 'linux', reason='RLIMIT_AS enforcement is Linux-only')
def test_linux_memory_limit_refuses_oversized_allocation_in_child():
    # The cap is applied only in the disposable child; no parent memory or
    # global resource limit is changed by this test.
    result = subprocess.run(
        [sys.executable, '-c',
         'from trader.strategy.callback_worker import _apply_memory_limit; '
         '_apply_memory_limit(128*1024*1024); bytearray(256*1024*1024)'],
        capture_output=True, text=True, timeout=20,
    )
    assert result.returncode != 0 and 'MemoryError' in result.stderr
