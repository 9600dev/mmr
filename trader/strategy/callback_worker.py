"""Bounded, stateful strategy callbacks in a separate spawn process.

This is a liveness boundary, NOT a privilege/security sandbox. Strategy code
runs with the service user's OS permissions. It receives no runtime, broker,
RPC client or order token: only context, read-only data facades and price
frames. The parent validates the returned signal and current deployment
generation before granting it any execution authority.

One worker owns one strategy instance. Import, constructor, install and enable
run before READY; callbacks then execute in order on that same instance. A
timeout, crash, invalid return or full input queue permanently fails the worker.
No automatic restart can erase state and silently continue a trading strategy.
Explicit enable/redeployment creates a fresh worker/generation.

Frame and queue bounds do not bound arbitrary strategy heap allocations. An
optional Linux ``memory_limit_bytes`` applies RLIMIT_AS before importing the
strategy; it limits virtual address space, including numerical-library memory
maps. There is no default portable heap limit. Requesting one on another OS is
an explicit error, never a silently ignored safety setting.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
import datetime as dt
import hashlib
import importlib.util
import json
import logging
import math
import multiprocessing
from multiprocessing.connection import Connection
import os
from pathlib import Path
import queue
import sys
import threading
import time
from typing import Any, Callable, TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd
    from trader.trading.strategy import Signal, StrategyContext, StrategyState


class UnsupportedStrategyAPI(RuntimeError):
    """A callback requested an API that has no isolated-runtime contract."""


class CallbackWorkerError(RuntimeError):
    """The strategy process failed and must not receive further bars."""


@dataclass(frozen=True)
class CallbackResult:
    conid: int
    bar_ts: Any
    generation: str
    signal: Signal | None


@dataclass(frozen=True)
class CallbackFailure:
    conid: int | None
    bar_ts: Any
    generation: str
    error_type: str
    message: str


@dataclass(frozen=True)
class _Work:
    conid: int
    frame: Any
    bar_ts: Any
    generation: str
    on_result: Callable[[CallbackResult], None]
    on_error: Callable[[CallbackFailure], None]


class _NoRuntime:
    def __getattr__(self, name):
        raise UnsupportedStrategyAPI(
            f'strategy_runtime.{name}() is unsupported live: strategy code runs in an isolated '
            'child process with NO strategy-runtime mutation API (no subscribe/unsubscribe, no '
            'order placement, no runtime state). Fix the strategy: remove the '
            f'self.strategy_runtime.{name}(...) call — declare every instrument it needs under '
            '`conids:`/`universe:` in strategy_runtime.yaml (the runtime subscribes on its behalf '
            'before enable()) and return a Signal from on_prices for execution.')


class _ReadOnlyData:
    """Accidental mutation guard; deliberately not described as a sandbox."""

    def __init__(self, factory, allowed, kind):
        self._factory = factory
        self._allowed = frozenset(allowed)
        self._kind = kind
        self._value = None

    def __getattr__(self, name):
        if name not in self._allowed:
            raise UnsupportedStrategyAPI(f'{self._kind}.{name} is not a read-only callback API')
        if self._value is None:
            self._value = self._factory()
        value = getattr(self._value, name)
        if name == 'get_tickdata':
            def get_tickdata(*args, **kwargs):
                return _ReadOnlyData(
                    lambda: value(*args, **kwargs),
                    ('read', 'get_data', 'get_date_range', 'date_summary', 'summary',
                     'history', 'date_exists', 'missing', 'list_symbols', 'get_schema'),
                    'tick_data')
            return get_tickdata
        return value


def _snapshot_context(context) -> dict:
    from trader.container import MMR_CONFIG_DIR

    values = {field.name: getattr(context, field.name) for field in fields(context)
              if field.name not in ('storage', 'universe_accessor', 'logger')}
    values['bar_size'] = int(context.bar_size)
    # JSON rejects opaque Python objects before any child has been created.
    values = json.loads(json.dumps(values, allow_nan=False))
    storage_path = getattr(context.storage, 'duckdb_path', None)
    accessor = context.universe_accessor
    universe_path = getattr(accessor, 'duckdb_path', None)
    return {
        'values': values,
        'storage_path': os.path.realpath(os.path.expanduser(storage_path)) if storage_path else None,
        'universe_path': os.path.realpath(os.path.expanduser(universe_path)) if universe_path else None,
        'universe_library': getattr(accessor, 'universe_library', ''),
        'config_dir': str(MMR_CONFIG_DIR),
    }


def _restore_context(snapshot):
    from trader.data.data_access import TickStorage
    from trader.data.universe import UniverseAccessor
    from trader.objects import BarSize
    from trader.trading.strategy import StrategyContext

    def storage():
        if not snapshot['storage_path']:
            raise UnsupportedStrategyAPI('no historical storage was supplied to this callback')
        return TickStorage(snapshot['storage_path'])

    def universe():
        if not snapshot['universe_path']:
            raise UnsupportedStrategyAPI('no universe store was supplied to this callback')
        return UniverseAccessor(snapshot['universe_path'], snapshot['universe_library'])

    values = dict(snapshot['values'])
    values['bar_size'] = BarSize(values['bar_size'])
    values['storage'] = _ReadOnlyData(
        storage, ('get_tickdata', 'read', 'list_libraries', 'list_libraries_barsize'), 'storage')
    values['universe_accessor'] = _ReadOnlyData(
        universe, ('get', 'get_all', 'list_universes', 'list_universes_count',
                   'find_contract', 'resolve_universe', 'resolve_universe_name', 'resolve_symbol'),
        'universe_accessor')
    values['logger'] = logging.getLogger(f"strategy.callback.{values['name']}")
    return StrategyContext(**values)


_MAX_RESPONSE_BYTES = 1024 * 1024


def _json_default(value):
    """Encode the numeric/temporal scalars strategies routinely put in metadata.

    numpy integers/bools, numpy arrays and pandas/py datetimes are not JSON
    native. Without this a strategy that attached ``np.int64`` to its signal
    permanently failed its worker over a serialisation detail rather than a
    trading fault. Non-finite floats stay refused (``allow_nan=False``): a NaN
    signal field is a real strategy fault the parent must not act on.
    """
    if getattr(value, 'shape', None) == () and callable(getattr(value, 'item', None)):
        return value.item()
    if callable(getattr(value, 'tolist', None)):
        return value.tolist()
    if callable(getattr(value, 'isoformat', None)):
        return value.isoformat()
    raise TypeError(f'{type(value).__name__} is not JSON serialisable in a strategy response')


def _send_json(conn, value):
    encoded = json.dumps(value, allow_nan=False, default=_json_default).encode('utf-8')
    if len(encoded) > _MAX_RESPONSE_BYTES:
        raise ValueError('strategy response exceeds 1 MiB')
    conn.send_bytes(encoded)


def _signal_dict(signal):
    from trader.trading.strategy import Signal
    if signal is None:
        return None
    if not isinstance(signal, Signal):
        raise TypeError(f'on_prices must return Signal or None, got {type(signal).__name__}')
    values = {field.name: getattr(signal, field.name) for field in fields(Signal)}
    values['action'] = signal.action.name
    values['date_time'] = signal.date_time.isoformat()
    values['close_by_time'] = signal.close_by_time.isoformat() if signal.close_by_time else None
    return values


def _signal_from_dict(values):
    from trader.objects import Action
    from trader.trading.strategy import Signal
    if values is None:
        return None
    if not isinstance(values, dict) or set(values) != {field.name for field in fields(Signal)}:
        raise ValueError('malformed strategy signal response')
    values = dict(values)
    values['action'] = Action[values['action']]
    values['date_time'] = dt.datetime.fromisoformat(values['date_time'])
    if values['close_by_time'] is not None:
        values['close_by_time'] = dt.time.fromisoformat(values['close_by_time'])
    if not isinstance(values['source_name'], str) or not isinstance(values['metadata'], dict):
        raise ValueError('signal source_name and metadata must be a string and a mapping')
    for key in ('probability', 'risk', 'quantity'):
        if (isinstance(values[key], bool) or not isinstance(values[key], (int, float))
                or not math.isfinite(values[key])):
            raise ValueError(f'signal {key} must be finite numeric data')
    return Signal(**values)


def _apply_memory_limit(limit: int | None) -> None:
    if limit is None:
        return
    if sys.platform != 'linux':
        raise UnsupportedStrategyAPI('memory_limit_bytes requires Linux RLIMIT_AS')
    import resource
    _, hard = resource.getrlimit(resource.RLIMIT_AS)
    if hard != resource.RLIM_INFINITY:
        limit = min(limit, hard)
    resource.setrlimit(resource.RLIMIT_AS, (limit, limit))


def _child_main(conn: Connection, source_path: str, source_hash: str,
                class_name: str, snapshot: dict, initial_state: int,
                max_frame_bytes: int, memory_limit_bytes: int | None = None,
                deployment_config: dict | None = None):
    try:
        # Spawn gives us fresh runtime globals. Carry only the config LOCATION
        # needed by import-time logging, not a live Container or service object.
        import trader.container as container
        container.MMR_CONFIG_DIR = Path(snapshot['config_dir'])
        os.environ['MMR_AUTO_EXECUTE_DISABLED'] = '1'
        os.environ.pop('MMR_AUTO_EXECUTE_LIVE', None)
        import pyarrow as pa
        from trader.strategy.parameters import apply_param_overrides
        from trader.trading.strategy import Strategy, StrategyState

        _apply_memory_limit(memory_limit_bytes)
        source = Path(source_path).read_bytes()
        if hashlib.sha256(source).hexdigest() != source_hash:
            raise ValueError('strategy source changed before isolated initialization')
        module_name = f'_mmr_callback_{source_hash}'
        spec = importlib.util.spec_from_file_location(module_name, source_path)
        if spec is None:
            raise ImportError(f'cannot load strategy source {source_path}')
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        exec(compile(source, source_path, 'exec'), module.__dict__)
        cls = getattr(module, class_name)
        if not isinstance(cls, type) or not issubclass(cls, Strategy) or cls is Strategy:
            raise TypeError(f'{class_name} is not a Strategy subclass')
        if cls.on_prices is Strategy.on_prices:
            raise UnsupportedStrategyAPI('live strategies must implement on_prices; on_bar/on_panel are backtest-only')
        instance = cls()
        instance.strategy_runtime = _NoRuntime()
        context = _restore_context(snapshot)
        raw_params = dict(context.params)
        context.params = {}
        if instance.install(context) is False:
            raise ValueError('strategy.install refused its context')
        apply_param_overrides(instance, raw_params)
        if deployment_config is not None:
            context.effective_config_hash = hashlib.sha256(json.dumps(
                {**deployment_config, 'params': context.params, 'auto_execute': context.auto_execute},
                sort_keys=True, allow_nan=False).encode()).hexdigest()
        if initial_state == int(StrategyState.RUNNING):
            instance.enable()
        else:
            instance.state = StrategyState(initial_state)
        _send_json(conn, {'kind': 'ready', 'effective_params': context.params,
                          'effective_config_hash': context.effective_config_hash,
                          'state': int(instance.state), 'capabilities': {'on_prices': True}})
        while True:
            data = conn.recv_bytes(max_frame_bytes + 1024 * 1024)
            frame = pa.ipc.open_stream(pa.BufferReader(data)).read_pandas()
            signal = instance.on_prices(frame)
            _send_json(conn, {'kind': 'result', 'signal': _signal_dict(signal)})
    except (EOFError, BrokenPipeError):
        pass
    except BaseException as exc:
        try:
            _send_json(conn, {'kind': 'error', 'error_type': type(exc).__name__, 'message': str(exc)[:8000]})
        except BaseException:
            pass
    finally:
        conn.close()


class StrategyCallbackWorker:
    """Nonblocking submit; callbacks carry the caller's immutable authority token.

    ``on_result(CallbackResult)`` and ``on_error(CallbackFailure)`` normally run
    on the supervisor thread. Refused submissions can report errors on the
    submitting thread. Always marshal both onto the parent event loop with
    ``call_soon_threadsafe``. ``max_pending`` counts queued AND in-flight work.
    A process starts only on ``start`` / ``wait_ready``; construction is cheap.
    """

    def __init__(self, source_path: str, source_hash: str, class_name: str,
                 context: StrategyContext, *, initial_state: StrategyState | int = 3,
                 callback_timeout_s: float = 5.0, startup_timeout_s: float = 60.0,
                 max_pending: int = 1, max_frame_bytes: int = 32 * 1024 * 1024,
                 memory_limit_bytes: int | None = None,
                 deployment_config: dict | None = None,
                 on_fatal: Callable[[CallbackFailure], None] | None = None):
        for name, value in (('callback_timeout_s', callback_timeout_s), ('startup_timeout_s', startup_timeout_s)):
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f'{name} must be positive and finite')
        if max_pending < 1 or max_frame_bytes < 1:
            raise ValueError('worker queue and frame limits must be positive')
        if memory_limit_bytes is not None:
            if type(memory_limit_bytes) is not int or memory_limit_bytes <= 0:
                raise ValueError('memory_limit_bytes must be a positive integer')
            if sys.platform != 'linux':
                raise UnsupportedStrategyAPI('memory_limit_bytes requires Linux RLIMIT_AS')
        self._args = (os.path.realpath(source_path), source_hash, class_name,
                      _snapshot_context(context), int(initial_state), max_frame_bytes,
                      memory_limit_bytes, deployment_config)
        self.callback_timeout_s = callback_timeout_s
        self.startup_timeout_s = startup_timeout_s
        self.max_frame_bytes = max_frame_bytes
        self._queue: queue.Queue[_Work] = queue.Queue(maxsize=max_pending)
        self._slots = threading.BoundedSemaphore(max_pending)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._ready = threading.Event()
        self._thread: threading.Thread | None = None
        self._process = None
        self._conn = None
        self._error: CallbackFailure | None = None
        self._last_work: _Work | None = None
        self.ready_metadata: dict = {}
        self._generation = str(getattr(context, 'deployment_generation', ''))
        # Fired exactly once, on the FIRST recorded failure, whether or not a
        # callback was in flight. ``on_error`` is per submitted work; a child
        # that exits while idle has no work to report through, and before this
        # hook such a death left the strategy RUNNING with opening authority
        # and the pulse reading N/N.
        self._on_fatal = on_fatal

    @property
    def pid(self) -> int | None:
        return self._process.pid if self._process is not None else None

    @property
    def failed(self) -> bool:
        return self._error is not None

    def start(self) -> None:
        with self._lock:
            if self._thread is not None:
                return
            if self._stop.is_set():
                raise CallbackWorkerError('strategy worker has been stopped')
            self._thread = threading.Thread(target=self._run, name='strategy-callback-supervisor', daemon=True)
            self._thread.start()

    def wait_ready(self) -> dict:
        self.start()
        if not self._ready.wait(self.startup_timeout_s + 3.0):
            self.stop()
            raise CallbackWorkerError('strategy worker initialization did not finish')
        if self._error is not None:
            raise CallbackWorkerError(f'{self._error.error_type}: {self._error.message}')
        if self._stop.is_set():
            raise CallbackWorkerError('strategy worker stopped before readiness')
        return dict(self.ready_metadata)

    def submit(self, conid: int, frame: pd.DataFrame, bar_ts: Any, generation: str,
               on_result: Callable[[CallbackResult], None],
               on_error: Callable[[CallbackFailure], None]) -> bool:
        work = _Work(conid, frame, bar_ts, generation, on_result, on_error)
        if not self._ready.is_set() or self._stop.is_set():
            error = self._error
            self._notify_error(work, CallbackFailure(
                conid, bar_ts, generation,
                error.error_type if error else 'CallbackWorkerError',
                error.message if error else 'strategy worker is not ready'))
            return False
        if not self._slots.acquire(blocking=False):
            self._fail('CallbackQueueFull', 'strategy callback queue is full; strategy state cannot skip a bar', work)
            return False
        try:
            if int(frame.memory_usage(index=True, deep=True).sum()) > self.max_frame_bytes:
                raise ValueError('strategy price frame exceeds configured byte limit')
            # The parent may share its frame with other strategies. This worker
            # owns a snapshot; neither queued serialization nor strategy writes
            # can change that shared frame.
            work = _Work(conid, frame.copy(deep=True), bar_ts, generation, on_result, on_error)
            self._queue.put_nowait(work)
            return True
        except Exception as exc:
            self._slots.release()
            self._fail(type(exc).__name__, str(exc), work)
            return False

    @staticmethod
    def _notify_error(work, failure):
        try:
            work.on_error(failure)
        except Exception:
            logging.exception('strategy callback error reporter raised')

    def _fail(self, error_type, message, work=None):
        failure = CallbackFailure(
            work.conid if work else None, work.bar_ts if work else None,
            work.generation if work else self._generation, error_type, message)
        with self._lock:
            first = self._error is None
            if first:
                self._error = failure
        self._stop.set()
        if work is not None:
            self._notify_error(work, failure)
        if first and self._on_fatal is not None:
            try:
                self._on_fatal(failure)
            except Exception:
                logging.exception('strategy worker fatal-error reporter raised')

    def _exchange(self, work, timeout):
        """Bound both pipe writes and reads, including a peer halted mid-frame."""
        finished = threading.Event()
        response = []
        errors = []
        conn = self._conn
        if conn is None:
            raise CallbackWorkerError('strategy pipe is unavailable')

        def io():
            try:
                if work is not None:
                    import pyarrow as pa
                    table = pa.Table.from_pandas(work.frame, preserve_index=True)
                    sink = pa.BufferOutputStream()
                    with pa.ipc.new_stream(sink, table.schema) as writer:
                        writer.write_table(table)
                    encoded = sink.getvalue()
                    if encoded.size > self.max_frame_bytes:
                        raise ValueError('serialized strategy frame exceeds configured byte limit')
                    conn.send_bytes(encoded.to_pybytes())
                response.append(json.loads(conn.recv_bytes(_MAX_RESPONSE_BYTES)))
            except BaseException as exc:
                errors.append(exc)
            finally:
                finished.set()

        io_thread = threading.Thread(target=io, name='strategy-callback-pipe', daemon=True)
        io_thread.start()
        deadline = time.monotonic() + timeout
        while not finished.wait(min(.025, max(0.0, deadline - time.monotonic()))):
            if self._stop.is_set():
                raise CallbackWorkerError('strategy callback stopped')
            if time.monotonic() >= deadline:
                raise TimeoutError(f'strategy {"initialization" if work is None else "callback"} exceeded {timeout:g}s')
        if errors:
            raise errors[0]
        return response[0]

    def _run(self):
        try:
            context = multiprocessing.get_context('spawn')
            self._conn, child_conn = context.Pipe(duplex=True)
            self._process = context.Process(target=_child_main, args=(child_conn, *self._args), daemon=True)
            try:
                self._process.start()
            finally:
                child_conn.close()
            ready = self._exchange(None, self.startup_timeout_s)
            if ready.get('kind') != 'ready':
                raise CallbackWorkerError(f"{ready.get('error_type', 'InitializationError')}: {ready.get('message', ready)}")
            self.ready_metadata = ready
            self._ready.set()
            while not self._stop.is_set():
                try:
                    work = self._queue.get(timeout=.05)
                except queue.Empty:
                    if not self._process.is_alive():
                        raise CallbackWorkerError('strategy process exited while idle')
                    continue
                result = None
                try:
                    self._last_work = work
                    response = self._exchange(work, self.callback_timeout_s)
                    if response.get('kind') != 'result':
                        raise CallbackWorkerError(f"{response.get('error_type', 'CallbackError')}: {response.get('message', response)}")
                    signal = _signal_from_dict(response['signal'])
                    result = CallbackResult(work.conid, work.bar_ts, work.generation, signal)
                except BaseException as exc:
                    self._fail(type(exc).__name__, str(exc), work)
                finally:
                    self._slots.release()
                    self._queue.task_done()
                # Completion releases capacity before making the result
                # observable. The caller may submit its next bar immediately
                # from this callback without a spurious queue-full failure.
                if result is not None and not self._stop.is_set():
                    try:
                        work.on_result(result)
                    except BaseException as exc:
                        self._fail(type(exc).__name__, str(exc), work)
        except BaseException as exc:
            self._fail(type(exc).__name__, str(exc), self._last_work)
        finally:
            self._stop.set()
            self._terminate_process()
            self._ready.set()

    def _terminate_process(self):
        process = self._process
        if process is not None and process.pid is not None:
            if process.is_alive():
                process.terminate()
            process.join(timeout=1.0)
            if process.is_alive():
                process.kill()
                process.join(timeout=1.0)
            if process.is_alive():
                logging.error('strategy callback process %s survived kill', process.pid)
        if self._conn is not None:
            self._conn.close()

    def stop(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=3.0)
            if thread.is_alive():
                raise CallbackWorkerError('strategy callback supervisor did not stop')
        while True:
            try:
                self._queue.get_nowait()
            except queue.Empty:
                break
            self._slots.release()
            self._queue.task_done()

    close = stop
