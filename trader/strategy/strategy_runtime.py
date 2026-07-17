from ib_async import Contract
from ib_async.ib import IB
from ib_async.ticker import Ticker
from reactivex.observer import AutoDetachObserver
from trader.common.exceptions import TraderConnectionException, TraderException
from trader.common.helpers import dateify
from trader.common.logging_helper import get_callstack, log_method, setup_logging
from trader.data.market_data import normalize_ticker
from trader.data.store import DateRange

from trader.data.data_access import SecurityDefinition, TickStorage
from trader.data.universe import UniverseAccessor
from trader.listeners.ib_history_worker import IBHistoryWorker, IBConnectivityError, IBNoDataError
from trader.messaging.clientserver import (
    MessageBusClient,
    MultithreadedTopicPubSub,
    RPCClient,
    RPCServer,
    TopicPubSub
)
from trader.objects import Action, BarSize, WhatToShow
from trader.data.event_store import EventStore, EventType, TradingEvent
from trader.strategy.auto_executor import AutoExecutor, SignalWork
from trader.trading.strategy import Signal, Strategy, StrategyConfig, StrategyContext, StrategyState
from typing import cast, Dict, List, Optional

import asyncio
import backoff
import datetime as dt
import exchange_calendars
import importlib
import importlib.util
import inspect
import os
import pandas as pd
import sys
import trader.messaging.strategy_service_api as bus
import yaml


logging = setup_logging(module_name='strategy_runtime')


error_table = {
    'trader.common.exceptions.TraderException': TraderException,
    'trader.common.exceptions.TraderConnectionException': TraderConnectionException
}


def _whattoshow_for_contract(contract: Contract) -> WhatToShow:
    """Pick the right IB whatToShow for an instrument's secType.

    For OHLCV history:
      * STK / FUT / IND / OPT  -> TRADES (real prints, larger IB chunk caps,
        not throttled the way MIDPOINT is, and matches what humans see on
        a chart).
      * CASH (FX spot)         -> MIDPOINT (FX has no consolidated tape,
        so trade prints don't exist; MIDPOINT is the only sensible bar).
      * anything else / unset  -> TRADES (safe default; IB will surface a
        162 "no data" if it's the wrong choice and the caller can retry).

    Override at the call site only if the strategy genuinely needs
    quote-mid history (e.g. spread modeling).
    """
    sec_type = (contract.secType or '').upper()
    if sec_type == 'CASH':
        return WhatToShow.MIDPOINT
    return WhatToShow.TRADES


def _count_recent(index, now_utc: pd.Timestamp, seconds: int) -> int:
    """Rows of a DatetimeIndex within the trailing window. Naive indexes are
    treated as UTC (matching ``_strategy_frame``'s convention)."""
    if index is None or len(index) == 0:
        return 0
    try:
        cutoff = now_utc - pd.Timedelta(seconds=seconds)
        if getattr(index, 'tz', None) is None:
            cutoff = cutoff.tz_localize(None)
        return int((index >= cutoff).sum())
    except Exception:
        return 0


def _age_seconds(ts, now_utc: pd.Timestamp) -> Optional[int]:
    """Whole seconds between a bar timestamp and now (naive ts = UTC)."""
    try:
        t = pd.Timestamp(ts)
        t = t.tz_localize('UTC') if t.tz is None else t.tz_convert('UTC')
        return int((now_utc - t).total_seconds())
    except Exception:
        return None


def build_runtime_status(
    now_utc: pd.Timestamp,
    strategies,
    streams: Dict[int, pd.DataFrame],
    last_dispatched_bar: Dict[tuple, pd.Timestamp],
    auto_exec_open: int,
) -> dict:
    """Pure snapshot of pipeline health: strategy states, tick flow per conId,
    freshest dispatched-bar age per conId, and open auto-exec positions.

    Consumed two ways: formatted by ``format_pulse`` into the periodic log
    line, and returned raw by the ``runtime_status`` RPC for `mmr verify` /
    healthchecks. Pure function of its inputs so tests can drive it with
    fabricated state.
    """
    states = {}
    running = 0
    for s in strategies:
        state = getattr(s, 'state', None)
        if state == StrategyState.RUNNING:
            running += 1
        ctx = getattr(s, '_context', None)
        states[getattr(s, 'name', None) or '?'] = {
            'state': getattr(state, 'value', None),
            'state_name': getattr(state, 'name', str(state)),
            'auto_execute': bool(getattr(ctx, 'auto_execute', False)) if ctx else False,
        }

    ticks_60s = {int(conid): _count_recent(getattr(df, 'index', None), now_utc, 60)
                 for conid, df in streams.items()}

    bar_age_s: Dict[int, int] = {}
    for key, ts in last_dispatched_bar.items():
        try:
            conid = int(key[0])
        except (TypeError, ValueError, IndexError):
            continue
        age = _age_seconds(ts, now_utc)
        if age is None:
            continue
        if conid not in bar_age_s or age < bar_age_s[conid]:
            bar_age_s[conid] = age

    return {
        'ts': str(now_utc),
        'strategies': states,
        'strategies_running': running,
        'strategies_total': len(states),
        'ticks_60s': ticks_60s,
        'bar_age_s': bar_age_s,
        'auto_exec_open': int(auto_exec_open),
    }


def format_pulse(status: dict) -> str:
    """One greppable INFO line per interval. ``ticks_60s=0`` for a subscribed
    conId during market hours is the escalate condition — a dead feed shows
    up as this line going to zero, not as an absence of errors."""
    ticks = ','.join(f'{k}:{v}' for k, v in sorted(status.get('ticks_60s', {}).items()))
    ages = ','.join(f'{k}:{v}' for k, v in sorted(status.get('bar_age_s', {}).items()))
    return (
        'pulse strategies={running}/{total} ticks_60s=[{ticks}] '
        'bar_age_s=[{ages}] auto_exec_open={open}'.format(
            running=status.get('strategies_running', 0),
            total=status.get('strategies_total', 0),
            ticks=ticks,
            ages=ages,
            open=status.get('auto_exec_open', 0),
        )
    )


class StrategyRuntime():
    def __init__(
        self,
        ib_server_address: str,
        ib_server_port: int,
        strategy_runtime_ib_client_id: int,
        duckdb_path: str,
        universe_library: str,
        zmq_pubsub_server_address: str,
        zmq_pubsub_server_port: int,
        zmq_rpc_server_address: str,
        zmq_rpc_server_port: int,
        zmq_strategy_rpc_server_address: str,
        zmq_strategy_rpc_server_port: int,
        zmq_messagebus_server_address: str,
        zmq_messagebus_server_port: int,
        strategies_directory: str,
        strategy_config_file: str,
        history_duckdb_path: str = '',
        paper_trading: bool = False,
        simulation: bool = False,
        trading_mode: str = 'paper',
    ):
        self.ib_server_address = ib_server_address
        self.ib_server_port = ib_server_port
        self.strategy_runtime_ib_client_id: int = strategy_runtime_ib_client_id
        self.duckdb_path = duckdb_path
        self.history_duckdb_path = history_duckdb_path or duckdb_path
        self.universe_library = universe_library
        self.simulation: bool = simulation
        self.paper_trading = paper_trading
        # trading_mode comes from trader.yaml (the same key the trader service
        # uses); paper_trading above predates it and is not Container-resolved.
        self.trading_mode = trading_mode
        self.zmq_pubsub_server_address = zmq_pubsub_server_address
        self.zmq_pubsub_server_port = zmq_pubsub_server_port
        self.zmq_rpc_server_address = zmq_rpc_server_address
        self.zmq_rpc_server_port = zmq_rpc_server_port
        self.zmq_strategy_rpc_server_address = zmq_strategy_rpc_server_address
        self.zmq_strategy_rpc_server_port = zmq_strategy_rpc_server_port
        self.zmq_messagebus_server_address = zmq_messagebus_server_address
        self.zmq_messagebus_server_port = zmq_messagebus_server_port

        self.strategies_directory = strategies_directory
        self.strategy_config_file = strategy_config_file
        self.startup_time: dt.datetime = dt.datetime.now()
        self.last_connect_time: dt.datetime

        self.zmq_strategy_rpc_server: RPCServer[bus.StrategyServiceApi]
        self.zmq_messagebus_client: MessageBusClient

        # todo: this is wrong as we'll have a whole bunch of different tickdata libraries for
        # different bartypes etc.
        self.storage: TickStorage

        self.universe_accessor: UniverseAccessor

        self.strategies: Dict[int, List[Strategy]] = {}
        self.strategy_implementations: List[Strategy] = []
        self.streams: Dict[int, pd.DataFrame] = {}
        # Historical OHLCV bars per (conId, bar_size), loaded from the DB on
        # subscribe. These PRIME the live frame so bar-based strategies have
        # warmup + today's opening bars — the live tick stream alone only holds
        # ticks since subscription.
        self._hist_bars: Dict[tuple, pd.DataFrame] = {}
        # Last completed bar timestamp dispatched per (conId, strategy name), so
        # a strategy sees each bar once (not on every tick).
        self._last_dispatched_bar: Dict[tuple, pd.Timestamp] = {}
        # Keep at most this many days of raw ticks per conid (bounds compute).
        self._tick_retention_days: int = 2

        self.historical_data_client: IBHistoryWorker

    def create_strategy_exception(self, exception_type: type, message: str, inner: Optional[Exception]):
        # todo use reflection here to automatically populate trader runtime vars that we care about
        # given a particular exception type
        data = self.storage if hasattr(self, 'data') else None
        last_connect_time = self.last_connect_time if hasattr(self, 'last_connect_time') else dt.datetime.min

        exception = exception_type(
            message,
            data is not None,
            False,
            self.startup_time,
            last_connect_time,
            inner,
            get_callstack(10)
        )
        logging.exception(exception)
        return exception

    @backoff.on_exception(backoff.expo, ConnectionRefusedError, max_tries=10, max_time=120)
    def connect(self):
        """Synchronous setup: wire up dependencies that don't need the loop.

        Anything that binds ZMQ sockets or creates asyncio tasks is deferred
        to ``run()``, which is async. Calling ``asyncio.run(coro)`` from this
        method used to spin up a throwaway loop and orphan the server task —
        the socket was bound, no task ever ran, requests silently piled up.
        """
        # avoids circular import
        from trader.messaging.trader_service_api import TraderServiceApi
        try:
            self.storage = TickStorage(self.history_duckdb_path)
            self.universe_accessor = UniverseAccessor(self.duckdb_path, self.universe_library)
            self.event_store = EventStore(self.duckdb_path)
            # G6: bridges auto_execute strategy signals to orders via the
            # proposal pipeline. Runs on its own worker thread; submitting
            # work never blocks the tick feed.
            self.auto_executor = AutoExecutor(
                duckdb_path=self.duckdb_path,
                paper_trading=(self.trading_mode == 'paper'),
                event_store=self.event_store,
            )
            self.trader_client = RPCClient[TraderServiceApi](
                zmq_server_address=self.zmq_rpc_server_address,
                zmq_server_port=self.zmq_rpc_server_port,
                error_table=error_table
            )
            self.last_connect_time = dt.datetime.now()

            self.zmq_strategy_rpc_server = RPCServer[bus.StrategyServiceApi](
                instance=bus.StrategyServiceApi(self),
                zmq_rpc_server_address=self.zmq_strategy_rpc_server_address,
                zmq_rpc_server_port=self.zmq_strategy_rpc_server_port,
            )

            self.zmq_messagebus_client = MessageBusClient(
                zmq_address=self.zmq_messagebus_server_address,
                zmq_port=self.zmq_messagebus_server_port,
            )

        except Exception as ex:
            raise self.create_strategy_exception(
                TraderConnectionException,
                message='strategy_runtime.connect() exception', inner=ex
            )

    @log_method
    def _persist_enabled(self, name: str, enabled: bool) -> None:
        """Persist a strategy's enabled/disabled state so it survives a restart
        (otherwise a runtime disable is silently undone when the config reloads)."""
        try:
            from trader.data.duckdb_store import DuckDBConnection
            db = DuckDBConnection.get_instance(self.duckdb_path)

            def _w(conn):
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS strategy_state "
                    "(name VARCHAR PRIMARY KEY, enabled BOOLEAN, updated_at TIMESTAMP)")
                conn.execute("DELETE FROM strategy_state WHERE name = ?", [name])
                conn.execute(
                    "INSERT INTO strategy_state (name, enabled, updated_at) VALUES (?, ?, ?)",
                    [name, bool(enabled), dt.datetime.now()])
            db.execute_atomic(_w)
        except Exception as ex:
            logging.warning('could not persist enabled-state for %s: %s', name, ex)

    def _load_enabled(self, name: str):
        """Return the persisted enabled state for *name* (True/False), or None if
        it was never explicitly enabled/disabled."""
        try:
            from trader.data.duckdb_store import DuckDBConnection
            db = DuckDBConnection.get_instance(self.duckdb_path)

            def _r(conn):
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS strategy_state "
                    "(name VARCHAR PRIMARY KEY, enabled BOOLEAN, updated_at TIMESTAMP)")
                row = conn.execute(
                    "SELECT enabled FROM strategy_state WHERE name = ?", [name]).fetchone()
                return row[0] if row else None
            return db.execute_atomic(_r)
        except Exception as ex:
            logging.warning('could not read enabled-state for %s: %s', name, ex)
            return None

    def enable_strategy(self, name: str, paper_only: bool = False) -> StrategyState:
        """Enable a strategy.

        ``paper_only`` is a load-time safety gate (see ``load_strategy``); it's
        ignored here. The param is retained for wire compatibility but has no
        effect — routing is determined by the trader_service's account, not
        per-strategy flags.
        """
        for implementation in self.strategy_implementations:
            if name == implementation.name:
                state = implementation.enable()
                self._persist_enabled(name, True)
                return state
        return StrategyState.ERROR

    @log_method
    def disable_strategy(self, name: str) -> StrategyState:
        for implementation in self.strategy_implementations:
            if name == implementation.name:
                state = implementation.disable()
                self._persist_enabled(name, False)
                return state
        return StrategyState.ERROR

    @log_method
    def get_strategy(self, name: str) -> Optional[Strategy]:
        for strategy in self.strategy_implementations:
            if strategy.name == name:
                return strategy
        return None

    def __get_enabled_strategies(self, conid: int) -> List[Strategy]:
        if conid in self.strategies:
            return [strategy for strategy in self.strategies[conid]
                    if strategy.state == StrategyState.RUNNING or strategy.state == StrategyState.WAITING_HISTORICAL_DATA]
        return []

    @log_method
    def get_strategies(self) -> List[Strategy]:
        return self.strategy_implementations

    def runtime_status(self) -> dict:
        """Health snapshot for the pulse line, `mmr verify`, and healthchecks."""
        executor = getattr(self, 'auto_executor', None)
        auto_open = executor.open_count() if executor is not None else 0
        return build_runtime_status(
            now_utc=pd.Timestamp.now(tz='UTC'),
            strategies=self.strategy_implementations,
            streams=self.streams,
            last_dispatched_bar=self._last_dispatched_bar,
            auto_exec_open=auto_open,
        )

    def _log_pulse(self) -> None:
        """Periodic heartbeat. A healthy pipeline is otherwise SILENT at INFO
        between signals, so a dead feed (gateway hang, dropped subscription,
        pubsub break) is indistinguishable from a quiet market in the logs —
        the pulse makes liveness positively visible. Never raises: the
        reconcile loop must not die to a formatting error."""
        try:
            logging.info(format_pulse(self.runtime_status()))
        except Exception as ex:
            logging.warning('pulse failed: %s', ex)

    def _cap_tick_stream(self, conId: int) -> None:
        """Bound the raw tick buffer to the retention window so resampling stays
        cheap over a long session."""
        df = self.streams.get(conId)
        if df is None or df.empty:
            return
        try:
            cutoff = df.index[-1] - pd.Timedelta(days=self._tick_retention_days)
            if df.index[0] < cutoff:
                self.streams[conId] = df.loc[df.index >= cutoff]
        except Exception:
            pass

    def _prime_hist_bars(self, conId: int, bar_size: BarSize) -> None:
        """One-time load of recent historical OHLCV bars for (conId, bar_size)
        from the DB into the priming cache, normalized to the live schema so it
        concatenates cleanly with resampled ticks. Marks the key as primed even
        on no-data so we don't re-read the DB on every tick."""
        from trader.data.duckdb_store import DuckDBDataStore
        from trader.data.market_data import normalize_historical
        key = (conId, bar_size)
        self._hist_bars[key] = pd.DataFrame()   # mark primed (default empty)
        try:
            ds = DuckDBDataStore(self.history_duckdb_path)
            end = dt.datetime.now(dt.timezone.utc)
            start = end - dt.timedelta(days=max(self._tick_retention_days, 5) + 5)
            df = ds.read(str(conId), start=start, end=end, bar_size=str(bar_size))
            if df is not None and not df.empty:
                norm = normalize_historical(df)
                idx = norm.index
                norm.index = idx.tz_localize('UTC') if idx.tz is None else idx.tz_convert('UTC')
                self._hist_bars[key] = norm
        except Exception as ex:
            logging.warning('could not prime hist bars for conId %s %s: %s', conId, bar_size, ex)

    def _strategy_frame(self, conId: int, bar_size: BarSize) -> Optional[pd.DataFrame]:
        """The OHLCV frame a bar-based strategy should see: historical priming
        bars + the live tick stream resampled to `bar_size` (completed bars only).
        This is what makes bar strategies work live — previously they were handed
        the raw per-tick, cumulative-volume stream and couldn't compute bars."""
        from trader.data.market_data import resample_ticks_to_bars
        key = (conId, bar_size)
        if key not in self._hist_bars:
            self._prime_hist_bars(conId, bar_size)
        try:
            freq = BarSize.to_pandas_freq(bar_size)
        except Exception:
            return self.streams.get(conId)   # unknown freq: legacy raw stream

        def _utc(df):
            if df is None or df.empty:
                return None
            idx = df.index
            if idx.tz is None:
                df = df.copy(); df.index = idx.tz_localize('UTC')
            return df

        hist = self._hist_bars.get(key)
        ticks = self.streams.get(conId)
        live = resample_ticks_to_bars(ticks, freq) if ticks is not None and not ticks.empty else None
        frames = [f for f in (_utc(hist), _utc(live)) if f is not None and not f.empty]
        if not frames:
            return None
        if len(frames) == 1:
            return frames[0].sort_index()
        combined = pd.concat(frames)
        return combined[~combined.index.duplicated(keep='last')].sort_index()

    def on_ticker_next(self, ticker: Ticker):
        # Deliberately no per-tick logging: one line per tick per instrument
        # floods the service log all session for zero triage value. Tick-flow
        # visibility comes from the periodic pulse line (ticks_60s) instead.
        if not ticker.contract:
            return
        conId = ticker.contract.conId

        # populate the raw tick buffer, then bound it so resampling stays cheap
        normalized = normalize_ticker(ticker)
        if conId not in self.streams:
            self.streams[conId] = normalized
        else:
            self.streams[conId] = pd.concat([self.streams[conId], normalized], axis=0, copy=False)
        self._cap_tick_stream(conId)

        # Execute the strategies attached to the conId. CRITICAL: each strategy
        # is isolated in its own try/except. Without this, one strategy raising
        # (e.g. a pandas IndexError on a short window) propagates all the way up
        # to the pubsub subscriber loop, which calls on_error and permanently
        # DETACHES this observer from the ticker subject — every subsequent tick
        # for ALL strategies is then silently dropped and open positions go
        # unmanaged. A single misbehaving strategy must not take down the feed.
        for strategy in self.__get_enabled_strategies(conId):
            try:
                # Hand the strategy proper OHLCV bars (historical priming +
                # resampled live ticks), and only when a NEW completed bar has
                # formed — so bar-based strategies see each bar once, matching
                # the backtest, instead of the raw per-tick cumulative-volume
                # stream re-evaluated on every tick.
                frame = self._strategy_frame(conId, strategy.bar_size)
                if frame is None or frame.empty:
                    continue
                last_bar = frame.index[-1]
                dkey = (conId, strategy.name)
                if self._last_dispatched_bar.get(dkey) == last_bar:
                    continue
                self._last_dispatched_bar[dkey] = last_bar
                # G6: evaluate time-based exits once per new bar, whether or
                # not the strategy emits a signal (mirrors the backtester's
                # per-bar exit_conditions check).
                self._check_time_exit(strategy, conId, frame, last_bar)
                signal = strategy.on_prices(frame)
            except Exception as ex:
                logging.exception(
                    'strategy %s raised on_prices for conId %s; disabling it and '
                    'continuing the tick feed', getattr(strategy, 'name', '?'), conId)
                try:
                    strategy.state = StrategyState.ERROR
                except Exception:
                    pass
                continue

            if not signal:
                continue
            try:
                if signal.action == Action.BUY:
                    logging.info('BUY signal from %s', strategy.name)
                elif signal.action == Action.SELL:
                    logging.info('SELL signal from %s', strategy.name)

                # Persist signal to event store
                event = TradingEvent(
                    event_type=EventType.SIGNAL,
                    timestamp=dt.datetime.now(),
                    strategy_name=signal.source_name,
                    conid=conId,
                    action=str(signal.action),
                    signal_probability=signal.probability,
                    signal_risk=signal.risk,
                )
                self.event_store.append(event)

                # Publish signal via MessageBus for cross-strategy use and subscribers
                self.zmq_messagebus_client.write('signal', signal)
            except Exception:
                # A failure persisting/publishing one signal must not kill the
                # feed or the other strategies either.
                logging.exception(
                    'failed to record/publish signal from %s for conId %s',
                    getattr(strategy, 'name', '?'), conId)

            # G6: hand the signal to the auto-executor (guards + long-only
            # decision + proposal-pipeline execution happen on its worker
            # thread). Isolated from the persist/publish block above so a
            # failure there can't swallow execution, and vice versa.
            try:
                self._submit_auto_execution(strategy, conId, signal, last_bar)
            except Exception:
                logging.exception(
                    'auto-execute submission failed for %s conId %s',
                    getattr(strategy, 'name', '?'), conId)

    def _submit_auto_execution(self, strategy: Strategy, conId: int, signal, last_bar) -> None:
        """Flatten the signal + strategy config to primitives and enqueue for
        the auto-executor worker. Guards are evaluated on the worker so the
        skip decision lands in the persistent decision log."""
        ctx = strategy._context
        # Bar interval in seconds for the executor's stale-bar gate. Unknown/
        # unparseable bar sizes leave it at 0, which disables the gate for
        # this strategy rather than blocking its trades.
        bar_size_seconds = 0.0
        try:
            bar_size_seconds = float(
                pd.Timedelta(BarSize.to_pandas_freq(strategy.bar_size)).total_seconds())
        except Exception:
            pass
        work = SignalWork(
            strategy_name=strategy.name or 'unknown',
            conid=conId,
            action=signal.action,
            bar_ts=last_bar,
            probability=float(getattr(signal, 'probability', 0.0) or 0.0),
            risk=float(getattr(signal, 'risk', 0.0) or 0.0),
            quantity=float(getattr(signal, 'quantity', 0.0) or 0.0),
            auto_execute=bool(ctx.auto_execute) if ctx else False,
            paper_only=bool(ctx.paper_only) if ctx else False,
            state_running=strategy.state == StrategyState.RUNNING,
            close_by_time=getattr(signal, 'close_by_time', None),
            max_hold_bars=getattr(signal, 'max_hold_bars', None),
            bar_size_seconds=bar_size_seconds,
        )
        self.auto_executor.submit_signal(work)

    def _check_time_exit(self, strategy: Strategy, conId: int, frame, last_bar) -> None:
        """If this (strategy, conid) has an open auto position, report the new
        bar so the executor can evaluate close_by_time / max_hold_bars.
        bars_held counts frame bars after the entry bar — the same bar
        arithmetic the backtester uses."""
        try:
            name = strategy.name or 'unknown'
            entry_ts = self.auto_executor.open_entry_bar(name, conId)
            if entry_ts is None:
                return
            idx = frame.index
            # Entry timestamps are stored tz-naive (DuckDB TIMESTAMP); strip
            # the frame's tz for comparison — same feed, same wall time.
            if getattr(idx, 'tz', None) is not None:
                idx = idx.tz_localize(None)
            bars_held = int((idx > pd.Timestamp(entry_ts)).sum())
            self.auto_executor.submit_bar(name, conId, last_bar, bars_held)
        except Exception:
            logging.exception('time-exit check failed for %s conId %s',
                              getattr(strategy, 'name', '?'), conId)

    def on_ticker_error(self, ex: Exception):
        logging.error('StrategyRuntime ticker stream error: %s', ex, exc_info=True)

    def on_ticker_completed(self):
        logging.debug('StrategyRuntime.on_completed')

    def subscribe(self, strategy: Strategy, contract: Contract) -> None:
        logging.debug('strategy_runtime.subscribe() contract: {} strategy: {}'.format(contract, strategy))
        if contract.conId not in self.strategies:
            self.strategies[contract.conId] = []
            self.strategies[contract.conId].append(strategy)
            self.trader_client.rpc().publish_contract(contract=contract, delayed=False)
        elif contract.conId in self.strategies and strategy not in self.strategies[contract.conId]:
            self.strategies[contract.conId].append(strategy)

    def subscribe_universe(self, strategy: Strategy, universe_name: str) -> None:
        logging.debug('strategy_runtime.subscribe_universe() universe: {} strategy: {}'.format(universe_name, strategy))
        universe = self.universe_accessor.get(universe_name)

        for security in universe.security_definitions:
            self.subscribe(strategy, SecurityDefinition.to_contract(security))

    def load_strategy(
        self,
        name: str,
        bar_size_str: str,
        conids: Optional[List[int]],
        universe: Optional[str],
        historical_days_prior: int,
        module: str,
        class_name: str,
        description: str,
        paper_only: bool = False,
        auto_execute: bool = False,
        params: Optional[Dict] = None,
    ) -> None:

        # Skip if strategy with this name already loaded
        if any(s.name == name for s in self.strategy_implementations):
            logging.debug('strategy {} already loaded, skipping'.format(name))
            return

        if not name or not class_name or not module or not bar_size_str:
            raise ValueError('invalid config. need name, bar_size, class_name and module specified')

        # paper_only gate: refuse to load strategies marked paper_only when the
        # trader_service is bound to a live account. Routing is service-level
        # (one trader_service → one IB account), so this is the only place it
        # makes sense to enforce the flag.
        if paper_only and not self.paper_trading:
            logging.error(
                'refusing to load strategy %s: paper_only=True but trader_service '
                'is running in LIVE mode', name,
            )
            return

        strategies_dir = os.path.abspath(os.path.expanduser(self.strategies_directory))

        def load_class_from_file(filename, classname):
            # Reject absolute paths and path traversal. Strategy modules must
            # live under ``strategies_directory`` — otherwise a malicious YAML
            # could load any .py on disk.
            requested = os.path.expanduser(filename)
            if os.path.isabs(requested):
                # Allow absolute paths only if they resolve inside strategies_dir
                filepath = os.path.abspath(requested)
            else:
                filepath = os.path.abspath(os.path.join(strategies_dir, requested))
                # Also accept a project-root-relative path like "strategies/foo.py"
                if not os.path.exists(filepath):
                    filepath = os.path.abspath(requested)

            if not filepath.startswith(strategies_dir + os.sep) and filepath != strategies_dir:
                raise ValueError(
                    f'strategy module {filename!r} resolves outside strategies '
                    f'directory {strategies_dir!r}; refusing to load'
                )

            if not os.path.exists(filepath):
                raise FileNotFoundError(f'strategy module not found: {filepath}')

            # Namespace the module key by the strategy NAME (unique) rather
            # than the filename, so two strategies with the same basename
            # (e.g. strategies/a/ma.py and strategies/b/ma.py) don't clobber
            # each other in sys.modules and reloads evict the previous copy.
            module_name = f'_mmr_strategy_{name}'
            sys.modules.pop(module_name, None)

            spec = importlib.util.spec_from_file_location(module_name, filepath)
            if not spec or not spec.loader:
                return None
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            try:
                spec.loader.exec_module(module)
            except Exception:
                sys.modules.pop(module_name, None)
                raise
            return getattr(module, classname, None)

        try:
            class_object = load_class_from_file(module, class_name)
            if not class_object:
                return

            if inspect.isclass(class_object) and issubclass(class_object, Strategy) and class_object is not Strategy:
                logging.debug('found implementation of Strategy {}'.format(class_object))

                instance = class_object()
                context = StrategyContext(
                    name=name,
                    bar_size=BarSize.parse_str(bar_size_str),
                    conids=conids if conids else [],
                    universe=universe,
                    historical_days_prior=historical_days_prior if historical_days_prior else 0,
                    paper_only=paper_only,
                    storage=self.storage,
                    universe_accessor=self.universe_accessor,
                    logger=logging,
                    module=module,
                    class_name=class_name,
                    description=description,
                    auto_execute=auto_execute,
                    params=params if params else {},
                )
                instance.install(context)
                # Give the strategy a reference to the runtime for subscriptions
                instance.strategy_runtime = self

                # Restore the persisted enabled/disabled state so a runtime
                # enable/disable survives a service restart. Unset (None) leaves
                # the strategy INSTALLED, as before.
                persisted = self._load_enabled(name)
                if persisted is True:
                    instance.enable()
                elif persisted is False:
                    instance.disable()

                self.strategy_implementations.append(cast(Strategy, instance))

        except Exception as ex:
            # Load failures used to be swallowed at DEBUG; a config typo could
            # silently disable a strategy. Log at ERROR with the cause so the
            # operator sees it.
            logging.error('failed to load strategy %s (%s): %s', name, class_name, ex)

    def config_loader(self, config_file: str):
        config_file = os.path.expanduser(config_file)
        logging.debug('loading config file {}'.format(config_file))
        # safe_load refuses Python-object tags — YAML-injection hardening.
        with open(config_file, 'r') as conf_file:
            config = yaml.safe_load(conf_file)
        if not config or 'strategies' not in config:
            logging.warning('strategy config %s has no strategies section', config_file)
            return

        for strategy_config in config['strategies']:
            self.load_strategy(
                name=strategy_config['name'],
                bar_size_str=strategy_config['bar_size'],
                conids=strategy_config.get('conids'),
                universe=strategy_config.get('universe'),
                historical_days_prior=strategy_config.get('historical_days_prior', 1),
                module=strategy_config.get('module', ''),
                class_name=strategy_config.get('class_name', ''),
                description=strategy_config.get('description', ''),
                paper_only=strategy_config.get('paper_only', False),
                auto_execute=strategy_config.get('auto_execute', False),
                params=strategy_config.get('params', {}),
            )

    async def _reconcile(self):
        """Re-check config and subscriptions. Safe to call repeatedly (idempotent).

        The body is synchronous (blocking RPC to trader_service for config
        reload + per-conId resolve + per-contract publish). We offload the
        whole thing to a thread so the event loop stays responsive — a
        portfolio universe with ~10 conIds used to stall the loop for
        ~1s every 30s, which surfaced as an asyncio "slow callback"
        warning and stalled live ticker dispatch.
        """
        await asyncio.to_thread(self._reconcile_sync)

    def _reconcile_sync(self):
        """Synchronous reconcile body. Called from ``_reconcile`` via
        ``asyncio.to_thread``; safe to call directly from non-async contexts."""
        # 1. Check for config file changes. If the YAML is mid-write when we
        # try to parse it, keep the old mtime so we retry on the next tick
        # rather than accepting a partial load.
        try:
            current_mtime = os.path.getmtime(self.strategy_config_file)
        except OSError:
            current_mtime = self._config_mtime

        if current_mtime != self._config_mtime:
            logging.info('strategy config changed, reloading')
            try:
                self.config_loader(self.strategy_config_file)
            except (yaml.YAMLError, ValueError, FileNotFoundError) as ex:
                logging.error(
                    'failed to reload strategy config (will retry next cycle): %s', ex,
                )
                # Don't advance _config_mtime — re-try on next reconcile
                return
            self._config_mtime = current_mtime

        # 2. Re-subscribe all strategies (idempotent — only new conIds trigger publish_contract).
        # Only swallow the well-known transient failures (trader_service bouncing,
        # RPC timeout, socket not-yet-connected). Any other exception is a real
        # bug and should propagate to the run() error handler so it gets logged
        # at ERROR rather than silently masked at DEBUG.
        try:
            for strategy in self.strategy_implementations:
                if strategy.conids:
                    for conId in strategy.conids:
                        security_definitions = self.trader_client.rpc().resolve_symbol(conId)
                        if security_definitions:
                            self.subscribe(strategy, SecurityDefinition.to_contract(security_definitions[0]))

                if strategy.universe:
                    self.subscribe_universe(strategy, strategy.universe)
        except (TimeoutError, ConnectionError) as ex:
            logging.debug('reconciliation RPC failed (trader_service may be restarting): %s', ex)

    async def _reconnect_historical_client(self):
        """Disconnect and reconnect the IB historical data client."""
        logging.info('reconnecting historical data IB client')
        try:
            self.historical_data_client.shutdown()
        except Exception:
            pass
        await self.historical_data_client.connect_async()

    @staticmethod
    def _try_get_exchange_calendar(security: Optional[SecurityDefinition]):
        """Best-effort lookup of an exchange_calendars Calendar for a security.

        Tries primaryExchange first (e.g. NASDAQ, ARCA) then falls back to
        the IB exchange field (often SMART, which has no calendar). Returns
        None if neither resolves — callers should treat None as "no
        calendar, skip the missing-range optimization and pull the full
        window."
        """
        if not security:
            return None
        try:
            return exchange_calendars.get_calendar(security.primaryExchange)
        except Exception:
            try:
                return exchange_calendars.get_calendar(security.exchange)
            except Exception:
                return None

    async def _fetch_history_with_resume(
        self,
        security: SecurityDefinition,
        bar_size: BarSize,
        historical_days: int,
        strategy_name: str,
    ):
        """Fetch historical bars only for the date ranges not already in DuckDB.

        Mirrors the cache-aware pattern in data_service: ask TickStorage
        which calendar days inside the requested window are missing, then
        pull *just those* from IB and write the result back. This turns a
        90-day backfill on every strategy_service restart into a no-op
        once the local store is warm.

        Errors:
          * IBNoDataError    -> swallowed (logged at warning); some IB
                                contracts genuinely have no history.
          * IBConnectivityError -> propagated; caller decides whether to
                                reconnect and retry.
        """
        contract = SecurityDefinition.to_contract(security)
        what_to_show = _whattoshow_for_contract(contract)

        tick_data = self.storage.get_tickdata(bar_size=bar_size)
        tz = security.timeZoneId or 'US/Eastern'
        # dateify() with timezone= returns a tz-aware dt.datetime even
        # when given a naive datetime or a dt.date.
        window_start = dateify(
            dt.datetime.now() - dt.timedelta(days=historical_days),
            timezone=tz, make_sod=True,
        )
        window_end = dateify(dt.datetime.now(), timezone=tz, make_eod=True)

        cal = self._try_get_exchange_calendar(security)
        if cal is not None:
            try:
                date_ranges = tick_data.missing(
                    security, cal,
                    date_range=DateRange(start=window_start, end=window_end),
                )
            except Exception as ex:
                logging.warning(
                    'tick_data.missing() failed for %s strategy %s: %s — '
                    'falling back to full-window pull',
                    security.symbol, strategy_name, ex,
                )
                date_ranges = [DateRange(start=window_start, end=window_end)]
        else:
            # No calendar -> can't compute trading-day gaps. Pull the full
            # window. tick_data.write() upserts so we still won't double-store.
            date_ranges = [DateRange(start=window_start, end=window_end)]

        if not date_ranges:
            logging.debug(
                'history cache hit for %s (%s, %sd) — skipping IB fetch',
                security.symbol, strategy_name, historical_days,
            )
            return

        for dr in date_ranges:
            # tick_data.missing() returns DateRanges whose start/end are
            # bare dt.date objects (from exchange_calendars sessions.date)
            # — despite DateRange being type-annotated dt.datetime. The
            # IB worker expects tz-aware datetimes (it reads .tzinfo on
            # the input), so promote here before the call.
            dr_start = dateify(dr.start, timezone=tz, make_sod=True)
            dr_end = dateify(dr.end, timezone=tz, make_eod=True)
            try:
                df = await self.historical_data_client.get_contract_history(
                    security=contract,
                    what_to_show=what_to_show,
                    bar_size=bar_size,
                    start_date=dr_start,
                    end_date=dr_end,
                )
            except IBNoDataError as ex:
                logging.warning(
                    'no historical data for %s (%s) %s..%s strategy %s: %s',
                    security.symbol, security.conId,
                    dr.start, dr.end, strategy_name, ex,
                )
                continue

            if df is not None and len(df) > 0:
                try:
                    tick_data.write(security, df)
                    logging.debug(
                        'wrote %d bars for %s (%s) strategy %s',
                        len(df), security.symbol, security.conId, strategy_name,
                    )
                except Exception as ex:
                    logging.warning(
                        'tick_data.write() failed for %s strategy %s: %s',
                        security.symbol, strategy_name, ex,
                    )

    async def get_historical_data(self):
        for strategy in self.strategy_implementations:
            historical_days = strategy.historical_days_prior if strategy.historical_days_prior else 1

            if strategy.conids:
                for conId in strategy.conids:
                    security_definitions = self.trader_client.rpc().resolve_symbol(conId)
                    if security_definitions:
                        try:
                            await self._fetch_history_with_resume(
                                security=security_definitions[0],
                                bar_size=strategy.bar_size,
                                historical_days=historical_days,
                                strategy_name=strategy.name,
                            )
                        except IBConnectivityError:
                            raise
                    else:
                        logging.error('could not find security definition for conId {} for strategy {}'.format(conId, strategy))

            if strategy.universe:
                # Iterate SecurityDefinitions directly so we can pass them to
                # _fetch_history_with_resume (which needs primaryExchange,
                # timeZoneId, etc. for calendar lookup and missing-range
                # computation; a bare Contract(conId=...) wouldn't suffice).
                for sd in self.universe_accessor.get(strategy.universe).security_definitions:
                    try:
                        await self._fetch_history_with_resume(
                            security=sd,
                            bar_size=strategy.bar_size,
                            historical_days=historical_days,
                            strategy_name=strategy.name,
                        )
                    except IBConnectivityError:
                        raise
        logging.debug('finished get_historical_data()')

    async def run(self):
        logging.info('starting strategy_runtime')
        logging.debug('StrategyRuntime.run()')

        # Async setup that used to happen inside connect() via asyncio.run():
        # we now do it here so the tasks land on the real service loop and
        # actually get a chance to run.
        await self.zmq_messagebus_client.connect()
        await self.zmq_strategy_rpc_server.serve()

        await self.trader_client.connect()

        self.zmq_subscriber = TopicPubSub[Ticker](
            self.zmq_pubsub_server_address,
            self.zmq_pubsub_server_port,
        )

        logging.debug('subscribing to tick stream')
        observable = await self.zmq_subscriber.subscriber('ticker')
        self.observer = AutoDetachObserver(
            on_next=self.on_ticker_next,
            on_error=self.on_ticker_error,
            on_completed=self.on_ticker_completed
        )
        self.subscription = observable.subscribe(self.observer)

        logging.debug('loading {} config file'.format(self.strategy_config_file))
        self.config_loader(self.strategy_config_file)

        logging.debug('subscribing to streams for all conids')

        # todo: i'm not sure the runtime should automagically subscribe here.
        # it's probably up to the strategy how they want to secure data
        for strategy in self.strategy_implementations:
            if strategy.conids:
                for conId in strategy.conids:
                    security_definitions = self.trader_client.rpc().resolve_symbol(conId)
                    if security_definitions:
                        self.subscribe(strategy, SecurityDefinition.to_contract(security_definitions[0]))
                    else:
                        logging.error('could not find security definition for conId {} for strategy {}. Disabling strategy.'
                                      .format(conId, strategy))
                        strategy.on_error(
                            Exception('could not find security definition for conId {} for strategy {}. Disabling strategy.'
                                      .format(conId, strategy))
                        )

            if strategy.universe:
                self.subscribe_universe(strategy, strategy.universe)

        logging.debug('starting connection to IB for historical data')

        self.historical_data_client = IBHistoryWorker(
            self.ib_server_address,
            self.ib_server_port,
            self.strategy_runtime_ib_client_id + 1,
        )
        max_retries = 5
        for attempt in range(1, max_retries + 1):
            try:
                if not self.historical_data_client.connected:
                    await self.historical_data_client.connect_async()
                await self.get_historical_data()
                break
            except IBConnectivityError as ex:
                if attempt == max_retries:
                    logging.error('historical data failed after {} attempts, giving up: {}'.format(max_retries, ex))
                    break
                wait = min(2 ** attempt, 30)
                logging.warning('IB connectivity error (attempt {}/{}), retrying in {}s: {}'.format(
                    attempt, max_retries, wait, ex))
                try:
                    await self._reconnect_historical_client()
                except Exception as reconnect_ex:
                    logging.error('reconnect failed: {}'.format(reconnect_ex))
                await asyncio.sleep(wait)
            except ConnectionError:
                if attempt == max_retries:
                    logging.error('IB not connected after {} attempts, giving up'.format(max_retries))
                    break
                wait = min(2 ** attempt, 30)
                logging.warning('IB not connected (attempt {}/{}), retrying in {}s'.format(
                    attempt, max_retries, wait))
                await asyncio.sleep(wait)
            except Exception as ex:
                logging.error('unexpected error fetching historical data: {}'.format(ex))
                break

        # Track config mtime for change detection
        try:
            self._config_mtime = os.path.getmtime(self.strategy_config_file)
        except OSError:
            self._config_mtime = 0.0

        # Stay alive and periodically reconcile subscriptions
        logging.info('entering reconciliation loop (30s interval)')
        while True:
            await asyncio.sleep(30)
            try:
                await self._reconcile()
            except Exception as ex:
                logging.error('reconciliation error: {}'.format(ex))
            self._log_pulse()

