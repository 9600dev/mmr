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
from typing import Any, cast, Dict, List, Optional

import asyncio
import backoff
import datetime as dt
import exchange_calendars
import importlib
import importlib.util
import inspect
import hashlib
import json
import os
import pandas as pd
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
import trader.messaging.strategy_service_api as bus
import yaml


logging = setup_logging(module_name='strategy_runtime')


error_table = {
    'trader.common.exceptions.TraderException': TraderException,
    'trader.common.exceptions.TraderConnectionException': TraderConnectionException
}


def _serialized_deployment(method):
    @wraps(method)
    def serialized(self, *args, **kwargs):
        if not hasattr(self, '_config_lock'):
            self._config_lock = threading.RLock()
        with self._config_lock:
            return method(self, *args, **kwargs)
    return serialized


# The auto-executor is long-only by construction (decide_signal has no
# short-open path); only 'long' is an honourable declaration. 'short'/'both'
# describe an executor that does not exist, so declaring them is a config
# error that disarms the strategy rather than silently mis-describing it.
_VALID_MANIFEST_DIRECTIONS = ('long', 'short', 'both')


def _validate_manifest(
    manifest: Optional[Dict],
    subscription_conids: set,
    auto_execute: bool,
) -> tuple:
    """Parse + validate an optional strategy manifest block.

    Returns ``(fields, disarm_reason)``. ``fields`` is a dict of the four
    context primitives (all ``None`` when the manifest is absent or invalid).
    ``disarm_reason`` is ``None`` when the manifest is absent or fully valid,
    otherwise a human-readable reason string; the caller then loads the
    strategy DISARMED (auto_execute stripped) and stores no manifest fields.

    Validation is deliberately strict — a malformed envelope must never
    silently default to "unchecked". Disarm when:
      * ``allowed_conids`` is not a list of ints (structural — unconditional);
      * ``auto_execute`` is set AND any allowed conId is not in the
        subscription set (you declared a tradeable instrument you don't watch);
      * ``direction`` is not one of long/short/both/null;
      * ``direction`` is short/both (the executor is long-only);
      * ``max_opening_orders_per_day`` / ``_per_hour`` is not a positive int
        or null.
    """
    fields = {
        'manifest_allowed_conids': None,
        'manifest_direction': None,
        'manifest_max_opens_per_day': None,
        'manifest_max_opens_per_hour': None,
    }
    if manifest is None:
        return fields, None
    if not isinstance(manifest, dict):
        return fields, f'manifest must be a mapping, got {type(manifest).__name__}'

    allowed = manifest.get('allowed_conids', None)
    if allowed is not None:
        # bool is a subclass of int — reject it explicitly so `true` can't
        # masquerade as a conId.
        if (isinstance(allowed, bool) or not isinstance(allowed, list)
                or not all(isinstance(c, int) and not isinstance(c, bool)
                           for c in allowed)):
            return fields, f'allowed_conids must be a list of ints or null, got {allowed!r}'
        allowed_ints = [int(c) for c in allowed]
        if auto_execute:
            missing = [c for c in allowed_ints if c not in subscription_conids]
            if missing:
                return fields, (
                    f'allowed_conids {missing} are not in the subscription set '
                    f'{sorted(subscription_conids)} — a tradeable conId must also '
                    f'be subscribed')
        fields['manifest_allowed_conids'] = allowed_ints

    direction = manifest.get('direction', None)
    if direction is not None:
        if direction not in _VALID_MANIFEST_DIRECTIONS:
            return fields, (
                f"direction must be one of 'long'/'short'/'both'/null, got {direction!r}")
        if direction in ('short', 'both'):
            return fields, (
                f"direction={direction!r} but the auto-executor is long-only — "
                f"refusing to arm a strategy that declares a short/both envelope")
        fields['manifest_direction'] = direction

    for key, field_name in (('max_opening_orders_per_day', 'manifest_max_opens_per_day'),
                            ('max_opening_orders_per_hour', 'manifest_max_opens_per_hour')):
        value = manifest.get(key, None)
        if value is not None:
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                return fields, f'{key} must be a positive int or null, got {value!r}'
            fields[field_name] = int(value)

    return fields, None


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


def _session_bar_ts(last_bar, params: Optional[dict],
                    default_tz: str = 'America/New_York'):
    """Convert a live (UTC) bar timestamp to the strategy's SESSION timezone
    for time-of-day comparisons (``close_by_time``).

    The backtester's frames are session-tz-aware (US history is ET-keyed,
    ASX Sydney-keyed), so ``bar_ts.time()`` compares in session wall time
    there. Live frames are UTC — without this conversion a 15:45 ET flatten
    fires at 15:45 UTC = 11:45 ET, 4h15m early (happened live 2026-07-20 on
    the first-ever live time-exit). SESSION_TZ param wins (the ASX strategies
    set Australia/Sydney); default matches US history storage. Any failure
    falls back to the raw bar (the old behavior) rather than blocking exits.
    """
    try:
        session_tz = (params or {}).get('SESSION_TZ') or default_tz
        ts = pd.Timestamp(last_bar)
        ts = ts.tz_localize('UTC') if ts.tzinfo is None else ts
        return ts.tz_convert(session_tz)
    except Exception:
        return last_bar


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


def _last_trade_age(ticks, now_utc: pd.Timestamp) -> Optional[int]:
    """Seconds since this instrument was last seen to actually TRADE.

    ``bar_age_s`` cannot answer this, and reading it as if it could is the
    trap this exists to close. ``normalize_ticker`` falls back to the bid/ask
    MIDPOINT when there is no trade price, so a quote-only tick still carries a
    non-NaN close, still forms a bar in ``resample_ticks_to_bars`` (which drops
    only NaN-close bars), and still gets dispatched — resetting ``bar_age_s``
    to seconds. Observed 2026-07-26: WDS sat at 218,093s (correct — Friday's
    ASX close) until three out-of-hours quote ticks at 11:45 on a SUNDAY dropped
    it to 263s. Nothing had traded; the ASX was shut.

    The only field that distinguishes a trade is the cumulative day volume
    counter, so this reports the age of the last tick where it INCREASED. A
    decrease is the day-boundary reset, not a trade, and is ignored — the next
    increase after it counts. ``None`` means no trade is visible in the retained
    tick stream, which is the honest answer for a market that is closed.

    Why it matters beyond tidiness: a feed delivering quotes but no trades
    during a session is a real partial-outage mode, and ``bar_age_s`` reads
    perfectly healthy throughout it.
    """
    try:
        volume = ticks['volume']
    except Exception:
        return None
    try:
        if volume is None or len(volume) < 2:
            return None
        traded = volume.diff() > 0          # NaN diffs compare False
        if not bool(traded.any()):
            return None
        return _age_seconds(volume.index[traded][-1], now_utc)
    except Exception:
        return None


def build_runtime_status(
    now_utc: pd.Timestamp,
    strategies,
    streams: Dict[int, pd.DataFrame],
    last_dispatched_bar: Dict[tuple, pd.Timestamp],
    auto_exec_open: int,
    oos_bars: Optional[Dict[int, int]] = None,
) -> dict:
    """Pure snapshot of pipeline health: strategy states, tick flow per conId,
    freshest dispatched-bar age per conId, last-trade age per conId, and open
    auto-exec positions.

    Consumed two ways: formatted by ``format_pulse`` into the periodic log
    line, and returned raw by the ``runtime_status`` RPC for `mmr verify` /
    healthchecks. Pure function of its inputs so tests can drive it with
    fabricated state.

    THE TWO AGE FIELDS MEASURE DIFFERENT THINGS — read them together:

    ``bar_age_s`` is the age of the last bar DISPATCHED to a strategy. It
    answers "is the dispatch pipeline moving", and a quote-only tick is enough
    to move it (see ``_last_trade_age``), so it is NOT a data-freshness metric.

    ``trade_age_s`` is the age of the last tick where cumulative volume rose,
    i.e. when the instrument last actually traded. Absent for a conId means no
    trade is visible in the retained stream — normal out of session, an
    ESCALATION during one.
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

    trade_age_s: Dict[int, int] = {}
    for conid, df in streams.items():
        age = _last_trade_age(df, now_utc)
        if age is not None:
            trade_age_s[int(conid)] = age

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
        'trade_age_s': trade_age_s,
        'oos_bars': {int(k): int(v) for k, v in (oos_bars or {}).items() if v},
        'auto_exec_open': int(auto_exec_open),
    }


def format_pulse(status: dict) -> str:
    """One greppable INFO line per interval. ``ticks_60s=0`` for a subscribed
    conId during market hours is the escalate condition — a dead feed shows
    up as this line going to zero, not as an absence of errors.

    ``trade_age_s`` is printed next to ``bar_age_s`` deliberately: the pair is
    only readable together. A small ``bar_age_s`` beside a large or missing
    ``trade_age_s`` means bars are being manufactured from quotes, not trades —
    which looks identical to health if you read ``bar_age_s`` alone."""
    ticks = ','.join(f'{k}:{v}' for k, v in sorted(status.get('ticks_60s', {}).items()))
    ages = ','.join(f'{k}:{v}' for k, v in sorted(status.get('bar_age_s', {}).items()))
    trades = ','.join(f'{k}:{v}' for k, v in sorted(status.get('trade_age_s', {}).items()))
    oos = ','.join(f'{k}:{v}' for k, v in sorted(status.get('oos_bars', {}).items()))
    return (
        'pulse strategies={running}/{total} ticks_60s=[{ticks}] '
        'bar_age_s=[{ages}] trade_age_s=[{trades}] oos_bars=[{oos}] '
        'auto_exec_open={open}'.format(
            running=status.get('strategies_running', 0),
            total=status.get('strategies_total', 0),
            ticks=ticks,
            ages=ages,
            trades=trades,
            oos=oos,
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
        # conIds we have asked trader_service to publish. Deliberately NOT
        # derived from `strategies` above: that is our routing table and
        # outlives everything, while this mirrors state inside ANOTHER process
        # and must be discarded when that process restarts.
        self._published_conids: set[int] = set()
        self._trader_boot_id: Optional[str] = None
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
        # Bars refused by the session gate, per conId, plus a once-per-day log
        # guard. Exposed in the pulse so a wrong venue mapping shows up as a
        # rising count rather than as a strategy that quietly stopped trading.
        self._oos_bars: Dict[int, int] = {}
        self._oos_logged: set = set()
        # Last refused bar_ts per conId, so the counter counts DISTINCT bars.
        # The gate runs per tick, and a refused bar sits at the frame tail until
        # the next in-session bar completes — so without this, every arriving
        # tick re-counted the same bar (WDS read oos_bars=230 pre-open for ~a
        # dozen actual bars). Refused bars arrive monotonically (always the
        # frame tail), so remembering only the LAST one per conId is exact
        # distinct-counting with O(conIds) memory — a seen-set would grow by
        # one entry per out-of-session minute for the life of the process.
        self._oos_last: Dict[int, Any] = {}
        # Keep at most this many days of raw ticks per conid (bounds compute).
        self._tick_retention_days: int = 2
        self._deployment_lock = threading.RLock()
        self._config_lock = threading.RLock()
        self._deployment_generations: Dict[str, str] = {}
        self._retired_strategies: List[Strategy] = []
        self._contracts: Dict[int, Contract] = {}
        self._live_bar_buffers: Dict[tuple, Any] = {}
        self._frame_cache: Dict[tuple, Any] = {}
        self._isolate_callbacks = True
        self._callback_workers: Dict[str, Any] = {}
        self._history_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix='strategy-history')
        self._history_jobs: Dict[tuple, Any] = {}
        self._history_retry_at: Dict[tuple, float] = {}
        self._history_versions: Dict[tuple, int] = {}
        self._managed_contexts: Dict[tuple, Strategy] = {}

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
                authority_check=self._opening_authorized,
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

    def _deployment_state(self) -> None:
        # Also supports runtimes constructed without the network setup.
        if not hasattr(self, '_deployment_lock'):
            self._deployment_lock = threading.RLock()
            self._deployment_generations = {}
            self._retired_strategies = []

    def _opening_authorized(self, name: str, generation: str) -> bool:
        self._deployment_state()
        with self._deployment_lock:
            strategy = self.get_strategy(name)
            return bool(generation and self._deployment_generations.get(name) == generation
                        and strategy is not None and strategy.state == StrategyState.RUNNING
                        and strategy.ctx.auto_execute)

    def _revoke_opening(self, strategy: Strategy) -> None:
        self._deployment_state()
        with self._deployment_lock:
            generation = getattr(strategy._context, 'deployment_generation', '')
            if self._deployment_generations.get(strategy.name) == generation:
                self._deployment_generations.pop(strategy.name, None)

    def _grant_generation(self, strategy: Strategy) -> None:
        self._deployment_state()
        with self._deployment_lock:
            generation = uuid.uuid4().hex
            strategy.ctx.deployment_generation = generation
            self._deployment_generations[strategy.ctx.name] = generation

    def _retire_strategy(self, strategy: Strategy) -> None:
        """Revoke openings immediately; retain its exit policy until flat."""
        self._revoke_opening(strategy)
        self._stop_callback_worker(strategy)
        strategy.ctx.auto_execute = False
        strategy.disable()
        self._retired_strategies.append(strategy)
        self.strategy_implementations.remove(strategy)
        for subscribers in self.strategies.values():
            if strategy in subscribers:
                subscribers.remove(strategy)

    @_serialized_deployment
    def enable_strategy(self, name: str, paper_only: bool = False) -> StrategyState:
        """Enable a strategy.

        ``paper_only`` is a load-time safety gate (see ``load_strategy``); it's
        ignored here. The param is retained for wire compatibility but has no
        effect — routing is determined by the trader_service's account, not
        per-strategy flags.
        """
        for implementation in self.strategy_implementations:
            if name == implementation.name:
                if implementation.ctx.auto_execute:
                    filepath = getattr(implementation, '_source_path', '')
                    if not filepath or not self._gauntlet_allows_arming(
                            name, filepath, implementation.ctx.class_name or ''):
                        self._revoke_opening(implementation)
                        return StrategyState.ERROR
                state = implementation.enable()
                self._grant_generation(implementation)
                if getattr(self, '_isolate_callbacks', False):
                    try:
                        self._start_callback_worker(implementation)
                        state = implementation.state
                    except Exception:
                        implementation.state = StrategyState.ERROR
                        self._revoke_opening(implementation)
                        logging.exception('strategy worker failed to enable %s', name)
                        return StrategyState.ERROR
                self._persist_enabled(name, True)
                return state
        return StrategyState.ERROR

    @log_method
    @_serialized_deployment
    def disable_strategy(self, name: str) -> StrategyState:
        for implementation in self.strategy_implementations:
            if name == implementation.name:
                self._revoke_opening(implementation)
                self._stop_callback_worker(implementation)
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
        status = build_runtime_status(
            now_utc=pd.Timestamp.now(tz='UTC'),
            strategies=self.strategy_implementations,
            streams=self.streams,
            last_dispatched_bar=self._last_dispatched_bar,
            oos_bars=self._oos_bars,
            auto_exec_open=auto_open,
        )
        if executor is not None and hasattr(executor, 'status_metrics'):
            status['execution'] = executor.status_metrics()
        status['deployments'] = {
            strategy.name: {
                'generation': strategy.ctx.deployment_generation,
                'effective_config_hash': strategy.ctx.effective_config_hash,
                'source_hash': strategy._source_hash,
            } for strategy in self.strategy_implementations
        }
        status['callback_workers'] = {
            name: {'pid': worker.pid, 'failed': worker.failed}
            # Snapshot: enable/disable mutate this dict from a worker thread.
            for name, worker in list(getattr(self, '_callback_workers', {}).items())
        }
        status['event_loop_lag_seconds'] = getattr(self, '_event_loop_lag_seconds', 0.0)
        status['signal_audit_pending'] = len(getattr(self, '_signal_audit_tasks', ()))
        status['signal_audit_overflow'] = getattr(self, '_signal_audit_overflow', 0)
        return status

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
            # Completed bars live in incremental buffers. Raw ticks are only
            # the recent health/sample view, not an ever-growing replay log.
            if any(key[0] == conId for key in getattr(self, '_live_bar_buffers', {})):
                self.streams[conId] = self.streams[conId].iloc[-2048:]
        except Exception:
            pass

    def _invalidate_history(self, conId: int, bar_size: BarSize) -> None:
        if hasattr(self, '_history_versions'):
            key = (conId, bar_size)
            self._history_versions[key] = self._history_versions.get(key, 0) + 1
            self._history_retry_at.pop(key, None)
        self._hist_bars.pop((conId, bar_size), None)
        getattr(self, '_frame_cache', {}).pop((conId, bar_size), None)

    def _prime_hist_bars(self, conId: int, bar_size: BarSize) -> None:
        """Only successful reads populate the cache; failed reads are retryable."""
        from trader.data.duckdb_store import DuckDBDataStore
        from trader.data.market_data import normalize_historical
        key = (conId, bar_size)
        version = getattr(self, '_history_versions', {}).get(key, 0)
        try:
            ds = DuckDBDataStore(self.history_duckdb_path)
            end = dt.datetime.now(dt.timezone.utc)
            candidates = list(self.strategies.get(conId, [])) + [
                s for (name, cid), s in getattr(self, '_managed_contexts', {}).items() if cid == conId]
            requested_days = max((getattr(s, 'historical_days_prior', 0) or 0
                                  for s in candidates
                                  if s.bar_size == bar_size), default=0)
            start = end - dt.timedelta(days=max(requested_days, self._tick_retention_days, 5) + 5)
            df = ds.read(str(conId), start=start, end=end, bar_size=str(bar_size))
            norm = pd.DataFrame()
            if df is not None and not df.empty:
                norm = normalize_historical(df).dropna(subset=['close'])
                norm.index = self._utc_index(norm.index)
            if version == getattr(self, '_history_versions', {}).get(key, 0):
                self._hist_bars[key] = norm
        except Exception as ex:
            if hasattr(self, '_history_retry_at'):
                self._history_retry_at[key] = time.monotonic() + 5
            logging.warning('could not prime hist bars for conId %s %s (will retry): %s', conId, bar_size, ex)

    @staticmethod
    def _utc_index(index):
        index = pd.DatetimeIndex(index)
        return index.tz_localize('UTC') if index.tz is None else index.tz_convert('UTC')

    def _filter_session_frame(self, conId, bar_size, frame, observed):
        """Filter EVERY row, retaining valid extended-hours and daily labels."""
        contract = getattr(self, '_contracts', {}).get(conId)
        if contract is None or frame.empty:
            return frame
        from trader.data.market_session import calendar_name_for, session_intervals
        name = calendar_name_for(contract.exchange, contract.primaryExchange)
        duration = pd.Timedelta(BarSize.to_pandas_freq(bar_size))
        if name is None or contract.secType in ('CASH', 'CRYPTO'):
            return frame.loc[frame.index + duration <= observed] if duration >= pd.Timedelta(days=1) else frame
        index = frame.index
        keep = pd.Series(False, index=index)
        if duration >= pd.Timedelta(days=1):
            # Daily bars carry session labels, not minute timestamps. UTC
            # midnight labels are already dates; local-midnight labels retain
            # their venue date after conversion.
            import pandas_market_calendars as mcal
            timezone = mcal.get_calendar(name).tz
            for stamp in index:
                day = (stamp.date() if stamp == stamp.normalize()
                       else stamp.tz_convert(timezone).date())
                lookup = session_intervals(name, day)
                keep.loc[stamp] = (not lookup.evaluable or
                                  (lookup.window is not None and lookup.window[1] <= observed))
        else:
            # Include adjacent calendar dates for overnight/Asian sessions;
            # vector comparisons keep this O(bars + sessions), not per tick.
            days = set(index.date)
            days |= {day + dt.timedelta(days=offset) for day in list(days) for offset in (-1, 1)}
            for day in sorted(days):
                lookup = session_intervals(name, day)
                if not lookup.evaluable:
                    keep |= index.date == day
                elif lookup.window:
                    intervals = lookup.intervals or (lookup.window,)
                    for start, end in intervals:
                        mask = (index >= start) & (index <= end)
                        if end != lookup.window[1]:
                            mask &= index < end
                        keep |= mask
        refused = frame.index[~keep.to_numpy()]
        if len(refused):
            self._note_out_of_session(conId, refused[-1])
        return frame.loc[keep.to_numpy()]

    def _strategy_frame(self, conId: int, bar_size: BarSize) -> Optional[pd.DataFrame]:
        """Shared completed bars, with one incremental buffer per subscription."""
        from trader.strategy.live_bars import LiveBarBuffer, DailySessionBuckets
        key = (conId, bar_size)
        if not hasattr(self, '_live_bar_buffers'):
            self._live_bar_buffers = {}
            self._frame_cache = {}
        if key not in self._hist_bars:
            if hasattr(self, '_history_pool'):
                job = self._history_jobs.get(key)
                if (job is None or job.done()) and time.monotonic() >= self._history_retry_at.get(key, 0):
                    self._history_jobs[key] = self._history_pool.submit(self._prime_hist_bars, conId, bar_size)
            else:
                self._prime_hist_bars(conId, bar_size)
        freq = BarSize.to_pandas_freq(bar_size)
        daily = pd.Timedelta(freq) >= pd.Timedelta(days=1)
        ticks = self.streams.get(conId)
        if key not in self._live_bar_buffers:
            daily_sessions = None
            contract = getattr(self, '_contracts', {}).get(conId)
            if daily and contract is not None and contract.secType not in ('CASH', 'CRYPTO'):
                from trader.data.market_session import calendar_name_for
                name = calendar_name_for(contract.exchange, contract.primaryExchange)
                if name:
                    daily_sessions = DailySessionBuckets(name)
            if ticks is not None and not ticks.empty:
                ticks = ticks.copy()
                ticks.index = self._utc_index(ticks.index)
            self._live_bar_buffers[key] = LiveBarBuffer(
                ticks, freq, self._tick_retention_days, daily_sessions=daily_sessions)
        buffer = self._live_bar_buffers[key]
        observed = buffer.last_tick if buffer.last_tick is not None else pd.Timestamp.now(tz='UTC')
        forming = observed.floor(freq)
        hist = self._hist_bars.get(key)
        revision = (id(hist), buffer.revision, observed.floor('min') if daily else forming)
        cached = self._frame_cache.get(key)
        if cached is not None and cached[0] == revision:
            return cached[1]
        live = buffer.completed
        if daily and buffer.current is not None:
            candidate = pd.DataFrame([buffer.current], index=pd.DatetimeIndex([buffer.bucket], name='date'))
            live = candidate if live.empty else pd.concat([live, candidate])
        frames = [frame for frame in (hist, live) if frame is not None and not frame.empty]
        if not frames:
            return None
        combined = pd.concat(frames) if len(frames) > 1 else frames[0].copy()
        combined.index = self._utc_index(combined.index)
        if buffer.daily_sessions is not None:
            combined.index = buffer.daily_sessions.historical_labels(combined.index)
        combined = combined[~combined.index.duplicated(keep='last')].sort_index()
        if not daily:
            combined = combined.loc[combined.index < forming]
        elif getattr(self, '_contracts', {}).get(conId) is None:
            combined = combined.loc[combined.index + pd.Timedelta(freq) <= observed]
        combined = self._filter_session_frame(conId, bar_size, combined, observed)
        self._frame_cache[key] = (revision, combined)
        return combined

    def _bar_in_session(self, contract, bar_ts) -> bool:
        """Session gate for a dispatched bar. Never raises and never blocks on
        uncertainty — an error here would silently stop a strategy receiving
        bars, which is worse than the pollution it guards against."""
        try:
            from trader.data.market_session import in_session
            return in_session(
                bar_ts,
                exchange=getattr(contract, 'exchange', '') or '',
                primary_exchange=getattr(contract, 'primaryExchange', '') or '',
                sec_type=getattr(contract, 'secType', '') or '',
            )
        except Exception as ex:
            logging.warning('session gate errored for conId %s (%s) — dispatching anyway',
                            getattr(contract, 'conId', '?'), ex)
            return True

    def _note_out_of_session(self, conId: int, bar_ts) -> None:
        """Count DISTINCT suppressed bars; say so ONCE per (conId, UTC day).

        Silent suppression is the failure mode to avoid: if the venue mapping is
        wrong, a strategy simply stops seeing bars and nothing says why. The
        running count also rides along in the pulse.
        """
        if self._oos_last.get(conId) == bar_ts:
            return                       # same refused bar, another tick — counted already
        self._oos_last[conId] = bar_ts
        self._oos_bars[conId] = self._oos_bars.get(conId, 0) + 1
        try:
            day = pd.Timestamp(bar_ts).date()
        except Exception:
            day = None
        key = (conId, day)
        if key not in self._oos_logged:
            self._oos_logged.add(key)
            logging.info(
                'conId %s: bar %s is outside the exchange session (extended hours '
                'included) — not dispatching. Quote-only ticks form bars off the '
                'bid/ask midpoint; see AUDIT_ROADMAP G8.', conId, bar_ts)

    def on_ticker_next(self, ticker: Ticker):
        # Deliberately no per-tick logging: one line per tick per instrument
        # floods the service log all session for zero triage value. Tick-flow
        # visibility comes from the periodic pulse line (ticks_60s) instead.
        if not ticker.contract:
            return
        conId = ticker.contract.conId
        if not hasattr(self, '_contracts'):
            self._contracts = {}
        self._contracts[conId] = ticker.contract

        # populate the raw tick buffer, then bound it so resampling stays cheap
        normalized = normalize_ticker(ticker)
        previous_ticks = self.streams.get(conId)
        if previous_ticks is not None and not previous_ticks.empty:
            if normalized.index[-1] < previous_ticks.index[-1]:
                logging.warning('ignoring out-of-order ticker for conId %s at %s', conId, normalized.index[-1])
                return
        for key, buffer in getattr(self, '_live_bar_buffers', {}).items():
            if key[0] == conId:
                buffer.append(normalized)
        if conId not in self.streams:
            self.streams[conId] = normalized
        else:
            self.streams[conId] = pd.concat([self.streams[conId], normalized], axis=0)
        self._cap_tick_stream(conId)

        # Persisted position policies survive replacement/removal and even a
        # service restart with no strategy left in the deployment YAML.
        managed_names = set()
        for (name, cid), context in list(getattr(self, '_managed_contexts', {}).items()):
            if cid != conId:
                continue
            managed_names.add(name)
            try:
                frame = self._strategy_frame(conId, context.bar_size)
                if frame is not None and not frame.empty:
                    self._dispatch_management_bar(context, conId, frame)
            except Exception:
                logging.exception('persisted exit-policy dispatch failed for %s conId %s', name, conId)

        # Execute the strategies attached to the conId. CRITICAL: each strategy
        # is isolated in its own try/except. Without this, one strategy raising
        # (e.g. a pandas IndexError on a short window) propagates all the way up
        # to the pubsub subscriber loop, which calls on_error and permanently
        # DETACHES this observer from the ticker subject — every subsequent tick
        # for ALL strategies is then silently dropped and open positions go
        # unmanaged. A single misbehaving strategy must not take down the feed.
        strategies = list(self.strategies.get(conId, []))
        strategies += [s for s in getattr(self, '_retired_strategies', [])
                       if conId in (s.conids or []) and s.name not in {x.name for x in strategies}]
        for strategy in strategies:
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
                # Position management remains live through DISABLED/ERROR and
                # deployment removal. It has a separate dispatch watermark.
                if strategy.name not in managed_names:
                    self._dispatch_management_bar(strategy, conId, frame)
                if strategy.state not in (StrategyState.RUNNING, StrategyState.WAITING_HISTORICAL_DATA):
                    continue
                if (hasattr(self, '_history_pool') and (getattr(strategy, 'historical_days_prior', 0) or 0) > 0
                        and (conId, strategy.bar_size) not in self._hist_bars):
                    continue
                dkey = (conId, strategy.name)
                previous = self._last_dispatched_bar.get(dkey)
                if previous is not None and last_bar <= previous:
                    continue
                # Out-of-session bars are NOT market data (AUDIT_ROADMAP G8).
                # normalize_ticker falls back to the bid/ask midpoint, so an
                # out-of-hours quote forms a real-looking bar; three of them on
                # a Sunday were dispatched to orb_wds on 2026-07-26. Extended
                # hours ARE in-session — this is not an RTH filter; see
                # trader/data/market_session.py. Deliberately does NOT update
                # _last_dispatched_bar, so bar_age_s keeps telling the truth.
                if (pd.Timedelta(BarSize.to_pandas_freq(strategy.bar_size)) < pd.Timedelta(days=1)
                        and not self._bar_in_session(ticker.contract, last_bar)):
                    self._note_out_of_session(conId, last_bar)
                    continue
                worker = getattr(self, '_callback_workers', {}).get(strategy.name)
                if worker is None and getattr(self, '_isolate_callbacks', False):
                    # enable/redeploy registers the worker from a thread after
                    # the strategy is already RUNNING. A bar in that window is
                    # retried on the next tick (the watermark is not advanced).
                    # Failing the strategy here set ERROR and revoked its
                    # opening authority while the enable RPC reported success.
                    self._note_worker_pending(strategy.name or '?', conId)
                    continue
                self._last_dispatched_bar[dkey] = last_bar
                # G6: evaluate time-based exits once per new bar, whether or
                # not the strategy emits a signal (mirrors the backtester's
                # per-bar exit_conditions check).
                if worker is not None:
                    getattr(self, '_worker_pending_logged', set()).discard(strategy.name)
                    generation = strategy.ctx.deployment_generation
                    worker.submit(conId, frame, last_bar, generation,
                                  lambda result, s=strategy: self._post_callback(self._callback_result, s, result),
                                  lambda error, s=strategy: self._post_callback(self._callback_error, s, error))
                    continue
                signal = strategy.on_prices(frame)
            except Exception as ex:
                logging.exception(
                    'strategy %s raised on_prices for conId %s; disabling it and '
                    'continuing the tick feed', getattr(strategy, 'name', '?'), conId)
                try:
                    strategy.state = StrategyState.ERROR
                    self._revoke_opening(strategy)
                except Exception:
                    pass
                continue

            self._handle_signal(strategy, conId, signal, last_bar)

    def _note_worker_pending(self, name: str, conId: int) -> None:
        logged = getattr(self, '_worker_pending_logged', None)
        if logged is None:
            logged = self._worker_pending_logged = set()
        if name in logged:
            return
        logged.add(name)
        logging.warning('strategy %s is RUNNING but its callback worker is not registered yet; '
                        'deferring bars (first: conId %s) until it is', name, conId)

    def _handle_signal(self, strategy, conId, signal, last_bar):
        if not signal:
            return
        signal.source_name = strategy.name
        signal.conid = conId
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
            if hasattr(self, '_loop'):
                self._queue_signal_audit(event)
            else:
                self.event_store.append(event)

            # Publish signal via MessageBus for cross-strategy use and subscribers
            self.zmq_messagebus_client.write('signal', signal)
        except Exception:
            # A failure persisting/publishing one signal must not kill the
            # feed or the other strategies either.
            logging.exception(
                'failed to record/publish signal from %s for conId %s',
                getattr(strategy, 'name', '?'), conId)

        try:
            self._submit_auto_execution(strategy, conId, signal, last_bar)
        except Exception:
            logging.exception('auto-execute submission failed for %s conId %s',
                              getattr(strategy, 'name', '?'), conId)

    def _queue_signal_audit(self, event):
        """A busy analytics store cannot stall bars or independent exits.

        Executed intents have their own durable journal. This bounded queue is
        the best-effort signal audit stream, including nonexecuted signals.
        Overflow is visible in health and logs, never an unbounded memory queue.
        """
        if not hasattr(self, '_signal_audit_tasks'):
            self._signal_audit_tasks = set()
        if len(self._signal_audit_tasks) >= 256:
            self._signal_audit_overflow = getattr(self, '_signal_audit_overflow', 0) + 1
            logging.error('signal audit backlog full for %s conId %s', event.strategy_name, event.conid)
            return
        async def record():
            try:
                await asyncio.to_thread(self.event_store.append, event)
            except Exception:
                logging.exception('signal audit persistence failed for %s conId %s', event.strategy_name, event.conid)
        task = self._loop.create_task(record())
        self._signal_audit_tasks.add(task)
        task.add_done_callback(self._signal_audit_tasks.discard)


    def _start_callback_worker(self, strategy, deployment_config=None):
        from trader.strategy.callback_worker import StrategyCallbackWorker
        self._stop_callback_worker(strategy)
        if not hasattr(self, '_callback_workers'):
            self._callback_workers = {}
        conids = set(strategy.conids or [])
        if strategy.universe:
            universe = self.universe_accessor.get(strategy.universe)
            conids.update(sd.conId for sd in universe.security_definitions)
        worker = StrategyCallbackWorker(
            source_path=strategy._source_path, source_hash=strategy._source_hash,
            class_name=strategy.ctx.class_name, context=strategy.ctx,
            initial_state=strategy.state,
            max_pending=max(1, min(256, len(conids))),
            deployment_config=deployment_config,
        )
        try:
            worker.start()
            metadata = worker.wait_ready()
        except Exception:
            worker.stop()
            raise
        strategy.ctx.params = metadata['effective_params']
        strategy.ctx.effective_config_hash = metadata['effective_config_hash']
        # Register before the state becomes dispatchable: the tick loop reads
        # both without a lock, and a RUNNING strategy with no worker is the
        # shape the dispatcher must otherwise defer around.
        self._callback_workers[strategy.name] = worker
        strategy.state = StrategyState(metadata['state'])

    def _stop_callback_worker(self, strategy):
        worker = getattr(self, '_callback_workers', {}).pop(strategy.name, None)
        if worker is not None:
            worker.stop()

    def _post_callback(self, callback, strategy, value):
        self._loop.call_soon_threadsafe(callback, strategy, value)

    def _callback_result(self, strategy, result):
        if result.signal is not None and result.signal.action == Action.BUY:
            if (self._deployment_generations.get(strategy.name) != result.generation
                    or strategy.state != StrategyState.RUNNING):
                return
        self._handle_signal(strategy, result.conid, result.signal, result.bar_ts)

    def _callback_error(self, strategy, error):
        if (self.get_strategy(strategy.name) is not strategy
                or self._deployment_generations.get(strategy.name) != error.generation):
            return
        strategy.state = StrategyState.ERROR
        self._revoke_opening(strategy)
        logging.error('strategy worker %s failed: %s: %s', strategy.name,
                      error.error_type, error.message)

    def _submit_auto_execution(self, strategy: Strategy, conId: int, signal, last_bar) -> None:
        """Flatten the signal + strategy config to primitives and enqueue for
        the auto-executor worker. Guards are evaluated on the worker so the
        skip decision lands in the persistent decision log."""
        ctx = strategy._context
        # Bar interval in seconds for the executor's stale-bar gate. Unknown/
        # unparseable intervals remain 0 so the executor refuses an open whose
        # freshness cannot be established.
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
            bar_ts=_session_bar_ts(last_bar, ctx.params if ctx else None),
            probability=float(getattr(signal, 'probability', 0.0) or 0.0),
            risk=float(getattr(signal, 'risk', 0.0) or 0.0),
            quantity=float(getattr(signal, 'quantity', 0.0) or 0.0),
            auto_execute=bool(ctx.auto_execute) if ctx else False,
            paper_only=bool(ctx.paper_only) if ctx else False,
            state_running=strategy.state == StrategyState.RUNNING,
            close_by_time=getattr(signal, 'close_by_time', None),
            max_hold_bars=getattr(signal, 'max_hold_bars', None),
            bar_size_seconds=bar_size_seconds,
            pyramid_max_adds=int(getattr(ctx, 'pyramid_max_adds', 0) or 0) if ctx else 0,
            trade_amount=float(getattr(ctx, 'trade_amount', 0.0) or 0.0) if ctx else 0.0,
            # Manifest primitives — carried exactly like pyramid_max_adds.
            # None on every field (no manifest declared) leaves the executor's
            # gate a no-op, so behaviour is unchanged for un-manifested
            # strategies.
            manifest_allowed_conids=(
                list(ctx.manifest_allowed_conids)
                if ctx and getattr(ctx, 'manifest_allowed_conids', None) is not None
                else None),
            manifest_direction=(
                getattr(ctx, 'manifest_direction', None) if ctx else None),
            manifest_max_opens_per_day=(
                getattr(ctx, 'manifest_max_opens_per_day', None) if ctx else None),
            manifest_max_opens_per_hour=(
                getattr(ctx, 'manifest_max_opens_per_hour', None) if ctx else None),
            deployment_generation=getattr(ctx, 'deployment_generation', '') if ctx else '',
        )
        self.auto_executor.submit_signal(work)

    def _dispatch_management_bar(self, strategy, conId, frame):
        if not hasattr(self, '_last_managed_bar'):
            self._last_managed_bar = {}
        key = (conId, strategy.name)
        last_bar = frame.index[-1]
        previous = self._last_managed_bar.get(key)
        if previous is None or last_bar > previous:
            if self._check_time_exit(strategy, conId, frame, last_bar):
                self._last_managed_bar[key] = last_bar

    def _refresh_management_contexts(self):
        records = self.auto_executor.managed_positions()
        contexts = {}
        for record in records:
            name, conid = record['strategy_name'], record['conid']
            seconds = record.get('bar_size_seconds')
            session_tz = record.get('session_tz')
            if seconds is None or session_tz is None:
                # The holding predates recorded exit policy. Its rule can only
                # run with the deployed strategy's real interval and session:
                # a default (one minute, UTC) executes a REAL reduction at the
                # wrong time. Use the loaded strategy's context, else defer.
                loaded = self.get_strategy(name)
                if loaded is None:
                    self._note_policy_unknown(name, conid)
                    continue
                if seconds is None:
                    seconds = pd.Timedelta(BarSize.to_pandas_freq(loaded.bar_size)).total_seconds()
                if session_tz is None:
                    session_tz = (loaded.ctx.params or {}).get('SESSION_TZ') or 'America/New_York'
            bar_size = next((size for size in BarSize if size <= BarSize.Days1
                             and pd.Timedelta(BarSize.to_pandas_freq(size)).total_seconds() == seconds), None)
            if bar_size is None:
                logging.error('cannot recover exit policy for %s conId %s: invalid interval %r', name, conid, seconds)
                continue
            entry = pd.Timestamp(record['entry_bar_ts'])
            entry = entry.tz_localize('UTC') if entry.tz is None else entry.tz_convert('UTC')
            days = int(max(1, (pd.Timestamp.now(tz='UTC') - entry).days + 2))
            context = Strategy()
            context.install(StrategyContext(
                name=name, bar_size=bar_size, conids=[conid], universe=None,
                historical_days_prior=days, paper_only=False,
                storage=self.storage, universe_accessor=self.universe_accessor, logger=logging,
                params={'SESSION_TZ': session_tz},
            ))
            context.disable()
            contexts[(name, conid)] = context
            if (name, conid) not in getattr(self, '_managed_contexts', {}):
                self._invalidate_history(conid, bar_size)
        self._managed_contexts = contexts

    def _note_policy_unknown(self, name: str, conid: int) -> None:
        logged = getattr(self, '_policy_unknown_logged', None)
        if logged is None:
            logged = self._policy_unknown_logged = set()
        if (name, conid) in logged:
            return
        logged.add((name, conid))
        logging.error('exit policy for %s conId %s has no recorded interval/session and the strategy is not '
                      'loaded; time exits are deferred (redeploy the strategy or close the holding manually)',
                      name, conid)

    def _check_time_exit(self, strategy: Strategy, conId: int, frame, last_bar) -> bool:
        """If this (strategy, conid) has an open auto position, report the new
        bar so the executor can evaluate close_by_time / max_hold_bars.
        The worker durably accumulates completed bars after this entry epoch;
        the retained frame is only an observation window, not a lifetime count.
        Return False when admission fails so the same bar can be retried."""
        try:
            name = strategy.name or 'unknown'
            entry_ts = self.auto_executor.open_entry_bar(name, conId)
            if entry_ts is None:
                return True
            idx = frame.index
            # Persisted naive entry timestamps represent UTC instants.
            idx = self._utc_index(idx)
            entry = pd.Timestamp(entry_ts)
            entry = entry.tz_localize('UTC') if entry.tz is None else entry.tz_convert('UTC')
            observed = tuple(idx[idx > entry].unique().sort_values().to_pydatetime())
            ctx = getattr(strategy, '_context', None)
            exit_bar = _session_bar_ts(last_bar, getattr(ctx, 'params', None) if ctx else None)
            return self.auto_executor.submit_bar(
                name, conId, exit_bar, len(observed),
                entry_bar_ts=entry, observed_bar_timestamps=observed,
            ) is not False
        except Exception:
            logging.exception('time-exit check failed for %s conId %s',
                              getattr(strategy, 'name', '?'), conId)
            return False

    def on_ticker_error(self, ex: Exception):
        logging.error('StrategyRuntime ticker stream error: %s', ex, exc_info=True)

    def on_ticker_completed(self):
        logging.debug('StrategyRuntime.on_completed')

    def subscribe(self, strategy: Strategy, contract: Contract) -> None:
        """Route ``contract``'s ticks to ``strategy``, asking trader_service to
        publish them if nobody has yet.

        The two facts are tracked SEPARATELY, and that separation is the fix
        for a live outage. "Which strategies want this conId" is our state and
        survives anything; "trader_service is publishing this conId" is THEIR
        state and dies with their process. This used to key the publish request
        off the strategies dict, so after a trader_service restart every conId
        was still in the dict, no request was re-sent, and the feed stayed dead
        until strategy_service happened to restart too. See
        ``_note_trader_boot`` for how the restart is detected.
        """
        logging.debug('strategy_runtime.subscribe() contract: {} strategy: {}'.format(contract, strategy))
        subscribers = self.strategies.setdefault(contract.conId, [])
        if strategy not in subscribers:
            subscribers.append(strategy)
        if contract.conId not in self._published_conids:
            self.trader_client.rpc().publish_contract(contract=contract, delayed=False)
            self._published_conids.add(contract.conId)

    def _note_trader_boot(self) -> bool:
        """Detect a trader_service RESTART and forget what it was publishing.

        Returns True when a restart was observed, so the caller knows this
        cycle's subscribe() calls are re-establishing a dead feed rather than
        doing nothing.

        A restart is invisible from here without this check: the RPC socket
        reconnects transparently, every call keeps working, and only the ticks
        stop. `mmr verify`'s tick_flow check and a `ticks_60s=0` pulse were the
        only signals, and neither self-heals — the 2026-07-27 outage ran 30
        minutes with both services reporting healthy, every strategy blind, and
        only the broker-side disaster stops still protecting the book.

        Unreadable status is NOT treated as a restart. Re-publishing every
        conId on a transient RPC failure would hammer trader_service with
        duplicate subscriptions on exactly the cycles it is least able to
        serve them.
        """
        try:
            status = self.trader_client.rpc().get_status()
        except (TimeoutError, ConnectionError) as ex:
            logging.debug('could not read trader_service status this cycle: %s', ex)
            return False
        except Exception as ex:
            # Deliberately broad, against the usual "let real bugs propagate"
            # rule. This check is what REPAIRS a dead feed; if it raises, the
            # reconcile body dies and nothing re-subscribes, which is strictly
            # worse than the outage it exists to fix. Logged at WARNING so it
            # cannot rot silently.
            logging.warning('trader_service boot check failed: %s', ex)
            return False
        boot_id = (status or {}).get('boot_id')
        if not boot_id:
            return False   # older trader_service; nothing to compare against

        previous, self._trader_boot_id = self._trader_boot_id, boot_id
        if previous == boot_id:
            return False

        # `previous is None` counts as a mismatch, and that is the whole point.
        # Normally there is nothing to discard, because a fresh process has
        # published nothing. But the two facts are learned INDEPENDENTLY, so
        # they can disagree: subscriptions can be published to instance A while
        # the first status read only succeeds against instance B, if
        # trader_service restarts inside our startup window. Then we hold
        # publishes we cannot attribute to the live process. Treating that as
        # "first read, nothing to compare" left the feed dead — observed on the
        # very first live test of this fix, which is why it is not written that
        # way. Unattributable publishes are re-sent; the cost is a duplicate
        # subscription, the alternative is silence.
        stale = len(self._published_conids)
        if stale:
            logging.warning(
                'trader_service boot %s -> %s — its market-data subscriptions '
                'died with it; re-subscribing %d conId(s)',
                previous or 'unknown', boot_id, stale)
            self._published_conids.clear()
            return True

        logging.info('trader_service boot id is %s', boot_id)
        return False

    def subscribe_universe(self, strategy: Strategy, universe_name: str) -> None:
        logging.debug('strategy_runtime.subscribe_universe() universe: {} strategy: {}'.format(universe_name, strategy))
        universe = self.universe_accessor.get(universe_name)

        for security in universe.security_definitions:
            self.subscribe(strategy, SecurityDefinition.to_contract(security))

    def _gauntlet_allows_arming(self, name: str, filepath: str, class_name: str) -> bool:
        """'No hash, no live' arm gate for auto_execute strategies: the
        current source hash must hold a PASS gauntlet record.

        Default is warn-only (the strategy still arms) so the live roster
        doesn't silently disarm on a restart before gauntlet records exist
        in this host's DB. Set ``MMR_GAUNTLET_ENFORCE=1`` (strict '1') to
        refuse arming — the strategy then loads DISARMED (auto_execute
        off), which keeps the executor able to close attributed positions
        but never open new ones.
        """
        from trader.data.backtest_store import compute_strategy_hash
        from trader.data.gauntlet_store import GauntletStore

        enforce = os.environ.get('MMR_GAUNTLET_ENFORCE', '') == '1'
        code_hash = compute_strategy_hash(filepath)
        problem = ''
        last_pass_hash = None
        if not code_hash:
            problem = f'source {filepath!r} could not be hashed'
        else:
            duckdb_path = getattr(self, 'duckdb_path', '') or ''
            if not duckdb_path:
                problem = 'gauntlet store unavailable (no duckdb_path)'
            else:
                try:
                    store = GauntletStore(duckdb_path)
                    if store.has_pass(code_hash, class_name):
                        return True
                    latest = store.latest_pass_for_class(class_name)
                    last_pass_hash = latest.code_hash if latest else None
                    problem = 'no PASS gauntlet record for current hash'
                except Exception as ex:
                    problem = f'gauntlet store error: {type(ex).__name__}: {ex}'

        hash_pair = (f'current={code_hash or "<unhashable>"} '
                     f'last_pass={last_pass_hash or "<none>"}')
        hint = f'run: mmr strategies gauntlet {filepath} --class {class_name}'
        if enforce:
            logging.error(
                'refusing to arm auto_execute strategy %s: %s (%s) — loading '
                'DISARMED; %s', name, problem, hash_pair, hint)
            return False
        logging.warning(
            'auto_execute strategy %s has no gauntlet PASS: %s (%s) — arming '
            'anyway (MMR_GAUNTLET_ENFORCE unset); %s', name, problem, hash_pair, hint)
        return True

    @_serialized_deployment
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
        pyramid_max_adds: int = 0,
        trade_amount: float = 0.0,
        manifest: Optional[Dict] = None,
    ) -> None:

        if not name or not class_name or not module or not bar_size_str:
            raise ValueError('invalid config. need name, bar_size, class_name and module specified')

        # paper_only gate: refuse to load strategies marked paper_only when the
        # trader_service is bound to a live account. Routing is service-level
        # (one trader_service → one IB account), so this is the only place it
        # makes sense to enforce the flag.
        if paper_only and not self.paper_trading:
            existing = self.get_strategy(name)
            if existing is not None:
                self._retire_strategy(existing)
            logging.error(
                'refusing to load strategy %s: paper_only=True but trader_service '
                'is running in LIVE mode', name,
            )
            return

        strategies_dir = os.path.realpath(os.path.expanduser(self.strategies_directory))

        def resolve_module_path(filename) -> str:
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

            filepath = os.path.realpath(filepath)
            if not filepath.startswith(strategies_dir + os.sep) and filepath != strategies_dir:
                raise ValueError(
                    f'strategy module {filename!r} resolves outside strategies '
                    f'directory {strategies_dir!r}; refusing to load'
                )

            if not os.path.exists(filepath):
                raise FileNotFoundError(f'strategy module not found: {filepath}')
            return filepath

        def load_class_from_file(filepath, classname):
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
                # Read current bytes directly. A same-second edit with an
                # unchanged length must not resurrect stale .pyc code.
                with open(filepath, 'rb') as source:
                    exec(compile(source.read(), filepath, 'exec'), module.__dict__)
            except Exception:
                sys.modules.pop(module_name, None)
                raise
            return getattr(module, classname, None)

        try:
            filepath = resolve_module_path(module)
            parsed_bar_size = BarSize.parse_str(bar_size_str)
            if parsed_bar_size in (BarSize.Weeks1, BarSize.Months1):
                raise ValueError('unsupported live bar interval: weekly/monthly bars are backtest-only')
            with open(filepath, 'rb') as source:
                source_hash = hashlib.sha256(source.read()).hexdigest()
            requested_config = dict(
                name=name, bar_size=bar_size_str, conids=conids or [], universe=universe,
                historical_days_prior=historical_days_prior, module=filepath,
                class_name=class_name, description=description, paper_only=paper_only,
                auto_execute=auto_execute, params=params or {},
                pyramid_max_adds=pyramid_max_adds, trade_amount=trade_amount,
                manifest=manifest, source_hash=source_hash)
            fingerprint = hashlib.sha256(json.dumps(
                requested_config, sort_keys=True, allow_nan=False).encode()).hexdigest()
            existing = self.get_strategy(name)
            previous_state = existing.state if existing is not None else None
            if existing is not None:
                if getattr(existing, '_requested_fingerprint', None) == fingerprint:
                    return
                # Do this BEFORE imports/validation. Failed replacement cannot
                # leave the superseded opening authority alive.
                self._retire_strategy(existing)
            # Gauntlet arm gate ("no hash, no live"): auto_execute needs a
            # PASS gauntlet record for the exact current source hash. The
            # CLI enforces this at deploy/enable, but the YAML can be
            # hand-edited and reconcile re-reads it — this is the
            # authoritative check.
            if auto_execute and not self._gauntlet_allows_arming(name, filepath, class_name):
                auto_execute = False

            # Strategy manifest — validate the declared trading envelope BEFORE
            # arming. Reuse the gauntlet-disarm pattern: a bad manifest loads
            # the strategy DISARMED (auto_execute stripped) with a loud ERROR,
            # rather than silently defaulting to "unchecked". The subscription
            # set is the explicit conids plus any universe expansion — an
            # allowed conId must be within it.
            subscription_conids = set(int(c) for c in (conids or []))
            if universe and getattr(self, 'universe_accessor', None) is not None:
                try:
                    _u = self.universe_accessor.get(universe)
                    subscription_conids |= {
                        int(d.conId) for d in _u.security_definitions}
                except Exception:
                    # Universe expansion failure leaves the subscription set as
                    # the explicit conids; the allowed-conids check errs toward
                    # disarming (safe) rather than passing an unverifiable set.
                    logging.warning(
                        'manifest: could not expand universe %r for %s — '
                        'validating allowed_conids against explicit conids only',
                        universe, name)
            manifest_fields, manifest_disarm = _validate_manifest(
                manifest, subscription_conids, auto_execute)
            if manifest_disarm is not None:
                logging.error(
                    'strategy %s has an invalid manifest: %s — loading DISARMED '
                    '(auto_execute stripped, manifest ignored)', name, manifest_disarm)
                auto_execute = False
                manifest_fields = {
                    'manifest_allowed_conids': None,
                    'manifest_direction': None,
                    'manifest_max_opens_per_day': None,
                    'manifest_max_opens_per_hour': None,
                }

            isolated = getattr(self, '_isolate_callbacks', False)
            class_object = Strategy if isolated else load_class_from_file(filepath, class_name)
            if not class_object:
                return

            if isolated or (inspect.isclass(class_object) and issubclass(class_object, Strategy) and class_object is not Strategy):
                logging.debug('found implementation of Strategy {}'.format(class_object))
                if not isolated and class_object.on_prices is Strategy.on_prices:
                    raise ValueError(
                        'unsupported live dispatch capability: implement on_prices; '
                        'on_bar/on_panel are backtest-only APIs')

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
                    pyramid_max_adds=pyramid_max_adds,
                    trade_amount=trade_amount,
                    manifest_allowed_conids=manifest_fields['manifest_allowed_conids'],
                    manifest_direction=manifest_fields['manifest_direction'],
                    manifest_max_opens_per_day=manifest_fields['manifest_max_opens_per_day'],
                    manifest_max_opens_per_hour=manifest_fields['manifest_max_opens_per_hour'],
                    params={},
                )
                instance.install(context)
                from trader.strategy.parameters import apply_param_overrides
                if isolated:
                    context.params = dict(params or {})
                else:
                    apply_param_overrides(instance, params)
                instance._source_path = filepath
                instance._source_hash = source_hash
                instance._requested_fingerprint = fingerprint
                instance._requested_params = dict(params or {})
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
                elif previous_state == StrategyState.RUNNING:
                    instance.enable()

                self._grant_generation(instance)
                if isolated:
                    self._start_callback_worker(instance, requested_config)
                else:
                    context.effective_config_hash = hashlib.sha256(json.dumps(
                        {**requested_config, 'params': context.params, 'auto_execute': auto_execute},
                        sort_keys=True, allow_nan=False).encode()).hexdigest()

                self.strategy_implementations.append(cast(Strategy, instance))
                for conid in conids or []:
                    if hasattr(self, '_hist_bars'):
                        self._invalidate_history(conid, context.bar_size)

        except Exception as ex:
            worker = getattr(self, '_callback_workers', {}).pop(name, None)
            if worker is not None:
                worker.stop()
            existing = self.get_strategy(name)
            if existing is not None:
                self._retire_strategy(existing)
            # Load failures used to be swallowed at DEBUG; a config typo could
            # silently disable a strategy. Log at ERROR with the cause so the
            # operator sees it.
            logging.error('failed to load strategy %s (%s): %s', name, class_name, ex)

    @_serialized_deployment
    def config_loader(self, config_file: str):
        config_file = os.path.expanduser(config_file)
        logging.debug('loading config file {}'.format(config_file))
        # safe_load refuses Python-object tags — YAML-injection hardening.
        with open(config_file, 'r') as conf_file:
            config = yaml.safe_load(conf_file)
        if not config or 'strategies' not in config:
            logging.warning('strategy config %s has no strategies section', config_file)
            return
        entries = config['strategies']
        if not isinstance(entries, list) or any(not isinstance(e, dict) for e in entries):
            raise ValueError('strategies must be a list of mappings')
        names = [e.get('name') for e in entries]
        if any(not isinstance(n, str) or not n for n in names) or len(set(names)) != len(names):
            raise ValueError('strategy names must be nonempty and unique')
        for entry in entries:
            if not entry.get('bar_size'):
                raise ValueError(f"strategy {entry['name']} requires bar_size")
        for strategy in list(self.strategy_implementations):
            if strategy.name not in names:
                self._retire_strategy(strategy)
        for strategy_config in entries:
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
                pyramid_max_adds=int(strategy_config.get('pyramid_max_adds', 0) or 0),
                trade_amount=float(strategy_config.get('trade_amount', 0.0) or 0.0),
                manifest=strategy_config.get('manifest'),
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

        source_changed = False
        for strategy in self.strategy_implementations:
            path = getattr(strategy, '_source_path', None)
            if path:
                try:
                    with open(path, 'rb') as source:
                        source_changed |= hashlib.sha256(source.read()).hexdigest() != getattr(strategy, '_source_hash', None)
                except OSError:
                    source_changed = True
        if current_mtime != self._config_mtime or source_changed:
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

        # 2. Re-subscribe all strategies (idempotent — only conIds trader_service
        # is not already publishing trigger publish_contract). Checked FIRST so
        # that if trader_service restarted, this cycle's subscribe() calls
        # actually re-establish the feed instead of no-opping against stale
        # bookkeeping.
        self._note_trader_boot()
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
        for (name, conid), context in list(getattr(self, '_managed_contexts', {}).items()):
            if conid not in self._published_conids:
                try:
                    definitions = self.trader_client.rpc().resolve_symbol(conid)
                    exact = [sd for sd in definitions or [] if sd.conId == conid]
                    if not exact:
                        raise ValueError(f'no exact contract for managed conId {conid}')
                    contract = SecurityDefinition.to_contract(exact[0])
                    self.trader_client.rpc().publish_contract(contract=contract, delayed=False)
                    self._published_conids.add(conid)
                except Exception:
                    logging.exception('could not restore exit feed for %s conId %s', name, conid)

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
                    await asyncio.to_thread(tick_data.write, security, df)
                    self._invalidate_history(security.conId, bar_size)
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
                                strategy_name=strategy.name or 'unknown',
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
                            strategy_name=strategy.name or 'unknown',
                        )
                    except IBConnectivityError:
                        raise
        logging.debug('finished get_historical_data()')

    async def _management_loop(self):
        """Broker reconciliation/protection must not depend on signal code."""
        expected = asyncio.get_running_loop().time()
        while True:
            now = asyncio.get_running_loop().time()
            self._event_loop_lag_seconds = max(0.0, now - expected)
            try:
                self.auto_executor.submit_management()
                self._refresh_management_contexts()
            except Exception:
                logging.exception('independent position management submission failed')
            expected = asyncio.get_running_loop().time() + 1.0
            await asyncio.sleep(1)

    async def run(self):
        try:
            await self._run_services()
        finally:
            task = getattr(self, '_management_task', None)
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            audit_tasks = list(getattr(self, '_signal_audit_tasks', ()))
            if audit_tasks:
                _, pending = await asyncio.wait(audit_tasks, timeout=2)
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
            workers = list(getattr(self, '_callback_workers', {}).values())
            self._callback_workers = {}
            for worker in workers:
                await asyncio.to_thread(worker.stop)
            executor = getattr(self, 'auto_executor', None)
            if executor is not None:
                await asyncio.to_thread(executor.stop)
            pool = getattr(self, '_history_pool', None)
            if pool is not None:
                pool.shutdown(wait=False, cancel_futures=True)
            subscription = getattr(self, 'subscription', None)
            if subscription is not None:
                subscription.dispose()
            subscriber = getattr(self, 'zmq_subscriber', None)
            if subscriber is not None:
                subscriber.subscriber_close()
            server = getattr(self, 'zmq_strategy_rpc_server', None)
            if server is not None:
                await server.aclose()
            client = getattr(self, 'trader_client', None)
            if client is not None:
                await asyncio.to_thread(client.close)
            bus_client = getattr(self, 'zmq_messagebus_client', None)
            if bus_client is not None:
                await bus_client.disconnect()
            history = getattr(self, 'historical_data_client', None)
            if history is not None:
                history.shutdown()

    async def _run_services(self):
        logging.info('starting strategy_runtime')
        logging.debug('StrategyRuntime.run()')
        self._loop = asyncio.get_running_loop()
        self._management_task = asyncio.create_task(self._management_loop())

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
        await asyncio.to_thread(self.config_loader, self.strategy_config_file)

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
