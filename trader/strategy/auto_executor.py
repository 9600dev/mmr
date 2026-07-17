"""Signal auto-execution — the strategy-side half of live trading (AUDIT_ROADMAP G6).

Bridges strategy signals to real orders. Until this existed, a Signal was
logged, written to the event store, published to the MessageBus 'signal'
topic — and nothing consumed it. ``auto_execute: true`` in
strategy_runtime.yaml was stored and displayed but never acted on.

Semantics deliberately mirror the backtester that validated the deployed
strategies (``trader/simulation/backtester.py``):

* **Long-only.** BUY opens a position, SELL closes the held quantity,
  SELL-when-flat is a no-op. No shorting, no pyramiding — one open position
  per (strategy, conid).
* **Time exits.** ``Signal.close_by_time`` / ``max_hold_bars`` are honored
  live: the runtime reports each new bar and the executor synthesizes a
  close when a condition triggers, comparing the *bar timestamp's*
  time-of-day (not wall clock) exactly like the backtester does.
* **Attribution.** The executor only ever closes quantity it opened itself
  (clamped to the live broker position). A manually-opened position in the
  same instrument is never touched.

Execution routes through the existing proposal pipeline (``sdk.MMR.propose``
→ ``approve``) rather than around it, so every auto-trade gets: position
sizing (confidence/ATR/liquidity-aware), FX-correct quantity conversion, the
proposal audit trail with CAS state transitions, and the server-side trading
filter + risk gate in ``place_expressive_order``.

Threading model: strategy signals arrive on the runtime's asyncio loop; all
execution work happens on a single daemon worker thread consuming a queue.
One worker means decisions are serialized — no per-key locking, and the
synchronous SDK (which uses ``asyncio.run`` internally and therefore must not
run on the event loop) gets a thread to itself. Queue items are plain
primitives; nothing mutable is shared across the boundary.

Safety rails, in the order they are checked:
  1. Global kill switch (``MMR_AUTO_EXECUTE_DISABLED=1``).
  2. Strategy must be RUNNING with ``auto_execute=True``.
  3. ``paper_only`` strategies refuse to trade in live mode.
  4. Per-(strategy, conid, bar) dedup — persisted, so a restart can't
     re-execute the same bar's signal.
  5. Per-(strategy, conid) cooldown between executions (default 300s).
  6. conId precision check: the strategy's conId must resolve exactly in the
     universe DB and round-trip back to the same conId via the symbol+hints
     the proposal pipeline will use — otherwise refuse loudly (stale conIds
     must never trade a different instrument).
  7. Everything the executioner already enforces server-side (trading
     filter, risk gate: position size, daily loss, open orders, signal rate).
"""
import datetime as dt
import os
import queue
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

from trader.common.logging_helper import setup_logging
from trader.data.duckdb_store import DuckDBConnection
from trader.data.event_store import EventStore, EventType, TradingEvent
from trader.objects import Action

# Named logger routed to the strategy_service log (logging.yaml) so the
# OPENED/CLOSING/CLOSED/CLOSE FAILED audit lines land in the same file as the
# BUY/SELL signal lines — the trading monitor watches exactly one file. A
# plain `import logging` here would route through the ROOT logger into
# trader.log instead.
logging = setup_logging(module_name='auto_executor')


class AutoExecutionError(Exception):
    pass


# ---------------------------------------------------------------------------
# Pure decision logic — no I/O, fully unit-testable.
# ---------------------------------------------------------------------------

@dataclass
class SignalWork:
    """A signal, flattened to primitives for the worker queue."""
    strategy_name: str
    conid: int
    action: Action
    bar_ts: Any                       # pd.Timestamp of the bar that produced the signal
    probability: float = 0.0
    risk: float = 0.0
    quantity: float = 0.0             # >0 = strategy-specified size (BUY only)
    auto_execute: bool = False
    paper_only: bool = False
    state_running: bool = True
    close_by_time: Optional[dt.time] = None
    max_hold_bars: Optional[int] = None
    bar_size_seconds: float = 0.0     # 0 = unknown -> stale-bar gate disabled


def bar_age_seconds(bar_ts, now_utc: Optional[dt.datetime] = None) -> Optional[float]:
    """Seconds between a bar timestamp and now. Naive timestamps are UTC wall
    time (the runtime's frames are UTC; ``_naive`` strips tz without
    converting). None on anything unparseable — the gate then stays off
    rather than blocking trades on a formatting quirk."""
    if bar_ts is None:
        return None
    try:
        now_utc = now_utc or dt.datetime.now(dt.timezone.utc)
        ts = bar_ts
        if hasattr(ts, 'to_pydatetime'):
            ts = ts.to_pydatetime()
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=dt.timezone.utc)
        return (now_utc - ts).total_seconds()
    except Exception:
        return None


@dataclass
class BarWork:
    """A new-bar notification for time-exit evaluation."""
    strategy_name: str
    conid: int
    bar_ts: Any
    bars_held: int


@dataclass
class Directive:
    kind: str          # 'open' | 'close' | 'skip'
    reason: str
    quantity: Optional[float] = None


def decide_signal(
    work: SignalWork,
    *,
    kill_switch: bool,
    paper_trading: bool,
    held_qty: float,
    already_executed_bar: bool,
    cooldown_active: bool,
    bar_age_seconds: Optional[float] = None,
    stale_bar_multiple: float = 3.0,
) -> Directive:
    """Long-only decision matching backtester semantics (backtester.py:372-424).

    ``held_qty`` is the executor-attributed open quantity for this
    (strategy, conid) — NOT the raw broker position, so a manual position in
    the same instrument neither blocks a strategy entry nor gets closed by a
    strategy exit. The risk gate server-side still sees the whole book.
    """
    if kill_switch:
        return Directive('skip', 'kill switch (MMR_AUTO_EXECUTE_DISABLED) is set')
    if not work.auto_execute:
        return Directive('skip', 'auto_execute is false')
    if not work.state_running:
        return Directive('skip', 'strategy is not RUNNING')
    if work.paper_only and not paper_trading:
        return Directive('skip', 'paper_only strategy in live trading mode')
    if already_executed_bar:
        return Directive('skip', f'already executed for bar {work.bar_ts}')

    if work.action == Action.BUY:
        if held_qty > 0:
            return Directive('skip', 'already holding — no pyramiding')
        # Stale-bar sanity gate (OPENS ONLY — a stale exit still reduces
        # risk; refusing it would be worse than acting on it). A bar much
        # older than its interval means the feed stalled, the queue backed
        # up, or a reconnect flushed old data — opening at CURRENT market
        # off that bar is trading on garbage (the G3 outage class).
        if (work.bar_size_seconds > 0 and bar_age_seconds is not None
                and bar_age_seconds > stale_bar_multiple * work.bar_size_seconds):
            return Directive(
                'skip',
                f'stale_bar: bar age {bar_age_seconds:.0f}s > {stale_bar_multiple:g}x '
                f'bar_size ({work.bar_size_seconds:.0f}s) — refusing to open on stale data')
        if cooldown_active:
            return Directive('skip', 'cooldown active')
        return Directive('open', 'BUY while flat',
                         quantity=work.quantity if work.quantity > 0 else None)

    if work.action == Action.SELL:
        if held_qty <= 0:
            return Directive('skip', 'SELL while flat — long-only, no-op')
        # Closes are never blocked by cooldown: being unable to exit is
        # strictly more dangerous than exiting twice (dedup prevents that).
        return Directive('close', 'SELL while holding', quantity=held_qty)

    return Directive('skip', f'unsupported action {work.action}')


def check_time_exit(
    bar_ts: Any,
    bars_held: int,
    close_by_time: Optional[dt.time],
    max_hold_bars: Optional[int],
) -> Optional[str]:
    """Return a trigger reason, or None. Mirrors backtester.py:469-515:
    time-of-day comes from the *bar timestamp*, so live and backtest agree on
    timezone semantics no matter what tz the feed stamps bars with."""
    if max_hold_bars is not None and bars_held >= max_hold_bars:
        return f'max_hold_bars={max_hold_bars}'
    if close_by_time is not None:
        t = bar_ts.time() if hasattr(bar_ts, 'time') else None
        if t is not None and t >= close_by_time:
            return f'close_by_time={close_by_time}'
    return None


# ---------------------------------------------------------------------------
# Persistent state — position attribution + per-bar execution dedup.
# ---------------------------------------------------------------------------

class AutoExecState:
    """DuckDB-backed state. Lives in the trading DB next to the event and
    proposal stores; all access via the locked short-lived-connection API."""

    def __init__(self, duckdb_path: str):
        self.db = DuckDBConnection(duckdb_path)
        self._ensure_tables()

    def _ensure_tables(self):
        def _create(conn):
            conn.execute("""
                CREATE TABLE IF NOT EXISTS auto_exec_positions (
                    strategy VARCHAR NOT NULL,
                    conid BIGINT NOT NULL,
                    quantity DOUBLE NOT NULL,
                    entry_bar_ts TIMESTAMP,
                    entry_time TIMESTAMP NOT NULL,
                    proposal_id BIGINT,
                    close_by_time VARCHAR,
                    max_hold_bars BIGINT,
                    status VARCHAR NOT NULL,          -- OPEN / CLOSED / CLOSED_EXTERNALLY / UNKNOWN
                    closed_reason VARCHAR,
                    close_proposal_id BIGINT,
                    updated TIMESTAMP NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS auto_exec_bar_log (
                    strategy VARCHAR NOT NULL,
                    conid BIGINT NOT NULL,
                    bar_ts TIMESTAMP NOT NULL,
                    action VARCHAR NOT NULL,
                    decision VARCHAR NOT NULL,
                    reason VARCHAR,
                    created TIMESTAMP NOT NULL
                )
            """)
        self.db.execute_atomic(_create)

    # -- dedup ---------------------------------------------------------------

    def executed_for_bar(self, strategy: str, conid: int, bar_ts) -> bool:
        row = self.db.execute(
            "SELECT 1 FROM auto_exec_bar_log WHERE strategy=? AND conid=? AND bar_ts=? "
            "AND decision IN ('open','close') LIMIT 1",
            [strategy, conid, _naive(bar_ts)], fetch='one')
        return row is not None

    def log_decision(self, strategy: str, conid: int, bar_ts, action: str,
                     decision: str, reason: str):
        self.db.execute(
            "INSERT INTO auto_exec_bar_log VALUES (?, ?, ?, ?, ?, ?, ?)",
            [strategy, conid, _naive(bar_ts), action, decision, reason, dt.datetime.now()])

    # -- position attribution --------------------------------------------------

    def open_position(self, strategy: str, conid: int) -> Optional[dict]:
        row = self.db.execute(
            "SELECT quantity, entry_bar_ts, close_by_time, max_hold_bars, proposal_id "
            "FROM auto_exec_positions WHERE strategy=? AND conid=? AND status='OPEN' LIMIT 1",
            [strategy, conid], fetch='one')
        if row is None:
            return None
        return {
            'quantity': row[0],
            'entry_bar_ts': row[1],
            'close_by_time': dt.time.fromisoformat(row[2]) if row[2] else None,
            'max_hold_bars': row[3],
            'proposal_id': row[4],
        }

    def record_open(self, strategy: str, conid: int, quantity: float, bar_ts,
                    proposal_id: Optional[int],
                    close_by_time: Optional[dt.time], max_hold_bars: Optional[int]):
        self.db.execute(
            "INSERT INTO auto_exec_positions VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', NULL, NULL, ?)",
            [strategy, conid, quantity, _naive(bar_ts), dt.datetime.now(), proposal_id,
             close_by_time.isoformat() if close_by_time else None,
             max_hold_bars, dt.datetime.now()])

    def record_close(self, strategy: str, conid: int, status: str, reason: str,
                     close_proposal_id: Optional[int] = None):
        self.db.execute(
            "UPDATE auto_exec_positions SET status=?, closed_reason=?, close_proposal_id=?, updated=? "
            "WHERE strategy=? AND conid=? AND status='OPEN'",
            [status, reason, close_proposal_id, dt.datetime.now(), strategy, conid])

    def all_open(self) -> list:
        rows = self.db.execute(
            "SELECT strategy, conid, quantity, entry_bar_ts FROM auto_exec_positions "
            "WHERE status='OPEN'", fetch='all')
        return rows or []


def _naive(ts):
    """DuckDB TIMESTAMP is tz-naive; strip tzinfo consistently so dedup keys
    written and read compare equal regardless of the feed's tz stamping."""
    if ts is None:
        return None
    if hasattr(ts, 'tzinfo') and ts.tzinfo is not None:
        try:
            return ts.tz_localize(None)          # pandas Timestamp
        except AttributeError:
            return ts.replace(tzinfo=None)       # stdlib datetime
    return ts


# ---------------------------------------------------------------------------
# The executor.
# ---------------------------------------------------------------------------

class AutoExecutor:
    KILL_SWITCH_ENV = 'MMR_AUTO_EXECUTE_DISABLED'

    def __init__(
        self,
        duckdb_path: str,
        paper_trading: bool,
        event_store: Optional[EventStore] = None,
        cooldown_seconds: float = 300.0,
        sdk_factory=None,
    ):
        self.paper_trading = paper_trading
        self.cooldown_seconds = cooldown_seconds
        self.event_store = event_store
        self.state = AutoExecState(duckdb_path)
        # sdk_factory is injectable for tests; production default builds the
        # real SDK lazily ON THE WORKER THREAD (MMR.connect() calls
        # asyncio.run, which would blow up on the runtime's event loop).
        self._sdk_factory = sdk_factory or self._default_sdk_factory
        self._sdk = None
        self._queue: 'queue.Queue' = queue.Queue()
        self._last_exec: Dict[Tuple[str, int], float] = {}
        # Loop-side read model: (strategy, conid) -> entry_bar_ts for open
        # auto positions, so the dispatch loop can compute bars_held from the
        # frame without queue round-trips. Guarded by _view_lock.
        self._open_view: Dict[Tuple[str, int], Any] = {}
        self._view_lock = threading.Lock()
        self._reconciled = False
        self._worker = threading.Thread(target=self._run, name='auto-executor', daemon=True)
        self._started = False
        self._load_open_view()

    # -- lifecycle -------------------------------------------------------------

    def start(self):
        if not self._started:
            self._started = True
            self._worker.start()

    def stop(self):
        if self._started:
            self._queue.put(None)

    @staticmethod
    def _default_sdk_factory():
        from trader.sdk import MMR
        return MMR().connect()

    def _get_sdk(self):
        if self._sdk is None:
            self._sdk = self._sdk_factory()
        return self._sdk

    @property
    def kill_switch(self) -> bool:
        return os.environ.get(self.KILL_SWITCH_ENV, '') not in ('', '0', 'false', 'False')

    @property
    def stale_bar_multiple(self) -> float:
        """Bar-age multiple beyond which a BUY is refused (opens only).
        Env-tunable for coarse bar sizes; malformed values fall back to 3."""
        try:
            return float(os.environ.get('MMR_STALE_BAR_MULTIPLE', '') or 3.0)
        except ValueError:
            return 3.0

    # -- loop-side API (called from the runtime's event loop; never blocks) ----

    def submit_signal(self, work: SignalWork):
        self.start()
        self._queue.put(work)

    def submit_bar(self, strategy_name: str, conid: int, bar_ts, bars_held: int):
        self.start()
        self._queue.put(BarWork(strategy_name, conid, bar_ts, bars_held))

    def open_entry_bar(self, strategy_name: str, conid: int):
        """Entry bar_ts if this (strategy, conid) has an open auto position."""
        with self._view_lock:
            return self._open_view.get((strategy_name, conid))

    def open_count(self) -> int:
        """Number of executor-attributed OPEN positions (loop-side view).
        Used by the runtime pulse / runtime_status RPC."""
        with self._view_lock:
            return len(self._open_view)

    # -- worker ---------------------------------------------------------------

    def _run(self):
        while True:
            item = self._queue.get()
            if item is None:
                return
            try:
                self._reconcile_once()
                if isinstance(item, SignalWork):
                    self._process_signal(item)
                elif isinstance(item, BarWork):
                    self._process_bar(item)
            except Exception:
                logging.exception('auto-executor: error processing %s', item)

    def _load_open_view(self):
        view = {}
        for strategy, conid, _qty, entry_bar_ts in self.state.all_open():
            view[(strategy, int(conid))] = entry_bar_ts
        with self._view_lock:
            self._open_view = view

    def _reconcile_once(self):
        """First-work reconciliation: attributed OPEN positions that no longer
        exist at the broker were closed externally — mark them so a stale
        attribution can never trigger a spurious close order."""
        if self._reconciled:
            return
        open_rows = self.state.all_open()
        if not open_rows:
            self._reconciled = True
            return
        try:
            broker = self._broker_positions()
        except Exception as ex:
            logging.warning('auto-executor: reconcile skipped (broker unavailable: %s)', ex)
            return  # retry on next work item
        for strategy, conid, qty, _entry in open_rows:
            if broker.get(int(conid), 0.0) <= 0:
                logging.warning(
                    'auto-executor: %s conId %s attributed qty %s not at broker — '
                    'marking CLOSED_EXTERNALLY', strategy, conid, qty)
                self.state.record_close(strategy, int(conid), 'CLOSED_EXTERNALLY',
                                        'position absent at broker on reconcile')
        self._load_open_view()
        self._reconciled = True

    def _broker_positions(self) -> Dict[int, float]:
        df = self._get_sdk().positions()
        if df is None or df.empty:
            return {}
        return {int(r['conId']): float(r['position']) for _, r in df.iterrows()}

    def _resolve_exact(self, conid: int) -> dict:
        """conId → (symbol, exchange, currency, sec_type), refusing anything
        imprecise. The proposal pipeline keys on symbol+hints, so we also
        verify those hints resolve BACK to the same conId before trading."""
        sdk = self._get_sdk()
        defs = sdk.resolve(conid)
        if not defs:
            raise AutoExecutionError(
                f'conId {conid} not found in universe DB — refusing to trade a stale conId')
        d = defs[0]
        symbol = getattr(d, 'symbol', '') or ''
        exchange = (getattr(d, 'primaryExchange', '') or getattr(d, 'exchange', '') or '')
        currency = getattr(d, 'currency', '') or ''
        sec_type = getattr(d, 'secType', 'STK') or 'STK'
        back = sdk.resolve(symbol, sec_type=sec_type, exchange=exchange, currency=currency)
        back_ids = {int(getattr(b, 'conId', 0) or 0) for b in (back or [])}
        if conid not in back_ids:
            raise AutoExecutionError(
                f'precision check failed: {symbol} ({exchange}/{currency}) does not '
                f'round-trip to conId {conid} (got {sorted(back_ids)})')
        return {'symbol': symbol, 'exchange': exchange, 'currency': currency,
                'sec_type': sec_type}

    # -- signal processing ------------------------------------------------------

    def _process_signal(self, work: SignalWork):
        key = (work.strategy_name, work.conid)
        pos = self.state.open_position(work.strategy_name, work.conid)
        held_qty = pos['quantity'] if pos else 0.0
        now = dt.datetime.now().timestamp()
        cooldown_active = (now - self._last_exec.get(key, 0.0)) < self.cooldown_seconds

        directive = decide_signal(
            work,
            kill_switch=self.kill_switch,
            paper_trading=self.paper_trading,
            held_qty=held_qty,
            already_executed_bar=self.state.executed_for_bar(
                work.strategy_name, work.conid, work.bar_ts),
            cooldown_active=cooldown_active,
            bar_age_seconds=bar_age_seconds(work.bar_ts),
            stale_bar_multiple=self.stale_bar_multiple,
        )

        if directive.kind == 'skip':
            logging.info('auto-executor: %s conId %s %s — skip: %s',
                         work.strategy_name, work.conid, work.action, directive.reason)
            self.state.log_decision(work.strategy_name, work.conid, work.bar_ts,
                                    str(work.action), 'skip', directive.reason)
            return

        try:
            if directive.kind == 'open':
                self._execute_open(work, directive)
            elif directive.kind == 'close':
                self._execute_close(work.strategy_name, work.conid, work.bar_ts,
                                    float(directive.quantity or 0.0),
                                    reason='SELL signal')
        except AutoExecutionError as ex:
            # Deliberate refusal (stale conId, precision round-trip failure) —
            # no order was placed. Log the decision so the refusal is auditable.
            logging.error('auto-executor: refused %s conId %s: %s',
                          work.strategy_name, work.conid, ex)
            self.state.log_decision(work.strategy_name, work.conid, work.bar_ts,
                                    str(work.action), 'refused', str(ex))

    def _execute_open(self, work: SignalWork, directive: Directive):
        ident = self._resolve_exact(work.conid)
        sdk = self._get_sdk()
        confidence = max(0.0, min(1.0, work.probability))
        logging.warning(
            'auto-executor: OPENING %s (conId %s) for %s — confidence %.2f%s',
            ident['symbol'], work.conid, work.strategy_name, confidence,
            f', qty {directive.quantity}' if directive.quantity else ' (auto-sized)')

        proposal_id, _leverage, _snap = sdk.propose(
            symbol=ident['symbol'],
            action='BUY',
            quantity=directive.quantity,
            reasoning=f'auto-executed {work.strategy_name} BUY signal @ bar {work.bar_ts}',
            confidence=confidence,
            source=f'strategy:{work.strategy_name}',
            metadata={'auto_executed': True, 'strategy': work.strategy_name,
                      'conid': work.conid, 'bar_ts': str(work.bar_ts)},
            sec_type=ident['sec_type'],
            exchange=ident['exchange'],
            currency=ident['currency'],
        )
        result = sdk.approve(proposal_id)
        if not result.is_success():
            # Ambiguous timeouts leave the proposal APPROVED (not FAILED) —
            # surface loudly and record nothing as open: the morning reconcile
            # (or operator) resolves it against the broker.
            logging.error('auto-executor: OPEN failed for %s conId %s (proposal #%s): %s',
                          work.strategy_name, work.conid, proposal_id, result.error)
            self.state.log_decision(work.strategy_name, work.conid, work.bar_ts,
                                    'BUY', 'open_failed', str(result.error))
            self._append_event(EventType.ORDER_REJECTED, work.strategy_name, work.conid,
                              'BUY', 0.0, f'auto-execute open failed: {result.error}')
            return

        executed_qty = self._executed_quantity(sdk, proposal_id, result.obj or [])
        self.state.log_decision(work.strategy_name, work.conid, work.bar_ts,
                                'BUY', 'open', f'proposal #{proposal_id}')
        if executed_qty <= 0:
            # We placed an order but can't attribute a quantity — record as
            # UNKNOWN (not OPEN) so time-exits don't fire zero-quantity closes,
            # and shout: the operator/reconcile must resolve this against the
            # broker before the strategy trades this instrument again.
            logging.error(
                'auto-executor: OPENED %s for %s but could not determine executed '
                'quantity (proposal #%s, orders %s) — recorded status UNKNOWN, '
                'resolve manually', ident['symbol'], work.strategy_name,
                proposal_id, result.obj)
            self.state.record_open(
                work.strategy_name, work.conid, 0.0, work.bar_ts, proposal_id,
                work.close_by_time, work.max_hold_bars)
            self.state.record_close(work.strategy_name, work.conid, 'UNKNOWN',
                                    'executed quantity could not be attributed')
            self._last_exec[(work.strategy_name, work.conid)] = dt.datetime.now().timestamp()
            return
        self.state.record_open(
            work.strategy_name, work.conid, executed_qty, work.bar_ts, proposal_id,
            work.close_by_time, work.max_hold_bars)
        self._last_exec[(work.strategy_name, work.conid)] = dt.datetime.now().timestamp()
        self._load_open_view()
        logging.warning('auto-executor: OPENED %s x%s for %s (proposal #%s, orders %s)',
                        ident['symbol'], executed_qty, work.strategy_name, proposal_id,
                        result.obj)

    def _execute_close(self, strategy_name: str, conid: int, bar_ts,
                       attributed_qty: float, reason: str):
        ident = self._resolve_exact(conid)
        sdk = self._get_sdk()
        broker_qty = self._broker_positions().get(conid, 0.0)
        qty = min(attributed_qty, broker_qty)
        if qty <= 0:
            logging.warning(
                'auto-executor: close requested for %s conId %s but broker holds %s — '
                'marking CLOSED_EXTERNALLY', strategy_name, conid, broker_qty)
            self.state.record_close(strategy_name, conid, 'CLOSED_EXTERNALLY',
                                    f'broker qty {broker_qty} at close time ({reason})')
            self._load_open_view()
            return

        logging.warning('auto-executor: CLOSING %s x%s for %s — %s',
                        ident['symbol'], qty, strategy_name, reason)
        proposal_id, _leverage, _snap = sdk.propose(
            symbol=ident['symbol'],
            action='SELL',
            quantity=qty,
            reasoning=f'auto-executed close for {strategy_name}: {reason}',
            confidence=1.0,
            source=f'strategy:{strategy_name}',
            metadata={'auto_executed': True, 'strategy': strategy_name,
                      'conid': conid, 'bar_ts': str(bar_ts), 'close_reason': reason},
            sec_type=ident['sec_type'],
            exchange=ident['exchange'],
            currency=ident['currency'],
        )
        result = sdk.approve(proposal_id)
        if not result.is_success():
            logging.error('auto-executor: CLOSE FAILED for %s conId %s (proposal #%s): %s '
                          '— position remains OPEN, will retry on next trigger',
                          strategy_name, conid, proposal_id, result.error)
            self.state.log_decision(strategy_name, conid, bar_ts,
                                    'SELL', 'close_failed', str(result.error))
            self._append_event(EventType.ORDER_REJECTED, strategy_name, conid,
                              'SELL', qty, f'auto-execute close failed: {result.error}')
            return

        self.state.log_decision(strategy_name, conid, bar_ts,
                                'SELL', 'close', f'proposal #{proposal_id} ({reason})')
        self.state.record_close(strategy_name, conid, 'CLOSED', reason, proposal_id)
        self._last_exec[(strategy_name, conid)] = dt.datetime.now().timestamp()
        self._load_open_view()
        logging.warning('auto-executor: CLOSED %s x%s for %s (proposal #%s)',
                        ident['symbol'], qty, strategy_name, proposal_id)

    def _process_bar(self, work: BarWork):
        pos = self.state.open_position(work.strategy_name, work.conid)
        if not pos:
            return
        trigger = check_time_exit(
            work.bar_ts, work.bars_held, pos['close_by_time'], pos['max_hold_bars'])
        if trigger is None:
            return
        # Dedup guards the double-fire: the close logs a decision for this
        # bar_ts, and executed_for_bar() blocks a second close on the same bar.
        if self.state.executed_for_bar(work.strategy_name, work.conid, work.bar_ts):
            return
        try:
            self._execute_close(work.strategy_name, work.conid, work.bar_ts,
                                float(pos['quantity']), reason=f'time exit: {trigger}')
        except AutoExecutionError as ex:
            logging.error('auto-executor: time-exit close refused for %s conId %s: %s',
                          work.strategy_name, work.conid, ex)
            self.state.log_decision(work.strategy_name, work.conid, work.bar_ts,
                                    'SELL', 'refused', str(ex))

    # -- helpers ----------------------------------------------------------------

    def _executed_quantity(self, sdk, proposal_id: int, order_ids: list) -> float:
        """Quantity we can attribute to this open.

        Auto-sized proposals persist an *amount*; the concrete quantity is
        computed inside approve() and lands only on the placed order. The
        order's totalQuantity is known the moment it is placed (no fill-wait),
        so we read it from the book via the order_ids approve returned. A
        partial fill would over-attribute, which is safe: every close clamps
        to the live broker position.
        """
        try:
            p = sdk._proposal_store().get(proposal_id)
            if p is not None and p.quantity:
                return float(p.quantity)
        except Exception:
            logging.exception('auto-executor: could not read proposal #%s', proposal_id)
        if order_ids:
            try:
                df = sdk.trades()
                if df is not None and not df.empty:
                    mine = df[df['orderId'].isin(list(order_ids))]
                    if not mine.empty:
                        return float(mine['totalQuantity'].sum())
            except Exception:
                logging.exception('auto-executor: trades lookup failed for orders %s', order_ids)
        return 0.0

    def _append_event(self, event_type: EventType, strategy: str, conid: int,
                      action: str, quantity: float, note: str):
        if self.event_store is None:
            return
        try:
            self.event_store.append(TradingEvent(
                event_type=event_type, timestamp=dt.datetime.now(),
                strategy_name=strategy, conid=conid, action=action,
                quantity=quantity, metadata={'note': note}))
        except Exception:
            logging.exception('auto-executor: event append failed')
