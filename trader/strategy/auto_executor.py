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
import hashlib
import os
import math
import sys
import time
import uuid
import queue
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

from trader.common.logging_helper import setup_logging
from trader.data.duckdb_store import DuckDBConnection
from trader.data.event_store import EventStore, EventType, TradingEvent
from trader.strategy.execution_intents import IntentStore, TERMINAL, merge_exit_scopes, timestamp_text
from trader.strategy.execution_queue import ExecutionWorkQueue
from trader.objects import Action
from trader.trading.order_math import reducible_quantity
from trader.trading.protective_stop import protective_stop_plan
from trader.trading.order_reference import split_order_reference

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
    # REQUIRED, deliberately: this is the stale-bar gate's denominator, and it
    # used to default to 0.0, which the gate read as "interval unknown" and
    # silently declined to run. A safety-critical input with a default that
    # disables the check is a trap for the next caller — the failure is at the
    # construction site, not in the gate, so it is removed here rather than
    # handled downstream. Every construction site passes keywords, so ordering
    # this before the defaulted fields breaks nothing.
    bar_size_seconds: float
    probability: float = 0.0
    risk: float = 0.0
    quantity: float = 0.0             # >0 = strategy-specified size (BUY only)
    trade_amount: float = 0.0         # >0 = fixed per-open $ notional (0 = auto-size)
    auto_execute: bool = False
    paper_only: bool = False
    state_running: bool = True
    close_by_time: Optional[dt.time] = None
    max_hold_bars: Optional[int] = None
    # Bounded pyramiding (validated 2026-07-22: the pyramid structure is the
    # statistically-real part of ORB's edge — see OPERATIONAL_STATE.md).
    # 0 = single-lot (default, historical behaviour); N = allow N adds after
    # the initial entry, so a stack tops out at N+1 fixed-size lots. Set per
    # strategy via ``pyramid_max_adds`` in strategy_runtime.yaml.
    pyramid_max_adds: int = 0
    deployment_generation: str = ''
    # Strategy manifest primitives (see StrategyContext / check_manifest).
    # All None => no declared envelope => unchecked (backward-compatible).
    # These are enforced OPENS ONLY, in _process_signal's open branch, so a
    # declared envelope can never block a close, skip, or time-exit.
    manifest_allowed_conids: Optional[list] = None
    manifest_direction: Optional[str] = None
    manifest_max_opens_per_day: Optional[int] = None
    manifest_max_opens_per_hour: Optional[int] = None


def bar_age_seconds(bar_ts, now_utc: Optional[dt.datetime] = None) -> Optional[float]:
    """Seconds between a bar timestamp and now. Naive timestamps are UTC wall
    time (the runtime's frames are UTC; ``_naive`` strips tz without
    converting). Unparseable or nonfinite ages return None so opening
    freshness fails closed; finite negative ages retain timestamp semantics."""
    if bar_ts is None:
        return None
    try:
        now_utc = now_utc or dt.datetime.now(dt.timezone.utc)
        ts = bar_ts
        if hasattr(ts, 'to_pydatetime'):
            ts = ts.to_pydatetime()
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=dt.timezone.utc)
        age = (now_utc - ts).total_seconds()
        return age if math.isfinite(age) else None
    except Exception:
        return None


@dataclass
class BarWork:
    """A new-bar notification for time-exit evaluation."""
    strategy_name: str
    conid: int
    bar_ts: Any
    bars_held: int
    entry_bar_ts: Any = None
    observed_bar_timestamps: Optional[tuple[Any, ...]] = None
    observed_progress: Optional[tuple[str, int]] = None


@dataclass
class ManagementWork:
    """Independent broker reconciliation; survives inactive strategy code."""


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
    live_armed: bool = False,
    held_lots: int = 0,
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
        # Disarming must not strand attributed positions — same principle as
        # the live double-arm ("closes are never gated"). A disarmed strategy
        # may still CLOSE what the executor opened for it; it may not open.
        if not (work.action == Action.SELL and held_qty > 0):
            return Directive('skip', 'auto_execute is false')
    if not work.state_running:
        return Directive('skip', 'strategy is not RUNNING')
    if work.paper_only and not paper_trading:
        return Directive('skip', 'paper_only strategy in live trading mode')
    if already_executed_bar:
        return Directive('skip', f'already executed for bar {work.bar_ts}')

    if work.action == Action.BUY:
        # Live double-arm (horserank's L2 analog): real-money auto-execution
        # needs BOTH the live trading mode AND an explicit, strict-'1' arming
        # knob — a single `--live` flag flip must never arm the whole book by
        # itself. Code default is DISARMED (`live_armed=False`); the un-armed
        # first live session records exactly what WOULD have traded via these
        # skip rows. OPENS ONLY — closes below are never gated, so disarming
        # with live positions open can't strand them unmanaged.
        if not paper_trading and not live_armed:
            return Directive(
                'skip',
                'live auto-execute not armed (set MMR_AUTO_EXECUTE_LIVE=1) — '
                'opens refused, closes unaffected')
        if held_qty > 0:
            # Bounded pyramiding: strategies with ``pyramid_max_adds > 0``
            # may add fixed-size lots until the stack holds 1 + max_adds.
            # Everything below (stale-bar, cooldown) applies to adds too.
            if work.pyramid_max_adds <= 0:
                return Directive('skip', 'already holding — no pyramiding')
            if held_lots > work.pyramid_max_adds:
                return Directive(
                    'skip',
                    f'pyramid stack full ({held_lots} lots, '
                    f'max_adds={work.pyramid_max_adds})')
        # Stale-bar sanity gate (OPENS ONLY — a stale exit still reduces
        # risk; refusing it would be worse than acting on it). A bar much
        # older than its interval means the feed stalled, the queue backed
        # up, or a reconnect flushed old data — opening at CURRENT market
        # off that bar is trading on garbage (the G3 outage class).
        # An undatable bar means the gate cannot answer its own question, and
        # every other gate input in this system refuses an open it cannot
        # evaluate (daily PnL, NetLiquidation, position value, and the leverage
        # check since 2026-07-26). This one used to be the exception and let
        # the open through on a bar of UNKNOWN age, which is the state it
        # exists to detect. Refusing is the benign direction here: this branch
        # is OPENS ONLY, so the worst case is no new positions, while existing
        # ones still exit and the broker-side stops are untouched.
        if bar_age_seconds is None or not math.isfinite(bar_age_seconds):
            return Directive(
                'skip',
                'stale_bar: bar timestamp not datable — cannot verify bar '
                'freshness, refusing to open (fail-closed)')
        if not math.isfinite(work.bar_size_seconds) or work.bar_size_seconds <= 0:
            return Directive(
                'skip',
                'stale_bar: bar interval must be positive and finite — '
                'cannot verify freshness, refusing to open (fail-closed)')
        if not math.isfinite(stale_bar_multiple) or stale_bar_multiple <= 0:
            return Directive(
                'skip',
                'stale_bar: age multiplier must be positive and finite — '
                'cannot verify freshness, refusing to open (fail-closed)')
        maximum_age = stale_bar_multiple * work.bar_size_seconds
        if not math.isfinite(maximum_age):
            return Directive(
                'skip',
                'stale_bar: age threshold is not finite — cannot verify '
                'freshness, refusing to open (fail-closed)')
        if bar_age_seconds > maximum_age:
            return Directive(
                'skip',
                f'stale_bar: bar age {bar_age_seconds:.0f}s > {stale_bar_multiple:g}x '
                f'bar_size ({work.bar_size_seconds:.0f}s) — refusing to open on stale data')
        if cooldown_active:
            return Directive('skip', 'cooldown active')
        reason = ('BUY while flat' if held_qty <= 0
                  else f'pyramid add (lot {held_lots + 1})')
        return Directive('open', reason,
                         quantity=work.quantity if work.quantity > 0 else None)

    if work.action == Action.SELL:
        if held_qty <= 0:
            return Directive('skip', 'SELL while flat — long-only, no-op')
        # Closes are never blocked by cooldown: being unable to exit is
        # strictly more dangerous than exiting twice (dedup prevents that).
        return Directive('close', 'SELL while holding', quantity=held_qty)

    return Directive('skip', f'unsupported action {work.action}')


def accept_empty_broker_read(
    first_empty_at: Optional[float],
    now: float,
    grace_seconds: float,
) -> bool:
    """May "the broker reports NO positions at all" be believed?

    Reconciliation marks every attributed position that is absent at the broker
    as CLOSED_EXTERNALLY and cancels its protective stop. That is right when a
    position really was closed while we were down. It is catastrophic when the
    broker read is merely EMPTY-because-early: real positions lose their
    attribution (so no strategy will ever close them) and lose their disaster
    stop (so nothing protects them), permanently — nothing re-attributes.

    That window is not theoretical. ``get_positions`` falls back to MMR's own
    portfolio cache when ``ib.positions()`` is empty, and both are empty for the
    first moments after trader_service connects — which is exactly when
    strategy_service starts pushing bars and the executor does its first-work
    reconcile.

    So an empty read is INCONCLUSIVE on first sight and is believed only if it
    is still empty ``grace_seconds`` later. A genuinely flat book converges
    (the second read agrees); a startup race does not (positions arrive). The
    grace period is bounded rather than unlimited because a stale attribution
    blocks new opens — refusing to ever reconcile would trade one silent
    failure for another.
    """
    if first_empty_at is None:
        return False
    try:
        elapsed = float(now) - float(first_empty_at)
    except (TypeError, ValueError):
        return False
    return elapsed >= float(grace_seconds)


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


def check_manifest(
    work: SignalWork,
    directive: Directive,
    opens_today: int,
    opens_hour: int,
) -> Optional[Directive]:
    """Manifest-envelope gate. **Pure** — turnover counts are passed in, no I/O.

    Returns ``None`` to allow the directive, or a ``Directive('refused', ...)``
    to refuse it. The caller invokes this ONLY for ``directive.kind == 'open'``
    (``_process_signal``'s open branch), so closes, skips, and the time-exit
    path never reach it — a declared envelope is structurally incapable of
    blocking an exit. Any manifest field left ``None`` is unchecked, so a
    strategy with no manifest is byte-identical to today.

    Rules (opens only):
      * **Universe** — the executor trades ``work.conid`` (the subscribed
        dispatch conId). If ``allowed_conids`` is declared and does not include
        it, the instrument is watched but not tradeable ⇒ refuse.
      * **Direction** — a ``'long'`` declaration on the long-only executor.
        Today an ``'open'`` directive is always a long (BUY) open —
        ``decide_signal`` has no short-open path — so this branch is
        unreachable now; it exists to catch a FUTURE short-path regression
        (an ``'open'`` synthesised from a SELL) under a ``'long'`` declaration.
      * **Turnover** — ``opens_today`` / ``opens_hour`` are the counts of PAST
        exposure-increasing orders (opens + pyramid adds, never closes) in the
        rolling windows; the cap binds when the count already reached the
        declared limit.
    """
    allowed = work.manifest_allowed_conids
    if allowed is not None and work.conid not in allowed:
        return Directive(
            'refused',
            f'manifest: conId {work.conid} not in allowed_conids {sorted(allowed)}')

    if (work.manifest_direction == 'long'
            and directive.kind == 'open' and work.action == Action.SELL):
        return Directive(
            'refused',
            'manifest: direction=long but this open is a short-open (SELL) — '
            'refusing (long-only invariant; a short path should never reach here)')

    if (work.manifest_max_opens_per_day is not None
            and opens_today >= work.manifest_max_opens_per_day):
        return Directive(
            'refused',
            f'manifest: max_opening_orders_per_day {work.manifest_max_opens_per_day} '
            f'reached ({opens_today} opens in last 24h)')

    if (work.manifest_max_opens_per_hour is not None
            and opens_hour >= work.manifest_max_opens_per_hour):
        return Directive(
            'refused',
            f'manifest: max_opening_orders_per_hour {work.manifest_max_opens_per_hour} '
            f'reached ({opens_hour} opens in last hour)')

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
            # Existing logs have no provable intent identity. Keep them
            # unkeyed; never infer an order from display text or a bar.
            conn.execute("ALTER TABLE auto_exec_bar_log ADD COLUMN IF NOT EXISTS intent_id VARCHAR")
            # Migration for pre-protective-stop databases.
            conn.execute("""
                ALTER TABLE auto_exec_positions
                ADD COLUMN IF NOT EXISTS protective_order_id BIGINT
            """)
            # Migration for pre-pyramiding databases (NULL = 1 lot).
            conn.execute("""
                ALTER TABLE auto_exec_positions
                ADD COLUMN IF NOT EXISTS lots BIGINT
            """)
            # A bar label can be reused and changes on pyramid adds. These
            # nullable fields identify the actual owned holding instead;
            # migrated rows intentionally have no invented creation proof.
            conn.execute("ALTER TABLE auto_exec_positions ADD COLUMN IF NOT EXISTS ownership_epoch VARCHAR")
            conn.execute("ALTER TABLE auto_exec_positions ADD COLUMN IF NOT EXISTS ownership_started_at DOUBLE")
            conn.execute("CREATE TABLE IF NOT EXISTS auto_exec_fill_progress "
                         "(intent_id VARCHAR PRIMARY KEY, quantity DOUBLE NOT NULL)")
            # Cost follows the exact inventory deltas and holding lifetime.
            # NULL is unknown price evidence, never zero-cost inventory. Old
            # holdings have no invented history when this table is introduced.
            conn.execute("CREATE TABLE IF NOT EXISTS auto_exec_cost_events ("
                         "sequence BIGINT PRIMARY KEY, ownership_epoch VARCHAR NOT NULL, "
                         "intent_id VARCHAR NOT NULL, start_quantity DOUBLE NOT NULL, "
                         "end_quantity DOUBLE NOT NULL, attributed_delta DOUBLE NOT NULL, "
                         "cumulative_quote_notional DOUBLE, price_conflict BOOLEAN NOT NULL, "
                         "UNIQUE(intent_id, end_quantity))")
            conn.execute('CREATE INDEX IF NOT EXISTS auto_exec_cost_epoch '
                         'ON auto_exec_cost_events(ownership_epoch)')
        self.db.execute_atomic(_create)

    # -- dedup ---------------------------------------------------------------

    def executed_for_bar(self, strategy: str, conid: int, bar_ts) -> bool:
        row = self.db.execute(
            "SELECT 1 FROM auto_exec_bar_log WHERE strategy=? AND conid=? AND bar_ts=? "
            "AND decision IN ('open','close') LIMIT 1",
            [strategy, conid, _naive(bar_ts)], fetch='one')
        return row is not None

    def log_decision(self, strategy: str, conid: int, bar_ts, action: str,
                     decision: str, reason: str, *, intent_id: Optional[str] = None):
        self.db.execute(
            "INSERT INTO auto_exec_bar_log "
            "(strategy, conid, bar_ts, action, decision, reason, created, intent_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [strategy, conid, _naive(bar_ts), action, decision, reason,
             dt.datetime.now(), intent_id])

    def count_opens_since(self, strategy: str, since: dt.datetime, *,
                          submitted_intent_ids: Optional[set[str]] = None) -> int:
        """Count distinct opening attempts across durable logs and intents.

        A delayed acknowledgement can leave only its log inside the window;
        a lost reply can leave only a submitted intent. Their union counts
        both without charging one acknowledged order twice. Repeated logs
        for the same intent also count once. Legacy logs have no exact ID:
        retain each as a separate count until it expires, even if that can
        temporarily overcount an overlapping legacy intent. Never guess the
        identity from a reason string, symbol or bar timestamp.
        """
        rows = self.db.execute(
            "SELECT intent_id, COUNT(*) FROM auto_exec_bar_log "
            "WHERE strategy=? AND decision='open' AND created >= ? "
            "GROUP BY intent_id",
            [strategy, since], fetch='all')
        identified = set(submitted_intent_ids or ())
        legacy_count = 0
        for intent_id, count in rows:
            if intent_id is None:
                legacy_count += int(count)
            else:
                identified.add(intent_id)
        return legacy_count + len(identified)

    # -- position attribution --------------------------------------------------

    def open_position(self, strategy: str, conid: int) -> Optional[dict]:
        def _read(conn):
            row = conn.execute(
                "SELECT quantity, entry_bar_ts, close_by_time, max_hold_bars, proposal_id, "
                "protective_order_id, lots, ownership_epoch, ownership_started_at "
                "FROM auto_exec_positions WHERE strategy=? AND conid=? AND status='OPEN' LIMIT 1",
                [strategy, conid]).fetchone()
            if row is None:
                return None
            average, reason = self._owned_average_cost(conn, row[7], float(row[0]))
            return {
                'quantity': row[0],
                'entry_bar_ts': row[1],
                'close_by_time': dt.time.fromisoformat(row[2]) if row[2] else None,
                'max_hold_bars': row[3],
                'proposal_id': row[4],
                'protective_order_id': row[5],
                'lots': int(row[6]) if row[6] else 1,  # pre-migration rows = 1 lot
                'ownership_epoch': row[7],
                'ownership_started_at': row[8],
                'avg_cost': average,
                'cost_evaluable': average is not None,
                'cost_unavailable_reason': reason,
            }
        return self.db.execute_atomic(_read)

    @staticmethod
    def _owned_average_cost(conn, epoch, expected_quantity):
        """Replay this holding's weighted cost, retaining unpriced intervals.

        Reductions remove proportional cost. A later proven endpoint can
        therefore recover an earlier interval even after reductions. An
        unknown intermediate endpoint cannot be distributed across different
        fills by guessing a uniform price.
        """
        rows = conn.execute(
            'SELECT e.start_quantity, e.attributed_delta, e.cumulative_quote_notional, '
            'e.price_conflict, p.cumulative_quote_notional, p.price_conflict '
            'FROM auto_exec_cost_events e LEFT JOIN auto_exec_cost_events p '
            'ON p.intent_id=e.intent_id AND p.end_quantity=e.start_quantity '
            'AND p.attributed_delta>0 WHERE e.ownership_epoch=? ORDER BY e.sequence',
            [epoch]).fetchall()
        quantity, notional, evaluable = 0.0, 0.0, True
        for start, delta, end_notional, conflict, previous_notional, previous_conflict in rows:
            if delta > 0:
                if start == 0:
                    previous_notional, previous_conflict = 0.0, False
                increment = None
                if (not conflict and end_notional is not None
                        and previous_notional is not None and not previous_conflict):
                    increment = end_notional - previous_notional
                    if not math.isfinite(increment) or increment <= 0:
                        increment = None
                quantity += delta
                if increment is None:
                    evaluable = False
                else:
                    notional += increment
            else:
                remaining = quantity + delta
                if quantity <= 0 or remaining < 0:
                    return None, 'owned fills have no complete cost history'
                notional *= remaining / quantity
                quantity = remaining
                if quantity == 0:
                    notional, evaluable = 0.0, True
        if not rows or quantity != expected_quantity:
            return None, 'owned fills have no complete cost history'
        if not evaluable or not math.isfinite(notional) or notional <= 0 or quantity <= 0:
            return None, 'owned fill price is unavailable or contradictory'
        average = notional / quantity
        if not math.isfinite(average) or average <= 0:
            return None, 'owned fill cost is not evaluable'
        return average, None

    @staticmethod
    def _record_cost_evidence(conn, intent_id, cumulative, quote_notional):
        if quote_notional is None:
            return
        row = conn.execute(
            'SELECT cumulative_quote_notional, price_conflict FROM auto_exec_cost_events '
            'WHERE intent_id=? AND end_quantity=? AND attributed_delta>0',
            [intent_id, cumulative]).fetchone()
        if row is None or row[1]:
            return
        if row[0] is None:
            conn.execute('UPDATE auto_exec_cost_events SET cumulative_quote_notional=? '
                         'WHERE intent_id=? AND end_quantity=?',
                         [quote_notional, intent_id, cumulative])
        elif row[0] != quote_notional:
            # Conflicting prices at the same cumulative checkpoint have no
            # revision ordering proof. Do not silently revalue the holding.
            conn.execute('UPDATE auto_exec_cost_events SET price_conflict=TRUE '
                         'WHERE intent_id=? AND end_quantity=?', [intent_id, cumulative])

    def unpriced_open_intents(self, strategy=None, conid=None) -> set[str]:
        """Only a current cumulative endpoint can be recovered by a replay.

        Earlier missing endpoints and migrated holdings remain explicitly
        unknown; a present cumulative average cannot invent their fill split.
        """
        query = (
            'SELECT DISTINCT e.intent_id FROM auto_exec_cost_events e '
            'JOIN auto_exec_positions p ON p.ownership_epoch=e.ownership_epoch '
            'JOIN auto_exec_fill_progress f ON f.intent_id=e.intent_id '
            "WHERE p.status='OPEN' AND e.attributed_delta>0 "
            'AND e.end_quantity=f.quantity AND e.cumulative_quote_notional IS NULL '
            'AND NOT e.price_conflict')
        params = []
        if strategy is not None:
            query += ' AND p.strategy=?'
            params.append(strategy)
        if conid is not None:
            query += ' AND p.conid=?'
            params.append(conid)
        rows = self.db.execute(query, params, fetch='all')
        return {row[0] for row in rows}

    def set_protective(self, strategy: str, conid: int, order_id: Optional[int]):
        self.db.execute(
            "UPDATE auto_exec_positions SET protective_order_id=?, updated=? "
            "WHERE strategy=? AND conid=? AND status='OPEN'",
            [order_id, dt.datetime.now(), strategy, conid])

    def record_open(self, strategy: str, conid: int, quantity: float, bar_ts,
                    proposal_id: Optional[int],
                    close_by_time: Optional[dt.time], max_hold_bars: Optional[int]):
        self.db.execute(
            "INSERT INTO auto_exec_positions "
            "(strategy, conid, quantity, entry_bar_ts, entry_time, proposal_id, "
            " close_by_time, max_hold_bars, status, closed_reason, close_proposal_id, "
            " updated, lots, ownership_epoch, ownership_started_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', NULL, NULL, ?, 1, ?, ?)",
            [strategy, conid, quantity, _naive(bar_ts), dt.datetime.now(), proposal_id,
             close_by_time.isoformat() if close_by_time else None,
             max_hold_bars, dt.datetime.now(), uuid.uuid4().hex, time.time()])

    def adopt_legacy_ownership(self, strategy: str, conid: int, quantity: float,
                               avg_cost: float, attestation: str) -> dict:
        """Assign an ownership epoch and cost basis to a pre-epoch holding.

        Migration deliberately invents neither (see ``_ensure_tables``); this is
        the explicit, operator-attested act the execution contract calls for.
        Without it a holding attributed by the previous build can never be
        closed, emergency-closed or re-protected by the executor. The basis is
        recorded as one priced opening interval so the ordinary cost replay
        values the holding, with the attestation kept as that event's intent
        id: the audit shows the basis was declared, not observed.
        """
        if not math.isfinite(quantity) or quantity <= 0:
            raise ValueError('legacy adoption requires the attributed positive quantity')
        if not math.isfinite(avg_cost) or avg_cost <= 0:
            raise ValueError('legacy adoption requires a finite positive average cost')
        epoch = uuid.uuid4().hex
        started = time.time()

        def _adopt(conn):
            row = conn.execute(
                "SELECT quantity, ownership_epoch FROM auto_exec_positions "
                "WHERE strategy=? AND conid=? AND status='OPEN' LIMIT 1", [strategy, conid]).fetchone()
            if row is None:
                raise ValueError(f'{strategy}/{conid} has no attributed OPEN holding')
            if row[1] is not None:
                raise ValueError(f'{strategy}/{conid} already has ownership epoch {row[1]}')
            if float(row[0]) != quantity:
                raise ValueError(f'attributed quantity is {float(row[0]):g}, not {quantity:g}; '
                                 're-read the holding before attesting')
            conn.execute(
                "UPDATE auto_exec_positions SET ownership_epoch=?, ownership_started_at=?, updated=? "
                "WHERE strategy=? AND conid=? AND status='OPEN'",
                [epoch, started, dt.datetime.now(), strategy, conid])
            sequence = conn.execute(
                'SELECT COALESCE(MAX(sequence), 0) + 1 FROM auto_exec_cost_events').fetchone()[0]
            conn.execute('INSERT INTO auto_exec_cost_events VALUES (?, ?, ?, ?, ?, ?, ?, FALSE)',
                         [sequence, epoch, attestation, 0.0, quantity, quantity, avg_cost * quantity])
            return {'strategy': strategy, 'conid': conid, 'quantity': quantity, 'avg_cost': avg_cost,
                    'ownership_epoch': epoch, 'ownership_started_at': started}
        return self.db.execute_atomic(_adopt)

    def record_add(self, strategy: str, conid: int, quantity_delta: float, bar_ts,
                   proposal_id: Optional[int],
                   close_by_time: Optional[dt.time], max_hold_bars: Optional[int]):
        """Fold a pyramid add into the open row. Latest-BUY-wins for the
        time-exit rules and entry_bar_ts (bars_held restarts at the add) —
        the same semantics the backtester applies to stacked entries.
        ``proposal_id`` tracks the latest add; per-lot audit detail lives in
        the proposal store and event store."""
        self.db.execute(
            "UPDATE auto_exec_positions SET quantity = quantity + ?, "
            "lots = COALESCE(lots, 1) + 1, entry_bar_ts = ?, proposal_id = ?, "
            "close_by_time = ?, max_hold_bars = ?, updated = ? "
            "WHERE strategy=? AND conid=? AND status='OPEN'",
            [quantity_delta, _naive(bar_ts), proposal_id,
             close_by_time.isoformat() if close_by_time else None,
             max_hold_bars, dt.datetime.now(), strategy, conid])

    def record_close(self, strategy: str, conid: int, status: str, reason: str,
                     close_proposal_id: Optional[int] = None):
        self.db.execute(
            "UPDATE auto_exec_positions SET status=?, closed_reason=?, close_proposal_id=?, updated=? "
            "WHERE strategy=? AND conid=? AND status='OPEN'",
            [status, reason, close_proposal_id, dt.datetime.now(), strategy, conid])

    def all_open(self) -> list:
        rows = self.db.execute(
            "SELECT strategy, conid, quantity, entry_bar_ts, protective_order_id "
            "FROM auto_exec_positions WHERE status='OPEN'", fetch='all')
        return rows or []

    def ownership_snapshot(self, intents: list[dict]) -> tuple[list[dict], dict[str, float]]:
        """Read owned inventory and its applied fill progress in one transaction.

        Intent acknowledgments live in a separate journal and may lag an
        inventory commit or describe only part of a broker replay. Emergency
        sizing needs the checkpoints committed with this exact inventory.
        Only intents for owners with an open position need checkpoint reads.
        """
        def _read(conn):
            rows = conn.execute(
                "SELECT strategy, conid, quantity, entry_bar_ts, protective_order_id, "
                "close_by_time, max_hold_bars, proposal_id, ownership_epoch, ownership_started_at "
                "FROM auto_exec_positions WHERE status='OPEN'").fetchall()
            positions = [dict(strategy=row[0], conid=int(row[1]), quantity=float(row[2]),
                              entry_bar_ts=row[3], protective_order_id=row[4],
                              close_by_time=dt.time.fromisoformat(row[5]) if row[5] else None,
                              max_hold_bars=row[6], proposal_id=row[7],
                              ownership_epoch=row[8], ownership_started_at=row[9]) for row in rows]
            owners = {(row['strategy'], row['conid']) for row in positions}
            ids = [intent['intent_id'] for intent in intents
                   if (intent['strategy'], intent['conid']) in owners]
            checkpoints = {}
            if ids:
                markers = ','.join('?' for _ in ids)
                checkpoints = dict(conn.execute(
                    f'SELECT intent_id, quantity FROM auto_exec_fill_progress WHERE intent_id IN ({markers})',
                    ids).fetchall())
            return positions, checkpoints
        return self.db.execute_atomic(_read)

    def apply_fill(self, intent: dict, cumulative: float, *, cumulative_quote_notional=None):
        """Atomically apply only the unseen execution delta to owned inventory.

        The checkpoint and position update share a transaction. A crash before
        or after it can replay the same broker observation without duplicating
        attribution; requested quantity never enters this calculation.
        """
        payload = intent['payload']
        try:
            quote_notional = (None if cumulative_quote_notional is None
                              else float(cumulative_quote_notional))
        except (TypeError, ValueError, OverflowError):
            quote_notional = None
        if quote_notional is not None and (not math.isfinite(quote_notional) or quote_notional <= 0):
            quote_notional = None
        reducing = intent['kind'] != 'OPEN'
        if reducing and (payload.get('attribution_unresolved')
                         or payload.get('ownership_epoch') is None):
            return 0.0  # do not consume evidence until its ownership is proven
        strategy, conid = intent['strategy'], intent['conid']
        def _apply(conn):
            checkpoint = conn.execute(
                'SELECT quantity FROM auto_exec_fill_progress WHERE intent_id=?',
                [intent['intent_id']]).fetchone()
            applied = float(checkpoint[0]) if checkpoint else 0.0
            delta = max(0.0, cumulative - applied)
            if delta <= 0:
                if not reducing and cumulative == applied:
                    self._record_cost_evidence(conn, intent['intent_id'], cumulative, quote_notional)
                return 0.0
            position = conn.execute(
                "SELECT quantity, ownership_epoch FROM auto_exec_positions "
                "WHERE strategy=? AND conid=? AND status='OPEN' LIMIT 1",
                [strategy, conid]).fetchone()
            now = dt.datetime.now()
            cost_epoch, cost_delta = None, 0.0
            if intent['kind'] == 'OPEN':
                bar = _naive(dt.datetime.fromisoformat(payload['bar_ts']))
                if position is None:
                    cost_epoch = uuid.uuid4().hex
                    conn.execute(
                        "INSERT INTO auto_exec_positions "
                        "(strategy,conid,quantity,entry_bar_ts,entry_time,proposal_id,close_by_time,"
                        "max_hold_bars,status,updated,lots,ownership_epoch,ownership_started_at) "
                        "VALUES (?,?,?,?,?,?,?,?,'OPEN',?,1,?,?)",
                        [strategy, conid, delta, bar, now, payload.get('proposal_id'),
                         payload.get('close_by_time'), payload.get('max_hold_bars'), now,
                         cost_epoch, payload.get('intent_created_at')])
                else:
                    cost_epoch = position[1]
                    conn.execute(
                        "UPDATE auto_exec_positions SET quantity=quantity+?, "
                        "lots=COALESCE(lots,1)+?, entry_bar_ts=?, proposal_id=?, close_by_time=?, max_hold_bars=?, updated=? "
                        "WHERE strategy=? AND conid=? AND status='OPEN'",
                        [delta, 1 if applied == 0 else 0, bar, payload.get('proposal_id'),
                         payload.get('close_by_time'), payload.get('max_hold_bars'), now, strategy, conid])
                cost_delta = delta
            elif position is not None and (not reducing or (
                    payload.get('ownership_epoch') is not None
                    and payload['ownership_epoch'] == position[1])):
                remaining = max(0.0, float(position[0]) - delta)
                cost_epoch = position[1]
                cost_delta = remaining - float(position[0])
                conn.execute(
                    "UPDATE auto_exec_positions SET quantity=?, status=?, closed_reason=?, close_proposal_id=?, updated=? "
                    "WHERE strategy=? AND conid=? AND status='OPEN'",
                    [remaining, 'CLOSED' if remaining == 0 else 'OPEN',
                     payload.get('reason', 'broker execution'), payload.get('proposal_id'), now, strategy, conid])
            if cost_epoch is not None and cost_delta:
                sequence = conn.execute(
                    'SELECT COALESCE(MAX(sequence), 0) + 1 FROM auto_exec_cost_events').fetchone()[0]
                conn.execute('INSERT INTO auto_exec_cost_events VALUES (?, ?, ?, ?, ?, ?, ?, FALSE)',
                             [sequence, cost_epoch, intent['intent_id'], applied, cumulative,
                              cost_delta, quote_notional if not reducing else None])
            conn.execute('INSERT INTO auto_exec_fill_progress VALUES (?, ?) '
                         'ON CONFLICT(intent_id) DO UPDATE SET quantity=excluded.quantity',
                         [intent['intent_id'], cumulative])
            if reducing and (position is None or
                    payload.get('ownership_epoch') is None or payload['ownership_epoch'] != position[1]):
                return 0.0  # this receipt belongs to an older holding, not the current row
            return delta
        return self.db.execute_atomic(_apply)


def _naive(ts):
    """Store UTC instants in DuckDB TIMESTAMP; naive feed times are UTC."""
    if ts is None:
        return None
    if hasattr(ts, 'tzinfo') and ts.tzinfo is not None:
        return ts.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return ts


# ---------------------------------------------------------------------------
# The executor.
# ---------------------------------------------------------------------------

class AutoExecutor:
    KILL_SWITCH_ENV = 'MMR_AUTO_EXECUTE_DISABLED'
    LIVE_ARM_ENV = 'MMR_AUTO_EXECUTE_LIVE'

    def __init__(
        self,
        duckdb_path: str,
        paper_trading: bool,
        event_store: Optional[EventStore] = None,
        cooldown_seconds: float = 300.0,
        sdk_factory=None,
        authority_check=None,
    ):
        self.paper_trading = paper_trading
        self.cooldown_seconds = cooldown_seconds
        self.event_store = event_store
        self.state = AutoExecState(duckdb_path)
        self.intents = IntentStore(duckdb_path)
        self.authority_check = authority_check
        self._management_queued = False
        self._snapshot_cache = None
        self._emergency_exits: dict[tuple[str, int], dict] = {}
        self._exit_overflow: dict[tuple[str, int], SignalWork] = {}
        self._bar_overflow: dict[tuple[str, int], BarWork] = {}
        # sdk_factory is injectable for tests; production default builds the
        # real SDK lazily ON THE WORKER THREAD (MMR.connect() calls
        # asyncio.run, which would blow up on the runtime's event loop).
        self._sdk_factory = sdk_factory or self._default_sdk_factory
        self._sdk = None
        self._queue = ExecutionWorkQueue()
        self._last_exec: Dict[Tuple[str, int], float] = {}
        # (strategy, conid) -> the reason its stale-bar gate is inert.
        # Deduped so a persistent condition logs once, not once per bar.
        self._stale_gate_warned: Dict[Tuple[str, int], str] = {}
        self._ownership_warnings: set[
            tuple[str, int, str] | tuple[str] | tuple[str, str, int, Optional[str]]
        ] = set()
        # Loop-side read model: (strategy, conid) -> entry_bar_ts for open
        # auto positions, so the dispatch loop can compute bars_held from the
        # frame without queue round-trips. Guarded by _view_lock.
        self._open_view: Dict[Tuple[str, int], Any] = {}
        self._managed_view: list[dict] = []
        # Exact committed OPEN identities awaiting view publication. This
        # records authority membership only; quantities still use the atomic
        # owned-inventory/checkpoint snapshot.
        self._unpublished_open_fills: set[str] = set()
        self._status_view: dict = {}
        self._view_lock = threading.Lock()
        self._reconciled = False
        # When the broker first reported an empty book (see
        # accept_empty_broker_read). None = the last read had positions.
        self._first_empty_broker_read: Optional[float] = None
        self._worker = threading.Thread(target=self._run, name='auto-executor', daemon=True)
        self._started = False
        self._load_open_view()
        # Loud, greppable arming state at startup: a live session that sits
        # silent because the double-arm isn't set must say so once, up front —
        # not read as "no signals today".
        if not self.paper_trading and not self.live_armed:
            logging.warning(
                'auto-executor: LIVE trading mode but %s != 1 — auto-execution is '
                'DISARMED (opens will be refused and logged; closes unaffected)',
                self.LIVE_ARM_ENV)

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
        Env-tunable; invalid, nonfinite or nonpositive values fall back to 3."""
        try:
            value = float(os.environ.get('MMR_STALE_BAR_MULTIPLE', '') or 3.0)
        except ValueError:
            return 3.0
        return value if math.isfinite(value) and value > 0 else 3.0

    @property
    def live_armed(self) -> bool:
        """Second arming knob for LIVE auto-execution. STRICT '1' — unlike
        the kill switch's loose truthy parsing, arming real money must not
        happen via 'true'/'yes'/typo (mirrors horserank's double-arm)."""
        return os.environ.get(self.LIVE_ARM_ENV, '') == '1'

    @property
    def empty_broker_grace_seconds(self) -> float:
        """How long an all-empty broker read must persist before reconcile
        believes it. Malformed values fall back to the default rather than to
        zero — zero would restore the startup-race bug this guards."""
        try:
            return float(os.environ.get('MMR_EMPTY_BROKER_GRACE_S', '') or 120.0)
        except ValueError:
            return 120.0

    @property
    def protective_stop_pct(self) -> float:
        """Disaster-stop distance (%) below entry for a broker-side GTC stop
        on every attributed open. This is NOT trade management — strategy
        exits fire long before it. It is the only protection that survives a
        dead feed / dead strategy_service while holding (the stale-bar gate
        only guards opens). Deliberately wide so it never competes with the
        strategy's own exits, which the backtester does not model. 0 (or
        malformed) disables."""
        try:
            return float(os.environ.get('MMR_PROTECTIVE_STOP_PCT', '') or 8.0)
        except ValueError:
            return 0.0

    # -- loop-side API (called from the runtime's event loop; never blocks) ----

    def submit_signal(self, work: SignalWork):
        if self.kill_switch:
            logging.info('auto-executor: signal suppressed by %s', self.KILL_SWITCH_ENV)
            return False
        self.start()
        if self._queue.put(work):
            return True
        if work.action == Action.SELL:
            # Like ordinary queue admission, this is not yet a durable
            # receipt. The worker claims the intent before submitting it.
            # Never wait on a SQLite writer from the market-data loop.
            key = (work.strategy_name, work.conid)
            pending_open = self.intents.has_pending_open(*key)
            with self._view_lock:
                if key not in self._open_view and not pending_open:
                    return True  # flat SELL; cannot acquire executor inventory
                self._exit_overflow.setdefault(key, work)
            self.submit_management()
            logging.error('auto-executor: owned exit admitted to overflow mailbox for %s/%s',
                          work.strategy_name, work.conid)
            return True
        logging.error('auto-executor: opening queue full; signal was NOT admitted for %s/%s',
                      work.strategy_name, work.conid)
        return False

    def submit_bar(self, strategy_name: str, conid: int, bar_ts, bars_held: int,
                   *, entry_bar_ts=None, observed_bar_timestamps=None):
        self.start()
        work = BarWork(strategy_name, conid, bar_ts, bars_held,
                       entry_bar_ts=entry_bar_ts, observed_bar_timestamps=observed_bar_timestamps)
        admitted = self._queue.put(work)
        if not admitted:
            # Preserve every owned bar observation during prolonged overload.
            # A compact staged count survives coalescing without retaining old
            # frames or writing SQLite on the market-data event loop.
            for pos in self.managed_positions():
                if (pos['strategy_name'], pos['conid']) == (strategy_name, conid):
                    if not self._entry_matches(work.entry_bar_ts, pos):
                        return True  # obsolete timer; a later BUY owns a new policy
                    key = (strategy_name, conid)
                    with self._view_lock:
                        previous = self._bar_overflow.get(key)
                        same_entry = (previous is not None and
                                      self._entry_text(previous.entry_bar_ts) == self._entry_text(entry_bar_ts))
                        if same_entry and previous is not None and previous.bar_ts > bar_ts:
                            return True  # the later completed-bar prefix already supersedes it
                        if entry_bar_ts is not None and observed_bar_timestamps is not None:
                            work.observed_progress = self.intents.preview_bar_progress(
                                strategy_name, conid, entry_bar_ts, observed_bar_timestamps,
                                staged_progress=previous.observed_progress if same_entry and previous else None)
                        self._bar_overflow[key] = work
                    self.submit_management()
                    return True
        return admitted

    def submit_management(self):
        with self._view_lock:
            if self._management_queued:
                return
            self._management_queued = True
        self.start()
        self._queue.put(ManagementWork())

    def managed_positions(self) -> list[dict]:
        """Loop-safe management metadata for active and retired deployments."""
        with self._view_lock:
            return [dict(row) for row in self._managed_view]

    def status_metrics(self) -> dict:
        with self._view_lock:
            metrics = dict(self._status_view)
            # These remain truthful even when the durable view cannot refresh.
            metrics['emergency_exit_intents'] = len(self._emergency_exits)
            metrics['durability_degraded'] = bool(self._emergency_exits)
            metrics['overflow_exit_intents'] = len(self._exit_overflow)
            metrics['overflow_bar_items'] = len(self._bar_overflow)
        metrics.update(self._queue.metrics())
        return metrics

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
            try:
                item = self._queue.get(timeout=1.0)
            except queue.Empty:
                item = ManagementWork()
            if item is None:
                return
            try:
                if isinstance(item, SignalWork):
                    try:
                        self._reconcile_once()
                    except Exception:
                        if item.action != Action.SELL:
                            raise
                        # _process_signal has a journal-independent reduction
                        # path. A failed startup reconcile must not prevent it
                        # from receiving the exit already admitted to the queue.
                        logging.exception('auto-executor: reconciliation unavailable; continuing owned exit')
                    self._process_signal(item)
                elif isinstance(item, BarWork):
                    self._process_bar(item)
                elif isinstance(item, ManagementWork):
                    with self._view_lock:
                        self._management_queued = False
                    self.manage_positions()
            except Exception:
                logging.exception('auto-executor: error processing %s', item)

    def adopt_legacy_holding(self, strategy: str, conid: int, avg_cost: Optional[float] = None) -> dict:
        """Operator-attested ownership for a holding attributed before epochs existed.

        Callable from any thread (the strategy_service RPC runs it off-loop).
        Requires a complete broker position read corroborating at least the
        attributed quantity; the cost basis comes from the operator or, failing
        that, the broker's average cost for the instrument. The worker picks
        the adopted holding up on its next management cycle, at which point
        its time exits, closes and protective repair resume.
        """
        conid = int(conid)
        position = self.state.open_position(strategy, conid)
        if position is None:
            raise AutoExecutionError(f'{strategy}/{conid} has no attributed OPEN holding to adopt')
        if position.get('ownership_epoch') is not None:
            raise AutoExecutionError(
                f'{strategy}/{conid} already has ownership epoch {position["ownership_epoch"]}')
        self._snapshot_cache = None  # an attestation corroborates against a FRESH broker read
        broker, complete = self._position_snapshot()
        if not complete:
            raise AutoExecutionError('broker position snapshot is incomplete; retry once the trader is connected')
        held = broker.get(conid, 0.0)
        attributed = float(position['quantity'])
        if not math.isfinite(held) or held < attributed:
            raise AutoExecutionError(
                f'broker holds {held:g} of conId {conid}, less than the {attributed:g} attributed to '
                f'{strategy}; reconcile before adopting')
        if avg_cost is None:
            avg_cost = self._broker_average_cost(conid)
            if avg_cost is None:
                raise AutoExecutionError('broker average cost is unavailable; pass the cost basis explicitly')
        basis = float(avg_cost)
        if not math.isfinite(basis) or basis <= 0:
            raise AutoExecutionError('average cost must be finite and positive')
        attestation = f'legacy-adoption:{dt.datetime.now(dt.timezone.utc).isoformat()}'
        adopted = self.state.adopt_legacy_ownership(strategy, conid, attributed, basis, attestation)
        logging.warning(
            'auto-executor: ADOPTED legacy holding %s/%s: %g @ %.4f (broker holds %g); ownership epoch %s '
            'assigned by operator attestation', strategy, conid, attributed, basis, held, adopted['ownership_epoch'])
        self._ownership_warnings.discard((strategy, conid, 'legacy holding'))
        self._snapshot_cache = None
        self.submit_management()
        return adopted

    def _broker_average_cost(self, conid: int) -> Optional[float]:
        sdk = self._get_sdk()
        if callable(getattr(sdk, 'execution_snapshot', None)):
            raw = self._execution_snapshot().get('positions', [])
            rows = raw.to_dict('records') if hasattr(raw, 'to_dict') else raw
        else:
            frame = sdk.positions()
            rows = frame.to_dict('records') if frame is not None and not frame.empty else []
        for row in rows:
            if int(row.get('conId', 0) or 0) != conid:
                continue
            try:
                value = float(row.get('avgCost'))
            except (TypeError, ValueError):
                return None
            return value if math.isfinite(value) and value > 0 else None
        return None

    def _load_open_view(self):
        # Hydrate every intent kind before the restart read model becomes
        # available: a CLOSE/protective fill can commit before its journal ack.
        intents = self.intents.all()
        positions, checkpoints = self.state.ownership_snapshot(intents)
        view = {}
        managed = []
        for position in positions:
            strategy, conid = position['strategy'], position['conid']
            entry_bar_ts = position['entry_bar_ts']
            view[(strategy, conid)] = entry_bar_ts
            owned_intents = [row for row in intents
                             if (row['strategy'], row['conid']) == (strategy, conid)]
            history = [row for row in owned_intents if row['kind'] == 'OPEN']
            payload = history[-1]['payload'] if history else {}
            managed.append(dict(strategy_name=strategy, conid=conid,
                                quantity=position['quantity'],
                                proposal_id=position['proposal_id'],
                                ownership_epoch=position['ownership_epoch'],
                                ownership_started_at=position['ownership_started_at'],
                                protective_order_id=position['protective_order_id'],
                                entry_bar=entry_bar_ts, entry_bar_ts=entry_bar_ts,
                                # None when the holding predates recorded exit
                                # policy (no OPEN intent payload). Defaulting
                                # here ran a daily strategy's max_hold_bars as
                                # minutes and close_by_time in UTC; the runtime
                                # now falls back to the loaded strategy or defers.
                                bar_size_seconds=payload.get('bar_size_seconds'),
                                session_tz=payload.get('session_tz'),
                                close_by_time=position['close_by_time'],
                                max_hold_bars=position['max_hold_bars'],
                                fill_checkpoints={row['intent_id']: checkpoints.get(row['intent_id'], 0.0)
                                                  for row in owned_intents}))
        with self._view_lock:
            self._open_view = view
            self._managed_view = managed
            self._unpublished_open_fills.clear()
        active = [row for row in intents if row['status'] not in TERMINAL]
        oldest = min((row['payload'].get('submitted_at', row['updated']) for row in active), default=time.time())
        metrics = dict(pending_intents=len(active), unknown_intents=sum(row['status'] in ('UNKNOWN', 'SUBMITTING') for row in active),
                       oldest_pending_seconds=max(0.0, time.time() - oldest),
                       managed_positions=len(managed),
                       unprotected_positions=sum(not row['protective_order_id'] for row in managed),
                       emergency_exit_intents=len(self._emergency_exits))
        with self._view_lock:
            self._status_view = metrics

    def manage_positions(self):
        """Reconcile pending executions, retry exits and repair every holding.

        This method requires neither an enabled strategy nor incoming ticks.
        Bar-count exits still use actual bar notifications, never a wall-clock
        approximation that could count weekends or feed outages as bars.
        """
        self._snapshot_cache = None
        self._claim_overflow_exits()
        self._retry_emergency_exits()
        self._reconciled = False
        self._reconcile_once()
        for intent in self.intents.all(kind='CLOSE', active=True):
            self._advance_close(intent)
        # Broker completion is separate from fulfilling the retained exit
        # request. A smaller old order can fill while a newer due request or
        # an unresolved opening still leaves owned inventory to reduce.
        for intent in self.intents.all(kind='CLOSE'):
            self._finish_close_request(intent)
        for strategy, conid, _qty, _entry, _protective in self.state.all_open():
            self._ensure_protective(strategy, int(conid))
        self._load_open_view()

    def _claim_overflow_exits(self):
        with self._view_lock:
            pending = list(self._exit_overflow.items())
        for key, work in pending:
            if self.kill_switch:
                with self._view_lock:
                    if self._exit_overflow.get(key) is work:
                        self._exit_overflow.pop(key, None)
                continue
            reason = 'SELL signal (queue overload)'
            try:
                self._execute_close(work.strategy_name, work.conid, work.bar_ts, 0.0, reason)
            except Exception:
                self._remember_emergency_exit(work.strategy_name, work.conid, work.bar_ts,
                                              f'{reason}; journal unavailable')
            with self._view_lock:
                if self._exit_overflow.get(key) is work:
                    self._exit_overflow.pop(key, None)
        with self._view_lock:
            pending_bars = list(self._bar_overflow.items())
        for key, bar_work in pending_bars:
            # Trusted retained policy remains independent of the signal kill
            # switch. Claim observed progress only on this execution worker.
            self._process_bar(bar_work)
            with self._view_lock:
                if self._bar_overflow.get(key) is bar_work:
                    self._bar_overflow.pop(key, None)

    def _reconcile_once(self):
        if self._reconciled:
            return
        self._reconcile_intents()
        open_rows = self.state.all_open()
        if not open_rows:
            self._reconciled = True
            return
        try:
            broker, complete = self._position_snapshot()
        except Exception as exc:
            logging.warning('auto-executor: reconciliation pending (broker unavailable: %s)', exc)
            return
        if not complete:
            return
        for strategy, conid, qty, _entry, protective_id in open_rows:
            held = broker.get(int(conid), 0.0)
            if not math.isfinite(held):
                continue
            if held <= 0:
                # Pending entry fills may arrive after this position snapshot.
                if self.intents.all(strategy=strategy, conid=int(conid), kind='OPEN', active=True):
                    continue
                if not self._cancel_protective(strategy, int(conid), protective_id,
                                               'externally closed position'):
                    continue
                self.state.record_close(strategy, int(conid), 'CLOSED_EXTERNALLY',
                                        'complete broker snapshot confirms position absent')
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
        defs = sdk.resolve(conid, sec_type='')
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

    def _now_utc(self) -> dt.datetime:
        """Wall clock, as a seam.

        The stale-bar gate compares a bar timestamp against now, so any test
        driving that path is time-dependent. Tests previously handled this by
        making the FIXTURE dynamic, which made the mutation score move without
        the code moving (~30 mutants flipping between identical runs), and then
        by stubbing out `bar_age_seconds` entirely, which cost ~26 points of
        coverage because the function's own mutants became unreachable.

        Overriding the clock is the version that costs nothing: the real
        `bar_age_seconds` still runs against a real bar timestamp, and only
        "now" is pinned.
        """
        return dt.datetime.now(dt.timezone.utc)

    def _warn_if_stale_gate_inert(self, work, age) -> None:
        """Report why opening freshness cannot be verified, once per owner.

        The decision gate refuses opens until both a datable bar timestamp
        and a known interval are available. Exit authority is unaffected.
        """
        reason = None
        if not math.isfinite(work.bar_size_seconds) or work.bar_size_seconds <= 0:
            reason = (f'bar interval unknown (bar_size_seconds='
                      f'{work.bar_size_seconds!r})')
        elif age is None or not math.isfinite(age):
            reason = f'bar timestamp not datable (bar_ts={work.bar_ts!r})'
        if reason is None:
            return
        key = (work.strategy_name, work.conid)
        if self._stale_gate_warned.get(key) == reason:
            return
        self._stale_gate_warned[key] = reason
        logging.warning(
            'auto-executor: STALE-BAR INPUT UNAVAILABLE for %s conId %s — %s. '
            'Opens are refused until bar freshness can be verified.',
            work.strategy_name, work.conid, reason)

    def _process_signal(self, work: SignalWork):
        if self.kill_switch:
            # The human-owned signal contract makes the global switch apply
            # to SELL signals too. Storage degradation cannot override it;
            # independent pending/time-exit management has its own lifecycle.
            logging.info('auto-executor: signal suppressed by %s', self.KILL_SWITCH_ENV)
            return
        try:
            return self._process_signal_durable(work)
        except AutoExecutionError:
            raise
        except Exception:
            if work.action != Action.SELL:
                raise  # Opens require readable execution state.
            self._remember_emergency_exit(work.strategy_name, work.conid, work.bar_ts,
                                          'SELL signal (local state unavailable)')
            self._retry_emergency_exits()

    def _process_signal_durable(self, work: SignalWork):
        self._snapshot_cache = None
        key = (work.strategy_name, work.conid)
        self._reconcile_intents(*key)
        pos = self.state.open_position(work.strategy_name, work.conid)
        if pos is None:
            # Retire fulfilled requests before a new unrelated entry can
            # inherit them. Unknown openings keep their exit request alive.
            for closing in self.intents.all(strategy=key[0], conid=key[1], kind='CLOSE'):
                self._finish_close_request(closing)
        held_qty = pos['quantity'] if pos else 0.0
        if (work.action == Action.SELL and not self.kill_switch
                and self.intents.all(strategy=work.strategy_name, conid=work.conid, kind='OPEN', active=True)):
            self._execute_close(work.strategy_name, work.conid, work.bar_ts, held_qty,
                                reason='SELL signal (including pending entry)')
            return
        now = dt.datetime.now().timestamp()
        cooldown_active = ((now - self._last_exec.get(key, 0.0)) < self.cooldown_seconds
                           or self.intents.submitted_recently(*key, self.cooldown_seconds))

        # Missing timestamp/interval evidence refuses opens. Explain the
        # unavailable input once per owner instead of repeating every bar.
        age = bar_age_seconds(work.bar_ts, self._now_utc())
        self._warn_if_stale_gate_inert(work, age)

        directive = decide_signal(
            work,
            kill_switch=self.kill_switch,
            paper_trading=self.paper_trading,
            held_qty=held_qty,
            already_executed_bar=self.state.executed_for_bar(
                work.strategy_name, work.conid, work.bar_ts),
            cooldown_active=cooldown_active,
            bar_age_seconds=age,
            stale_bar_multiple=self.stale_bar_multiple,
            live_armed=self.live_armed,
            held_lots=pos['lots'] if pos else 0,
        )

        if directive.kind == 'skip':
            logging.info('auto-executor: %s conId %s %s — skip: %s',
                         work.strategy_name, work.conid, work.action, directive.reason)
            self.state.log_decision(work.strategy_name, work.conid, work.bar_ts,
                                    str(work.action), 'skip', directive.reason)
            return

        try:
            if directive.kind == 'open':
                if work.deployment_generation and (self.authority_check is None or not
                        self.authority_check(work.strategy_name, work.deployment_generation)):
                    self.state.log_decision(work.strategy_name, work.conid, work.bar_ts,
                                            'BUY', 'refused', 'deployment authority revoked')
                    return
                unresolved = self.intents.all(strategy=work.strategy_name, conid=work.conid, active=True)
                pending_successor = any(
                    self._close_request_pending(intent, pos)
                    for intent in self.intents.all(strategy=key[0], conid=key[1], kind='CLOSE')
                    if intent['status'] in TERMINAL)
                with self._view_lock:
                    emergency_pending = key in self._emergency_exits
                if emergency_pending or pending_successor or any(intent['kind'] != 'PROTECTIVE' or intent['status'] != 'WORKING'
                                            for intent in unresolved):
                    self.state.log_decision(work.strategy_name, work.conid, work.bar_ts,
                                            'BUY', 'skip', 'unresolved execution intent reserves exposure')
                    return
                # Manifest envelope — OPENS ONLY. Checked here, inside the open
                # branch and BEFORE any propose, so the close/skip/time-exit
                # paths are structurally exempt (an envelope can never block an
                # exit). A refusal places no order.
                refusal = self._manifest_gate(work, directive)
                if refusal is not None:
                    # Direction refusals are a regression signal (an
                    # unexpected short path appeared) — loud ERROR. Universe /
                    # turnover refusals are ordinary policy — WARNING. Both
                    # write decision='refused' to the audit log and propose
                    # nothing (mirrors the deliberate-refusal path below).
                    log = (logging.error
                           if refusal.reason.startswith('manifest: direction')
                           else logging.warning)
                    log('auto-executor: %s conId %s %s — %s',
                        work.strategy_name, work.conid, work.action, refusal.reason)
                    self.state.log_decision(work.strategy_name, work.conid, work.bar_ts,
                                            str(work.action), 'refused', refusal.reason)
                    return
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

    def _manifest_gate(self, work: SignalWork, directive: Directive) -> Optional[Directive]:
        """Run the pure ``check_manifest`` gate, supplying the rolling-window
        turnover counts it needs. Fast no-op (no DB read) when the strategy
        declared no manifest — this keeps the no-manifest path byte-identical.

        The per-day window is a ROLLING 24h (``now - 24h``), not a session/
        calendar day: it needs no session-boundary bookkeeping and survives a
        restart because the counts come straight from the persisted bar log."""
        if (work.manifest_allowed_conids is None
                and work.manifest_direction is None
                and work.manifest_max_opens_per_day is None
                and work.manifest_max_opens_per_hour is None):
            return None
        now = time.time()
        submitted = [intent for intent in
                     self.intents.all(strategy=work.strategy_name, kind='OPEN')
                     if intent['status'] != 'REJECTED']
        opens_today = 0
        opens_hour = 0
        if work.manifest_max_opens_per_day is not None:
            since = now - 86400
            recent = {intent['intent_id'] for intent in submitted
                      if max(intent['payload'].get('submitted_at', 0.0),
                             intent['payload'].get('receipt_observed_at', 0.0)) >= since}
            opens_today = self.state.count_opens_since(
                work.strategy_name, dt.datetime.fromtimestamp(since),
                submitted_intent_ids=recent)
        if work.manifest_max_opens_per_hour is not None:
            since = now - 3600
            recent = {intent['intent_id'] for intent in submitted
                      if max(intent['payload'].get('submitted_at', 0.0),
                             intent['payload'].get('receipt_observed_at', 0.0)) >= since}
            opens_hour = self.state.count_opens_since(
                work.strategy_name, dt.datetime.fromtimestamp(since),
                submitted_intent_ids=recent)
        return check_manifest(work, directive, opens_today, opens_hour)

    def _execute_open(self, work: SignalWork, directive: Directive):
        position = self.state.open_position(work.strategy_name, work.conid)
        if position is not None and position.get('ownership_epoch') is None:
            raise AutoExecutionError(
                'cannot add exposure while strategy ownership requires reconciliation')
        broker, _complete = self._position_snapshot()
        observed = broker.get(work.conid)
        if observed is not None and observed < 0:
            raise AutoExecutionError('long-only automated entry cannot overlap an existing broker short position')
        ident = self._resolve_exact(work.conid)
        payload = dict(bar_ts=timestamp_text(work.bar_ts), quantity=directive.quantity,
                       amount=work.trade_amount if not directive.quantity and work.trade_amount > 0 else None,
                       probability=max(0.0, min(1.0, work.probability)), risk=work.risk,
                       close_by_time=work.close_by_time.isoformat() if work.close_by_time else None,
                       max_hold_bars=work.max_hold_bars, bar_size_seconds=work.bar_size_seconds,
                       session_tz=str(getattr(work.bar_ts, 'tzinfo', None) or 'UTC'),
                       deployment_generation=work.deployment_generation, ident=ident)
        intent = self.intents.create(work.strategy_name, work.conid, 'OPEN', payload)
        # The durable reservation precedes even proposal creation. A crash in
        # propose/approve cannot make a restart mistake this instrument for flat.
        self._submit_intent(intent)
        self._ensure_protective(work.strategy_name, work.conid)

    def _submit_intent(self, intent):
        sdk = self._get_sdk()
        payload = intent['payload']
        action = 'BUY' if intent['kind'] == 'OPEN' else 'SELL'
        ident = payload['ident']
        self.intents.update(intent, status='SUBMITTING', submitted_at=time.time())
        approve_started = False
        try:
            proposal_id = payload.get('proposal_id')
            if proposal_id is None:
                proposal_id, _leverage, _snap = sdk.propose(
                    symbol=ident['symbol'], action=action, quantity=payload.get('quantity'),
                    amount=payload.get('amount'), confidence=payload.get('probability', 1.0),
                    reasoning=f"auto-executed {'open' if action == 'BUY' else 'close'} for {intent['strategy']}: {payload.get('reason', 'signal')}",
                    source=f"strategy:{intent['strategy']}",
                    metadata={'auto_executed': True, 'strategy': intent['strategy'],
                              'conid': intent['conid'], 'con_id': intent['conid'],
                              'risk_level': payload.get('risk', 0.0),
                              'bar_ts': payload['bar_ts'], 'close_reason': payload.get('reason'),
                              'client_intent_id': intent['intent_id']},
                    sec_type=ident['sec_type'], exchange=ident['exchange'], currency=ident['currency'])
                self.intents.update(intent, proposal_id=int(proposal_id))
            if action == 'BUY':
                generation = payload.get('deployment_generation', '')
                authority = not generation or (self.authority_check is not None and
                                               self.authority_check(intent['strategy'], generation))
                if not authority or self.kill_switch or (not self.paper_trading and not self.live_armed):
                    self.intents.update(intent, status='REJECTED', error='opening authority revoked before approval')
                    return
            approve_started = True
            if payload.get('resume_approval'):
                result = sdk.approve(proposal_id, resume=True)
            else:
                result = sdk.approve(proposal_id)
            self._snapshot_cache = None
        except Exception as exc:
            # Even a connection error may occur after server receipt. The
            # absence of a reply is never proof that the broker did nothing.
            self.intents.update(intent, status='UNKNOWN' if approve_started else 'REJECTED', error=str(exc))
            logging.exception('auto-executor: submission UNKNOWN for %s', intent['intent_id'])
            return
        if not result.is_success():
            error = str(result.error)
            self.intents.update(intent, status='UNKNOWN', error=error)
            # A terminal rejection/cancellation can race with a real partial
            # fill. Recover executions before treating the response as refusal.
            self._reconcile_intent(intent)
            if intent['payload'].get('order_ids'):
                return
            explicit_rejection = (not any(word in error.lower() for word in
                                          ('unknown', 'timeout', 'timed out', 'connection'))
                                  and any(word in error.lower() for word in ('reject', 'refus', 'risk gate')))
            self.intents.update(intent, status='REJECTED' if explicit_rejection else 'UNKNOWN', error=error)
            self.state.log_decision(intent['strategy'], intent['conid'],
                                    dt.datetime.fromisoformat(payload['bar_ts']), action,
                                    'open_failed' if action == 'BUY' else 'close_failed', error)
            if explicit_rejection:
                self._append_event(EventType.ORDER_REJECTED, intent['strategy'], intent['conid'],
                                   action, payload.get('quantity') or 0.0, error)
            return
        obj = result.obj
        order_ids = obj.get('order_ids', []) if isinstance(obj, dict) else (obj or [])
        # Keep the receipt time with the durable submission receipt even if the
        # following DuckDB activity-log write fails. It still consumes
        # opening capacity after a delayed acknowledgement or restart.
        self.intents.update(intent, status='WORKING', order_ids=[int(oid) for oid in order_ids],
                            receipt_observed_at=time.time())
        self.state.log_decision(intent['strategy'], intent['conid'],
                                dt.datetime.fromisoformat(payload['bar_ts']), action,
                                'open' if action == 'BUY' else 'close', f'proposal #{proposal_id}; awaiting fills',
                                intent_id=intent['intent_id'])
        self._last_exec[(intent['strategy'], intent['conid'])] = time.time()
        self._reconcile_intent(intent)

    def _capture_exit_scope(self, strategy: str, conid: int) -> dict:
        """Capture authority once, when an explicit SELL is processed."""
        cached_position = False
        try:
            position = self.state.open_position(strategy, conid)
        except Exception:
            cached_position = True
            position = next((row for row in self.managed_positions()
                             if (row['strategy_name'], row['conid']) == (strategy, conid)), None)
        try:
            openings = self.intents.all(strategy=strategy, conid=conid, kind='OPEN')
        except Exception:
            openings = [item for item in self.intents.cached()
                        if (item['strategy'], item['conid'], item['kind']) == (strategy, conid, 'OPEN')]
        openings = [item for item in openings if item['status'] not in TERMINAL
                    or cached_position and item['intent_id'] in self._unpublished_open_fills]
        return dict(positions=([dict(entry_bar_ts=self._entry_text(position['entry_bar_ts']),
                                    proposal_id=position.get('proposal_id'))] if position else []),
                    openings={item['intent_id']: dict(entry_bar_ts=item['payload']['bar_ts'],
                                                      proposal_id=item['payload'].get('proposal_id'))
                              for item in openings})

    def _scope_matches_position(self, scope: dict, position) -> bool:
        if position is None:
            return False
        for captured in scope['positions']:
            if (self._entry_matches(captured['entry_bar_ts'], position)
                    and captured.get('proposal_id') == position.get('proposal_id')):
                return True
        cached = {item['intent_id']: item for item in self.intents.cached()}
        for intent_id, captured in scope['openings'].items():
            proposal_id = captured.get('proposal_id')
            if proposal_id is None:
                opening = cached.get(intent_id)
                if (opening is not None and opening['kind'] == 'OPEN'
                        and self._entry_text(opening['payload']['bar_ts']) == captured['entry_bar_ts']):
                    proposal_id = opening['payload'].get('proposal_id')
            if (proposal_id is not None and proposal_id == position.get('proposal_id')
                    and self._entry_matches(captured['entry_bar_ts'], position)):
                return True
        return False

    def _scope_has_pending_openings(self, intent) -> bool:
        scope = intent['payload'].get('explicit_exit_scope')
        if scope is None:
            return bool(self.intents.all(strategy=intent['strategy'], conid=intent['conid'],
                                         kind='OPEN', active=True))
        known = {item['intent_id']: item for item in self.intents.all(
            strategy=intent['strategy'], conid=intent['conid'], kind='OPEN')}
        return any(intent_id not in known or known[intent_id]['status'] not in TERMINAL
                   for intent_id in scope['openings'])

    def _scope_accepts_opening(self, scope: dict, opening) -> bool:
        captured = scope['openings'].get(opening['intent_id'])
        if captured is not None:
            return self._entry_text(opening['payload']['bar_ts']) == captured['entry_bar_ts']
        proposal_id = opening['payload'].get('proposal_id')
        return proposal_id is not None and any(
            proposal_id == position.get('proposal_id')
            and self._entry_text(opening['payload']['bar_ts']) == position['entry_bar_ts']
            for position in scope['positions'])

    def _execute_close(self, strategy_name: str, conid: int, bar_ts,
                       attributed_qty: float, reason: str, *, entry_bar_ts=None, explicit=True):
        scope = self._capture_exit_scope(strategy_name, conid) if explicit and entry_bar_ts is None else None
        try:
            self._execute_close_durable(strategy_name, conid, bar_ts, attributed_qty, reason,
                                        entry_bar_ts=entry_bar_ts, exit_scope=scope)
        except AutoExecutionError:
            raise
        except Exception:
            logging.exception('auto-executor: durable exit journal unavailable; retaining emergency reduction')
            parents = [item for item in self.intents.cached()
                       if (item['strategy'], item['conid'], item['kind']) == (strategy_name, conid, 'CLOSE')
                       and item['status'] not in TERMINAL
                       and ((scope is not None and item['payload'].get('policy_entry_bar_ts') is None
                             and merge_exit_scopes(item['payload'].get('explicit_exit_scope'), scope)
                             == item['payload'].get('explicit_exit_scope'))
                            or (scope is None and (
                                item['payload'].get('policy_entry_bar_ts') == self._entry_text(entry_bar_ts)
                                or ((item['payload'].get('successor_exit') or {}).get('entry_bar_ts')
                                    == self._entry_text(entry_bar_ts)
                                    and (item['payload'].get('successor_exit') or {}).get('bar_ts')
                                    == timestamp_text(bar_ts)))))]
            parent = parents[0] if len(parents) == 1 else None
            retained_scope = merge_exit_scopes(parent['payload'].get('explicit_exit_scope') if parent else None, scope)
            self._remember_emergency_exit(strategy_name, conid, bar_ts, reason, entry_bar_ts=entry_bar_ts,
                                          explicit_exit_scope=retained_scope, capture_explicit=False,
                                          parent_intent_id=parent['intent_id'] if parent else None)
            self._retry_emergency_exits()

    def _execute_close_durable(self, strategy_name: str, conid: int, bar_ts,
                              attributed_qty: float, reason: str, *, entry_bar_ts=None, exit_scope=None):
        self._reconcile_intents(strategy_name, conid)
        position = self.state.open_position(strategy_name, conid)
        if entry_bar_ts is not None and not self._entry_matches(entry_bar_ts, position):
            return
        outstanding = self.intents.all(strategy=strategy_name, conid=conid, kind='CLOSE', active=True)
        if not outstanding:
            request_id = uuid.uuid4().hex
            intent = self.intents.create(strategy_name, conid, 'CLOSE',
                dict(bar_ts=timestamp_text(bar_ts), quantity=attributed_qty, reason=reason,
                     ident=self._resolve_exact(conid), policy_entry_bar_ts=self._entry_text(entry_bar_ts),
                     exit_request_active=True, exit_request_id=request_id,
                     explicit_exit_scope=exit_scope), status='WAITING')
            if intent['payload'].get('exit_request_id') == request_id:
                self._advance_close(intent)
                return
            # The claim can return an existing attempt created after the
            # read. It needs the same handoff as any other pending close.
            outstanding = [intent]
        if outstanding:
            if exit_scope is not None:
                self.intents.retain_exit_scope(outstanding[0], exit_scope)
            if entry_bar_ts is None and outstanding[0]['payload'].get('policy_entry_bar_ts') is not None:
                # An explicit SELL owns flattening authority independently of
                # a prior timer's obsolete entry epoch.
                self.intents.update(outstanding[0], policy_entry_bar_ts=None, reason=reason)
            if position is not None:
                request = dict(request_id=uuid.uuid4().hex, bar_ts=timestamp_text(bar_ts), reason=reason,
                               entry_bar_ts=self._entry_text(position['entry_bar_ts']),
                               policy_entry_bar_ts=self._entry_text(entry_bar_ts))
                self._retain_close_successor(outstanding[0], request)
            self._advance_close(outstanding[0])
            self._finish_close_request(outstanding[0])
            return

    def _retain_close_successor(self, intent, request):
        """Retain newer exit authority without changing a broker attempt."""
        self.intents.retain_exit_successor(intent, request)

    def _close_request_pending(self, intent, position):
        payload = intent['payload']
        successor = payload.get('successor_exit')
        active = payload.get('exit_request_active', intent['status'] in ('CANCELLED', 'REJECTED'))
        if not successor and not active:
            return False
        scope = payload.get('explicit_exit_scope')
        if scope is not None and (self._scope_matches_position(scope, position)
                                  or self._scope_has_pending_openings(intent)):
            return True
        if successor:
            return position is not None and self._entry_matches(successor['entry_bar_ts'], position)
        return bool(active and position is not None and self._close_entry_matches(intent, position))

    def _close_entry_matches(self, intent, position):
        scope = intent['payload'].get('explicit_exit_scope')
        if scope is not None:
            if self._scope_matches_position(scope, position):
                return True
            if intent['payload'].get('request_entry_bar_ts') is None:
                return position is None and self._scope_has_pending_openings(intent)
        return (self._entry_matches(intent['payload'].get('policy_entry_bar_ts'), position)
                and self._entry_matches(intent['payload'].get('request_entry_bar_ts'), position))

    def _finish_close_request(self, intent):
        """Move terminal attempts' still-valid demand to one residual close."""
        if intent['status'] not in TERMINAL:
            return
        payload = intent['payload']
        successor = payload.get('successor_exit')
        if not successor and not payload.get('exit_request_active', intent['status'] in ('CANCELLED', 'REJECTED')):
            return
        strategy, conid = intent['strategy'], intent['conid']
        position = self.state.open_position(strategy, conid)
        token = successor['request_id'] if successor else None
        if position is None:
            if self._scope_has_pending_openings(intent):
                return  # a later receipt can still add owned inventory
            self.intents.finish_exit_request(intent, token)
            return
        request = successor or dict(request_id=uuid.uuid4().hex, bar_ts=payload['bar_ts'],
                                    reason=payload.get('reason', 'pending exit'),
                                    entry_bar_ts=payload.get('request_entry_bar_ts'),
                                    policy_entry_bar_ts=payload.get('policy_entry_bar_ts'))
        scope = payload.get('explicit_exit_scope')
        scoped = scope is not None and self._scope_matches_position(scope, position)
        bound = (request['entry_bar_ts'] is not None or scope is None) and (
            self._entry_matches(request['entry_bar_ts'], position)
            and self._entry_matches(request.get('policy_entry_bar_ts'), position))
        if not scoped and not bound:
            if scope is not None and self._scope_has_pending_openings(intent):
                return
            self.intents.finish_exit_request(intent, token)
            return
        # Another request can already own the single executable close slot.
        # Leave this request durable until that attempt's outcome is known.
        if self.intents.all(strategy=strategy, conid=conid, kind='CLOSE', active=True):
            return
        next_intent = self.intents.create(strategy, conid, 'CLOSE',
            dict(bar_ts=request['bar_ts'], quantity=position['quantity'], reason=request['reason'],
                 ident=self._resolve_exact(conid), policy_entry_bar_ts=request.get('policy_entry_bar_ts'),
                 request_entry_bar_ts=request['entry_bar_ts'], exit_request_active=True,
                 exit_request_id=request['request_id'], explicit_exit_scope=scope), status='WAITING')
        if next_intent['payload'].get('exit_request_id') != request['request_id']:
            return  # create returned a different already-active request
        self.intents.finish_exit_request(intent, token)
        self._advance_close(next_intent)

    def _advance_close(self, intent):
        """Keep exit intent alive while cancelling conflicting executable work.

        No policy gate refuses this exit. Cancellation uncertainty leaves a
        durable pending reduction that management retries independently of the
        strategy, instead of submitting a second executable SELL.
        """
        self._reconcile_intent(intent)
        if (intent['status'] in TERMINAL or intent['payload'].get('order_ids')
                or intent['payload'].get('attribution_unresolved')):
            return
        if intent['status'] in ('SUBMITTING', 'UNKNOWN', 'WORKING') and (
                intent['payload'].get('proposal_id') or intent['payload'].get('emergency')
                or 'cumulative_filled' in intent['payload']):
            # Restored emergency receipts can retain permId/reference while
            # lacking a cancellation ID or proposal. Observed/attempted work
            # still needs exact no-send proof before another submission.
            # A native pre-proposal SUBMITTING claim has none of these facts.
            try:
                snapshot = self._execution_snapshot(intent)
            except Exception as exc:
                logging.warning('auto-executor: exit %s awaits retry proof: %s',
                                intent['intent_id'], exc)
                return
            if not snapshot.get('retry_safe', False):
                return
            if intent['payload'].get('proposal_id'):
                self.intents.update(intent, resume_approval=True)
        strategy, conid = intent['strategy'], intent['conid']
        try:
            if not self._close_entry_matches(intent, self.state.open_position(strategy, conid)):
                self.intents.update(intent, status='RESOLVED',
                                    never_submitted=self._unsubmitted_close(intent))
                return
            if 'ident' not in intent['payload']:
                self.intents.update(intent, ident=self._resolve_exact(conid))
            opens = self.intents.all(strategy=strategy, conid=conid, kind='OPEN', active=True)
            for opening in opens:
                if self._opening_is_broker_terminal(opening):
                    # Final quantity can remain unknown after bounded history
                    # replay even though IB proves the BUY has no live
                    # remainder. Retain that opening reservation, but allow
                    # reduction of the independently confirmed owned shares.
                    self._reconcile_intent(opening)
                    continue
                ids = opening['payload'].get('order_ids', [])
                if not ids:
                    return  # unresolved opening must be reconciled before exit size is final
                if not all(self._cancel_order(oid) for oid in ids):
                    return
                self._reconcile_intent(opening)
            position = self.state.open_position(strategy, conid)
            if not position or not self._close_entry_matches(intent, position):
                self.intents.update(intent, status='RESOLVED',
                                    never_submitted=self._unsubmitted_close(intent))
                return
            broker, complete = self._position_snapshot()
            if not complete:
                return
            broker_qty = broker.get(conid, 0.0)
            if not math.isfinite(broker_qty):
                return
            qty = reducible_quantity(position['quantity'], broker_qty)
            if not self._execution_snapshot().get('complete', False):
                # An incomplete order book defers this close exactly like the
                # incomplete position read above. Letting the protective lookup
                # raise here escalated a routine, still-WAITING close into the
                # emergency path: two independent submitters for one holding.
                return
            own = self._own_live_protectives(self._get_sdk(), strategy, conid)
            for protective in self.intents.all(strategy=strategy, conid=conid, kind='PROTECTIVE', active=True):
                if protective['payload'].get('attribution_unresolved'):
                    return  # uncertainty is a reservation even without a cancellation ID
                ids = protective['payload'].get('order_ids', [])
                if not ids or not all(isinstance(oid, int) and not isinstance(oid, bool) and oid > 0
                                      for oid in ids):
                    return  # a lost acknowledgment or legacy zero ID cannot release protection
                for oid in ids:
                    if oid not in own:
                        own.append(oid)
            tracked = position.get('protective_order_id')
            if tracked and int(tracked) not in own:
                own.append(int(tracked))
            if not all(self._cancel_protective(strategy, conid, oid, intent['payload']['reason']) for oid in own):
                return
            # Fills may race with cancellation. Refresh both ownership and
            # broker quantity after cancellation confirmation before sizing.
            self._reconcile_intents(strategy, conid)
            if self.intents.all(strategy=strategy, conid=conid, kind='PROTECTIVE', active=True):
                return  # new or still-uncertain protection must join the next cancellation cycle
            position = self.state.open_position(strategy, conid)
            broker, complete = self._position_snapshot()
            if not complete:
                return
            if not position or not self._close_entry_matches(intent, position):
                self.intents.update(intent, status='RESOLVED',
                                    never_submitted=self._unsubmitted_close(intent))
                self._load_open_view()
                return
            broker_qty = broker.get(conid, 0.0)
            if not math.isfinite(broker_qty):
                return
            qty = reducible_quantity(position['quantity'], broker_qty)
            if qty <= 0:
                self.state.record_close(strategy, conid, 'CLOSED_EXTERNALLY',
                                        'complete broker snapshot confirms no reducible holding')
                self.intents.update(intent, status='RESOLVED',
                                    never_submitted=self._unsubmitted_close(intent))
                self._load_open_view()
                return
            epoch = position.get('ownership_epoch')
            if epoch is None:
                self._warn_ownership_pending(strategy, conid, 'legacy holding')
                return
            bound_epoch = intent['payload'].get('ownership_epoch')
            if bound_epoch is not None and bound_epoch != epoch:
                # A proven-unsent retry still has an immutable physical
                # identity. Its request can transfer to a new attempt.
                self.intents.update(intent, status='RESOLVED',
                                    never_submitted=self._unsubmitted_close(intent))
                return
            self.intents.update(intent, quantity=qty, ownership_epoch=epoch,
                                ownership_started_at=position.get('ownership_started_at'))
            self._submit_intent(intent)
        except Exception as exc:
            logging.warning('auto-executor: exit %s remains pending: %s', intent['intent_id'], exc)
            self._remember_emergency_exit(strategy, conid, dt.datetime.fromisoformat(intent['payload']['bar_ts']),
                                          intent['payload'].get('reason', 'pending exit'),
                                          entry_bar_ts=intent['payload'].get('policy_entry_bar_ts'),
                                          request_entry_bar_ts=intent['payload'].get('request_entry_bar_ts'),
                                          explicit_exit_scope=intent['payload'].get('explicit_exit_scope'),
                                          capture_explicit=False, parent_intent_id=intent['intent_id'])
            self._retry_emergency_exits()

    @staticmethod
    def _emergency_identity(parent_intent_id=None, ownership_epoch=None) -> str:
        identity = f'emergency-{int(time.time() * 1000):x}-{uuid.uuid4().hex[:12]}'
        if parent_intent_id is not None:
            # Full digest, fixed size even for long server-side identifiers.
            # The resulting ID fits the RPC's 160-byte recovery-ID limit.
            identity += '-parent-' + hashlib.sha256(parent_intent_id.encode()).hexdigest()
        if ownership_epoch is not None:
            if (not isinstance(ownership_epoch, str) or len(ownership_epoch) != 32
                    or any(char not in '0123456789abcdef' for char in ownership_epoch)):
                raise AutoExecutionError('invalid emergency ownership epoch')
            identity += '-epoch-' + ownership_epoch
        return identity

    @staticmethod
    def _emergency_epoch(identity):
        if '-epoch-' not in identity:
            return None
        epoch = identity.rsplit('-epoch-', 1)[1]
        return epoch if len(epoch) == 32 and all(char in '0123456789abcdef' for char in epoch) else None

    def _remember_emergency_exit(self, strategy, conid, bar_ts, reason, *, entry_bar_ts=None,
                                 request_entry_bar_ts=None, explicit_exit_scope=None, capture_explicit=True,
                                 parent_intent_id=None):
        key = (strategy, conid)
        policy_entry = self._entry_text(entry_bar_ts)
        if capture_explicit and policy_entry is None and explicit_exit_scope is None:
            explicit_exit_scope = self._capture_exit_scope(strategy, conid)
        with self._view_lock:
            if key not in self._emergency_exits:
                self._emergency_exits[key] = dict(
                    intent_id=self._emergency_identity(parent_intent_id), parent_intent_id=parent_intent_id,
                    strategy=strategy, conid=conid, bar_ts=timestamp_text(bar_ts),
                    reason=reason, attempted=False, policy_entry_bar_ts=policy_entry,
                    request_entry_bar_ts=self._entry_text(request_entry_bar_ts),
                    explicit_exit_scope=merge_exit_scopes(None, explicit_exit_scope))
            else:
                previous = self._emergency_exits[key]
                previous['explicit_exit_scope'] = merge_exit_scopes(
                    previous.get('explicit_exit_scope'), explicit_exit_scope)
                old_policy = previous.get('policy_entry_bar_ts')
                owned = next((row for row in self._managed_view
                              if (row['strategy_name'], row['conid']) == key), None)
                binding = self._entry_text(request_entry_bar_ts) or policy_entry or (
                    self._entry_text(owned['entry_bar_ts']) if owned else None)
                retained = previous.get('successor_exit')
                newest = (retained['entry_bar_ts'] if retained else
                          previous.get('request_entry_bar_ts') or old_policy)
                # Exception retries of A cannot downgrade a retained B.
                if binding is not None and newest is not None and binding < newest:
                    return
                if not previous['attempted']:
                    if policy_entry is None or old_policy is not None:
                        previous.update(policy_entry_bar_ts=policy_entry, reason=reason,
                                        bar_ts=timestamp_text(bar_ts))
                    if previous.get('request_entry_bar_ts') is not None and binding is not None:
                        previous['request_entry_bar_ts'] = binding
                elif binding is not None:
                    policy = None if old_policy is None or policy_entry is None else policy_entry
                    if retained and retained.get('policy_entry_bar_ts') is None:
                        policy = None
                    if (not retained or retained['entry_bar_ts'] != binding
                            or retained.get('policy_entry_bar_ts') != policy):
                        previous['successor_exit'] = dict(
                            request_id=uuid.uuid4().hex, entry_bar_ts=binding,
                            policy_entry_bar_ts=policy, bar_ts=timestamp_text(bar_ts), reason=reason)
                    if policy_entry is None:
                        previous.update(policy_entry_bar_ts=None, reason=reason)
        logging.critical('auto-executor: emergency exit pending for %s/%s; local durability is degraded',
                         strategy, conid)

    def _retry_emergency_exits(self):
        with self._view_lock:
            pending = list(self._emergency_exits.values())
            positions = [dict(row) for row in self._managed_view]
        for emergency in pending:
            try:
                prior = None
                strategy, conid = emergency['strategy'], emergency['conid']
                snapshot = self._execution_snapshot()
                if not snapshot.get('complete', False):
                    continue
                rows = snapshot.get('orders', [])
                if hasattr(rows, 'to_dict'):
                    rows = rows.to_dict('records')
                def belongs(row, intent_id, ids=()):
                    return (int(row.get('conId', 0) or 0) == conid
                            and self._matches_intent_order(row, intent_id, ids))
                placed = [row for row in rows if belongs(row, emergency['intent_id'], emergency.get('order_ids', []))]
                if placed:
                    try:
                        restored = self.intents.restore_exit(
                            emergency['intent_id'], strategy, conid,
                            dict(bar_ts=emergency['bar_ts'], reason=emergency['reason'],
                                 order_ids=[int(row['orderId']) for row in placed if int(row['orderId']) > 0],
                                 emergency=True, policy_entry_bar_ts=emergency.get('policy_entry_bar_ts'),
                                 request_entry_bar_ts=emergency.get('request_entry_bar_ts'),
                                 explicit_exit_scope=emergency.get('explicit_exit_scope'),
                                 ownership_epoch=emergency.get('ownership_epoch'),
                                 ownership_started_at=emergency.get('ownership_started_at'),
                                 attribution_unresolved=emergency.get('ownership_epoch') is None,
                                 exit_request_active=True))
                        if emergency.get('explicit_exit_scope') is not None:
                            self.intents.retain_exit_scope(restored, emergency['explicit_exit_scope'])
                        if emergency.get('successor_exit'):
                            self._retain_close_successor(restored, emergency['successor_exit'])
                        self._reconcile_intent(restored)
                        if restored['status'] in TERMINAL:
                            self._finish_close_request(restored)
                            with self._view_lock:
                                self._emergency_exits.pop((strategy, conid), None)
                        continue
                    except Exception:
                        # The broker order still exists even if neither local
                        # journal can acknowledge it. Never submit it again.
                        continue
                if emergency['attempted']:
                    lookup = getattr(self._get_sdk(), 'execution_snapshot', None)
                    if not callable(lookup):
                        continue
                    prior = lookup(intent_id=emergency['intent_id'])
                    if not prior.get('retry_safe', False):
                        continue
                    successor = emergency.get('successor_exit')
                    if successor:
                        # RETRYABLE plus an empty scoped topology proves the
                        # old wire intent never sent an order. The successor
                        # can need a different quantity, so give it a new ID
                        # instead of changing the old request's fingerprint.
                        if prior.get('orders') is None or len(prior['orders']):
                            continue
                        replacement = dict(
                            intent_id=self._emergency_identity(emergency.get('parent_intent_id')),
                            parent_intent_id=emergency.get('parent_intent_id'),
                            strategy=strategy, conid=conid, attempted=False,
                            bar_ts=successor['bar_ts'], reason=successor['reason'],
                            policy_entry_bar_ts=successor.get('policy_entry_bar_ts'),
                            request_entry_bar_ts=successor['entry_bar_ts'],
                            explicit_exit_scope=merge_exit_scopes(None, emergency.get('explicit_exit_scope')))
                        with self._view_lock:
                            if self._emergency_exits.get((strategy, conid)) is emergency:
                                self._emergency_exits[(strategy, conid)] = replacement
                                pending.append(replacement)
                        continue
                owned = next((row for row in positions
                              if row['strategy_name'] == strategy and row['conid'] == conid), None)
                if owned is None:
                    continue  # unavailable ownership is not permission to sell manual inventory
                scope = emergency.get('explicit_exit_scope')
                scoped = scope is not None and self._scope_matches_position(scope, owned)
                bound = (scope is None or emergency.get('request_entry_bar_ts') is not None) and (
                    self._entry_matches(emergency.get('policy_entry_bar_ts'), owned)
                    and self._entry_matches(emergency.get('request_entry_bar_ts'), owned))
                if not scoped and not bound:
                    if (scope is not None and self._scope_matches_position(
                            scope, self.state.open_position(strategy, conid))):
                        # Durable ownership still matches the captured SELL,
                        # but its published view has not caught up. Retain the
                        # request without sizing or sending from the stale view.
                        continue
                    expected = emergency.get('request_entry_bar_ts') or emergency.get('policy_entry_bar_ts')
                    if expected is not None and expected > self._entry_text(owned['entry_bar_ts']):
                        continue  # cached ownership has not caught up with the validated request
                    if not emergency['attempted']:
                        with self._view_lock:
                            self._emergency_exits.pop((strategy, conid), None)
                    continue
                quantity = float(owned['quantity'])
                checkpoints = owned.get('fill_checkpoints', {})
                superseded = False
                opening_delta = 0.0
                reducing_delta = 0.0
                for intent in self.intents.cached():
                    if (intent['strategy'], intent['conid']) != (strategy, conid):
                        continue
                    if intent['kind'] != 'OPEN':
                        if self._unsubmitted_close(intent):
                            continue
                        if (intent['payload'].get('attribution_unresolved')
                                or intent['payload'].get('ownership_epoch') is None):
                            raise AutoExecutionError('reducing order ownership requires reconciliation')
                        if intent['payload']['ownership_epoch'] != owned.get('ownership_epoch'):
                            continue  # an old holding's receipt is not a current inventory delta
                    matched = [row for row in rows if belongs(row, self._broker_intent_id(intent),
                                                              intent['payload'].get('order_ids', []))]
                    if not matched and intent['status'] not in TERMINAL:
                        if (intent['kind'] == 'CLOSE' and intent['status'] == 'WAITING'
                                and not intent['payload'].get('proposal_id')
                                and not intent['payload'].get('order_ids')):
                            continue  # close claim exists, but no submission has begun
                        raise AutoExecutionError('unresolved broker execution; emergency exit remains pending')
                    if not matched:
                        continue
                    if any(row.get('identityAmbiguous', False) for row in matched):
                        raise AutoExecutionError('ambiguous broker identity; ownership requires reconciliation')
                    fills = [float(row['filled']) for row in matched]
                    if not all(math.isfinite(value) and value >= 0 for value in fills):
                        raise AutoExecutionError('unreadable emergency fill quantities')
                    delta = max(0.0, sum(fills) - checkpoints.get(intent['intent_id'], 0.0))
                    covered = (intent['kind'] == 'OPEN' and scope is not None
                               and self._scope_accepts_opening(scope, intent))
                    if delta > 0 and intent['kind'] == 'OPEN' and scope is not None and not covered:
                        raise AutoExecutionError('opening fill outside captured explicit exit authority')
                    expected_entry = emergency.get('request_entry_bar_ts') or emergency.get('policy_entry_bar_ts')
                    if (delta > 0 and intent['kind'] == 'OPEN' and not covered and expected_entry is not None
                            and self._entry_text(intent['payload']['bar_ts']) > expected_entry):
                        # Storage may still expose the old entry while a
                        # fresh broker receipt proves a later BUY now owns
                        # the holding period. Do not execute its old timer.
                        superseded = True
                        break
                    if intent['kind'] == 'OPEN':
                        opening_delta += delta
                    else:
                        reducing_delta += delta
                    quantity += delta if intent['kind'] == 'OPEN' else -delta
                if superseded:
                    if not emergency['attempted']:
                        with self._view_lock:
                            self._emergency_exits.pop((strategy, conid), None)
                    continue
                if opening_delta > 0 and reducing_delta >= float(owned['quantity']):
                    raise AutoExecutionError('ownership may have crossed flat; refresh its epoch before emergency reduction')
                broker, complete = self._position_snapshot()
                if not complete:
                    continue
                quantity = reducible_quantity(max(0.0, quantity), broker.get(conid, 0.0))
                if quantity <= 0:
                    continue
                epoch = owned.get('ownership_epoch')
                if epoch is None:
                    self._warn_ownership_pending(strategy, conid, 'legacy holding')
                    continue
                if emergency['attempted'] and emergency.get('ownership_epoch') != epoch:
                    # The scoped lookup above proved this attempt sent no
                    # order. A different holding needs a new physical ID,
                    # even when the retained explicit request still applies.
                    if prior is None or prior.get('orders') is None or len(prior['orders']):
                        continue
                    replacement = dict(emergency, attempted=False,
                                       intent_id=self._emergency_identity(emergency.get('parent_intent_id')))
                    replacement.pop('ownership_epoch', None)
                    replacement.pop('ownership_started_at', None)
                    with self._view_lock:
                        if self._emergency_exits.get((strategy, conid)) is emergency:
                            self._emergency_exits[(strategy, conid)] = replacement
                            pending.append(replacement)
                    continue
                endpoint = getattr(self._get_sdk(), 'emergency_close_position', None)
                if not callable(endpoint):
                    raise AutoExecutionError('SDK has no journal-independent reduce-only endpoint')
                if not emergency['attempted']:
                    emergency['intent_id'] = self._emergency_identity(emergency.get('parent_intent_id'), epoch)
                    emergency.update(ownership_epoch=epoch,
                                     ownership_started_at=owned.get('ownership_started_at'))
                emergency['attempted'] = True
                result = endpoint(con_id=conid, quantity=quantity, strategy_name=strategy,
                                  client_intent_id=emergency['intent_id'])
                self._snapshot_cache = None
                if not result.is_success():
                    logging.error('auto-executor: emergency reduction remains pending: %s', result.error)
            except Exception:
                logging.exception('auto-executor: emergency reduction pending; no local durability claim')

    def _execution_snapshot(self, intent=None):
        sdk = self._get_sdk()
        method = getattr(sdk, 'execution_snapshot', None)
        if callable(method):
            intent_id = self._broker_intent_id(intent) if intent is not None else ''
            ids = (sorted({int(oid) for oid in intent['payload'].get('order_ids', []) if int(oid) > 0})
                   if intent is not None else [])
            # Global completeness cannot establish that every physical leg
            # of this intent is present. The server adds its durable topology
            # to this query; cache only the same scope until the next work
            # cycle or broker mutation invalidates all observations.
            key = (intent_id, tuple(ids))
            if self._snapshot_cache is None:
                self._snapshot_cache = {}
            if key in self._snapshot_cache:
                return self._snapshot_cache[key]
            result = method(intent_id=intent_id, order_ids=ids or None)
            if not isinstance(result, dict):
                raise AutoExecutionError('execution snapshot has no completeness metadata')
            self._snapshot_cache[key] = result
            return result
        # Compatibility for SDK adapters that expose the historical DataFrame
        # interface. Production uses the explicit completeness-bearing RPC.
        orders = sdk.trades()
        if orders is None:
            raise AutoExecutionError('broker orders snapshot unavailable')
        rows = orders.to_dict('records')
        complete = bool(orders.attrs.get('complete', True))
        if intent is not None:
            ids = {int(oid) for oid in intent['payload'].get('order_ids', []) if int(oid) > 0}
            matched = [row for row in rows if self._matches_intent_order(row, self._broker_intent_id(intent), ids)]
            observed = {int(row['orderId']) for row in matched if int(row['orderId']) > 0}
            complete = complete and ids <= observed
        return {'complete': complete, 'orders': rows}

    def _position_snapshot(self):
        sdk = self._get_sdk()
        if callable(getattr(sdk, 'execution_snapshot', None)):
            snapshot = self._execution_snapshot()
            raw = snapshot.get('positions', [])
            rows = raw.to_dict('records') if hasattr(raw, 'to_dict') else raw
            broker = {int(row['conId']): float(row['position']) for row in rows}
            complete = bool(snapshot.get('positions_complete', False))
            return broker, complete and all(math.isfinite(qty) for qty in broker.values())
        frame = sdk.positions()
        broker = ({int(row['conId']): float(row['position']) for _, row in frame.iterrows()}
                  if frame is not None and not frame.empty else {})
        if frame is not None and 'complete' in frame.attrs:
            return broker, bool(frame.attrs['complete']) and all(math.isfinite(qty) for qty in broker.values())
        if broker:
            self._first_empty_broker_read = None
            return broker, all(math.isfinite(qty) for qty in broker.values())
        now = time.time()
        if self._first_empty_broker_read is None:
            self._first_empty_broker_read = now
        return broker, accept_empty_broker_read(self._first_empty_broker_read, now,
                                               self.empty_broker_grace_seconds)

    @staticmethod
    def _broker_intent_id(intent):
        return intent['payload'].get('broker_intent_id') or intent['intent_id']

    @staticmethod
    def _observed_intent_id(row):
        # DataFrame adapters represent an absent string cell as NaN, which
        # is truthy. It is absence, not a different durable string identity.
        return next((value for value in
                          (row.get('clientIntentId'), row.get('client_intent_id'),
                           split_order_reference(row.get('brokerOrderRef'))[1],
                           split_order_reference(row.get('orderRef'))[1])
                          if isinstance(value, str) and value), '')

    @classmethod
    def _matches_intent_order(cls, row, intent_id, order_ids):
        reference = cls._observed_intent_id(row)
        # IB order IDs are scoped to a client and can recur in replay history.
        # An explicit durable reference always wins over a numeric fallback.
        return (reference == intent_id if reference else
                int(row.get('orderId', 0) or 0) in order_ids)

    def _reconcile_intent(self, intent):
        if intent['status'] in ('REJECTED', 'RESOLVED'):
            return
        if self._unsubmitted_close(intent):
            return  # a request has no physical execution until its first submission
        if (intent['kind'] != 'OPEN' and intent['payload'].get('ownership_epoch') is None
                and not intent['payload'].get('attribution_unresolved')):
            # A historical bar label or current broker balance cannot prove
            # which holding a late protective receipt belongs to.
            self.intents.update(intent, status='UNKNOWN', attribution_unresolved=True)
        if intent['payload'].get('attribution_unresolved'):
            if intent['status'] != 'UNKNOWN':
                self.intents.update(intent, status='UNKNOWN')
            self._warn_ownership_pending(intent['strategy'], intent['conid'], intent['intent_id'])
            return
        try:
            snapshot = self._execution_snapshot(intent)
        except Exception as exc:
            logging.warning('auto-executor: intent reconciliation deferred: %s', exc)
            return
        ids = set(intent['payload'].get('order_ids', []))
        rows = snapshot.get('orders', [])
        if hasattr(rows, 'to_dict'):
            rows = rows.to_dict('records')
        matched = [row for row in rows if self._matches_intent_order(row, self._broker_intent_id(intent), ids)
            and int(row.get('conId', 0) or 0) == intent['conid']
            and str(row.get('action', '')).upper() == ('BUY' if intent['kind'] == 'OPEN' else 'SELL')]
        if not matched:
            return
        # Order IDs become durable even if approve's reply was lost.
        # Completed-order history can omit the scoped numeric ID (zero) while
        # preserving permId/reference. It cannot erase an ID already proven
        # for this durable intent, or turn zero into a cancellation target.
        observed_ids = sorted({int(oid) for oid in ids if int(oid) > 0}
                              | {int(row['orderId']) for row in matched if int(row['orderId']) > 0})
        if observed_ids != intent['payload'].get('order_ids'):
            self.intents.update(intent, order_ids=observed_ids)
        ambiguous = any(row.get('identityAmbiguous', False) for row in matched)
        cumulative = 0.0
        quote_notional = 0.0
        cost_evaluable = not ambiguous and snapshot.get('complete', False)
        for row in matched:
            if row.get('identityAmbiguous', False):
                continue  # aliases are not independent quantitative evidence
            value = row.get('filled')
            try:
                fill = float(value)
            except (TypeError, ValueError):
                return  # acceptance/totalQuantity is not an execution
            if not math.isfinite(fill) or fill < 0:
                return
            cumulative += fill
            if fill > 0:
                try:
                    price = float(row.get('avgFillPrice'))
                except (TypeError, ValueError, OverflowError):
                    price = 0.0
                if not math.isfinite(price) or not 0 < price < sys.float_info.max:
                    cost_evaluable = False
                else:
                    quote_notional += fill * price
        if not math.isfinite(quote_notional) or quote_notional <= 0:
            cost_evaluable = False
        statuses = {row.get('status') for row in matched}
        if ambiguous or not snapshot.get('complete', False) or any(row.get('fillQuantityKnown') is False for row in matched):
            # A known partial fill is attributable even when another physical
            # leg is absent from bounded replay. Missing topology is never
            # evidence that the opening reservation has finished.
            status = 'UNKNOWN'
        elif statuses <= {'Filled', 'Cancelled', 'ApiCancelled', 'Inactive'}:
            status = 'FILLED' if statuses == {'Filled'} else 'CANCELLED'
        else:
            status = 'WORKING'
        unchanged = (cumulative == intent['payload'].get('cumulative_filled')
                     and status == intent['status'])
        # Price can arrive after quantity/status, including after Filled.
        # The cost checkpoint is atomic with inventory and is independently
        # replayable without rewriting the intent's existing status.
        delta = self.state.apply_fill(
            intent, cumulative,
            cumulative_quote_notional=quote_notional if cost_evaluable else None)
        if delta and intent['kind'] == 'OPEN':
            self._unpublished_open_fills.add(intent['intent_id'])
        if not unchanged:
            self.intents.update(intent, status=status, cumulative_filled=cumulative)
        if delta:
            self._load_open_view()

    @staticmethod
    def _unsubmitted_close(intent):
        payload = intent['payload']
        # Native WAITING retirement can prove there was never a submission.
        # Missing metadata alone cannot reconstruct that fact for old history.
        return (intent['kind'] == 'CLOSE'
                and (intent['status'] == 'WAITING' or (
                    intent['status'] == 'RESOLVED' and payload.get('never_submitted') is True))
                and not payload.get('proposal_id') and not payload.get('order_ids')
                and not payload.get('submitted_at'))

    def _warn_ownership_pending(self, strategy, conid, identity):
        key = (strategy, conid, identity)
        if key in self._ownership_warnings:
            return
        self._ownership_warnings.add(key)
        logging.warning(
            'auto-executor: ownership proof unavailable for %s/%s (%s); automatic protection '
            'and exit coordination await reconciliation; existing working protection is retained',
            strategy, conid, identity)

    def _reconcile_intents(self, strategy=None, conid=None):
        self._adopt_observed_protectives(strategy, conid)
        # Terminal observations are replayed too: IB can report Cancelled and
        # then a late fill. Atomic checkpoints make both restart and late-fill
        # replay harmless, without deleting broker truth on cancellation.
        try:
            unpriced = self.state.unpriced_open_intents(strategy=strategy, conid=conid)
            self._ownership_warnings.discard(('cost-history-unavailable',))
        except Exception as exc:
            # Optional price recovery cannot block ordinary reconciliation
            # or journal-independent exits during an ownership-store outage.
            unpriced = set()
            key = ('cost-history-unavailable',)
            if key not in self._ownership_warnings:
                self._ownership_warnings.add(key)
                logging.warning('auto-executor: owned fill price recovery deferred: %s', exc)
        for intent in self.intents.all(strategy=strategy, conid=conid):
            if (intent['status'] not in ('REJECTED', 'RESOLVED', 'FILLED')
                    or (intent['kind'] == 'OPEN' and intent['status'] == 'FILLED'
                        and intent['intent_id'] in unpriced)):
                self._reconcile_intent(intent)

    def _adopt_observed_protectives(self, strategy=None, conid=None):
        positions = [row for row in self.state.all_open()
                     if (strategy is None or row[0] == strategy)
                     and (conid is None or int(row[1]) == conid)]
        if not positions:
            return
        try:
            snapshot = self._execution_snapshot()
        except Exception:
            return  # no inference from an unavailable snapshot
        orders = snapshot.get('orders', [])
        if hasattr(orders, 'to_dict'):
            orders = orders.to_dict('records')
        existing = self.intents.all()
        for owner, instrument, _qty, entry, tracked in positions:
            position = self.state.open_position(owner, int(instrument))
            if position is None:
                continue
            for row in orders:
                ref_owner, _encoded_id = split_order_reference(row.get('orderRef'))
                recovered_id = self._observed_intent_id(row)
                if (not isinstance(recovered_id, str) or not recovered_id.startswith('emergency-')
                        or ref_owner != owner or row.get('action') != 'SELL'
                        or int(row.get('conId', 0) or 0) != int(instrument)):
                    continue
                try:
                    created_at = int(recovered_id.split('-')[1], 16) / 1000.0
                    entry_at = dt.datetime.fromisoformat(timestamp_text(entry)).timestamp()
                except (TypeError, ValueError, IndexError):
                    continue
                if created_at < entry_at or any(item['intent_id'] == recovered_id for item in existing):
                    continue
                payload: dict[str, Any] = dict(bar_ts=timestamp_text(entry), reason='recovered emergency exit', emergency=True,
                               quantity=float(row.get('totalQuantity', 0)), order_ids=[int(row['orderId'])],
                               exit_request_active=True, policy_entry_bar_ts=timestamp_text(entry),
                               request_entry_bar_ts=timestamp_text(entry))
                physical_epoch = self._emergency_epoch(recovered_id)
                same_holding = physical_epoch is not None and physical_epoch == position.get('ownership_epoch')
                payload.update(ownership_epoch=physical_epoch,
                               ownership_started_at=(position.get('ownership_started_at')
                                                     if same_holding else None),
                               attribution_unresolved=physical_epoch is None,
                               exit_request_active=same_holding)
                if '-parent-' in recovered_id:
                    digest = recovered_id.rsplit('-parent-', 1)[1].split('-epoch-', 1)[0]
                    parents = [item for item in existing
                               if (item['strategy'], item['conid'], item['kind']) == (owner, int(instrument), 'CLOSE')
                               and hashlib.sha256(item['intent_id'].encode()).hexdigest() == digest]
                    if len(parents) == 1:
                        prior = parents[0]['payload']
                        successor = prior.get('successor_exit') or {}
                        entry_requests = (prior.get('request_entry_bar_ts'),
                                          prior.get('policy_entry_bar_ts'), successor.get('entry_bar_ts'))
                        retained_request = (prior.get('explicit_exit_scope') is not None or any(
                            value is not None and self._entry_matches(value, position) for value in entry_requests))
                        payload.update(parent_intent_id=parents[0]['intent_id'],
                                       explicit_exit_scope=merge_exit_scopes(None, prior.get('explicit_exit_scope')),
                                       policy_entry_bar_ts=prior.get('policy_entry_bar_ts'),
                                       request_entry_bar_ts=prior.get('request_entry_bar_ts'),
                                       successor_exit=prior.get('successor_exit'),
                                       exit_request_active=same_holding or retained_request)
                    else:
                        # Broker execution still needs attribution, but a
                        # missing/ambiguous parent cannot authorize a retry.
                        payload.update(exit_request_active=False, recovery_authority_unknown=True)
                        logging.error('auto-executor: recovered emergency %s has no unique durable parent', recovered_id)
                restored = self.intents.restore_exit(recovered_id, owner, int(instrument), payload)
                existing.append(restored)
            candidates = self._protective_rows(orders, owner, int(instrument))
            candidates.extend(row for row in orders if tracked
                              and int(row.get('orderId', 0) or 0) == int(tracked)
                              and split_order_reference(row.get('orderRef'))[0] == owner and row.get('action') == 'SELL'
                              and int(row.get('conId', 0) or 0) == int(instrument))
            candidates.extend(row for row in orders
                              if self._is_owned_protective(row, owner, int(instrument))
                              and row.get('orderType')
                              and any(row.get(field) in ('Filled', 'Cancelled', 'ApiCancelled', 'Inactive')
                                      for field in ('status', 'brokerStatus')))
            position = self.state.open_position(owner, int(instrument))
            if position is None:
                continue
            epoch = position.get('ownership_epoch')
            origin = position.get('ownership_started_at')
            seen = set()
            for row in candidates:
                oid = int(row['orderId'])
                broker_id = self._observed_intent_id(row)
                physical = (row.get('account'), row.get('clientId'), row.get('permId'), oid, broker_id)
                if physical in seen:
                    continue
                seen.add(physical)
                # A native adopted alias can share its numeric ID with a
                # different physical order. Aliasless legacy checkpoints
                # still pass through the original historical-hash guard.
                matches = [intent for intent in existing
                           if (intent['strategy'], intent['conid'], intent['kind'])
                           == (owner, int(instrument), 'PROTECTIVE')
                           and ((broker_id and self._broker_intent_id(intent) == broker_id)
                                or (oid > 0 and oid in intent['payload'].get('order_ids', [])
                                    and (not broker_id or intent['payload'].get('adopted'))
                                    and (not intent['payload'].get('adopted')
                                         or not intent['payload'].get('broker_intent_id')
                                         or self.intents.protective_identity(
                                             owner, int(instrument), row, intent['payload']['bar_ts'])
                                         == intent['intent_id'])))]
                if len(matches) > 1:
                    raise AutoExecutionError('protective identity matches multiple local intents')
                prior = matches[0] if matches else None
                if prior is not None and not prior['payload'].get('adopted'):
                    continue
                terminal = any(row.get(field) in ('Filled', 'Cancelled', 'ApiCancelled', 'Inactive')
                               for field in ('status', 'brokerStatus'))
                created = row.get('brokerIntentCreatedAt')
                times_known = all(isinstance(value, (int, float)) and not isinstance(value, bool)
                                  and math.isfinite(value) and value > 0 for value in (origin, created))
                current_claim = bool(times_known and created >= origin)
                if prior is None and terminal and times_known and created < origin:
                    continue  # an older claim cannot acquire this holding's ownership
                saved = prior['payload'] if prior else {}
                bound_epoch = saved.get('ownership_epoch')
                bound_origin = saved.get('ownership_started_at')
                if bound_epoch is not None:
                    unresolved = bool(saved.get('attribution_unresolved'))
                    if unresolved and bound_epoch == epoch and current_claim:
                        unresolved = False
                elif prior is not None:
                    # Enrich old adopt-* identities only with both their exact
                    # historical physical hash and current-origin proof. A
                    # reused numeric order ID or bar label is insufficient.
                    proven_legacy = (epoch is not None and current_claim
                                     and self._entry_matches(saved['bar_ts'], position))
                    unresolved = not proven_legacy
                    if proven_legacy:
                        bound_epoch, bound_origin = epoch, origin
                else:
                    bound_epoch, bound_origin = epoch, origin
                    unresolved = epoch is None or not current_claim
                unresolved = unresolved or bool(row.get('identityAmbiguous', False))
                adopted = self.intents.adopt_protective(
                    owner, int(instrument), row, entry or self._now_utc(),
                    broker_intent_id=broker_id, ownership_epoch=bound_epoch,
                    ownership_started_at=bound_origin, attribution_unresolved=unresolved,
                    existing=prior)
                if prior is None:
                    existing.append(adopted)

    @staticmethod
    def _entry_text(value):
        if value is None:
            return None
        return timestamp_text(dt.datetime.fromisoformat(value) if isinstance(value, str) else value)

    @classmethod
    def _entry_matches(cls, expected, position) -> bool:
        return expected is None or (position is not None and
                                    cls._entry_text(expected) == cls._entry_text(position['entry_bar_ts']))

    def _bar_count(self, work: BarWork, *, durable: bool) -> int:
        if work.entry_bar_ts is None or work.observed_bar_timestamps is None:
            return work.bars_held  # compatible direct callers already supply a full count
        args = (work.strategy_name, work.conid, work.entry_bar_ts, work.observed_bar_timestamps)
        if durable:
            try:
                return self.intents.observe_bar_count(*args, staged_progress=work.observed_progress)
            except Exception:
                logging.exception('auto-executor: observed bar progress is volatile; journal unavailable')
        return self.intents.preview_bar_count(*args, remember=durable, staged_progress=work.observed_progress)

    def _process_bar(self, work: BarWork):
        self._snapshot_cache = None
        # Self-heal the disaster stop: positions opened before the feature
        # shipped, whose placement failed, or whose TRACKED stop was cancelled
        # externally (resize re-creates; verify/adopt lives inside
        # _ensure_protective). Called unconditionally — the old caller-side
        # 'only when untracked' guard silently skipped the verify path, which
        # is exactly the state the live resize test left behind.
        self._ensure_protective(work.strategy_name, work.conid)
        state_available = True
        try:
            # Protection repair may discover a later BUY fill. Its new entry
            # epoch and policy must win over work queued before reconciliation.
            pos = self.state.open_position(work.strategy_name, work.conid)
        except Exception:
            state_available = False
            pos = next((row for row in self.managed_positions()
                        if (row['strategy_name'], row['conid']) ==
                        (work.strategy_name, work.conid)), None)
        if not pos or not self._entry_matches(work.entry_bar_ts, pos):
            return
        trigger = check_time_exit(
            work.bar_ts, self._bar_count(work, durable=True), pos['close_by_time'], pos['max_hold_bars'])
        if trigger is None:
            return
        if not state_available:
            self._remember_emergency_exit(work.strategy_name, work.conid, work.bar_ts,
                                          f'time exit: {trigger} (local state unavailable)',
                                          entry_bar_ts=work.entry_bar_ts, capture_explicit=False)
            self._retry_emergency_exits()
            return
        # Dedup guards the double-fire: the close logs a decision for this
        # bar_ts, and executed_for_bar() blocks a second close on the same bar.
        try:
            if self.state.executed_for_bar(work.strategy_name, work.conid, work.bar_ts):
                return
        except Exception:
            # The broker coordinator's intent identity and reduction
            # reservations also protect this degraded path against replay.
            self._remember_emergency_exit(work.strategy_name, work.conid, work.bar_ts,
                                          f'time exit: {trigger} (dedup storage unavailable)',
                                          entry_bar_ts=work.entry_bar_ts, capture_explicit=False)
            self._retry_emergency_exits()
            return
        try:
            self._execute_close(work.strategy_name, work.conid, work.bar_ts,
                                float(pos['quantity']), reason=f'time exit: {trigger}',
                                entry_bar_ts=work.entry_bar_ts, explicit=False)
        except AutoExecutionError as ex:
            logging.error('auto-executor: time-exit close refused for %s conId %s: %s',
                          work.strategy_name, work.conid, ex)
            self.state.log_decision(work.strategy_name, work.conid, work.bar_ts,
                                    'SELL', 'refused', str(ex))

    # -- protective stops -------------------------------------------------------

    def _ensure_protective(self, strategy: str, conid: int):
        if self.protective_stop_pct <= 0:
            return
        try:
            self._reconcile_intents(strategy, conid)
            pos = self.state.open_position(strategy, conid)
            if not pos or self.intents.all(strategy=strategy, conid=conid, kind='CLOSE', active=True):
                return
            snapshot = self._execution_snapshot()
            if not snapshot.get('complete', False):
                return
            rows = snapshot.get('orders', [])
            if hasattr(rows, 'to_dict'):
                rows = rows.to_dict('records')
            own = self._protective_rows(rows, strategy, conid)
            sdk = self._get_sdk()
            plan = self._protective_plan(conid, pos, snapshot)
            if plan is None:
                self._warn_cost_pending(strategy, conid, pos)
                return
            # Pending cancel remains executable. Never remove a reservation
            # until a fresh complete broker observation confirms termination.
            coverage = sum(max(0.0, float(row.get('totalQuantity', 0)) -
                               float(row.get('filled', 0) or 0)) for row in own)
            if own and abs(coverage - plan.quantity) < 1e-9:
                self.state.set_protective(strategy, conid, int(own[0]['orderId']))
                return
            for row in own:
                if not self._cancel_protective(strategy, conid, int(row['orderId']), 'resize protective to actual fills'):
                    return
            # Even an already-terminal probe can reveal a fill newer than
            # the scoped snapshots used above. Reconcile it before sizing
            # replacement protection; broker inventory can include manual
            # shares that are not ours to protect or sell.
            self._snapshot_cache = None
            self._reconcile_intents(strategy, conid)
            unresolved = self.intents.all(strategy=strategy, conid=conid, kind='PROTECTIVE', active=True)
            for prior in unresolved:
                if prior['payload'].get('attribution_unresolved'):
                    return
                ids = prior['payload'].get('order_ids')
                if not ids:
                    return  # placement reply lost; must recover its identity
                if not all(self._order_is_terminal(oid) for oid in ids):
                    return
                # Terminal status alone does not checkpoint its fills.
                # Keep the reservation until scoped replay applies them.
            if unresolved:
                # Terminal probes bypass the cache. Their newly observed
                # fills must also reach attribution before the final plan.
                self._snapshot_cache = None
                self._reconcile_intents(strategy, conid)
            pos = self.state.open_position(strategy, conid)
            if (not pos or self.intents.all(strategy=strategy, conid=conid, kind='CLOSE', active=True)
                    or self.intents.all(strategy=strategy, conid=conid, kind='PROTECTIVE', active=True)):
                return
            snapshot = self._execution_snapshot()
            if not snapshot.get('complete', False):
                return
            rows = snapshot.get('orders', [])
            if hasattr(rows, 'to_dict'):
                rows = rows.to_dict('records')
            if self._protective_rows(rows, strategy, conid):
                return  # a replacement appeared while cancellation was in flight
            plan = self._protective_plan(conid, pos, snapshot)
            if plan is None:
                self._warn_cost_pending(strategy, conid, pos)
                return
            if pos.get('ownership_epoch') is None:
                self._warn_ownership_pending(strategy, conid, 'legacy holding')
                return
            intent = self.intents.create(strategy, conid, 'PROTECTIVE',
                                        dict(bar_ts=timestamp_text(pos['entry_bar_ts']), quantity=plan.quantity,
                                             stop_price=plan.stop_price, ownership_epoch=pos['ownership_epoch'],
                                             ownership_started_at=pos.get('ownership_started_at')), status='SUBMITTING')
            result = sdk.place_protective_order(
                symbol=self._resolve_exact(conid)['symbol'], action='SELL', quantity=plan.quantity,
                order_type='STP', aux_price=plan.stop_price, tif='GTC',
                **{key: self._resolve_exact(conid)[key] for key in ('sec_type', 'exchange', 'currency')},
                con_id=conid, order_ref=strategy, client_intent_id=intent['intent_id'])
            self._snapshot_cache = None
            if not result.is_success():
                error = str(result.error)
                rejected = ('reject' in error.lower() or 'refus' in error.lower()) and not any(
                    word in error.lower() for word in ('unknown', 'timeout', 'connection'))
                self.intents.update(intent, status='REJECTED' if rejected else 'UNKNOWN', error=error)
                return
            order_id = int(getattr(getattr(result.obj, 'order', None), 'orderId', 0) or 0)
            if not order_id:
                self.intents.update(intent, status='UNKNOWN', error='protective placement returned no order ID')
                return
            self.intents.update(intent, status='WORKING', order_ids=[order_id])
            self.state.set_protective(strategy, conid, order_id)
        except Exception:
            logging.exception('auto-executor: protection repair deferred for %s conId %s', strategy, conid)

    def _protective_plan(self, conid, position, snapshot):
        sdk = self._get_sdk()
        if callable(getattr(sdk, 'execution_snapshot', None)):
            if not snapshot.get('positions_complete', False) or snapshot.get('positions') is None:
                return None
            import pandas as pd
            frame = pd.DataFrame(snapshot['positions'])
        else:
            frame = sdk.positions()
            if frame is not None and not frame.attrs.get('complete', True):
                return None
        if frame is None or frame.empty:
            return None
        mine = frame[frame['conId'] == conid]
        if mine.empty:
            return None
        return protective_stop_plan(position['quantity'], mine.iloc[0]['position'],
                                    position.get('avg_cost'), self.protective_stop_pct)

    def _warn_cost_pending(self, strategy, conid, position):
        if position.get('cost_evaluable', False):
            return
        key = ('cost', strategy, conid, position.get('ownership_epoch'))
        if key in self._ownership_warnings:
            return
        self._ownership_warnings.add(key)
        logging.warning(
            'auto-executor: owned fill price unavailable for %s conId %s; '
            'protective repair deferred for holding %s (%s)',
            strategy, conid, position.get('ownership_epoch'),
            position.get('cost_unavailable_reason', 'owned fill cost is unknown'))

    @staticmethod
    def _is_owned_protective(row, strategy, conid):
        return (split_order_reference(row.get('orderRef'))[0] == strategy
                and int(row.get('conId', 0) or 0) == conid
                and str(row.get('action', '')).upper() == 'SELL'
                and str(row.get('orderType', '')).upper() in ('STP', 'STP LMT', 'TRAIL', 'TRAIL LIMIT'))

    @classmethod
    def _protective_rows(cls, rows, strategy, conid):
        return [row for row in rows if cls._is_owned_protective(row, strategy, conid)
                and str(row.get('status', '')) in ('PendingSubmit', 'ApiPending', 'PreSubmitted', 'Submitted', 'PendingCancel')]

    def _own_live_protectives(self, sdk, strategy: str, conid: int) -> list:
        snapshot = self._execution_snapshot()
        if not snapshot.get('complete', False):
            raise AutoExecutionError('orders snapshot incomplete; protective cancellation remains pending')
        rows = snapshot.get('orders', [])
        if hasattr(rows, 'to_dict'):
            rows = rows.to_dict('records')
        return [int(row['orderId']) for row in self._protective_rows(rows, strategy, conid)]

    def _opening_is_broker_terminal(self, intent) -> bool:
        snapshot = self._execution_snapshot(intent)
        if not snapshot.get('complete', False):
            return False
        rows = snapshot.get('orders', [])
        if hasattr(rows, 'to_dict'):
            rows = rows.to_dict('records')
        matched = [row for row in rows
                   if self._matches_intent_order(row, intent['intent_id'], intent['payload'].get('order_ids', []))
                   and int(row.get('conId', 0) or 0) == intent['conid']
                   and str(row.get('action', '')).upper() == 'BUY']
        if any(row.get('identityAmbiguous', False) for row in matched):
            return False
        return bool(matched) and all(
            next((value for value in (row.get('brokerStatus'), row.get('status'))
                  if isinstance(value, str) and value), '') in
            ('Filled', 'Cancelled', 'ApiCancelled', 'Inactive') for row in matched)

    def _order_is_terminal(self, order_id):
        sdk = self._get_sdk()
        method = getattr(sdk, 'execution_snapshot', None)
        explicit_snapshot = callable(method)
        snapshot = (method(order_ids=[int(order_id)]) if explicit_snapshot
                    else self._execution_snapshot())
        if not snapshot.get('complete', False):
            return False
        rows = snapshot.get('orders', [])
        if hasattr(rows, 'to_dict'):
            rows = rows.to_dict('records')
        found = [row for row in rows if int(row.get('orderId', 0) or 0) == int(order_id)]
        if not found:
            # The production protocol must retain a known order's outcome;
            # absence from open orders cannot tell cancelled from filled.
            return not explicit_snapshot
        if any(row.get('identityAmbiguous', False) for row in found):
            return False
        return all(row.get('status') in ('Filled', 'Cancelled', 'ApiCancelled', 'Inactive')
                   for row in found)

    def _cancel_order(self, order_id):
        if self._order_is_terminal(order_id):
            return True
        result = self._get_sdk().cancel(int(order_id))
        self._snapshot_cache = None
        if result is not None and not result.is_success():
            return False
        return self._order_is_terminal(order_id)

    def _cancel_protective(self, strategy: str, conid: int, order_id, context: str):
        try:
            if any(intent['payload'].get('attribution_unresolved')
                   for intent in self.intents.all(strategy=strategy, conid=conid,
                                                  kind='PROTECTIVE', active=True)):
                return False  # retain working protection until its fill ownership is proven
            if not order_id:
                return True
            if not self._cancel_order(order_id):
                logging.warning('auto-executor: protective cancellation remains pending: %s (%s)', order_id, context)
                return False
            self._snapshot_cache = None  # a terminal-only probe can reveal a newer fill too
            for intent in self.intents.all(strategy=strategy, conid=conid, kind='PROTECTIVE', active=True):
                if int(order_id) in intent['payload'].get('order_ids', []):
                    if intent['payload'].get('attribution_unresolved'):
                        return False
                    self._reconcile_intent(intent)
                    if intent['status'] not in ('FILLED', 'CANCELLED'):
                        return False  # terminal probe did not checkpoint the fill
            self.state.set_protective(strategy, conid, None)
            return True
        except Exception:
            logging.exception('auto-executor: protective cancellation uncertain: %s', order_id)
            return False

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
