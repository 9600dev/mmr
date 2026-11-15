from dataclasses import dataclass, field
from enum import Enum
from trader.data.duckdb_store import DuckDBConnection
from typing import Any, Dict, List, Optional

import datetime as dt
import json
import math
import sys


class EventType(str, Enum):
    SIGNAL = 'SIGNAL'
    ORDER_SUBMITTED = 'ORDER_SUBMITTED'
    ORDER_FILLED = 'ORDER_FILLED'
    ORDER_CANCELLED = 'ORDER_CANCELLED'
    ORDER_REJECTED = 'ORDER_REJECTED'
    RISK_GATE_REJECTED = 'RISK_GATE_REJECTED'
    # Operator/automatic release of a fail-closed block (2026-09-14). The
    # durable record lives where the act happened (SQLite reservation
    # settlement table, restore acknowledgment file); these mirror it into the
    # cross-service audit trail so `strategies signals`-style reads see it.
    RESERVATION_SETTLED = 'RESERVATION_SETTLED'
    RESTORE_MARKER_ACKNOWLEDGED = 'RESTORE_MARKER_ACKNOWLEDGED'


@dataclass
class TradingEvent:
    event_type: EventType
    timestamp: dt.datetime
    strategy_name: str
    conid: int = 0
    symbol: str = ''
    action: str = ''
    quantity: float = 0.0
    price: float = 0.0
    order_id: int = 0
    signal_probability: float = 0.0
    signal_risk: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    id: Optional[int] = None


class EventStore:
    _CREATE_TABLE = """
        CREATE TABLE IF NOT EXISTS trading_events (
            id INTEGER PRIMARY KEY,
            event_type VARCHAR NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            strategy_name VARCHAR NOT NULL,
            conid INTEGER DEFAULT 0,
            symbol VARCHAR DEFAULT '',
            action VARCHAR DEFAULT '',
            quantity DOUBLE DEFAULT 0.0,
            price DOUBLE DEFAULT 0.0,
            order_id INTEGER DEFAULT 0,
            signal_probability DOUBLE DEFAULT 0.0,
            signal_risk DOUBLE DEFAULT 0.0,
            metadata VARCHAR DEFAULT '{}'
        )
    """

    _CREATE_SEQUENCE = """
        CREATE SEQUENCE IF NOT EXISTS trading_events_id_seq START 1
    """

    _INSERT = """
        INSERT INTO trading_events
        (id, event_type, timestamp, strategy_name, conid, symbol, action,
         quantity, price, order_id, signal_probability, signal_risk, metadata)
        VALUES (nextval('trading_events_id_seq'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    def __init__(self, duckdb_path: str):
        self.duckdb_path = duckdb_path
        self.db = DuckDBConnection.get_instance(duckdb_path)
        self._ensure_table()

    def _ensure_table(self):
        def _init(conn):
            conn.execute(self._CREATE_TABLE)
            conn.execute(self._CREATE_SEQUENCE)
            conn.execute('CREATE TABLE IF NOT EXISTS trading_event_receipts '
                         '(identity VARCHAR PRIMARY KEY)')
        self.db.execute_atomic(_init)

    def append_once(self, event: TradingEvent, identity: str) -> bool:
        """Commit an event and its durable replay receipt in one transaction.

        The broker outbox may retry after a crash between this commit and its
        acknowledgement.  An in-memory terminal flag cannot close that gap.
        """
        def _append(conn):
            if conn.execute('SELECT 1 FROM trading_event_receipts WHERE identity=?',
                            [identity]).fetchone():
                return False
            conn.execute(self._INSERT, [
                event.event_type.value, event.timestamp, event.strategy_name,
                event.conid, event.symbol, event.action, event.quantity,
                event.price, event.order_id, event.signal_probability,
                event.signal_risk, json.dumps(event.metadata),
            ])
            conn.execute('INSERT INTO trading_event_receipts VALUES (?)', [identity])
            return True
        return self.db.execute_atomic(_append)

    def append(self, event: TradingEvent) -> None:
        self.db.execute(self._INSERT, [
            event.event_type.value,
            event.timestamp,
            event.strategy_name,
            event.conid,
            event.symbol,
            event.action,
            event.quantity,
            event.price,
            event.order_id,
            event.signal_probability,
            event.signal_risk,
            json.dumps(event.metadata),
        ])

    def _rows_to_events(self, rows: list) -> List[TradingEvent]:
        events = []
        for row in rows:
            events.append(TradingEvent(
                id=row[0],
                event_type=EventType(row[1]),
                timestamp=row[2],
                strategy_name=row[3],
                conid=row[4],
                symbol=row[5],
                action=row[6],
                quantity=row[7],
                price=row[8],
                order_id=row[9],
                signal_probability=row[10],
                signal_risk=row[11],
                metadata=json.loads(row[12]) if row[12] else {},
            ))
        return events

    def query_by_strategy(self, strategy_name: str, limit: int = 100) -> List[TradingEvent]:
        rows = self.db.execute(
            "SELECT * FROM trading_events WHERE strategy_name = ? ORDER BY timestamp DESC LIMIT ?",
            [strategy_name, limit],
            fetch='all',
        )
        return self._rows_to_events(rows or [])

    def query_by_conid(self, conid: int, limit: int = 100) -> List[TradingEvent]:
        rows = self.db.execute(
            "SELECT * FROM trading_events WHERE conid = ? ORDER BY timestamp DESC LIMIT ?",
            [conid, limit],
            fetch='all',
        )
        return self._rows_to_events(rows or [])

    def query_signals(self, limit: int = 100) -> List[TradingEvent]:
        rows = self.db.execute(
            "SELECT * FROM trading_events WHERE event_type = ? ORDER BY timestamp DESC LIMIT ?",
            [EventType.SIGNAL.value, limit],
            fetch='all',
        )
        return self._rows_to_events(rows or [])

    def query_all(self, limit: int = 100) -> List[TradingEvent]:
        rows = self.db.execute(
            "SELECT * FROM trading_events ORDER BY timestamp DESC LIMIT ?",
            [limit],
            fetch='all',
        )
        return self._rows_to_events(rows or [])

    def query_by_order_ids(self, order_ids: List[int],
                           since: Optional[dt.datetime] = None) -> List[TradingEvent]:
        """Order lifecycle events for ``order_ids`` (one query), oldest first.

        ``since`` bounds the read so an order id IB reuses after a paper-account
        reset can't attach a stranger's fills to an old proposal.
        """
        ids = sorted({int(o) for o in order_ids if o})
        if not ids:
            return []
        marks = ','.join('?' for _ in ids)
        sql = (f"SELECT * FROM trading_events WHERE order_id IN ({marks}) "
               "AND event_type IN ('ORDER_SUBMITTED','ORDER_FILLED','ORDER_CANCELLED','ORDER_REJECTED')")
        params: list = list(ids)
        if since is not None:
            sql += " AND timestamp >= ?"
            params.append(since)
        rows = self.db.execute(sql + " ORDER BY timestamp, id", params, fetch='all')
        return self._rows_to_events(rows or [])

    def query_since(self, since: dt.datetime, event_type: Optional[EventType] = None) -> List[TradingEvent]:
        if event_type:
            rows = self.db.execute(
                "SELECT * FROM trading_events WHERE timestamp >= ? AND event_type = ? ORDER BY timestamp DESC",
                [since, event_type.value],
                fetch='all',
            )
        else:
            rows = self.db.execute(
                "SELECT * FROM trading_events WHERE timestamp >= ? ORDER BY timestamp DESC",
                [since],
                fetch='all',
            )
        return self._rows_to_events(rows or [])

    def count_since(self, since: dt.datetime, event_type: Optional[EventType] = None,
                    strategy_name: Optional[str] = None) -> int:
        query = "SELECT COUNT(*) FROM trading_events WHERE timestamp >= ?"
        params: list = [since]
        if event_type:
            query += " AND event_type = ?"
            params.append(event_type.value)
        if strategy_name:
            query += " AND strategy_name = ?"
            params.append(strategy_name)
        row = self.db.execute(query, params, fetch='one')
        return row[0] if row else 0

    # Fill tags that are NOT strategy names (manual/CLI order paths). Kept in
    # the aggregation output but excluded from "per-strategy" views by the CLI.
    # orderRef values that do not name a strategy. 'global' is the default
    # algo_name of the direct `mmr buy` / `mmr sell` path — manual orders that
    # used to appear in `strategies pnl` as a strategy called "global".
    NON_STRATEGY_TAGS = ('', 'order', 'proposal', 'to_market', 'global', 'manual')

    def realized_pnl_by_strategy(self) -> dict:
        """Aggregate ORDER_FILLED events into per-strategy realized PnL.

        Requires fills tagged with the strategy name via orderRef (approve
        derives it from proposal.metadata['strategy']); fills recorded before
        tagging shipped carry 'proposal' and land under that key. Pairing
        exploits the auto-executor's long-only invariant — see
        ``pair_fills_long_only``.

        ``closed_trades`` / ``wins`` count EXIT ORDERS, not fill rows: the
        order tracker writes one ORDER_FILLED row per cumulative-fill delta,
        so an exit filled in three partials is three rows and one trade.

        Every figure is GROSS (``pnl_basis: 'gross'``): price x quantity and
        nothing else. ORDER_FILLED events carry no commission (the order
        tracker never reads IB's commission report), so there is nothing here
        to net off; ``commissions_included`` says so rather than leaving the
        numbers to be read as net.
        """
        rows = self.db.execute(
            "SELECT strategy_name, conid, action, quantity, price, timestamp, metadata, order_id "
            "FROM trading_events WHERE event_type = ? ORDER BY timestamp ASC, id ASC",
            [EventType.ORDER_FILLED.value],
            fetch='all',
        )
        # Fills annotated excluded (e.g. executed under a since-fixed bug)
        # must not pollute the live-vs-backtest ledger. Exclusion is
        # all-or-nothing per fill; annotate BOTH sides of a round trip or the
        # survivor shows up as an open lot / unmatched sell.
        kept, excluded = [], 0
        for r in (rows or []):
            try:
                meta = json.loads(r[6]) if r[6] else {}
            except (TypeError, ValueError):
                meta = {}
            if not isinstance(meta, dict):
                meta = {}
            if meta.get('excluded'):
                excluded += 1
            else:
                # Replay can prove filled quantity without a price. Keep that
                # inventory movement in pairing, but never use a placeholder
                # or explicitly unproven price as its cost basis.
                kept.append((*r[:4], fill_price_or_none(
                    r[4], price_evaluable=meta.get('price_evaluable', True)), r[5],
                    fill_order_key(meta, r[7])))
        closed, open_lots, unmatched = pair_fills_long_only(kept)

        today = dt.date.today()
        strategies: Dict[str, dict] = {}

        def _bucket(name: str) -> dict:
            return strategies.setdefault(name, {
                'realized_total': 0.0, 'realized_today': 0.0,
                'closed_trades': 0, 'wins': 0, 'open_lots': [],
                'unevaluable_trades': 0, 'unevaluable_trades_today': 0,
            })

        for trade in closed:
            b = _bucket(trade['strategy'])
            b['closed_trades'] += 1
            closed_at = trade['closed_at']
            closed_today = (closed_at is not None
                            and getattr(closed_at, 'date', None)
                            and closed_at.date() == today)
            if trade['pnl'] is None:
                b['realized_total'] = None
                b['wins'] = None
                b['unevaluable_trades'] += 1
                if closed_today:
                    b['realized_today'] = None
                    b['unevaluable_trades_today'] += 1
            else:
                if b['realized_total'] is not None:
                    b['realized_total'] += trade['pnl']
                if trade['pnl'] > 0 and b['wins'] is not None:
                    b['wins'] += 1
                if closed_today and b['realized_today'] is not None:
                    b['realized_today'] += trade['pnl']
        for lot in open_lots:
            _bucket(lot['strategy'])['open_lots'].append(
                {'conid': lot['conid'], 'quantity': lot['quantity'],
                 'entry_price': lot['entry_price']})

        return {
            'strategies': strategies,
            'closed_trades': closed,
            'unmatched_sells': unmatched,
            'excluded_fills': excluded,
            'unevaluable_trades': sum(
                b['unevaluable_trades'] for b in strategies.values()),
            'pnl_basis': 'gross',
            'commissions_included': False,
        }

    def list_fills(self, strategy: Optional[str] = None,
                   limit: int = 100) -> List[TradingEvent]:
        """ORDER_FILLED events, newest first, with ids — the handles needed
        to annotate/exclude specific fills from the PnL ledger."""
        query = ("SELECT id, event_type, timestamp, strategy_name, conid, symbol, "
                 "action, quantity, price, order_id, signal_probability, "
                 "signal_risk, metadata FROM trading_events WHERE event_type = ?")
        params: list = [EventType.ORDER_FILLED.value]
        if strategy:
            query += " AND strategy_name = ?"
            params.append(strategy)
        query += " ORDER BY timestamp DESC, id DESC LIMIT ?"
        params.append(limit)
        return self._rows_to_events(self.db.execute(query, params, fetch='all') or [])

    def set_fill_exclusion(self, event_ids: List[int], excluded: bool,
                           reason: str = '') -> int:
        """Annotate fills as excluded-from-ledger (or re-include them).

        Metadata merge runs atomically per event under one connection —
        same discipline as ProposalStore.update_metadata. Returns the number
        of events actually updated. Raises ValueError if any id is not an
        ORDER_FILLED event: silently annotating the wrong event type would
        hide the mistake this feature exists to make auditable.
        """
        updated = 0
        for event_id in event_ids:
            def _annotate(conn, _id=event_id):
                nonlocal updated
                row = conn.execute(
                    "SELECT event_type, metadata FROM trading_events WHERE id = ?",
                    [_id]).fetchone()
                if row is None:
                    raise ValueError(f'no trading event with id {_id}')
                if row[0] != EventType.ORDER_FILLED.value:
                    raise ValueError(
                        f'event {_id} is {row[0]!r}, not ORDER_FILLED — '
                        'only fills can be excluded from the PnL ledger')
                try:
                    meta = json.loads(row[1]) if row[1] else {}
                except (TypeError, ValueError):
                    meta = {}
                if excluded:
                    meta['excluded'] = True
                    meta['exclude_reason'] = reason
                else:
                    meta.pop('excluded', None)
                    meta.pop('exclude_reason', None)
                conn.execute("UPDATE trading_events SET metadata = ? WHERE id = ?",
                             [json.dumps(meta), _id])
                updated += 1
            self.db.execute_atomic(_annotate)
        return updated


def fill_price_or_none(price: Any, *, price_evaluable: bool = True) -> Optional[float]:
    """Return a proven positive fill price, excluding IB's unset sentinel.

    Legacy zero prices and replay fills with no price evidence remain
    unknown. Their quantities must still be matched by the ledger.
    """
    if price_evaluable is not True or isinstance(price, bool):
        return None
    try:
        value = float(price)
    except (TypeError, ValueError, OverflowError):
        return None
    return value if math.isfinite(value) and 0 < value < sys.float_info.max else None


def fill_order_key(metadata: Any, order_id: Any) -> Optional[str]:
    """The broker order an ORDER_FILLED row belongs to, or None if unknown.

    The order tracker derives fill rows as cumulative deltas per
    ``broker_identity``, so rows sharing one are by construction partial fills
    of one order. The numeric order id is the fallback for rows without it;
    IB has re-issued ids after a gateway recreate, which is why it is second.
    Zero is "not recorded", never an order.
    """
    identity = metadata.get('broker_identity') if isinstance(metadata, dict) else None
    if isinstance(identity, str) and identity:
        return f'broker:{identity}'
    if isinstance(order_id, int) and not isinstance(order_id, bool) and order_id > 0:
        return f'order:{order_id}'
    return None


def pair_fills_long_only(fills) -> tuple:
    """Pair BUY/SELL fills into round trips under the long-only invariant.

    ``fills``: iterable of (strategy_name, conid, action, quantity, price,
    timestamp[, order_key]) in CHRONOLOGICAL order. A BUY opens/extends the
    (strategy, conid) lot at a volume-weighted entry; a SELL realizes
    ``(exit - entry) x min(sell_qty, lot_qty)``. A SELL with no lot (manual
    interleaving, fills predating attribution) is counted, never guessed at.
    Missing or invalid prices produce ``None`` basis/PnL, but their known
    quantities still consume inventory. An unknown weighted-average basis
    remains unknown until the lot is flat; an unknown exit price does not
    alter the basis of the remaining shares.

    A closed trade is one SELL ORDER against a lot, not one fill row.
    ``order_key`` (see ``fill_order_key``) names the order a row belongs to:
    consecutive SELL rows of one (strategy, conid) that share it are partial
    fills of one exit and are merged into ONE closed trade — quantity and PnL
    summed, exit price volume-weighted, ``closed_at`` the last fill, ``fills``
    the number of rows. If any of them is unpriced the trade's PnL is unknown.
    A BUY in between ends the merge (the cost basis changed). Rows with no
    ``order_key`` cannot be grouped and stay one trade each. The cap on an
    oversized SELL applies to the order as well: once its earlier fills have
    flattened the lot, the rest of the same order is the capped excess and is
    not an unmatched sell.

    Returns ``(closed_trades, open_lots, unmatched_sells)``. Pure function —
    tests drive it with fabricated rows.
    """
    lots: Dict[tuple, list] = {}
    closed: List[dict] = []
    # (strategy, conid) -> (order_key, closed trade) while that lot's most
    # recent fill was a SELL; a BUY clears it.
    last_exit: Dict[tuple, tuple] = {}
    unmatched_sells = 0
    for row in fills:
        strategy, conid, action, quantity, price, ts = row[:6]
        order_key = row[6] if len(row) > 6 else None
        try:
            qty = float(quantity or 0.0)
        except (TypeError, ValueError, OverflowError):
            continue
        if not math.isfinite(qty) or qty <= 0 or isinstance(quantity, bool):
            continue
        px = fill_price_or_none(price)
        key = (strategy, conid)
        side = (action or '').upper()
        if side == 'BUY':
            last_exit.pop(key, None)
            if key in lots:
                held_qty, held_px = lots[key]
                new_qty = held_qty + qty
                basis = (held_qty / new_qty * held_px + qty / new_qty * px
                         if held_px is not None and px is not None else None)
                lots[key] = [new_qty, basis]
            else:
                lots[key] = [qty, px]
        elif side == 'SELL':
            previous = last_exit.get(key)
            same_order = (order_key is not None and previous is not None
                          and previous[0] == order_key)
            if key not in lots or lots[key][0] <= 0:
                if not same_order:
                    unmatched_sells += 1
                continue
            held_qty, held_px = lots[key]
            close_qty = min(qty, held_qty)
            pnl = (px - held_px) * close_qty if px is not None and held_px is not None else None
            if pnl is not None and not math.isfinite(pnl):
                pnl = None
            if same_order and previous is not None:
                trade = previous[1]
                merged_qty = trade['quantity'] + close_qty
                trade['exit_price'] = (
                    (trade['exit_price'] * trade['quantity'] + px * close_qty) / merged_qty
                    if trade['exit_price'] is not None and px is not None else None)
                trade['pnl'] = (trade['pnl'] + pnl
                                if trade['pnl'] is not None and pnl is not None else None)
                trade['quantity'] = merged_qty
                trade['closed_at'] = ts
                trade['fills'] += 1
            else:
                trade = {
                    'strategy': strategy, 'conid': conid, 'quantity': close_qty,
                    'entry_price': held_px, 'exit_price': px,
                    'pnl': pnl, 'closed_at': ts, 'fills': 1,
                }
                closed.append(trade)
            last_exit[key] = (order_key, trade)
            remaining = held_qty - close_qty
            if remaining <= 1e-9:
                del lots[key]
            else:
                lots[key][0] = remaining
    open_lots = [
        {'strategy': k[0], 'conid': k[1], 'quantity': v[0], 'entry_price': v[1]}
        for k, v in lots.items()
    ]
    return closed, open_lots, unmatched_sells
