from dataclasses import dataclass, field
from enum import Enum
from trader.data.duckdb_store import DuckDBConnection
from typing import Any, Dict, List, Optional

import datetime as dt
import json


class EventType(str, Enum):
    SIGNAL = 'SIGNAL'
    ORDER_SUBMITTED = 'ORDER_SUBMITTED'
    ORDER_FILLED = 'ORDER_FILLED'
    ORDER_CANCELLED = 'ORDER_CANCELLED'
    ORDER_REJECTED = 'ORDER_REJECTED'
    RISK_GATE_REJECTED = 'RISK_GATE_REJECTED'


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
        self.db.execute_atomic(_init)

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
