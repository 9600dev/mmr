from trader.data.duckdb_store import DuckDBConnection
from trader.trading.proposal import ExecutionSpec, ProposalStatus, TradeProposal
from typing import List, Optional

import datetime as dt
import json


class ProposalStore:
    _CREATE_TABLE = """
        CREATE TABLE IF NOT EXISTS trade_proposals (
            id INTEGER PRIMARY KEY,
            symbol VARCHAR NOT NULL,
            action VARCHAR NOT NULL,
            quantity DOUBLE,
            amount DOUBLE,
            execution VARCHAR DEFAULT '{}',
            reasoning VARCHAR DEFAULT '',
            confidence DOUBLE DEFAULT 0.0,
            thesis VARCHAR DEFAULT '',
            source VARCHAR DEFAULT 'manual',
            metadata VARCHAR DEFAULT '{}',
            status VARCHAR DEFAULT 'PENDING',
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            order_ids VARCHAR DEFAULT '[]',
            rejection_reason VARCHAR DEFAULT '',
            sec_type VARCHAR DEFAULT 'STK'
        )
    """

    _CREATE_SEQUENCE = """
        CREATE SEQUENCE IF NOT EXISTS trade_proposals_id_seq START 1
    """

    _INSERT = """
        INSERT INTO trade_proposals
        (id, symbol, action, quantity, amount, execution, reasoning, confidence,
         thesis, source, metadata, status, created_at, updated_at, order_ids,
         rejection_reason, sec_type)
        VALUES (nextval('trade_proposals_id_seq'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    def __init__(self, duckdb_path: str):
        self.duckdb_path = duckdb_path
        self.db = DuckDBConnection.get_instance(duckdb_path)
        self._ensure_table()

    def _ensure_table(self):
        self.db.execute_atomic(lambda conn: (
            conn.execute(self._CREATE_TABLE),
            conn.execute(self._CREATE_SEQUENCE),
        ))

    def add(self, proposal: TradeProposal) -> int:
        """Add a proposal and return its assigned id."""
        now = dt.datetime.now()
        execution_json = json.dumps(proposal.execution.to_dict())
        # Persist exchange/currency hints in metadata (avoids DB schema migration)
        meta = dict(proposal.metadata)
        if proposal.exchange:
            meta['_exchange'] = proposal.exchange
        if proposal.currency:
            meta['_currency'] = proposal.currency
        if proposal.group:
            meta['_group'] = proposal.group
        metadata_json = json.dumps(meta)
        order_ids_json = json.dumps(proposal.order_ids)

        def _insert(conn):
            conn.execute(self._INSERT, [
                proposal.symbol,
                proposal.action,
                proposal.quantity,
                proposal.amount,
                execution_json,
                proposal.reasoning,
                proposal.confidence,
                proposal.thesis,
                proposal.source,
                metadata_json,
                proposal.status,
                now,
                now,
                order_ids_json,
                proposal.rejection_reason,
                proposal.sec_type,
            ])
            result = conn.execute("SELECT currval('trade_proposals_id_seq')").fetchone()
            return result[0]

        return self.db.execute_atomic(_insert)

    def update_metadata(self, proposal_id: int, extra: dict) -> None:
        """Merge extra keys into an existing proposal's metadata."""
        proposal = self.get(proposal_id)
        if not proposal:
            return
        merged = {**proposal.metadata, **extra}
        now = dt.datetime.now()
        conn = self.db.execute(
            "UPDATE trade_proposals SET metadata = ?, updated_at = ? WHERE id = ?",
            [json.dumps(merged), now, proposal_id]
        )
        conn.close()

    def update_status(self, id: int, status: str, **kwargs) -> None:
        """Update proposal status and optional fields (order_ids, rejection_reason)."""
        now = dt.datetime.now()
        sets = ["status = ?", "updated_at = ?"]
        params: list = [status, now]

        if 'order_ids' in kwargs:
            sets.append("order_ids = ?")
            params.append(json.dumps(kwargs['order_ids']))

        if 'rejection_reason' in kwargs:
            sets.append("rejection_reason = ?")
            params.append(kwargs['rejection_reason'])

        params.append(id)
        query = f"UPDATE trade_proposals SET {', '.join(sets)} WHERE id = ?"
        conn = self.db.execute(query, params)
        conn.close()

    def get(self, id: int) -> Optional[TradeProposal]:
        """Get a proposal by id."""
        conn = self.db.execute(
            "SELECT * FROM trade_proposals WHERE id = ?", [id]
        )
        rows = conn.fetchall()
        conn.close()
        proposals = self._rows_to_proposals(rows)
        return proposals[0] if proposals else None

    def query(self, status: Optional[str] = None, limit: int = 50) -> List[TradeProposal]:
        """Query proposals, optionally filtered by status."""
        if status:
            conn = self.db.execute(
                "SELECT * FROM trade_proposals WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                [status, limit]
            )
        else:
            conn = self.db.execute(
                "SELECT * FROM trade_proposals ORDER BY created_at DESC LIMIT ?",
                [limit]
            )
        rows = conn.fetchall()
        conn.close()
        return self._rows_to_proposals(rows)

    def delete(self, id: int) -> bool:
        """Delete a proposal by id. Returns True if a row was deleted."""
        def _delete(conn):
            conn.execute("DELETE FROM trade_proposals WHERE id = ?", [id])
            result = conn.execute(
                "SELECT COUNT(*) FROM trade_proposals WHERE id = ?", [id]
            ).fetchone()
            return result[0] == 0

        return self.db.execute_atomic(_delete)

    def _rows_to_proposals(self, rows: list) -> List[TradeProposal]:
        proposals = []
        for row in rows:
            execution_dict = json.loads(row[5]) if row[5] else {}
            metadata_dict = json.loads(row[10]) if row[10] else {}
            order_ids_list = json.loads(row[14]) if row[14] else []

            # Restore exchange/currency/group from metadata (stored with _ prefix)
            exchange = metadata_dict.pop('_exchange', '')
            currency = metadata_dict.pop('_currency', '')
            group = metadata_dict.pop('_group', '')

            proposals.append(TradeProposal(
                id=row[0],
                symbol=row[1],
                action=row[2],
                quantity=row[3],
                amount=row[4],
                execution=ExecutionSpec.from_dict(execution_dict),
                reasoning=row[6],
                confidence=row[7],
                thesis=row[8],
                source=row[9],
                metadata=metadata_dict,
                status=row[11],
                created_at=row[12],
                updated_at=row[13],
                order_ids=order_ids_list,
                rejection_reason=row[15],
                sec_type=row[16],
                exchange=exchange,
                currency=currency,
                group=group,
            ))
        return proposals
