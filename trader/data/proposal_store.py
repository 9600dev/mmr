from trader.data.duckdb_store import DuckDBConnection
from trader.data.proposal_transitions import (
    _ALLOWED_TRANSITIONS,
    _TERMINAL,
    is_known_status,
    is_valid_transition,
)
from trader.trading.proposal import ExecutionSpec, ProposalStatus, TradeProposal
from typing import List, Optional

import datetime as dt
import json


# The proposal state machine (transition table + pure validity predicates) lives
# in trader.data.proposal_transitions — a dependency-light module CrossHair can
# import and symbolically verify without the duckdb/dill import chain. It is
# re-exported here so this module's public surface is unchanged:
#   PENDING  → APPROVED | REJECTED | EXPIRED | FAILED
#   APPROVED → EXECUTED | FAILED | REJECTED
# Terminal states admit NO further transitions and their metadata is immutable.
__all__ = [
    'ProposalStore',
    'InvalidProposalTransition',
    'is_known_status',
    'is_valid_transition',
]


class InvalidProposalTransition(ValueError):
    """Raised when update_status is called with an illegal state transition."""


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
        def _init(conn):
            conn.execute(self._CREATE_TABLE)
            conn.execute(self._CREATE_SEQUENCE)
        self.db.execute_atomic(_init)

    def add(self, proposal: TradeProposal) -> int:
        """Add a proposal and return its assigned id.

        New proposals must be born PENDING — every other status is only
        reachable through the state machine (update_status / try_transition).
        Accepting an arbitrary initial status would let a caller mint an
        already-APPROVED (or worse, EXECUTED) row that never passed through
        the CAS claim that prevents double-execution.
        """
        if proposal.status != ProposalStatus.PENDING.value:
            raise InvalidProposalTransition(
                f'new proposals must have status PENDING, got {proposal.status!r} — '
                f'other states are only reachable via update_status/try_transition'
            )
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
        """Merge extra keys into an existing proposal's metadata. Atomic under the row lock.

        Terminal rows (EXECUTED / REJECTED / EXPIRED / FAILED) are immutable —
        including their metadata. No production path writes metadata after a
        terminal transition (the propose() enrichment writes all happen while
        the row is PENDING), so a post-terminal metadata write is a bug or a
        tamper, and mutating the audit record of a completed trade is exactly
        what terminal immutability exists to prevent. Raises
        InvalidProposalTransition on a terminal row. A missing row is a no-op
        (metadata enrichment is best-effort at the call sites).
        """
        now = dt.datetime.now()

        def _merge(conn):
            row = conn.execute(
                "SELECT metadata, status FROM trade_proposals WHERE id = ?",
                [proposal_id],
            ).fetchone()
            if not row:
                return
            if row[1] in _TERMINAL:
                raise InvalidProposalTransition(
                    f'proposal {proposal_id} is {row[1]!r} (terminal) — '
                    f'metadata is immutable on terminal proposals'
                )
            existing = json.loads(row[0]) if row[0] else {}
            merged = {**existing, **extra}
            conn.execute(
                "UPDATE trade_proposals SET metadata = ?, updated_at = ? WHERE id = ?",
                [json.dumps(merged), now, proposal_id],
            )

        self.db.execute_atomic(_merge)

    def update_status(self, id: int, status: str, **kwargs) -> None:
        """Update proposal status and optional fields (order_ids, rejection_reason).

        Validates the status transition against the proposal state machine:
        PENDING  → APPROVED | REJECTED | EXPIRED | FAILED
        APPROVED → EXECUTED | FAILED | REJECTED
        EXECUTED | REJECTED | EXPIRED | FAILED → (terminal, no transitions)

        Raises InvalidProposalTransition if the transition is illegal.
        Idempotent updates to the same terminal state are rejected too — callers
        that genuinely want to re-mark a terminal proposal must explicitly delete
        and recreate it.
        """
        if not is_known_status(status):
            raise InvalidProposalTransition(
                f'unknown proposal status: {status!r}'
            )

        now = dt.datetime.now()

        def _update(conn):
            row = conn.execute(
                "SELECT status FROM trade_proposals WHERE id = ?",
                [id],
            ).fetchone()
            if not row:
                raise InvalidProposalTransition(
                    f'proposal {id} not found'
                )
            current = row[0]
            if current == status:
                # Per the docstring: same-status "transitions" are rejected, not
                # silently absorbed. Silently passing here let a second concurrent
                # approver re-mark PENDING→APPROVED as success and double-execute.
                raise InvalidProposalTransition(
                    f'proposal {id} is already {current!r}; refusing no-op transition'
                )
            if not is_valid_transition(current, status):
                raise InvalidProposalTransition(
                    f'cannot transition proposal {id} from {current!r} to {status!r}'
                )

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
            conn.execute(query, params)

        self.db.execute_atomic(_update)

    def try_transition(self, id: int, from_status: str, to_status: str, **kwargs) -> bool:
        """Atomically move a proposal from `from_status` to `to_status`.

        Compare-and-swap: the UPDATE only matches a row still in `from_status`,
        so exactly one of N concurrent callers wins. Returns True if this caller
        performed the transition, False if the row was not in `from_status`
        (already claimed, terminal, or missing). Use this — not get()+update_status
        — whenever the transition guards a side effect like placing a live order.
        """
        if not is_known_status(to_status):
            raise InvalidProposalTransition(f'unknown proposal status: {to_status!r}')
        if not is_valid_transition(from_status, to_status):
            raise InvalidProposalTransition(
                f'cannot transition proposal {id} from {from_status!r} to {to_status!r}'
            )

        now = dt.datetime.now()

        def _cas(conn):
            sets = ["status = ?", "updated_at = ?"]
            params: list = [to_status, now]
            if 'order_ids' in kwargs:
                sets.append("order_ids = ?")
                params.append(json.dumps(kwargs['order_ids']))
            if 'rejection_reason' in kwargs:
                sets.append("rejection_reason = ?")
                params.append(kwargs['rejection_reason'])
            params.extend([id, from_status])
            query = (
                f"UPDATE trade_proposals SET {', '.join(sets)} "
                f"WHERE id = ? AND status = ? RETURNING id"
            )
            return conn.execute(query, params).fetchall()

        rows = self.db.execute_atomic(_cas)
        return bool(rows)

    def get(self, id: int) -> Optional[TradeProposal]:
        rows = self.db.execute(
            "SELECT * FROM trade_proposals WHERE id = ?", [id], fetch='all',
        )
        proposals = self._rows_to_proposals(rows or [])
        return proposals[0] if proposals else None

    def query(self, status: Optional[str] = None, limit: int = 50) -> List[TradeProposal]:
        if status:
            rows = self.db.execute(
                "SELECT * FROM trade_proposals WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                [status, limit],
                fetch='all',
            )
        else:
            rows = self.db.execute(
                "SELECT * FROM trade_proposals ORDER BY created_at DESC LIMIT ?",
                [limit],
                fetch='all',
            )
        return self._rows_to_proposals(rows or [])

    def delete(self, id: int, force: bool = False) -> bool:
        """Delete a proposal by id. Returns True if the row no longer exists.

        Terminal rows (EXECUTED / REJECTED / EXPIRED / FAILED) are the audit
        trail of what was (or wasn't) traded — deleting one requires an
        explicit ``force=True``; without it the call raises
        InvalidProposalTransition. Non-terminal rows delete freely, and a
        missing id is a successful no-op.
        """
        def _delete(conn):
            row = conn.execute(
                "SELECT status FROM trade_proposals WHERE id = ?", [id]
            ).fetchone()
            if row and row[0] in _TERMINAL and not force:
                raise InvalidProposalTransition(
                    f'proposal {id} is {row[0]!r} (terminal) — deleting audit-trail '
                    f'rows requires force=True'
                )
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
