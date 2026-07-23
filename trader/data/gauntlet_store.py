"""Gauntlet-run storage — DuckDB-backed record of strategy gauntlet verdicts.

The gauntlet ("no hash, no live") is a machine gate: a strategy source file
must have a PASS record for its EXACT SHA-256 source hash before it can be
deployed or enabled. This store is the gate's memory. Edit one byte of the
strategy and the hash — and therefore the PASS — no longer applies.

Rows are append-only history (one per ``mmr strategies gauntlet`` run), so
"has this exact code ever passed?" and "what did the checks say last time?"
are both answerable after the fact.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import datetime as dt
import json

from trader.data.duckdb_store import DuckDBConnection


@dataclass
class GauntletRecord:
    strategy_name: str
    module_path: str
    class_name: str
    code_hash: str
    verdict: str                                  # 'PASS' | 'FAIL'
    checks: Dict[str, Any] = field(default_factory=dict)  # per-stage tri-state + details
    notes: str = ''
    id: Optional[int] = None
    created: Optional[dt.datetime] = None


class GauntletStore:
    _CREATE_TABLE = """
        CREATE TABLE IF NOT EXISTS gauntlet_runs (
            id INTEGER PRIMARY KEY,
            created TIMESTAMP NOT NULL,
            strategy_name VARCHAR DEFAULT '',
            module_path VARCHAR NOT NULL,
            class_name VARCHAR NOT NULL,
            code_hash VARCHAR NOT NULL,
            verdict VARCHAR NOT NULL,
            checks_json VARCHAR DEFAULT '{}',
            notes VARCHAR DEFAULT ''
        )
    """

    _CREATE_SEQUENCE = """
        CREATE SEQUENCE IF NOT EXISTS gauntlet_runs_id_seq START 1
    """

    _INSERT = """
        INSERT INTO gauntlet_runs (
            id, created, strategy_name, module_path, class_name,
            code_hash, verdict, checks_json, notes
        )
        VALUES (nextval('gauntlet_runs_id_seq'), ?, ?, ?, ?, ?, ?, ?, ?)
    """

    _SELECT_COLUMNS = (
        'id', 'created', 'strategy_name', 'module_path', 'class_name',
        'code_hash', 'verdict', 'checks_json', 'notes',
    )
    _SELECT_FIELDS = ', '.join(_SELECT_COLUMNS)

    def __init__(self, duckdb_path: str):
        self.duckdb_path = duckdb_path
        self.db = DuckDBConnection.get_instance(duckdb_path)
        self._ensure_table()

    def _ensure_table(self):
        def _init(conn):
            conn.execute(self._CREATE_TABLE)
            conn.execute(self._CREATE_SEQUENCE)
        self.db.execute_atomic(_init)

    def record(self, record: GauntletRecord) -> int:
        """Persist a gauntlet run and return its assigned id.

        An empty ``code_hash`` is refused: the whole point of the gate is
        keying on the exact source bytes, and an empty hash means the file
        couldn't be read — recording it would grant a PASS to *nothing in
        particular*.
        """
        if not record.code_hash:
            raise ValueError(
                f'refusing to record gauntlet run for {record.module_path!r}: '
                f'empty code_hash (source file unreadable?)'
            )
        if record.verdict not in ('PASS', 'FAIL'):
            raise ValueError(
                f"gauntlet verdict must be 'PASS' or 'FAIL', got {record.verdict!r}"
            )
        now = dt.datetime.now()

        def _insert(conn):
            conn.execute(self._INSERT, [
                now,
                record.strategy_name,
                record.module_path,
                record.class_name,
                record.code_hash,
                record.verdict,
                json.dumps(record.checks, default=str),
                record.notes,
            ])
            row = conn.execute("SELECT currval('gauntlet_runs_id_seq')").fetchone()
            return row[0]

        return self.db.execute_atomic(_insert)

    def latest_for_hash(self, code_hash: str,
                        class_name: Optional[str] = None) -> Optional[GauntletRecord]:
        """Most recent gauntlet run for this exact source hash, or None.

        Pass ``class_name`` to additionally scope the lookup to one class:
        a gauntlet run authorizes only the exact class that was run, so a
        second, untested class living in the same source file does not
        inherit its sibling's verdict. Legacy rows whose ``class_name``
        differs (or is NULL) simply won't match an equality filter — they
        require a re-run rather than being honoured for the wrong class
        (fail-closed)."""
        if not code_hash:
            return None
        if class_name is None:
            where, params = "code_hash = ?", [code_hash]
        else:
            where, params = "code_hash = ? AND class_name = ?", [code_hash, class_name]
        rows = self.db.execute(
            f"SELECT {self._SELECT_FIELDS} FROM gauntlet_runs "
            f"WHERE {where} ORDER BY created DESC, id DESC LIMIT 1",
            params, fetch='all',
        ) or []
        records = self._rows_to_records(rows)
        return records[0] if records else None

    def has_pass(self, code_hash: str, class_name: Optional[str] = None) -> bool:
        """True iff this exact source hash has ever recorded a PASS.

        When ``class_name`` is given, the PASS must belong to that exact
        class — a PASS for a sibling class in the same file does NOT
        satisfy the gate (see ``latest_for_hash`` for the legacy-row
        fail-closed note)."""
        if not code_hash:
            return False
        if class_name is None:
            where, params = "code_hash = ?", [code_hash]
        else:
            where, params = "code_hash = ? AND class_name = ?", [code_hash, class_name]
        row = self.db.execute(
            f"SELECT COUNT(*) FROM gauntlet_runs WHERE {where} AND verdict = 'PASS'",
            params, fetch='one',
        )
        return bool(row and row[0])

    def latest_pass_for_class(self, class_name: str) -> Optional[GauntletRecord]:
        """Most recent PASS for a class name regardless of source hash —
        used only to enrich warnings ("current hash X, last PASS was hash
        Y") when the current file has no PASS of its own."""
        rows = self.db.execute(
            f"SELECT {self._SELECT_FIELDS} FROM gauntlet_runs "
            f"WHERE class_name = ? AND verdict = 'PASS' "
            f"ORDER BY created DESC, id DESC LIMIT 1",
            [class_name], fetch='all',
        ) or []
        records = self._rows_to_records(rows)
        return records[0] if records else None

    def _rows_to_records(self, rows) -> List[GauntletRecord]:
        records = []
        for row in rows:
            try:
                checks = json.loads(row[7]) if row[7] else {}
            except (ValueError, TypeError):
                checks = {}
            records.append(GauntletRecord(
                id=row[0],
                created=row[1],
                strategy_name=row[2],
                module_path=row[3],
                class_name=row[4],
                code_hash=row[5],
                verdict=row[6],
                checks=checks,
                notes=row[8],
            ))
        return records
