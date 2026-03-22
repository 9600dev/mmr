"""Position groups — named groups with allocation budgets for organizing thematic trades."""

from dataclasses import dataclass, field
from trader.data.duckdb_store import DuckDBConnection
from typing import Dict, List, Optional

import datetime as dt


@dataclass
class PositionGroup:
    name: str
    description: str = ''
    max_allocation_pct: float = 0.0   # 0 = no budget limit
    max_positions: int = 0            # 0 = no limit
    created_at: Optional[dt.datetime] = None
    updated_at: Optional[dt.datetime] = None
    members: List[str] = field(default_factory=list)


class PositionGroupStore:
    """DuckDB-backed storage for position groups and their members."""

    _CREATE_GROUPS = """
        CREATE TABLE IF NOT EXISTS position_groups (
            name VARCHAR PRIMARY KEY,
            description VARCHAR DEFAULT '',
            max_allocation_pct DOUBLE DEFAULT 0.0,
            max_positions INTEGER DEFAULT 0,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
    """

    _CREATE_MEMBERS = """
        CREATE TABLE IF NOT EXISTS position_group_members (
            group_name VARCHAR NOT NULL,
            symbol VARCHAR NOT NULL,
            added_at TIMESTAMP NOT NULL,
            PRIMARY KEY (group_name, symbol)
        )
    """

    def __init__(self, duckdb_path: str):
        self.duckdb_path = duckdb_path
        self.db = DuckDBConnection.get_instance(duckdb_path)
        self._ensure_tables()

    def _ensure_tables(self):
        self.db.execute_atomic(lambda conn: (
            conn.execute(self._CREATE_GROUPS),
            conn.execute(self._CREATE_MEMBERS),
        ))

    def create_group(self, name: str, description: str = '',
                     max_allocation_pct: float = 0.0,
                     max_positions: int = 0) -> PositionGroup:
        """Create a new group. Raises ValueError if name already exists."""
        now = dt.datetime.now()

        def _insert(conn):
            existing = conn.execute(
                "SELECT name FROM position_groups WHERE name = ?", [name]
            ).fetchone()
            if existing:
                raise ValueError(f"Group '{name}' already exists")
            conn.execute(
                "INSERT INTO position_groups (name, description, max_allocation_pct, "
                "max_positions, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                [name, description, max_allocation_pct, max_positions, now, now]
            )

        self.db.execute_atomic(_insert)
        return PositionGroup(
            name=name, description=description,
            max_allocation_pct=max_allocation_pct,
            max_positions=max_positions,
            created_at=now, updated_at=now,
        )

    def delete_group(self, name: str) -> bool:
        """Delete a group and its members. Returns True if group existed."""
        def _delete(conn):
            row = conn.execute(
                "SELECT name FROM position_groups WHERE name = ?", [name]
            ).fetchone()
            if not row:
                return False
            conn.execute("DELETE FROM position_group_members WHERE group_name = ?", [name])
            conn.execute("DELETE FROM position_groups WHERE name = ?", [name])
            return True

        return self.db.execute_atomic(_delete)

    def get_group(self, name: str) -> Optional[PositionGroup]:
        """Get a group by name, including its members."""
        conn = self.db.execute(
            "SELECT name, description, max_allocation_pct, max_positions, "
            "created_at, updated_at FROM position_groups WHERE name = ?", [name]
        )
        row = conn.fetchone()
        conn.close()
        if not row:
            return None

        members = self.get_members(name)
        return PositionGroup(
            name=row[0], description=row[1],
            max_allocation_pct=row[2], max_positions=row[3],
            created_at=row[4], updated_at=row[5],
            members=members,
        )

    def list_groups(self) -> List[PositionGroup]:
        """List all groups with their members."""
        conn = self.db.execute(
            "SELECT name, description, max_allocation_pct, max_positions, "
            "created_at, updated_at FROM position_groups ORDER BY name"
        )
        rows = conn.fetchall()
        conn.close()

        all_memberships = self.get_all_memberships()
        groups = []
        for row in rows:
            groups.append(PositionGroup(
                name=row[0], description=row[1],
                max_allocation_pct=row[2], max_positions=row[3],
                created_at=row[4], updated_at=row[5],
                members=all_memberships.get(row[0], []),
            ))
        return groups

    def update_group(self, name: str, description: Optional[str] = None,
                     max_allocation_pct: Optional[float] = None,
                     max_positions: Optional[int] = None) -> bool:
        """Update group fields. Returns False if group not found."""
        now = dt.datetime.now()
        sets = ["updated_at = ?"]
        params: list = [now]

        if description is not None:
            sets.append("description = ?")
            params.append(description)
        if max_allocation_pct is not None:
            sets.append("max_allocation_pct = ?")
            params.append(max_allocation_pct)
        if max_positions is not None:
            sets.append("max_positions = ?")
            params.append(max_positions)

        params.append(name)
        query = f"UPDATE position_groups SET {', '.join(sets)} WHERE name = ?"
        conn = self.db.execute(query, params)
        conn.close()

        # Verify update happened
        group = self.get_group(name)
        return group is not None

    def add_member(self, group_name: str, symbol: str) -> bool:
        """Add a symbol to a group. Returns False if group doesn't exist or symbol already in group."""
        now = dt.datetime.now()

        def _add(conn):
            group = conn.execute(
                "SELECT name FROM position_groups WHERE name = ?", [group_name]
            ).fetchone()
            if not group:
                return False
            existing = conn.execute(
                "SELECT symbol FROM position_group_members "
                "WHERE group_name = ? AND symbol = ?", [group_name, symbol]
            ).fetchone()
            if existing:
                return False
            conn.execute(
                "INSERT INTO position_group_members (group_name, symbol, added_at) "
                "VALUES (?, ?, ?)", [group_name, symbol, now]
            )
            conn.execute(
                "UPDATE position_groups SET updated_at = ? WHERE name = ?",
                [now, group_name]
            )
            return True

        return self.db.execute_atomic(_add)

    def remove_member(self, group_name: str, symbol: str) -> bool:
        """Remove a symbol from a group. Returns True if it was removed."""
        def _remove(conn):
            existing = conn.execute(
                "SELECT symbol FROM position_group_members "
                "WHERE group_name = ? AND symbol = ?", [group_name, symbol]
            ).fetchone()
            if not existing:
                return False
            conn.execute(
                "DELETE FROM position_group_members "
                "WHERE group_name = ? AND symbol = ?", [group_name, symbol]
            )
            conn.execute(
                "UPDATE position_groups SET updated_at = ? WHERE name = ?",
                [dt.datetime.now(), group_name]
            )
            return True

        return self.db.execute_atomic(_remove)

    def get_members(self, group_name: str) -> List[str]:
        """Get all symbols in a group."""
        conn = self.db.execute(
            "SELECT symbol FROM position_group_members "
            "WHERE group_name = ? ORDER BY symbol", [group_name]
        )
        rows = conn.fetchall()
        conn.close()
        return [row[0] for row in rows]

    def get_groups_for_symbol(self, symbol: str) -> List[str]:
        """Get all group names that contain a symbol."""
        conn = self.db.execute(
            "SELECT group_name FROM position_group_members "
            "WHERE symbol = ? ORDER BY group_name", [symbol]
        )
        rows = conn.fetchall()
        conn.close()
        return [row[0] for row in rows]

    def get_all_memberships(self) -> Dict[str, List[str]]:
        """Get all group→members mappings."""
        conn = self.db.execute(
            "SELECT group_name, symbol FROM position_group_members ORDER BY group_name, symbol"
        )
        rows = conn.fetchall()
        conn.close()
        memberships: Dict[str, List[str]] = {}
        for group_name, symbol in rows:
            memberships.setdefault(group_name, []).append(symbol)
        return memberships
