"""Durable server intent claims and broker IDs, before any broker submission."""
from __future__ import annotations

import json
import math
import sys
import time

from trader.data.execution_journal import ExecutionJournal


def _reservation_metadata(conid, action, quantity, is_exit, broker_reference):
    if conid is None and action is None and quantity is None and is_exit is None:
        return None
    if (not isinstance(conid, int) or isinstance(conid, bool) or conid <= 0
            or action not in {'BUY', 'SELL'} or not isinstance(is_exit, bool)
            or not isinstance(quantity, (float, int)) or isinstance(quantity, bool)
            or not math.isfinite(quantity) or not 0 < quantity < sys.float_info.max
            or not isinstance(broker_reference, str) or not broker_reference):
        raise ValueError('reservation requires exact conId, action and finite positive quantity')
    return {'conid': conid, 'action': action, 'quantity': float(quantity),
            'is_exit': int(is_exit), 'broker_reference': broker_reference}


class EmergencyOrderJournal:
    """Process-local exit recovery when storage is unavailable.

    This deliberately cannot authorize opens or claim crash durability. Broker
    orderRef carries the same intent ID so reconciliation can recover after a
    process loss while the durable journal is unavailable.
    """
    def __init__(self):
        self.rows: dict[str, dict] = {}
        self._reservations: dict[tuple, dict] = {}

    def claim(self, intent_id: str, fingerprint: str, account: str) -> bool:
        row = self.rows.get(intent_id)
        if row is not None:
            if row['fingerprint'] != fingerprint or row['account'] != account:
                raise ValueError('intent ID reused with a different account or payload')
            if row['status'] == 'RETRYABLE' and not row['orders']:
                row['status'] = 'CLAIMED'
                return True
            return False
        self.rows[intent_id] = dict(fingerprint=fingerprint, account=account,
                                   status='CLAIMED', orders=[], error='', outcome=None)
        return True

    def get(self, intent_id: str) -> dict | None:
        return self.rows.get(intent_id)

    def reserve_order(self, intent_id: str, order_id: int, client_id: int, *,
                      conid=None, action=None, quantity=None, is_exit=None, broker_reference=None) -> None:
        row = self.rows[intent_id]
        metadata = _reservation_metadata(conid, action, quantity, is_exit, broker_reference or intent_id)
        key = (intent_id, client_id, order_id)
        prior = self._reservations.get(key)
        if prior is not None and metadata is not None and any(prior[name] != value for name, value in metadata.items()):
            raise ValueError('physical reservation metadata is immutable')
        if prior is None:
            self._reservations[key] = dict(intent_id=intent_id, client_id=client_id,
                                          order_id=order_id, settled=0, **(metadata or {
                                              'conid': None, 'action': None, 'quantity': None,
                                              'is_exit': None, 'broker_reference': None}))
        identity = dict(orderId=order_id, clientId=client_id)
        if identity not in row['orders']:
            row['orders'].append(identity)
        row['status'] = 'SUBMITTING'

    def finish(self, intent_id: str, status: str, error: str = '', outcome=None) -> None:
        self.rows[intent_id].update(status=status, error=error, outcome=outcome)

    def discard_order(self, intent_id: str, order_id: int, client_id: int) -> None:
        row = self.rows[intent_id]
        row['orders'] = [order for order in row['orders']
                         if order != {'orderId': order_id, 'clientId': client_id}]
        self._reservations.pop((intent_id, client_id, order_id), None)

    def reservations(self, account: str) -> list[dict]:
        return [dict(item, account=account, leg_count=len(self.rows[item['intent_id']]['orders']))
                for item in self._reservations.values()
                if not item['settled'] and self.rows[item['intent_id']]['account'] == account]

    def settle_reservations(self, keys: list[tuple]) -> None:
        for key in keys:
            if key in self._reservations:
                self._reservations[key]['settled'] = 1


class ServerOrderJournal:
    def __init__(self, database_path: str):
        self.journal = ExecutionJournal(database_path)
        with self.journal.transaction() as conn:
            conn.execute('''CREATE TABLE IF NOT EXISTS server_order_intents (
                intent_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
                account TEXT NOT NULL, status TEXT NOT NULL,
                orders TEXT NOT NULL DEFAULT '[]', error TEXT NOT NULL DEFAULT '',
                outcome TEXT, created_at REAL)''')
            columns = {row['name'] for row in conn.execute('PRAGMA table_info(server_order_intents)')}
            if 'outcome' not in columns:
                conn.execute('ALTER TABLE server_order_intents ADD COLUMN outcome TEXT')
            if 'created_at' not in columns:
                # Historical claims have no trustworthy creation checkpoint.
                # A migration/replay timestamp would grant false provenance.
                conn.execute('ALTER TABLE server_order_intents ADD COLUMN created_at REAL')
            existed = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='server_order_reservations'").fetchone()
            conn.execute('''CREATE TABLE IF NOT EXISTS server_order_reservations (
                intent_id TEXT NOT NULL, client_id INTEGER NOT NULL, order_id INTEGER NOT NULL,
                conid INTEGER, action TEXT, quantity REAL, is_exit INTEGER, broker_reference TEXT,
                settled INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(intent_id,client_id,order_id))''')
            reservation_columns = {row['name'] for row in conn.execute('PRAGMA table_info(server_order_reservations)')}
            if 'broker_reference' not in reservation_columns:
                conn.execute('ALTER TABLE server_order_reservations ADD COLUMN broker_reference TEXT')
            conn.execute('CREATE INDEX IF NOT EXISTS server_unsettled_reservations '
                         'ON server_order_reservations(settled,intent_id)')
            if not existed:
                # Old claims prove an attempted physical identity, not its
                # instrument/side/size. Never manufacture those at migration.
                for row in conn.execute('SELECT intent_id,orders FROM server_order_intents').fetchall():
                    for leg in json.loads(row['orders']):
                        conn.execute('INSERT INTO server_order_reservations(intent_id,client_id,order_id) VALUES (?,?,?)',
                                     (row['intent_id'], leg['clientId'], leg['orderId']))

    def claim(self, intent_id: str, fingerprint: str, account: str) -> bool:
        with self.journal.transaction() as conn:
            row = conn.execute('SELECT fingerprint, account, status, orders FROM server_order_intents WHERE intent_id=?',
                               (intent_id,)).fetchone()
            if row is not None:
                if row['fingerprint'] != fingerprint or row['account'] != account:
                    raise ValueError('intent ID reused with a different account or payload')
                if row['status'] == 'RETRYABLE' and not json.loads(row['orders']):
                    conn.execute('UPDATE server_order_intents SET status=? WHERE intent_id=?',
                                 ('CLAIMED', intent_id))
                    return True
                return False
            conn.execute('INSERT INTO server_order_intents(intent_id,fingerprint,account,status,created_at) '
                         'VALUES (?,?,?,?,?)', (intent_id, fingerprint, account, 'CLAIMED', time.time()))
            return True

    def get(self, intent_id: str) -> dict | None:
        with self.journal.transaction() as conn:
            row = conn.execute('SELECT * FROM server_order_intents WHERE intent_id=?', (intent_id,)).fetchone()
            if row is None:
                return None
            result = dict(row)
            result['orders'] = json.loads(result['orders'])
            result['outcome'] = json.loads(result['outcome']) if result['outcome'] else None
            return result

    def get_many(self, intent_ids: list[str]) -> dict[str, dict]:
        """Read exact claim provenance and reserved broker IDs in one transaction."""
        if any(not isinstance(value, str) or not value for value in intent_ids):
            raise ValueError('broker intent IDs must be nonempty strings')
        ids = list(dict.fromkeys(intent_ids))
        if not ids:
            return {}
        result = {}
        with self.journal.transaction() as conn:
            # Keep one coherent read even when the broker returns more IDs
            # than an older SQLite build permits in one parameter list.
            for offset in range(0, len(ids), 500):
                chunk = ids[offset:offset + 500]
                placeholders = ','.join('?' for _ in chunk)
                rows = conn.execute(
                    'SELECT intent_id,account,orders,created_at FROM server_order_intents '
                    f'WHERE intent_id IN ({placeholders})', chunk).fetchall()
                for row in rows:
                    item = dict(row)
                    item['orders'] = json.loads(item['orders'])
                    result[item['intent_id']] = item
        return result

    def reserve_order(self, intent_id: str, order_id: int, client_id: int, *,
                      conid=None, action=None, quantity=None, is_exit=None, broker_reference=None) -> None:
        metadata = _reservation_metadata(conid, action, quantity, is_exit, broker_reference or intent_id)
        with self.journal.transaction() as conn:
            row = conn.execute('SELECT orders FROM server_order_intents WHERE intent_id=?', (intent_id,)).fetchone()
            if row is None:
                raise ValueError('broker submission has no durable intent claim')
            prior = conn.execute('SELECT * FROM server_order_reservations WHERE intent_id=? AND client_id=? AND order_id=?',
                                 (intent_id, client_id, order_id)).fetchone()
            if prior is not None and metadata is not None and any(prior[name] != value for name, value in metadata.items()):
                raise ValueError('physical reservation metadata is immutable')
            if prior is None:
                values = metadata or {'conid': None, 'action': None, 'quantity': None,
                                      'is_exit': None, 'broker_reference': None}
                conn.execute('INSERT INTO server_order_reservations '
                             '(intent_id,client_id,order_id,conid,action,quantity,is_exit,broker_reference) VALUES (?,?,?,?,?,?,?,?)',
                             (intent_id, client_id, order_id, values['conid'], values['action'],
                              values['quantity'], values['is_exit'], values['broker_reference']))
            orders = json.loads(row['orders'])
            identity = {'orderId': order_id, 'clientId': client_id}
            if identity not in orders:
                orders.append(identity)
            conn.execute('UPDATE server_order_intents SET orders=?,status=? WHERE intent_id=?',
                         (json.dumps(orders), 'SUBMITTING', intent_id))

    def finish(self, intent_id: str, status: str, error: str = '', outcome=None) -> None:
        with self.journal.transaction() as conn:
            conn.execute('UPDATE server_order_intents SET status=?,error=?,outcome=? WHERE intent_id=?',
                         (status, error, json.dumps(outcome) if outcome is not None else None, intent_id))

    def discard_order(self, intent_id: str, order_id: int, client_id: int) -> None:
        """Release only an ID proven not to have crossed the broker boundary."""
        with self.journal.transaction() as conn:
            row = conn.execute('SELECT orders FROM server_order_intents WHERE intent_id=?', [intent_id]).fetchone()
            orders = [order for order in json.loads(row['orders'])
                      if order != {'orderId': order_id, 'clientId': client_id}]
            conn.execute('UPDATE server_order_intents SET orders=? WHERE intent_id=?',
                         [json.dumps(orders), intent_id])
            conn.execute('DELETE FROM server_order_reservations WHERE intent_id=? AND client_id=? AND order_id=?',
                         (intent_id, client_id, order_id))

    def reservations(self, account: str) -> list[dict]:
        """Only unsettled physical attempts; intent completion is not broker proof."""
        with self.journal.transaction() as conn:
            rows = conn.execute('''SELECT r.*,i.account,i.orders FROM server_order_reservations r
                JOIN server_order_intents i ON i.intent_id=r.intent_id
                WHERE r.settled=0 AND i.account=?''', (account,)).fetchall()
            result = []
            for row in rows:
                item = dict(row)
                item['leg_count'] = len(json.loads(item.pop('orders')))
                result.append(item)
            return result

    def settle_reservations(self, keys: list[tuple]) -> None:
        """Called only after exact, unambiguous broker final-quantity evidence."""
        with self.journal.transaction() as conn:
            conn.executemany('UPDATE server_order_reservations SET settled=1 '
                             'WHERE intent_id=? AND client_id=? AND order_id=?', keys)
