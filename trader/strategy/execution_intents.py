"""Durable strategy intents, distinct from order acceptance and inventory.

An unresolved submission reserves its instrument across process restarts.
Broker order IDs and cumulative fills are observations of this intent, never
inferred from the quantity requested or from an aggregate account position.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import copy
import threading
import json
import time
import uuid

from trader.data.execution_journal import ExecutionJournal


TERMINAL = frozenset({'FILLED', 'CANCELLED', 'REJECTED', 'RESOLVED'})


def merge_exit_scopes(previous: dict | None, current: dict | None) -> dict | None:
    """Union explicitly acquired authority without enlarging it on a retry."""
    if previous is None:
        return copy.deepcopy(current)
    merged = copy.deepcopy(previous)
    if current is not None:
        for position in current['positions']:
            if position not in merged['positions']:
                merged['positions'].append(copy.deepcopy(position))
        for intent_id, opening in current['openings'].items():
            merged['openings'].setdefault(intent_id, copy.deepcopy(opening))
    return merged


class IntentStore:
    def __init__(self, database_path: str):
        self.journal = ExecutionJournal(database_path)
        self._cache: dict[str, dict] = {}
        self._cache_lock = threading.RLock()
        # One checkpoint per entry epoch, not an ever-growing set of bars.
        # Hydrate before runtime admission begins so overflow decisions after
        # restart use the same observed history as ordinary worker processing.
        with self.journal.transaction() as conn:
            self._bar_progress: dict[tuple[str, int, str], tuple[str, int]] = {
                (row['strategy'], row['conid'], row['entry_bar_ts']):
                (row['watermark'], row['bars_held'])
                for row in conn.execute('SELECT * FROM execution_bar_progress')}

    @staticmethod
    def _advance_bars(progress: tuple[str, int], observations) -> tuple[str, int]:
        watermark, count = progress
        new = sorted({value for observed in observations
                      if (value := timestamp_text(observed)) > watermark})
        return (new[-1], count + len(new)) if new else progress

    def preview_bar_progress(self, strategy: str, conid: int, entry_bar_ts, observations,
                             *, staged_progress: tuple[str, int] | None = None) -> tuple[str, int]:
        """Compact a coalesced timer mailbox without disk I/O or cache mutation."""
        entry = timestamp_text(entry_bar_ts)
        with self._cache_lock:
            cached = self._bar_progress.get((strategy, conid, entry), (entry, 0))
        staged = staged_progress or (entry, 0)
        return self._advance_bars((max(cached[0], staged[0]), max(cached[1], staged[1])), observations)

    def preview_bar_count(self, strategy: str, conid: int, entry_bar_ts, observations,
                          *, remember: bool = False,
                          staged_progress: tuple[str, int] | None = None) -> int:
        """Loop-safe estimate; optional volatile retention during an outage.

        Only actually observed, strictly newer completed bars count. Replayed
        frames and late older bars cannot move the watermark backwards.
        """
        entry = timestamp_text(entry_bar_ts)
        key = (strategy, conid, entry)
        with self._cache_lock:
            progress = self.preview_bar_progress(strategy, conid, entry_bar_ts, observations,
                                                  staged_progress=staged_progress)
            if remember:
                self._bar_progress[key] = progress
            return progress[1]

    def observe_bar_count(self, strategy: str, conid: int, entry_bar_ts, observations,
                          *, staged_progress: tuple[str, int] | None = None) -> int:
        """Commit accumulated observations independently of frame retention.

        SQLite access belongs on the executor worker. Cached progress may be
        ahead after a transient outage; recovery must commit that prefix even
        when those bars have already disappeared from the current frame.
        """
        entry = timestamp_text(entry_bar_ts)
        key = (strategy, conid, entry)
        with self.journal.transaction() as conn:
            row = conn.execute('SELECT watermark, bars_held FROM execution_bar_progress '
                               'WHERE strategy=? AND conid=? AND entry_bar_ts=?', key).fetchone()
            durable = (row['watermark'], row['bars_held']) if row else (entry, 0)
            with self._cache_lock:
                cached = self._bar_progress.get(key, (entry, 0))
            staged = staged_progress or (entry, 0)
            # Both checkpoints describe monotone prefixes of this entry's
            # completed-bar stream. Keep the most advanced observed prefix.
            progress = self._advance_bars(
                (max(durable[0], cached[0], staged[0]), max(durable[1], cached[1], staged[1])), observations)
            conn.execute('INSERT INTO execution_bar_progress VALUES (?, ?, ?, ?, ?) '
                         'ON CONFLICT(strategy, conid, entry_bar_ts) DO UPDATE SET '
                         'watermark=excluded.watermark, bars_held=excluded.bars_held',
                         (*key, *progress))
        with self._cache_lock:
            self._bar_progress[key] = progress
        return progress[1]

    def _remember(self, intent):
        with self._cache_lock:
            self._cache[intent['intent_id']] = copy.deepcopy(intent)
        return intent

    def cached(self) -> list[dict]:
        """Last durable observations, available during a local storage outage."""
        with self._cache_lock:
            return copy.deepcopy(list(self._cache.values()))

    def has_pending_open(self, strategy: str, conid: int) -> bool:
        """Loop-safe admission check using only last durable observations."""
        with self._cache_lock:
            return any(row['strategy'] == strategy and row['conid'] == conid
                       and row['kind'] == 'OPEN' and row['status'] not in TERMINAL
                       for row in self._cache.values())

    @staticmethod
    def _decode(row) -> dict:
        result = dict(row)
        result['payload'] = json.loads(result['payload'])
        return result

    def get(self, intent_id: str) -> dict | None:
        """Read one intent by its durable identity (None when unknown)."""
        with self.journal.transaction() as conn:
            row = conn.execute('SELECT * FROM execution_intents WHERE intent_id=?', [intent_id]).fetchone()
        return self._remember(self._decode(row)) if row else None

    def all(self, *, strategy=None, conid=None, kind=None, active=False) -> list[dict]:
        query = 'SELECT * FROM execution_intents WHERE 1=1'
        params = []
        for column, value in [('strategy', strategy), ('conid', conid), ('kind', kind)]:
            if value is not None:
                query += f' AND {column}=?'
                params.append(value)
        if active:
            query += " AND status NOT IN ('FILLED','CANCELLED','REJECTED','RESOLVED')"
        query += ' ORDER BY updated, intent_id'
        with self.journal.transaction() as conn:
            return [self._remember(self._decode(row)) for row in conn.execute(query, params).fetchall()]

    def create(self, strategy: str, conid: int, kind: str, payload: dict,
               status: str = 'CREATED') -> dict:
        """Claim one outstanding intent of a kind for an owned instrument.

        Concurrent executor processes share this constraint under SQLite's
        write transaction. The durable ID is created before propose/approve.
        """
        with self.journal.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM execution_intents WHERE strategy=? AND conid=? AND kind=? "
                "AND status NOT IN ('FILLED','CANCELLED','REJECTED','RESOLVED') LIMIT 1",
                [strategy, conid, kind]).fetchone()
            if row:
                return self._remember(self._decode(row))
            intent_id = 'auto-' + uuid.uuid4().hex
            payload = dict(payload, intent_created_at=time.time())
            conn.execute('INSERT INTO execution_intents VALUES (?, ?, ?, ?, ?, ?, ?)',
                         [intent_id, strategy, conid, kind, status,
                          json.dumps(payload), time.time()])
            row = conn.execute('SELECT * FROM execution_intents WHERE intent_id=?',
                               [intent_id]).fetchone()
            return self._remember(self._decode(row))

    def update(self, intent: dict, *, status=None, **payload):
        with self.journal.transaction() as conn:
            row = conn.execute('SELECT * FROM execution_intents WHERE intent_id=?',
                               [intent['intent_id']]).fetchone()
            current = self._decode(row)
            if ('intent_created_at' in payload
                    and payload['intent_created_at'] != current['payload'].get('intent_created_at')):
                raise ValueError('an intent creation timestamp is immutable')
            if current['payload'].get('ownership_epoch') is not None and any(
                    key in payload and payload[key] != current['payload'].get(key)
                    for key in ('ownership_epoch', 'ownership_started_at')):
                raise ValueError('a physical execution ownership binding is immutable')
            current['payload'].update(payload)
            current['status'] = status or current['status']
            conn.execute('UPDATE execution_intents SET status=?, payload=?, updated=? WHERE intent_id=?',
                         [current['status'], json.dumps(current['payload']), time.time(), intent['intent_id']])
        intent.update(current)
        self._remember(intent)

    def finish_exit_request(self, intent: dict, successor_id: str | None) -> bool:
        """Consume only the exit request that the worker actually fulfilled.

        A newly retained successor must survive an older worker observation.
        Broker status and IDs remain unchanged: completing the desire to
        flatten is distinct from changing the physical order's history.
        """
        with self.journal.transaction() as conn:
            row = conn.execute('SELECT * FROM execution_intents WHERE intent_id=?',
                               [intent['intent_id']]).fetchone()
            current = self._decode(row)
            pending = current['payload'].get('successor_exit')
            if (pending.get('request_id') if pending else None) != successor_id:
                return False
            if current['payload'].get('explicit_exit_scope') != intent['payload'].get('explicit_exit_scope'):
                return False
            current['payload'].update(exit_request_active=False, successor_exit=None)
            conn.execute('UPDATE execution_intents SET payload=?, updated=? WHERE intent_id=?',
                         [json.dumps(current['payload']), time.time(), intent['intent_id']])
        intent.update(current)
        self._remember(intent)
        return True

    def retain_exit_scope(self, intent: dict, scope: dict):
        """A fresh explicit SELL may add authority; timer retries may not."""
        with self.journal.transaction() as conn:
            row = conn.execute('SELECT * FROM execution_intents WHERE intent_id=?',
                               [intent['intent_id']]).fetchone()
            current = self._decode(row)
            current['payload'].update(
                explicit_exit_scope=merge_exit_scopes(current['payload'].get('explicit_exit_scope'), scope),
                exit_request_active=True)
            conn.execute('UPDATE execution_intents SET payload=?, updated=? WHERE intent_id=?',
                         [json.dumps(current['payload']), time.time(), intent['intent_id']])
        intent.update(current)
        self._remember(intent)

    def retain_exit_successor(self, intent: dict, request: dict):
        """Merge authority against the committed request, never a stale copy."""
        with self.journal.transaction() as conn:
            row = conn.execute('SELECT * FROM execution_intents WHERE intent_id=?',
                               [intent['intent_id']]).fetchone()
            current = self._decode(row)
            previous = current['payload'].get('successor_exit')
            request = dict(request)
            if (current['payload'].get('policy_entry_bar_ts') is None
                    or previous and previous.get('policy_entry_bar_ts') is None):
                request['policy_entry_bar_ts'] = None
            replace = not previous or request['entry_bar_ts'] > previous['entry_bar_ts']
            if previous and request['entry_bar_ts'] == previous['entry_bar_ts']:
                replace = request.get('policy_entry_bar_ts') != previous.get('policy_entry_bar_ts')
            if replace:
                current['payload'].update(successor_exit=request, exit_request_active=True)
                conn.execute('UPDATE execution_intents SET payload=?, updated=? WHERE intent_id=?',
                             [json.dumps(current['payload']), time.time(), intent['intent_id']])
        intent.update(current)
        self._remember(intent)

    @staticmethod
    def protective_identity(strategy: str, conid: int, row: dict, entry_bar_text: str) -> str:
        """Keep the historical physical adoption key stable across upgrades."""
        identity = [strategy, conid, row.get('account', ''), row.get('clientId', 0),
                    row.get('permId', 0), row['orderId'], entry_bar_text]
        return 'adopt-' + hashlib.sha256(json.dumps(identity).encode()).hexdigest()

    def adopt_protective(self, strategy: str, conid: int, row: dict, entry_bar, *,
                         broker_intent_id: str = '', ownership_epoch=None,
                         ownership_started_at=None, attribution_unresolved=False,
                         existing: dict | None = None) -> dict:
        """Track a ref-owned stop replaced by another authorized workflow.

        Its fills must reduce strategy ownership just like fills of stops we
        submitted ourselves. Broker identity plus entry instant prevents an
        unrelated historic order from becoming a new position's execution.
        """
        original_bar = existing['payload']['bar_ts'] if existing else timestamp_text(entry_bar)
        historical_id = self.protective_identity(strategy, conid, row, original_bar)
        intent_id = existing['intent_id'] if existing else historical_id
        payload = dict(bar_ts=timestamp_text(entry_bar), order_ids=[int(row['orderId'])],
                       quantity=float(row.get('totalQuantity', 0)), adopted=True,
                       broker_intent_id=broker_intent_id, ownership_epoch=ownership_epoch,
                       ownership_started_at=ownership_started_at,
                       attribution_unresolved=attribution_unresolved)
        with self.journal.transaction() as conn:
            conn.execute('INSERT OR IGNORE INTO execution_intents VALUES (?, ?, ?, ?, ?, ?, ?)',
                         [intent_id, strategy, conid, 'PROTECTIVE',
                          'UNKNOWN' if attribution_unresolved else 'WORKING', json.dumps(payload), time.time()])
            current = self._decode(conn.execute(
                'SELECT * FROM execution_intents WHERE intent_id=?', [intent_id]).fetchone())
            saved = current['payload']
            if ((current['strategy'], current['conid'], current['kind']) != (strategy, conid, 'PROTECTIVE')
                    or not saved.get('adopted')):
                raise ValueError('protective adoption identity conflicts with an existing intent')
            alias = saved.get('broker_intent_id')
            if alias and broker_intent_id and alias != broker_intent_id:
                raise ValueError('protective broker identity changed')
            if not alias and intent_id != historical_id:
                raise ValueError('legacy protective identity requires reconciliation')
            if broker_intent_id:
                saved['broker_intent_id'] = broker_intent_id
            if saved.get('ownership_epoch') is None and not attribution_unresolved:
                saved.update(ownership_epoch=ownership_epoch, ownership_started_at=ownership_started_at)
            if saved.get('attribution_unresolved') and not attribution_unresolved:
                if saved.get('ownership_epoch') != ownership_epoch:
                    raise ValueError('unresolved protection belongs to a different ownership epoch')
                saved['attribution_unresolved'] = False
                current['status'] = 'WORKING'
            elif attribution_unresolved and saved.get('ownership_epoch') is None:
                saved['attribution_unresolved'] = True
                current['status'] = 'UNKNOWN'
            conn.execute('UPDATE execution_intents SET payload=?,status=?,updated=? WHERE intent_id=?',
                         [json.dumps(saved), current['status'], time.time(), intent_id])
        if existing is not None:
            existing.update(current)
        return self._remember(current)

    def restore_exit(self, intent_id: str, strategy: str, conid: int, payload: dict) -> dict:
        """Recover an emergency reduction from its persistent broker reference."""
        with self.journal.transaction() as conn:
            conn.execute('INSERT OR IGNORE INTO execution_intents VALUES (?, ?, ?, ?, ?, ?, ?)',
                         [intent_id, strategy, conid, 'CLOSE', 'WORKING', json.dumps(payload), time.time()])
            row = conn.execute('SELECT * FROM execution_intents WHERE intent_id=?', [intent_id]).fetchone()
        return self._remember(self._decode(row))

    def submitted_recently(self, strategy: str, conid: int, seconds: float) -> bool:
        cutoff = time.time() - seconds
        for intent in self.all(strategy=strategy, conid=conid, kind='OPEN'):
            if intent['status'] == 'REJECTED':
                continue
            if intent['status'] == 'RESOLVED' and intent['payload'].get('never_submitted') is True:
                continue  # proven unsent: it consumed no broker capacity and holds no cooldown
            if intent['payload'].get('submitted_at', 0) > cutoff:
                return True
        return False


def timestamp_text(value) -> str:
    if hasattr(value, 'to_pydatetime'):
        value = value.to_pydatetime()
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.timezone.utc)
        return value.astimezone(dt.timezone.utc).isoformat()
    raise ValueError(f'execution intent requires a datetime bar timestamp, got {value!r}')
