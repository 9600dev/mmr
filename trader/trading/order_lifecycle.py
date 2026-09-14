"""Broker execution ingestion and decisive-status observation.

Order acceptance, cumulative fills and cancellation are independent facts.
The event-loop callback only snapshots primitives and resolves waiters.  A
worker commits them to an independent SQLite journal and retries delivery to
DuckDB through an idempotent outbox.  Broker executions replayed on reconnect
are safe to ingest again; status-only feeds retain cumulative-fill checkpoints.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import math
import queue
import sys
import tempfile
import threading
import time
import weakref
from dataclasses import asdict
from typing import Dict, List, Optional

from trader.common.logging_helper import setup_logging
from trader.data.event_store import EventStore, EventType, TradingEvent
from trader.data.execution_journal import ExecutionJournal
from trader.trading.order_reference import split_order_reference

logging = setup_logging(module_name='order_lifecycle')
_ACCEPTED = {'PreSubmitted', 'Submitted'}
_FILLED = {'Filled'}
_DEAD = {'Cancelled', 'ApiCancelled', 'Inactive'}


def _number(value, default=0.0) -> float:
    try:
        result = float(value)
        return result if math.isfinite(result) else default
    except (ValueError, TypeError):
        return default


def _quantity(value) -> Optional[float]:
    """An IB unset/missing quantity is not evidence of zero execution."""
    try:
        result = float(value)
        return result if math.isfinite(result) and 0 <= result < sys.float_info.max else None
    except (ValueError, TypeError):
        return None


class OrderLifecycleTracker:
    def __init__(self, event_store: Optional[EventStore] = None):
        self._event_store = event_store
        self._waiters: Dict[int, List[tuple]] = {}
        # A provisional (permId=0) status belongs to the actual observed Trade,
        # not merely a reused numeric scope. Weak refs do not retain IB history.
        self._trade_receipts: dict[str, weakref.ReferenceType] = {}
        self._acceptance_rows: dict[str, dict] = {}
        self._unproven_trades: dict[tuple, list[weakref.ReferenceType]] = {}
        self._snapshots: dict[str, dict] = {}
        self._permanent_identities: dict[str, str] = {}
        self._scoped_identities: dict[tuple, set[str]] = {}
        self._identity_conflicts: set[str] = set()
        self._lock = threading.RLock()
        self._start_lock = threading.Lock()
        self._queue: queue.Queue = queue.Queue(maxsize=8192)
        self._journal: Optional[ExecutionJournal] = None
        self._worker: Optional[threading.Thread] = None
        self._delivery_worker: Optional[threading.Thread] = None
        self._temporary = None
        self._ingest_error = ''
        self._delivery_error = ''
        self._identity_error = ''
        self._replay_required = False
        self._closed = threading.Event()
        self._wake = threading.Event()
        self._delivery_wake = threading.Event()
        self._pending_submissions: set[str] = set()
        self._submission_wake = threading.Event()
        if event_store is not None:
            self._start_worker()

    def set_event_store(self, event_store: EventStore) -> None:
        self._event_store = event_store
        self._start_worker()

    def _start_worker(self):
        with self._start_lock:
            if self._worker is not None:
                return
            path = getattr(self._event_store, 'duckdb_path', None)
            if not path:
                # In-memory/fake stores have no restart identity. Production
                # EventStore always supplies its durable database location.
                self._temporary = tempfile.TemporaryDirectory(prefix='mmr-lifecycle-')
                path = self._temporary.name + '/events'
            self._journal = ExecutionJournal(path)
            with self._journal.transaction() as conn:
                persisted = conn.execute('SELECT cumulative,notional,snapshot FROM broker_order_progress').fetchall()
                undelivered = conn.execute('SELECT identity,payload FROM broker_event_outbox WHERE delivered=0').fetchall()
            with self._lock:
                self._pending_submissions = {
                    identity for identity, payload in undelivered
                    if json.loads(payload)['event_type'] == EventType.ORDER_SUBMITTED.value}
                # Callbacks can precede journal attachment at startup. Their
                # derived snapshots/aliases know nothing of previous fills;
                # restore durable truth before replaying their raw observations.
                self._snapshots.clear()
                self._trade_receipts.clear()
                self._acceptance_rows.clear()
                self._unproven_trades.clear()
                self._permanent_identities.clear()
                self._scoped_identities.clear()
                for cumulative, notional, payload in persisted:
                    row = self._restore_checkpoint(cumulative, notional, payload)
                    self._snapshots[row['identity']] = row
                    self._acceptance_rows[row['identity']] = dict(row)
                    self._remember_identity(row)
                self._identity_conflicts = {
                    identity for identities in self._scoped_identities.values()
                    if len(identities) > 1
                    and any(not self._snapshots[candidate]['permId'] for candidate in identities)
                    for identity in identities}
                if self._identity_conflicts:
                    # An old duplicate audit fill cannot be undone by replay.
                    self._identity_error = 'ambiguous persisted broker identities; reconciliation required'
                    self._replay_required = True
                for _ in range(self._queue.qsize()):
                    buffered = self._queue.get_nowait()
                    raw = buffered[0]
                    receipt = buffered[2] if len(buffered) > 2 else None
                    try:
                        row = self._store_observation(raw, receipt)
                        # Replace before task_done: flush must not observe a
                        # drained queue while startup still holds observations.
                        self._queue.put_nowait((raw, row, receipt))
                    except ValueError as exc:
                        self._ingest_error = str(exc)
                        self._replay_required = True
                        logging.exception('buffered broker identity requires replay')
                    finally:
                        self._queue.task_done()
            self._worker = threading.Thread(target=self._run, name='broker-events', daemon=True)
            self._delivery_worker = threading.Thread(target=self._delivery_loop,
                                                     name='broker-audit-outbox', daemon=True)
            self._worker.start()
            self._delivery_worker.start()

    @staticmethod
    def _restore_checkpoint(cumulative, notional, payload) -> dict:
        row = json.loads(payload)
        row.setdefault('brokerStatus', row['status'])
        if cumulative > row['filled']:
            # Old late-attachment code could preserve the numeric checkpoint
            # but overwrite its JSON with an earlier/default-zero observation.
            # This proves a lower bound, not the final executed quantity; the
            # separate terminal column may likewise be stale.
            average = notional / cumulative if notional > 0 else 0.0
            row.update(filled=cumulative,
                       avgFillPrice=average if 0 < average < sys.float_info.max else 0.0,
                       status='Unknown', fillQuantityKnown=False, remaining=None)
        elif row['status'] == 'Filled' and row['filled'] <= 0:
            row.update(status='Unknown', brokerStatus='Filled',
                       fillQuantityKnown=False, remaining=None)
        return row

    @staticmethod
    def _merge_observation(observation, previous) -> dict:
        """Merge only evidence that predates this observation."""
        row = dict(observation)
        status = row['brokerStatus']
        row['status'] = status
        total = row['totalQuantity']
        if previous and not row['permId']:
            row['permId'] = previous['permId']
        if previous and previous['filled'] > row['filled']:
            row['filled'] = previous['filled']
            row['avgFillPrice'] = previous['avgFillPrice']
            if status in _FILLED | _DEAD:
                # A smaller terminal quantity contradicts the confirmed
                # lower bound. Keep that bound without claiming it is final.
                row['fillQuantityKnown'] = False
        elif (previous and previous['filled'] == row['filled']
              and row['avgFillPrice'] <= 0):
            row['avgFillPrice'] = previous['avgFillPrice']
        if (previous and previous.get('fillQuantityKnown', True)
                and previous['status'] in _FILLED | _DEAD
                and (previous['status'] != 'Filled' or previous['filled'] > 0)
                and (previous['status'] == 'Filled' or status in _DEAD)
                and previous['filled'] >= row['filled']):
            row['fillQuantityKnown'] = True
            if previous['status'] == 'Filled':
                row['status'] = 'Filled'
        elif total > 0 and row['filled'] >= total and status == 'Filled':
            row['fillQuantityKnown'] = True
        if not row['fillQuantityKnown'] and status in _FILLED | _DEAD:
            row['status'] = 'Unknown'
        row['remaining'] = max(total - row['filled'], 0.0) if row['fillQuantityKnown'] else None
        if previous and not row['orderId']:
            row['orderId'] = previous['orderId']
            row['clientId'] = previous['clientId']
        return row

    def _remember_unproven_trade(self, scope, receipt) -> None:
        candidates = self._unproven_trades.setdefault(scope, [])
        trade = receipt()
        if not any(item is receipt or (trade is not None and item() is trade) for item in candidates):
            candidates.append(receipt)

    def _store_observation(self, raw, trade_receipt=None) -> dict:
        # Caller holds the callback lock through normalization AND enqueue;
        # persistence must see the same order as these in-memory checkpoints.
        identity = self._logical_identity(raw)
        previous = self._acceptance_rows.get(identity)
        prior_receipt = self._trade_receipts.get(identity)
        trade = trade_receipt() if trade_receipt is not None else None
        same_trade = trade is not None and prior_receipt is not None and prior_receipt() is trade
        scope = self._order_scope(raw)
        if (trade_receipt is not None and not raw['permId']
                and previous is not None and not same_trade):
            # Numeric-scope reuse cannot turn the old row's retained Filled or
            # Submitted status into proof for a fresh provisional Trade.
            self._remember_unproven_trade(scope, trade_receipt)
        else:
            if (raw['permId'] and previous is not None and not previous['permId']
                    and not same_trade):
                # A permanent receipt cannot identify an older provisional
                # object solely by scope, including a persisted legacy row
                # whose original Trade object no longer exists after restart.
                if prior_receipt is not None:
                    self._remember_unproven_trade(scope, prior_receipt)
                previous = None
            accepted = self._merge_observation(raw, previous)
            accepted['identity'] = identity
            self._acceptance_rows[identity] = accepted
            if trade_receipt is not None:
                self._trade_receipts[identity] = trade_receipt
            else:
                self._trade_receipts.pop(identity, None)
            if raw['permId'] and trade is not None:
                unresolved = [item for item in self._unproven_trades.get(scope, ()) if item() is not trade]
                if unresolved:
                    self._unproven_trades[scope] = unresolved
                else:
                    self._unproven_trades.pop(scope, None)
        # Acceptance identity does not change durable cumulative-fill merging.
        row = self._merge_observation(raw, self._snapshots.get(identity))
        row['identity'] = identity
        self._snapshots[identity] = row
        self._remember_identity(row)
        return row

    @staticmethod
    def _order_scope(row) -> tuple:
        # The reference and instrument prevent numeric-ID reuse from joining
        # distinct intents. A day boundary does not create a different order.
        return (row['account'], row['clientId'], row['orderId'],
                row.get('brokerOrderRef', row['orderRef']), row['conId'], row['action'])

    def _remember_identity(self, row) -> None:
        if row['permId']:
            permanent = f"{row['account']}:perm:{row['permId']}"
            self._permanent_identities[permanent] = row['identity']
        if row['orderId']:
            self._scoped_identities.setdefault(self._order_scope(row), set()).add(row['identity'])

    def _logical_identity(self, row) -> str:
        if row['permId']:
            known = self._permanent_identities.get(row['identity'])
            if known is not None:
                return known
        candidates = [identity for identity in self._scoped_identities.get(self._order_scope(row), ())
                      if not row['permId'] or self._snapshots[identity]['permId'] in (0, row['permId'])]
        if len(candidates) > 1 or (not candidates and row['identity'] in self._snapshots):
            raise ValueError('ambiguous broker order identity; execution replay required')
        return candidates[0] if candidates else row['identity']

    @property
    def health(self) -> dict:
        error = '; '.join(part for part in (self._ingest_error, self._delivery_error, self._identity_error) if part)
        return {'healthy': not bool(error) and not self._replay_required and not self._pending_submissions,
                'pending': self._queue.unfinished_tasks,
                'pending_submissions': len(self._pending_submissions),
                'replay_required': self._replay_required, 'error': error}

    def record_submission(self, event: TradingEvent, timeout: float = 5.0) -> None:
        """Durably charge an attempt and wait for risk-accounting visibility.

        Call off the broker loop while holding the account order lock. The row
        exists before broker submission, so a crash cannot erase a consumed
        rate/turnover reservation. Delivery retries by identity after restart.
        """
        if self._journal is None or self._closed.is_set():
            raise RuntimeError('submission journal is unavailable')
        identity = event.metadata['submission_identity']
        with self._lock:
            self._pending_submissions.add(identity)
        # The callback lock must never cover SQLite's lock wait: on_trade and
        # on_execution take it on the broker loop to update their snapshots.
        with self._journal.transaction() as conn:
            self._outbox(conn, identity, event)
            delivered = conn.execute('SELECT delivered FROM broker_event_outbox WHERE identity=?',
                                     [identity]).fetchone()[0]
        if delivered:
            with self._lock:
                self._pending_submissions.discard(identity)
            return
        self._delivery_wake.set()
        deadline = time.monotonic() + timeout
        while True:
            with self._lock:
                if identity not in self._pending_submissions:
                    return
            remaining = deadline - time.monotonic()
            if remaining <= 0 or self._closed.is_set():
                raise TimeoutError(f'submission audit remains pending for {identity}')
            self._submission_wake.wait(min(remaining, 0.05))
            self._submission_wake.clear()

    def snapshot(self, order_ids: Optional[list[int]] = None) -> list[dict]:
        with self._lock:
            rows = []
            for identity, row in self._snapshots.items():
                if order_ids is not None and row['orderId'] not in order_ids:
                    continue
                snapshot = dict(row)
                if identity in self._identity_conflicts:
                    snapshot['identityAmbiguous'] = True
                rows.append(snapshot)
            return rows

    def execution_receipts(self, order_ids: Optional[list[int]] = None) -> list[dict]:
        if self._journal is None:
            return []
        with self._journal.transaction() as conn:
            rows = conn.execute('SELECT payload FROM broker_execution_receipts').fetchall()
        receipts = [json.loads(row[0]) for row in rows]
        return [row for row in receipts if order_ids is None or row['orderId'] in order_ids]

    def mark_replay_complete(self) -> bool:
        """Acknowledge an authoritative broker replay after its queue drained."""
        if self._queue.unfinished_tasks or self._ingest_error or self._identity_error:
            return False
        self._replay_required = False
        return True

    def begin_replay(self):
        """Suspend new-exposure authority until broker history is observed."""
        self._replay_required = True

    def on_trade(self, trade, *, completed: bool = False) -> None:
        self._observe(trade, completed=completed)

    def on_execution(self, trade, fill, *, completed: bool = False) -> None:
        """IB execDetailsEvent sink; ``execId`` is durable replay identity."""
        self._observe(trade, fill, completed=completed)

    def _observe(self, trade, fill=None, *, completed: bool = False) -> None:
        try:
            order = getattr(trade, 'order', None)
            status_obj = getattr(trade, 'orderStatus', None)
            contract = getattr(trade, 'contract', None)
            execution = getattr(fill, 'execution', None)
            oid = int(getattr(execution, 'orderId', 0) or getattr(order, 'orderId', 0) or 0)
            # orderStatus enriches its own permId before openOrder necessarily
            # updates Order. All supplied nonzero IDs must name the same order.
            permanent_ids = {int(getattr(source, 'permId', 0) or 0)
                             for source in (order, status_obj, execution)} - {0}
            if len(permanent_ids) > 1 or any(identity < 0 for identity in permanent_ids):
                raise ValueError('contradictory broker permanent identities; execution replay required')
            perm_id = next(iter(permanent_ids), 0)
            status = str(getattr(status_obj, 'status', '') or '')
            if (not oid and not perm_id) or not status:
                return
            reported = _quantity(getattr(status_obj, 'filled', None))
            filled = reported or 0.0
            quantity_known = reported is not None and (not completed or reported > 0)
            average = _number(getattr(status_obj, 'avgFillPrice', 0))
            if not 0 < average < sys.float_info.max:
                average = 0.0
            # completedOrder constructs OrderStatus(filled=0) even when the
            # actual fill amount is missing. Its separate filledQuantity is
            # valid evidence only when the broker supplied a real value.
            completed_quantity = _quantity(getattr(order, 'filledQuantity', None)) if completed else None
            if completed_quantity is not None:
                filled = max(filled, completed_quantity)
                quantity_known = True
            # A status average prices only its own cumulative endpoint.
            # Completed-order quantity can be newer than OrderStatus.
            if reported != filled:
                average = 0.0
            total = _quantity(getattr(order, 'totalQuantity', None)) or 0.0
            if execution is not None:
                cum = _quantity(getattr(execution, 'cumQty', None)) or 0.0
                if cum >= filled and cum > 0:
                    filled = cum
                    execution_average = _number(getattr(execution, 'avgPrice', 0))
                    if 0 < execution_average < sys.float_info.max:
                        average = execution_average
                    elif reported != cum or not average:
                        # One execution prices the cumulative quantity only
                        # when it covers that entire quantity. A partial
                        # receipt still prices its own audit delta below.
                        shares = _quantity(getattr(execution, 'shares', None))
                        price = _number(getattr(execution, 'price', 0))
                        average = price if shares == cum and 0 < price < sys.float_info.max else 0.0
                if total > 0 and filled >= total:
                    quantity_known = True
            if status == 'Filled':
                if filled <= 0 or (not reported and completed_quantity is None
                                   and not (total > 0 and filled >= total)):
                    quantity_known = False
            now = dt.datetime.now(dt.timezone.utc)
            raw_reference = str(getattr(order, 'orderRef', '') or getattr(execution, 'orderRef', '') or '')
            strategy, intent_id = split_order_reference(raw_reference)
            row: dict = dict(orderId=oid, status=status, brokerStatus=status, filled=filled,
                       fillQuantityKnown=quantity_known,
                       remaining=max(total - filled, 0.0), totalQuantity=total,
                       avgFillPrice=average, orderRef=strategy or 'order',
                       brokerOrderRef=raw_reference, clientIntentId=intent_id,
                       action=str(getattr(order, 'action', '') or ''),
                       orderType=str(getattr(order, 'orderType', '') or ''),
                       conId=int(getattr(contract, 'conId', 0) or 0),
                       symbol=str(getattr(contract, 'symbol', '') or ''),
                       account=str(getattr(execution, 'acctNumber', '') or getattr(order, 'account', '') or ''),
                       clientId=int(getattr(execution, 'clientId', getattr(order, 'clientId', 0)) or 0),
                       permId=perm_id,
                       observed_at=now.isoformat())
            row['identity'] = (f"{row['account']}:perm:{row['permId']}" if row['permId']
                               else f"{row['account']}:{now.date()}:{row['clientId']}:{oid}")
            if execution is not None:
                execution_time = getattr(execution, 'time', None)
                row['execution'] = dict(
                    execId=str(getattr(execution, 'execId', '') or ''), orderId=oid,
                    shares=_number(getattr(execution, 'shares', 0)),
                    cumQty=_number(getattr(execution, 'cumQty', 0)),
                    price=_number(getattr(execution, 'price', 0)),
                    account=row['account'], permId=row['permId'],
                    time=execution_time.isoformat() if hasattr(execution_time, 'isoformat') else None)
            try:
                receipt = weakref.ref(trade)
            except TypeError:
                receipt = None  # minimal legacy adapter, numeric API only
            with self._lock:
                raw = row
                row = self._store_observation(raw, receipt)
                # The receipt belongs to this raw FIFO observation, including
                # when startup rebinds its provisional logical identity.
                self._queue.put_nowait((raw, row, receipt))
            if row['orderId']:
                self._resolve_waiters(row['orderId'])
            self._wake.set()
            # Direct synchronous consumers retain read-after-write semantics.
            # IB invokes this on its asyncio loop, where no persistence wait
            # is permitted. Tests of asynchronous ingestion use flush explicitly.
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                if self._event_store is not None:
                    self.flush()
        except queue.Full:
            self._ingest_error = 'broker event queue full; execution replay required'
            self._replay_required = True
            logging.critical(self._ingest_error)
        except Exception as exc:
            self._ingest_error = str(exc)
            self._replay_required = True
            logging.exception('order lifecycle observation failed')

    @staticmethod
    def _decisive(status: str) -> Optional[str]:
        if status in _FILLED:
            return 'filled'
        if status in _ACCEPTED:
            return 'accepted'
        if status in _DEAD:
            return 'rejected'
        return None  # pending — not decisive yet

    @staticmethod
    def _trade_selection(order_id: int, trade):
        """Freeze native physical scope; False means its identity is unproven."""
        if trade is None:
            return None  # legacy numeric caller
        try:
            order, status, contract = trade.order, trade.orderStatus, trade.contract
            oid, client, conid = order.orderId, order.clientId, contract.conId
            if (type(order_id) is not int or order_id <= 0
                    or type(oid) is not int or oid != order_id
                    or type(client) is not int or client < 0
                    or type(conid) is not int or conid <= 0):
                return False
            for observed, expected in ((status.orderId, oid), (status.clientId, client)):
                if type(observed) is not int or observed < 0 or (observed and observed != expected):
                    return False
            account, reference, action = order.account, order.orderRef, order.action
            if (not isinstance(account, str) or not account
                    or not isinstance(reference, str) or action not in {'BUY', 'SELL'}):
                return False
            permanent = (order.permId, status.permId)
            if any(type(value) is not int or value < 0 for value in permanent):
                return False
            known = set(permanent) - {0}
            if len(known) > 1:
                return False
            return ((account, client, oid, reference, conid, action), next(iter(known), 0), trade)
        except (AttributeError, TypeError, ValueError):
            return False

    def _selected_status(self, order_id: int, selection) -> Optional[str]:
        """Read under _lock; an ambiguous or contradictory scope is unknown."""
        if type(order_id) is not int or order_id <= 0 or selection is False:
            return None
        if selection is None:
            if any(scope[2] == order_id for scope in self._unproven_trades):
                return None
            rows = [row for row in self._acceptance_rows.values() if row['orderId'] == order_id]
        else:
            scope, permanent, trade = selection
            rows = []
            for identity in self._scoped_identities.get(scope, ()):
                row = self._acceptance_rows.get(identity)
                if row is None or self._order_scope(row) != scope:
                    continue  # an older provisional alias is not current proof
                receipt = self._trade_receipts.get(identity)
                same_trade = receipt is not None and receipt() is trade
                if permanent:
                    if row['permId'] != permanent and not (row['permId'] == 0 and same_trade):
                        continue
                elif not same_trade:
                    # A future real callback can establish this receipt, even
                    # if it promotes the Trade from zero to a permanent ID.
                    continue
                rows.append(row)
        if len(rows) != 1 or rows[0]['identity'] in self._identity_conflicts:
            return None
        return rows[0]['status']

    def latest_status(self, order_id: int, *, trade=None) -> Optional[str]:
        selection = self._trade_selection(order_id, trade)
        with self._lock:
            return self._selected_status(order_id, selection)

    async def wait_decisive(self, order_id: int, timeout: float = 10.0, *, trade=None) -> str:
        """Await decisive evidence for the supplied native Trade's identity.

        Numeric legacy callers require one unambiguous physical observation.
        Returns filled/accepted/rejected, or timeout when no decisive matching
        evidence arrives (also immediately for an invalid native selector).
        A timeout is UNKNOWN: the order may still be live, never a rejection.
        """
        selection = self._trade_selection(order_id, trade)
        if selection is False:
            return 'timeout'
        loop = asyncio.get_event_loop()
        with self._lock:
            cur = self._selected_status(order_id, selection)
            decisive = self._decisive(cur) if cur else None
            if decisive is not None:
                return decisive
            fut: asyncio.Future = loop.create_future()
            waiter = (fut, selection)
            self._waiters.setdefault(order_id, []).append(waiter)
        try:
            return await asyncio.wait_for(fut, timeout)
        except asyncio.TimeoutError:
            return 'timeout'
        finally:
            with self._lock:
                waiters = self._waiters.get(order_id)
                if waiters and waiter in waiters:
                    waiters.remove(waiter)
                if waiters is not None and not waiters:
                    self._waiters.pop(order_id, None)

    def _resolve_waiters(self, order_id: int) -> None:
        with self._lock:
            for fut, selection in self._waiters.get(order_id, []):
                cur = self._selected_status(order_id, selection)
                decisive = self._decisive(cur) if cur else None
                if decisive is not None and not fut.done():
                    fut.set_result(decisive)

    def flush(self, timeout: float = 5.0) -> bool:
        """Wait for queued observations and their audit outbox (never on IB loop)."""
        deadline = time.monotonic() + timeout
        self._wake.set()
        self._delivery_wake.set()
        while time.monotonic() < deadline:
            if self._queue.unfinished_tasks == 0 and not self._pending_outbox():
                return True
            time.sleep(.005)
        return False

    def close(self, timeout: float = 5.0):
        self.flush(timeout)
        self._closed.set()
        self._wake.set()
        self._delivery_wake.set()
        if self._worker is not None:
            self._worker.join(timeout)
        if self._delivery_worker is not None:
            self._delivery_worker.join(timeout)

    def _pending_outbox(self) -> bool:
        if self._journal is None:
            return False
        with self._journal.transaction() as conn:
            return conn.execute('SELECT 1 FROM broker_event_outbox WHERE delivered=0 LIMIT 1').fetchone() is not None

    def _run(self):
        pending = None
        while not self._closed.is_set():
            try:
                if pending is None:
                    try:
                        pending = self._queue.get_nowait()
                    except queue.Empty:
                        pass
                if pending is not None:
                    self._persist_observation(pending[1])
                    self._ingest_error = ''
                    self._queue.task_done()
                    pending = None
                    self._delivery_wake.set()
                if self._queue.empty():
                    self._wake.wait()
                    self._wake.clear()
            except Exception as exc:
                if self._ingest_error != str(exc):
                    logging.warning('broker journal delivery will retry: %s', exc)
                self._ingest_error = str(exc)
                self._wake.wait(.1)
                self._wake.clear()

    def _delivery_loop(self):
        # Historical DuckDB can wait through a bulk-writer lock. This separate
        # delivery thread must never prevent the ingestion thread from making
        # later broker observations durable in the SQLite outbox.
        while not self._closed.is_set():
            try:
                self._deliver()
                self._delivery_error = ''
                if not self._pending_outbox():
                    self._delivery_wake.wait()
                    self._delivery_wake.clear()
            except Exception as exc:
                if self._delivery_error != str(exc):
                    logging.warning('broker audit outbox will retry: %s', exc)
                self._delivery_error = str(exc)
                self._delivery_wake.wait(.1)
                self._delivery_wake.clear()

    def _persist_observation(self, row):
        assert self._journal is not None
        identity = row['identity']
        with self._journal.transaction() as conn:
            previous = conn.execute('SELECT cumulative, notional, terminal, snapshot FROM broker_order_progress WHERE identity=?',
                                    [identity]).fetchone()
            if previous:
                cumulative, notional, terminal, payload = tuple(previous)
                # Never consult _snapshots here: it can already contain a
                # future observation with a different cumulative fill price.
                row = self._merge_observation(row, self._restore_checkpoint(cumulative, notional, payload))
            else:
                cumulative, notional, terminal = 0.0, 0.0, None
            execution = row.get('execution')
            if execution and execution['execId']:
                conn.execute('INSERT OR IGNORE INTO broker_execution_receipts VALUES (?, ?)',
                             [row['account'] + ':' + execution['execId'], json.dumps(execution)])
            new_cumulative = max(cumulative, row['filled'])
            delta = new_cumulative - cumulative
            new_notional = new_cumulative * row['avgFillPrice']
            if not math.isfinite(new_notional) or new_notional < 0:
                new_notional = 0.0
            if delta > 0:
                # A real completed quantity can arrive without its price.
                # Unknown earlier cost cannot become zero cost when deriving
                # a later incremental fill price from cumulative averages.
                price = ((new_notional - notional) / delta
                         if new_notional > 0 and (cumulative == 0 or notional > 0) else 0.0)
                if (execution and execution['shares'] == delta
                        and execution['cumQty'] == new_cumulative
                        and 0 < execution['price'] < sys.float_info.max):
                    price = execution['price']
                price_evaluable = math.isfinite(price) and 0 < price < sys.float_info.max
                if not price_evaluable:
                    price = 0.0
                metadata = {'status': row['status'], 'cumulative_filled': new_cumulative,
                            'remaining': row['remaining'], 'broker_identity': identity,
                            'broker_status': row['brokerStatus'],
                            'fill_quantity_known': row['fillQuantityKnown'],
                            'price_evaluable': price_evaluable}
                if execution:
                    metadata['execId'] = execution['execId']
                if terminal == 'dead':
                    metadata['superseded'] = 'Cancelled'
                event = self._event(row, EventType.ORDER_FILLED, delta, price, metadata)
                self._outbox(conn, f'{identity}:fill:{new_cumulative:.12g}', event)
                cumulative, notional = new_cumulative, new_notional
            elif new_notional > 0 and row['filled'] == cumulative:
                # Price evidence can arrive after its quantity checkpoint.
                # It establishes cost for future deltas without duplicating
                # the already-recorded execution quantity.
                notional = new_notional
            if terminal == 'filled' and cumulative == 0:
                terminal = None  # discard legacy default Filled/0 pseudo-evidence
            if row['status'] == 'Filled':
                terminal = 'filled'
            elif row['status'] in _DEAD and terminal not in ('filled', 'dead'):
                event_type = EventType.ORDER_REJECTED if row['status'] == 'Inactive' else EventType.ORDER_CANCELLED
                event = self._event(row, event_type, row['totalQuantity'], row['avgFillPrice'],
                                    {'status': row['status'], 'cumulative_filled': cumulative,
                                     'remaining': row['remaining'], 'broker_identity': identity})
                self._outbox(conn, identity + ':dead', event)
                terminal = 'dead'
            conn.execute('INSERT INTO broker_order_progress VALUES (?, ?, ?, ?, ?) '
                         'ON CONFLICT(identity) DO UPDATE SET cumulative=excluded.cumulative, '
                         'notional=excluded.notional, terminal=excluded.terminal, snapshot=excluded.snapshot',
                         [identity, cumulative, notional, terminal, json.dumps(row)])

    @staticmethod
    def _event(row, kind, quantity, price, metadata):
        return TradingEvent(event_type=kind,
                            timestamp=dt.datetime.fromisoformat(row['observed_at']),
                            strategy_name=row['orderRef'], conid=row['conId'], symbol=row['symbol'],
                            action=row['action'], quantity=quantity, price=price,
                            order_id=row['orderId'], metadata=metadata)

    @staticmethod
    def _outbox(conn, identity, event):
        payload = asdict(event)
        payload['event_type'] = event.event_type.value
        payload['timestamp'] = event.timestamp.isoformat()
        conn.execute('INSERT OR IGNORE INTO broker_event_outbox(identity, payload) VALUES (?, ?)',
                     [identity, json.dumps(payload)])

    def _deliver(self):
        if self._journal is None or self._event_store is None:
            return
        with self._journal.transaction() as conn:
            rows = conn.execute('SELECT identity, payload FROM broker_event_outbox WHERE delivered=0 LIMIT 128').fetchall()
        for identity, payload in rows:
            data = json.loads(payload)
            data['event_type'] = EventType(data['event_type'])
            data['timestamp'] = dt.datetime.fromisoformat(data['timestamp'])
            event = TradingEvent(**data)
            append_once = getattr(self._event_store, 'append_once', None)
            if callable(append_once):
                append_once(event, identity)
            else:
                self._event_store.append(event)
            with self._journal.transaction() as conn:
                conn.execute('UPDATE broker_event_outbox SET delivered=1 WHERE identity=?', [identity])
            with self._lock:
                self._pending_submissions.discard(identity)
            self._submission_wake.set()
