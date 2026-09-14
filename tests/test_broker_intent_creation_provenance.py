"""Immutable server-claim time is provenance, never a replay observation time."""

from contextlib import contextmanager
import threading
from types import SimpleNamespace

import pytest
from ib_async.order import Order, OrderStatus, Trade

from review.test_review_order_contract import _coordinated_trader, _stock
from trader.data.execution_journal import ExecutionJournal
from trader.data.server_order_journal import ServerOrderJournal
import trader.data.server_order_journal as journal_module


INTENT = 'protective:immutable-claim'
CREATED = 1_700_000_000.25


@pytest.fixture
def claim_clock(monkeypatch):
    clock = [CREATED]
    monkeypatch.setattr(journal_module, 'time', SimpleNamespace(time=lambda: clock[0]), raising=False)
    return clock


def test_claim_time_survives_retry_reservation_finish_and_restart(tmp_path, claim_clock):
    path = str(tmp_path / 'orders.duckdb')
    journal = ServerOrderJournal(path)
    try:
        assert journal.claim(INTENT, 'payload', 'DU_FAKE')
        assert journal.get(INTENT)['created_at'] == CREATED
        claim_clock[0] += 600
        journal.finish(INTENT, 'RETRYABLE', 'coordination deferred')
        assert journal.claim(INTENT, 'payload', 'DU_FAKE')
        assert journal.get(INTENT)['created_at'] == CREATED
        journal.reserve_order(INTENT, 17, 7)
        journal.discard_order(INTENT, 17, 7)
        journal.reserve_order(INTENT, 18, 7)
        journal.finish(INTENT, 'SUBMITTED', outcome={'order_ids': [18]})
        assert not journal.claim(INTENT, 'payload', 'DU_FAKE')
        assert journal.get(INTENT)['created_at'] == CREATED
    finally:
        journal.journal.close()
    restarted = ServerOrderJournal(path)
    try:
        assert restarted.get(INTENT)['created_at'] == CREATED
        assert restarted.get_many([INTENT])[INTENT]['created_at'] == CREATED
    finally:
        restarted.journal.close()


def test_legacy_migration_preserves_original_fields_and_unknown_creation(tmp_path, claim_clock):
    path = str(tmp_path / 'legacy.duckdb')
    old = ExecutionJournal(path)
    before = dict(intent_id=INTENT, fingerprint='old-payload', account='DU_FAKE',
                  status='RETRYABLE', orders='[]', error='old-error', outcome=None)
    with old.transaction() as conn:
        conn.execute('''CREATE TABLE server_order_intents (
            intent_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, account TEXT NOT NULL,
            status TEXT NOT NULL, orders TEXT NOT NULL, error TEXT NOT NULL, outcome TEXT)''')
        conn.execute('INSERT INTO server_order_intents VALUES (?,?,?,?,?,?,?)', tuple(before.values()))
    old.close()

    for attempt in range(2):
        journal = ServerOrderJournal(path)
        try:
            with journal.journal.transaction() as conn:
                raw = dict(conn.execute('SELECT * FROM server_order_intents').fetchone())
            assert raw.pop('created_at') is None, 'migration cannot invent an original claim time'
            if attempt == 0:
                assert raw == before
                assert journal.claim(INTENT, 'old-payload', 'DU_FAKE')
                journal.reserve_order(INTENT, 17, 7)
                journal.finish(INTENT, 'SUBMITTED')
            assert journal.get(INTENT)['created_at'] is None
        finally:
            journal.journal.close()
        claim_clock[0] += 1000


def test_bulk_provenance_reads_only_exact_requested_ids_in_one_transaction(tmp_path, claim_clock, monkeypatch):
    journal = ServerOrderJournal(str(tmp_path / 'bulk.duckdb'))
    try:
        for name in (INTENT, INTENT.upper(), 'unrelated'):
            journal.claim(name, name, 'DU_FAKE')
            journal.reserve_order(name, 17, 7)
        original = journal.journal.transaction
        transactions = []

        @contextmanager
        def counted():
            transactions.append(True)
            with original() as conn:
                yield conn

        monkeypatch.setattr(journal.journal, 'transaction', counted)
        rows = journal.get_many([INTENT, INTENT, 'missing'])
        assert list(rows) == [INTENT]
        assert rows[INTENT]['account'] == 'DU_FAKE'
        assert rows[INTENT]['orders'] == [{'orderId': 17, 'clientId': 7}]
        assert rows[INTENT]['created_at'] == CREATED
        assert len(transactions) == 1
    finally:
        journal.journal.close()


@pytest.fixture
def provenance_trader(tmp_path, claim_clock):
    trader = _coordinated_trader(tmp_path)
    journal = trader.server_order_journal()
    try:
        yield trader, journal
    finally:
        trader.order_tracker.close()
        journal.journal.close()


def reserve(journal, account, order_ids=(17,), intent=INTENT):
    journal.claim(intent, intent, account)
    for order_id in order_ids:
        journal.reserve_order(intent, order_id, 7)
    journal.finish(intent, 'SUBMITTED')


def observe(trader, *, order_id=17, client_id=7, perm_id=1717, reference=INTENT,
            account=None, completed=False):
    ref = 'owner' + ('|mmr:' + reference if reference else '')
    trade = Trade(_stock(), Order(orderId=order_id, clientId=client_id, permId=perm_id,
                                 account=account or trader.ib_account, action='SELL',
                                 orderType='STP', totalQuantity=10, filledQuantity=10, orderRef=ref),
                  OrderStatus(status='Filled', filled=10, avgFillPrice=10))
    trader.order_tracker.on_trade(trade, completed=completed)


@pytest.mark.asyncio
@pytest.mark.parametrize('scoped', [False, True])
async def test_snapshot_stamps_exact_native_intent_and_physical_identity(provenance_trader, scoped):
    trader, journal = provenance_trader
    reserve(journal, trader.ib_account)
    observe(trader)
    snapshot = await trader.execution_snapshot(intent_id=INTENT if scoped else '')
    assert snapshot['complete']
    row, = snapshot['orders']
    assert row['clientIntentId'] == INTENT
    assert row['brokerIntentCreatedAt'] == CREATED


@pytest.mark.asyncio
@pytest.mark.parametrize('mismatch', ['account', 'client', 'order', 'reference', 'missing_reference'])
async def test_unproven_identity_has_no_creation_hint(provenance_trader, mismatch):
    trader, journal = provenance_trader
    reserve(journal, 'DU_OTHER' if mismatch == 'account' else trader.ib_account)
    changes = {'client': {'client_id': 99}, 'order': {'order_id': 18},
               'reference': {'reference': 'other-intent'}, 'missing_reference': {'reference': ''}}
    observe(trader, **changes.get(mismatch, {}))
    snapshot = await trader.execution_snapshot()
    assert snapshot['complete'], 'optional creation provenance does not redefine broker completeness'
    row, = snapshot['orders']
    assert 'brokerIntentCreatedAt' not in row


@pytest.mark.asyncio
async def test_scoped_numeric_fallback_does_not_invent_reference_provenance(provenance_trader):
    trader, journal = provenance_trader
    reserve(journal, trader.ib_account)
    observe(trader, reference='')
    snapshot = await trader.execution_snapshot(intent_id=INTENT, order_ids=[17])
    assert snapshot['complete']
    row, = snapshot['orders']
    assert row['clientIntentId'] == INTENT  # existing scoped numeric recovery is unchanged
    assert 'brokerIntentCreatedAt' not in row


@pytest.mark.asyncio
@pytest.mark.parametrize('scoped', [False, True])
async def test_single_leg_permanent_only_replay_can_recover_creation(provenance_trader, scoped):
    trader, journal = provenance_trader
    reserve(journal, trader.ib_account)
    observe(trader, order_id=0, client_id=0, completed=True)
    snapshot = await trader.execution_snapshot(intent_id=INTENT if scoped else '')
    assert snapshot['complete']
    row, = snapshot['orders']
    assert row['orderId'] == 0 and row['permId'] == 1717
    assert row['brokerIntentCreatedAt'] == CREATED


@pytest.mark.asyncio
@pytest.mark.parametrize('ambiguity', ['two_legs', 'two_observations', 'filtered_observations',
                                     'contradictory_client', 'extra_requested_id'])
async def test_permanent_only_provenance_requires_unambiguous_single_leg(provenance_trader, ambiguity):
    trader, journal = provenance_trader
    reserve(journal, trader.ib_account, (17, 18) if ambiguity == 'two_legs' else (17,))
    if ambiguity == 'filtered_observations':
        # Native client 0 can query its completed (0, 0) receipt while the
        # second same-reference observation stays outside the numeric filter.
        trader.trading_runtime_ib_client_id = 0
    observe(trader, order_id=0, client_id=99 if ambiguity == 'contradictory_client' else 0,
            completed=True)
    if ambiguity in ('two_observations', 'filtered_observations'):
        observe(trader, order_id=18, perm_id=1818)
    snapshot = await trader.execution_snapshot(
        intent_id=INTENT if ambiguity == 'extra_requested_id' else '',
        order_ids=([17, 18] if ambiguity == 'extra_requested_id'
                   else [0] if ambiguity == 'filtered_observations' else None))
    row = next(row for row in snapshot['orders'] if row['orderId'] == 0)
    assert 'brokerIntentCreatedAt' not in row


@pytest.mark.asyncio
@pytest.mark.parametrize('created_at', [None, -1.0, float('inf'), 'not-a-time'])
async def test_unknown_or_invalid_persisted_time_has_no_hint(provenance_trader, created_at):
    trader, journal = provenance_trader
    reserve(journal, trader.ib_account)
    with journal.journal.transaction() as conn:
        conn.execute('UPDATE server_order_intents SET created_at=? WHERE intent_id=?', (created_at, INTENT))
    observe(trader)
    snapshot = await trader.execution_snapshot()
    assert snapshot['complete']
    assert 'brokerIntentCreatedAt' not in snapshot['orders'][0]


@pytest.mark.asyncio
async def test_bulk_provenance_lookup_runs_off_the_broker_event_loop(provenance_trader, monkeypatch):
    trader, journal = provenance_trader
    reserve(journal, trader.ib_account)
    observe(trader)
    main_thread = threading.get_ident()
    calls = []

    def lookup(ids):
        calls.append((list(ids), threading.get_ident()))
        return {INTENT: dict(account=trader.ib_account, orders=[dict(orderId=17, clientId=7)],
                             created_at=CREATED)}

    monkeypatch.setattr(journal, 'get_many', lookup, raising=False)
    snapshot = await trader.execution_snapshot()
    assert snapshot['orders'][0]['brokerIntentCreatedAt'] == CREATED
    assert len(calls) == 1 and calls[0][0] == [INTENT]
    assert calls[0][1] != main_thread


@pytest.mark.asyncio
async def test_provenance_read_failure_preserves_complete_snapshot(provenance_trader, monkeypatch):
    trader, journal = provenance_trader
    reserve(journal, trader.ib_account)
    observe(trader)

    def unavailable(_ids):
        raise OSError('provenance journal is unreadable')

    monkeypatch.setattr(journal, 'get_many', unavailable, raising=False)
    snapshot = await trader.execution_snapshot()
    assert snapshot['complete'] and snapshot['positions_complete']
    assert snapshot['orders'][0]['filled'] == 10
    assert 'brokerIntentCreatedAt' not in snapshot['orders'][0]
