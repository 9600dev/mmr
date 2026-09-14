"""Unobserved native sends retain account reduction capacity across restart.

These controls use the real server journal, order coordinator and snapshot
producer with a deterministic IB adapter. A missing send receipt is ambiguous;
the tests neither claim that the first order filled nor perform broker I/O.
"""
import copy
import sqlite3
from types import SimpleNamespace

import pytest
from ib_async import OrderStatus, Position, Stock, StopOrder, Trade

from review.test_review_order_contract import _coordinated_trader, _stock
from trader.data.server_order_journal import EmergencyOrderJournal, ServerOrderJournal
from trader.trading.risk_gate import RiskGate, RiskLimits
from trader.trading.trading_runtime import _CURRENT_INTENT, _CURRENT_JOURNAL


@pytest.fixture
def reservation_server(tmp_path):
    servers = []

    def make():
        server = _coordinated_trader(tmp_path, held=40)
        # A real reconnect continues with a broker-provided unused order ID.
        sequence = iter(range(1 + 100 * len(servers), 100 + 100 * len(servers)))
        server.client.ib.client.getReqId = lambda: next(sequence)
        servers.append(server)
        return server

    yield SimpleNamespace(make=make)
    for server in servers:
        server.order_tracker.close(timeout=1)
        if getattr(server, '_server_order_journal', None) is not None:
            server._server_order_journal.journal.close()


async def uncertain_stop(server, *, contract=None, action="SELL"):
    sent = []
    acknowledged_send = server.client.subscribe_place_order.side_effect

    async def missing_receipt(contract, order):
        reserved, = [row for row in server.server_order_journal().reservations(server.ib_account)
                     if row['intent_id'] == 'uncertain-stop']
        assert (reserved['conid'], reserved['action'], reserved['quantity']) == (contract.conId, action, 40)
        sent.append(copy.deepcopy(order))
        raise ConnectionError("simulated send outcome unavailable")

    server.client.subscribe_place_order.side_effect = missing_receipt
    result = await server.place_standalone_order(
        contract or _stock(), action, 40, "STP", aux_price=8,
        order_ref="server_capacity", client_intent_id="uncertain-stop")
    server.client.subscribe_place_order.side_effect = acknowledged_send
    assert not result.is_success()
    assert len(sent) == 1 and sent[0].orderId > 0
    claim = server.server_order_journal().get("uncertain-stop")
    assert claim["status"] == "UNKNOWN" and len(claim["orders"]) == 1
    scoped = await server.execution_snapshot(intent_id="uncertain-stop")
    assert not scoped["complete"] and not scoped["retry_safe"]
    assert scoped["orders"] == []
    assert server.working_trades(_stock()) == []
    return sent[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("restart", [False, True])
async def test_unobserved_native_stop_reserves_capacity_for_a_distinct_close(
    reservation_server, restart,
):
    server = reservation_server.make()
    await uncertain_stop(server)
    if restart:
        server = reservation_server.make()

    result = await server.place_expressive_order(
        _stock(), "SELL", 40, {"order_type": "LIMIT", "limit_price": 9.5},
        client_intent_id="independent-close")

    assert not server.placed, "uncertain STP40 plus independent SELL40 can exceed held40"
    assert not result.is_success()
    first = server.server_order_journal().get("uncertain-stop")
    assert first["status"] == "UNKNOWN" and len(first["orders"]) == 1
    assert server.inventory == 40


@pytest.mark.asyncio
async def test_exact_known_cancelled_zero_fill_releases_reserved_capacity_after_restart(
    reservation_server,
):
    server = reservation_server.make()
    original = await uncertain_stop(server)
    server = reservation_server.make()
    original.clientId = 7
    original.permId = 1717
    original.filledQuantity = 0
    server.order_tracker.on_trade(Trade(_stock(), original, OrderStatus(
        orderId=original.orderId, permId=1717, status="Cancelled",
        filled=0, remaining=0, avgFillPrice=0)), completed=True)
    proof = await server.execution_snapshot(intent_id="uncertain-stop")
    assert proof["complete"]
    row, = proof["orders"]
    assert row["fillQuantityKnown"] and row["filled"] == 0
    assert row["clientIntentId"] == "uncertain-stop"

    result = await server.place_expressive_order(
        _stock(), "SELL", 40, {"order_type": "LIMIT", "limit_price": 9.5},
        client_intent_id="independent-close")

    assert result.is_success(), result.error
    trade, = server.placed
    assert trade.order.action == "SELL" and trade.order.totalQuantity == 40
    assert trade.order.orderId != original.orderId
    assert server.inventory == 40, "acknowledgement is not a fill"


@pytest.mark.asyncio
async def test_returned_pending_submit_with_default_zero_remaining_still_reserves_total(
    reservation_server,
):
    server = reservation_server.make()
    result = await server.place_standalone_order(
        _stock(), "SELL", 40, "STP", aux_price=8,
        order_ref="other_owner", client_intent_id="pending-stop")
    assert result.is_success()
    first, = server.placed
    # ib_async registers a newly sent Trade in exactly this state, before
    # orderStatus supplies remaining. The original physical quantity is 40.
    first.orderStatus.status = "PendingSubmit"
    first.orderStatus.remaining = 0
    first.orderStatus.filled = 0
    server.order_tracker.on_trade(first)

    replacement = await server.place_standalone_order(
        _stock(), "SELL", 40, "STP", aux_price=7,
        order_ref="another_owner", client_intent_id="another-stop")

    assert len(server.placed) == 1, "PendingSubmit does not certify zero executable shares"
    assert not replacement.is_success()
    server.client.ib.cancelOrder.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize('held,requested,expected', [(140, 40, 40), (60, 40, 20), (60, 10, 10)])
async def test_known_unobserved_quantity_leaves_only_the_proven_free_capacity(
    reservation_server, held, requested, expected,
):
    server = reservation_server.make()
    await uncertain_stop(server)
    server.inventory = held
    result = await server.place_expressive_order(
        _stock(), 'SELL', requested, {'order_type': 'LIMIT', 'limit_price': 9.5},
        client_intent_id='remaining-capacity')
    assert result.is_success(), result.error
    trade, = server.placed
    assert trade.order.totalQuantity == expected
    assert expected + 40 <= held
    server.client.ib.cancelOrder.assert_not_called()
    original, = [row for row in server.server_order_journal().reservations(server.ib_account)
                 if row['intent_id'] == 'uncertain-stop']
    assert original['quantity'] == 40, 'clamping another order cannot shrink this reservation'


@pytest.mark.asyncio
@pytest.mark.parametrize('other_scope', ['account', 'conid', 'direction'])
async def test_unobserved_capacity_is_scoped_to_exact_account_instrument_and_side(
    reservation_server, other_scope,
):
    server = reservation_server.make()
    original_account = server.ib_account
    other = Stock('OTHER', 'SMART', 'USD', conId=200)
    if other_scope == 'account':
        server.ib_account = 'DU_OTHER_FAKE'
    elif other_scope == 'conid':
        server.get_positions = lambda: [Position(server.ib_account, other, server.inventory, 10)]
    else:
        server.inventory = -40
    await uncertain_stop(server, contract=other if other_scope == 'conid' else None,
                         action='BUY' if other_scope == 'direction' else 'SELL')
    server.ib_account, server.inventory = original_account, 40
    server.get_positions = lambda: [Position(server.ib_account, _stock(), server.inventory, 10)]
    result = await server.place_expressive_order(
        _stock(), 'SELL', 40, {'order_type': 'LIMIT', 'limit_price': 9.5},
        client_intent_id='independent-scope')
    assert result.is_success(), result.error
    trade, = server.placed
    assert trade.order.totalQuantity == 40 and trade.order.action == 'SELL'


def terminal(server, order, *, zero_id=False, unknown=False, mismatch=None):
    order = copy.deepcopy(order)
    order.clientId, order.permId = 7, 1717
    if not unknown:
        order.filledQuantity = 0
    if zero_id:
        order.orderId, order.clientId = 0, 0
    contract = _stock()
    if mismatch == 'account':
        order.account = 'DU_OTHER_FAKE'
    elif mismatch == 'client':
        order.clientId = 99
    elif mismatch == 'order':
        order.orderId = 99
    elif mismatch == 'reference':
        order.orderRef = 'other|mmr:other-request'
    elif mismatch == 'conid':
        contract = Stock('OTHER', 'SMART', 'USD', conId=200)
    elif mismatch == 'action':
        order.action = 'BUY'
    trade = Trade(contract, order, OrderStatus(
        orderId=order.orderId, permId=order.permId, status='Cancelled',
        filled=0, remaining=0, avgFillPrice=0))
    server.order_tracker.on_trade(trade, completed=True)
    if mismatch == 'ambiguous':
        # Native replay exposes this flag when a legacy identity collision
        # cannot be repaired without rewriting historical fill evidence.
        snapshot = server.order_tracker.snapshot
        server.order_tracker.snapshot = lambda *args: [dict(row, identityAmbiguous=True)
                                                       for row in snapshot(*args)]


@pytest.mark.asyncio
@pytest.mark.parametrize('mismatch', ['account', 'client', 'order', 'reference', 'conid', 'action', 'unknown', 'ambiguous'])
async def test_terminal_looking_but_unproven_evidence_cannot_release_capacity(
    reservation_server, mismatch,
):
    server = reservation_server.make()
    order = await uncertain_stop(server)
    terminal(server, order, mismatch=mismatch, unknown=mismatch == 'unknown')
    result = await server.place_expressive_order(
        _stock(), 'SELL', 40, {'order_type': 'LIMIT', 'limit_price': 9.5},
        client_intent_id='unproven-terminal')
    assert not result.is_success()
    assert not server.placed


@pytest.mark.asyncio
async def test_singleton_permanent_identity_can_retire_the_exact_unknown_send(reservation_server):
    server = reservation_server.make()
    order = await uncertain_stop(server)
    terminal(server, order, zero_id=True)
    result = await server.place_expressive_order(
        _stock(), 'SELL', 40, {'order_type': 'LIMIT', 'limit_price': 9.5},
        client_intent_id='after-singleton-terminal')
    assert result.is_success(), result.error
    assert len(server.placed) == 1
    assert all(row['intent_id'] != 'uncertain-stop'
               for row in server.server_order_journal().reservations(server.ib_account))


@pytest.mark.asyncio
async def test_zero_id_proof_cannot_retire_two_legs_split_between_journals(reservation_server):
    server = reservation_server.make()
    order = await uncertain_stop(server)
    server.inventory = 80
    emergency = EmergencyOrderJournal()
    emergency.claim('uncertain-stop', 'reservation-write-failed', server.ib_account)
    emergency.reserve_order('uncertain-stop', 2, 7, conid=100, action='SELL', quantity=40, is_exit=True)
    emergency.finish('uncertain-stop', 'UNKNOWN')
    server._emergency_order_journal = emergency
    terminal(server, order, zero_id=True)
    result = await server.place_expressive_order(
        _stock(), 'SELL', 40, {'order_type': 'LIMIT', 'limit_price': 9.5},
        client_intent_id='after-ambiguous-terminal')
    assert not result.is_success()
    assert not server.placed
    assert len(server.server_order_journal().reservations(server.ib_account)) == 1
    assert len(emergency.reservations(server.ib_account)) == 1


def _legacy_claim(server, client_id=7):
    """A migrated claim: physical identity only, no instrument/side/size."""
    journal = server.server_order_journal()
    journal.claim('legacy', 'old-payload', server.ib_account)
    journal.reserve_order('legacy', 17, client_id)
    journal.finish('legacy', 'SUBMITTED')
    return journal


@pytest.mark.asyncio
async def test_legacy_reserved_identity_defers_until_broker_replay_is_complete(reservation_server):
    """Before this process has replayed open orders and execution history, an
    unobserved legacy identity proves nothing and keeps every reduction pending."""
    server = reservation_server.make()
    journal = _legacy_claim(server)
    server._execution_history_ready = False
    result = await server.place_expressive_order(
        _stock(), 'SELL', 40, {'order_type': 'LIMIT', 'limit_price': 9.5},
        client_intent_id='after-legacy')
    assert not result.is_success() and 'UNKNOWN' in result.error
    assert not server.placed
    row, = journal.reservations(server.ib_account)
    assert row['conid'] is row['quantity'] is row['action'] is None


@pytest.mark.asyncio
async def test_legacy_identity_of_this_client_unobserved_after_replay_releases_capacity(reservation_server):
    """Found in review (2026-09-11): the migration writes a NULL-scoped claim for
    every leg of every pre-existing intent, and the scope check raised BEFORE the
    per-contract filter, so one orphan legacy leg blocked every non-protective
    exit on every instrument until the SQLite journal was hand-edited.

    After a complete replay every working order under this client id has been
    observed with that id; an absent legacy identity is therefore not working
    and carries no future executable capacity. It is settled and the exit
    proceeds at full size."""
    server = reservation_server.make()
    journal = _legacy_claim(server, client_id=server.trading_runtime_ib_client_id)
    result = await server.place_expressive_order(
        _stock(), 'SELL', 40, {'order_type': 'LIMIT', 'limit_price': 9.5},
        client_intent_id='after-legacy')
    assert result.is_success(), result.error
    trade, = server.placed
    assert trade.order.action == 'SELL' and trade.order.totalQuantity == 40
    unsettled = {row['intent_id'] for row in journal.reservations(server.ib_account)}
    assert unsettled == {'after-legacy'}, 'the legacy claim is settled durably; only the new order reserves'
    assert server.inventory == 40, 'acknowledgement is not a fill'


@pytest.mark.asyncio
async def test_legacy_identity_of_another_client_keeps_deferring_after_replay(reservation_server):
    """Another client's working orders are reported with orderId 0 and can never
    be matched by identity, so a legacy claim under a foreign client id is not
    released by this process's replay."""
    server = reservation_server.make()
    journal = _legacy_claim(server, client_id=server.trading_runtime_ib_client_id + 1)
    result = await server.place_expressive_order(
        _stock(), 'SELL', 40, {'order_type': 'LIMIT', 'limit_price': 9.5},
        client_intent_id='after-legacy')
    assert not result.is_success() and 'UNKNOWN' in result.error
    assert not server.placed
    row, = journal.reservations(server.ib_account)
    assert row['conid'] is row['quantity'] is row['action'] is None


@pytest.mark.parametrize('field,value', [('conid', 200), ('action', 'BUY'), ('quantity', 20), ('is_exit', False), ('broker_reference', 'other')])
def test_physical_reservation_metadata_cannot_be_rewritten(tmp_path, field, value):
    journal = ServerOrderJournal(str(tmp_path / 'immutable.duckdb'))
    metadata = dict(conid=100, action='SELL', quantity=40, is_exit=True, broker_reference='immutable')
    try:
        journal.claim('immutable', 'payload', 'DU_FAKE')
        journal.reserve_order('immutable', 17, 7, **metadata)
        with pytest.raises(ValueError, match='immutable'):
            journal.reserve_order('immutable', 17, 7, **dict(metadata, **{field: value}))
        row, = journal.reservations('DU_FAKE')
        assert {key: row[key] for key in metadata} == metadata
        assert journal.get('immutable')['orders'] == [{'orderId': 17, 'clientId': 7}]
    finally:
        journal.journal.close()


def test_migration_retains_unknown_legacy_scope_without_rewriting_claims(tmp_path):
    path = str(tmp_path / 'old.duckdb')
    old = sqlite3.connect(path + '.execution.sqlite3')
    old.execute('''CREATE TABLE server_order_intents (
        intent_id TEXT PRIMARY KEY,fingerprint TEXT,account TEXT,status TEXT,
        orders TEXT,error TEXT,outcome TEXT,created_at REAL)''')
    original = ('legacy', 'payload', 'DU_FAKE', 'UNKNOWN', '[{"orderId":17,"clientId":7}]', '', None, 12345.0)
    old.execute('INSERT INTO server_order_intents VALUES (?,?,?,?,?,?,?,?)', original)
    old.commit()
    old.close()
    for _ in range(2):
        journal = ServerOrderJournal(path)
        try:
            with journal.journal.transaction() as connection:
                assert tuple(connection.execute('SELECT * FROM server_order_intents').fetchone()) == original
            row, = journal.reservations('DU_FAKE')
            assert row['conid'] is row['action'] is row['quantity'] is row['broker_reference'] is None
            assert (row['client_id'], row['order_id']) == (7, 17)
        finally:
            journal.journal.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('warm_cache', [False, True])
async def test_capacity_read_failure_requires_a_complete_process_cache(
    reservation_server, monkeypatch, warm_cache,
):
    server = reservation_server.make()
    journal = server.server_order_journal()
    if warm_cache:
        assert await server.unobserved_reduction_quantity(_stock(), 'SELL') == 0
    monkeypatch.setattr(journal, 'reservations', lambda _account: (_ for _ in ()).throw(OSError('read unavailable')))
    result = await server.place_expressive_order(
        _stock(), 'SELL', 40, {'order_type': 'LIMIT', 'limit_price': 9.5},
        client_intent_id='read-failure')
    if warm_cache:
        assert result.is_success(), result.error
        assert len(server.placed) == 1
        # Every later local reserve updates the previously complete cache.
        server.placed.clear()
        server._submitted_trades.clear()
        later = await server.place_expressive_order(
            _stock(), 'SELL', 40, {'order_type': 'LIMIT', 'limit_price': 9.5},
            client_intent_id='second-read-failure')
        assert not later.is_success() and not server.placed
    else:
        assert not result.is_success() and not server.placed


@pytest.mark.asyncio
async def test_visible_stop_and_native_same_quantity_oca_modification_count_once(reservation_server):
    server = reservation_server.make()
    server.inventory = 100
    for index in range(2):
        result = await server.place_standalone_order(
            _stock(), 'SELL', 50, 'STP', aux_price=8,
            order_ref='owner', client_intent_id=f'tranche-{index}')
        assert result.is_success(), result.error
    result = await server.resize_position(_stock(), 50, client_intent_id='oca-resize')
    assert result.is_success(), result.error
    assert len(server.placed) == 3
    server.client.ib.cancelOrder.assert_not_called()
    trim, = [trade for trade in server.placed if trade.order.orderType == 'MKT']
    paired, = [trade for trade in server.placed if trade.order.orderType == 'STP'
               and trade.order.ocaGroup == trim.order.ocaGroup]
    assert paired.order.ocaType == trim.order.ocaType == 2
    assert paired.order.totalQuantity == trim.order.totalQuantity == 50
    assert await server.unobserved_reduction_quantity(_stock(), 'SELL') == 0


@pytest.mark.asyncio
async def test_complete_cache_and_emergency_journal_preserve_available_reduction_capacity(
    reservation_server, monkeypatch,
):
    server = reservation_server.make()
    await uncertain_stop(server)
    server.inventory = 60
    monkeypatch.setattr(server, 'server_order_journal', lambda: (_ for _ in ()).throw(OSError('sidecar unavailable')))
    result = await server.place_expressive_order(
        _stock(), 'SELL', 40, {'order_type': 'LIMIT', 'limit_price': 9.5},
        client_intent_id='emergency-free-capacity')
    assert result.is_success(), result.error
    trade, = server.placed
    assert trade.order.totalQuantity == 20
    row, = server._emergency_order_journal.reservations(server.ib_account)
    assert row['quantity'] == 20 and row['conid'] == 100
    server.client.ib.cancelOrder.assert_not_called()


@pytest.mark.asyncio
async def test_first_healthy_open_warms_full_account_capacity_before_a_later_outage(
    reservation_server, monkeypatch,
):
    server = reservation_server.make()
    server.inventory = 0
    server.risk_gate = RiskGate(RiskLimits(), server.event_store)
    result = await server.place_expressive_order(
        _stock(), 'BUY', 1, {'order_type': 'LIMIT', 'limit_price': 9.5},
        client_intent_id='healthy-opening')
    assert result.is_success(), result.error
    monkeypatch.setattr(server, 'server_order_journal', lambda: (_ for _ in ()).throw(OSError('later outage')))
    assert await server.unobserved_reduction_quantity(_stock(), 'SELL') == 0


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['partial-emergency', 'failed-durable-read'])
async def test_partial_local_reservations_cannot_initialize_a_complete_cache(
    reservation_server, monkeypatch, kind,
):
    server = reservation_server.make()
    durable = server.server_order_journal()
    durable.claim('hidden-older', 'old', server.ib_account)
    durable.reserve_order('hidden-older', 77, 7, conid=100, action='SELL', quantity=40, is_exit=True)
    journal = EmergencyOrderJournal() if kind == 'partial-emergency' else durable
    if kind == 'partial-emergency':
        server._emergency_order_journal = journal
    journal.claim('new-local', 'new', server.ib_account)
    monkeypatch.setattr(durable, 'reservations', lambda _account: (_ for _ in ()).throw(OSError('prior claims unreadable')))
    intent_token = _CURRENT_INTENT.set('new-local')
    journal_token = _CURRENT_JOURNAL.set(journal)
    try:
        order = StopOrder('SELL', 1, 8, account=server.ib_account)
        await server.reserve_broker_order(order, is_exit=True, contract=_stock())
    finally:
        _CURRENT_JOURNAL.reset(journal_token)
        _CURRENT_INTENT.reset(intent_token)
    assert journal.get('new-local')['orders'], 'pre-wire reservation remains recorded'
    assert getattr(server, '_server_reservation_cache', None) is None
    with pytest.raises(RuntimeError, match='UNKNOWN'):
        await server.unobserved_reduction_quantity(_stock(), 'SELL')
