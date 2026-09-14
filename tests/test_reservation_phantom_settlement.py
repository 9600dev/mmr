"""A phantom "unconfirmed send" reservation must have a way out.

Field origin (2026-09-14): ``DEFERRED: SELL 40 not placed: all 40 held are
reserved by working reductions owned elsewhere [] and 40 by unconfirmed
earlier sends; that owner must retire them`` — a reservation whose send never
reached IB blocked every reduction and protective stop on the instrument
forever. Nothing listed it, nothing could settle it, and the message named
no tool. These tests pin the operator path (list -> settle -> close proceeds),
the refusals that keep that path from hiding real exposure, and the one
narrow automatic settlement.
"""
import time
from types import SimpleNamespace

import pytest
from ib_async import Contract, Order, OrderStatus, Trade

from review.test_review_order_contract import _coordinated_trader, _stock
from test_server_reduction_reservations import uncertain_stop
from trader.messaging.trader_service_api import TraderServiceApi


@pytest.fixture
def phantom_server(tmp_path):
    servers = []

    def make():
        server = _coordinated_trader(tmp_path, held=40)
        sequence = iter(range(1 + 100 * len(servers), 100 + 100 * len(servers)))
        server.client.ib.client.getReqId = lambda: next(sequence)
        # status() reads these; the review fixture does not set them.
        server._ib_upstream_connected = True
        server._ib_upstream_error = ''
        server.data = None
        servers.append(server)
        return server

    yield SimpleNamespace(make=make)
    for server in servers:
        server.order_tracker.close(timeout=1)
        if getattr(server, '_server_order_journal', None) is not None:
            server._server_order_journal.journal.close()


def _close(server, intent_id='independent-close'):
    return server.place_expressive_order(
        _stock(), 'SELL', 40, {'order_type': 'LIMIT', 'limit_price': 9.5},
        client_intent_id=intent_id)


@pytest.mark.asyncio
async def test_phantom_reservation_deferral_names_the_identity_and_the_tool(phantom_server):
    server = phantom_server.make()
    sent = await uncertain_stop(server)

    result = await _close(server)

    assert not result.is_success() and 'DEFERRED' in result.error
    assert 'unconfirmed earlier sends' in result.error
    assert f'uncertain-stop/7/{sent.orderId} (40)' in result.error, result.error
    assert 'mmr reservations' in result.error
    assert 'mmr reservations settle <intent_id> <client_id> <order_id> --reason' in result.error
    assert not server.placed


@pytest.mark.asyncio
async def test_listing_shows_the_phantom_as_blocking_with_no_observation(phantom_server):
    server = phantom_server.make()
    sent = await uncertain_stop(server)

    rows = await TraderServiceApi(server).list_order_reservations()

    row, = [r for r in rows if r['intent_id'] == 'uncertain-stop']
    assert (row['client_id'], row['order_id']) == (7, sent.orderId)
    assert (row['conid'], row['action'], row['quantity']) == (_stock().conId, 'SELL', 40)
    assert row['observation'] is None
    assert row['blocking'] is True and row['reserved_quantity'] == 40
    assert row['settled'] is False and row['intent_status'] == 'UNKNOWN'
    assert row['reserved_at'] and row['created']
    assert server.status()['unsettled_reservations'] == 1


@pytest.mark.asyncio
async def test_operator_settlement_releases_the_phantom_and_the_next_close_proceeds(phantom_server):
    server = phantom_server.make()
    sent = await uncertain_stop(server)
    api = TraderServiceApi(server)

    settled = await api.settle_order_reservation(
        'uncertain-stop', 7, sent.orderId, 'broker confirms no order with this id was ever received')

    assert settled['settled'] is True, settled
    assert settled['actor'] == 'operator'
    assert settled['reason'].startswith('broker confirms')
    audit, = server.server_order_journal().settlements(server.ib_account)
    assert (audit['intent_id'], audit['client_id'], audit['order_id']) == ('uncertain-stop', 7, sent.orderId)
    assert audit['actor'] == 'operator' and audit['evidence']['rule'] == 'operator'
    rows = await api.list_order_reservations(include_settled=True)
    assert [r for r in rows if r['intent_id'] == 'uncertain-stop'][0]['settled'] is True
    assert await api.list_order_reservations() == []
    assert server.status()['unsettled_reservations'] == 0

    result = await _close(server)

    assert result.is_success(), result.error
    trade, = server.placed
    assert trade.order.action == 'SELL' and trade.order.totalQuantity == 40
    assert server.inventory == 40, 'acknowledgement is not a fill'


@pytest.mark.asyncio
async def test_settlement_survives_restart(phantom_server):
    server = phantom_server.make()
    sent = await uncertain_stop(server)
    assert (await server.settle_order_reservation('uncertain-stop', 7, sent.orderId, 'never reached IB'))['settled']

    server = phantom_server.make()
    result = await _close(server)

    assert result.is_success(), result.error
    assert len(server.placed) == 1


@pytest.mark.asyncio
async def test_settlement_requires_a_reason_and_an_existing_unsettled_row(phantom_server):
    server = phantom_server.make()
    sent = await uncertain_stop(server)

    for reason in ('', '   '):
        refused = await server.settle_order_reservation('uncertain-stop', 7, sent.orderId, reason)
        assert refused['settled'] is False and 'reason' in refused['error']
    missing = await server.settle_order_reservation('no-such-intent', 7, sent.orderId, 'x')
    assert missing['settled'] is False and 'no unsettled reservation' in missing['error']
    assert not (await _close(server)).is_success(), 'nothing was released'


@pytest.mark.asyncio
@pytest.mark.parametrize('status', ['Submitted', 'PreSubmitted', 'PendingSubmit', 'Unknown'])
async def test_settlement_refused_while_a_broker_observation_matches(phantom_server, status):
    """A tracker row for the identity means the send DID reach IB. Only exact
    broker final-quantity evidence may retire it; the operator cannot."""
    server = phantom_server.make()
    sent = await uncertain_stop(server)
    sent.clientId = 7
    server.order_tracker.on_trade(Trade(_stock(), sent, OrderStatus(
        orderId=sent.orderId, status=status, filled=0, remaining=40)))

    refused = await server.settle_order_reservation('uncertain-stop', 7, sent.orderId, 'operator says so')

    assert refused['settled'] is False
    assert 'broker evidence matches' in refused['error'], refused
    assert refused['observation'][0]['status'] == status
    assert server.server_order_journal().settlements(server.ib_account) == []
    rows = await server.list_order_reservations()
    assert [r for r in rows if r['intent_id'] == 'uncertain-stop'][0]['settled'] is False


@pytest.mark.asyncio
async def test_settlement_refused_while_the_broker_view_is_incomplete(phantom_server):
    server = phantom_server.make()
    sent = await uncertain_stop(server)
    server.order_tracker.begin_replay()

    refused = await server.settle_order_reservation('uncertain-stop', 7, sent.orderId, 'x')

    assert refused['settled'] is False and 'broker view incomplete' in refused['error']


@pytest.mark.asyncio
async def test_settlement_refused_when_ib_async_saw_the_identity_this_session(phantom_server):
    server = phantom_server.make()
    sent = await uncertain_stop(server)
    server.client.ib.trades = lambda: [Trade(_stock(), Order(orderId=sent.orderId, clientId=7),
                                             OrderStatus(orderId=sent.orderId, status='PendingSubmit'))]

    refused = await server.settle_order_reservation('uncertain-stop', 7, sent.orderId, 'x')

    assert refused['settled'] is False and 'placeOrder' in refused['error']


@pytest.mark.asyncio
async def test_settlement_refused_when_not_durable(phantom_server, monkeypatch):
    """An in-memory-only release would come back after a restart and would
    have let a close through in between: no durable audit row, no release."""
    server = phantom_server.make()
    sent = await uncertain_stop(server)
    journal = server.server_order_journal()
    monkeypatch.setattr(journal, 'record_settlement',
                        lambda *args, **kwargs: (_ for _ in ()).throw(OSError('database is locked')))

    refused = await server.settle_order_reservation('uncertain-stop', 7, sent.orderId, 'x')

    assert refused['settled'] is False and 'not persisted' in refused['error']
    monkeypatch.undo()
    assert await server.unobserved_reduction_quantity(_stock(), 'SELL') == 40
    assert journal.settlements(server.ib_account) == []
    assert not (await _close(server)).is_success()


# --- the narrow automatic settlement -----------------------------------------

def _age_reservation(server, intent_id, seconds):
    with server.server_order_journal().journal.transaction() as conn:
        conn.execute('UPDATE server_order_reservations SET reserved_at=? WHERE intent_id=?',
                     (time.time() - seconds, intent_id))


@pytest.mark.asyncio
async def test_own_session_unobserved_send_settles_automatically_with_audit(phantom_server):
    """Reserved under THIS client id, in the current IB session, after this
    process's replay completed, never seen by the tracker or by ib_async,
    older than the callback grace: the send did not reach IB."""
    server = phantom_server.make()
    sent = await uncertain_stop(server)
    server.client.ib.trades = lambda: []
    server._execution_replay_completed_epoch = time.time() - 120
    _age_reservation(server, 'uncertain-stop', 60)

    assert await server.unobserved_reduction_quantity(_stock(), 'SELL') == 0

    audit, = server.server_order_journal().settlements(server.ib_account)
    assert (audit['intent_id'], audit['client_id'], audit['order_id']) == ('uncertain-stop', 7, sent.orderId)
    assert audit['actor'] == 'trader_service:automatic'
    assert audit['evidence']['rule'] == 'phantom-own-session'
    assert await server.list_order_reservations() == []
    result = await _close(server)
    assert result.is_success(), result.error


@pytest.mark.asyncio
@pytest.mark.parametrize('why', [
    'predates-replay', 'too-young', 'seen-by-ib-async', 'trades-unreadable', 'other-client',
    'replay-incomplete', 'no-replay-epoch',
])
async def test_automatic_settlement_does_not_apply_outside_its_conditions(phantom_server, why):
    server = phantom_server.make()
    sent = await uncertain_stop(server)
    server.client.ib.trades = lambda: []
    server._execution_replay_completed_epoch = time.time() - 120
    _age_reservation(server, 'uncertain-stop', 60)
    if why == 'predates-replay':
        server._execution_replay_completed_epoch = time.time() - 10
    elif why == 'too-young':
        _age_reservation(server, 'uncertain-stop', 5)
    elif why == 'seen-by-ib-async':
        server.client.ib.trades = lambda: [Trade(_stock(), Order(orderId=sent.orderId, clientId=7),
                                                 OrderStatus(orderId=sent.orderId, status='PendingSubmit'))]
    elif why == 'trades-unreadable':
        del server.client.ib.trades
    elif why == 'other-client':
        with server.server_order_journal().journal.transaction() as conn:
            conn.execute('UPDATE server_order_reservations SET client_id=8 WHERE intent_id=?', ('uncertain-stop',))
    elif why == 'replay-incomplete':
        server.order_tracker.begin_replay()
    elif why == 'no-replay-epoch':
        server._execution_replay_completed_epoch = None

    assert await server.unobserved_reduction_quantity(_stock(), 'SELL') == 40
    assert server.server_order_journal().settlements(server.ib_account) == []
    assert not (await _close(server)).is_success()


@pytest.mark.asyncio
async def test_legacy_reservation_without_reserved_at_is_never_auto_settled(phantom_server):
    """Rows migrated before reserved_at existed have no provenance."""
    server = phantom_server.make()
    await uncertain_stop(server)
    server.client.ib.trades = lambda: []
    server._execution_replay_completed_epoch = time.time() - 120
    with server.server_order_journal().journal.transaction() as conn:
        conn.execute('UPDATE server_order_reservations SET reserved_at=NULL')

    assert await server.unobserved_reduction_quantity(_stock(), 'SELL') == 40
    assert server.server_order_journal().settlements(server.ib_account) == []


@pytest.mark.asyncio
async def test_matched_working_reservation_is_listed_as_not_blocking(phantom_server):
    server = phantom_server.make()
    server.inventory = 100
    server.get_positions = lambda: [__import__('ib_async').Position(server.ib_account, _stock(), 100, 10)]
    assert (await server.place_standalone_order(_stock(), 'SELL', 40, 'STP', aux_price=8,
                                                client_intent_id='working-stop')).is_success()

    row, = await server.list_order_reservations()

    assert row['intent_id'] == 'working-stop'
    assert row['observation']['status'] == 'Submitted'
    assert row['blocking'] is False and row['reserved_quantity'] == 0
    assert server.status()['unsettled_reservations'] == 1, 'unsettled is not the same as blocking'
