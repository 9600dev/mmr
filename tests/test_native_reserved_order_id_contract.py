"""The physical order ID on the native wire must be reserved beforehand."""

import asyncio
import struct

import pytest
from ib_async import IB, Stock, StopOrder

from trader.common.reactivex import EventSubject
from trader.data.server_order_journal import ServerOrderJournal
from trader.listeners.ibreactive import IBAIORx
from trader.trading.trading_runtime import Trader, _CURRENT_INTENT, _CURRENT_JOURNAL


def _incoming(*fields):
    payload = "\0".join(str(field) for field in fields).encode() + b"\0"
    return struct.pack(">I", len(payload)) + payload


@pytest.mark.asyncio
@pytest.mark.parametrize("next_valid_id", [0, 17])
async def test_native_wire_identity_matches_the_preexisting_reservation(
    tmp_path, monkeypatch, next_valid_id,
):
    native = IB()
    outgoing = []
    # Replace only transport output. Native framing, readiness, ID allocation,
    # IB.placeOrder and the IBAIORx submission adapter remain real.
    monkeypatch.setattr(native.client.conn, "sendMsg", outgoing.append)
    native.client.clientId = 7
    native.wrapper.clientId = 7
    native.client._onSocketHasData(_incoming(native.client.MaxClientVersion, "20260909 22:00:00"))
    native.client._onSocketHasData(_incoming(9, 1, next_valid_id))
    native.client._onSocketHasData(_incoming(15, 1, "DU_FAKE,"))
    assert native.client.isReady()
    assert native.client._reqIdSeq == next_valid_id

    # This is the low-level-ready boundary inside reconnect, before native
    # high-level startup requests consume IDs. An already admitted order can
    # resume here from its off-loop journal read; this is not a claim that a
    # completed IBAIORx.connect() normally leaves request ID zero unused.
    adapter = IBAIORx.__new__(IBAIORx)
    adapter.ib = native
    adapter.trades_subject = EventSubject(native.orderStatusEvent)
    server = Trader.__new__(Trader)
    server.client = adapter
    server.ib_account = "DU_FAKE"
    server.trading_runtime_ib_client_id = 7
    server.duckdb_path = str(tmp_path / "reserved-id.duckdb")
    journal = ServerOrderJournal(server.duckdb_path)
    server._server_order_journal = journal
    intent_id = "native-id-boundary"
    assert journal.claim(intent_id, "fixed-protective-payload", server.ib_account)
    intent_token = _CURRENT_INTENT.set(intent_id)
    journal_token = _CURRENT_JOURNAL.set(journal)
    try:
        contract = Stock("OWN", "SMART", "USD", conId=123)
        order = StopOrder("SELL", 1, 8, account=server.ib_account, orderRef="owner")
        await server.reserve_broker_order(order, is_exit=True, contract=contract)
        before_send = journal.get(intent_id)
        reserved, = before_send["orders"]
        observable = await adapter.subscribe_place_order(contract, order)
        trade = await asyncio.wait_for(observable, timeout=1.0)
        order_packets = [packet[4:].split(b"\0") for packet in outgoing
                         if packet[4:].split(b"\0", 1)[0] == b"3"]
        wire, = order_packets
        wire_id = int(wire[1])
        assert reserved["orderId"] == wire_id == trade.order.orderId
        assert reserved["orderId"] > 0
        assert reserved["clientId"] == trade.order.clientId == 7
        assert journal.get(intent_id)["orders"] == before_send["orders"]
    finally:
        _CURRENT_JOURNAL.reset(journal_token)
        _CURRENT_INTENT.reset(intent_token)
        adapter.trades_subject.dispose()
        native.disconnect()
        journal.journal.close()
