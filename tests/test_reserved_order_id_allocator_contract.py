"""New allocation must be usable before this order is reserved or submitted.

The native protocol/framing boundary is covered separately. These controls
exercise the supported getReqId-only adapter and real server journal, with
the existing coordinated submission fake; no broker is contacted.
"""

import datetime as dt
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from ib_async import StopOrder

from review.test_review_order_contract import _stock
from test_server_reduction_reservations import reservation_server
from trader.common import exceptions as exception_support
from trader.trading.trading_runtime import _CURRENT_INTENT, _CURRENT_JOURNAL


@pytest.mark.asyncio
@pytest.mark.parametrize("allocated", [
    pytest.param((0, 0), id="zero-repeated-once"),
    pytest.param((False,), id="false-is-not-native-zero"),
    pytest.param((True,), id="true-is-not-a-positive-id"),
    pytest.param((-1,), id="negative-id"),
    pytest.param((None,), id="missing-id"),
    pytest.param((17.0,), id="floating-id"),
    pytest.param(("17",), id="string-id"),
])
async def test_invalid_new_id_refuses_before_this_order_reservation_and_submission(
        reservation_server, monkeypatch, allocated):
    server = reservation_server.make()
    server.startup_time = dt.datetime.now()
    outputs = iter(allocated)
    calls = []

    def next_id():
        calls.append(True)
        return next(outputs)

    # No updateReqId or other allocator capability is supplied by this
    # supported minimal adapter. Only exact integer zero permits one retry.
    server.client.ib.client = SimpleNamespace(getReqId=next_id)
    journal = server.server_order_journal()
    reserve = Mock(wraps=journal.reserve_order)
    monkeypatch.setattr(journal, "reserve_order", reserve)
    errors = []
    # Preserve the actual structured exception without formatting a large
    # generated traceback when this ordinary control joins mutation checks.
    monkeypatch.setattr(exception_support, "logging", SimpleNamespace(error=errors.append))
    intent_id = "invalid-allocation"

    result = await server.place_standalone_order(
        _stock(), "SELL", 40, "STP", aux_price=8,
        order_ref="allocator_contract", client_intent_id=intent_id)

    assert not result.is_success()
    assert len(calls) == len(allocated)
    reserve.assert_not_called()
    server.client.subscribe_place_order.assert_not_awaited()
    claim = journal.get(intent_id)
    assert claim["status"] == "REJECTED" and claim["orders"] == []
    assert journal.reservations(server.ib_account) == []
    assert getattr(server, "_emergency_order_journal", None) is None
    assert not server.placed and server.inventory == 40
    assert len(errors) == 1 and isinstance(errors[0].inner, ValueError)


@pytest.mark.asyncio
async def test_preassigned_positive_id_and_exact_reference_bypass_allocation(reservation_server):
    server = reservation_server.make()
    calls = []

    def forbidden_allocation():
        calls.append(True)
        raise AssertionError("an existing physical identity must not be reallocated")

    server.client.ib.client = SimpleNamespace(getReqId=forbidden_allocation)
    journal = server.server_order_journal()
    intent_id = "preassigned-positive"
    assert journal.claim(intent_id, "fixed-protective-payload", server.ib_account)
    reference = "allocator_contract|mmr:" + intent_id
    order = StopOrder("SELL", 40, 8, account=server.ib_account,
                      orderId=17, clientId=7, orderRef=reference)
    intent_token = _CURRENT_INTENT.set(intent_id)
    journal_token = _CURRENT_JOURNAL.set(journal)
    try:
        await server.reserve_broker_order(order, is_exit=True, contract=_stock())
    finally:
        _CURRENT_JOURNAL.reset(journal_token)
        _CURRENT_INTENT.reset(intent_token)

    assert calls == []
    assert (order.orderId, order.clientId, order.orderRef) == (17, 7, reference)
    assert journal.get(intent_id)["orders"] == [{"orderId": 17, "clientId": 7}]
    reservation, = journal.reservations(server.ib_account)
    assert (reservation["intent_id"], reservation["order_id"], reservation["client_id"]) == (
        intent_id, 17, 7)
    assert (reservation["conid"], reservation["action"], reservation["quantity"]) == (100, "SELL", 40)
    assert reservation["broker_reference"] == intent_id
