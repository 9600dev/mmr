"""Protected-resize OCA confirmation against the REAL ib_async (audit B1.7).

``Trader._resize_position`` modifies a protective stop into a reduce-with-block
OCA group, confirms the group with the broker, and only then sends the trim.
The confirmation read ``Trade.order.ocaGroup``. ib_async never puts the
modified value there:

* ``IB.placeOrder`` on an existing order id returns the EXISTING ``Trade`` and
  keeps its original ``Order`` object; the edit only travels on the wire
  (ib_async 2.1.0 ``ib.py`` ``placeOrder``, the "modification of an existing
  order" branch);
* ``Wrapper.openOrder`` — the broker's echo — copies exactly six fields onto an
  existing ``Trade`` (``permId``, ``totalQuantity``, ``lmtPrice``, ``auxPrice``,
  ``orderType``, ``orderRef``) and drops the rest, ``ocaGroup`` / ``ocaType``
  included (``wrapper.py`` ``openOrder``).

The first two tests pin that library behaviour, so an ib_async upgrade that
changes it is noticed. The rest drive the real coordinator through a real,
never-connected ``IB`` whose socket is replaced by a scripted gateway that
answers through the real ``Wrapper`` callbacks, the way ``Decoder`` does. No
network, no account.
"""
import asyncio
import copy
import itertools
from types import SimpleNamespace

import pytest
from ib_async import IB, OrderState, StopOrder

from review.test_review_order_contract import _coordinated_trader, _stock
from trader.listeners.ibreactive import IBAIORx, broker_order_echo


CLIENT_ID = 7


class _Gateway:
    """IB at the socket boundary of a real ``ib_async.IB``.

    Receives what ``Client`` would send and answers by calling the real
    ``Wrapper`` callbacks with freshly built ("wire-decoded") objects. Orders
    rest unfilled: a stop is ``PreSubmitted`` (held at IB), anything else
    ``Submitted``.

    ``apply_oca=False`` models a broker that did not apply an OCA change in a
    modification: its echo keeps the previous group.
    """

    def __init__(self, ib: IB, account: str, held: float, apply_oca: bool = True):
        self.ib, self.account, self.held, self.apply_oca = ib, account, held, apply_oca
        self.book = {}           # orderId -> SimpleNamespace(contract, order, status, remaining)
        self.cancel_requests = []
        self._perm_ids = itertools.count(9001)
        client, wrapper = ib.client, ib.wrapper
        client.isReady = lambda: True                 # ib.isConnected() / getReqId()
        client.updateReqId(1)                         # what nextValidId does on connect
        client.placeOrder = self._place_order
        client.cancelOrder = self._cancel_order
        client.reqOpenOrders = self._req_open_orders
        client.reqPositions = self._req_positions
        wrapper.clientId = CLIENT_ID
        wrapper.accounts = [account]

    # -- what Client sends -------------------------------------------------
    def _place_order(self, order_id, contract, order):
        wire = copy.copy(order)
        # The broker's record names the connection and the id on the message;
        # ib_async stamps them on a new Order only after the send.
        wire.orderId, wire.clientId = order_id, CLIENT_ID
        resting = self.book.get(order_id)
        if resting is None:
            wire.permId = next(self._perm_ids)
            status = 'PreSubmitted' if wire.orderType in ('STP', 'TRAIL') else 'Submitted'
            self.book[order_id] = SimpleNamespace(
                contract=contract, order=wire, status=status,
                remaining=float(wire.totalQuantity))
        else:
            wire.permId = resting.order.permId
            if not self.apply_oca:
                wire.ocaGroup, wire.ocaType = resting.order.ocaGroup, resting.order.ocaType
            resting.order = wire
        asyncio.get_running_loop().call_soon(self._echo, order_id)

    def _cancel_order(self, order_id, manual_cancel_time=''):
        self.cancel_requests.append(order_id)
        resting = self.book[order_id]
        resting.status, resting.remaining = 'Cancelled', 0.0
        asyncio.get_running_loop().call_soon(self._echo, order_id)

    def _req_open_orders(self):
        def answer():
            for order_id, resting in self.book.items():
                if resting.status != 'Cancelled':
                    self._echo(order_id)
            self.ib.wrapper.openOrderEnd()
        asyncio.get_running_loop().call_soon(answer)

    def _req_positions(self):
        def answer():
            self.ib.wrapper.position(self.account, _stock(), self.held, 10.0)
            self.ib.wrapper.positionEnd()
        asyncio.get_running_loop().call_soon(answer)

    # -- what Decoder delivers ---------------------------------------------
    def _echo(self, order_id):
        resting = self.book[order_id]
        wrapper = self.ib.wrapper
        if resting.status != 'Cancelled':
            wrapper.openOrder(order_id, resting.contract, copy.copy(resting.order),
                              OrderState(status=resting.status))
        wrapper.orderStatus(order_id, resting.status, 0.0, resting.remaining, 0.0,
                            resting.order.permId, 0, 0.0, CLIENT_ID, '')

    # -- test reads -----------------------------------------------------------
    def resting(self, order_type):
        return [r for r in self.book.values()
                if r.order.orderType == order_type and r.status != 'Cancelled']


def _library_trader(tmp_path, *, apply_oca=True):
    """The coordinated runtime of the review suite over a real ``IBAIORx``.

    Only the socket is replaced. ``IB.placeOrder``, ``Wrapper.openOrder``,
    ``reqOpenOrdersAsync`` / ``reqPositionsAsync`` and ``IBAIORx``'s own
    placement path are the production code.
    """
    trader = _coordinated_trader(tmp_path)
    rx_client = IBAIORx('127.0.0.1', 0, CLIENT_ID, trader.ib_account)   # never connected
    gateway = _Gateway(rx_client.ib, trader.ib_account, held=100, apply_oca=apply_oca)
    rx_client.get_snapshot = trader.client.get_snapshot
    trader.client = rx_client
    # As Trader.setup_subscriptions does on the current IB instance.
    rx_client.ib.orderStatusEvent.connect(trader.order_tracker.on_trade, keep_ref=True)
    return trader, gateway


async def _two_stops(trader):
    for _ in range(2):
        result = await trader.place_standalone_order(_stock(), 'SELL', 50, 'STP', aux_price=8)
        assert result.is_success(), result.error
    await asyncio.sleep(0)       # let the gateway's acknowledgements arrive


def _bare_ib():
    ib = IB()
    ib.client.placeOrder = lambda *args, **kwargs: None      # no socket
    ib.client.isReady = lambda: True
    ib.client.updateReqId(41)
    ib.wrapper.clientId = CLIENT_ID
    return ib


def test_library_modification_keeps_the_trades_original_order():
    ib = _bare_ib()
    stop = StopOrder('SELL', 50, 9.2, account='DU1', tif='GTC')
    trade = ib.placeOrder(_stock(), stop)
    trade.orderStatus.status = 'PreSubmitted'
    edit = copy.copy(trade.order)
    edit.ocaGroup, edit.ocaType = 'mmr-resize-abc', 2

    modified = ib.placeOrder(_stock(), edit)

    assert modified is trade
    assert modified.order is stop and modified.order is not edit
    assert (modified.order.ocaGroup, modified.order.ocaType) == ('', 0)


def test_library_open_order_echo_drops_the_oca_group():
    ib = _bare_ib()
    trade = ib.placeOrder(_stock(), StopOrder('SELL', 50, 9.2, account='DU1', tif='GTC'))
    trade.orderStatus.status = 'PreSubmitted'
    echo = copy.copy(trade.order)
    echo.permId, echo.auxPrice = 999, 9.1
    echo.ocaGroup, echo.ocaType = 'mmr-resize-abc', 2

    ib.wrapper.openOrder(trade.order.orderId, _stock(), echo, OrderState(status='PreSubmitted'))

    session = ib.wrapper.trades[(CLIENT_ID, trade.order.orderId)]
    assert session is trade
    assert (session.order.permId, session.order.auxPrice) == (999, 9.1)     # carried
    assert (session.order.ocaGroup, session.order.ocaType) == ('', 0)       # dropped


def _echoed_stop(ib, **echo_fields):
    """Place a stop on ``ib`` and deliver the broker's echo of it."""
    trade = ib.placeOrder(_stock(), StopOrder('SELL', 50, 9.2, account='DU1', tif='GTC'))
    echo = copy.copy(trade.order)
    for name, value in echo_fields.items():
        setattr(echo, name, value)
    ib.wrapper.openOrder(trade.order.orderId, _stock(), echo, OrderState(status='PreSubmitted'))
    return trade


def test_broker_echo_is_recorded_for_an_order_the_session_tracks():
    rx_client = IBAIORx('127.0.0.1', 0, CLIENT_ID)
    gateway_free = rx_client.ib
    gateway_free.client.placeOrder = lambda *args, **kwargs: None
    gateway_free.client.isReady = lambda: True
    gateway_free.client.updateReqId(41)
    gateway_free.wrapper.clientId = CLIENT_ID

    trade = _echoed_stop(gateway_free, permId=999, ocaGroup='mmr-resize-abc', ocaType=2)

    echo = broker_order_echo(gateway_free, trade.order)
    assert echo is not None and echo is not trade.order
    assert (echo.ocaGroup, echo.ocaType, echo.permId) == ('mmr-resize-abc', 2, 999)
    # The library's own handling is untouched: six fields, nothing more.
    assert (trade.order.permId, trade.order.ocaGroup, trade.order.ocaType) == (999, '', 0)


def test_no_echo_is_no_evidence():
    plain = _bare_ib()                           # recorder never installed
    trade = _echoed_stop(plain, ocaGroup='mmr-resize-abc', ocaType=2)
    assert broker_order_echo(plain, trade.order) is None

    rx_client = IBAIORx('127.0.0.1', 0, CLIENT_ID)
    rx_client.ib.client.placeOrder = lambda *args, **kwargs: None
    rx_client.ib.client.isReady = lambda: True
    rx_client.ib.client.updateReqId(41)
    rx_client.ib.wrapper.clientId = CLIENT_ID
    unechoed = rx_client.ib.placeOrder(_stock(), StopOrder('SELL', 50, 9.2, account='DU1'))
    assert broker_order_echo(rx_client.ib, unechoed.order) is None


def test_a_what_if_reply_is_not_recorded_as_an_order():
    rx_client = IBAIORx('127.0.0.1', 0, CLIENT_ID)
    probe = StopOrder('SELL', 50, 9.2, account='DU1', whatIf=True)
    probe.orderId, probe.clientId = 55, CLIENT_ID
    rx_client.ib.wrapper.openOrder(55, _stock(), probe, OrderState(initMarginChange='10.0'))
    assert broker_order_echo(rx_client.ib, probe) is None


@pytest.mark.asyncio
@pytest.mark.parametrize('path', ['connect', 'connect_async'])
async def test_a_reconnect_installs_the_recorder_on_the_fresh_ib(monkeypatch, path):
    """Every reconnect replaces ``IB()``; the recorder must follow, and start
    empty (an echo from the previous connection says nothing about this one)."""
    async def no_socket_async(self, *args, **kwargs):
        return self

    monkeypatch.setattr(IB, 'connectAsync', no_socket_async)
    monkeypatch.setattr(IB, 'connect', lambda self, *args, **kwargs: self)
    rx_client = IBAIORx('127.0.0.1', 0, CLIENT_ID)
    first = rx_client.ib
    first.client.placeOrder = lambda *args, **kwargs: None
    first.client.isReady = lambda: True
    first.client.updateReqId(41)
    first.wrapper.clientId = CLIENT_ID
    before = _echoed_stop(first, ocaGroup='mmr-resize-abc', ocaType=2)
    assert broker_order_echo(first, before.order) is not None
    first.client.isReady = lambda: False         # the connection dropped

    if path == 'connect':
        rx_client.connect()
    else:
        await rx_client.connect_async()

    assert rx_client.ib is not first
    assert broker_order_echo(rx_client.ib, before.order) is None
    rx_client.ib.client.placeOrder = lambda *args, **kwargs: None
    rx_client.ib.client.isReady = lambda: True
    rx_client.ib.client.updateReqId(41)
    rx_client.ib.wrapper.clientId = CLIENT_ID
    after = _echoed_stop(rx_client.ib, ocaGroup='mmr-resize-def', ocaType=2)
    assert broker_order_echo(rx_client.ib, after.order).ocaGroup == 'mmr-resize-def'


@pytest.mark.asyncio
async def test_protected_resize_is_confirmed_against_the_real_library(tmp_path):
    """The broker applies and echoes the OCA group; the resize must go through.

    Before the fix this failed with "UNKNOWN: broker has not confirmed the
    matching protected tranche; trim not submitted" for every protected
    resize, because the confirmation read a field ib_async never updates.
    """
    trader, gateway = _library_trader(tmp_path)
    await _two_stops(trader)

    result = await trader.resize_position(_stock(), 50, client_intent_id='real-library-resize')

    assert result.is_success(), result.error
    assert result.obj['status'] == 'SUBMITTED'
    assert result.obj['protection'] == 'OCA_REDUCE_WITH_BLOCK'
    await asyncio.sleep(0)
    trim, = gateway.resting('MKT')
    paired = [r for r in gateway.resting('STP') if r.order.ocaGroup]
    untouched = [r for r in gateway.resting('STP') if not r.order.ocaGroup]
    assert len(paired) == 1 and len(untouched) == 1
    assert trim.order.ocaGroup == paired[0].order.ocaGroup
    assert trim.order.ocaType == paired[0].order.ocaType == 2
    assert trim.order.totalQuantity == paired[0].order.totalQuantity == 50
    assert gateway.cancel_requests == [], 'a protected resize must not cancel protection'
    # The session's Trade carries the confirmed linkage: reduction-capacity
    # accounting reads it there and must count the pair once (100 working
    # against 100 held, not 150).
    session = next(t for t in trader.client.ib.openTrades()
                   if t.order.orderId == paired[0].order.orderId)
    assert (session.order.ocaGroup, session.order.ocaType) == (trim.order.ocaGroup, 2)


@pytest.mark.asyncio
async def test_resize_is_not_protected_when_the_broker_does_not_echo_the_group(tmp_path):
    """The safety property: no broker evidence of the linkage, no trim."""
    trader, gateway = _library_trader(tmp_path, apply_oca=False)
    await _two_stops(trader)

    result = await trader.resize_position(_stock(), 50, client_intent_id='unlinked-resize')

    assert not result.is_success() and 'UNKNOWN' in result.error
    await asyncio.sleep(0)
    assert gateway.resting('MKT') == [], 'the trim was sent without a confirmed OCA peer'
    assert len(gateway.resting('STP')) == 2
    assert gateway.cancel_requests == []
    assert trader.server_order_journal().get('unlinked-resize')['status'] == 'UNKNOWN'


@pytest.mark.asyncio
async def test_a_locally_edited_order_is_not_broker_evidence(tmp_path):
    """What the old test fake did: the session's Trade shows the group because
    something local put it there. The broker's echo does not have it, so the
    resize is not protected."""
    trader, gateway = _library_trader(tmp_path, apply_oca=False)
    await _two_stops(trader)
    ib = trader.client.ib
    library_place = ib.placeOrder

    def place_and_adopt_the_edit(contract, order):
        trade = library_place(contract, order)
        trade.order = order                 # not what ib_async does
        return trade

    ib.placeOrder = place_and_adopt_the_edit

    result = await trader.resize_position(_stock(), 50, client_intent_id='local-only-resize')

    assert not result.is_success() and 'UNKNOWN' in result.error
    await asyncio.sleep(0)
    assert gateway.resting('MKT') == []
    assert gateway.cancel_requests == []
