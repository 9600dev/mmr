import pytest
from ib_async.contract import Contract
from ib_async.order import Order, Trade


from trader.trading.book import BookSubject


def _make_order(order_id=1):
    order = Order()
    order.orderId = order_id
    order.action = "BUY"
    order.totalQuantity = 100
    return order


def _make_trade(order_id=1):
    order = _make_order(order_id)
    contract = Contract()
    contract.conId = 4391
    contract.symbol = "AMD"
    trade = Trade(contract=contract, order=order)
    return trade


class TestBookSubject:
    def test_add_trade_and_retrieve(self):
        book = BookSubject()
        trade = _make_trade(order_id=10)
        book.add_update_trade(trade)
        retrieved = book.get_trade(10)
        assert retrieved is not None
        assert retrieved.order.orderId == 10

    def test_add_order_and_retrieve(self):
        book = BookSubject()
        order = _make_order(order_id=20)
        book.add_update_trade(order)
        retrieved = book.get_order(20)
        assert retrieved is not None
        assert retrieved.orderId == 20

    def test_get_by_order_id(self):
        book = BookSubject()
        book.add_update_trade(_make_trade(order_id=1))
        book.add_update_trade(_make_trade(order_id=2))
        assert book.get_trade(1) is not None
        assert book.get_trade(2) is not None
        assert book.get_trade(999) is None

    def test_get_nonexistent_returns_none(self):
        book = BookSubject()
        assert book.get_trade(999) is None
        assert book.get_order(999) is None
