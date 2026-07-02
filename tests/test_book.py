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


def _make_trade_with_status(order_id, status):
    trade = _make_trade(order_id)
    trade.orderStatus.status = status
    return trade


class TestOpenOrderCount:
    def test_counts_only_active_orders(self):
        book = BookSubject()
        book.add_update_trade(_make_trade_with_status(1, 'Submitted'))
        book.add_update_trade(_make_trade_with_status(2, 'PreSubmitted'))
        book.add_update_trade(_make_trade_with_status(3, 'Filled'))
        book.add_update_trade(_make_trade_with_status(4, 'Cancelled'))
        # Only the two working orders count against the open-order limit; the
        # book retains all four for the audit log.
        assert book.get_open_order_count() == 2
        assert len(book.get_orders()) == 4

    def test_order_without_status_counts_as_open(self):
        book = BookSubject()
        # An Order-only entry (e.g. from reqAllOrders after reconnect) has no
        # Trade/status; count it conservatively as open.
        book.add_update_trade(_make_order(order_id=50))
        assert book.get_open_order_count() == 1
