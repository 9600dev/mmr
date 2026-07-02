"""Tests for OrderValidator.sanity_check_order.

The validator is intentionally conservative: it rejects only structurally
malformed orders (bad quantity, missing/non-positive prices for the order type)
and must never block a legitimate, well-formed order.
"""
from ib_async.contract import Contract
from ib_async.order import LimitOrder, MarketOrder, Order, StopOrder, Trade
from ib_async.ticker import Ticker

from trader.objects import ContractOrderPair
from trader.trading.book import BookSubject
from trader.trading.order_validator import OrderValidator


def _pair(order):
    c = Contract()
    c.conId = 4391
    c.symbol = "AMD"
    return ContractOrderPair(contract=c, order=order)


def _check(order):
    return OrderValidator().sanity_check_order(_pair(order), BookSubject(), Ticker())


class TestSanityCheckOrder:
    def test_valid_market_order_passes(self):
        assert _check(MarketOrder("BUY", 100)) is True

    def test_valid_limit_order_passes(self):
        assert _check(LimitOrder("SELL", 50, 123.45)) is True

    def test_zero_quantity_rejected(self):
        assert _check(MarketOrder("BUY", 0)) is False

    def test_negative_quantity_rejected(self):
        assert _check(MarketOrder("BUY", -10)) is False

    def test_nan_quantity_rejected(self):
        assert _check(MarketOrder("BUY", float("nan"))) is False

    def test_limit_order_without_price_rejected(self):
        assert _check(LimitOrder("BUY", 100, 0.0)) is False

    def test_limit_order_negative_price_rejected(self):
        assert _check(LimitOrder("BUY", 100, -5.0)) is False

    def test_stop_order_without_stop_price_rejected(self):
        assert _check(StopOrder("SELL", 100, 0.0)) is False

    def test_bad_action_rejected(self):
        o = Order()
        o.action = "HODL"
        o.orderType = "MKT"
        o.totalQuantity = 100
        assert _check(o) is False
