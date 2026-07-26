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


class TestRejectionNamesTheOffendingValue:
    """A refusal is read by an operator, not only branched on by a caller.

    'invalid action' tells you a rule fired; "invalid action 'HODL'" tells you
    which field to go and fix. These assert the reason quotes what was actually
    received — the difference between a log line that ends an investigation and
    one that starts it.
    """

    def _reason(self, order):
        return OrderValidator()._check_order(order)

    def test_a_bad_action_is_quoted_verbatim(self):
        o = Order()
        o.action = "HODL"
        o.orderType = "MKT"
        o.totalQuantity = 100
        assert repr("HODL") in self._reason(o)

    def test_an_empty_action_is_reported_as_empty_not_as_a_placeholder(self):
        o = Order()
        o.action = ""
        o.orderType = "MKT"
        o.totalQuantity = 100
        assert repr("") in self._reason(o)

    def test_a_bad_quantity_is_quoted(self):
        assert repr(0.0) in self._reason(MarketOrder("BUY", 0.0))

    def test_a_missing_limit_price_reason_names_the_order_type_and_price(self):
        reason = self._reason(LimitOrder("BUY", 100, 0.0))
        assert "LMT" in reason and repr(0.0) in reason

    def test_a_missing_stop_price_reason_names_the_order_type_and_price(self):
        reason = self._reason(StopOrder("SELL", 100, 0.0))
        assert "STP" in reason and repr(0.0) in reason


class TestSanityCheckBasket:
    """The basket path had no test at all. It is all-or-nothing by design: a
    basket is placed as a unit, so accepting the sane legs of a malformed basket
    would half-execute an intent nobody expressed."""

    def _basket(self, *orders):
        from trader.objects import Basket
        return Basket(orders=[_pair(o) for o in orders], hedges=[])

    def _check_basket(self, basket):
        return OrderValidator().sanity_check_basket(basket, BookSubject(), {})

    def test_an_all_valid_basket_passes(self):
        assert self._check_basket(
            self._basket(MarketOrder("BUY", 100), LimitOrder("SELL", 50, 12.5))) is True

    def test_one_malformed_leg_rejects_the_whole_basket(self):
        assert self._check_basket(
            self._basket(MarketOrder("BUY", 100), MarketOrder("SELL", 0))) is False

    def test_a_malformed_leg_in_last_position_is_still_caught(self):
        """Guards against a loop that returns on the first leg only."""
        assert self._check_basket(
            self._basket(MarketOrder("BUY", 1), MarketOrder("BUY", 2),
                         LimitOrder("SELL", 50, 0.0))) is False

    def test_an_empty_basket_passes(self):
        assert self._check_basket(self._basket()) is True
