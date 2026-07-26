from ib_async import Order, Ticker
from trader.common.logging_helper import setup_logging
from trader.objects import Basket, ContractOrderPair
from trader.trading.book import BookSubject
from trader.trading.order_structure import rejection_for_order
from typing import Dict, Optional


logging = setup_logging(module_name='trading_runtime')


class OrderValidator():
    """Structural sanity checks for orders and baskets.

    The DECISION lives in ``trader.trading.order_structure`` — pure, contracted,
    symbolically checked and mutation-tested. This class is the object-shaped
    façade the executioner's ``SANITY_CHECK`` condition calls; it holds no policy
    of its own. The same decision is enforced unconditionally at the placement
    chokepoint (``TradeExecutioner.subscribe_place_order_direct``), which is what
    covers the automated order paths this class never saw.
    """

    def __init__(
        self,
    ):
        pass

    def _check_order(self, order: Order) -> Optional[str]:
        """Return a rejection reason, or None if the order is structurally sane."""
        return rejection_for_order(order)

    def sanity_check_order(
        self,
        contract_order: ContractOrderPair,
        book: BookSubject,
        contract_ticker: Ticker,
    ) -> bool:
        reason = self._check_order(contract_order.order)
        if reason is not None:
            logging.warning('sanity_check_order rejected %s: %s', contract_order, reason)
            return False
        return True

    def sanity_check_basket(
        self,
        basket: Basket,
        book: BookSubject,
        prices: Dict[Order, Ticker],
    ) -> bool:
        """All-or-nothing: one malformed leg rejects the whole basket.

        A basket is placed as a unit, so accepting the sane legs of a malformed
        basket would half-execute an intent nobody expressed.
        """
        for pair in getattr(basket, 'orders', []) or []:
            reason = self._check_order(pair.order)
            if reason is not None:
                logging.warning('sanity_check_basket rejected %s: %s', pair, reason)
                return False
        return True
