import math

from enum import Enum
from ib_async import Contract, LimitOrder, MarketOrder, Order, StopLimitOrder, StopOrder, Ticker, Trade
from reactivex import Observable, Observer
from reactivex.abc import DisposableBase
from reactivex.disposable import Disposable
from reactivex.subject import Subject
from trader.common.exceptions import trader_exception, TraderException
from trader.common.logging_helper import get_callstack, log_method, setup_logging
from trader.data.universe import Universe, UniverseAccessor
from trader.objects import Action, Basket, ContractOrderPair
from trader.trading.book import BookSubject
from typing import cast, Dict, List, Optional, TYPE_CHECKING

import reactivex as rx


logging = setup_logging(module_name='trading_runtime')

if TYPE_CHECKING:
    from trader.trading.trading_runtime import Trader


def _finite_positive(x) -> bool:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return False
    return math.isfinite(v) and v > 0


class OrderValidator():
    def __init__(
        self,
    ):
        pass

    def _check_order(self, order: Order) -> Optional[str]:
        """Return a rejection reason, or None if the order is structurally sane.

        Deliberately conservative: it only rejects orders that are *definitely*
        malformed (non-positive/NaN quantity, missing or non-positive prices for
        the order type). It does NOT second-guess price levels — that would risk
        blocking legitimate wide-limit orders. The point is to stop a
        quantity=0 / NaN-price / priceless-limit order from ever reaching IB.
        """
        action = str(getattr(order, 'action', '') or '').upper()
        if action not in ('BUY', 'SELL'):
            return f'invalid action {action!r}'

        if not _finite_positive(getattr(order, 'totalQuantity', 0)):
            return f'non-positive or non-finite quantity {getattr(order, "totalQuantity", None)!r}'

        otype = str(getattr(order, 'orderType', '') or '').upper()
        if otype in ('LMT', 'STP LMT'):
            if not _finite_positive(getattr(order, 'lmtPrice', 0)):
                return f'{otype} order requires a positive limit price (got {getattr(order, "lmtPrice", None)!r})'
        if otype in ('STP', 'STP LMT'):
            if not _finite_positive(getattr(order, 'auxPrice', 0)):
                return f'{otype} order requires a positive stop price (got {getattr(order, "auxPrice", None)!r})'
        return None

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
        for pair in getattr(basket, 'orders', []) or []:
            reason = self._check_order(pair.order)
            if reason is not None:
                logging.warning('sanity_check_basket rejected %s: %s', pair, reason)
                return False
        return True

