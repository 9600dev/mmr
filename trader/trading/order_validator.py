from enum import Enum
from ib_insync import Contract, LimitOrder, MarketOrder, Order, StopLimitOrder, StopOrder, Ticker, Trade
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


class OrderValidator():
    def __init__(
        self,
    ):
        pass

    def sanity_check_order(
        self,
        contract_order: ContractOrderPair,
        book: BookSubject,
        contract_ticker: Ticker,
    ) -> bool:
        return True

    def sanity_check_basket(
        self,
        basket: Basket,
        book: BookSubject,
        prices: Dict[Order, Ticker],
    ) -> bool:
        return True

