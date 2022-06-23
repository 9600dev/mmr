import expression
import pandas as pd
from expression import pipe
from expression.collections import seq, Seq
from ib_insync.objects import Position, PortfolioItem
from ib_insync.contract import Contract
from ib_insync.order import Trade, Order, OrderStatus
from trader.common.logging_helper import setup_logging
from trader.common.helpers import ListHelper
from trader.common.reactive import AsyncCachedSubject, AsyncEventSubject
from eventkit import Event

logging = setup_logging(module_name='book')

from typing import List, Dict, Tuple, Union, cast, Optional

class BookSubject(AsyncEventSubject[Union[Trade, Order]]):
    def __init__(self):
        self.orders: Dict[int, List[Order]] = {}
        self.trades: Dict[int, List[Trade]] = {}
        super().__init__()

    # we go with order or trade here, because reconnecting with
    # the server means we're doing a reqAllOrder call, which returns
    # orders only
    async def add_update_trade(self, order: Union[Trade, Order]):
        logging.debug('updating trade book with {}'.format(order))

        if type(order) is Trade:
            order = cast(Trade, order)
            if order.order.orderId not in self.trades: self.trades[order.order.orderId] = []
            if order.order.orderId not in self.orders: self.orders[order.order.orderId] = []
            self.trades[order.order.orderId] = [order] + self.trades[order.order.orderId]
            self.orders[order.order.orderId] = [order.order] + self.orders[order.order.orderId]

        if type(order) is Order:
            order = cast(Order, order)
            if order.orderId not in self.orders: self.orders[order.orderId] = []
            self.orders[order.orderId] = [order] + self.orders[order.orderId]

    def get_orders(self) -> Dict[int, List[Order]]:
        return self.orders

    def get_trades(self) -> Dict[int, List[Trade]]:
        return self.trades

    def get_trade(self, order_id: int) -> Optional[Trade]:
        if order_id in self.trades:
            return self.trades[order_id][0]
        return None

    def get_order(self, order_id: int) -> Optional[Order]:
        if order_id in self.orders:
            return self.orders[order_id][0]
        return None

    def get_book(self) -> Tuple[Dict[int, List[Trade]], Dict[int, List[Order]]]:
        return (self.trades, self.orders)

    async def asend(self, value: Union[Trade, Order]) -> None:
        await self.add_update_trade(value)
        await super().asend(value)

    async def filter_book_by_contract(self, contract: Contract, value: Trade):
        return contract.conId == value.contract.conId
