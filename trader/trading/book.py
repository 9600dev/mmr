from ib_insync import TradeLogEntry
from ib_insync.contract import Contract
from ib_insync.order import Order, Trade
from trader.common.helpers import flatten_list
from trader.common.logging_helper import setup_logging
from trader.common.reactivex import EventSubject
from trader.objects import TradeLogSimple


logging = setup_logging(module_name='book')

from typing import cast, Dict, List, Optional, Tuple, Union


class BookSubject(EventSubject[Union[Trade, Order]]):
    def __init__(self):
        self.orders: Dict[int, List[Order]] = {}
        self.trades: Dict[int, List[Trade]] = {}
        super().__init__()

    # we go with order or trade here, because reconnecting with
    # the server means we're doing a reqAllOrder call, which returns
    # orders only
    def add_update_trade(self, order: Union[Trade, Order]):
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

    def on_next(self, value: Union[Trade, Order]) -> None:
        self.add_update_trade(value)
        super().on_next(value)

    def filter_book_by_contract(self, contract: Contract, value: Trade):
        return contract.conId == value.contract.conId

    def get_trade_log(self) -> List[TradeLogSimple]:
        logging.debug('book.get_trade_log()')
        trade_logs: List[TradeLogSimple] = []

        for _, trades in self.trades.items():
            last_trade = trades[0]
            for log in last_trade.log:
                trade_logs.append(TradeLogSimple(
                    time=log.time,
                    conId=last_trade.contract.conId,
                    secType=last_trade.contract.secType,
                    symbol=last_trade.contract.symbol,
                    exchange=last_trade.contract.exchange,
                    currency=last_trade.contract.currency,
                    orderId=last_trade.order.orderId,
                    status=log.status,
                    message=log.message,
                    errorCode=log.errorCode,
                    clientId=last_trade.order.clientId,
                    action=last_trade.order.action,
                    totalQuantity=last_trade.order.totalQuantity,
                    lmtPrice=last_trade.order.lmtPrice,
                    orderRef=last_trade.order.orderRef,
                ))

        trade_logs.sort(key=lambda log: log.time, reverse=True)
        return trade_logs

