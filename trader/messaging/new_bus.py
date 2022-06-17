import lightbus
from lightbus.api import Api, Event
from dataclasses import dataclass
from ib_insync.objects import Position, PortfolioItem
from ib_insync.contract import Contract
from ib_insync.order import Order, Trade
from trader.container import Container

import trader.trading.trading_runtime as runtime
from trader.data.universe import Universe
from trader.common.helpers import DictHelper

from typing import List, Dict, Tuple, Optional
from trader.messaging.clientserver import RPCHandler

class NewTraderServiceApi(RPCHandler):
    def __init__(self, trader):
        self.trader: runtime.Trader = trader

    # json can't encode Dict's with keys that aren't primitive and hashable
    # so we often have to convert to weird containers like List[Tuple]
    @RPCHandler.rpcmethod
    def get_positions(self) -> List[Position]:
        return self.trader.portfolio.get_positions()

    @RPCHandler.rpcmethod
    def get_portfolio(self) -> List[PortfolioItem]:
        return self.trader.client.ib.portfolio()
        # return self.trader.portfolio.get_portfolio_items()

    def get_universes(self) -> Dict[str, int]:
        return self.trader.universe_accessor.list_universes_count()

    def get_trades(self) -> Dict[int, List[Trade]]:
        return self.trader.book.get_trades()

    def get_orders(self) -> Dict[int, List[Order]]:
        return self.trader.book.get_orders()

    def cancel_order(self, order_id: int) -> Optional[Trade]:
        return self.trader.cancel_order(order_id)
