import asyncio
from dataclasses import dataclass
from ib_insync.objects import Position, PortfolioItem
from ib_insync.contract import Contract
from ib_insync.order import Order, Trade
from ib_insync.ticker import Ticker
from trader.container import Container

import trader.trading.trading_runtime as runtime
from trader.data.universe import Universe
from trader.common.helpers import DictHelper

from typing import List, Dict, Tuple, Optional
from trader.messaging.clientserver import RPCHandler

class TraderServiceApi(RPCHandler):
    def __init__(self, trader):
        self.trader: runtime.Trader = trader

    # json can't encode Dict's with keys that aren't primitive and hashable
    # so we often have to convert to weird containers like List[Tuple]
    @RPCHandler.rpcmethod
    def get_positions(self) -> list[Position]:
        return self.trader.portfolio.get_positions()

    @RPCHandler.rpcmethod
    def get_portfolio(self) -> list[PortfolioItem]:
        return self.trader.client.ib.portfolio()

    @RPCHandler.rpcmethod
    def get_universes(self) -> dict[str, int]:
        return self.trader.universe_accessor.list_universes_count()

    @RPCHandler.rpcmethod
    def get_trades(self) -> dict[int, list[Trade]]:
        return self.trader.book.get_trades()

    @RPCHandler.rpcmethod
    def get_orders(self) -> dict[int, list[Order]]:
        return self.trader.book.get_orders()

    @RPCHandler.rpcmethod
    def temp_place_order(self, contract: Contract, action: str, equity_amount: float) -> Trade:
        from trader.trading.trading_runtime import Action, Trader
        # todo: need to figure out the async stuff here
        act = Action.BUY if 'BUY' in action else Action.SELL
        cached_observer = asyncio.run(self.trader.temp_handle_order(
            contract=contract,
            action=act,
            equity_amount=equity_amount,
            delayed=True,
            debug=True
        ))
        return asyncio.run(cached_observer.wait_value())

    @RPCHandler.rpcmethod
    def cancel_order(self, order_id: int) -> Optional[Trade]:
        return self.trader.cancel_order(order_id)

    @RPCHandler.rpcmethod
    def get_snapshot(self, contract: Contract, delayed: bool) -> Ticker:
        return asyncio.run(self.trader.client.get_snapshot(contract=contract, delayed=delayed))
