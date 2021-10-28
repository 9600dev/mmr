import lightbus
from lightbus.api import Api, Event
from dataclasses import dataclass
from ib_insync.objects import Position, PortfolioItem
from ib_insync.contract import Contract
from ib_insync.order import Order, Trade
from trader.container import Container
from trader.trading.trading_runtime import Action, Trader
from trader.data.universe import Universe
from trader.common.helpers import DictHelper

from typing import List, Dict, Tuple, Optional

class TraderServiceApi(Api):
    # this resolves a singleton trader instance, which if instantiated from
    # the trader runtime, will have all the things needed to reflect on the current
    # state of the trading system.
    # If it's resolved from outside the runtime (i.e. from bus.py import *) it still
    # fires up properly.
    trader = Container().resolve(Trader)

    class Meta:
        name = 'service'

    # json can't encode Dict's with keys that aren't primitive and hashable
    # so we often have to convert to weird containers like List[Tuple]
    async def get_positions(self) -> List[Position]:
        return self.trader.portfolio.get_positions()

    async def get_portfolio(self) -> List[PortfolioItem]:
        return self.trader.client.ib.portfolio()
        # return self.trader.portfolio.get_portfolio_items()

    async def get_universes(self) -> Dict[str, int]:
        return self.trader.universe_accessor.list_universes_count()

    async def get_trades(self) -> Dict[int, List[Trade]]:
        return self.trader.book.get_trades()

    async def get_orders(self) -> Dict[int, List[Order]]:
        return self.trader.book.get_orders()

    async def cancel_order(self, order_id: int) -> Optional[Trade]:
        return self.trader.cancel_order(order_id)

    async def temp_place_order(self, contract: Contract, action: str, equity_amount: float) -> Trade:
        act = Action.BUY if 'BUY' in action else Action.SELL
        cached_observer = await self.trader.temp_handle_order(
            contract=contract,
            action=act,
            equity_amount=equity_amount,
            delayed=True,
            debug=True
        )
        return await cached_observer.wait_value()

    async def reconnect(self):
        return self.trader.reconnect()

bus = lightbus.create(config_file=Container().config()['lightbus_config_file'])
bus.client.register_api(TraderServiceApi())
