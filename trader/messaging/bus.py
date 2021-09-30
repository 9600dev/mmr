import lightbus
from lightbus.api import Api, Event
from dataclasses import dataclass
from ib_insync.objects import Position, PortfolioItem
from ib_insync.contract import Contract
from trader.container import Container
from trader.trading.trading_runtime import Trader
from trader.data.universe import Universe

from typing import List, Dict

class TraderServiceApi(Api):
    # this resolves a singleton trader instance, which if instantiated from
    # the trader runtime, will have all the things needed to reflect on the current
    # state of the trading system.
    # If it's resolved from outside the runtime (i.e. from bus.py import *) it still
    # fires up properly.
    trader = Container().resolve(Trader)

    class Meta:
        name = 'service'

    async def get_positions(self) -> List[Position]:
        return self.trader.portfolio.get_positions()

    async def get_portfolio(self) -> List[PortfolioItem]:
        return self.trader.client.ib.portfolio()
        # return self.trader.portfolio.get_portfolio_items()

    async def get_universes(self) -> Dict[str, int]:
        return self.trader.universe_accessor.list_universes_count()

    async def reconnect(self):
        return self.trader.reconnect()

bus = lightbus.create(config_file=Container().config()['lightbus_config_file'])
bus.client.register_api(TraderServiceApi())
