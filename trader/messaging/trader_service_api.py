import asyncio
import sys
import os
import aioreactive as rx

# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
# PACKAGE_PARENT = '../..'
# SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
# sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from dataclasses import dataclass
from ib_insync.objects import Position, PortfolioItem
from ib_insync.contract import Contract
from ib_insync.order import Order, Trade
from ib_insync.ticker import Ticker
from trader.container import Container
from trader.common.reactive import SuccessFail, SuccessFailObservable, SuccessFailEnum
from aioreactive.observables import AsyncAnonymousObservable
from aioreactive.observers import AsyncAnonymousObserver, safe_observer, auto_detach_observer
from expression.core import pipe
from expression.system.disposable import AsyncDisposable

import trader.trading.trading_runtime as runtime
from trader.data.universe import Universe
from trader.common.helpers import DictHelper

from typing import List, Dict, Tuple, Optional
from trader.messaging.clientserver import RPCHandler

from trader.common.logging_helper import setup_logging
logging = setup_logging(module_name='trader_service_api')


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
        cached_observer = asyncio.get_event_loop().run_until_complete(self.trader.temp_handle_order(
            contract=contract,
            action=act,
            equity_amount=equity_amount,
            delayed=True,
            debug=True
        ))
        return asyncio.get_event_loop().run_until_complete(cached_observer.wait_value())

    @RPCHandler.rpcmethod
    def cancel_order(self, order_id: int) -> Optional[Trade]:
        return self.trader.cancel_order(order_id)

    @RPCHandler.rpcmethod
    def get_snapshot(self, contract: Contract, delayed: bool) -> Ticker:
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self.trader.client.get_snapshot(contract=contract, delayed=delayed))

    @RPCHandler.rpcmethod
    def publish_contract(self, contract: Contract, delayed: bool) -> SuccessFail:
        task = asyncio.Event()
        disposable: AsyncDisposable = AsyncDisposable.empty()
        result: Optional[SuccessFail] = None

        async def asend(val: SuccessFail):
            nonlocal result
            result = val
            task.set()

        async def wait_for_subscription():
            nonlocal disposable
            success_fail = await self.trader.publish_contract(contract, delayed)
            disposable = await success_fail.subscribe_async(AsyncAnonymousObserver(asend=asend))

        loop = asyncio.get_event_loop()
        loop.run_until_complete(wait_for_subscription())
        loop.run_until_complete(task.wait())
        loop.run_until_complete(disposable.dispose_async())

        return result if result else SuccessFail(SuccessFailEnum.FAIL)

    @RPCHandler.rpcmethod
    def get_published_contracts(self) -> list[Contract]:
        return list(self.trader.zmq_pubsub_contracts.keys())
