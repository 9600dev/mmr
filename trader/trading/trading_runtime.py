from re import I
import sys
import os

# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

import pandas as pd
import datetime as dt
import backoff
import aioreactive as rx
import nest_asyncio

from asyncio.events import AbstractEventLoop
from aioreactive.types import Projection
from expression.core import pipe
from aioreactive.observers import AsyncAnonymousObserver
from enum import Enum

from trader.common.logging_helper import setup_logging
logging = setup_logging(module_name='trading_runtime')

from arctic import Arctic, TICK_STORE
from arctic.tickstore.tickstore import TickStore
from ib_insync.ib import IB
from ib_insync.contract import Contract, Forex, Future, Stock
from ib_insync.objects import PortfolioItem, Position, BarData
from ib_insync.order import LimitOrder, Order, Trade
from ib_insync.util import df
from ib_insync.ticker import Ticker

from trader.listeners.ibaiorx import IBAIORx
from trader.common.contract_sink import ContractSink
from trader.common.listener_helpers import Helpers
from trader.common.observers import ConsoleObserver, ArcticObserver, ComplexConsoleObserver, ContractSinkObserver, NullObserver
from trader.data.data_access import SecurityDefinition, TickData
from trader.data.universe import UniverseAccessor, Universe
from trader.container import Container
from trader.trading.book import Book
from trader.trading.algo import Algo
from trader.trading.portfolio import Portfolio
from trader.trading.executioner import Executioner
from trader.trading.strategy import Strategy
from trader.common.reactive import AsyncCachedObserver, AsyncEventSubject, AsyncCachedSubject
from trader.common.singleton import Singleton
from trader.common.helpers import get_network_ip, Pipe
from trader.messaging.bus_server import start_lightbus

from typing import List, Dict, Tuple, Callable, Optional, Set, Generic, TypeVar, cast, Union

class Action(Enum):
    BUY = 1
    SELL = 2

    def __str__(self):
        if self.value == 1: return 'BUY'
        if self.value == 2: return 'SELL'


class Trader(metaclass=Singleton):
    def __init__(self,
                 ib_server_address: str,
                 ib_server_port: int,
                 arctic_server_address: str,
                 arctic_library: str,
                 arctic_universe_library: str,
                 redis_server_address: str,
                 redis_server_port: str,
                 paper_trading: bool = False,
                 simulation: bool = False):
        self.ib_server_address = ib_server_address
        self.ib_server_port = ib_server_port
        self.arctic_server_address = arctic_server_address
        self.arctic_library = arctic_library
        self.arctic_universe_library = arctic_universe_library
        self.simulation: bool = simulation
        self.paper_trading = paper_trading
        self.redis_server_address = redis_server_address
        self.redis_server_port = redis_server_port

        # todo I think you can have up to 24 connections to TWS (and have multiple TWS instances running)
        # so we need to take this from single client, to multiple client
        self.client: IBAIORx
        self.data: TickData
        self.universe_accessor: UniverseAccessor

        # the live ticker data streams we have
        self.contract_subscriptions: Dict[Contract, ContractSink] = {}
        # the strategies we're using
        self.strategies: List[Strategy] = []
        # current order book (outstanding orders)
        self.book: Book = Book()
        # portfolio (current and past positions)
        self.portfolio: Portfolio = Portfolio()
        # takes care of execution of orders
        self.executioner: Executioner
        # a list of all the universes of stocks we have registered
        self.universes: List[Universe]

    @backoff.on_exception(backoff.expo, ConnectionRefusedError, max_tries=10, max_time=120)
    def connect(self):
        self.client = IBAIORx(self.ib_server_address, self.ib_server_port)
        self.data = TickData(self.arctic_server_address, self.arctic_library)
        self.universe_accessor = UniverseAccessor(self.arctic_server_address, self.arctic_universe_library)
        self.universes = self.universe_accessor.get_all()
        self.contract_subscriptions = {}
        self.client.ib.connectedEvent += self.connected_event
        self.client.ib.disconnectedEvent += self.disconnected_event
        self.client.connect()

    def reconnect(self):
        # this will force a reconnect through the disconnected event
        self.client.ib.disconnect()

    async def __update_positions(self, positions: List[Position]):
        logging.debug('__update_positions')
        for position in positions:
            self.portfolio.add_position(position)

    async def __update_portfolio(self, portfolio_item: PortfolioItem):
        logging.debug('__update_portfolio')
        self.portfolio.add_portfolio_item(portfolio_item=portfolio_item)
        await self.update_portfolio_universe()

    async def __update_book(self, trade: Trade):
        logging.debug('__update_book')
        self.book.add_update_trade(trade)

    async def setup_subscriptions(self):
        if not self.is_ib_connected():
            raise ConnectionError('not connected to interactive brokers')

        error = False

        async def handle_subscription_exception(ex):
            logging.exception(ex)
            error = True

        positions = await self.client.subscribe_positions()
        await positions.subscribe_async(AsyncCachedObserver(self.__update_positions,
                                                            athrow=handle_subscription_exception,
                                                            capture_asend_exception=True))

        portfolio = await self.client.subscribe_portfolio()
        await portfolio.subscribe_async(AsyncCachedObserver(self.__update_portfolio,
                                                            athrow=handle_subscription_exception,
                                                            capture_asend_exception=True))

        # because the portfolio subscription is synchronous, an observer isn't attached
        # as the ib.portfolio() method is called, so call it again
        for p in self.client.ib.portfolio():
            await self.client.portfolio_subject.asend(p)

        # make sure we're getting either live, or delayed data
        await self.client.ib.reqMarketDataType(3)

    async def connected_event(self):
        logging.debug('connected_event')
        await self.setup_subscriptions()

    async def disconnected_event(self):
        logging.debug('disconnected_event')
        self.connect()

    async def update_portfolio_universe(self):
        universe = self.universe_accessor.get('portfolio')

        # find missing contracts
        missing_contract_list: List[Contract] = []
        for portfolio_item in self.portfolio.get_portfolio_items():
            if not universe.find_contract(portfolio_item.contract):
                missing_contract_list.append(portfolio_item.contract)

        for contract in missing_contract_list:
            contract_details = await self.client.get_contract_details(contract)
            if contract_details and len(contract_details) >= 1:
                universe.security_definitions.append(SecurityDefinition.from_contract_details(contract_details[0]))

        if len(missing_contract_list) > 0:
            logging.debug('updating portfolio universe with {} securities'.format(str(len(missing_contract_list))))
            self.universe_accessor.update(universe)

    async def place_order(self, contract: Contract, order: Order) -> AsyncCachedSubject[Trade]:
        async def handle_exception(ex):
            logging.exception(ex)
            # todo sort out the book here

        order_stream = await self.client.subscribe_place_order(contract, order)
        await order_stream.subscribe_async(AsyncCachedObserver(self.__update_book,
                                                               athrow=handle_exception,
                                                               capture_asend_exception=True))
        return order_stream

    async def handle_order(
        self,
        contract: Contract,
        action: Action,
        amount: float
    ) -> AsyncCachedObserver[Trade]:
        # todo make sure amount is less than outstanding profit

        # grab the latest price of instrument
        subject = await self.client.subscribe_contract(contract=contract, snapshot=True)

        xs = pipe(
            subject,
            Pipe[Ticker].take(1)
        )

        observer = AsyncCachedObserver[Ticker]()
        await xs.subscribe_async(observer)
        tick = await observer.wait_value()

        # assess if we should trade
        quantity = amount / tick.bid

        # put an order in
        order = LimitOrder(action=str(action), totalQuantity=quantity, lmtPrice=tick.bid)
        return await self.client.subscribe_place_order(contract=contract, order=order)

    def is_ib_connected(self) -> bool:
        return self.client.ib.isConnected()

    def red_button(self):
        self.client.ib.reqGlobalCancel()

    def status(self) -> Dict[str, bool]:
        # todo lots of work here
        status = {
            'ib_connected': self.client.ib.isConnected(),
            'arctic_connected': self.data is not None
        }
        return status

    def get_universes(self) -> List[Universe]:
        return self.universes

    def run(self):
        self.client.run()
