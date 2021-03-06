from distutils.log import error
import sys
import os
import time

from trader.objects import WhatToShow

# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

import pandas as pd
import datetime as dt
import backoff
import reactivex as rx
import reactivex.operators as ops
import threading

import trader.messaging.trader_service_api as bus
import asyncio
from asyncio.events import AbstractEventLoop
from enum import Enum

from trader.common.logging_helper import setup_logging, get_callstack
logging = setup_logging(module_name='trading_runtime')

from arctic import Arctic, TICK_STORE
from arctic.date import DateRange
from arctic.tickstore.tickstore import TickStore
from ib_insync.ib import IB
from ib_insync.contract import Contract, Forex, Future, Stock
from ib_insync.objects import PortfolioItem, Position, BarData
from ib_insync.order import LimitOrder, Order, Trade
from ib_insync.util import df
from ib_insync.ticker import Ticker
from eventkit import Event

# from trader.listeners.ibaiorx import IBAIORx, IBAIORxError
from trader.listeners.ibreactive import IBAIORx, IBAIORxError
from reactivex.abc import DisposableBase
from reactivex.observable import Observable
from reactivex.observer import Observer, AutoDetachObserver
from reactivex.scheduler import ThreadPoolScheduler, NewThreadScheduler
from reactivex.disposable import Disposable

from trader.common.contract_sink import ContractSink
from trader.common.listener_helpers import Helpers
from trader.common.observers import ConsoleObserver, ArcticObserver, ComplexConsoleObserver, ContractSinkObserver, NullObserver
from trader.common.exceptions import TraderException, TraderConnectionException
from trader.data.data_access import SecurityDefinition, TickData
from trader.data.universe import UniverseAccessor, Universe
from trader.container import Container
from trader.trading.book import BookSubject
from trader.trading.algo import Algo
from trader.trading.portfolio import Portfolio
from trader.trading.executioner import Executioner
from trader.trading.strategy import Strategy
from trader.common.reactivex import SuccessFailEnum, SuccessFailObservable, SuccessFail
from trader.common.singleton import Singleton
from trader.common.helpers import get_network_ip, Pipe, dateify, timezoneify, ListHelper
from trader.data.market_data import MarketData, SecurityDataStream
from trader.messaging.clientserver import RPCServer, MultithreadedTopicPubSub

from typing import List, Dict, Tuple, Callable, Optional, Set, Generic, TypeVar, cast, Union

# notes
# https://groups.io/g/insync/topic/using_reqallopenorders/27261173?p=,,,20,0,0,0::recentpostdate%2Fsticky,,,20,2,0,27261173
# talks about trades/orders being tied to clientId, which means we'll need to always have a consistent clientid

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
                 zmq_pubsub_server_address: str,
                 zmq_pubsub_server_port: int,
                 zmq_rpc_server_address: str,
                 zmq_rpc_server_port: int,
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
        self.zmq_pubsub_server_address = zmq_pubsub_server_address
        self.zmq_pubsub_server_port = zmq_pubsub_server_port
        self.zmq_rpc_server_address = zmq_rpc_server_address
        self.zmq_rpc_server_port = zmq_rpc_server_port

        # todo I think you can have up to 24 connections to TWS (and have multiple TWS instances running)
        # so we need to take this from single client, to multiple client
        self.client: IBAIORx
        self.data: TickData
        self.universe_accessor: UniverseAccessor

        # the live ticker data streams we have
        self.contract_subscriptions: Dict[Contract, ContractSink] = {}
        # the minute-by-minute MarketData stream's we're subscribed to
        self.market_data_subscriptions: Dict[SecurityDefinition, SecurityDataStream] = {}
        # the strategies we're using
        self.strategies: List[Strategy] = []
        # current order book (outstanding orders, trades etc)
        self.book: BookSubject = BookSubject()
        # portfolio (current and past positions)
        self.portfolio: Portfolio = Portfolio()
        # takes care of execution of orders
        self.executioner: Executioner
        # a list of all the universes of stocks we have registered
        self.universes: List[Universe]
        self.market_data = 3
        self.zmq_rpc_server: RPCServer[bus.TraderServiceApi]
        self.zmq_pubsub_server: MultithreadedTopicPubSub[Ticker]
        self.zmq_pubsub_contracts: Dict[int, Observable[IBAIORxError]] = {}
        self.zmq_pubsub_contract_filters: Dict[int, bool] = {}
        self.zmq_pubsub_contract_subscription: DisposableBase = Disposable()
        self.startup_time: dt.datetime = dt.datetime.now()
        self.last_connect_time: dt.datetime
        self.load_test: bool = False

    def create_trader_exception(self, exception_type: type, message: str, inner: Optional[Exception]):
        # todo use reflection here to automatically populate trader runtime vars that we care about
        # given a particular exception type
        data = self.data if hasattr(self, 'data') else None
        client = self.client.is_connected() if hasattr(self, 'client') else False
        last_connect_time = self.last_connect_time if hasattr(self, 'last_connect_time') else dt.datetime.min

        exception = exception_type(
            data is not None,
            client,
            self.startup_time,
            last_connect_time,
            message,
            inner,
            get_callstack(10)
        )
        logging.exception(exception)
        return exception

    @backoff.on_exception(backoff.expo, ConnectionRefusedError, max_tries=10, max_time=120)
    def connect(self):
        try:
            self.client = IBAIORx(self.ib_server_address, self.ib_server_port)
            self.data = TickData(self.arctic_server_address, self.arctic_library)
            self.universe_accessor = UniverseAccessor(self.arctic_server_address, self.arctic_universe_library)
            self.universes = self.universe_accessor.get_all()
            self.clear_portfolio_universe()
            self.contract_subscriptions = {}
            self.market_data_subscriptions = {}
            self.client.ib.connectedEvent += self.connected_event
            self.client.ib.disconnectedEvent += self.disconnected_event
            self.client.connect()
            self.last_connect_time = dt.datetime.now()
            self.zmq_rpc_server = RPCServer[bus.TraderServiceApi](bus.TraderServiceApi(self))
            self.zmq_pubsub_server = MultithreadedTopicPubSub[Ticker](
                zmq_pubsub_server_address=self.zmq_pubsub_server_address,
                zmq_pubsub_server_port=self.zmq_pubsub_server_port
            )
            self.zmq_pubsub_server.start()
            self.zmq_pubsub_contracts = {}
            self.zmq_pubsub_contract_filters = {}
            self.zmq_pubsub_contract_subscription = Disposable()

            self.run(self.zmq_rpc_server.serve())
        except Exception as ex:
            raise self.create_trader_exception(type(TraderConnectionException), message='connect() exception', inner=ex)

    async def shutdown(self):
        logging.debug('trading_runtime.shutdown()')
        self.client.ib.connectedEvent -= self.connected_event
        self.client.ib.disconnectedEvent -= self.disconnected_event
        self.client.ib.disconnect()

        for contract, sink in self.contract_subscriptions.items():
            sink.dispose()

        self.zmq_pubsub_contract_subscription.dispose()

        # for security_definition, security_datastream in self.market_data_subscriptions.items():
        #   security_datastream.dispose()

        await self.book.dispose_async()
        await self.client.shutdown()

    def reconnect(self):
        # this will force a reconnect through the disconnected event
        self.client.ib.disconnect()

    def __update_positions(self, positions: List[Position]):
        logging.debug('__update_positions')
        for position in positions:
            self.portfolio.add_position(position)

    def __update_portfolio(self, portfolio_item: PortfolioItem):
        logging.debug('__update_portfolio')
        self.portfolio.add_portfolio_item(portfolio_item=portfolio_item)
        self.update_portfolio_universe(portfolio_item)

    async def setup_subscriptions(self):
        if not self.is_ib_connected():
            raise ConnectionError('not connected to interactive brokers')

        def handle_subscription_exception(ex):
            exception = self.create_trader_exception(TraderException, message='setup_subscriptions()', inner=ex)
            raise exception

        def handle_completed():
            logging.debug('handle_completed()')

        # have the book subscribe to all relevant trade events
        await self.book.subscribe_to_eventkit_event(
            [
                self.client.ib.orderStatusEvent,
                self.client.ib.orderModifyEvent,
                self.client.ib.newOrderEvent,
                self.client.ib.cancelOrderEvent,
                self.client.ib.openOrderEvent,
            ]
        )

        positions = await self.client.subscribe_positions()
        positions.subscribe(Observer(
            on_next=self.__update_positions,
            on_error=handle_subscription_exception,
            on_completed=handle_completed
        ))
        portfolio = await self.client.subscribe_portfolio()
        portfolio.subscribe(Observer(
            on_next=self.__update_portfolio,
            on_error=handle_subscription_exception,
        ))

        # because the portfolio subscription is synchronous, an observer isn't attached
        # as the ib.portfolio() method is called, so call it again
        for p in self.client.ib.portfolio():
            self.client.portfolio_subject.on_next(p)

        # make sure we're getting either live, or delayed data
        self.client.ib.reqMarketDataType(self.market_data)

        orders = await self.client.ib.reqAllOpenOrdersAsync()
        for o in orders:
            await self.book.asend(o)

    async def connected_event(self):
        logging.debug('connected_event')
        await self.setup_subscriptions()

    async def disconnected_event(self):
        logging.debug('disconnected_event')
        self.connect()

    def clear_portfolio_universe(self):
        logging.debug('clearing portfolio universe')
        universe = self.universe_accessor.get('portfolio')
        universe.security_definitions.clear()
        self.universe_accessor.update(universe)

    def publish_contract(self, contract: Contract, delayed: bool) -> Observable[IBAIORxError]:
        if contract.conId in self.zmq_pubsub_contract_filters:
            return self.zmq_pubsub_contracts[contract.conId]

        def on_next(ticker: Ticker):
            self.zmq_pubsub_server.put(('ticker', ticker))

        def on_completed():
            del self.zmq_pubsub_contracts[contract.conId]
            del self.zmq_pubsub_contract_filters[contract.conId]
            logging.debug('publish_contract.aclose() for {}'.format(contract))

        def on_error(ex):
            del self.zmq_pubsub_contracts[contract.conId]
            del self.zmq_pubsub_contract_filters[contract.conId]
            raise self.create_trader_exception(TraderException, message='publish_contract() on_error', inner=ex)

        if len(self.zmq_pubsub_contract_filters) == 0:
            # setup the observable for the first time
            try:
                auto_detach = AutoDetachObserver(on_next=on_next, on_completed=on_completed, on_error=on_error)
                subscription = self.client.contracts_subject.subscribe(auto_detach)  # , scheduler=NewThreadScheduler())
                self.zmq_pubsub_contract_subscription = subscription
            except Exception as ex:
                # todo not sure how to deal with this error condition yet
                raise self.create_trader_exception(TraderException, message='publish_contract()', inner=ex)

        error_observable = self.client.subscribe_contract_direct(contract, delayed=delayed)
        self.zmq_pubsub_contract_filters[contract.conId] = True
        self.zmq_pubsub_contracts[contract.conId] = error_observable
        return error_observable

    def update_portfolio_universe(self, portfolio_item: PortfolioItem):
        """
        Grabs the current portfolio from TWS and adds a new version to the 'portfolio' table.
        """
        universe = self.universe_accessor.get('portfolio')
        if not ListHelper.isin(
            universe.security_definitions,
            lambda definition: definition.conId == portfolio_item.contract.conId
        ):
            contract = portfolio_item.contract
            contract_details = self.client.get_contract_details(contract)
            if contract_details and len(contract_details) >= 1:
                universe.security_definitions.append(
                    SecurityDefinition.from_contract_details(contract_details[0])
                )

            logging.debug('updating portfolio universe with {}'.format(portfolio_item))
            self.universe_accessor.update(universe)

            if not ListHelper.isin(
                list(self.market_data_subscriptions.keys()),
                lambda subscription: subscription.conId == portfolio_item.contract.conId
            ):
                logging.debug('subscribing to market data stream for portfolio item {}'.format(portfolio_item.contract))
                security = cast(SecurityDefinition, universe.find_contract(portfolio_item.contract))
                date_range = DateRange(
                    start=dateify(dt.datetime.now() - dt.timedelta(days=30)),
                    end=timezoneify(dt.datetime.now(), timezone='America/New_York')
                )
                security_stream = SecurityDataStream(
                    security=security,
                    bar_size='1 min',
                    date_range=date_range,
                    existing_data=None
                )
                # await self.client.subscribe_contract_history(
                #     contract=portfolio_item.contract,
                #     start_date=dateify(dt.datetime.now() - dt.timedelta(days=30)),
                #     what_to_show=WhatToShow.TRADES,
                #     observer=security_stream
                # )
                self.market_data_subscriptions[security] = security_stream

    def start_load_test(self):
        async def _load_test_helper():
            amd = Contract(symbol='AMD', conId=4391, exchange='SMART', primaryExchange='NASDAQ', currency='USD')
            ticker = Ticker(
                contract=amd,
                time=dt.datetime.now(),
                bid=87.05,
                ask=87.06,
                prevBid=87.05,
                prevAsk=87.06,
                askSize=100.0,
                bidSize=100.0,
                prevAskSize=100.0,
                prevBidSize=100.0,
                lastSize=0,
                halted=0,
                close=85.00,
                low=84.00,
                high=86.00,
                open=85.50,
                last=87.05,
            )
            counter = 0
            timer = dt.datetime.now()
            while self.load_test:
                self.client._contracts_source.on_next(set([ticker]))

                # asyncio.sleep(0)
                # any asyncio.sleep here seems to give us a 100x slowdown.
                # await asyncio.sleep(0.000001)
                # sleep 0.000001 give us about 9000 /sec.
                # asyncio.sleep(0) gives us about 29k tickers/sec
                # no sleep gives us 400k/sec but no active control over the process
                counter = counter + 1
                delta = dt.datetime.now() - timer
                if delta.seconds >= 10:
                    task_num = len(asyncio.all_tasks())
                    threading_num = threading.active_count()
                    logging.critical('{} tickers per second, {} tasks, {} threads'.format(float(counter) / 10.0, task_num, threading_num))
                    counter = 0
                    timer = dt.datetime.now()
            logging.debug('load test stopped')

        self.load_test = True
        logging.critical('starting start_load_test()')
        task = asyncio.create_task(_load_test_helper())

    # async def temp_place_order(
    #     self,
    #     contract: Contract,
    #     order: Order
    # ) -> AsyncCachedObserver[Trade]:
    #     async def handle_exception(ex):
    #         logging.exception(ex)
    #         # todo sort out the book here

    #     async def handle_trade(trade: Trade):
    #         logging.debug('handle_trade {}'.format(trade))
    #         # todo figure out what we want to do here

    #     observer = AsyncCachedObserver(asend=handle_trade,
    #                                    athrow=handle_exception,
    #                                    capture_asend_exception=True)

    #     disposable = await self.client.subscribe_place_order(contract, order, observer)
    #     return observer

    # async def temp_handle_order(
    #     self,
    #     contract: Contract,
    #     action: Action,
    #     equity_amount: float,
    #     delayed: bool = False,
    #     debug: bool = False,
    # ) -> AsyncCachedObserver[Trade]:
    #     # todo make sure amount is less than outstanding profit

    #     # grab the latest price of instrument
    #     subject = await self.client.subscribe_contract(contract=contract, one_time_snapshot=True)

    #     xs = pipe(
    #         subject,
    #         Pipe[Ticker].take(1)
    #     )

    #     observer = AsyncCachedObserver[Ticker]()
    #     await xs.subscribe_async(observer)
    #     latest_tick = await observer.wait_value()

    #     # todo perform tick sanity checks

    #     # assess if we should trade
    #     quantity = equity_amount / latest_tick.bid

    #     if quantity < 1 and quantity > 0:
    #         quantity = 1.0

    #     # round the quantity
    #     quantity_int = round(quantity)

    #     logging.debug('temp_handle_order assessed quantity: {} on bid: {}'.format(
    #         quantity_int, latest_tick.bid
    #     ))

    #     limit_price = latest_tick.bid
    #     # if debug, move the buy/sell by 10%
    #     if debug and action == Action.BUY:
    #         limit_price = limit_price * 0.9
    #         limit_price = round(limit_price * 0.9, ndigits=2)
    #     if debug and action == Action.SELL:
    #         limit_price = round(limit_price * 1.1, ndigits=2)

    #     # put an order in
    #     order = LimitOrder(action=str(action), totalQuantity=quantity_int, lmtPrice=limit_price)
    #     return await self.temp_place_order(contract=contract, order=order)

    def cancel_order(self, order_id: int) -> Optional[Trade]:
        # get the Order
        order = self.book.get_order(order_id)
        if order and order.clientId == self.client.client_id_counter:
            logging.info('cancelling order {}'.format(order))
            trade = self.client.ib.cancelOrder(order)
            return trade
        else:
            logging.error('either order does not exist, or originating client_id is different: {} {}'
                          .format(order, self.client.client_id_counter))
            return None

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

    def run(self, *args):
        self.client.run(*args)
