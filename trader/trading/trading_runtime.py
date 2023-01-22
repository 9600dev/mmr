

import os
import sys


# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from ib_insync.contract import Contract
from ib_insync.objects import PortfolioItem, Position
from ib_insync.order import LimitOrder, MarketOrder, Order, StopLimitOrder, StopOrder, Trade
from ib_insync.ticker import Ticker
from reactivex.abc import DisposableBase, ObserverBase
from reactivex.disposable import Disposable
from reactivex.observable import Observable
from reactivex.observer import AutoDetachObserver, Observer
from reactivex.subject import Subject
from trader.common.contract_sink import ContractSink
from trader.common.exceptions import TraderConnectionException, TraderException
from trader.common.helpers import ListHelper
from trader.common.logging_helper import get_callstack, setup_logging
from trader.common.singleton import Singleton
from trader.data.data_access import SecurityDefinition, TickStorage
from trader.data.market_data import SecurityDataStream
from trader.data.universe import Universe, UniverseAccessor
from trader.listeners.ibreactive import IBAIORx, IBAIORxError
from trader.messaging.clientserver import MultithreadedTopicPubSub, RPCServer
from trader.objects import Action
from trader.trading.book import BookSubject
from trader.trading.executioner import TradeExecutioner
from trader.trading.portfolio import Portfolio
# from trader.trading.strategy import Strategy
from typing import cast, Dict, List, Optional, Union

import asyncio
import backoff
import datetime as dt
import threading
import trader.messaging.trader_service_api as bus


logging = setup_logging(module_name='trading_runtime')


# notes
# https://groups.io/g/insync/topic/using_reqallopenorders/27261173?p=,,,20,0,0,0::recentpostdate%2Fsticky,,,20,2,0,27261173
# talks about trades/orders being tied to clientId, which means we'll need to always have a consistent clientid


class Trader(metaclass=Singleton):
    def __init__(self,
                 ib_server_address: str,
                 ib_server_port: int,
                 trading_runtime_ib_client_id: int,
                 arctic_server_address: str,
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
        self.trading_runtime_ib_client_id = trading_runtime_ib_client_id
        self.arctic_server_address = arctic_server_address
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
        self.data: TickStorage
        self.universe_accessor: UniverseAccessor

        # the live ticker data streams we have
        self.contract_subscriptions: Dict[Contract, ContractSink] = {}
        # the minute-by-minute MarketData stream's we're subscribed to
        self.market_data_subscriptions: Dict[SecurityDefinition, SecurityDataStream] = {}
        # the strategies we're using
        # self.strategies: List[Strategy] = []
        # current order book (outstanding orders, trades etc)
        self.book: BookSubject = BookSubject()
        # portfolio (current and past positions)
        self.portfolio: Portfolio = Portfolio()
        # takes care of execution of orders
        self.executioner: TradeExecutioner
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
        self.tws_client_ids: List[int] = [self.trading_runtime_ib_client_id, self.trading_runtime_ib_client_id + 1]

        self.disposables: List[DisposableBase] = []

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
            self.client = IBAIORx(self.ib_server_address, self.ib_server_port, self.trading_runtime_ib_client_id)
            self.data = TickStorage(self.arctic_server_address)
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
            raise self.create_trader_exception(TraderConnectionException, message='connect() exception', inner=ex)

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

        for disposable in self.disposables:
            disposable.dispose()

        self.book.dispose()
        await self.client.shutdown()

    def reconnect(self):
        # this will force a reconnect through the disconnected event
        self.client.ib.disconnect()

    def __update_positions(self, positions: Union[List[Position], Position]):
        logging.debug('__update_positions')
        if type(positions) is Position:
            self.portfolio.add_position(positions)
        elif type(positions) is list:
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

        positions_observer = Observer(
            on_next=self.__update_positions,
            on_error=handle_subscription_exception,
            on_completed=handle_completed
        )

        positions_disposable = await self.client.subscribe_positions(positions_observer)
        self.disposables.append(positions_disposable)

        portfolio_disposable = await self.client.subscribe_portfolio(Observer(
            on_next=self.__update_portfolio,
            on_error=handle_subscription_exception,
        ))
        self.disposables.append(portfolio_disposable)

        # make sure we're getting either live, or delayed data
        self.client.ib.reqMarketDataType(self.market_data)

        orders = await self.client.ib.reqAllOpenOrdersAsync()
        for o in orders:
            self.book.on_next(o)

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

            # if not ListHelper.isin(
            #     list(self.market_data_subscriptions.keys()),
            #     lambda subscription: subscription.conId == portfolio_item.contract.conId
            # ):
            #     logging.debug('subscribing to market data stream for portfolio item {}'.format(portfolio_item.contract))
            #     security = cast(SecurityDefinition, universe.find_contract(portfolio_item.contract))
            #     date_range = DateRange(
            #         start=dateify(dt.datetime.now() - dt.timedelta(days=30)),
            #         end=timezoneify(dt.datetime.now(), timezone='America/New_York')
            #     )
            #     security_stream = SecurityDataStream(
            #         security=security,
            #         bar_size='1 min',
            #         date_range=date_range,
            #         existing_data=None
            #     )
            #     await self.client.subscribe_contract_history(
            #         contract=portfolio_item.contract,
            #         start_date=dateify(dt.datetime.now() - dt.timedelta(days=30)),
            #         what_to_show=WhatToShow.TRADES,
            #         observer=security_stream
            #     )
            #     self.market_data_subscriptions[security] = security_stream

    async def place_order(
        self,
        contract: Contract,
        order: Order,
        observer: ObserverBase[Trade],
    ) -> DisposableBase:
        disposable: DisposableBase = Disposable()
        try:
            disposable = await self.client.subscribe_place_order(contract, order, observer)
        except Exception as ex:
            # todo not sure how to deal with this error condition yet
            raise self.create_trader_exception(TraderException, message='place_order()', inner=ex)
        return disposable

    async def handle_order(
        self,
        contract: Contract,
        action: Action,
        equity_amount: Optional[float],
        quantity: Optional[float],
        limit_price: Optional[float],
        market_order: bool,
        stop_loss_percentage: float,
        observer: Observer[Trade],
        debug: bool = False,
    ) -> DisposableBase:
        # todo make sure amount is less than outstanding profit

        if limit_price and limit_price <= 0.0:
            raise ValueError('limit_price specified but invalid: {}'.format(limit_price))
        if stop_loss_percentage >= 1.0 or stop_loss_percentage < 0.0:
            raise ValueError('stop_loss_percentage invalid: {}'.format(stop_loss_percentage))
        if not equity_amount and not quantity:
            raise ValueError('equity_amount or quantity need to be specified')

        # grab the latest price of instrument
        latest_tick: Ticker = await self.client.get_snapshot(contract, delayed=False)
        order_price = 0.0

        if not quantity and equity_amount:
            # assess if we should trade
            quantity = equity_amount / latest_tick.bid

            if quantity < 1 and quantity > 0:
                quantity = 1.0

            # toddo round the quantity, but probably shouldn't do this given IB supports fractional shares.
            quantity = round(quantity)

        logging.debug('handle_order assessed quantity: {} on bid: {}'.format(
            quantity, latest_tick.bid
        ))

        if limit_price:
            order_price = float(limit_price)
        elif market_order:
            order_price = latest_tick.ask

        # if debug, move the buy/sell by 10%
        if debug and action == Action.BUY:
            order_price = order_price * 0.9
            order_price = round(order_price * 0.9, ndigits=2)
        if debug and action == Action.SELL:
            order_price = round(order_price * 1.1, ndigits=2)

        stop_loss_price = 0.0

        # calculate stop_loss
        if stop_loss_percentage > 0.0:
            stop_loss_price = round(order_price - order_price * stop_loss_percentage, ndigits=2)

        subject = Subject[Trade]()
        disposable: DisposableBase = Disposable()

        def on_next(trade: Trade):
            logging.debug('handle_order.on_next()')
            subject.on_next(trade)

        def on_completed():
            logging.debug('handle_order.on_completed()')
            subject.on_completed()
            disposable.dispose()

        def on_error(ex):
            logging.debug('handle_order.on_error()')
            # todo: retry logic here
            subject.on_error(ex)

        # put an order in
        disposable = subject.subscribe(observer)
        order: Order = Order()

        if market_order and stop_loss_price > 0:
            order = StopOrder(action=str(action), totalQuantity=cast(float, quantity), stopPrice=stop_loss_price)
        elif market_order and stop_loss_price == 0.0:
            order = MarketOrder(action=str(action), totalQuantity=cast(float, quantity))

        if not market_order and stop_loss_price > 0:
            order = StopLimitOrder(
                action=str(action),
                totalQuantity=cast(float, quantity),
                lmtPrice=order_price,
                stopPrice=stop_loss_price
            )
        elif not market_order and stop_loss_price == 0.0:
            order = LimitOrder(action=str(action), totalQuantity=cast(float, quantity), lmtPrice=order_price)

        disposable = await self.place_order(contract=contract, order=order, observer=subject)
        return disposable

    def cancel_order(self, order_id: int) -> Optional[Trade]:
        # get the Order
        order = self.book.get_order(order_id)
        if order and order.clientId == self.trading_runtime_ib_client_id:
            logging.info('cancelling order {}'.format(order))
            trade = self.client.ib.cancelOrder(order)
            return trade
        else:
            logging.error('either order does not exist, or originating client_id is different: {} {}'
                          .format(order, self.trading_runtime_ib_client_id))
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

    def get_unique_client_id(self) -> int:
        new_client_id = max(self.tws_client_ids) + 1
        self.tws_client_ids.append(new_client_id)
        self.tws_client_ids.append(new_client_id + 1)
        return new_client_id

    def release_client_id(self, client_id: int):
        if client_id in self.tws_client_ids:
            self.tws_client_ids.remove(client_id)

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
                    logging.critical(
                        '{} tickers per second, {} tasks, {} threads'.format(
                            float(counter) / 10.0,
                            task_num,
                            threading_num
                        )
                    )
                    counter = 0
                    timer = dt.datetime.now()
            logging.debug('load test stopped')

        self.load_test = True
        logging.critical('starting start_load_test()')
        task = asyncio.create_task(_load_test_helper())

    def run(self, *args):
        self.client.run(*args)
