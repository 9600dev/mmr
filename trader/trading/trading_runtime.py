import os
import sys


# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from ib_insync.contract import Contract
from ib_insync.objects import PnLSingle, PortfolioItem, Position
from ib_insync.order import LimitOrder, MarketOrder, Order, StopLimitOrder, StopOrder, Trade
from ib_insync.ticker import Ticker
from reactivex import pipe
from reactivex.abc import DisposableBase, ObserverBase
from reactivex.disposable import Disposable
from reactivex.observable import Observable
from reactivex.observer import AutoDetachObserver, Observer
from reactivex.scheduler.eventloop.asynciothreadsafescheduler import AsyncIOThreadSafeScheduler
from reactivex.subject import Subject
from trader.common.contract_sink import ContractSink
from trader.common.dataclass_cache import DataClassCache, DataClassEvent, UpdateEvent
from trader.common.exceptions import trader_exception, TraderConnectionException, TraderException
from trader.common.helpers import ListHelper
from trader.common.logging_helper import get_callstack, log_method, setup_logging
from trader.common.reactivex import AnonymousObserver, SuccessFail
from trader.common.singleton import Singleton
from trader.data.data_access import PortfolioSummary, SecurityDefinition, TickStorage
from trader.data.market_data import SecurityDataStream
from trader.data.universe import Universe, UniverseAccessor
from trader.listeners.ibreactive import IBAIORx, IBAIORxError
from trader.messaging.clientserver import MessageBusServer, MultithreadedTopicPubSub, RPCClient, RPCServer
from trader.objects import Action, ContractOrderPair, ExecutorCondition
from trader.trading.book import BookSubject
from trader.trading.executioner import TradeExecutioner
from trader.trading.portfolio import Portfolio
from trader.trading.strategy import Strategy, StrategyConfig, StrategyState
from typing import cast, Dict, List, NamedTuple, Optional, Tuple, Union

import asyncio
import backoff
import datetime as dt
import reactivex as rx
import reactivex.operators as ops
import threading
import trader.messaging.strategy_service_api as strategy_bus
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
                 zmq_strategy_rpc_server_address: str,
                 zmq_strategy_rpc_server_port: int,
                 zmq_messagebus_server_address: str,
                 zmq_messagebus_server_port: int,
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
        self.zmq_strategy_rpc_server_address = zmq_strategy_rpc_server_address
        self.zmq_strategy_rpc_server_port = zmq_strategy_rpc_server_port
        self.zmq_messagebus_server_address = zmq_messagebus_server_address
        self.zmq_messagebus_server_port = zmq_messagebus_server_port

        # todo I think you can have up to 24 connections to TWS (and have multiple TWS instances running)
        # so we need to take this from single client, to multiple client
        self.client: IBAIORx
        self.data: TickStorage
        self.universe_accessor: UniverseAccessor

        # the live ticker data streams we have
        self.contract_subscriptions: Dict[Contract, ContractSink] = {}
        # the minute-by-minute MarketData stream's we're subscribed to
        self.market_data_subscriptions: Dict[SecurityDefinition, SecurityDataStream] = {}

        # current order book (outstanding orders, trades etc)
        self.book: BookSubject = BookSubject()
        # portfolio (current and past positions)
        self.portfolio: Portfolio = Portfolio()
        # pnl for current portfolio
        self.pnl: DataClassCache = DataClassCache[PnLSingle](lambda pnl: str((pnl.account, pnl.conId)))
        self.pnl_subscriptions: Dict[Tuple[str, int], bool] = {}
        # takes care of execution of orders
        self.executioner: TradeExecutioner
        # a list of all the universes of stocks we have registered
        self.universes: List[Universe]
        self.market_data = 3
        self.zmq_rpc_server: RPCServer[bus.TraderServiceApi]
        self.zmq_pubsub_server: MultithreadedTopicPubSub[Ticker]
        self.zmq_dataclass_server: MultithreadedTopicPubSub[DataClassEvent]
        self.zmq_pubsub_contracts: Dict[int, Observable[IBAIORxError]] = {}
        self.zmq_pubsub_contract_filters: Dict[int, bool] = {}
        self.zmq_pubsub_contract_subscription: DisposableBase = Disposable()

        self.zmq_strategy_client: RPCClient[strategy_bus.StrategyServiceApi]
        self.zmq_messagebus: MessageBusServer

        self.startup_time: dt.datetime = dt.datetime.now()
        self.last_connect_time: dt.datetime
        self.load_test: bool = False
        self.tws_client_ids: List[int] = [self.trading_runtime_ib_client_id, self.trading_runtime_ib_client_id + 1]
        self.scheduler = AsyncIOThreadSafeScheduler(asyncio.get_event_loop())

        self.disposables: List[DisposableBase] = []

    @backoff.on_exception(backoff.expo, ConnectionRefusedError, max_tries=10, max_time=120)
    def connect(self):
        logging.debug('trading_runtime.connect() connecting to services: %s:%s' % (self.ib_server_address, self.ib_server_port))
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
            self.zmq_rpc_server = RPCServer[bus.TraderServiceApi](
                instance=bus.TraderServiceApi(self),
                zmq_rpc_server_address=self.zmq_rpc_server_address,
                zmq_rpc_server_port=self.zmq_rpc_server_port
            )
            self.zmq_pubsub_server = MultithreadedTopicPubSub[Ticker](
                zmq_pubsub_server_address=self.zmq_pubsub_server_address,
                zmq_pubsub_server_port=self.zmq_pubsub_server_port
            )
            self.zmq_pubsub_server.start()

            self.zmq_dataclass_server = MultithreadedTopicPubSub[DataClassEvent](
                zmq_pubsub_server_address=self.zmq_pubsub_server_address,
                zmq_pubsub_server_port=self.zmq_pubsub_server_port + 1
            )
            self.zmq_dataclass_server.start()

            self.zmq_messagebus = MessageBusServer(self.zmq_messagebus_server_address, self.zmq_messagebus_server_port)
            self.run(self.zmq_messagebus.start())

            self.zmq_pubsub_contracts = {}
            self.zmq_pubsub_contract_filters = {}
            self.zmq_pubsub_contract_subscription = Disposable()

            # connect to the strategy server
            self.zmq_strategy_client = RPCClient[strategy_bus.StrategyServiceApi](
                self.zmq_strategy_rpc_server_address,
                self.zmq_strategy_rpc_server_port,
                timeout=5,
            )

            # fire up the executioner
            self.executioner = TradeExecutioner()
            self.executioner.connect(self)

            self.run(self.zmq_strategy_client.connect())
            self.run(self.zmq_rpc_server.serve())

        except Exception as ex:
            raise trader_exception(self, TraderConnectionException, message='trading_runtime connect() exception', inner=ex)

    @log_method
    async def shutdown(self):
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

    @log_method
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

    def __dataclass_server_put(self, message: DataClassEvent):
        # logging.debug('__dataclass_server_put: {}'.format(message))
        self.zmq_dataclass_server.put(('dataclass', message))

    @log_method
    async def setup_subscriptions(self):
        if not self.is_ib_connected():
            raise ConnectionError('not connected to interactive brokers')

        def handle_subscription_exception(ex):
            exception = trader_exception(self, TraderException, message='setup_subscriptions()', inner=ex)
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

        positions_disposable = (await self.client.subscribe_positions()).subscribe(positions_observer)
        self.disposables.append(positions_disposable)

        portfolio_disposable = (await self.client.subscribe_portfolio()).subscribe(AnonymousObserver(
            on_next=self.__update_portfolio,
            on_error=handle_subscription_exception,
        ))
        self.disposables.append(portfolio_disposable)

        # subscribe to all portfolio changes, then make sure we're subscribing to the pnl for each
        def __subscribe_pnl(portfolio_item: PortfolioItem):
            # todo this is a hack, we need to figure out how to make this work with async
            async def __async_subscribe_pnl(portfolio_item: PortfolioItem):
                if portfolio_item.contract and (portfolio_item.account, portfolio_item.contract.conId) not in self.pnl_subscriptions:  # noqa: E501
                    observable = await self.client.subscribe_single_pnl(
                        portfolio_item.account,
                        portfolio_item.contract,
                    )
                    disposable = observable.subscribe(self.pnl.create_observer(error_func=handle_subscription_exception))

                    self.disposables.append(disposable)
                    self.pnl_subscriptions.update({(portfolio_item.account, portfolio_item.contract.conId): True})

            self.run(__async_subscribe_pnl(portfolio_item))

        disposable = (await self.client.subscribe_portfolio()).subscribe(
            AnonymousObserver(
                on_next=__subscribe_pnl,
                on_error=handle_subscription_exception,
            )
        )
        self.disposables.append(disposable)

        pnl_router_disposable = self.pnl.subscribe(on_next=self.__dataclass_server_put, on_error=handle_subscription_exception)
        self.disposables.append(pnl_router_disposable)

        # push book updates
        def __update_book(trade_order: Union[Trade, Order]):
            event = UpdateEvent(trade_order)
            self.__dataclass_server_put(event)

        book_update_disposable = self.book.subscribe(
            Observer(
                on_next=__update_book,
                on_error=handle_subscription_exception,
            )
        )
        self.disposables.append(book_update_disposable)

        # make sure we're getting either live, or delayed data
        self.client.ib.reqMarketDataType(self.market_data)

        orders = await self.client.ib.reqAllOpenOrdersAsync()
        for o in orders:
            self.book.on_next(o)

        # ensure that pnl is getting pumped out of zmq
        scheduled_disposable = self.scheduler.schedule_periodic(10, lambda x: self.pnl.post_all())
        self.disposables.append(scheduled_disposable)

    @log_method
    async def connected_event(self):
        await self.setup_subscriptions()

    @log_method
    async def disconnected_event(self):
        self.connect()

    @log_method
    def enable_strategy(self, name: str, paper: bool) -> SuccessFail[StrategyState]:
        try:
            return self.zmq_strategy_client.rpc().enable_strategy(name, paper)
        except Exception as ex:
            logging.error('enable_strategy: {}'.format(ex))
            return SuccessFail.fail(exception=ex)

    @log_method
    def disable_strategy(self, name: str) -> SuccessFail[StrategyState]:
        try:
            return self.zmq_strategy_client.rpc().disable_strategy(name)
        except Exception as ex:
            logging.error('disable_strategy: {}'.format(ex))
            return SuccessFail.fail(exception=ex)

    @log_method
    def get_strategies(self) -> SuccessFail[List[StrategyConfig]]:
        try:
            return SuccessFail.success(obj=self.zmq_strategy_client.rpc().get_strategies())
        except Exception as ex:
            return SuccessFail.fail(exception=ex)

    @log_method
    def clear_portfolio_universe(self):
        universe = self.universe_accessor.get('portfolio')
        universe.security_definitions.clear()
        self.universe_accessor.update(universe)

    def resolve_symbol_to_security_definitions(self, symbol: Union[str, int]) -> list[Tuple[Universe, SecurityDefinition]]:
        int_symbol = 0

        if type(symbol) is str and symbol.isnumeric():
            int_symbol = int(symbol)
        if type(symbol) is int:
            int_symbol = symbol

        # see if conid is in the cache
        if int_symbol > 0:
            # not in the cache
            result = self.universe_accessor.resolve_conid(int_symbol)
            if result: return [result]
            return []
        else:
            # not in the cache
            universe_definition = self.universe_accessor.resolve_first_symbol(symbol)
            if universe_definition:
                return [universe_definition]
            return []

    @log_method
    def resolve_symbol(
        self,
        symbol: Union[str, int],
        primary_exchange: str = ''
    ) -> Optional[SecurityDefinition]:
        # todo
        # because resolving conids is all messed up, we're going to try a few heuristics
        int_symbol = 0

        if type(symbol) is str and symbol.isnumeric():
            int_symbol = int(symbol)
        if type(symbol) is int:
            int_symbol = symbol

        result = self.resolve_symbol_to_security_definitions(symbol)
        if result and int_symbol > 0:
            # we know we used conid here, they're unique so...
            return result[0][1]
        if result and int_symbol == 0:
            for _, security_definition in result:
                for portfolio_item in self.portfolio.get_portfolio_items():
                    if (
                        portfolio_item.contract.symbol == security_definition.symbol
                        or portfolio_item.contract.localSymbol == security_definition.symbol
                    ):
                        return security_definition

        contract_details = self.run(
            self.client.ib.reqContractDetailsAsync(
                Contract(symbol=str(symbol), exchange='SMART')
            )
        )

        if contract_details:
            return SecurityDefinition.from_contract_details(contract_details[0])
        else:
            return None

    @log_method
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
            raise trader_exception(self, TraderException, message='publish_contract() on_error', inner=ex)

        if len(self.zmq_pubsub_contract_filters) == 0:
            # setup the observable for the first time
            try:
                auto_detach = AutoDetachObserver(on_next=on_next, on_completed=on_completed, on_error=on_error)
                subscription = self.client.contracts_subject.subscribe(auto_detach)  # , scheduler=NewThreadScheduler())
                self.zmq_pubsub_contract_subscription = subscription
            except Exception as ex:
                # todo not sure how to deal with this error condition yet
                raise trader_exception(self, TraderException, message='publish_contract()', inner=ex)

        error_observable = self.client.subscribe_contract_direct(contract, delayed=delayed)
        self.zmq_pubsub_contract_filters[contract.conId] = True
        self.zmq_pubsub_contracts[contract.conId] = error_observable
        return error_observable

    @log_method
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

    @log_method
    async def place_order(
        self,
        contract: Contract,
        order: Order,
        condition: ExecutorCondition,
    ) -> Observable[Trade]:
        return await self.executioner.place_order(contract_order=ContractOrderPair(contract, order), condition=condition)

    @log_method
    async def place_order_simple(
        self,
        contract: Contract,
        action: Action,
        equity_amount: Optional[float],
        quantity: Optional[float],
        limit_price: Optional[float],
        market_order: bool,
        stop_loss_percentage: float,
        algo_name: str = 'global',
        debug: bool = False,
    ) -> Observable[Trade]:
        latest_tick: Ticker = await self.client.get_snapshot(contract)

        contract_order = self.executioner.helper_create_order(
            contract,
            action,
            latest_tick,
            equity_amount,
            quantity,
            limit_price,
            market_order,
            stop_loss_percentage,
            algo_name=algo_name,
            debug=debug
        )
        return await self.executioner.place_order(contract_order=contract_order, condition=ExecutorCondition.SANITY_CHECK)

    @log_method
    def cancel_order(self, order_id: int) -> Optional[Trade]:
        return self.executioner.cancel_order_id(order_id)

    @log_method
    def cancel_all(self) -> SuccessFail[List[int]]:
        failed_cancels = []
        for order_id, _ in self.book.get_orders().items():
            trade: Optional[Trade] = self.cancel_order(order_id)
            if not trade:
                failed_cancels.append(order_id)
            if trade and trade.orderStatus.status != 'Cancelled':
                failed_cancels.append(order_id)

        if len(failed_cancels) > 0:
            return SuccessFail.fail(failed_cancels)
        else:
            return SuccessFail.success()

    def is_ib_connected(self) -> bool:
        return self.client.ib.isConnected()

    @log_method
    def red_button(self):
        self.client.ib.reqGlobalCancel()

    @log_method
    def status(self) -> Dict[str, bool]:
        # todo lots of work here
        status = {
            'ib_connected': self.client.ib.isConnected(),
            'arctic_connected': self.data is not None
        }
        return status

    @log_method
    def get_universes(self) -> List[Universe]:
        return self.universes

    @log_method
    def get_unique_client_id(self) -> int:
        new_client_id = max(self.tws_client_ids) + 1
        self.tws_client_ids.append(new_client_id)
        self.tws_client_ids.append(new_client_id + 1)
        return new_client_id

    @log_method
    def get_pnl(self) -> List[PnLSingle]:
        return self.pnl.get_all()

    @log_method
    def get_portfolio_summary(self) -> List[PortfolioSummary]:
        def find_pnl_or_nan(account: str, contract: Contract) -> float:
            if str((account, contract.conId)) in self.pnl.cache:
                return self.pnl.cache[str((account, contract.conId))].dailyPnL
            else:
                return float('nan')

        summary: List[PortfolioSummary] = []

        for portfolio_item in self.portfolio.get_portfolio_items():
            summary.append(PortfolioSummary(
                contract=portfolio_item.contract,
                position=portfolio_item.position,
                marketValue=portfolio_item.marketValue,
                averageCost=portfolio_item.averageCost,
                unrealizedPNL=portfolio_item.unrealizedPNL,
                realizedPNL=portfolio_item.realizedPNL,
                account=portfolio_item.account,
                marketPrice=portfolio_item.marketPrice,
                dailyPNL=find_pnl_or_nan(portfolio_item.account, portfolio_item.contract)
            ))
        return summary

    @log_method
    def get_positions(self) -> List[Position]:
        return self.portfolio.get_positions()

    @log_method
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
