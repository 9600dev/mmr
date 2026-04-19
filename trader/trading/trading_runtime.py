from ib_async.contract import Contract
from ib_async.objects import PnLSingle, PortfolioItem, Position
from ib_async.order import LimitOrder, MarketOrder, Order, StopLimitOrder, StopOrder, Trade
from ib_async.ticker import Ticker
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

from trader.data.data_access import PortfolioSummary, SecurityDefinition, TickStorage
from trader.data.event_store import EventStore, EventType, TradingEvent
from trader.data.market_data import SecurityDataStream
from trader.data.universe import Universe, UniverseAccessor
from trader.trading.risk_gate import RiskGate, RiskLimits
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


class Trader():
    def __init__(self,
                 ib_server_address: str,
                 ib_server_port: int,
                 trading_runtime_ib_client_id: int,
                 ib_account: str,
                 duckdb_path: str,
                 universe_library: str,
                 zmq_pubsub_server_address: str,
                 zmq_pubsub_server_port: int,
                 zmq_rpc_server_address: str,
                 zmq_rpc_server_port: int,
                 zmq_strategy_rpc_server_address: str,
                 zmq_strategy_rpc_server_port: int,
                 zmq_messagebus_server_address: str,
                 zmq_messagebus_server_port: int,
                 history_duckdb_path: str = '',
                 paper_trading: bool = False,
                 simulation: bool = False):
        self.ib_server_address = ib_server_address
        self.ib_server_port = ib_server_port
        self.trading_runtime_ib_client_id = trading_runtime_ib_client_id
        self.ib_account = ib_account
        self.duckdb_path = duckdb_path
        self.history_duckdb_path = history_duckdb_path or duckdb_path
        self.universe_library = universe_library
        self.simulation: bool = simulation
        self.paper_trading = paper_trading
        self.zmq_pubsub_server_address = zmq_pubsub_server_address
        self.zmq_pubsub_server_port = zmq_pubsub_server_port
        self.zmq_rpc_server_address = zmq_rpc_server_address
        self.zmq_rpc_server_port = zmq_rpc_server_port
        self.zmq_strategy_rpc_server_address = zmq_strategy_rpc_server_address
        self.zmq_strategy_rpc_server_port = zmq_strategy_rpc_server_port
        self.zmq_messagebus_server_address = zmq_messagebus_server_address
        self.zmq_messagebus_server_port = zmq_messagebus_server_port

        # todo you can have up to 24 connections to IB Gateway
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
        self._pnl_subscriptions_lock: threading.Lock = threading.Lock()
        # The main event loop, captured on connect(). Used when IB callbacks
        # fire on threads other than the loop thread.
        self._main_loop: Optional[asyncio.AbstractEventLoop] = None
        # takes care of execution of orders
        self.executioner: TradeExecutioner
        # a list of all the universes of stocks we have registered
        self.market_data = 3
        self.zmq_rpc_server: RPCServer[bus.TraderServiceApi]
        self.zmq_pubsub_server: MultithreadedTopicPubSub
        self.zmq_pubsub_contracts: Dict[int, Observable[IBAIORxError]] = {}
        self.zmq_pubsub_contract_filters: Dict[int, bool] = {}
        self.zmq_pubsub_contract_subscription: DisposableBase = Disposable()

        self.zmq_strategy_client: RPCClient[strategy_bus.StrategyServiceApi]
        self.zmq_messagebus: MessageBusServer

        self.startup_time: dt.datetime = dt.datetime.now()
        self.last_connect_time: dt.datetime
        self.load_test: bool = False
        self.tws_client_ids: List[int] = [self.trading_runtime_ib_client_id, self.trading_runtime_ib_client_id + 1]
        self.scheduler: Optional[AsyncIOThreadSafeScheduler] = None

        # IB upstream connectivity tracking
        # These error codes indicate Gateway is connected locally but lost upstream IBKR connection
        self._ib_upstream_connected: bool = True
        self._ib_upstream_error: str = ''

        self.disposables: List[DisposableBase] = []

    @backoff.on_exception(backoff.expo, (ConnectionRefusedError, TimeoutError), max_tries=10, max_time=120)
    def connect(self):
        logging.debug('trading_runtime.connect() connecting to services: %s:%s' % (self.ib_server_address, self.ib_server_port))
        try:
            self.client = IBAIORx(
                ib_server_address=self.ib_server_address,
                ib_server_port=self.ib_server_port,
                ib_client_id=self.trading_runtime_ib_client_id,
                ib_account=self.ib_account,
            )
            self.data = TickStorage(self.history_duckdb_path)
            self.universe_accessor = UniverseAccessor(self.duckdb_path, self.universe_library)
            self.clear_portfolio_universe()
            self.contract_subscriptions = {}
            self.market_data_subscriptions = {}
            self.client.ib.connectedEvent += self.connected_event
            self.client.ib.disconnectedEvent += self.disconnected_event
            self.client.connect()

            # Track IB upstream connectivity via error codes
            self.client.error_subject.subscribe(AnonymousObserver(
                on_next=self._on_ib_error,
                on_error=lambda e: None,
            ))

            # Safety check: verify IB account matches expected trading mode.
            # Paper accounts start with "D" (e.g. DU..., DF...), live accounts don't.
            managed = self.client.ib.managedAccounts()
            if managed:
                active_account = self.ib_account if self.ib_account else managed[0]
                is_paper_account = active_account.startswith('D')
                if self.paper_trading and not is_paper_account:
                    self.client.ib.disconnect()
                    raise TraderConnectionException(
                        f'SAFETY: trading_mode is "paper" but connected to live account "{active_account}". '
                        f'Managed accounts: {managed}. Refusing to continue.'
                    )
                if not self.paper_trading and is_paper_account:
                    self.client.ib.disconnect()
                    raise TraderConnectionException(
                        f'SAFETY: trading_mode is "live" but connected to paper account "{active_account}". '
                        f'Managed accounts: {managed}. Check your config.'
                    )
                logging.info('trading mode verified: %s, account: %s', 'paper' if self.paper_trading else 'live', active_account)

            self.last_connect_time = dt.datetime.now()
            self.zmq_rpc_server = RPCServer[bus.TraderServiceApi](
                instance=bus.TraderServiceApi(self),
                zmq_rpc_server_address=self.zmq_rpc_server_address,
                zmq_rpc_server_port=self.zmq_rpc_server_port
            )
            self.zmq_pubsub_server = MultithreadedTopicPubSub(
                zmq_pubsub_server_address=self.zmq_pubsub_server_address,
                zmq_pubsub_server_port=self.zmq_pubsub_server_port
            )
            self.zmq_pubsub_server.start()

            self.zmq_messagebus = MessageBusServer(self.zmq_messagebus_server_address, self.zmq_messagebus_server_port)
            self.run(self.zmq_messagebus.start())

            self.zmq_pubsub_contracts = {}
            self.zmq_pubsub_contract_filters = {}
            self.zmq_pubsub_contract_subscription = Disposable()

            # connect to the strategy server
            self.zmq_strategy_client = RPCClient[strategy_bus.StrategyServiceApi](
                self.zmq_strategy_rpc_server_address,
                self.zmq_strategy_rpc_server_port,
                timeout=6,
            )

            # initialize event store and risk gate
            self.event_store = EventStore(self.duckdb_path)
            self.risk_gate = RiskGate(RiskLimits(), self.event_store)

            # load trading filters (allowlist/denylist)
            from trader.trading.trading_filter import TradingFilter
            self.risk_gate.trading_filter = TradingFilter.load()

            # fire up the executioner
            self.executioner = TradeExecutioner()
            self.executioner.connect(self)

            self.run(self.zmq_strategy_client.connect())
            self.run(self.zmq_rpc_server.serve())

        except KeyboardInterrupt:
            logging.info('connect() interrupted, shutting down')
            raise
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
        # Schedule the async universe update onto the trader's main loop even
        # when this callback fires on an IB/eventkit thread. The old code fell
        # back to a *synchronous* disk-IO path in that case, which blocked
        # every other IB event on the callback thread.
        coro = self.update_portfolio_universe(portfolio_item)
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(coro)
            return
        except RuntimeError:
            pass

        main_loop = self._main_loop
        if main_loop is not None and main_loop.is_running():
            asyncio.run_coroutine_threadsafe(coro, main_loop)
        else:
            # Truly no loop available (shutdown path, tests). Fall back to the
            # sync version but close the coroutine to avoid "never awaited".
            coro.close()
            self._update_portfolio_universe_sync(portfolio_item)

    def __dataclass_server_put(self, message: DataClassEvent):
        # logging.debug('__dataclass_server_put: {}'.format(message))
        self.zmq_pubsub_server.put(('dataclass', message))

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
            async def __async_subscribe_pnl(portfolio_item: PortfolioItem):
                if not portfolio_item.contract:
                    return
                key = (portfolio_item.account, portfolio_item.contract.conId)
                # Atomic "first claim wins" — prevents two concurrent portfolio
                # events from both crossing the earlier check-then-act gap and
                # leaking duplicate PnL subscriptions on reconnect.
                with self._pnl_subscriptions_lock:
                    if key in self.pnl_subscriptions:
                        return
                    self.pnl_subscriptions[key] = True

                try:
                    observable = await self.client.subscribe_single_pnl(
                        portfolio_item.contract,
                    )
                    disposable = observable.subscribe(
                        self.pnl.create_observer(error_func=handle_subscription_exception),
                    )
                    self.disposables.append(disposable)
                except Exception as ex:
                    # Back out the registry entry so a retry can re-attempt.
                    with self._pnl_subscriptions_lock:
                        self.pnl_subscriptions.pop(key, None)
                    logging.warning(f'Failed to subscribe PnL for {portfolio_item.contract}: {ex}')

            try:
                loop = asyncio.get_running_loop()
                loop.create_task(__async_subscribe_pnl(portfolio_item))
                return
            except RuntimeError:
                pass

            # Off-loop-thread callback: hand off to the main loop if captured,
            # otherwise fall back to the legacy sync-run path.
            main_loop = self._main_loop
            if main_loop is not None and main_loop.is_running():
                asyncio.run_coroutine_threadsafe(__async_subscribe_pnl(portfolio_item), main_loop)
            else:
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
        if self.scheduler is None:
            self.scheduler = AsyncIOThreadSafeScheduler(asyncio.get_running_loop())
        scheduled_disposable = self.scheduler.schedule_periodic(10, lambda x: self.pnl.post_all())
        self.disposables.append(scheduled_disposable)

    def _on_ib_error(self, error: IBAIORxError):
        """Track IB upstream connectivity from error codes."""
        # 1100: connectivity lost between IB and TWS/Gateway
        # 2103: market data farm connection broken
        # 2105: HMDS data farm connection broken
        # 2157: sec-def data farm connection broken
        if error.errorCode in (1100, 2103, 2105, 2157):
            self._ib_upstream_connected = False
            self._ib_upstream_error = error.errorString
            logging.warning('IB upstream connection lost: %s', error.errorString)

        # 1102: connectivity restored (with possible data loss)
        # 2104: market data farm connection OK
        # 2106: HMDS data farm connection OK
        # 2158: sec-def data farm connection OK
        elif error.errorCode in (1102, 2104, 2106, 2158):
            self._ib_upstream_connected = True
            self._ib_upstream_error = ''
            logging.info('IB upstream connection restored: %s', error.errorString)

    @log_method
    async def connected_event(self):
        # Capture the main event loop now that we're running inside it. Used
        # to schedule async work from IB callback threads without spinning up
        # a throwaway loop.
        try:
            self._main_loop = asyncio.get_running_loop()
        except RuntimeError:
            pass

        # Dispose old subscriptions before re-subscribing (happens on reconnect)
        for disposable in self.disposables:
            try:
                disposable.dispose()
            except Exception:
                pass
        self.disposables.clear()
        with self._pnl_subscriptions_lock:
            self.pnl_subscriptions.clear()

        await self.setup_subscriptions()

    @log_method
    async def disconnected_event(self):
        # Guard against multiple concurrent reconnection attempts
        if hasattr(self, '_reconnecting') and self._reconnecting:
            logging.debug('reconnection already in progress, skipping')
            return

        self._reconnecting = True
        try:
            attempt = 0
            while True:
                attempt += 1
                delay = min(2 ** min(attempt, 7), 120)  # exponential backoff, cap at 2 minutes

                logging.warning(
                    'IB Gateway disconnected — reconnection attempt %d in %ds',
                    attempt, delay
                )

                t_before = asyncio.get_event_loop().time()
                await asyncio.sleep(delay)
                t_after = asyncio.get_event_loop().time()

                # Detect system sleep: if the actual elapsed time is much longer
                # than the requested delay, the system likely slept. Reset backoff
                # so we get a fresh set of fast retries after wake.
                elapsed = t_after - t_before
                if elapsed > delay * 3 and delay > 4:
                    logging.info(
                        'detected system sleep (requested %ds, elapsed %.0fs) — resetting backoff',
                        delay, elapsed,
                    )
                    attempt = 1

                try:
                    await self.client.connect_async()
                    # Re-attach event handlers to the fresh IB instance
                    if self.connected_event not in self.client.ib.connectedEvent:
                        self.client.ib.connectedEvent += self.connected_event
                    if self.disconnected_event not in self.client.ib.disconnectedEvent:
                        self.client.ib.disconnectedEvent += self.disconnected_event
                    logging.info('reconnected to IB Gateway on attempt %d', attempt)
                    await self.connected_event()
                    return
                except Exception as ex:
                    logging.error('reconnection attempt %d failed: %s', attempt, ex)
        finally:
            self._reconnecting = False

    @log_method
    async def enable_strategy(self, name: str, paper: bool) -> SuccessFail[StrategyState]:
        try:
            return self.zmq_strategy_client.rpc().enable_strategy(name, paper)
        except Exception as ex:
            logging.error('enable_strategy: {}'.format(ex))
            return SuccessFail.fail(exception=ex)

    @log_method
    async def disable_strategy(self, name: str) -> SuccessFail[StrategyState]:
        try:
            return self.zmq_strategy_client.rpc().disable_strategy(name)
        except Exception as ex:
            logging.error('disable_strategy: {}'.format(ex))
            return SuccessFail.fail(exception=ex)

    @log_method
    async def get_strategies(self) -> SuccessFail[List[StrategyConfig]]:
        try:
            rpc_call = self.zmq_strategy_client.rpc().get_strategies()
            # rpc_call = SuccessFail.success(await (await self.zmq_strategy_client.awaitable_rpc()).get_strategies())
            return SuccessFail.success(rpc_call)
        except Exception as ex:
            return SuccessFail.fail(exception=ex)

    @log_method
    async def reload_strategies(self) -> SuccessFail[List[StrategyConfig]]:
        try:
            return self.zmq_strategy_client.rpc().reload_strategies()
        except Exception as ex:
            logging.error('reload_strategies: {}'.format(ex))
            return SuccessFail.fail(exception=ex)

    @log_method
    def clear_portfolio_universe(self):
        universe = self.universe_accessor.get('portfolio')
        universe.security_definitions.clear()
        self.universe_accessor.update(universe)

    @log_method
    async def resolve_contract(self, contract: Contract) -> List[SecurityDefinition]:
        """Resolve a partial Contract (e.g. with strike/expiry/right) to full SecurityDefinitions via IB."""
        contract_details = await self.client.ib.reqContractDetailsAsync(contract)
        if contract_details:
            return [SecurityDefinition.from_contract_details(cd) for cd in contract_details]
        return []

    @log_method
    async def resolve_symbol(
        self,
        symbol: Union[str, int],
        exchange: str = '',
        universe: str = '',
        sec_type: str = '',
    ) -> List[SecurityDefinition]:
        def __blocking_resolve_symbol_to_security_definitions(
            symbol: Union[str, int],
            exchange: str = '',
            universe: str = '',
            sec_type: str = '',
            first_only: bool = False,
        ) -> list[SecurityDefinition]:
            return self.universe_accessor.resolve_symbol(
                symbol=symbol,
                exchange=exchange,
                universe=universe,
                first_only=first_only
            )

        # if we're asking about conid's, we only want the first one
        first_only = False
        if type(symbol) is int:
            first_only = True

        # this could take a while
        result = await asyncio.to_thread(
            __blocking_resolve_symbol_to_security_definitions,
            symbol,
            exchange,
            universe,
            sec_type,
            first_only,
        )

        if len(result) > 0:
            return result

        # No IB fallback. resolve_symbol is a local DB lookup only.
        # Guessing with a partially-specified Contract can resolve to the
        # wrong instrument (e.g. SOXL → AEQLIT/CAD, 4391 → TSEJ).
        # Use resolve_contract() for explicit IB discovery with a
        # fully-specified Contract.
        if type(symbol) is int:
            logging.warning('conId %d not found in local universe DB', symbol)
        else:
            logging.warning("Symbol '%s' not found in local universe DB — "
                            "add it with `universe add` or use `resolve` CLI command", symbol)
        return []

    @log_method
    async def resolve_universe(
        self,
        symbol: Union[str, int],
        exchange: str = '',
        universe: str = '',
        sec_type: str = '',
    ) -> List[Tuple[str, SecurityDefinition]]:
        def __blocking_resolve_universe(
            symbol: Union[str, int],
            exchange: str = '',
            universe: str = '',
            sec_type: str = '',
        ) -> list[Tuple[str, SecurityDefinition]]:
            return self.universe_accessor.resolve_universe_name(
                symbol=symbol,
                exchange=exchange,
                universe=universe,
                sec_type=sec_type
            )

        # this could take a while
        return await asyncio.to_thread(__blocking_resolve_universe, symbol, exchange, universe, sec_type)

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

    async def update_portfolio_universe(self, portfolio_item: PortfolioItem):
        """
        Grabs the current portfolio from IB Gateway and adds a new version to the 'portfolio' table.
        """
        universe = self.universe_accessor.get('portfolio')
        if not ListHelper.isin(
            universe.security_definitions,
            lambda definition: definition.conId == portfolio_item.contract.conId
        ):
            contract = portfolio_item.contract
            try:
                contract_details = await self.client.get_contract_details_async(contract)
            except Exception as ex:
                logging.warning(f'Failed to get contract details for {contract}: {ex}')
                return
            if contract_details and len(contract_details) >= 1:
                universe.security_definitions.append(
                    SecurityDefinition.from_contract_details(contract_details[0])
                )

            logging.debug('updating portfolio universe with {}'.format(portfolio_item))
            self.universe_accessor.update(universe)

    def _update_portfolio_universe_sync(self, portfolio_item: PortfolioItem):
        """Sync fallback when no event loop is running."""
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
    async def check_order_margin(self, contract: Contract, order: Order) -> dict:
        """Run whatIfOrder to get margin impact without placing."""
        order_state = await self.client.ib.whatIfOrderAsync(contract, order)
        numeric = order_state.numeric(2)
        return {
            'initMarginBefore': numeric.initMarginBefore,
            'maintMarginBefore': numeric.maintMarginBefore,
            'equityWithLoanBefore': numeric.equityWithLoanBefore,
            'initMarginChange': numeric.initMarginChange,
            'maintMarginChange': numeric.maintMarginChange,
            'equityWithLoanChange': numeric.equityWithLoanChange,
            'initMarginAfter': numeric.initMarginAfter,
            'maintMarginAfter': numeric.maintMarginAfter,
            'equityWithLoanAfter': numeric.equityWithLoanAfter,
            'commission': numeric.commission,
            'warningText': order_state.warningText,
        }

    @log_method
    async def place_expressive_order(
        self,
        contract: Contract,
        action: str,
        quantity: float,
        execution_spec: dict,
        algo_name: str = 'proposal',
    ) -> SuccessFail:
        """Place an order with full execution specification (brackets, trailing stops, etc.)."""
        from trader.trading.proposal import ExecutionSpec
        spec = ExecutionSpec.from_dict(execution_spec)

        # Validate execution spec before placing any orders
        validation_errors = spec.validate()
        if validation_errors:
            return SuccessFail.fail(error=f'Invalid execution spec: {"; ".join(validation_errors)}')

        trades: List[Trade] = []

        reverse_action = 'SELL' if action == 'BUY' else 'BUY'

        common = dict(
            action=action,
            totalQuantity=quantity,
            account=self.ib_account,
            orderRef=algo_name,
            tif=spec.tif,
            outsideRth=spec.outside_rth,
        )
        if spec.tif == 'GTD' and spec.good_till_date:
            common['goodTillDate'] = spec.good_till_date

        def _build_entry(**common) -> Order:
            if spec.order_type == 'MARKET':
                return MarketOrder(**common)
            else:
                return LimitOrder(lmtPrice=spec.limit_price, **common)

        # --- Pre-trade risk checks ---

        # 0. Trading filter check (denylist/allowlist)
        if getattr(self, 'risk_gate', None) is not None:
            instrument_result = self.risk_gate.check_instrument(
                symbol=contract.symbol, exchange=contract.exchange or '', sec_type=contract.secType or '',
            )
            if not instrument_result.approved:
                return SuccessFail.fail(error=instrument_result.reason)

        # Build a temporary entry order for margin simulation
        probe_order = _build_entry(**common)

        # 1. whatIfOrder margin check
        margin_impact = None
        try:
            margin_impact = await self.check_order_margin(contract, probe_order)
        except Exception as ex:
            logging.warning(f'whatIfOrder failed, proceeding without margin check: {ex}')

        # 2. Leverage limit check
        if margin_impact and getattr(self, 'risk_gate', None) is not None:
            net_liq = 0.0
            for v in self.client.ib.accountValues():
                if v.tag == 'NetLiquidation' and v.currency != 'BASE':
                    net_liq = float(v.value)
                    break

            leverage_result = self.risk_gate.check_leverage(margin_impact, net_liq)
            if not leverage_result.approved:
                return SuccessFail.fail(error=leverage_result.reason)

        # 3. Risk gate checks (open orders, daily loss)
        if getattr(self, 'risk_gate', None) is not None:
            from trader.trading.strategy import Signal
            signal = Signal(
                source_name='proposal',
                action=Action.BUY if action == 'BUY' else Action.SELL,
                probability=1.0,
                risk=0.0,
            )
            gate_result = self.risk_gate.evaluate(
                signal=signal,
                open_order_count=len(self.book.get_orders()) if hasattr(self, 'book') else 0,
            )
            if not gate_result.approved:
                return SuccessFail.fail(error=f'Risk gate: {gate_result.reason}')

        async def _place_and_wait(c: Contract, o: Order) -> Optional[Trade]:
            """Place a single child order and await the IB ack. Returns the
            Trade object or None on failure (observer emitted on_error)."""
            event = asyncio.Event()
            result: Dict[str, Optional[Trade]] = {'trade': None}

            def _on_next(trade: Trade):
                result['trade'] = trade
                event.set()

            obs = await self.executioner.subscribe_place_order_direct(c, o)
            obs.subscribe(Observer(
                on_next=_on_next,
                on_error=lambda e: event.set(),
                on_completed=lambda: None,
            ))
            await event.wait()
            return result['trade']

        def _cancel_trade_safely(trade: Optional[Trade]) -> None:
            """Best-effort cancel of a staged (transmit=False) child order."""
            if trade is None or not getattr(trade, 'order', None):
                return
            try:
                self.client.ib.cancelOrder(trade.order)
            except Exception as ex:
                logging.warning(
                    'failed to cancel partial bracket leg %s: %s',
                    getattr(trade.order, 'orderId', '?'), ex,
                )

        try:
            if spec.exit_type == 'BRACKET':
                entry = _build_entry(**common)
                entry.transmit = False

                entry_trade = await _place_and_wait(contract, entry)
                if entry_trade is None:
                    return SuccessFail.fail(error='Failed to place entry order')

                trades.append(entry_trade)
                parent_id = entry_trade.order.orderId

                # Take-profit
                tp = LimitOrder(
                    action=reverse_action,
                    totalQuantity=quantity,
                    lmtPrice=spec.take_profit_price,
                    parentId=parent_id,
                    transmit=False,
                    account=self.ib_account,
                    tif=spec.tif,
                    outsideRth=spec.outside_rth,
                )
                tp_trade = await _place_and_wait(contract, tp)
                if tp_trade is None:
                    # Roll back the staged entry — it was transmit=False so no
                    # market-side exposure yet; cancelling keeps the book
                    # consistent with the caller's understanding that the
                    # bracket failed atomically.
                    _cancel_trade_safely(entry_trade)
                    return SuccessFail.fail(
                        error='Bracket aborted: take-profit order rejected; entry rolled back'
                    )
                trades.append(tp_trade)

                # Stop-loss (transmit=True triggers the whole bracket)
                sl = StopOrder(
                    action=reverse_action,
                    totalQuantity=quantity,
                    stopPrice=spec.stop_loss_price,
                    parentId=parent_id,
                    transmit=True,
                    account=self.ib_account,
                    tif=spec.tif,
                    outsideRth=spec.outside_rth,
                )
                sl_trade = await _place_and_wait(contract, sl)
                if sl_trade is None:
                    # Same as above: cancel TP + entry before the bracket is
                    # ever transmitted to the market.
                    _cancel_trade_safely(tp_trade)
                    _cancel_trade_safely(entry_trade)
                    return SuccessFail.fail(
                        error='Bracket aborted: stop-loss order rejected; entry + TP rolled back'
                    )
                trades.append(sl_trade)

            elif spec.exit_type == 'TRAILING_STOP':
                entry = _build_entry(**common)
                entry.transmit = False

                task = asyncio.Event()
                entry_trade = None

                def on_entry_ts(trade: Trade):
                    nonlocal entry_trade
                    entry_trade = trade
                    task.set()

                observable = await self.executioner.subscribe_place_order_direct(contract, entry)
                observable.subscribe(Observer(on_next=on_entry_ts, on_error=lambda e: task.set(), on_completed=lambda: None))
                await task.wait()

                if entry_trade is None:
                    return SuccessFail.fail(error='Failed to place entry order')

                trades.append(entry_trade)
                parent_id = entry_trade.order.orderId

                trail = Order(
                    orderType='TRAIL',
                    action=reverse_action,
                    totalQuantity=quantity,
                    parentId=parent_id,
                    transmit=True,
                    account=self.ib_account,
                    tif=spec.tif,
                    outsideRth=spec.outside_rth,
                )
                if spec.trailing_stop_percent:
                    trail.trailingPercent = spec.trailing_stop_percent
                elif spec.trailing_stop_amount:
                    trail.auxPrice = spec.trailing_stop_amount

                trail_task = asyncio.Event()
                trail_trade: Optional[Trade] = None

                def on_trail(trade: Trade):
                    nonlocal trail_trade
                    trail_trade = trade
                    trail_task.set()

                trail_obs = await self.executioner.subscribe_place_order_direct(contract, trail)
                trail_obs.subscribe(Observer(on_next=on_trail, on_error=lambda e: trail_task.set(), on_completed=lambda: None))
                await trail_task.wait()
                if trail_trade:
                    trades.append(trail_trade)

            elif spec.exit_type == 'STOP_LOSS':
                entry = _build_entry(**common)
                entry.transmit = False

                task = asyncio.Event()
                entry_trade = None

                def on_entry_sl(trade: Trade):
                    nonlocal entry_trade
                    entry_trade = trade
                    task.set()

                observable = await self.executioner.subscribe_place_order_direct(contract, entry)
                observable.subscribe(Observer(on_next=on_entry_sl, on_error=lambda e: task.set(), on_completed=lambda: None))
                await task.wait()

                if entry_trade is None:
                    return SuccessFail.fail(error='Failed to place entry order')

                trades.append(entry_trade)
                parent_id = entry_trade.order.orderId

                sl = StopOrder(
                    action=reverse_action,
                    totalQuantity=quantity,
                    stopPrice=spec.stop_loss_price,
                    parentId=parent_id,
                    transmit=True,
                    account=self.ib_account,
                    tif=spec.tif,
                    outsideRth=spec.outside_rth,
                )

                sl_task = asyncio.Event()
                sl_trade: Optional[Trade] = None

                def on_sl_only(trade: Trade):
                    nonlocal sl_trade
                    sl_trade = trade
                    sl_task.set()

                sl_obs = await self.executioner.subscribe_place_order_direct(contract, sl)
                sl_obs.subscribe(Observer(on_next=on_sl_only, on_error=lambda e: sl_task.set(), on_completed=lambda: None))
                await sl_task.wait()
                if sl_trade:
                    trades.append(sl_trade)

            else:
                # NONE — simple entry only
                entry = _build_entry(**common)
                entry.transmit = True

                task = asyncio.Event()
                entry_trade = None

                def on_entry_simple(trade: Trade):
                    nonlocal entry_trade
                    entry_trade = trade
                    task.set()

                observable = await self.executioner.subscribe_place_order_direct(contract, entry)
                observable.subscribe(Observer(on_next=on_entry_simple, on_error=lambda e: task.set(), on_completed=lambda: None))
                await task.wait()
                if entry_trade:
                    trades.append(entry_trade)

            return SuccessFail.success(obj=trades)

        except Exception as ex:
            logging.error(f'place_expressive_order error: {ex}')
            return SuccessFail.fail(exception=ex)

    async def place_standalone_order(
        self,
        contract: Contract,
        action: str,
        quantity: float,
        order_type: str,
        aux_price: float = 0,
        limit_price: float = 0,
        trailing_percent: float = 0,
        tif: str = 'GTC',
        outside_rth: bool = True,
    ) -> SuccessFail:
        """Place a standalone order (e.g. protective stop for an existing position).

        order_type: 'STP' (stop), 'TRAIL' (trailing stop), 'LMT' (take-profit limit)
        """
        try:
            if order_type == 'STP':
                order = StopOrder(
                    action=action,
                    totalQuantity=quantity,
                    stopPrice=aux_price,
                    account=self.ib_account,
                    tif=tif,
                    outsideRth=outside_rth,
                    transmit=True,
                )
            elif order_type == 'TRAIL':
                order = Order(
                    orderType='TRAIL',
                    action=action,
                    totalQuantity=quantity,
                    account=self.ib_account,
                    tif=tif,
                    outsideRth=outside_rth,
                    transmit=True,
                )
                if trailing_percent:
                    order.trailingPercent = trailing_percent
                elif aux_price:
                    order.auxPrice = aux_price
            elif order_type == 'LMT':
                order = LimitOrder(
                    action=action,
                    totalQuantity=quantity,
                    lmtPrice=limit_price,
                    account=self.ib_account,
                    tif=tif,
                    outsideRth=outside_rth,
                    transmit=True,
                )
            else:
                return SuccessFail.fail(error=f'Unsupported order_type: {order_type}')

            task = asyncio.Event()
            result_trade: Optional[Trade] = None

            def on_next(trade: Trade):
                nonlocal result_trade
                result_trade = trade
                task.set()

            observable = await self.executioner.subscribe_place_order_direct(contract, order)
            observable.subscribe(Observer(on_next=on_next, on_error=lambda e: task.set(), on_completed=lambda: None))
            await task.wait()

            if result_trade:
                return SuccessFail.success(obj=result_trade)
            else:
                return SuccessFail.fail(error='Failed to place standalone order')

        except Exception as ex:
            logging.error(f'place_standalone_order error: {ex}')
            return SuccessFail.fail(exception=ex)

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
        skip_risk_gate: bool = False,
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
        return await self.executioner.place_order(
            contract_order=contract_order,
            condition=ExecutorCondition.SANITY_CHECK,
            skip_risk_gate=skip_risk_gate,
        )

    @log_method
    def cancel_order(self, order_id: int) -> Optional[Trade]:
        return self.executioner.cancel_order_id(order_id)

    @log_method
    def cancel_all(self) -> SuccessFail[List[int]]:
        cancelled = []
        failed_cancels = []
        for order_id, _ in self.book.get_orders().items():
            trade: Optional[Trade] = self.cancel_order(order_id)
            if trade:
                cancelled.append(order_id)
            else:
                failed_cancels.append(order_id)

        if failed_cancels:
            return SuccessFail.fail(error=f'Failed to cancel: {failed_cancels}')
        else:
            return SuccessFail.success(obj=cancelled)

    async def scanner_data(self, **kwargs) -> list[dict]:
        return await self.client.scanner_data(**kwargs)

    async def get_snapshots_batch(self, contracts, delayed: bool = False) -> list[dict]:
        return await self.client.get_snapshots_batch(contracts, delayed)

    async def get_history_bars(self, contract, duration: str = '60 D', bar_size: str = '1 day') -> list[dict]:
        return await self.client.get_history_bars(contract, duration, bar_size)

    async def get_fundamental_data(self, contract, report_type: str = 'ReportSnapshot') -> str:
        return await self.client.get_fundamental_data(contract, report_type)

    async def get_market_depth(self, contract, num_rows: int = 5, is_smart_depth: bool = False) -> dict:
        return await self.client.get_market_depth(contract, num_rows=num_rows, is_smart_depth=is_smart_depth)

    async def get_news_headlines(self, conId: int, provider_codes: str = '',
                                  total_results: int = 5) -> list[dict]:
        return await self.client.get_news_headlines(conId, provider_codes, total_results)

    def is_ib_connected(self) -> bool:
        return self.client.ib.isConnected()

    @log_method
    def red_button(self):
        self.client.ib.reqGlobalCancel()

    @log_method
    def status(self) -> dict:
        status = {
            'ib_connected': self.client.ib.isConnected(),
            'ib_upstream_connected': self._ib_upstream_connected,
            'storage_connected': self.data is not None,
        }
        if not self._ib_upstream_connected:
            status['ib_upstream_error'] = self._ib_upstream_error
        return status

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
    async def get_shortable_shares(self, contract: Contract) -> float:
        return await self.client.get_shortable_shares(contract)

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
