from ib_async import Contract
from ib_async.ib import IB
from ib_async.ticker import Ticker
from reactivex.observer import AutoDetachObserver
from trader.common.exceptions import TraderConnectionException, TraderException
from trader.common.logging_helper import get_callstack, log_method, setup_logging
from trader.data.market_data import normalize_ticker

from trader.data.data_access import SecurityDefinition, TickStorage
from trader.data.universe import UniverseAccessor
from trader.listeners.ib_history_worker import IBHistoryWorker, IBConnectivityError, IBNoDataError
from trader.messaging.clientserver import (
    MessageBusClient,
    MultithreadedTopicPubSub,
    RPCClient,
    RPCServer,
    TopicPubSub
)
from trader.objects import Action, BarSize, WhatToShow
from trader.data.event_store import EventStore, EventType, TradingEvent
from trader.trading.strategy import Signal, Strategy, StrategyConfig, StrategyContext, StrategyState
from typing import cast, Dict, List, Optional

import asyncio
import backoff
import datetime as dt
import importlib
import importlib.util
import inspect
import os
import pandas as pd
import sys
import trader.messaging.strategy_service_api as bus
import yaml


logging = setup_logging(module_name='strategy_runtime')


error_table = {
    'trader.common.exceptions.TraderException': TraderException,
    'trader.common.exceptions.TraderConnectionException': TraderConnectionException
}


class StrategyRuntime():
    def __init__(
        self,
        ib_server_address: str,
        ib_server_port: int,
        strategy_runtime_ib_client_id: int,
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
        strategies_directory: str,
        strategy_config_file: str,
        paper_trading: bool = False,
        simulation: bool = False
    ):
        self.ib_server_address = ib_server_address
        self.ib_server_port = ib_server_port
        self.strategy_runtime_ib_client_id: int = strategy_runtime_ib_client_id
        self.duckdb_path = duckdb_path
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

        self.strategies_directory = strategies_directory
        self.strategy_config_file = strategy_config_file
        self.startup_time: dt.datetime = dt.datetime.now()
        self.last_connect_time: dt.datetime

        self.zmq_strategy_rpc_server: RPCServer[bus.StrategyServiceApi]
        self.zmq_messagebus_client: MessageBusClient

        # todo: this is wrong as we'll have a whole bunch of different tickdata libraries for
        # different bartypes etc.
        self.storage: TickStorage

        self.universe_accessor: UniverseAccessor

        self.strategies: Dict[int, List[Strategy]] = {}
        self.strategy_implementations: List[Strategy] = []
        self.streams: Dict[int, pd.DataFrame] = {}

        self.historical_data_client: IBHistoryWorker

    def create_strategy_exception(self, exception_type: type, message: str, inner: Optional[Exception]):
        # todo use reflection here to automatically populate trader runtime vars that we care about
        # given a particular exception type
        data = self.storage if hasattr(self, 'data') else None
        last_connect_time = self.last_connect_time if hasattr(self, 'last_connect_time') else dt.datetime.min

        exception = exception_type(
            message,
            data is not None,
            False,
            self.startup_time,
            last_connect_time,
            inner,
            get_callstack(10)
        )
        logging.exception(exception)
        return exception

    @backoff.on_exception(backoff.expo, ConnectionRefusedError, max_tries=10, max_time=120)
    def connect(self):
        # avoids circular import
        from trader.messaging.trader_service_api import TraderServiceApi
        try:
            self.storage = TickStorage(self.duckdb_path)
            self.universe_accessor = UniverseAccessor(self.duckdb_path, self.universe_library)
            self.event_store = EventStore(self.duckdb_path)
            self.trader_client = RPCClient[TraderServiceApi](
                zmq_server_address=self.zmq_rpc_server_address,
                zmq_server_port=self.zmq_rpc_server_port,
                error_table=error_table
            )
            self.last_connect_time = dt.datetime.now()

            self.zmq_strategy_rpc_server = RPCServer[bus.StrategyServiceApi](
                instance=bus.StrategyServiceApi(self),
                zmq_rpc_server_address=self.zmq_strategy_rpc_server_address,
                zmq_rpc_server_port=self.zmq_strategy_rpc_server_port,
            )

            self.zmq_messagebus_client = MessageBusClient(
                zmq_address=self.zmq_messagebus_server_address,
                zmq_port=self.zmq_messagebus_server_port,
            )

            asyncio.run(self.zmq_messagebus_client.connect())
            asyncio.run(self.zmq_strategy_rpc_server.serve())

        except Exception as ex:
            raise self.create_strategy_exception(
                TraderConnectionException,
                message='strategy_runtime.connect() exception', inner=ex
            )

    @log_method
    def enable_strategy(self, name: str, paper: bool) -> StrategyState:
        for implementation in self.strategy_implementations:
            if name == implementation.name:
                implementation.paper = paper
                return implementation.enable()
        return StrategyState.ERROR

    @log_method
    def disable_strategy(self, name: str) -> StrategyState:
        for implementation in self.strategy_implementations:
            if name == implementation.name:
                return implementation.disable()
        return StrategyState.ERROR

    @log_method
    def get_strategy(self, name: str) -> Optional[Strategy]:
        for strategy in self.strategy_implementations:
            if strategy.name == name:
                return strategy
        return None

    def __get_enabled_strategies(self, conid: int) -> List[Strategy]:
        if conid in self.strategies:
            return [strategy for strategy in self.strategies[conid]
                    if strategy.state == StrategyState.RUNNING or strategy.state == StrategyState.WAITING_HISTORICAL_DATA]
        return []

    @log_method
    def get_strategies(self) -> List[Strategy]:
        return self.strategy_implementations

    def on_ticker_next(self, ticker: Ticker):
        if ticker.contract:
            logging.debug('StrategyRuntime.on_ticker_next({} {})'.format(ticker.contract.symbol, ticker.contract.conId))
        else:
            logging.debug('StrategyRuntime.on_ticker_next()')

        conId = 0

        if not ticker.contract:
            logging.debug('no contract associated with Ticker')
            return
        else:
            conId = ticker.contract.conId

        # populate the dataframe subscription cache
        normalized = normalize_ticker(ticker)
        if conId not in self.streams:
            self.streams[conId] = normalized
        else:
            self.streams[conId] = pd.concat([self.streams[conId], normalized], axis=0, copy=False)

        # execute the strategies attached to the conId's
        for strategy in self.__get_enabled_strategies(conId):
            signal = strategy.on_prices(self.streams[conId])
            if signal:
                if signal.action == Action.BUY:
                    logging.info('BUY signal from %s', strategy.name)
                elif signal.action == Action.SELL:
                    logging.info('SELL signal from %s', strategy.name)

                # Persist signal to event store
                event = TradingEvent(
                    event_type=EventType.SIGNAL,
                    timestamp=dt.datetime.now(),
                    strategy_name=signal.source_name,
                    conid=conId,
                    action=str(signal.action),
                    signal_probability=signal.probability,
                    signal_risk=signal.risk,
                )
                self.event_store.append(event)

                # Publish signal via MessageBus for cross-strategy use and subscribers
                self.zmq_messagebus_client.write('signal', signal)

    def on_ticker_error(self, ex: Exception):
        logging.debug('StrategyRuntime.on_error')

    def on_ticker_completed(self):
        logging.debug('StrategyRuntime.on_completed')

    def subscribe(self, strategy: Strategy, contract: Contract) -> None:
        logging.debug('strategy_runtime.subscribe() contract: {} strategy: {}'.format(contract, strategy))
        if contract.conId not in self.strategies:
            self.strategies[contract.conId] = []
            self.strategies[contract.conId].append(strategy)
            self.trader_client.rpc().publish_contract(contract=contract, delayed=False)
        elif contract.conId in self.strategies and strategy not in self.strategies[contract.conId]:
            self.strategies[contract.conId].append(strategy)

    def subscribe_universe(self, strategy: Strategy, universe_name: str) -> None:
        logging.debug('strategy_runtime.subscribe_universe() universe: {} strategy: {}'.format(universe_name, strategy))
        universe = self.universe_accessor.get(universe_name)

        for security in universe.security_definitions:
            self.subscribe(strategy, SecurityDefinition.to_contract(security))

    def load_strategy(
        self,
        name: str,
        bar_size_str: str,
        conids: Optional[List[int]],
        universe: Optional[str],
        historical_days_prior: int,
        module: str,
        class_name: str,
        description: str,
        paper: bool = False,
        auto_execute: bool = False,
    ) -> None:

        # Skip if strategy with this name already loaded
        if any(s.name == name for s in self.strategy_implementations):
            logging.debug('strategy {} already loaded, skipping'.format(name))
            return

        if not name or not class_name or not module or not bar_size_str:
            raise ValueError('invalid config. need name, bar_size, class_name and module specified')

        def load_class_from_file(filename, classname):
            # Get the absolute path of the file
            filepath = os.path.abspath(filename)

            # Create a module name based on the filename
            module_name = os.path.splitext(os.path.basename(filename))[0]

            # Load the module using importlib
            spec = importlib.util.spec_from_file_location(module_name, filepath)
            if spec:
                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module
                if spec.loader:
                    spec.loader.exec_module(module)
                else:
                    return None

                # Get the class object using getattr
                class_object = getattr(module, classname)
                return class_object
            else:
                return None

        try:
            class_object = load_class_from_file(module, class_name)
            if not class_object:
                return

            if inspect.isclass(class_object) and issubclass(class_object, Strategy) and class_object is not Strategy:
                logging.debug('found implementation of Strategy {}'.format(class_object))

                instance = class_object()
                context = StrategyContext(
                    name=name,
                    bar_size=BarSize.parse_str(bar_size_str),
                    conids=conids if conids else [],
                    universe=universe,
                    historical_days_prior=historical_days_prior if historical_days_prior else 0,
                    paper=paper,
                    storage=self.storage,
                    universe_accessor=self.universe_accessor,
                    logger=logging,
                    module=module,
                    class_name=class_name,
                    description=description,
                    auto_execute=auto_execute,
                )
                instance.install(context)
                # Give the strategy a reference to the runtime for subscriptions
                instance.strategy_runtime = self

                self.strategy_implementations.append(cast(Strategy, instance))

        except Exception as ex:
            logging.debug(ex)

    def config_loader(self, config_file: str):
        config_file = os.path.expanduser(config_file)
        logging.debug('loading config file {}'.format(config_file))
        conf_file = open(config_file, 'r')
        config = yaml.load(conf_file, Loader=yaml.FullLoader)

        for strategy_config in config['strategies']:
            self.load_strategy(
                name=strategy_config['name'],
                bar_size_str=strategy_config['bar_size'],
                conids=strategy_config.get('conids'),
                universe=strategy_config.get('universe'),
                historical_days_prior=strategy_config.get('historical_days_prior', 1),
                module=strategy_config.get('module', ''),
                class_name=strategy_config.get('class_name', ''),
                description=strategy_config.get('description', ''),
                paper=strategy_config.get('paper', False),
                auto_execute=strategy_config.get('auto_execute', False),
            )

    async def _reconcile(self):
        """Re-check config and subscriptions. Safe to call repeatedly (idempotent)."""
        # 1. Check for config file changes
        try:
            current_mtime = os.path.getmtime(self.strategy_config_file)
        except OSError:
            current_mtime = self._config_mtime

        if current_mtime != self._config_mtime:
            logging.info('strategy config changed, reloading')
            self.config_loader(self.strategy_config_file)
            self._config_mtime = current_mtime

        # 2. Re-subscribe all strategies (idempotent — only new conIds trigger publish_contract)
        # Wrapped in try/except so RPC timeouts (e.g. trader_service restarting) don't block the loop
        try:
            for strategy in self.strategy_implementations:
                if strategy.conids:
                    for conId in strategy.conids:
                        security_definitions = self.trader_client.rpc().resolve_symbol(conId)
                        if security_definitions:
                            self.subscribe(strategy, SecurityDefinition.to_contract(security_definitions[0]))

                if strategy.universe:
                    self.subscribe_universe(strategy, strategy.universe)
        except (TimeoutError, ConnectionError, Exception) as ex:
            logging.debug('reconciliation RPC failed (trader_service may be restarting): %s', ex)

    async def _reconnect_historical_client(self):
        """Disconnect and reconnect the IB historical data client."""
        logging.info('reconnecting historical data IB client')
        try:
            self.historical_data_client.shutdown()
        except Exception:
            pass
        await self.historical_data_client.connect_async()

    async def get_historical_data(self):
        for strategy in self.strategy_implementations:
            historical_days = strategy.historical_days_prior if strategy.historical_days_prior else 1

            if strategy.conids:
                for conId in strategy.conids:
                    security_definitions = self.trader_client.rpc().resolve_symbol(conId)
                    if security_definitions:
                        try:
                            await self.historical_data_client.get_contract_history(
                                security=SecurityDefinition.to_contract(security_definitions[0]),
                                what_to_show=WhatToShow.MIDPOINT,
                                bar_size=strategy.bar_size,
                                start_date=dt.datetime.now() - dt.timedelta(days=historical_days),
                                end_date=dt.datetime.now(),
                            )
                        except IBNoDataError as ex:
                            logging.warning('no historical data for conId {} strategy {}: {}'.format(
                                conId, strategy.name, ex))
                        except IBConnectivityError:
                            raise
                    else:
                        logging.error('could not find security definition for conId {} for strategy {}'.format(conId, strategy))

            if strategy.universe:
                conids = [x.conId for x in self.universe_accessor.get(strategy.universe).security_definitions]
                for conId in conids:
                    try:
                        await self.historical_data_client.get_contract_history(
                            security=Contract(conId=conId),
                            what_to_show=WhatToShow.MIDPOINT,
                            bar_size=strategy.bar_size,
                            start_date=dt.datetime.now() - dt.timedelta(days=historical_days),
                            end_date=dt.datetime.now(),
                        )
                    except IBNoDataError as ex:
                        logging.warning('no historical data for conId {} strategy {}: {}'.format(
                            conId, strategy.name, ex))
                    except IBConnectivityError:
                        raise
        logging.debug('finished get_historical_data()')

    async def run(self):
        logging.info('starting strategy_runtime')
        logging.debug('StrategyRuntime.run()')

        await self.trader_client.connect()

        self.zmq_subscriber = TopicPubSub[Ticker](
            self.zmq_pubsub_server_address,
            self.zmq_pubsub_server_port,
        )

        logging.debug('subscribing to tick stream')
        observable = await self.zmq_subscriber.subscriber('ticker')
        self.observer = AutoDetachObserver(
            on_next=self.on_ticker_next,
            on_error=self.on_ticker_error,
            on_completed=self.on_ticker_completed
        )
        self.subscription = observable.subscribe(self.observer)

        logging.debug('loading {} config file'.format(self.strategy_config_file))
        self.config_loader(self.strategy_config_file)

        logging.debug('subscribing to streams for all conids')

        # todo: i'm not sure the runtime should automagically subscribe here.
        # it's probably up to the strategy how they want to secure data
        for strategy in self.strategy_implementations:
            if strategy.conids:
                for conId in strategy.conids:
                    security_definitions = self.trader_client.rpc().resolve_symbol(conId)
                    if security_definitions:
                        self.subscribe(strategy, SecurityDefinition.to_contract(security_definitions[0]))
                    else:
                        logging.error('could not find security definition for conId {} for strategy {}. Disabling strategy.'
                                      .format(conId, strategy))
                        strategy.on_error(
                            Exception('could not find security definition for conId {} for strategy {}. Disabling strategy.'
                                      .format(conId, strategy))
                        )

            if strategy.universe:
                self.subscribe_universe(strategy, strategy.universe)

        logging.debug('starting connection to IB for historical data')

        self.historical_data_client = IBHistoryWorker(
            self.ib_server_address,
            self.ib_server_port,
            self.strategy_runtime_ib_client_id + 1,
        )
        max_retries = 5
        for attempt in range(1, max_retries + 1):
            try:
                if not self.historical_data_client.connected:
                    await self.historical_data_client.connect_async()
                await self.get_historical_data()
                break
            except IBConnectivityError as ex:
                if attempt == max_retries:
                    logging.error('historical data failed after {} attempts, giving up: {}'.format(max_retries, ex))
                    break
                wait = min(2 ** attempt, 30)
                logging.warning('IB connectivity error (attempt {}/{}), retrying in {}s: {}'.format(
                    attempt, max_retries, wait, ex))
                try:
                    await self._reconnect_historical_client()
                except Exception as reconnect_ex:
                    logging.error('reconnect failed: {}'.format(reconnect_ex))
                await asyncio.sleep(wait)
            except ConnectionError:
                if attempt == max_retries:
                    logging.error('IB not connected after {} attempts, giving up'.format(max_retries))
                    break
                wait = min(2 ** attempt, 30)
                logging.warning('IB not connected (attempt {}/{}), retrying in {}s'.format(
                    attempt, max_retries, wait))
                await asyncio.sleep(wait)
            except Exception as ex:
                logging.error('unexpected error fetching historical data: {}'.format(ex))
                break

        # Track config mtime for change detection
        try:
            self._config_mtime = os.path.getmtime(self.strategy_config_file)
        except OSError:
            self._config_mtime = 0.0

        # Stay alive and periodically reconcile subscriptions
        logging.info('entering reconciliation loop (30s interval)')
        while True:
            await asyncio.sleep(30)
            try:
                await self._reconcile()
            except Exception as ex:
                logging.error('reconciliation error: {}'.format(ex))

