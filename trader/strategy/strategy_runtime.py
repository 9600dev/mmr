from ib_insync import Contract
from ib_insync.ib import IB
from ib_insync.ticker import Ticker
from reactivex.observer import AutoDetachObserver
from trader.common.exceptions import TraderConnectionException, TraderException
from trader.common.listener_helpers import Helpers
from trader.common.logging_helper import get_callstack, log_method, setup_logging
from trader.common.singleton import Singleton
from trader.data.data_access import SecurityDefinition, TickStorage
from trader.data.universe import UniverseAccessor
from trader.listeners.ib_history_worker import IBHistoryWorker
from trader.messaging.clientserver import MultithreadedTopicPubSub, RemotedClient, RPCServer, TopicPubSub
from trader.objects import Action, BarSize, WhatToShow
from trader.trading.strategy import Strategy, StrategyMetadata, StrategyState
from typing import cast, Dict, List, Optional

import asyncio
import backoff
import datetime as dt
import importlib
import inspect
import os
import pandas as pd
import trader.messaging.strategy_service_api as bus
import yaml


logging = setup_logging(module_name='strategy_runtime')


error_table = {
    'trader.common.exceptions.TraderException': TraderException,
    'trader.common.exceptions.TraderConnectionException': TraderConnectionException
}


class StrategyRuntime(metaclass=Singleton):
    def __init__(
        self,
        ib_server_address: str,
        ib_server_port: int,
        strategy_runtime_ib_client_id: int,
        arctic_server_address: str,
        arctic_universe_library: str,
        zmq_pubsub_server_address: str,
        zmq_pubsub_server_port: int,
        zmq_rpc_server_address: str,
        zmq_rpc_server_port: int,
        zmq_strategy_rpc_server_address: str,
        zmq_strategy_rpc_server_port: int,
        strategies_directory: str,
        strategy_config_file: str,
        paper_trading: bool = False,
        simulation: bool = False
    ):
        self.ib_server_address = ib_server_address
        self.ib_server_port = ib_server_port
        self.strategy_runtime_ib_client_id: int = strategy_runtime_ib_client_id
        self.arctic_server_address = arctic_server_address
        self.arctic_universe_library = arctic_universe_library
        self.simulation: bool = simulation
        self.paper_trading = paper_trading
        self.zmq_pubsub_server_address = zmq_pubsub_server_address
        self.zmq_pubsub_server_port = zmq_pubsub_server_port
        self.zmq_rpc_server_address = zmq_rpc_server_address
        self.zmq_rpc_server_port = zmq_rpc_server_port
        self.zmq_strategy_rpc_server_address = zmq_strategy_rpc_server_address
        self.zmq_strategy_rpc_server_port = zmq_strategy_rpc_server_port
        self.strategies_directory = strategies_directory
        self.strategy_config_file = strategy_config_file
        self.startup_time: dt.datetime = dt.datetime.now()
        self.last_connect_time: dt.datetime

        self.zmq_strategy_rpc_server: RPCServer[bus.StrategyServiceApi]
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
            self.storage = TickStorage(self.arctic_server_address)
            self.universe_accessor = UniverseAccessor(self.arctic_server_address, self.arctic_universe_library)
            self.remoted_client = RemotedClient[TraderServiceApi](error_table=error_table)
            self.last_connect_time = dt.datetime.now()

            self.zmq_strategy_rpc_server = RPCServer[bus.StrategyServiceApi](
                instance=bus.StrategyServiceApi(self),
                zmq_rpc_server_address=self.zmq_strategy_rpc_server_address,
                zmq_rpc_server_port=self.zmq_strategy_rpc_server_port,
            )

            asyncio.run(self.zmq_strategy_rpc_server.serve())
        except Exception as ex:
            raise self.create_strategy_exception(TraderConnectionException, message='strategy_runtime.connect() exception', inner=ex)

    @log_method
    def enable_strategy(self, strategy: Strategy) -> StrategyState:
        for strategy in self.strategy_implementations:
            if strategy.name == strategy.name:
                return strategy.enable()
        return StrategyState.ERROR

    @log_method
    def disable_strategy(self, strategy: Strategy) -> StrategyState:
        for strategy in self.strategy_implementations:
            if strategy.name == strategy.name:
                return strategy.disable()
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

    @log_method
    def load_strategies(self):
        for root, dirs, files in os.walk(self.strategies_directory):
            for file_name in files:
                file = os.path.join(root, file_name)
                try:
                    relative_import = os.path.relpath(file).replace('.py', '').replace('.pyc', '').replace('/', '.')
                    module = importlib.import_module(relative_import)
                    for x in dir(module):
                        obj = getattr(module, x)

                        if inspect.isclass(obj) and issubclass(obj, Strategy) and obj is not Strategy:
                            logging.debug('found implementation of Strategy {}'.format(obj))
                            # todo: fix this
                            # here's where we need bar_size to strategy
                            instance = obj(self.storage, self.universe_accessor, logging)
                            self.strategy_implementations.append(cast(Strategy, instance))

                            # logic to install and download data for the strategy
                            # todo: once bugs are out, automatically enable strategies
                            instance.install(self)
                            # instance.enable()

                except Exception as ex:
                    logging.debug(ex)

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
        if conId not in self.streams:
            self.streams[conId] = pd.DataFrame(Helpers.df(ticker))
        else:
            result = pd.concat([self.streams[conId], Helpers.df(ticker)], axis=0, copy=False)
            self.streams[conId] = result

        # execute the strategies attached to the conId's
        for strategy in self.__get_enabled_strategies(conId):
            signal = strategy.on_next(self.streams[conId])
            if signal and signal.action == Action.BUY:
                logging.info('BUY action')
            elif signal and signal.action == Action.SELL:
                logging.info('SELL action')

    def on_ticker_error(self, ex: Exception):
        logging.debug('StrategyRuntime.on_error')

    def on_ticker_completed(self):
        logging.debug('StrategyRuntime.on_completed')

    def subscribe(self, strategy: Strategy, contract: Contract) -> None:
        logging.debug('strategy_runtime.subscribe() contract: {} strategy: {}'.format(contract, strategy))
        if contract.conId not in self.strategies:
            self.strategies[contract.conId] = []
            self.strategies[contract.conId].append(strategy)
            self.remoted_client.rpc().publish_contract(contract=contract, delayed=False)
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
        description: str,
    ) -> None:
        root_path = os.path.abspath(os.getcwd())
        file = os.path.join(root_path, module)
        try:
            relative_import = os.path.relpath(file).replace('.py', '').replace('/', '.')
            loaded_module = importlib.import_module(relative_import)
            for x in dir(loaded_module):
                obj = getattr(loaded_module, x)

                # todo, might have to find StrategyConfig and load that too
                if inspect.isclass(obj) and issubclass(obj, Strategy) and obj is not Strategy:
                    logging.debug('found implementation of Strategy {}'.format(obj))

                    # todo: fix this
                    # here's where we need bar_size to strategy
                    instance = obj(self.storage, self.universe_accessor, logging)
                    instance.name = name
                    instance.bar_size = BarSize.parse_str(bar_size_str)
                    instance.description = description
                    instance.conids = conids
                    instance.universe = universe
                    instance.historical_days_prior = historical_days_prior
                    instance.description = description

                    self.strategy_implementations.append(cast(Strategy, instance))

                    # logic to install and download data for the strategy
                    if instance.install(self):
                        instance.enable()

        except Exception as ex:
            logging.debug(ex)

    def config_loader(self, config_file: str):
        logging.debug('loading config file {}'.format(config_file))
        conf_file = open(config_file, 'r')
        config = yaml.load(conf_file, Loader=yaml.FullLoader)

        for strategy_config in config['strategies']:
            self.load_strategy(
                name=strategy_config['name'],
                bar_size_str=strategy_config['bar_size'],
                conids=strategy_config['conids'] if 'conids' in strategy_config else None,
                universe=strategy_config['universe'] if 'universe' in strategy_config else None,
                historical_days_prior=strategy_config['historical_days_prior'] if 'historical_days_prior' in strategy_config else 1,
                module=strategy_config['module'] if 'module' in strategy_config else '',
                description=strategy_config['description'] if 'description' in strategy_config else '',
            )

    async def get_historical_data(self):
        for strategy in self.strategy_implementations:
            historical_days = strategy.historical_days_prior if strategy.historical_days_prior else 1

            if strategy.conids:
                for conId in strategy.conids:
                    await self.historical_data_client.get_contract_history(
                        security=Contract(conId=conId),
                        what_to_show=WhatToShow.MIDPOINT,
                        bar_size=strategy.bar_size,
                        start_date=dt.datetime.now() - dt.timedelta(days=historical_days),
                        end_date=dt.datetime.now(),
                    )

            if strategy.universe:
                conids = [x.conId for x in self.universe_accessor.get(strategy.universe).security_definitions]
                for conId in conids:
                    await self.historical_data_client.get_contract_history(
                        security=Contract(conId=conId),
                        what_to_show=WhatToShow.MIDPOINT,
                        bar_size=strategy.bar_size,
                        start_date=dt.datetime.now() - dt.timedelta(days=historical_days),
                        end_date=dt.datetime.now(),
                    )
        logging.debug('finished get_historical_data()')

    async def run(self):
        logging.info('starting strategy_runtime')
        logging.debug('StrategyRuntime.run()')

        asyncio.get_event_loop().run_until_complete(self.remoted_client.connect())

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
        for strategy in self.strategy_implementations:
            if strategy.conids:
                for conId in strategy.conids:
                    self.subscribe(strategy, Contract(conId=conId))

            if strategy.universe:
                self.subscribe_universe(strategy, strategy.universe)

        logging.debug('starting connection to IB for historical data')

        self.historical_data_client = IBHistoryWorker(
            self.ib_server_address,
            self.ib_server_port,
            self.strategy_runtime_ib_client_id + 1,
        )
        self.historical_data_client.connect()
        await self.get_historical_data()

