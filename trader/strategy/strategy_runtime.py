from ib_insync.ticker import Ticker
from reactivex.observer import AutoDetachObserver
from trader.common.listener_helpers import Helpers
from trader.common.logging_helper import setup_logging
from trader.common.singleton import Singleton
from trader.data.data_access import TickData
from trader.data.universe import UniverseAccessor
from trader.messaging.clientserver import RPCServer, TopicPubSub
from trader.strategy.strategies.smi_crossover import SMICrossOver
from trader.trading.strategy import Strategy
from typing import Dict, List

import pandas as pd
import trader.messaging.trader_service_api as bus


logging = setup_logging(module_name='strategy_runtime')


class StrategyRuntime(metaclass=Singleton):
    def __init__(self,
                 arctic_server_address: str,
                 arctic_library: str,
                 arctic_universe_library: str,
                 zmq_pubsub_server_address: str,
                 zmq_pubsub_server_port: int,
                 zmq_rpc_server_address: str,
                 zmq_rpc_server_port: int,
                 paper_trading: bool = False,
                 simulation: bool = False):
        self.arctic_server_address = arctic_server_address
        self.arctic_library = arctic_library
        self.arctic_universe_library = arctic_universe_library
        self.simulation: bool = simulation
        self.paper_trading = paper_trading
        self.zmq_pubsub_server_address = zmq_pubsub_server_address
        self.zmq_pubsub_server_port = zmq_pubsub_server_port
        self.zmq_rpc_server_address = zmq_rpc_server_address
        self.zmq_rpc_server_port = zmq_rpc_server_port

        self.data: TickData
        self.universe_accessor: UniverseAccessor
        self.zmq_rpc_server: RPCServer[bus.TraderServiceApi]
        self.strategies: Dict[int, List[Strategy]] = {}
        self.strategies[0] = []

        self.streams: Dict[int, pd.DataFrame] = {}

    def on_next(self, ticker: Ticker):
        logging.debug('StrategyRuntime.on_next()')
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
            self.streams[conId].append(Helpers.df(ticker))

        # execute the strategies attached to the conId's
        for strategy in (self.strategies[conId] + self.strategies[0]):
            strategy.on_next(self.streams[conId])

    def on_error(self, ex: Exception):
        pass

    def on_completed(self):
        pass

    async def run(self):
        logging.debug('StrategyRuntime.run()')
        self.strategies[0].append(SMICrossOver())

        self.zmq_subscriber = TopicPubSub[Ticker](
            self.zmq_pubsub_server_address,
            self.zmq_pubsub_server_port,
        )

        observable = await self.zmq_subscriber.subscriber('default')
        self.observer = AutoDetachObserver(on_next=self.on_next, on_error=self.on_error, on_completed=self.on_completed)
        self.subscription = observable.subscribe(self.observer)
