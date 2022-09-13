from ib_insync import Contract
from ib_insync.ticker import Ticker
from reactivex.observer import AutoDetachObserver
from trader.common.exceptions import TraderConnectionException, TraderException
from trader.common.listener_helpers import Helpers
from trader.common.logging_helper import setup_logging
from trader.common.singleton import Singleton
from trader.data.data_access import TickData
from trader.data.universe import UniverseAccessor
from trader.messaging.clientserver import RemotedClient, TopicPubSub
from trader.messaging.trader_service_api import TraderServiceApi
from trader.objects import Action
from trader.strategy.strategies.smi_crossover import SMICrossOver
from trader.trading.strategy import Strategy
from typing import Dict, List

import asyncio
import pandas as pd


logging = setup_logging(module_name='strategy_runtime')


error_table = {
    'trader.common.exceptions.TraderException': TraderException,
    'trader.common.exceptions.TraderConnectionException': TraderConnectionException
}


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

        self.accessor = UniverseAccessor(arctic_server_address, arctic_universe_library)
        self.remoted_client = RemotedClient[TraderServiceApi](error_table=error_table)
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
            result = pd.concat([self.streams[conId], Helpers.df(ticker)], axis=0, copy=False)
            self.streams[conId] = result

        def __get_strategies(conId: int) -> List[Strategy]:
            if conId in self.strategies:
                return self.strategies[conId]
            else:
                return []

        # execute the strategies attached to the conId's
        for strategy in (__get_strategies(conId) + self.strategies[0]):
            signal = strategy.on_next(self.streams[conId])
            if signal and signal.action == Action.BUY:
                logging.info('BUY action')
            elif signal and signal.action == Action.SELL:
                logging.info('SELL action')

    def on_error(self, ex: Exception):
        logging.debug('StrategyRuntime.on_error')

    def on_completed(self):
        logging.debug('StrategyRuntime.on_completed')

    async def run(self):
        logging.debug('StrategyRuntime.run()')

        self.strategies[0].append(SMICrossOver())

        asyncio.get_event_loop().run_until_complete(self.remoted_client.connect())
        # definition = self.accessor.resolve_first_symbol('NCM')
        # if not definition:
        #     return

        # contract = Universe.to_contract(definition[1])

        # start publishing ticks
        contract = Contract(symbol='NCM', primaryExchange='ASX', exchange='SMART', currency='AUD')
        self.remoted_client.rpc().publish_contract(contract=contract, delayed=False)

        self.zmq_subscriber = TopicPubSub[Ticker](
            self.zmq_pubsub_server_address,
            self.zmq_pubsub_server_port,
        )

        logging.debug('subscribing to ticks')
        observable = await self.zmq_subscriber.subscriber('ticker')
        self.observer = AutoDetachObserver(on_next=self.on_next, on_error=self.on_error, on_completed=self.on_completed)
        self.subscription = observable.subscribe(self.observer)