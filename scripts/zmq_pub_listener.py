import sys
import os

# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

import pandas as pd
import datetime as dt
import aioreactive as rx
import trader.messaging.trader_service_api as bus
import asyncio
import signal
import click
from asyncio.events import AbstractEventLoop
from aioreactive.types import AsyncObservable, Projection
from expression.core import pipe
from aioreactive.observers import AsyncAnonymousObserver, auto_detach_observer, safe_observer
from enum import Enum

from trader.common.logging_helper import setup_logging
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
from trader.messaging.clientserver import PubSubAsyncSubject, PubSubClient, TopicPubSub
from trader.common.helpers import rich_dict

from typing import List, Dict, Tuple, Callable, Optional, Set, Generic, TypeVar, cast, Union

class ZmqPrettyPrinter():
    def __init__(
        self,
        zmq_pubsub_server_address: str,
        zmq_pubsub_server_port: int,
        zmq_topic: str = 'default'
    ):
        self.zmq_pubsub_server_address = zmq_pubsub_server_address
        self.zmq_pubsub_server_port = zmq_pubsub_server_port
        self.zmq_topic = zmq_topic

    async def asend(self, ticker: Ticker):
        snap = {
            'symbol': ticker.contract.symbol if ticker.contract else '',
            'exchange': ticker.contract.exchange if ticker.contract else '',
            'primaryExchange': ticker.contract.primaryExchange if ticker.contract else '',
            'currency': ticker.contract.currency if ticker.contract else '',
            'time': ticker.time,
            'bid': ticker.bid,
            'bidSize': ticker.bidSize,
            'ask': ticker.ask,
            'askSize': ticker.askSize,
            'last': ticker.last,
            'lastSize': ticker.lastSize,
            'open': ticker.open,
            'high': ticker.high,
            'low': ticker.low,
            'close': ticker.close,
            'halted': ticker.halted
        }
        rich_dict(snap)

    async def listen(self):
        logging.debug('zmq_pub_listener listen({}, {}, {}'.format(
            self.zmq_pubsub_server_address,
            self.zmq_pubsub_server_port,
            self.zmq_topic
        ))

        sub = TopicPubSub[Ticker](
            self.zmq_pubsub_server_address,
            self.zmq_pubsub_server_port,
            self.zmq_topic
        )

        observable = await sub.subscriber()
        observer = AsyncAnonymousObserver(asend=self.asend)
        safe_obv, auto_detach = auto_detach_observer(observer)
        subscription = await pipe(safe_obv, observable.subscribe_async, auto_detach)

    def console_listener(self):
        loop = asyncio.new_event_loop()
        try:
            loop.create_task(self.listen())
            loop.run_forever()
        except KeyboardInterrupt:
            logging.info('KeyboardInterrupt')


@click.command()
@click.option('--zmq_pubsub_server_address', required=True, default='tcp://127.0.0.1')
@click.option('--zmq_pubsub_server_port', required=True, default=42002)
def main(
    zmq_pubsub_server_address: str,
    zmq_pubsub_server_port: int
):
    printer = ZmqPrettyPrinter(zmq_pubsub_server_address, zmq_pubsub_server_port)
    printer.console_listener()

if __name__ == '__main__':
    main()
