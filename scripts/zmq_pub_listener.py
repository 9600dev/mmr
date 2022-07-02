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
import rich
import time
from rich.console import Console
from asyncio.events import AbstractEventLoop
from aioreactive.types import AsyncObservable, Projection, AsyncObserver
from expression.core import pipe
from expression.system import AsyncDisposable
from aioreactive.observers import AsyncAnonymousObserver, auto_detach_observer, safe_observer
from enum import Enum

from trader.common.logging_helper import setup_logging
logging = setup_logging(module_name='trading_runtime')

from rich.live import Live
from rich.table import Table
from arctic import Arctic, TICK_STORE
from arctic.date import DateRange
from arctic.tickstore.tickstore import TickStore
from ib_insync.ib import IB
from ib_insync.contract import Contract, Forex, Future, Stock
from ib_insync.objects import PortfolioItem, Position, BarData
from ib_insync.order import LimitOrder, Order, Trade
from ib_insync.ticker import Ticker
from trader.messaging.clientserver import PubSubAsyncSubject, PubSubClient, TopicPubSub
from trader.common.helpers import rich_dict, rich_table

from typing import List, Dict, Tuple, Callable, Optional, Set, Generic, TypeVar, cast, Union

class RichLiveDataFrame():
    def __init__(self, console: rich.console.Console):
        self.table = Table()
        self.live = Live()
        self.console = console
        self.first: bool = True

    def print_console(self, df: pd.DataFrame, title: Optional[str] = None):
        def move(y, x):
            print("\033[%d;%dH" % (y, x))

        if self.first:
            self.console.clear()
            self.first = False

        self.table = Table()
        for column in df.columns:
            self.table.add_column(column)

        for index, value_list in enumerate(df.values.tolist()):
            row = [str(x) for x in value_list]
            self.table.add_row(*row)

        # self.console.clear()
        move(0, 0)
        if title:
            self.console.print(title)
        self.console.print(self.table)


class ZmqPrettyPrinter():
    def __init__(
        self,
        zmq_pubsub_server_address: str,
        zmq_pubsub_server_port: int,
        csv: bool = False,
    ):
        self.zmq_pubsub_server_address = zmq_pubsub_server_address
        self.zmq_pubsub_server_port = zmq_pubsub_server_port
        self.contract_ticks: Dict[Contract, Ticker] = {}
        self.console = rich.console.Console()
        self.rich_live = RichLiveDataFrame(self.console)
        self.subscription: AsyncDisposable
        self.observer: AsyncObserver
        self.csv = csv
        self.counter = 0
        self.zmq_subscriber: TopicPubSub
        self.wait_handle: asyncio.Event = asyncio.Event()
        self.being_shutdown = False

    def print_console(self, ticker: Optional[Ticker] = None):
        def get_snap(ticker: Ticker):
            date_time_str = ticker.time.strftime('%H:%M.%S') if ticker.time else ''
            return {
                'symbol': ticker.contract.symbol if ticker.contract else '',
                'primaryExchange': ticker.contract.primaryExchange if ticker.contract else '',
                'currency': ticker.contract.currency if ticker.contract else '',
                'time': date_time_str,
                'bid': '%.2f' % ticker.bid,
                'ask': '%.2f' % ticker.ask,
                'last': '%.2f' % ticker.last,
                'lastSize': int(ticker.lastSize),
                'open': '%.2f' % ticker.open,
                'high': '%.2f' % ticker.high,
                'low': '%.2f' % ticker.low,
                'close': '%.2f' % ticker.close,
                'halted': int(ticker.halted)
            }

        # self.console.clear(True)
        # rich_table(data_frame, False, True, ['currency', 'bid', 'ask', 'last', 'open', 'high', 'low', 'close'])
        if not self.csv:
            self.counter += 1
            data = [get_snap(ticker) for contract, ticker in self.contract_ticks.items()]
            data_frame = pd.DataFrame(data)
            self.rich_live.print_console(data_frame, 'Ctrl-c to stop...')
            if self.counter % 1000 == 0:
                self.console.clear()
                self.contract_ticks.clear()
        else:
            t = get_snap(ticker)  # type: ignore
            str_values = [str(v) for v in t.values()]
            print(','.join(str_values))

    async def asend(self, ticker: Ticker):
        if ticker.contract:
            self.contract_ticks[ticker.contract] = ticker
            try:
                self.print_console(ticker)
            except Exception as ex:
                logging.exception(ex)
                self.wait_handle.set()
                raise ex

    async def athrow(self, ex: Exception):
        logging.exception(ex)
        self.wait_handle.set()
        raise ex

    async def listen(self, topic: str):
        try:
            logging.debug('zmq_pub_listener listen({}, {}, {})'.format(
                self.zmq_pubsub_server_address,
                self.zmq_pubsub_server_port,
                topic
            ))

            self.zmq_subscriber = TopicPubSub[Ticker](
                self.zmq_pubsub_server_address,
                self.zmq_pubsub_server_port,
            )

            observable = await self.zmq_subscriber.subscriber(topic=topic)
            observer = AsyncAnonymousObserver(asend=self.asend, athrow=self.athrow)
            self.observer, auto_detach = auto_detach_observer(observer)
            self.subscription = await pipe(self.observer, observable.subscribe_async, auto_detach)

            await self.wait_handle.wait()
        except KeyboardInterrupt:
            logging.debug('KeyboardInterrupt')
            self.wait_handle.set()
        finally:
            await self.shutdown()

    # https://www.joeltok.com/blog/2020-10/python-asyncio-create-task-fails-silently
    async def shutdown(self):
        logging.debug('shutdown()')
        self.wait_handle.set()
        if not self.being_shutdown:
            self.being_shutdown = True
            await self.zmq_subscriber.subscriber_close()
            await self.observer.aclose()
            await self.subscription.dispose_async()


@click.command()
@click.option('--csv', required=True, is_flag=True, default=False)
@click.option('--topic', required=True, default='ticker')
@click.option('--zmq_pubsub_server_address', required=True, default='tcp://127.0.0.1')
@click.option('--zmq_pubsub_server_port', required=True, default=42002)
def main(
    csv: bool,
    topic: str,
    zmq_pubsub_server_address: str,
    zmq_pubsub_server_port: int
):
    printer = ZmqPrettyPrinter(zmq_pubsub_server_address, zmq_pubsub_server_port, csv=csv)

    def stop_loop(loop: AbstractEventLoop):
        loop.run_until_complete(printer.shutdown())

    loop = asyncio.get_event_loop()
    loop.set_debug(enabled=True)
    loop.add_signal_handler(signal.SIGINT, stop_loop, loop)
    loop.run_until_complete(printer.listen(topic=topic))

if __name__ == '__main__':
    main()
