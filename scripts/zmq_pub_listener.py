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

    def print_console(self, df: pd.DataFrame, rebuild: bool = False):
        self.table = Table()
        for column in df.columns:
            self.table.add_column(column)

        for index, value_list in enumerate(df.values.tolist()):
            row = [str(x) for x in value_list]
            self.table.add_row(*row)

        self.console.clear()
        self.console.print(self.table)


class ZmqPrettyPrinter():
    def __init__(
        self,
        zmq_pubsub_server_address: str,
        zmq_pubsub_server_port: int,
        zmq_topic: str = 'default',
        csv: bool = False,
    ):
        self.zmq_pubsub_server_address = zmq_pubsub_server_address
        self.zmq_pubsub_server_port = zmq_pubsub_server_port
        self.zmq_topic = zmq_topic
        self.contract_ticks: Dict[Contract, Ticker] = {}
        self.console = rich.console.Console()
        self.rich_live = RichLiveDataFrame(self.console)
        self.subscription: AsyncDisposable
        self.observer: AsyncObserver
        self.csv = csv

    def print_console(self, ticker: Optional[Ticker] = None):
        def get_snap(ticker: Ticker):
            return {
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

        # self.console.clear(True)
        # rich_table(data_frame, False, True, ['currency', 'bid', 'ask', 'last', 'open', 'high', 'low', 'close'])
        if not self.csv:
            data = [get_snap(ticker) for contract, ticker in self.contract_ticks.items()]
            data_frame = pd.DataFrame(data)
            self.rich_live.print_console(data_frame)
        else:
            t = get_snap(ticker)  # type: ignore
            str_values = [str(v) for v in t.values()]
            print(','.join(str_values))

    async def asend(self, ticker: Ticker):
        if ticker.contract:
            self.contract_ticks[ticker.contract] = ticker
            self.print_console(ticker)

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
        self.observer, auto_detach = auto_detach_observer(observer)
        self.subscription = await pipe(self.observer, observable.subscribe_async, auto_detach)

    async def shutdown(self):
        await self.observer.aclose()
        await self.subscription.dispose_async()

    def console_listener(self):
        def stop_loop(loop: AbstractEventLoop):
            if loop.is_running():
                loop.run_until_complete(self.shutdown())
                pending_tasks = [
                    task for task in asyncio.all_tasks() if not task.done()
                ]

                if len(pending_tasks) > 0:
                    for task in pending_tasks:
                        logging.debug(task.get_stack())

                    logging.debug('waiting five seconds for {} pending tasks'.format(len(pending_tasks)))
                    loop.run_until_complete(asyncio.wait(pending_tasks, timeout=5))
                loop.stop()

        loop = asyncio.new_event_loop()
        loop.add_signal_handler(signal.SIGINT, stop_loop, loop)
        try:
            loop.create_task(self.listen())
            loop.run_forever()
        except KeyboardInterrupt:
            logging.info('KeyboardInterrupt')
            stop_loop(loop)


@click.command()
@click.option('--csv', required=True, is_flag=True, default=False)
@click.option('--zmq_pubsub_server_address', required=True, default='tcp://127.0.0.1')
@click.option('--zmq_pubsub_server_port', required=True, default=42002)
def main(
    csv: bool,
    zmq_pubsub_server_address: str,
    zmq_pubsub_server_port: int
):
    printer = ZmqPrettyPrinter(zmq_pubsub_server_address, zmq_pubsub_server_port, csv=csv)
    printer.console_listener()

if __name__ == '__main__':
    main()
