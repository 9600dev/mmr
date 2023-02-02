import os
import sys


# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from asyncio.events import AbstractEventLoop
from ib_insync.contract import Contract
from ib_insync.ticker import Ticker
from reactivex.abc import DisposableBase, ObserverBase
from reactivex.observer import AutoDetachObserver
from rich.live import Live
from rich.table import Table
from trader.common.logging_helper import setup_logging
from trader.messaging.clientserver import TopicPubSub
from typing import Dict, List, Optional

import asyncio
import click
import functools
import numpy as np
import pandas as pd
import plotext as plt
import rich
import signal


logging = setup_logging(module_name='trading_runtime')

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

class LiveGraph():
    def __init__(self, console: rich.console.Console, height: int = 0):
        self.console = console
        self.first: bool = True
        self.y_values: List[float] = []
        self.height = height

    def print_console(self, df: pd.DataFrame, column_name: str):
        if self.first:
            self.console.clear()
            self.first = False

        self.y_values.append(float(df[column_name].values[-1]))

        if self.height > 0:
            plt.plot_size(width=plt.tw(), height=self.height)
            plt.clear_terminal()

        plt.clear_data()
        plt.theme('dark')
        plt.grid(1, 1)
        plt.xaxes(False, False)
        plt.plot(
            list(range(0, len(self.y_values))),
            self.y_values,
            marker='hd')
        plt.show()


# https://stackoverflow.com/questions/35223896/listen-to-keypress-with-asyncio
class Prompt():
    def __init__(self, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.q = asyncio.Queue()
        self.loop.add_reader(sys.stdin, self.got_input)

    def got_input(self):
        asyncio.ensure_future(self.q.put(sys.stdin.readline()), loop=self.loop)

    async def __call__(self, msg, end='\n', flush=False):
        print(msg, end=end, flush=flush)
        return (await self.q.get()).rstrip('\n')


class ZmqPrettyPrinter():
    def __init__(
        self,
        zmq_pubsub_server_address: str,
        zmq_pubsub_server_port: int,
        csv: bool = False,
        live_graph: bool = False,
        filter_symbol: str = '',
        height: int = 0,
    ):
        self.zmq_pubsub_server_address = zmq_pubsub_server_address
        self.zmq_pubsub_server_port = zmq_pubsub_server_port
        self.contract_ticks: Dict[Contract, Ticker] = {}
        self.console = rich.console.Console()
        self.rich_live = RichLiveDataFrame(self.console)
        self.live_graph = live_graph
        self.filter_symbol = filter_symbol
        self.graph = LiveGraph(self.console, height=height)
        self.subscription: DisposableBase
        self.observer: ObserverBase
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
                'lastSize': int(ticker.lastSize) if not np.isnan(ticker.lastSize) else -1,
                'open': '%.2f' % ticker.open,
                'high': '%.2f' % ticker.high,
                'low': '%.2f' % ticker.low,
                'close': '%.2f' % ticker.close,
                'halted': int(ticker.halted) if not np.isnan(ticker.halted) else -1
            }

        if (ticker and ticker.contract and self.filter_symbol in ticker.contract.symbol) or \
           (ticker and ticker.contract and self.filter_symbol.isnumeric() and int(self.filter_symbol) == ticker.contract.conId):

            if not self.csv and not self.live_graph:
                self.counter += 1
                data = [get_snap(ticker) for contract, ticker in self.contract_ticks.items()]
                data_frame = pd.DataFrame(data)
                self.rich_live.print_console(data_frame, 'Hit Enter to stop...')
                if self.counter % 1000 == 0:
                    self.console.clear()
                    self.contract_ticks.clear()
            elif self.live_graph:
                self.counter += 1
                # data = [get_snap(ticker) for contract, ticker in self.contract_ticks.items()]
                data_frame = pd.DataFrame([get_snap(ticker)])
                self.graph.print_console(data_frame, 'ask')
            else:
                t = get_snap(ticker)  # type: ignore
                str_values = [str(v) for v in t.values()]
                print(','.join(str_values))

    def on_next(self, ticker: Ticker):
        if ticker.contract:
            self.contract_ticks[ticker.contract] = ticker
            try:
                self.print_console(ticker)
            except Exception as ex:
                logging.exception(ex)
                self.wait_handle.set()
                raise ex

    def on_error(self, ex: Exception):
        logging.exception(ex)
        self.wait_handle.set()
        raise ex

    def on_completed(self):
        logging.debug('zmq_pub_listener.on_completed')

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
            self.observer = AutoDetachObserver(on_next=self.on_next, on_error=self.on_error, on_completed=self.on_completed)
            self.subscription = observable.subscribe(self.observer)

            prompt = Prompt()
            raw_input = functools.partial(prompt, end='', flush=True)

            await prompt("Press enter to exit...")
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
            self.zmq_subscriber.subscriber_close()
            self.observer.on_completed()
            self.subscription.dispose()


@click.command()
@click.option('--csv', required=True, is_flag=True, default=False)
@click.option('--live_graph', required=False, is_flag=True, default=False)
@click.option('--topic', required=True, default='ticker')
@click.option('--filter_symbol', required=False, default='')
@click.option('--zmq_pubsub_server_address', required=True, default='tcp://127.0.0.1')
@click.option('--zmq_pubsub_server_port', required=True, default=42002)
def main(
    csv: bool,
    live_graph: bool,
    topic: str,
    filter_symbol: str,
    zmq_pubsub_server_address: str,
    zmq_pubsub_server_port: int
):
    printer = ZmqPrettyPrinter(
        zmq_pubsub_server_address,
        zmq_pubsub_server_port,
        csv=csv,
        live_graph=live_graph,
        filter_symbol=filter_symbol,
    )

    def stop_loop(loop: AbstractEventLoop):
        loop.run_until_complete(printer.shutdown())

    loop = asyncio.get_event_loop()
    loop.set_debug(enabled=True)
    loop.add_signal_handler(signal.SIGINT, stop_loop, loop)
    loop.run_until_complete(printer.listen(topic=topic))

if __name__ == '__main__':
    main()
