from asyncio import AbstractEventLoop
import sys
import os

# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

import asyncio
import pandas as pd
import rx
import datetime as dt
import click
import backoff
import os

from arctic import Arctic, TICK_STORE
from arctic.tickstore.tickstore import TickStore
from tabulate import tabulate
from enum import Enum
from typing import List, Dict, Tuple, Callable, Optional, Set, Generic, TypeVar, cast, Union
from asyncio import BaseEventLoop
from rx import operators as ops
from rx.subject import Subject
from rx.scheduler import ThreadPoolScheduler, CatchScheduler, CurrentThreadScheduler
from rx.scheduler.eventloop import AsyncIOThreadSafeScheduler
from rx.core.typing import Observable, Observer, Disposable, Scheduler, OnNext, OnError, OnCompleted
from ib_insync import Stock, IB, Contract, Forex, BarData, Future
from ib_insync.util import df
from ib_insync.ticker import Ticker

from trader.listeners.ibrx import IBRx
from trader.common.contract_sink import ContractSink
from trader.common.listener_helpers import Helpers
from trader.common.logging_helper import setup_logging
from trader.common.observers import ConsoleObserver, ArcticObserver, ComplexConsoleObserver, ContractSinkObserver, NullObserver
from trader.messaging.messaging_client import MessagingPublisher

logging = setup_logging(module_name='ib_listener')

class IBListener():
    def __init__(self,
                 arctic_server_address: str = 'localhost',
                 arctic_library: str = 'Ticks',
                 publish_ip_address: str = '127.0.0.1',
                 publish_port: int = 5001,
                 loop: AbstractEventLoop = None):
        self.arctic_server_address = arctic_server_address
        self.arctic_library = arctic_library
        self.publish_ip_address = publish_ip_address
        self.publish_port = publish_port
        self.ib_client = IB()
        self.loop = loop
        scheduler = None
        if self.loop:
            scheduler = AsyncIOThreadSafeScheduler(self.loop)
        self.client: IBRx = IBRx(ib=self.ib_client, scheduler=scheduler)
        self.symbol_subscriptions: Dict[str, ContractSink] = {}
        self.store: Arctic
        self.library: TickStore
        self.messaging_publisher: MessagingPublisher

    @backoff.on_exception(backoff.expo, ConnectionRefusedError, max_tries=10, max_time=120)
    def connect(self,
                ib_server_address: str = '127.0.0.1',
                ib_server_port: int = 7496,
                client_id: int = 2):
        self.client.connect(ib_server_address=ib_server_address,
                            ib_server_port=ib_server_port,
                            client_id=client_id)
        self.store = Arctic(self.arctic_server_address)
        self.store.initialize_library(self.arctic_library, lib_type=TICK_STORE)
        self.library = self.store[self.arctic_library]
        self.messaging_publisher = MessagingPublisher(publish_ip_address=self.publish_ip_address,
                                                      publish_port=self.publish_port,
                                                      loop=self.loop)

    def console_symbol(self, contract: Contract):
        if contract.symbol in self.symbol_subscriptions:
            return self.symbol_subscriptions[contract.symbol].subscribe(ConsoleObserver())

        symbol_sink = ContractSink(contract)
        observable = self.client.subscribe_contract(contract)
        observable.subscribe(symbol_sink)
        self.symbol_subscriptions[contract.symbol] = symbol_sink

        symbol_sink.subscribe(ContractSinkObserver())

    def subscribe_symbol(self, symbol: str) -> Observable:
        symbol_sink = ContractSink(Helpers.equity(symbol))
        observable = self.client.subscribe_contract(Helpers.equity(symbol))
        observable.subscribe(symbol_sink)
        self.symbol_subscriptions[symbol] = symbol_sink

        observable.pipe(  # type: ignore
            Helpers.dict_transformer
        ).subscribe(ArcticObserver(symbol, self.library))

        observable.pipe(  # type: ignore
            Helpers.dict_transformer
        ).subscribe(self.messaging_publisher)
        return observable

    def subscribe_symbols(self, symbols: List[str]):
        for symbol in symbols:
            self.subscribe_symbol(symbol)

        # transform all the subscriptions into pandas transformers, then
        # zip to display to the console
        subscriptions = list(self.symbol_subscriptions.values())
        pd_subs = list(map(lambda sub: sub.pipe(Helpers.symbol_sink_to_pandas), subscriptions))  # type: ignore

        rx.with_latest_from(*pd_subs).pipe(  # type: ignore
            ops.map(lambda _tuple: pd.concat(list(_tuple)))  # type: ignore
        ).subscribe(ComplexConsoleObserver())

    def run(self):
        self.client.run()

@click.command()
@click.option('--symbols', required=True, help='filename or comma seperated list AAPL,TSLA')
@click.option('--ib_server_address', required=False, default='127.0.0.1', help='tws trader API address: 127.0.0.1')
@click.option('--ib_server_port', required=False, default=7496, help='port for tws server API: 7496')
@click.option('--ib_client_id', required=False, default=2, help='ib client id: 2')
@click.option('--arctic_server_address', required=False, default='localhost', help='arctic server ip address: localhost')
@click.option('--arctic_library', required=False, default='Ticks', help='tick store library name: Ticks')
@click.option('--publish_ip_address', required=False, default='127.0.0.1', help='zeromq publish address: 127.0.0.1')
@click.option('--publish_port', required=False, default=5001, help='zeromq publish port: 5001')
def main(symbols: str,
         ib_server_address: str,
         ib_server_port: int,
         ib_client_id: int,
         arctic_server_address: str,
         arctic_library: str,
         publish_ip_address: str,
         publish_port: int):

    all_symbols: List[str] = []
    if os.path.exists(symbols):
        with open(symbols) as f:
            for line in f.readlines():
                symbol = line.strip()
                all_symbols.append(symbol)
    elif ',' in symbols:
        all_symbols = symbols.split(',')
    else:
        all_symbols.append(symbols)

    loop = asyncio.get_event_loop()
    ib_listener = IBListener(arctic_server_address, arctic_library, publish_ip_address, publish_port, loop)
    ib_listener.connect(ib_server_address=ib_server_address, ib_server_port=ib_server_port, client_id=ib_client_id)
    ib_listener.subscribe_symbols(all_symbols)
    ib_listener.run()


if __name__ == '__main__':
    main()
