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

from trader.common.logging_helper import setup_logging
logging = setup_logging(module_name='trader')

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
# from trader.listeners.historical_simulator import HistoricalIB
from trader.common.contract_sink import ContractSink
from trader.common.listener_helpers import Helpers
from trader.common.observers import ConsoleObserver, ArcticObserver, ComplexConsoleObserver, ContractSinkObserver, NullObserver
from trader.common.data import TickData


class Trader():
    def __init__(self,
                 ib_client,
                 simulation: bool,
                 arctic_server_address: str,
                 arctic_library: str):
        self.client: IBRx = IBRx(ib=ib_client, scheduler=AsyncIOThreadSafeScheduler(asyncio.get_event_loop()))
        self.data: TickData = TickData(arctic_server_address, arctic_library)
        self.contract_subscriptions: Dict[int, ContractSink] = {}
        self.simulation: bool = simulation

    @backoff.on_exception(backoff.expo, ConnectionRefusedError, max_tries=10, max_time=120)
    def connect(self,
                ib_server_address: str,
                ib_server_port: int,
                client_id: int):
        self.client.connect(server_address=ib_server_address, port=ib_server_port, client_id=client_id)

    def console_contract(self, contract: Contract):
        if contract.symbol in self.contract_subscriptions:
            return self.contract_subscriptions[contract.conId].subscribe(ConsoleObserver())

        contract_sink = ContractSink(contract)
        observable = self.client.subscribe_contract(contract)
        observable.subscribe(contract_sink)
        self.contract_subscriptions[contract.conId] = contract_sink

        contract_sink.subscribe(ContractSinkObserver())

    def subscribe_contracts(self, csv_file: str):
        contracts = Helpers.contracts_from_df(self.data.get_contract_from_csv(csv_file))

        for contract in contracts:
            print(contract)
            # subscribe
            contract_sink = ContractSink(contract)
            observable = self.client.subscribe_contract(contract)
            observable.subscribe(contract_sink)
            self.contract_subscriptions[contract.conId] = contract_sink

            # if not self.simulation:
            #     observable.pipe(  # type: ignore
            #         Helpers.dict_transformer
            #     ).subscribe(ArcticObserver(str(contract.conId), self.arctic_ticks))

        # transform all the subscriptions into pandas transformers, then
        # zip to display to the console
        subscriptions = list(self.contract_subscriptions.values())
        pd_subs = list(map(lambda sub: sub.pipe(Helpers.contract_sink_to_pandas), subscriptions))  # type: ignore

        # with_latest_from only produces if the first observable produces
        # rx.with_latest_from(*pd_subs).pipe(  # type: ignore
        rx.combine_latest(*pd_subs).pipe(   # type: ignore
            ops.map
            (
                lambda _tuple: pd.concat(list(_tuple))  # type: ignore
            )
        ).subscribe(ComplexConsoleObserver())
        # ).subscribe(NullObserver())

    def run(self):
        self.client.run()

@click.command()
@click.option('--simulation', required=False, default=False, help='load with historical data')
@click.option('--contract_csv_file', required=False,
              default='/home/trader/mmr/data/subscribed.csv', help='csv to subscribe')
@click.option('--ib_server_address', required=False, default='127.0.0.1', help='tws trader api address')
@click.option('--ib_server_port', required=False, default=7496, help='port for tws server api')
@click.option('--arctic_server_address', required=False, default='127.0.0.1', help='arctic server ip address: 127.0.0.1')
@click.option('--arctic_library', required=False, default='Historical', help='tick store library name: Historical')
def main(simulation: bool,
         contract_csv_file: str,
         ib_server_address: str,
         ib_server_port: int,
         arctic_server_address: str,
         arctic_library: str):
    if simulation:
        ib_client = HistoricalIB(logger=logging)
    else:
        ib_client = IB()  # type: ignore

    trader = Trader(ib_client,
                    simulation=simulation,
                    arctic_server_address=arctic_server_address,
                    arctic_library=arctic_library)
    trader.connect(ib_server_address=ib_server_address, ib_server_port=ib_server_port, client_id=10)
    trader.subscribe_contracts(contract_csv_file)
    trader.run()


if __name__ == '__main__':
    main()
