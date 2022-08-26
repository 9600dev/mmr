import datetime
import pandas as pd
import rx
import rx.disposable as disposable
import datetime as dt

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
from ib_insync import Stock, IB, Contract, Forex, BarData
from ib_insync.util import df
from ib_insync.ticker import Ticker
from trader.common.contract_sink import ContractSink

class NullObserver(Observer[ContractSink]):
    def on_next(self, data: ContractSink):
        return

    def on_completed(self):
        print('completed')

    def on_error(self, error):
        print('error')
        print(error)

class ArcticObserver(Observer[Dict]):
    def __init__(self, symbol: str, library: TickStore):
        self.symbol: str = symbol
        self.library = library

    def on_next(self, data):
        try:
            data['index'] = data['time']
            data.pop('time')
            self.library.write(self.symbol, [data])
        except Exception as ex:
            print(ex)

    def on_error(self, error):
        print(error)

    def on_completed(self):
        print('completed')

class ComplexConsoleObserver(Observer[pd.DataFrame]):
    def on_next(self, data: pd.DataFrame):
        try:
            print(chr(27) + "[2J")
            print(tabulate(data, headers=list(data.columns), tablefmt='psql'))
        except Exception as ex:
            print(ex)

    def on_completed(self):
        print('completed')

    def on_error(self, error):
        print('error')
        print(error)

class ContractSinkObserver(Observer[ContractSink]):
    def on_next(self, data: ContractSink):
        try:
            print(chr(27) + "[2J")
            print(tabulate(data.last(), headers=list(data.last().columns), tablefmt='psql'))
        except Exception as ex:
            print(ex)

    def on_completed(self):
        print('completed')

    def on_error(self, error):
        print('error')
        print(error)

class ConsoleObserver(Observer):
    def on_next(self, data):
        print(data)

    def on_completed(self):
        print('completed')

    def on_error(self, error):
        print('error')
        print(error)
