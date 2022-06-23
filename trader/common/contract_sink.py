import datetime
import ib_insync as ibapi
import asyncio
import pandas as pd
import rx
import rx.disposable as disposable
import datetime as dt
import threading
import itertools
import functools
import os

from arctic import Arctic, TICK_STORE
from tabulate import tabulate
from enum import Enum
from typing import List, Dict, Tuple, Callable, Optional, Set, Generic, TypeVar, cast, Union
from asyncio import BaseEventLoop
from rx import operators as ops
from rx.subject import Subject
from rx.scheduler import ThreadPoolScheduler, CatchScheduler, CurrentThreadScheduler
from rx.scheduler.eventloop import AsyncIOThreadSafeScheduler
from rx.core.typing import Observable, Observer, Disposable, Scheduler, OnNext, OnError, OnCompleted
from ib_insync import Stock, IB, Contract, Forex, Future
from ib_insync.ticker import Ticker

class ContractSink(rx.Observable, Observer):
    def __init__(self, contract: Contract):
        super(rx.Observable, self).__init__()
        super(Observer, self).__init__()
        self.contract: Contract = contract
        self.latest_tick: Ticker = Ticker()
        self.latest_df: pd.DataFrame = pd.DataFrame()
        self.last_tick: Ticker = Ticker()
        self.last_df: pd.DataFrame = pd.DataFrame()
        self.data_frame: pd.DataFrame = pd.DataFrame()
        self.subject = Subject()

    def subscribe(self,
                  observer: Optional[Union[Observer, OnNext]] = None,
                  on_error: Optional[OnError] = None,
                  on_completed: Optional[OnCompleted] = None,
                  on_next: Optional[OnNext] = None,
                  *,
                  scheduler: Optional[Scheduler] = None,
                  ) -> Disposable:
        return self.subject.subscribe(observer)

    def _subscribe(self, observer):
        return self.subject

    def dispose(self):
        self.subject.dispose()

    # todo this is a duplicate from the Helpers class because we can't have circular imports
    def symbol_from_contract(self, contract: Contract) -> int:
        return contract.conId
        # if type(contract) is Forex:
        #     return contract.symbol + contract.currency
        # if type(contract) is Stock:
        #     return contract.symbol + contract.currency
        # if type(contract) is Future:
        #     return contract.symbol
        # else:
        #     raise ValueError('not implemented')

    def df(self):
        return self.data_frame

    def latest_tick_df(self):
        return self.latest_df

    # todo this is a duplicate from the Helpers class because we can't have circular imports
    def df_from_ticker(self, t: Ticker) -> pd.DataFrame:
        symbol = 0
        if t.contract:
            symbol = self.symbol_from_contract(t.contract)

        return pd.DataFrame([[symbol,
                              t.time,
                              t.marketDataType,
                              t.bid,
                              t.bidSize,
                              t.ask,
                              t.askSize,
                              t.last,
                              t.lastSize,
                              t.prevBid,
                              t.prevBidSize,
                              t.prevAsk,
                              t.prevAskSize,
                              t.prevLast,
                              t.prevLastSize,
                              t.volume,
                              t.vwap,
                              t.halted]],
                            columns=['contract', 'time', 'marketDataType', 'bid', 'bidSize', 'ask', 'askSize', 'last',
                                     'lastSize', 'prevBid', 'prevBidSize', 'prevAsk', 'prevAskSize', 'prevLast',
                                     'prevLastSize', 'volume', 'vwap', 'halted'])

    def last(self):
        # return pd.DataFrame(self.data_frame.iloc[-1])
        return self.data_frame.tail(1)

    def on_next(self, tick: Ticker):
        self.last_tick = self.latest_tick
        self.last_df = self.latest_df

        self.latest_tick = tick
        self.latest_df = self.df_from_ticker(tick)
        if len(self.data_frame) == 0:
            self.data_frame = self.latest_df
        else:
            self.data_frame = self.data_frame.append(self.latest_df, ignore_index=True)
        self.subject.on_next(self)

    def on_completed(self):
        print('completed')

    def on_error(self, error):
        print(error)

    def __str__(self):
        return str(self.last())

    def pipe(self, *operators: Callable[[Observable], Observable]) -> Observable:  # type: ignore
        return self.subject.pipe(*operators)
