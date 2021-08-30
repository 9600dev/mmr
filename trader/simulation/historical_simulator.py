import datetime
import ib_insync as ibapi
import asyncio
import pandas as pd
import rx
import rx.disposable as disposable
import datetime as dt
import numpy as np
import time
import logging
import coloredlogs
import random
import threading
import itertools
import functools

from enum import Enum
from typing import List, Dict, Tuple, Callable, Optional, Set, Generic, TypeVar, cast
from asyncio import BaseEventLoop
from rx import operators as ops
from rx.subject import Subject
from rx.scheduler import ThreadPoolScheduler, CatchScheduler, CurrentThreadScheduler
from rx.scheduler.eventloop import AsyncIOThreadSafeScheduler
from rx.core.typing import Observable, Observer, Disposable, Scheduler
from ib_insync import Stock, IB, Contract, Forex, BarData, TagValue
from ib_insync.util import df
from ib_insync.ticker import Ticker
from arctic import Arctic, TICK_STORE
from arctic.tickstore.tickstore import TickStore
from arctic.date import DateRange
from trader.common.listener_helpers import Helpers
from eventkit import Event

class HistoricalIB():
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.store: Arctic = Arctic('localhost')
        self.library: TickStore
        self.ib = IB()
        self.connect()
        self.set_simulation_parameters(start_date=dt.datetime(2020, 8, 27), end_date=dt.datetime.now())

    def set_simulation_parameters(self,
                                  start_date: dt.datetime,
                                  end_date: dt.datetime,
                                  real_time: bool = False):
        self.start_date = start_date
        self.end_date = end_date
        self.real_time = real_time
        self.available_symbols: List = self.library.list_symbols()
        self.subscribed_contracts: Dict[str, dt.datetime] = {}
        self.current_dt: dt.datetime = start_date

    def connect(self, host: str = '127.0.0.1', port: int = 7496, clientId: int = 1,
                timeout: float = 4, readonly: bool = False, account: str = ''):
        self.logger.info('connect')
        self._createEvents()
        self.store.initialize_library('Historical', lib_type=TICK_STORE)
        self.library = self.store['Historical']

    def _createEvents(self):
        self.pendingTickersEvent = Event('pendingTickersEvent')

    def reqMktData(
            self, contract: Contract, genericTickList: str = '',
            snapshot: bool = False, regulatorySnapshot: bool = False,
            mktDataOptions: List[TagValue] = None) -> Ticker:
        self.logger.info('reqMktData {}'.format(contract))
        if contract.symbol not in self.available_symbols:
            raise ValueError('symbol not available {}'.format(contract.symbol))
        if contract.symbol in self.subscribed_contracts:
            raise ValueError('already requested symbol {}'.format(contract.symbol))

        self.subscribed_contracts[contract.symbol] = self.current_dt

        # todo: return tickers
        return Ticker()

    def sleep(self, value: float):
        return self.ib.sleep(value)

    def run(self):
        asyncio.get_event_loop().run_until_complete(self.loop_worker())
        # return self.ib.run()

    async def loop_worker(self):
        def wrap_row(row: pd.Series, prev_ticker: Ticker):
            ticker = Ticker()
            # ticker.date = row['date']
            ticker.contract = Stock(symbol=row['symbol'], exchange='SMART', currency='USD')
            ticker.time = row['date']
            ticker.ask = row['open_ask']
            ticker.bid = row['open_bid']
            ticker.last = row['close_trades']
            ticker.volume = row['volume_trades'] + prev_ticker.volume
            ticker.prevAsk = prev_ticker.ask
            ticker.prevBid = prev_ticker.bid
            ticker.bidSize = 10.0
            ticker.askSize = 10.0
            ticker.prevBidSize = ticker.volume - prev_ticker.volume
            ticker.prevAskSize = ticker.volume - prev_ticker.volume
            ticker.prevLast = prev_ticker.last
            ticker.prevLastSize = ticker.volume - prev_ticker.volume
            ticker.halted = 0.0
            ticker.vwap = np.nan
            ticker.lastSize = row['volume_trades']  # todo:// this is wrong
            ticker.contract = Stock(row['symbol'], currency='USD', exchange='SMART')
            return ticker

        # grab a day's worth of data for each subscribed contract
        data_frames: List[pd.DataFrame] = []
        start_date = self.current_dt
        timer = time.time()

        prev_tickers: Dict[str, Ticker] = {}

        for symbol in self.subscribed_contracts.keys():
            data = self.library.read(symbol, date_range=DateRange(start_date, start_date + dt.timedelta(days=1)))
            data['symbol'] = symbol
            data['date'] = data.index
            prev_tickers[symbol] = Ticker()
            prev_tickers[symbol].volume = 0.0
            data_frames.append(data)

        total_data: pd.DataFrame = pd.concat(data_frames).sort_index()
        logging.debug('loop_worker total data: {}'.format(len(total_data)))
        counter = 0

        logging.debug('loop_worker total time: {}'.format(time.time() - timer))
        timer = time.time()
        previous_day = 0

        for timestamp, group in total_data.groupby(total_data.index):
            ticker_list = []
            if timestamp.to_pydatetime().day != previous_day:
                print('current day: {}'.format(timestamp))
                print(time.time() - timer)
                previous_day = timestamp.to_pydatetime().day

            # group is the "symbols" we're subscribed to
            # index is the time (in historical case, 5 seconds)
            for index, row in group.iterrows():
                symbol = row['symbol']
                prev_ticker = prev_tickers[symbol]
                ticker = wrap_row(row, prev_ticker)
                ticker_list.append(ticker)
                prev_tickers[symbol] = ticker
                counter = counter + 1
            self.pendingTickersEvent.emit(set(ticker_list))

        print(time.time() - timer)
        print('done')
        return 0
