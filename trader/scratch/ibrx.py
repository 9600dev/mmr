import datetime
import ib_insync as ibapi
import asyncio
from ib_insync.contract import ContractDescription, ContractDetails
import pandas as pd
import rx
import rx.disposable as disposable
import datetime as dt
import time
import random
import threading
import itertools
import functools
import backoff

from enum import Enum
from typing import List, Dict, Tuple, Callable, Optional, Set, Generic, TypeVar, Union, cast
from asyncio import BaseEventLoop
from rx import operators as ops
from rx.subject import Subject
from rx.scheduler import ThreadPoolScheduler, CatchScheduler, CurrentThreadScheduler
from rx.scheduler.periodicscheduler import PeriodicScheduler
from rx.scheduler.eventloop import AsyncIOThreadSafeScheduler
from rx.core.typing import Observable, Observer, Disposable, Scheduler
from ib_insync import Stock, IB, Contract, Forex, BarData, Future, Position
from ib_insync.util import df
from ib_insync.ticker import Ticker

from trader.common.listener_helpers import Helpers
from trader.common.logging_helper import setup_logging

logging = setup_logging(module_name='ibrx')

class IBRx():
    client_id_counter = 2
    error_code = 0

    def __init__(
        self,
        ib: Optional[IB] = None,
        scheduler: Optional[PeriodicScheduler] = None,
    ):
        # self.event_loop = event_loop
        self.scheduler: Optional[PeriodicScheduler] = scheduler
        self.connected: bool = False
        self.ib: IB
        if ib:
            self.ib = ib
        else:
            self.ib = IB()
        self.market_data_subject = Subject()
        self.polling_loop_subject = Subject()
        self.contracts: Dict[int, Observable] = {}
        self.ticker_cache: Dict[int, Tuple[dt.datetime, Ticker]] = {}

        # try binding helper methods to things we care about
        Contract.to_df = Helpers.to_df  # type: ignore

    def __handle_error(self, reqId, errorCode, errorString, contract):
        global error_code

        if errorCode == 2104 or errorCode == 2158 or errorCode == 2106:
            return

        logging.warning('ibrx reqId: {} errorCode {} errorString {} contract {}'.format(reqId,
                                                                                        errorCode,
                                                                                        errorString,
                                                                                        contract))

    @backoff.on_exception(backoff.expo, Exception, max_tries=3, max_time=30)
    def connect(self,
                ib_server_address: str = '127.0.0.1',
                ib_server_port: int = 7496,
                client_id: Optional[int] = None):
        # we often have client_id clashes, so try incrementally updating a static counter
        IBRx.client_id_counter += 1
        if not client_id:
            client_id = IBRx.client_id_counter

        if self.__handle_error not in self.ib.errorEvent:
            self.ib.errorEvent += self.__handle_error

        self.ib.connect(ib_server_address, ib_server_port, clientId=client_id, timeout=9)
        self.ib.pendingTickersEvent += self.on_pending_tickers
        return self

    def on_pending_tickers(self, tickers: Set[Ticker]):
        for ticker in tickers:
            date_time = dt.datetime.now()
            if ticker.contract and Helpers.symbol(ticker.contract) in self.ticker_cache:
                self.ticker_cache[Helpers.symbol(ticker.contract)] = (date_time, ticker)  # type: ignore
            self.market_data_subject.on_next(ticker)

    def _filter_ticker(self, contract: Contract, ticker: Ticker) -> bool:
        if not ticker.contract:
            return False
        else:
            return ticker.contract.conId == contract.conId

    def subscribe_contract(self,
                           contract: Contract,
                           transformer: Callable[[Observable], Observable] = Helpers.noop_transformer) -> Observable:
        if Helpers.symbol(contract) in self.contracts:
            return self.contracts[Helpers.symbol(contract)]

        ticker: Ticker = self.ib.reqMktData(contract, '', False, False, None)

        obs = self.market_data_subject.pipe(
            ops.filter(lambda ticker: self._filter_ticker(contract, ticker)),  # type: ignore
            transformer
        )

        self.contracts[Helpers.symbol(contract)] = obs
        return obs

    def unsubscribe_contract(self, contract: Contract):
        ticker = self.ib.reqMktData(contract)
        del self.contracts[Helpers.symbol(contract)]

    def subscribe_contract_timedelta(self,
                                     contract: Contract,
                                     refresh_period: dt.timedelta,
                                     transformer: Callable[[Observable], Observable] = Helpers.noop_transformer) -> Observable:
        def poll_cache(symbol: int) -> None:
            (date_time, ticker) = self.ticker_cache[symbol]
            self.polling_loop_subject.on_next(ticker)

        if self.scheduler:
            if Helpers.symbol(contract) not in self.contracts:
                self.subscribe_contract(contract)
            if Helpers.symbol(contract) not in self.ticker_cache:
                self.ticker_cache[Helpers.symbol(contract)] = (dt.datetime.now(), Ticker())

            self.scheduler.schedule_periodic(refresh_period, lambda x: poll_cache(Helpers.symbol(contract)))
            return self.polling_loop_subject.pipe(
                ops.filter(lambda ticker: self._filter_ticker(contract, ticker))  # type: ignore
            )

        else:
            raise ValueError('self.scheduler not set. Set a scheduler to poll periodically')

    def get_contract_details(self,
                             contract: Contract) -> List[ContractDetails]:
        result = self.ib.reqContractDetails(contract)
        if not result:
            return []
        else:
            return cast(List[ContractDetails], result)

    def get_matching_symbols(self, symbol: str) -> List[ContractDescription]:
        return self.ib.reqMatchingSymbols(symbol)

    def get_fundamental_data_sync(self, contract: Contract, report_type: str):
        return self.ib.reqFundamentalData(contract, reportType=report_type)

    def get_positions(self) -> List[Position]:
        return self.ib.positions()

    def get_conid_sync(self,
                       symbols: Union[str, List[str]],
                       secType: str = 'STK',
                       primaryExchange: str = 'SMART',
                       currency: str = 'USD') -> Union[Optional[Contract], List[Contract]]:
        """
        Args:
            secType (str): the security type
            * 'STK' = Stock (or ETF)
            * 'OPT' = Option
            * 'FUT' = Future
            * 'IND' = Index
            * 'FOP' = Futures option
            * 'CASH' = Forex pair
            * 'CFD' = CFD
            * 'BAG' = Combo
            * 'WAR' = Warrant
            * 'BOND'= Bond
            * 'CMDTY'= Commodity
            * 'NEWS' = News
            * 'FUND'= Mutual fund
        """
        def get_conid_helper(symbol: str, secType: str, primaryExchange: str, currency: str) -> Optional[Contract]:
            contract_desc: List[ContractDescription] = self.get_matching_symbols(symbol)
            f: List[ContractDescription] = []
            if len(contract_desc) == 1 and contract_desc[0].contract:
                return contract_desc[0].contract
            elif len(contract_desc) > 0:
                if secType:
                    f = f + [desc for desc in contract_desc if desc.contract and desc.contract.secType == secType]
                if currency:
                    f = f + [desc for desc in contract_desc if desc.contract and desc.contract.currency == currency]
                if len(f) > 0:
                    return f[0].contract
                else:
                    return None
            else:
                logging.info('get_conid_helper for {} returned nothing'.format(symbol))
                return None
        if type(symbols) is list:
            result = [get_conid_helper(symbol, secType, primaryExchange, currency) for symbol in symbols]
            return [r for r in result if r]
        else:
            return get_conid_helper(str(symbols), secType, primaryExchange, currency)

    def get_contract_history(self,
                             contract: Contract,
                             start_date: dt.datetime,
                             end_date: dt.datetime = dt.datetime.now(),
                             bar_size_setting: str = '5 secs',
                             to_pandas: bool = False) -> List[BarData]:
        dt = end_date
        bars_list = []

        while dt >= start_date:
            bars = self.ib.reqHistoricalData(
                contract,
                endDateTime=dt,
                durationStr='1 D',
                barSizeSetting=bar_size_setting,
                whatToShow='MIDPOINT',
                useRTH=True,
                formatDate=1)
            if not bars:
                break
            for bar in bars:
                bars_list.append(bar)
            dt = bars[0].date

        if to_pandas:
            return df(bars_list)
        else:
            return bars_list

    def sleep(self, seconds: float):
        self.ib.sleep(seconds)

    def run(self):
        self.ib.run()

    def client(self):
        return self.ib
