# the fundamentals of asyncio: https://www.integralist.co.uk/posts/python-asyncio/

import os
import asyncio
import datetime
from re import I

from eventkit import event
import aiohttp
import datetime as dt
import aioreactive as rx
from aioreactive.subject import AsyncMultiSubject
from aioreactive.types import AsyncObservable, AsyncObserver
import pandas as pd
import ib_insync as ibapi
import backoff
import nest_asyncio
import random

from ib_insync.contract import ContractDescription, ContractDetails
from ib_insync import Stock, IB, Contract, Forex, BarData, Future, BarDataList, RealTimeBar
from ib_insync.objects import PortfolioItem, Position, RealTimeBarList
from ib_insync.util import df
from ib_insync.ticker import Ticker
from eventkit.event import Event

from asyncio import BaseEventLoop
from asyncio.events import AbstractEventLoop
from expression.core import pipe

from enum import Enum
from typing import (
    List,
    Dict,
    Tuple,
    Callable,
    Coroutine,
    Awaitable,
    Optional,
    Set,
    Generic,
    TypeVar,
    Union,
    cast,
)

from trader.common.listener_helpers import Helpers
from trader.common.logging_helper import setup_logging
from trader.common.reactive import AsyncCachedSubject, AsyncCachedObserver, awaitify, AsyncEventSubject

logging = setup_logging(module_name="ibaiorx")

class WhatToShow(Enum):
    def __str__(self):
        if self.value == 1: return 'TRADES'
        if self.value == 2: return 'MIDPOINT'
        if self.value == 3: return 'BID'
        if self.value == 4: return 'ASK'

    TRADES = 1
    MIDPOINT = 2
    BID = 3
    ASK = 4

class ReportType(Enum):
    def __str__(self):
        return self.name

    ReportsFinSummary = 1       # Financial summary
    ReportsOwnership = 2        # Company’s ownership
    ReportSnapshot = 3          # Company’s financial overview
    ReportsFinStatements = 4    # Financial Statements
    RESC = 5                    # Analyst Estimates
    CalendarReport = 6          # Company’s calendar


TAny = TypeVar('TAny')
TKey = TypeVar('TKey')
TValue = TypeVar('TValue')


class IBAIORx():
    client_id_counter = random.randint(5, 35)

    def __init__(
        self,
        ib_server_address: str,
        ib_server_port: int,
    ):
        self.ib_server_address = ib_server_address
        self.ib_server_port = ib_server_port

        nest_asyncio.apply()

        self.ib = IB()
        self.market_data_subject: rx.AsyncSubject[Ticker] = rx.AsyncSubject[Ticker]()

        def mapper(tickers: Set[Ticker]) -> rx.AsyncObservable[Ticker]:
            return rx.from_iterable(tickers)

        self.bars_data_subject = AsyncEventSubject[RealTimeBarList](eventkit_event=self.ib.barUpdateEvent)
        self.positions_subject = AsyncEventSubject[List[Position]](eventkit_event=self.ib.positionEvent)
        self.portfolio_subject = AsyncEventSubject[PortfolioItem](eventkit_event=self.ib.updatePortfolioEvent)
        self._contracts_source = AsyncEventSubject[Set[Ticker]](eventkit_event=self.ib.pendingTickersEvent)
        # we have to flatten from Set[Ticker] to a single stream of tickers
        # that each subscriber can filter on
        self.contracts_subject = pipe(
            self._contracts_source,
            rx.flat_map(mapper)
        )

        self.contracts_cache: Dict[Contract, rx.AsyncObservable] = {}
        self.bars_cache: Dict[Contract, rx.AsyncObservable[RealTimeBarList]] = {}

        # try binding helper methods to things we care about
        Contract.to_df = Helpers.to_df  # type: ignore

    def __handle_error(self, reqId, errorCode, errorString, contract):
        global error_code

        if errorCode == 2104 or errorCode == 2158 or errorCode == 2106:
            return

        logging.warning(
            "ibrx reqId: {} errorCode {} errorString {} contract {}".format(
                reqId, errorCode, errorString, contract
            )
        )

    @backoff.on_exception(backoff.expo, Exception, max_tries=3, max_time=30)
    def connect(self):
        # we often have client_id clashes, so try incrementally updating a static counter
        IBAIORx.client_id_counter += 1

        if self.__handle_error not in self.ib.errorEvent:
            self.ib.errorEvent += self.__handle_error

        self.ib.connect(
            self.ib_server_address, self.ib_server_port, clientId=IBAIORx.client_id_counter, timeout=10
        )

        return self

    def _filter_contract(self, contract: Contract, data) -> bool:
        if data.contract:
            return data.contract.conId == contract.conId
        else:
            logging.debug('_filter_contract failed, as there is no contract object')
            return False

    async def subscribe_contract(self, contract: Contract) -> rx.AsyncObservable:
        if contract in self.contracts_cache:
            return self.contracts_cache[contract]

        self._contracts_source.call_event_subscriber_sync(lambda: {self.ib.reqMktData(contract, "", False, False, None)})

        xs = pipe(
            self.contracts_subject,
            rx.filter(lambda ticker: self._filter_contract(contract, ticker)),  # type: ignore
        )

        self.contracts_cache[contract] = xs
        return xs

    def unsubscribe_contract(self, contract: Contract):
        raise ValueError('not implemented')

    async def subscribe_barlist(self,
                                contract: Contract,
                                wts: WhatToShow = WhatToShow.MIDPOINT) -> rx.AsyncObservable[RealTimeBarList]:
        # todo this method subscribes and populates a RealTimeBarsList object,
        # which I'm sure will end up being a memory leak
        bar_size = 5

        if contract in self.bars_cache:
            return self.bars_cache[contract]

        self.bars_data_subject.call_event_subscriber_sync(
            lambda: self.ib.reqRealTimeBars(contract, bar_size, str(wts), False)
        )

        xs = pipe(
            self.bars_data_subject,
            rx.filter(lambda bar_data_list: self._filter_contract(contract, bar_data_list)),  # type: ignore
        )

        self.bars_cache[contract] = xs
        return xs

    def unsubscribe_barlist(self, contract: Contract):
        if contract in self.bars_cache and self.bars_data_subject.get_value():
            self.ib.cancelRealTimeBars(cast(RealTimeBarList, self.bars_data_subject.get_value()))
            del self.bars_cache[contract]
        else:
            logging.debug('unsubscribe_barlist failed for {}'.format(contract))

    async def subscribe_positions(self) -> rx.AsyncObservable[List[Position]]:
        await self.positions_subject.call_event_subscriber(self.ib.reqPositionsAsync())
        return self.positions_subject

    async def subscribe_portfolio(self) -> rx.AsyncObservable[PortfolioItem]:
        portfolio_items = self.ib.portfolio()
        for item in portfolio_items:
            await self.portfolio_subject.asend(item)
        return self.portfolio_subject

    async def get_contract_details(self, contract: Contract) -> List[ContractDetails]:
        result = await self.ib.reqContractDetailsAsync(contract)
        if not result:
            return []
        else:
            return cast(List[ContractDetails], result)

    async def get_fundamental_data_sync(self,
                                        contract: Contract,
                                        report_type: ReportType = ReportType.ReportSnapshot) -> rx.AsyncObservable[str]:
        return rx.from_async(self.ib.reqFundamentalDataAsync(contract, reportType=str(report_type)))

    def get_matching_symbols(self, symbol: str) -> List[ContractDescription]:
        return self.ib.reqMatchingSymbols(symbol)

    def get_conid_sync(
        self,
        symbols: Union[str, List[str]],
        secType: str = "STK",
        primaryExchange: str = "SMART",
        currency: str = "USD",
    ) -> Union[Optional[Contract], List[Contract]]:
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

        def get_conid_helper(
            symbol: str, secType: str, primaryExchange: str, currency: str
        ) -> Optional[Contract]:
            contract_desc: List[ContractDescription] = self.get_matching_symbols(symbol)
            f: List[ContractDescription] = []
            if len(contract_desc) == 1 and contract_desc[0].contract:
                return contract_desc[0].contract
            elif len(contract_desc) > 0:
                if secType:
                    f = f + [
                        desc
                        for desc in contract_desc
                        if desc.contract and desc.contract.secType == secType
                    ]
                if currency:
                    f = f + [
                        desc
                        for desc in contract_desc
                        if desc.contract and desc.contract.currency == currency
                    ]
                if len(f) > 0:
                    return f[0].contract
                else:
                    return None
            else:
                logging.info("get_conid_helper for {} returned nothing".format(symbol))
                return None

        if type(symbols) is list:
            result = [
                get_conid_helper(symbol, secType, primaryExchange, currency)
                for symbol in symbols
            ]
            return [r for r in result if r]
        else:
            return get_conid_helper(str(symbols), secType, primaryExchange, currency)

    def get_contract_history(
        self,
        contract: Contract,
        start_date: dt.datetime,
        end_date: dt.datetime = dt.datetime.now(),
        bar_size_setting: str = "5 secs",
        to_pandas: bool = False,
    ) -> List[BarData]:
        dt = end_date
        bars_list = []

        while dt >= start_date:
            bars = self.ib.reqHistoricalData(
                contract,
                endDateTime=dt,
                durationStr="1 D",
                barSizeSetting=bar_size_setting,
                whatToShow="MIDPOINT",
                useRTH=True,
                formatDate=1,
            )
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
