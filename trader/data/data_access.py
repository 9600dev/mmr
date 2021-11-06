import os
from types import resolve_bases
import numpy as np
import pandas as pd
import vectorbt as vbt
import datetime as dt
from dataclasses import dataclass, fields
from dateutil.tz import tzlocal, gettz
from dateutil.tz.tz import tzfile

from trader.common.logging_helper import setup_logging

logging = setup_logging(module_name='data')

from pandas.core.base import PandasObject
from arctic import Arctic, TICK_STORE, VERSION_STORE
from arctic.date import DateRange, string_to_daterange
from arctic.tickstore.tickstore import TickStore
from arctic.store.version_store import VersionStore
from arctic.exceptions import NoDataFoundException, OverlappingDataException
from typing import Tuple, List, Optional, Dict, TypeVar, Generic, Type, Union, cast, Set
from durations import Duration
from exchange_calendars import ExchangeCalendar
from pandas import DatetimeIndex
from ib_insync.contract import Contract, ContractDetails
from trader.common.helpers import dateify, daily_close, daily_open, market_hours, get_contract_from_csv, symbol_to_contract
from trader.data.contract_metadata import ContractMetadata

# todo make sure all writes are monotonically ordered, as TickStore assumes this
# right now, I don't think this is the case

@dataclass
class SecurityDefinition:
    symbol: str
    exchange: str
    conId: int
    secType: str
    primaryExchange: str
    currency: str
    minTick: float
    orderTypes: str
    validExchanges: str
    priceMagnifier: float
    longName: str
    category: str
    subcategory: str
    tradingHours: str
    timeZoneId: str
    liquidHours: str
    stockType: str
    bondType: str
    couponType: str
    callable: bool
    putable: bool
    coupon: int
    convertable: bool
    maturity: str
    issueDate: str
    nextOptionDate: str
    nextOptionPartial: bool
    nextOptionType: str
    marketRuleIds: str
    company_name: str = ''
    industry: str = ''

    @staticmethod
    def from_contract_details(d: ContractDetails):
        return SecurityDefinition(
            symbol=d.contract.symbol if d.contract else '',
            exchange=d.contract.exchange if d.contract else '',
            conId=d.contract.conId if d.contract else -1,
            secType=d.contract.secIdType if d.contract else '',
            primaryExchange=d.contract.primaryExchange if d.contract else '',
            currency=d.contract.currency if d.contract else '',
            minTick=d.minTick,
            orderTypes=d.orderTypes,
            validExchanges=d.validExchanges,
            priceMagnifier=d.priceMagnifier,
            longName=d.longName,
            category=d.category,
            subcategory=d.subcategory,
            tradingHours=d.tradingHours,
            timeZoneId=d.timeZoneId,
            liquidHours=d.liquidHours,
            stockType=d.stockType,
            bondType=d.bondType,
            couponType=d.couponType,
            callable=d.callable,
            putable=d.putable,
            coupon=d.coupon,
            convertable=d.convertible,
            maturity=d.maturity,
            issueDate=d.issueDate,
            nextOptionDate=d.nextOptionDate,
            nextOptionPartial=d.nextOptionPartial,
            nextOptionType=d.nextOptionType,
            marketRuleIds=d.marketRuleIds,
            company_name=d.longName,
            industry=d.industry,
        )

class Data():
    def __init__(self,
                 arctic_server_address: str,
                 arctic_library: str,
                 lib_type: str,
                 timezone: str = 'America/New_York'):
        self.arctic_server_address = arctic_server_address
        self.arctic_library = arctic_library
        self.store = Arctic(self.arctic_server_address)
        self.store.initialize_library(self.arctic_library, lib_type=lib_type)
        # deliberately duck-typed because VersionStore and TickStore share no heirarchy
        self.library = self.store[self.arctic_library]
        self.zone: tzfile = gettz(timezone)  # type: ignore

    # arctic stupidly returns the read data in the users local timezone
    # todo:// patch this up properly in the arctic source
    def _fix_df_timezone(self, data_frame: pd.DataFrame):
        data_frame.index = data_frame.index.tz_convert(self.zone)  # type: ignore
        return data_frame

    def _to_symbol(self, contract: Union[Contract, SecurityDefinition, int]) -> str:
        if type(contract) == int:
            return str(contract)
        elif type(contract) is Contract:
            return str(cast(Contract, contract).conId)
        elif type(contract) is SecurityDefinition:
            definition = cast(SecurityDefinition, contract)
            return str(definition.conId)
        else:
            raise ValueError('cast not supported')

    def get_date_range_from_datetime(self, day: dt.datetime):
        date_time = dateify(day)
        return DateRange(start=date_time, end=date_time + dt.timedelta(days=1))

    def read(self,
             contract: Union[Contract, SecurityDefinition, int],
             date_range: DateRange = DateRange(dt.datetime(1970, 1, 1), dt.datetime.now())) -> pd.DataFrame:
        try:
            return self._fix_df_timezone(self.library.read(self._to_symbol(contract), date_range))
        except NoDataFoundException:
            return pd.DataFrame()

    def write(self,
              contract: Union[Contract, SecurityDefinition, int],
              data_frame: pd.DataFrame):
        self.library.write(self._to_symbol(contract), data_frame)

    def delete(self,
               contract: Union[Contract, SecurityDefinition, int]):
        self.library.delete(self._to_symbol(contract))

    def list_symbols(self) -> List[str]:
        return self.library.list_symbols()

    def arctic_list_libraries(self) -> List[str]:
        return self.store.list_libraries(1)

T = TypeVar('T')

class DictData(Data, Generic[T]):
    def __init__(self,
                 arctic_server_address: str,
                 arctic_library: str):
        if not arctic_library:
            raise ValueError('arctic_library must be supplied')
        super().__init__(arctic_server_address=arctic_server_address,
                         arctic_library=arctic_library,
                         lib_type=VERSION_STORE)

    def read(self,
             contract: Union[Contract, SecurityDefinition, int],
             date_range: DateRange = DateRange(dt.datetime(1970, 1, 1), dt.datetime.now())) -> Optional[T]:
        try:
            return self.library.read(self._to_symbol(contract)).data
        except NoDataFoundException:
            return None

    def write(self, contract: Union[Contract, SecurityDefinition, int], data: T) -> None:
        logging.info('DictData writing contract {}'.format(contract))
        self.library.write(self._to_symbol(contract), data, prune_previous_version=True)

    def delete(self, contract: Union[Contract, SecurityDefinition, int]) -> None:
        logging.info('DictData deleting contract {}'.format(contract))
        self.library.delete(self._to_symbol(contract))


class TickData(Data):
    def __init__(self,
                 arctic_server_address: str,
                 arctic_library: str):
        super().__init__(arctic_server_address=arctic_server_address,
                         arctic_library=arctic_library,
                         lib_type=TICK_STORE)
        self.metadata = DictData[ContractMetadata](arctic_server_address=arctic_server_address,
                                                   arctic_library=arctic_library + 'Metadata')
        PandasObject.daily_open = daily_open  # type: ignore
        PandasObject.daily_close = daily_close  # type: ignore
        PandasObject.market_hours = market_hours  # type: ignore
        logging.info('initializing TickData {} {}'.format(arctic_server_address, arctic_library))

    def get_schema(self) -> Set[str]:
        return {'date', 'open', 'high', 'low', 'close', 'volume', 'average', 'bar_count', 'bar_size'}

    def date_summary(self, contract: Union[Contract, SecurityDefinition, int]) -> Tuple[dt.datetime, dt.datetime]:
        if type(contract) is int:
            contract = Contract(conId=cast(int, contract))
        contract = cast(Contract, contract)
        min_date = dateify(self.library.min_date(symbol=self._to_symbol(contract)), timezone=self.zone)
        max_date = dateify(self.library.max_date(symbol=self._to_symbol(contract)), timezone=self.zone)
        return (min_date, max_date)

    def summary(
        self,
        contract: Union[Contract, SecurityDefinition, int]
    ) -> Tuple[dt.datetime, dt.datetime, pd.Series, pd.Series]:
        if type(contract) is int:
            contract = Contract(conId=cast(int, contract))

        contract = cast(Contract, contract)
        min_date = self.library.min_date(symbol=self._to_symbol(contract))
        max_date = self.library.max_date(symbol=self._to_symbol(contract))
        min_date_range = DateRange(min_date, min_date)
        max_date_range = DateRange(max_date, max_date + dt.timedelta(days=1))

        return (dateify(min_date, self.zone),
                dateify(max_date, self.zone),
                self.read(contract, date_range=min_date_range).iloc[0],
                self.read(contract, date_range=max_date_range).iloc[-1])

    def read_metadata(self, contract: Union[Contract, SecurityDefinition, int]) -> ContractMetadata:
        metadata = self.metadata.read(contract)
        if not metadata:
            return ContractMetadata(contract=Contract(conId=int(self._to_symbol(contract))),
                                    history_no_data_dates=[], history_overlapping_data_dates=[])
        else:
            return metadata

    def write_metadata(self, contract: Contract, metadata: ContractMetadata):
        self.metadata.write(contract, metadata)

    def write_resolve_overlap(
        self,
        contract: Union[Contract, SecurityDefinition, int],
        data_frame: pd.DataFrame
    ):
        try:
            super().write(contract, data_frame)
        except OverlappingDataException:
            # grab all the existing data
            # merge it with the data_frame
            # then rewrite it
            logging.debug('OverlappingDataException, re-writing dataframe')
            existing_data = self.read(contract)
            temp_df = existing_data.append(data_frame)
            result = cast(pd.DataFrame, temp_df[~temp_df.index.duplicated(keep='first')])
            result.sort_index(inplace=True)
            super().delete(contract=contract)
            self.write(contract=contract, data_frame=result)  # type: ignore

    def read(self,
             contract: Union[Contract, SecurityDefinition, int],
             date_range: DateRange = DateRange(dt.datetime(1970, 1, 1), dt.datetime.now())) -> pd.DataFrame:
        return self.get_data(contract, date_range=date_range)

    def get_date_range(self,
                       period: Optional[str] = None,
                       date_range: Optional[DateRange] = None) -> DateRange:
        if not period and not date_range:
            raise ValueError('period or date_range must be set')

        actual_date_range: DateRange
        if period:
            start_date = dt.datetime.now() - dt.timedelta(seconds=Duration(period).seconds)
            end_date = dt.datetime.now()
            actual_date_range = DateRange(start=start_date, end=end_date)
        elif date_range:
            actual_date_range = date_range
        else:
            raise ValueError('cannot get here')
        return actual_date_range

    # https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases
    def get_data(self,
                 contract: Union[Contract, SecurityDefinition, int],
                 pd_offset: Optional[str] = None,
                 period: Optional[str] = None,
                 date_range: Optional[DateRange] = None) -> pd.DataFrame:
        actual_date_range: DateRange
        if period or date_range:
            actual_date_range = self.get_date_range(period, date_range)
        else:
            actual_date_range = DateRange(self.library.min_date(symbol=self._to_symbol(contract)),
                                          self.library.max_date(symbol=self._to_symbol(contract)))

        try:
            df = self._fix_df_timezone(self.library.read(symbol=self._to_symbol(contract),
                                                         date_range=actual_date_range))
        except NoDataFoundException:
            return pd.DataFrame()

        if pd_offset:
            return df.resample(pd_offset).last()
        else:
            return df

    def history(self,
                contract: Union[Contract, SecurityDefinition, int],
                pd_offset: Optional[str] = None,
                period: Optional[str] = None,
                date_range: Optional[DateRange] = None) -> pd.DataFrame:
        return self.get_data(contract, pd_offset, period, date_range)

    def date_exists(self,
                    contract: Union[Contract, SecurityDefinition, int],
                    date_time: dt.datetime) -> bool:
        date_range = DateRange(date_time, date_time + dt.timedelta(days=1))
        try:
            result = self.get_data(contract, date_range=date_range)
            return len(result) > 0
        except NoDataFoundException:
            return False

    def missing(self,
                contract: Union[Contract, SecurityDefinition, int],
                exchange_calendar: ExchangeCalendar,
                pd_offset: Optional[str] = None,
                period: Optional[str] = None,
                date_range: Optional[DateRange] = None) -> List[DateRange]:
        if not pd_offset and not period and not date_range:
            date_range = DateRange(dateify(self.library.min_date(symbol=self._to_symbol(contract))),
                                   dateify() - dt.timedelta(days=1))

        df = self.get_data(contract, pd_offset, period, date_range)

        no_data_dates: List[dt.date] = []
        contract_metadata = self.metadata.read(contract)
        if contract_metadata:
            no_data_dates = [d.date() for d in contract_metadata.history_no_data_dates]

        dates: List[dt.date] = []
        sessions = exchange_calendar.all_sessions.date  # type: ignore
        # no data case
        if len(df) == 0:
            dates = no_data_dates
        else:
            # make sure we have all the trading days
            dates = df.resample('D').first().index.date
            dates = list(dates) + no_data_dates

        # filter
        actual_range = self.get_date_range(period, date_range)
        dates = [d for d in dates if d >= actual_range.start.date() and d <= actual_range.end.date()]
        sessions = [d for d in sessions if d >= actual_range.start.date() and d <= actual_range.end.date()]

        ranges: List[DateRange] = []
        start_date = None
        end_date = None
        for d in sessions:
            if d not in dates:
                if not start_date:
                    start_date = d
                end_date = d
            else:
                if start_date and end_date:
                    ranges.append(DateRange(start=start_date, end=end_date))
                    start_date = None
                    end_date = None
        if start_date and end_date:
            ranges.append(DateRange(start=start_date, end=end_date))

        return ranges

    def dump(self, csv_file_location: str):
        for symbol in self.library.list_symbols():
            csv_filename = csv_file_location + '/' + symbol + '.csv'
            logging.info('writing {} to {}'.format(symbol, csv_filename))
            data_frame = self.history(symbol_to_contract(symbol))
            data_frame.index.name = 'date'
            data_frame.to_csv(csv_file_location + '/' + symbol + '.csv', header=True)
