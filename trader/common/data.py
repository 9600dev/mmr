import os
from types import resolve_bases
import numpy as np
import pandas as pd
import vectorbt as vbt
import datetime as dt
from dateutil.tz import tzlocal, gettz
from dateutil.tz.tz import tzfile

from trader.common.logging_helper import setup_logging

logging = setup_logging(module_name='data')

from pandas.core.base import PandasObject
from arctic import Arctic, TICK_STORE, VERSION_STORE
from arctic.date import DateRange, string_to_daterange
from arctic.tickstore.tickstore import TickStore
from arctic.store.version_store import VersionStore
from arctic.exceptions import NoDataFoundException
from typing import Tuple, List, Optional, Dict, TypeVar, Generic, Type, Union, cast
from durations import Duration
from exchange_calendars import ExchangeCalendar
from pandas import DatetimeIndex
from ib_insync.contract import Contract
from trader.common.helpers import dateify, daily_close, daily_open, market_hours


class ContractMetadata():
    def __init__(self,
                 contract: Contract,
                 history_no_data_dates: List[dt.datetime],
                 history_overlapping_data_dates: List[dt.datetime]):
        self.contract = contract
        self.history_no_data_dates = history_no_data_dates
        self.history_overlapping_data = history_overlapping_data_dates

    def to_dict(self):
        return vars(self)

    def add_no_data(self, date_time: dt.datetime):
        self.history_no_data_dates.append(date_time)

    def add_overlapping_data(self, date_time: dt.datetime):
        self.history_overlapping_data.append(date_time)

    def has_been_crawled(self, date_time: dt.datetime):
        return date_time in self.history_no_data_dates or date_time in self.history_overlapping_data

    def __str__(self):
        return '{} {} {}'.format(self.contract, self.history_no_data_dates, self.history_overlapping_data)


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
    def fix_df_timezone(self, data_frame: pd.DataFrame):
        data_frame.index = data_frame.index.tz_convert(self.zone)  # type: ignore
        return data_frame

    def contract_to_symbol(self, contract: Contract) -> str:
        if type(contract) == int:
            return str(contract)
        return str(contract.conId)

    def symbol_to_contract(self, symbol: str) -> Contract:
        if type(symbol) is int or type(symbol) is np.int or type(symbol) is np.int64:
            return Contract(conId=int(symbol))
        if type(symbol) is str and symbol.isnumeric():
            return Contract(conId=int(symbol))
        raise ValueError('todo implement this')

    def get_date_range_from_datetime(self, day: dt.datetime):
        date_time = dateify(day)
        return DateRange(start=date_time, end=date_time + dt.timedelta(days=1))

    def read(self,
             contract: Contract,
             date_range: DateRange = DateRange(dt.datetime(1970, 1, 1), dt.datetime.now())) -> pd.DataFrame:
        try:
            return self.fix_df_timezone(self.library.read(self.contract_to_symbol(contract), date_range))
        except NoDataFoundException:
            return pd.DataFrame()

    def write(self,
              contract: Contract,
              data_frame: pd.DataFrame):
        self.library.write(self.contract_to_symbol(contract), data_frame)

    def delete(self,
               contract: Contract):
        self.library.delete(self.contract_to_symbol(contract))

    def list_symbols(self) -> List[str]:
        return self.library.list_symbols()

    def get_contract_from_csv(self,
                              contract_csv_file: str = '/home/trader/mmr/data/symbols_historical.csv'
                              ) -> pd.DataFrame:
        if not os.path.exists(contract_csv_file):
            raise ValueError('csv_file {} not found'.format(contract_csv_file))
        return pd.read_csv(contract_csv_file)

T = TypeVar('T')

class DictData(Data, Generic[T]):
    def __init__(self,
                 arctic_server_address: str = '127.0.0.1',
                 arctic_library: str = ''):
        if not arctic_library:
            raise ValueError('arctic_library must be supplied')
        super().__init__(arctic_server_address=arctic_server_address,
                         arctic_library=arctic_library,
                         lib_type=VERSION_STORE)

    def read(self,
             contract: Contract,
             date_range: DateRange = DateRange(dt.datetime(1970, 1, 1), dt.datetime.now())) -> Optional[T]:
        try:
            return self.library.read(self.contract_to_symbol(contract)).data
        except NoDataFoundException:
            return None

    def write(self, contract: Contract, data: T) -> None:
        logging.info('DictData writing contract {}'.format(contract))
        self.library.write(self.contract_to_symbol(contract), data, prune_previous_version=True)

    def delete(self, contract: Contract) -> None:
        logging.info('DictData deleting contract {}'.format(contract))
        self.library.delete(self.contract_to_symbol(contract))


class TickData(Data):
    def __init__(self,
                 arctic_server_address: str = '127.0.0.1',
                 arctic_library: str = 'Historical'):
        super().__init__(arctic_server_address=arctic_server_address,
                         arctic_library=arctic_library,
                         lib_type=TICK_STORE)
        self.metadata = DictData[ContractMetadata](arctic_server_address=arctic_server_address,
                                                   arctic_library=arctic_library + 'Metadata')
        PandasObject.daily_open = daily_open  # type: ignore
        PandasObject.daily_close = daily_close  # type: ignore
        PandasObject.market_hours = market_hours  # type: ignore
        logging.info('initializing TickData {} {}'.format(arctic_server_address, arctic_library))

    def date_summary(self, contract: Union[Contract, int]) -> Tuple[dt.datetime, dt.datetime]:
        if type(contract) is int:
            contract = Contract(conId=cast(int, contract))
        contract = cast(Contract, contract)
        min_date = dateify(self.library.min_date(symbol=self.contract_to_symbol(contract)), timezone=self.zone)
        max_date = dateify(self.library.max_date(symbol=self.contract_to_symbol(contract)), timezone=self.zone)
        return (min_date, max_date)

    def summary(self, contract: Union[Contract, int]) -> Tuple[dt.datetime, dt.datetime, pd.Series, pd.Series]:
        if type(contract) is int:
            contract = Contract(conId=cast(int, contract))

        contract = cast(Contract, contract)
        min_date = self.library.min_date(symbol=self.contract_to_symbol(contract))
        max_date = self.library.max_date(symbol=self.contract_to_symbol(contract))
        min_date_range = DateRange(min_date, min_date)
        max_date_range = DateRange(max_date, max_date + dt.timedelta(days=1))

        return (dateify(min_date, self.zone),
                dateify(max_date, self.zone),
                self.read(contract, date_range=min_date_range).iloc[0],
                self.read(contract, date_range=max_date_range).iloc[-1])

    def read_metadata(self, contract: Contract) -> ContractMetadata:
        metadata = self.metadata.read(contract)
        if not metadata:
            return ContractMetadata(contract=contract, history_no_data_dates=[], history_overlapping_data_dates=[])
        else:
            return metadata

    def write_metadata(self, contract: Contract, metadata: ContractMetadata):
        self.metadata.write(contract, metadata)

    def read(self,
             contract: Contract,
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
                 contract: Contract,
                 pd_offset: Optional[str] = None,
                 period: Optional[str] = None,
                 date_range: Optional[DateRange] = None) -> pd.DataFrame:
        actual_date_range: DateRange
        if period or date_range:
            actual_date_range = self.get_date_range(period, date_range)
        else:
            actual_date_range = DateRange(self.library.min_date(symbol=self.contract_to_symbol(contract)),
                                          self.library.max_date(symbol=self.contract_to_symbol(contract)))

        df = self.fix_df_timezone(self.library.read(symbol=self.contract_to_symbol(contract),
                                                    date_range=actual_date_range))
        if pd_offset:
            return df.resample(pd_offset).last()
        else:
            return df

    def history(self,
                contract: Contract,
                pd_offset: Optional[str] = None,
                period: Optional[str] = None,
                date_range: Optional[DateRange] = None) -> pd.DataFrame:
        return self.get_data(contract, pd_offset, period, date_range)

    def date_exists(self,
                    contract: Contract,
                    date_time: dt.datetime) -> bool:
        date_range = DateRange(date_time, date_time + dt.timedelta(days=1))
        try:
            result = self.get_data(contract, date_range=date_range)
            return len(result) > 0
        except NoDataFoundException:
            return False

    def missing(self,
                contract: Contract,
                exchange_calendar: ExchangeCalendar,
                pd_offset: Optional[str] = None,
                period: Optional[str] = None,
                date_range: Optional[DateRange] = None) -> List[DateRange]:
        if not pd_offset and not period and not date_range:
            date_range = DateRange(dateify(self.library.min_date(symbol=self.contract_to_symbol(contract))),
                                   dateify() - dt.timedelta(days=1))

        df = self.get_data(contract, pd_offset, period, date_range)

        no_data_dates: List[dt.date] = []
        contract_metadata = self.metadata.read(contract)
        if contract_metadata:
            no_data_dates = [d.date() for d in contract_metadata.history_no_data_dates]

        # make sure we have all the trading days
        dates: List[dt.date] = df.resample('D').first().dropna().index.date
        dates = dates + no_data_dates
        sessions = exchange_calendar.all_sessions.date  # type: ignore

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
        return ranges

    def dump(self, csv_file_location: str):
        for symbol in self.library.list_symbols():
            csv_filename = csv_file_location + '/' + symbol + '.csv'
            logging.info('writing {} to {}'.format(symbol, csv_filename))
            data_frame = self.history(self.symbol_to_contract(symbol))
            data_frame.index.name = 'date'
            data_frame.to_csv(csv_file_location + '/' + symbol + '.csv', header=True)
