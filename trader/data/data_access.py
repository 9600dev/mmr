from dataclasses import dataclass
from dateutil.tz import gettz
from dateutil.tz.tz import tzfile
from durations import Duration
from exchange_calendars import ExchangeCalendar
from ib_async.contract import Contract, ContractDetails
from pandas.core.base import PandasObject
from trader.common.helpers import daily_close, daily_open, dateify, market_hours, symbol_to_contract
from trader.common.logging_helper import setup_logging
from trader.data.store import DateRange
from trader.data.duckdb_store import DuckDBDataStore, DuckDBObjectStore, _default_db_path
from trader.objects import BarSize
from typing import cast, Generic, List, NamedTuple, Optional, Set, Tuple, TypeVar, Union

import datetime as dt
import pandas as pd


logging = setup_logging(module_name='data')

# todo make sure all writes are monotonically ordered, as TickStore assumes this
# right now, I don't think this is the case


class PortfolioSummary(NamedTuple):
    contract: Contract
    position: float
    marketPrice: float
    marketValue: float
    averageCost: float
    unrealizedPNL: float
    realizedPNL: float
    account: str
    dailyPNL: float


@dataclass(eq=True, frozen=True)
class SecurityDefinition:
    symbol: str
    exchange: str
    conId: int
    secType: str
    primaryExchange: str
    currency: str
    tradingClass: str
    includeExpired: bool
    secIdType: str
    secId: str
    description: str
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
    minSize: float
    sizeIncrement: float
    suggestedSizeIncrement: float
    bondType: str
    couponType: str
    callable: bool
    putable: bool
    coupon: float
    convertable: bool
    maturity: str
    issueDate: str
    nextOptionDate: str
    nextOptionPartial: bool
    nextOptionType: str
    marketRuleIds: str
    company_name: str = ''
    industry: str = ''
    contractMonth: str = ''

    @staticmethod
    def from_contract_details(d: ContractDetails):
        return SecurityDefinition(
            symbol=d.contract.symbol if d.contract else '',
            exchange=d.contract.exchange if d.contract else '',
            conId=d.contract.conId if d.contract else -1,
            secType=d.contract.secType if d.contract else '',
            primaryExchange=d.contract.primaryExchange if d.contract else '',
            currency=d.contract.currency if d.contract else '',
            tradingClass=d.contract.tradingClass if d.contract else '',
            includeExpired=d.contract.includeExpired if d.contract else False,
            secIdType=d.contract.secIdType if d.contract else '',
            secId=d.contract.secId if d.contract else '',
            description=d.contract.description if d.contract else '',
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
            minSize=d.minSize,
            sizeIncrement=d.sizeIncrement,
            suggestedSizeIncrement=d.suggestedSizeIncrement,
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
            contractMonth=d.contractMonth,
        )

    @staticmethod
    def to_contract(definition: Union['SecurityDefinition', Contract]) -> Contract:
        if isinstance(definition, SecurityDefinition):
            contract = Contract(secType=definition.secType, conId=definition.conId, symbol=definition.symbol,
                                currency=definition.currency, exchange=definition.exchange,
                                primaryExchange=definition.primaryExchange)
            return contract
        elif isinstance(definition, Contract):
            return definition
        else:
            raise ValueError('unable to cast type to Contract')


@dataclass(eq=True, frozen=True)
class MMRBarData():
    date: dt.datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    average: float
    bar_count: int
    bar_size: int
    what_to_show: int

    @staticmethod
    def has_schema(data_frame: pd.DataFrame) -> bool:
        for column in MMRBarData.__annotations__.keys():
            if column not in data_frame.columns:
                return False
        return True

class IBTradeConfirm(NamedTuple):
    accountId: str
    acctAlias: str
    model: str
    currency: str
    assetCategory: str
    symbol: str
    description: str
    conid: str
    securityID: str
    securityIDType: str
    cusip: str
    isin: str
    listingExchange: str
    underlyingConid: str
    underlyingSymbol: str
    underlyingSecurityID: str
    underlyingListingExchange: str
    issuer: str
    multiplier: str
    strike: str
    expiry: str
    putCall: str
    principalAdjustFactor: str
    transactionType: str
    tradeID: str
    orderID: str
    execID: str
    brokerageOrderID: str
    orderReference: str
    volatilityOrderLink: str
    clearingFirmID: str
    origTradePrice: str
    origTradeDate: str
    origTradeID: str
    orderTime: str
    dateTime: str
    reportDate: str
    settleDate: str
    tradeDate: str
    exchange: str
    buySell: str
    quantity: str
    price: str
    amount: str
    proceeds: str
    commission: str
    brokerExecutionCommission: str
    brokerClearingCommission: str
    thirdPartyExecutionCommission: str
    thirdPartyClearingCommission: str
    thirdPartyRegulatoryCommission: str
    otherCommission: str
    commissionCurrency: str
    tax: str
    code: str
    orderType: str
    levelOfDetail: str
    traderID: str
    isAPIOrder: str
    allocatedTo: str
    accruedInt: str
    rfqID: str
    serialNumber: str
    deliveryType: str
    commodityType: str
    fineness: str
    weight: str


class IBChangeInDividendAccrual(NamedTuple):
    accountId: str
    acctAlias: str
    model: str
    currency: str
    fxRateToBase: str
    assetCategory: str
    symbol: str
    description: str
    conid: str
    securityID: str
    securityIDType: str
    cusip: str
    isin: str
    listingExchange: str
    underlyingConid: str
    underlyingSymbol: str
    underlyingSecurityID: str
    underlyingListingExchange: str
    issuer: str
    multiplier: str
    strike: str
    expiry: str
    putCall: str
    principalAdjustFactor: str
    reportDate: str
    date: str
    exDate: str
    payDate: str
    quantity: str
    tax: str
    fee: str
    grossRate: str
    grossAmount: str
    netAmount: str
    code: str
    fromAcct: str
    toAcct: str


class IBInterestAccrualsCurrency(NamedTuple):
    accountId: str
    acctAlias: str
    model: str
    currency: str
    fromDate: str
    toDate: str
    startingAccrualBalance: str
    interestAccrued: str
    accrualReversal: str
    fxTranslation: str
    endingAccrualBalance: str


class IBSLBFee(NamedTuple):
    accountId: str
    acctAlias: str
    model: str
    currency: str
    fxRateToBase: str
    assetCategory: str
    symbol: str
    description: str
    conid: str
    securityID: str
    securityIDType: str
    cusip: str
    isin: str
    listingExchange: str
    underlyingConid: str
    underlyingSymbol: str
    underlyingSecurityID: str
    underlyingListingExchange: str
    issuer: str
    multiplier: str
    strike: str
    expiry: str
    putCall: str
    principalAdjustFactor: str
    valueDate: str
    startDate: str
    type: str
    exchange: str
    quantity: str
    collateralAmount: str
    feeRate: str
    fee: str
    carryCharge: str
    ticketCharge: str
    totalCharges: str
    marketFeeRate: str
    grossLendFee: str
    netLendFeeRate: str
    netLendFee: str
    code: str
    fromAcct: str
    toAcct: str


T = TypeVar('T')


class Data():
    def __init__(self,
                 duckdb_path: str,
                 library_name: str,
                 lib_type: str = '',
                 timezone: str = 'America/New_York'):
        self.duckdb_path = duckdb_path
        self.library_name = library_name
        if duckdb_path and (
            duckdb_path.endswith('.duckdb')
            or duckdb_path.startswith('/')
            or duckdb_path.startswith('~')
        ):
            db_path = duckdb_path
        else:
            db_path = _default_db_path()
        self.library = DuckDBDataStore(db_path)
        self.zone: tzfile = gettz(timezone)  # type: ignore

    def _fix_df_timezone(self, data_frame: pd.DataFrame):
        if data_frame.empty:
            return data_frame
        if data_frame.index.tz is not None:
            data_frame.index = data_frame.index.tz_convert(self.zone)  # type: ignore
        return data_frame

    def _to_symbol(self, contract: Union[Contract, SecurityDefinition, int, str]) -> str:
        if type(contract) is str:
            return contract
        elif type(contract) == int:
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
        result = self.library.read(self._to_symbol(contract), start=date_range.start, end=date_range.end)
        if result.empty:
            return pd.DataFrame()
        return self._fix_df_timezone(result)

    def write(self,
              contract: Union[Contract, SecurityDefinition, int],
              data_frame: pd.DataFrame):
        self.library.write(self._to_symbol(contract), data_frame)

    def delete(self,
               contract: Union[Contract, SecurityDefinition, int]):
        self.library.delete(self._to_symbol(contract))

    def list_symbols(self) -> List[str]:
        return self.library.list_symbols()


class DictData(Data, Generic[T]):
    def __init__(self,
                 duckdb_path: str,
                 library_name: str):
        if not library_name:
            raise ValueError('library_name must be supplied')
        self.duckdb_path = duckdb_path
        self.library_name = library_name
        if duckdb_path and (
            duckdb_path.endswith('.duckdb')
            or duckdb_path.startswith('/')
            or duckdb_path.startswith('~')
        ):
            db_path = duckdb_path
        else:
            db_path = _default_db_path()
        self.zone: tzfile = gettz('America/New_York')  # type: ignore
        self.object_store = DuckDBObjectStore(db_path)

    def _to_symbol(self, contract: Union[Contract, SecurityDefinition, int, str]) -> str:
        if type(contract) is str:
            return contract
        elif type(contract) == int:
            return str(contract)
        elif type(contract) is Contract:
            return str(cast(Contract, contract).conId)
        elif type(contract) is SecurityDefinition:
            definition = cast(SecurityDefinition, contract)
            return str(definition.conId)
        else:
            raise ValueError('cast not supported')

    def read(self,
             contract: Union[Contract, SecurityDefinition, int],
             date_range: DateRange = DateRange(dt.datetime(1970, 1, 1), dt.datetime.now())) -> Optional[T]:
        key = self.library_name + '/' + self._to_symbol(contract)
        return self.object_store.read(key)

    def write(self, contract: Union[Contract, SecurityDefinition, int], data: T) -> None:
        logging.info('DictData writing contract {}'.format(contract))
        key = self.library_name + '/' + self._to_symbol(contract)
        self.object_store.write(key, data)

    def delete(self, contract: Union[Contract, SecurityDefinition, int]) -> None:
        logging.info('DictData deleting contract {}'.format(contract))
        key = self.library_name + '/' + self._to_symbol(contract)
        self.object_store.delete(key)

    def list_symbols(self) -> List[str]:
        prefix = self.library_name + '/'
        all_keys = self.object_store.list_symbols()
        return [k[len(prefix):] for k in all_keys if k.startswith(prefix)]


class TickData(Data):
    def __init__(self,
                 duckdb_path: str,
                 library_name: str):
        super().__init__(duckdb_path=duckdb_path,
                         library_name=library_name,
                         lib_type='')
        PandasObject.daily_open = daily_open  # type: ignore
        PandasObject.daily_close = daily_close  # type: ignore
        PandasObject.market_hours = market_hours  # type: ignore
        logging.info('initializing TickData {} {}'.format(duckdb_path, library_name))

    def get_schema(self) -> Set[str]:
        return {'date', 'open', 'high', 'low', 'close', 'volume', 'average', 'bar_count', 'bar_size'}

    def date_summary(self, contract: Union[Contract, SecurityDefinition, int]) -> Tuple[dt.datetime, dt.datetime]:
        if type(contract) is int:
            contract = Contract(conId=cast(int, contract))
        contract = cast(Contract, contract)
        try:
            min_date = dateify(self.library.min_date(symbol=self._to_symbol(contract)), timezone=self.zone)
            max_date = dateify(self.library.max_date(symbol=self._to_symbol(contract)), timezone=self.zone)
        except ValueError:
            return (dateify(dt.datetime(1970, 1, 1), timezone=self.zone),
                    dateify(dt.datetime(1970, 1, 1), timezone=self.zone))
        return (min_date, max_date)

    def summary(
        self,
        contract: Union[Contract, SecurityDefinition, int]
    ) -> Tuple[dt.datetime, dt.datetime, pd.Series, pd.Series]:
        if type(contract) is int:
            contract = Contract(conId=cast(int, contract))

        contract = cast(Contract, contract)
        try:
            min_date = self.library.min_date(symbol=self._to_symbol(contract))
            max_date = self.library.max_date(symbol=self._to_symbol(contract))
        except ValueError:
            empty_series = pd.Series(dtype='float64')
            return (dateify(dt.datetime(1970, 1, 1), self.zone),
                    dateify(dt.datetime(1970, 1, 1), self.zone),
                    empty_series, empty_series)
        min_date_range = DateRange(min_date, min_date)
        max_date_range = DateRange(max_date, max_date + dt.timedelta(days=1))

        return (dateify(min_date, self.zone),
                dateify(max_date, self.zone),
                self.read(contract, date_range=min_date_range).iloc[0],
                self.read(contract, date_range=max_date_range).iloc[-1])

    def write_resolve_overlap(
        self,
        contract: Union[Contract, SecurityDefinition, int],
        data_frame: pd.DataFrame
    ):
        # DuckDB store handles upserts natively, so overlapping data
        # is resolved automatically.  We merge with existing data to
        # maintain dedup semantics when merging overlapping data.
        existing_data = self.read(contract)
        if existing_data.empty:
            self.write(contract, data_frame)
            return

        temp_df = pd.concat([existing_data, data_frame])
        result = cast(pd.DataFrame, temp_df[~temp_df.index.duplicated(keep='first')])
        result.sort_index(inplace=True)

        # todo this probably shouldn't go here -- there's a bug upstream
        # somewhere which kicks out a 'Timestamp' object has no attribute 'astype' exception
        try:
            result.index = pd.to_datetime(result.index)  # type: ignore
        except ValueError as ve:
            logging.debug('pd.to_datetime failed with {}'.format(ve))
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
            try:
                actual_date_range = DateRange(
                    self.library.min_date(symbol=self._to_symbol(contract)),
                    self.library.max_date(symbol=self._to_symbol(contract))
                )
            except ValueError:
                return pd.DataFrame()

        # Filter by bar_size (library_name) so that when multiple bar sizes
        # share the same DuckDB table, only the relevant rows are returned.
        bar_size_filter = self.library_name if self.library_name else None
        result = self.library.read(
            symbol=self._to_symbol(contract),
            start=actual_date_range.start,
            end=actual_date_range.end,
            bar_size=bar_size_filter,
        )
        if result.empty:
            return pd.DataFrame()

        df = self._fix_df_timezone(result)

        if pd_offset:
            return df.resample(pd_offset).last()  # type: ignore
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
        result = self.get_data(contract, date_range=date_range)
        return len(result) > 0

    def missing(self,
                contract: Union[Contract, SecurityDefinition, int],
                exchange_calendar: ExchangeCalendar,
                pd_offset: Optional[str] = None,
                period: Optional[str] = None,
                date_range: Optional[DateRange] = None) -> List[DateRange]:
        if not pd_offset and not period and not date_range:
            try:
                date_range = DateRange(dateify(self.library.min_date(symbol=self._to_symbol(contract))),
                                       dateify() - dt.timedelta(days=1))
            except ValueError:
                return []

        df = self.get_data(contract, pd_offset, period, date_range)

        no_data_dates: List[dt.date] = []

        dates: List[dt.date] = []
        sessions = exchange_calendar.sessions.date  # type: ignore
        # no data case
        if len(df) == 0:
            dates = no_data_dates
        else:
            # make sure we have all the trading days
            dates = df.resample('D').first().index.date  # type: ignore
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


class TickStorage():
    def __init__(
        self,
        duckdb_path: str
    ):
        self.duckdb_path = duckdb_path

    def list_libraries(self) -> List[str]:
        # In the DuckDB world, "libraries" are just bar_size strings that have
        # been used as prefixes.  We look for symbols in the tick_data table
        # that have an associated bar_size value.
        if self.duckdb_path and (
            self.duckdb_path.endswith('.duckdb')
            or self.duckdb_path.startswith('/')
            or self.duckdb_path.startswith('~')
        ):
            db_path = self.duckdb_path
        else:
            db_path = _default_db_path()
        store = DuckDBDataStore(db_path)
        rows = store._db.execute(
            f"SELECT DISTINCT bar_size FROM {store.TABLE_NAME} "
            f"WHERE bar_size IS NOT NULL ORDER BY bar_size",
            fetch='all',
        ) or []
        all_bar_sizes = [row[0] for row in rows]
        return [x for x in all_bar_sizes if x in BarSize.bar_sizes()]

    def list_libraries_barsize(self) -> List[BarSize]:
        return [BarSize.parse_str(x) for x in self.list_libraries()]

    def get_tickdata(self, bar_size: BarSize):
        # todo: sharding this by bar_size doesn't seem right, but
        # neither does universe_name + bar_size
        library_name = TickStorage.history_to_library_hash(bar_size)
        library = TickData(self.duckdb_path, library_name)
        return library

    def read(
        self,
        contract: Union[Contract, SecurityDefinition, int],
        date_range: DateRange = DateRange(dt.datetime(1970, 1, 1), dt.datetime.now())
    ) -> pd.DataFrame:
        results = pd.DataFrame()

        for bar_size in self.list_libraries_barsize():
            result = self.get_tickdata(bar_size).read(contract=contract, date_range=date_range)
            results = pd.concat([results, result])
        return results

    @staticmethod
    def history_to_library_hash(bar_size: BarSize):
        return str(bar_size)
