from trader.data.store import DateRange
from ib_async.ticker import Ticker
from reactivex import Observer
from reactivex.disposable import Disposable
from reactivex.subject import Subject
from trader.common.logging_helper import setup_logging
from trader.data.data_access import SecurityDefinition, TickData
from trader.data.universe import Universe, UniverseAccessor
from trader.listeners.ib_history_worker import IBHistoryWorker, WhatToShow
from trader.listeners.ibreactive import IBAIORx
from typing import Optional

import datetime as dt
import exchange_calendars
import math
import numpy as np
import pandas as pd


logging = setup_logging(module_name='data')

# Canonical column schema for on_prices() DataFrames.
NORMALIZED_COLUMNS = [
    'open', 'high', 'low', 'close', 'volume',
    'vwap', 'bar_count',
    'bid', 'ask', 'last', 'last_size',
]


def _ib_nan(val) -> float:
    """Convert IB sentinel values to NaN.

    IB uses ``sys.float_info.max`` (1.7976931348623157e+308) as a sentinel
    for "no data" on bid/ask/last fields.
    """
    if val is None:
        return np.nan
    try:
        f = float(val)
    except (TypeError, ValueError):
        return np.nan
    if not math.isfinite(f) or f >= 1e308:
        return np.nan
    return f


def normalize_ticker(ticker: Ticker) -> pd.DataFrame:
    """Convert a live IB Ticker into a single-row normalized DataFrame.

    Maps the 70+ Ticker fields down to the 11 canonical columns.
    The index is a DatetimeIndex from ``ticker.time``.
    """
    ts = ticker.time or dt.datetime.now(dt.timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)

    bid = _ib_nan(ticker.bid)
    ask = _ib_nan(ticker.ask)
    last = _ib_nan(ticker.last)
    last_size = _ib_nan(ticker.lastSize)

    # For open/high/low/close: use last trade price when available,
    # otherwise midpoint of bid/ask, otherwise NaN.
    mid = np.nan
    if not np.isnan(bid) and not np.isnan(ask):
        mid = (bid + ask) / 2.0
    price = last if not np.isnan(last) else mid

    row = {
        'open': price,
        'high': price,
        'low': price,
        'close': price,
        'volume': _ib_nan(ticker.volume),
        'vwap': _ib_nan(ticker.vwap),
        'bar_count': np.nan,
        'bid': bid,
        'ask': ask,
        'last': last,
        'last_size': last_size,
    }

    idx = pd.DatetimeIndex([ts], name='date')
    return pd.DataFrame([row], index=idx, columns=NORMALIZED_COLUMNS)


def normalize_historical(df: pd.DataFrame) -> pd.DataFrame:
    """Convert a DuckDB / backtester historical DataFrame to the normalized schema.

    Renames ``average`` → ``vwap``, adds ``bid``, ``ask``, ``last`` (= close),
    ``last_size`` as NaN, and drops ``bar_size`` / ``what_to_show``.
    """
    if df.empty:
        return df

    out = df.copy()

    # Rename average → vwap
    if 'average' in out.columns:
        out = out.rename(columns={'average': 'vwap'})

    # Drop metadata columns
    for col in ('bar_size', 'what_to_show'):
        if col in out.columns:
            out = out.drop(columns=[col])

    # Add missing columns with appropriate defaults
    if 'vwap' not in out.columns:
        out['vwap'] = np.nan
    if 'bar_count' not in out.columns:
        out['bar_count'] = np.nan
    if 'bid' not in out.columns:
        out['bid'] = np.nan
    if 'ask' not in out.columns:
        out['ask'] = np.nan
    if 'last' not in out.columns:
        out['last'] = out['close'] if 'close' in out.columns else np.nan
    if 'last_size' not in out.columns:
        out['last_size'] = np.nan

    # Ensure column order matches the canonical schema
    existing = [c for c in NORMALIZED_COLUMNS if c in out.columns]
    out = out[existing]

    # Ensure index name is 'date'
    if out.index.name != 'date':
        out.index.name = 'date'

    return out

class SecurityDataStream(Subject[pd.DataFrame]):
    def __init__(
            self,
            security: SecurityDefinition,
            bar_size: str,
            date_range: DateRange,
            existing_data: Optional[pd.DataFrame] = None):
        super().__init__()
        self.security = security
        self.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'average', 'bar_count', 'bar_size']
        self.date_range: DateRange
        self.df: pd.DataFrame = pd.DataFrame([], columns=self.columns)
        self.is_being_backfilled: bool = True
        if existing_data is not None:
            self.df = existing_data
        self.bar_size = bar_size

    def on_next(self, value: pd.DataFrame) -> None:
        self.check_disposed()

        if self.is_stopped:
            return

        # todo: gotta be a faster way here
        self.df = pd.concat([self.df, value])

        for obv in list(self.observers):
            obv.on_next(self.df)

    async def subscribe_async(self, observer: Observer[pd.DataFrame]) -> Disposable:
        self.check_disposed()

        self.observers.append(observer)

        def dispose() -> None:
            if observer in self.observers:
                self.observers.remove(observer)

        result = Disposable(dispose)

        # send the last cached result
        observer.on_next(self.df)
        return result

    def data(self) -> pd.DataFrame:
        return self.df


class MarketData():
    def __init__(
        self,
        client: IBAIORx,
        duckdb_path: str,
        universe_library: str,
    ):
        self.client: IBAIORx = client
        self.duckdb_path = duckdb_path
        self.universe_library = universe_library
        self.universe = UniverseAccessor(self.duckdb_path, self.universe_library)
        self.data = TickData(self.duckdb_path, 'bardata')

    async def subscribe_security(
        self,
        security: SecurityDefinition,
        bar_size: str,
        start_date: dt.datetime,
        back_fill: bool,
    ) -> SecurityDataStream:
        # todo this is probably obsolete

        # if we backfill, this essentially awaits until back_fill is complete
        if not start_date.tzinfo:
            raise ValueError('start_date must specify a timezone (start_date.tzinfo)')

        end_date = dt.datetime.now().astimezone(start_date.tzinfo)
        date_range = DateRange(start=start_date, end=end_date)

        if back_fill:
            # grab the existing data we have
            history_worker = IBHistoryWorker(self.client.ib)
            # todo we need to use the ib_history batch infrastructure here and have it
            # notify via events, but for now we'll just grab the data ourselves.
            calendar = exchange_calendars.get_calendar(security.primaryExchange)
            date_ranges = self.data.missing(
                contract=security,
                exchange_calendar=calendar,
                date_range=date_range
            )
            for date_dr in date_ranges:
                result = await history_worker.get_contract_history(
                    security=security,
                    what_to_show=WhatToShow.TRADES,
                    bar_size=bar_size,
                    start_date=start_date,
                    end_date=end_date,
                    filter_between_dates=True,
                    tz_info='America/New_York'
                )
                logging.info('writing backfill data for {} {}'.format(security, date_dr))
                self.data.write_resolve_overlap(security, data_frame=result)

        df = self.data.get_data(security, date_range=date_range)
        stream = SecurityDataStream(security=security, bar_size=bar_size, date_range=date_range, existing_data=df)
        observable = await self.client.subscribe_contract_history(
            contract=Universe.to_contract(security),
            start_date=df.index[-1].to_pydatetime(),  # type: ignore
            what_to_show=WhatToShow.TRADES,
        )

        disposable = observable.subscribe(stream)

        return stream
