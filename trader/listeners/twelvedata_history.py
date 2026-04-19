from trader.common.logging_helper import setup_logging
from trader.objects import BarSize, WhatToShow
from twelvedata import TDClient

import datetime as dt
import pandas as pd
import pytz


logging = setup_logging(module_name='twelvedata_history')


class TwelveDataHistoryWorker:
    def __init__(self, twelvedata_api_key: str):
        if not twelvedata_api_key:
            raise ValueError('twelvedata_api_key is required')
        self.client = TDClient(apikey=twelvedata_api_key)

    def get_history(
        self,
        ticker: str,
        bar_size: BarSize,
        start_date: dt.datetime,
        end_date: dt.datetime,
        timezone: str = 'US/Eastern',
    ) -> pd.DataFrame:
        interval = BarSize.to_twelvedata_interval(bar_size)

        if bar_size in (BarSize.Days1, BarSize.Weeks1, BarSize.Months1):
            from_ = start_date.strftime('%Y-%m-%d')
            to = end_date.strftime('%Y-%m-%d')
        else:
            from_ = start_date.strftime('%Y-%m-%d %H:%M:%S')
            to = end_date.strftime('%Y-%m-%d %H:%M:%S')

        logging.info('get_history {} {} {} {} to {}'.format(
            ticker, bar_size, interval, from_, to
        ))

        ts = self.client.time_series(
            symbol=ticker,
            interval=interval,
            start_date=from_,
            end_date=to,
            outputsize=5000,
            timezone=timezone,
        )

        try:
            df = ts.as_pandas()
        except Exception as ex:
            logging.error('twelvedata time_series failed for {}: {}'.format(ticker, ex))
            raise

        if df is None or len(df) == 0:
            logging.info('no data returned for {} from {} to {}'.format(ticker, from_, to))
            return pd.DataFrame()

        df = df.copy()
        df.index.name = 'date'

        idx = pd.to_datetime(df.index)
        if idx.tz is None:
            idx = idx.tz_localize(pytz.timezone(timezone))
        else:
            idx = idx.tz_convert(pytz.timezone(timezone))
        df.index = idx

        for col in ('open', 'high', 'low', 'close', 'volume'):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        df['average'] = float('nan')
        df['bar_count'] = 0
        df['bar_size'] = str(bar_size)
        df['what_to_show'] = int(WhatToShow.TRADES)

        keep = ['open', 'high', 'low', 'close', 'volume', 'average', 'bar_count', 'bar_size', 'what_to_show']
        df = df[[c for c in keep if c in df.columns]]

        df.sort_index(ascending=True, inplace=True)

        logging.info('get_history returned {} rows for {}'.format(len(df), ticker))
        return df
