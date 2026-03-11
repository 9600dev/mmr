from massive import RESTClient
from trader.common.logging_helper import setup_logging
from trader.objects import BarSize, WhatToShow

import datetime as dt
import pandas as pd
import pytz


logging = setup_logging(module_name='massive_history')


class MassiveHistoryWorker:
    def __init__(self, massive_api_key: str):
        self.client = RESTClient(api_key=massive_api_key)

    def get_history(
        self,
        ticker: str,
        bar_size: BarSize,
        start_date: dt.datetime,
        end_date: dt.datetime,
        timezone: str = 'US/Eastern',
    ) -> pd.DataFrame:
        multiplier, timespan = BarSize.to_massive_timespan(bar_size)

        # Massive API expects date strings or timestamps
        from_ = start_date.strftime('%Y-%m-%d')
        to = end_date.strftime('%Y-%m-%d')

        logging.info('get_history {} {} {}x{} {} to {}'.format(
            ticker, bar_size, multiplier, timespan, from_, to
        ))

        aggs = list(self.client.list_aggs(
            ticker=ticker,
            multiplier=multiplier,
            timespan=timespan,
            from_=from_,
            to=to,
            limit=50000,
        ))

        if not aggs:
            logging.info('no data returned for {} from {} to {}'.format(ticker, from_, to))
            return pd.DataFrame()

        tz = pytz.timezone(timezone)
        rows = []
        for agg in aggs:
            if agg.timestamp is None:
                continue
            # Massive timestamps are Unix ms in UTC
            ts = dt.datetime.fromtimestamp(agg.timestamp / 1000, tz=pytz.utc)
            ts = ts.astimezone(tz)
            rows.append({
                'date': ts,
                'open': agg.open,
                'high': agg.high,
                'low': agg.low,
                'close': agg.close,
                'volume': agg.volume,
                'average': agg.vwap,
                'bar_count': agg.transactions,
                'bar_size': str(bar_size),
                'what_to_show': int(WhatToShow.TRADES),
            })

        df = pd.DataFrame(rows)
        df = df.set_index('date')
        df.sort_index(ascending=True, inplace=True)

        logging.info('get_history returned {} rows for {}'.format(len(df), ticker))
        return df
