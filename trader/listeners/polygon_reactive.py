from polygon import build_option_symbol, ForexClient, StocksClient, StreamClient
from trader.common.helpers import dateify

import coloredlogs
import datetime as dt
import logging
import numpy as np
import os
import pandas as pd
import polygon
import pytz
import time


class PolygonFinancials():
    def __init__(self, financials: pd.DataFrame, dividends: pd.DataFrame, splits: pd.DataFrame):
        self.financials = financials
        self.splits = splits
        self.dividends = dividends


class PolygonReactive():
    def __init__(self,
                 polygon_api_key: str,
                 limit: int = 50000,
                 request_sleep: int = 0):

        if polygon_api_key is None or polygon_api_key == '' and os.getenv('POLYGON_API_KEY'):
            polygon_api_key = os.getenv('POLYGON_API_KEY')  # type: ignore
        else:
            raise ValueError('polygon_api_key is not found {} or POLYGON_API_KEY set incorrectly'.format(polygon_api_key))

        self.client: polygon.AsyncClient = polygon.AsyncClient(polygon_api_key)
        self.request_sleep = request_sleep
        self.limit = limit

        # It also exposes a few methods which you could use to create your own reconnect mechanism.
        # Method polygon.streaming.async_streaming.AsyncStreamClient.reconnect() is one of them

    def date(self, date_time: dt.datetime) -> str:
        return dt.datetime.strftime(date_time, '%Y-%m-%d')

    def date_from_ts(self, unixtimestamp: int, zone: str = 'America/New_York') -> dt.datetime:
        return dt.datetime.fromtimestamp(unixtimestamp / 1000.0, tz=pytz.timezone(zone))

    def date_from_nanots(self, unixtimestamp: int, zone: str = 'America/New_York') -> dt.datetime:
        return dt.datetime.fromtimestamp(unixtimestamp / 1000000000.0, tz=pytz.timezone(zone))

    def round_to_second(self, unixtimestamp: int, zone: str = 'America/New_York') -> np.int64:
        d = self.date_from_nanots(unixtimestamp, zone)
        date_time = dt.datetime(d.year, d.month, d.day, d.hour, d.minute, d.second)
        return np.int64(date_time.timestamp())

    def get_all_equity_symbols(self):
        yesterday = dt.datetime.now() - dt.timedelta(days=1)
        result = self.client.stocks_equities_grouped_daily('US', 'STOCKS', self.date(yesterday))
        symbols = [r['T'] for r in result.results]  # type: ignore
        return symbols

    def get_grouped_daily(self, market: str, date_time: dt.datetime) -> pd.DataFrame:
        resp = self.client.stocks_equities_grouped_daily('US', market, date=self.date(date_time))

        columns = {
            'T': 'symbol',
            'v': 'volume',
            'o': 'open',
            'c': 'close',
            'h': 'high',
            'l': 'low',
            't': 'date',
            'vw': 'vwap',
            'n': 'items'
        }

        df = pd.DataFrame(resp.results)  # type: ignore
        df = df.rename(columns=columns)  # type: ignore

        return df

    def get_aggregates(self,
                       symbol: str,
                       multiplier: int,
                       timespan: str,
                       start_date: dt.datetime,
                       end_date: dt.datetime) -> pd.DataFrame:
        timespans = ['day', 'minute', 'hour', 'week', 'month', 'quarter', 'year']
        if timespan not in timespans:
            raise ValueError('incorrect timespan, must be {}'.format(timespans))
        start_date = dateify(start_date, timezone='America/New_York')
        end_date = dateify(end_date + dt.timedelta(days=1), timezone='America/New_York')
        logging.info('get_aggregates {} mul: {} timespan: {} {} {}'.format(symbol,
                                                                           multiplier,
                                                                           timespan,
                                                                           start_date,
                                                                           end_date))
        result = self.client.stocks_equities_aggregates(symbol,
                                                        multiplier,
                                                        timespan,
                                                        self.date(start_date),
                                                        self.date(end_date),
                                                        **{'limit': self.limit})

        time.sleep(self.request_sleep)

        columns = {
            'T': 'symbol',
            'v': 'volume',
            'o': 'open',
            'c': 'close',
            'h': 'high',
            'l': 'low',
            't': 'date',
            'n': 'items',
            'vw': 'vwap'
        }

        df = pd.DataFrame(result.results)  # type: ignore
        df = df.rename(columns=columns)  # type: ignore
        df['symbol'] = symbol
        df['date'] = df['date'].apply(self.date_from_ts)  # convert to proper timezone
        df.index = df['date']
        df.drop(columns=['date'], inplace=True)
        df['volume'] = df['volume'].astype(int)
        df = df.reindex(['symbol', 'open', 'close', 'high', 'low', 'volume', 'vwap', 'items'], axis=1)

        # we have more work to do
        if len(df) == 50000:
            last_date = df.index[-1].to_pydatetime()
            combined = df.append(self.get_aggregates(symbol, multiplier, timespan, last_date, end_date))
            result = combined[~combined.index.duplicated()]
            return result[(result.index >= start_date) & (result.index <= end_date)]

        return df[(df.index >= start_date) & (df.index <= end_date)]

    def get_aggregates_as_ib(self,
                             symbol: str,
                             multiplier: int,
                             timespan: str,
                             start_date: dt.datetime,
                             end_date: dt.datetime) -> pd.DataFrame:
        result = self.get_aggregates(symbol, multiplier, timespan, start_date, end_date)
        # interactive brokers history mapping

        mapping = {
            'open_trades': 'open',
            'high_trades': 'high',
            'low_trades': 'low',
            'close_trades': 'close',
            'volume_trades': 'volume',
            'average_trades': 'vwap',
            'open_bid': 'open',
            'high_bid': 'high',
            'low_bid': 'low',
            'close_bid': 'close',
            'open_ask': 'open',
            'high_ask': 'high',
            'low_ask': 'low',
            'close_ask': 'close',
            'barCount_trades': 'items',
        }
        for key, value in mapping.items():
            result[key] = result[value]

        result.rename_axis(None, inplace=True)
        result.drop(columns=['symbol', 'open', 'close', 'high', 'low', 'volume', 'vwap', 'items'], inplace=True)
        result = result.reindex(
            ['high_ask', 'high_trades', 'close_trades', 'low_bid', 'average_trades',
             'open_trades', 'low_trades', 'barCount_trades', 'open_bid', 'volume_trades',
             'low_ask', 'high_bid', 'close_ask', 'close_bid', 'open_ask'], axis=1)  # type: ignore
        return result

    def get_financials(self, symbol: str) -> PolygonFinancials:
        financials = self.client.reference_stock_financials(symbol, **{'limit': self.limit}).results
        dividends = self.client.reference_stock_dividends(symbol, **{'limit': self.limit}).results
        splits = self.client.reference_stock_splits(symbol, **{'limit': self.limit}).results

        result = PolygonFinancials(pd.DataFrame(financials), pd.DataFrame(dividends), pd.DataFrame(splits))  # type: ignore
        return result

    def get_historic_ticks(self, symbol: str, date_time: dt.datetime) -> pd.DataFrame:
        trades_resp = self.client.historic_trades_v2(symbol, self.date(date_time))
        quotes_resp = self.client.historic_n___bbo_quotes_v2(symbol, self.date(date_time))

        trades = trades_resp.results.copy()
        quotes = quotes_resp.results.copy()

        while len(trades_resp.results) == self.limit:
            last = trades[-1]['y']  # type: ignore
            trades_resp = self.client.historic_trades_v2(symbol, self.date(date_time), timestamp=last)
            trades = trades + trades_resp.results
            logging.info('historic trades for {} last {}'.format(symbol, self.date_from_nanots(last)))

        while len(quotes_resp.results) == self.limit:
            last = quotes[-1]['y']  # type: ignore
            quotes_resp = self.client.historic_n___bbo_quotes_v2(symbol, self.date(date_time), timestamp=last)
            quotes = quotes + quotes_resp.results
            logging.info('historic quotes for {} last {}'.format(symbol, self.date_from_nanots(last)))

        trades_df = pd.DataFrame(trades)
        quotes_df = pd.DataFrame(quotes)

        trades_columns = {
            'y': 'date',
            's': 'volume',
            'p': 'price'
        }

        trades_df = trades_df.rename(columns=trades_columns)  # type: ignore
        trades_df = trades_df[list(trades_columns.values())]

        quotes_columns = {
            'y': 'date',
            'p': 'bid',
            's': 'bid_size',
            'P': 'ask',
            'S': 'ask_size'
        }

        quotes_df = quotes_df.rename(columns=quotes_columns)  # type: ignore
        quotes_df = quotes_df[list(quotes_columns.values())]

        trades_df = trades_df.set_index('date')
        quotes_df = quotes_df.set_index('date')

        ffill_cols = ['bid', 'bid_size', 'ask', 'ask_size']

        print(len(trades_df))
        print(len(quotes_df))

        # todo:// probably should change the unixtimestamp out of nanos into seconds?
        joined = trades_df.join(quotes_df, how='outer')
        joined.loc[:, ffill_cols] = joined.loc[:, ffill_cols].ffill()

        joined = joined.reset_index()
        joined.date = joined.date.apply(self.round_to_second)
        joined = joined.set_index('date')

        # collapse same price same time volume
        joined = joined.groupby([joined.index, joined.price])['volume'].sum().reset_index().set_index('date')
        # grab the latest price in that nanosecond
        joined = joined.groupby('date').tail(1)

        return joined[~pd.isna(joined.price)], trades_df, quotes_df  # type: ignore

def crawl_financials():
    coloredlogs.install(level='INFO')
    client = PolygonListener()
    symbols = pd.read_csv('complete_symbols.csv').symbols.to_list()

    for symbol in symbols:
        df = client.get_financials(symbol)
        df.to_csv('financials/' + symbol + '.csv', index=False, header=True)
        print('{} {}'.format(symbol, len(df)))

def crawl_daily():
    coloredlogs.install(level='INFO')
    client = PolygonListener()
    start_date = dt.datetime(2005, 1, 1)
    # end_date = dt.datetime(2006, 12, 31)
    end_date = dt.datetime.now()

    current_date = end_date
    written = False
    column_order = ['date', 'symbol', 'open', 'close', 'high', 'low', 'volume']

    while current_date >= start_date:
        df = client.get_grouped_daily('BONDS', current_date)
        if len(df) == 0:
            print('skipping day {}'.format(current_date))
            current_date = current_date - dt.timedelta(days=1)
            continue
        if not written:
            df[column_order].to_csv('bonds_daily.csv', index=False, header=True)
            written = True
        else:
            df[column_order].to_csv('bonds_daily.csv', index=False, header=False, mode='a')
        print('completed {}'.format(current_date))
        current_date = current_date - dt.timedelta(days=1)

def crawl_minute():
    coloredlogs.install(level='INFO')
    client = PolygonListener()
    start_date = dt.datetime(2006, 1, 1)
    # end_date = dt.datetime(2006, 12, 31)
    end_date = dt.datetime.now() - dt.timedelta(days=1)

    current_date = end_date
    written = False
    column_order = ['date', 'symbol', 'open', 'close', 'high', 'low', 'vwap', 'volume']
    dir_output = '5minute-polygon/'
    symbols = list(pd.read_csv('zacks-screen.csv').Ticker)
    print(len(symbols))
    left = len(symbols)
    days_diff = 30

    for symbol in symbols:
        written = False
        left = left - 1
        current_date = end_date
        old_date = current_date
        print('{} symbols left, starting {}'.format(left, symbol))
        while current_date >= start_date:
            try:
                sd = current_date - dt.timedelta(days=days_diff)
                df = client.get_aggregates(symbol, 5, 'minute', sd, current_date)
                old_date = current_date
                current_date = sd
            except Exception as ex:
                print(ex)
                continue
            if len(df) == 0:
                print('skipping day {}'.format(current_date))
                current_date = current_date - dt.timedelta(days=1)
                continue
            df['symbol'] = symbol
            if not written:
                df[column_order].sort_values(by='date', ascending=False).to_csv(dir_output + symbol + '.csv', index=False, header=True)
                written = True
            else:
                df[column_order].sort_values(by='date', ascending=False).to_csv(dir_output + symbol + '.csv', index=False, header=False, mode='a')
            print('completed {} {} to {}'.format(symbol, current_date, old_date))
            current_date = current_date - dt.timedelta(days=1)

def test():
    coloredlogs.install(level='INFO')
    client = PolygonListener()
    joined, trades_df, quotes_df = client.get_historic_ticks('AAPL', dt.datetime(2020, 4, 2))
    joined.to_csv('test.csv')
    return joined, trades_df, quotes_df

