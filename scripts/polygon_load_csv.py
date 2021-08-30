import pandas as pd
from arctic import Arctic, TICK_STORE
from arctic.tickstore.tickstore import TickStore
from arctic.date import DateRange
import os

def load_csv(arctic_library, csv_file: str):
    csv = pd.read_csv(csv_file)
    symbol = csv['symbol'].iloc[0]
    # polygon has unix timestamp in ms format UTC timezone
    csv.date = pd.to_datetime(csv.date, unit='ms', utc=True)
    csv['open_trades'] = csv.open
    csv['high_trades'] = csv.high
    csv['low_trades'] = csv.low
    csv['close_trades'] = csv.close
    csv['volume_trades'] = csv.volume
    csv['average_trades'] = csv.vwap
    csv['open_bid'] = csv.open
    csv['high_bid'] = csv.high
    csv['low_bid'] = csv.low
    csv['close_bid'] = csv.close
    csv['open_ask'] = csv.open
    csv['high_ask'] = csv.high
    csv['low_ask'] = csv.low
    csv['close_ask'] = csv.close
    csv['bar_size'] = '5 minutes'
    csv.set_index(csv.date, inplace=True)
    csv.drop(['date', 'symbol', 'open', 'close', 'high', 'low', 'vwap', 'volume'], axis=1, inplace=True)

    print('writing {} {} records'.format(symbol, len(csv)))
    arctic_library.write(symbol, csv)


def main():
    store = Arctic('localhost')
    lib = 'NASDAQ-5-Minute'
    store.initialize_library(lib, lib_type=TICK_STORE)
    library = store[lib]

    directory = '/home/trader/mmr/listeners/5minute-polygon/'
    files = os.listdir(directory)
    for f in files:
        load_csv(library, directory + f)



