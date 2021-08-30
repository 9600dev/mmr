import sys
import os

# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import pandas as pd
import datetime as dt
import click
import random
import os
import logging

from typing import List, Dict, Tuple, Callable, Optional, Set, Generic, TypeVar, cast, Union
from ib_insync import Stock, IB, Contract, Forex, BarData, Future

from tabulate import tabulate
from trader.listeners.ibrx import IBRx
from trader.common.data import Data, TickData
from finviz.screener import Screener

class FinvizScraper():
    def scrape(self, filters: List[str], table: str, order: str) -> pd.DataFrame:
        stock_list = Screener(filters=filters, table=table, order=order)
        print(stock_list)
        print(len(stock_list.data))

        # change the columns to match the rest of trader
        data_frame = pd.DataFrame(stock_list.data)
        data_frame = data_frame.drop(columns=['No.'])
        data_frame = data_frame.rename(columns={'Ticker': 'symbol'})
        data_frame.columns = data_frame.columns.str.lower()

        return data_frame

@click.command()
@click.option('--filters', required=True, multiple=True, default=['exch_nasd'], help='exch_nasd, idx_sp500')
@click.option('--order', required=False, default='-marketcap', help='sort order: marketcap')
@click.option('--table', required=False, default='Overview',
              type=click.Choice(['Overview', 'Valuation', 'Financial', 'Ownership', 'Marketcap', 'Performance', 'Technical']))
@click.option('--csv_output_file', required=False, help='csv output file')
def main(filters: List[str], order: str, table: str, csv_output_file: str):
    f = FinvizScraper()
    result = f.scrape(filters, table, order)
    if csv_output_file:
        result.to_csv(csv_output_file, header=True, index=False)
    else:
        print(len(result))
        print(tabulate(result, headers=list(result.columns), tablefmt='psql'))
        print(len(result))
if __name__ == '__main__':
    main()


