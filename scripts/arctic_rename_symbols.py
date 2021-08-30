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
from arctic import Arctic, TICK_STORE

from typing import List, Dict, Tuple, Callable, Optional, Set, Generic, TypeVar, cast, Union
from ib_insync import Stock, IB, Contract, Forex, BarData, Future

from trader.listeners.ibrx import IBRx
from trader.common.data import Data, TickData

def rename_symbols(csv_file: str,
                   arctic_server_address: str,
                   arctic_library: str):
    contracts = pd.read_csv(csv_file)
    data = TickData(arctic_server_address, arctic_library)

    symbol_list: List[str] = data.list_symbols(data.historical)  # type: ignore
    for index, row in contracts.iterrows():
        symbol = row['symbol']
        conid = row['conId']
        if symbol in symbol_list:
            print('getting {} {}'.format(symbol, conid))
            result = data.get_data(symbol)
            contract = Contract(conId=conid)
            data.write(contract, result)
            print('deleting {}'.format(symbol))
            data.delete(contract)


@click.command()
@click.option('--contract_csv_file', required=False, help='conid csv file')
@click.option('--arctic_server_address', required=False, default='127.0.0.1', help='arctic server address 127.0.0.1')
@click.option('--arctic_library', required=False, default='Historical', help='arctic library to rename')
def main(contract_csv_file: str,
         arctic_server_address: str,
         arctic_library: str):
    rename_symbols(contract_csv_file, arctic_server_address, arctic_library)

if __name__ == '__main__':
    logging.disable(logging.CRITICAL)
    logger = logging.getLogger('__main__')
    logger.propagate = False
    main()
