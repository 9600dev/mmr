import sys
import os

# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

import datetime as dt
import ib_insync as ibapi
import click
import pandas as pd

from redis import Redis
from rq import Queue
from rq.job import Job
from dateutil.tz import tzlocal
from typing import Tuple, List, Optional

from trader.common.command_line import common_options
from trader.data.data_access import TickData
from trader.common.logging_helper import setup_logging
from trader.common.helpers import date_range, dateify, symbol_to_contract
from trader.common.listener_helpers import Helpers
from trader.batch.ib_history_batch import IBHistoryQueuer, IBHistoryWorker

logging = setup_logging(module_name='ib_history_queuer')

@click.command()
@common_options()
@click.option('--contracts', required=False, help='filename or comma seperated list of conIds to get history for')
@click.option('--exclude_contract_csv_file', required=False, help='csv filename for conIds to exclude')
# @click.option('--ib_server_address', required=False, default='127.0.0.1', help='tws trader API address: 127.0.0.1')
# @click.option('--ib_server_port', required=False, default=7496, help='port for tws server API: 7496')
# @click.option('--ib_client_id', required=False, default=5, help='ib client id: 5')
@click.option('--bar_size', required=False, default='1 min', help='IB bar size: 1 min')
# @click.option('--arctic_server_address', required=False, default='127.0.0.1', help='arctic server ip address: 127.0.0.1')
# @click.option('--arctic_library', required=True, help='tick store library name, eg: IB_NASDAQ_TOP_500_1min')
# @click.option('--redis_server_address', required=False, default='127.0.0.1', help='redis server ip address: 127.0.0.1')
# @click.option('--redis_server_port', required=False, default=6379, help='redis server port: 6379')
@click.option('--enqueue', required=False, is_flag=True, default=False, help='queue up history work')
@click.option('--prev_days', required=False, default=5, help='Enqueue today minus prev_days: default 5 days')
def main(contracts: str,
         exclude_contract_csv_file: str,
         ib_server_address: str,
         ib_server_port: int,
         ib_client_id: int,
         bar_size: str,
         arctic_server_address: str,
         arctic_library: str,
         redis_server_address: str,
         redis_server_port: int,
         enqueue: bool,
         prev_days: int):
    # history queuer
    if enqueue:
        if not contracts:
            raise ValueError('contract file or comma seperated list of conids is required')
        # queue up history
        queuer = IBHistoryQueuer(ib_server_address,
                                 ib_server_port,
                                 ib_client_id,
                                 arctic_server_address,
                                 arctic_library,
                                 redis_server_address,
                                 redis_server_port)
        start_date = dateify(dt.datetime.now() - dt.timedelta(days=prev_days + 1))
        logging.info('enqueing ib history from {} onwards'.format(start_date))

        if os.path.exists(contracts):
            # here's where we look at our symbol data
            included = Helpers.contracts_from_df(pd.read_csv(contracts))
            excluded = []
            if exclude_contract_csv_file and os.path.exists(exclude_contract_csv_file):
                excluded = Helpers.contracts_from_df(pd.read_csv(exclude_contract_csv_file))

            contracts_to_queue = [s for s in included if s not in excluded]
            queuer.queue_history(contracts_to_queue, bar_size, start_date)
        elif ',' in contracts:
            data = TickData(arctic_server_address, arctic_library)
            temp_symbols = contracts.split(',')
            list_contracts = [symbol_to_contract(s) for s in temp_symbols]
            queuer.queue_history(list_contracts, bar_size=bar_size, start_date=start_date)
        else:
            raise ValueError('contract file, or comma seperated list of conids not found or defined')
    # history worker
    else:
        raise ValueError('todo not done')

if __name__ == '__main__':
    main()
