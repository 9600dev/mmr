from re import I
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
import backoff

from trader.common.logging_helper import setup_logging
logging = setup_logging(module_name='history_job')

from redis import Redis
from rq import Queue
from rq.job import Job
from ib_insync import Stock, IB, Index, Contract, Ticker, BarDataList
from dateutil.tz import tzlocal
from typing import Tuple, List, Optional

from trader.data.data_access import TickData
from trader.common.helpers import date_range, dateify, day_iter, get_exchange_calendar, pdt
from trader.listeners.polygon_listener import PolygonListener
from trader.common.listener_helpers import Helpers
from trader.batch.polygon_batch import PolygonWorker, PolygonQueuer

@click.command()
@click.option('--contracts', required=False, help='filename or comma seperated list of IB conIds')
@click.option('--exclude_contract_csv_file', required=False, help='csv filename for conIds to exclude')
@click.option('--multiplier', required=False, default=1, help='bar size: default 1')
@click.option('--timespan', required=False, default='minute', help='minute, hour, day, week, month, quarter, year: default minute')
@click.option('--arctic_server_address', required=False, default='127.0.0.1', help='arctic server ip address: 127.0.0.1')
@click.option('--arctic_library', required=False, default='HistoricalPolygon', help='tick store library name: HistoricalPolygon')
@click.option('--redis_server_address', required=False, default='127.0.0.1', help='redis server ip address: 127.0.0.1')
@click.option('--redis_server_port', required=False, default=6379, help='redis server port: 6379')
@click.option('--enqueue', required=False, is_flag=True, default=True, help='queue up price history: default True')
@click.option('--enqueue_financial', required=False, is_flag=True, default=False, help='queue up financial details history: default False')
@click.option('--prev_days', required=False, default=5, help='Enqueue today minus prev_days: default 5 days')
def main(contracts: str,
         exclude_contract_csv_file: str,
         multiplier: int,
         timespan: str,
         arctic_server_address: str,
         arctic_library: str,
         redis_server_address: str,
         redis_server_port: int,
         enqueue: bool,
         enqueue_financial: bool,
         prev_days: int):

    if not contracts:
        raise ValueError('contract file or comma seperated list of conids is required')

    if enqueue or enqueue_financial:
        start_date = dateify(dt.datetime.now() - dt.timedelta(days=prev_days + 1), timezone='America/New_York')
        end_date = dateify(dt.datetime.now() - dt.timedelta(days=4), timezone='America/New_York')

        read_type = 'price data' if enqueue else 'financial data'
        logging.info('enqueuing polygon {} from {} to {}'.format(read_type,
                                                                 start_date,
                                                                 dateify(dt.datetime.now() - dt.timedelta(days=1))))
        # queue up history
        queuer = PolygonQueuer(arctic_server_address,
                               arctic_library,
                               redis_server_address,
                               redis_server_port)
        if os.path.exists(contracts):
            # here's where we look at our symbol data
            included = Helpers.contracts_from_df(pd.read_csv(contracts))
            excluded = []
            if exclude_contract_csv_file and os.path.exists(exclude_contract_csv_file):
                excluded = Helpers.contracts_from_df(pd.read_csv(exclude_contract_csv_file))

            contracts_to_queue = [s for s in included if s not in excluded]
            if enqueue_financial:
                queuer.queue_financials(contracts=contracts_to_queue)
            else:
                queuer.queue_history(contracts=contracts_to_queue,
                                     multiplier=multiplier,
                                     timespan=timespan,
                                     start_date=start_date,
                                     end_date=end_date)
        elif ',' in contracts:
            symbols = contracts.split(',')
            data = TickData(arctic_server_address, arctic_library)
            contracts_to_queue = [data.symbol_to_contract(s) for s in symbols]
            if enqueue_financial:
                queuer.queue_financials(contracts=contracts_to_queue)
            else:
                queuer.queue_history(contracts=contracts_to_queue,
                                     multiplier=multiplier,
                                     timespan=timespan,
                                     start_date=start_date,
                                     end_date=end_date)
        else:
            raise ValueError('valid contract file or comma seperated list of conids is required')
    else:
        raise ValueError('todo not done')


if __name__ == '__main__':
    main()
