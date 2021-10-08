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

from trader.common.command_line import common_options, default_config, cli_norepl
from trader.data.data_access import TickData
from trader.common.logging_helper import setup_logging
from trader.common.helpers import date_range, dateify, symbol_to_contract
from trader.common.listener_helpers import Helpers
from trader.batch.ib_history_batch import IBHistoryQueuer, IBHistoryWorker
from trader.data.universe import UniverseAccessor, Universe

logging = setup_logging(module_name='ib_history_queuer')

@cli_norepl.command()
@click.option('--universe', required=True, help='name of universe to grab history for')
@click.option('--arctic_universe_library', required=True, help='arctic library that contains universe definitions')
@click.option('--bar_size', required=True, default='1 min', help='IB bar size: 1 min')
@click.option('--prev_days', required=True, default=5, help='Enqueue today minus prev_days: default 5 days')
@common_options()
@default_config()
def ib_history(
    ib_server_address: str,
    ib_server_port: int,
    arctic_server_address: str,
    redis_server_address: str,
    redis_server_port: int,
    universe: str,
    arctic_universe_library: str,
    bar_size: str,
    prev_days: int,
    **args
):

    # queue up history
    queuer = IBHistoryQueuer(
        ib_server_address,
        ib_server_port,
        arctic_server_address,
        universe,
        redis_server_address,
        redis_server_port
    )

    start_date = dateify(dt.datetime.now() - dt.timedelta(days=prev_days + 1), timezone='America/New_York')
    logging.info('enqueing IB history from {} to {} days'.format(start_date, prev_days))

    accessor = UniverseAccessor(arctic_server_address, arctic_universe_library)
    u = accessor.get(universe)
    queuer.queue_history(u.security_definitions, bar_size, start_date)


if __name__ == '__main__':
    ib_history()
