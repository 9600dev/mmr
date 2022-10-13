import os
import sys


# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from trader.batch.ib_history_batch import IBHistoryQueuer
from trader.common.command_line import cli_norepl, common_options, default_config
from trader.common.helpers import dateify
from trader.common.logging_helper import setup_logging
from trader.data.universe import UniverseAccessor
from trader.objects import BarSize

import click
import datetime as dt


logging = setup_logging(module_name='ib_history_queuer')


@cli_norepl.command()
@click.option('--universe', required=True, help='name of universe to grab history for')
@click.option('--bar_size', required=True, default='1 min', help='IB bar size: 1 min')
@click.option('--prev_days', required=True, default=5, help='Enqueue today minus prev_days: default 5 days')
@common_options()
@default_config()
def get_universe_history_ib(
    ib_server_address: str,
    ib_server_port: int,
    arctic_server_address: str,
    redis_server_address: str,
    redis_server_port: int,
    arctic_universe_library: str,
    universe: str,
    bar_size: str,
    prev_days: int,
    **args
):
    bar_size_enum = BarSize.parse_str(bar_size)

    # queue up history
    queuer = IBHistoryQueuer(
        ib_server_address,
        ib_server_port,
        arctic_server_address,
        bar_size_enum,
        redis_server_address,
        redis_server_port,
    )

    start_date = dateify(dt.datetime.now() - dt.timedelta(days=prev_days + 1), timezone='America/New_York')
    logging.info('enqueing IB history from {} to {} days'.format(start_date, prev_days))

    accessor = UniverseAccessor(arctic_server_address, arctic_universe_library)
    u = accessor.get(universe)
    queuer.queue_history(u.security_definitions, start_date)


@cli_norepl.command()
@click.option('--symbol', required=True, help='conid of the security to backfill')
@click.option('--universe', required=True, help='name of universe to the security lives in')
@click.option('--arctic_universe_library', required=True, help='arctic library that contains universe definitions')
@click.option('--bar_size', required=True, default='1 min', help='IB bar size: 1 min')
@click.option('--prev_days', required=True, default=5, help='Enqueue today minus prev_days: default 5 days')
@common_options()
@default_config()
def get_symbol_history_ib(
    ib_server_address: str,
    ib_server_port: int,
    arctic_server_address: str,
    redis_server_address: str,
    redis_server_port: int,
    universe: str,
    symbol: str,
    arctic_universe_library: str,
    bar_size: str,
    prev_days: int,
    **args
):

    bar_size_enum = BarSize.parse_str(bar_size)

    # queue up history
    queuer = IBHistoryQueuer(
        ib_server_address,
        ib_server_port,
        arctic_server_address,
        bar_size_enum,
        redis_server_address,
        redis_server_port
    )
    start_date = dateify(dt.datetime.now() - dt.timedelta(days=prev_days + 1), timezone='America/New_York')
    logging.info('enqueing IB history from {} to {} days'.format(start_date, prev_days))

    accessor = UniverseAccessor(arctic_server_address, arctic_universe_library)
    u = accessor.get(universe)

    # get a security definition
    security_definition = u.find_symbol(symbol)
    if security_definition:
        queuer.queue_history([security_definition], start_date)
    else:
        logging.debug('cannot find symbol {} in universe {}'.format(symbol, universe))
