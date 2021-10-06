import sys
import os
import click
import tempfile
import click_repl
import asyncio
from dataclasses import asdict

# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

import logging
import coloredlogs
from trader.data.universe import Universe, UniverseAccessor
from trader.listeners.ibaiorx import IBAIORx
from trader.common.helpers import rich_table
from trader.common.command_line import common_options, default_config, cli
from scripts.ib_resolve import IBResolver
from eoddata_scraper import EodDataScraper
from ib_resolve import main as ib_resolve_main
from prompt_toolkit.history import FileHistory

def build_and_load(
    ib_server_address: str,
    ib_server_port: int,
    arctic_server_address: str,
    arctic_universe_library: str,
    sectype: str,
    exchange: str,
    primary_exchange: str,
    currency: str,
    csv_output_file: str
):
    print('starting {} universe bootstrap temp file'.format(primary_exchange))
    n = EodDataScraper()
    result = n.scrape(primary_exchange)
    securities = len(result)
    result.to_csv(csv_output_file, header=True, index=False)

    client = IBAIORx(ib_server_address, ib_server_port)
    client.connect()
    resolver = IBResolver(client)
    asyncio.run(resolver.fill_csv(csv_output_file, csv_output_file, sectype, exchange, primary_exchange, currency))

    u = UniverseAccessor(arctic_server_address, arctic_universe_library)
    with open(csv_output_file, 'r') as f:
        csv_string = f.read()
        print('updating trader host with new universe')
        u.update_from_csv_str(primary_exchange, csv_string)
    print('finished {} bootstrap, with {} securities loaded'.format(primary_exchange, str(securities)))


def delete_bootstrap(arctic_server_address: str, arctic_universe_library: str):
    u = UniverseAccessor(arctic_server_address, arctic_universe_library)
    names = u.list_universes()
    for n in names:
        print('deleting {}'.format(n))
        u.delete(n)


@cli.command()
@common_options()
@default_config()
def bootstrap(
    ib_server_address: str,
    ib_server_port: int,
    arctic_server_address: str,
    arctic_universe_library: str, **args
):
    delete_bootstrap(arctic_server_address, arctic_universe_library)

    tf = tempfile.NamedTemporaryFile(suffix='.csv')
    build_and_load(
        ib_server_address,
        ib_server_port,
        arctic_server_address,
        arctic_universe_library,
        'STK',
        'SMART',
        'NASDAQ',
        'USD',
        tf.name
    )

    tf = tempfile.NamedTemporaryFile(suffix='.csv')
    build_and_load(
        ib_server_address,
        ib_server_port,
        arctic_server_address,
        arctic_universe_library,
        'STK',
        'SMART',
        'ASX',
        'AUD',
        tf.name
    )

    tf = tempfile.NamedTemporaryFile(suffix='.csv')
    build_and_load(
        ib_server_address,
        ib_server_port,
        arctic_server_address,
        arctic_universe_library,
        'STK',
        'SMART',
        'LSE',
        'GBP',
        tf.name
    )


@cli.command('list')
@common_options()
@default_config()
def list_universe(arctic_server_address: str, arctic_universe_library: str, **args):
    universes = UniverseAccessor(arctic_server_address, arctic_universe_library).list_universes_count()
    for name, count in universes.items():
        print('universe: {}, symbols {}'.format(name, str(count)))
    return 0


@cli.command()
@click.option('--name', required=True, help='Name of universe')
@common_options()
@default_config()
def get(name: str, arctic_server_address: str, arctic_universe_library: str, **args):
    if get:
        accessor = UniverseAccessor(arctic_server_address, arctic_universe_library)
        universe = accessor.get(name)
        list_definitions = [asdict(security) for security in universe.security_definitions]
        rich_table(list_definitions, True)
        return 0


@cli.command()
@click.option('--name', help='Name of the universe to create')
@click.option('--csv_file', help='Csv file of securities to load into universe')
@common_options()
@default_config()
def create(
    name: str,
    csv_file: str,
    ib_server_address: str,
    ib_server_port: int,
    arctic_server_address: str,
    arctic_universe_library: str
):
    client = IBAIORx(ib_server_address, ib_server_port)
    client.connect()

    resolver = IBResolver(client)
    temp_file = tempfile.NamedTemporaryFile(suffix='.csv')
    print('resolving to {}'.format(temp_file.name))
    asyncio.run(resolver.fill_csv(csv_file, temp_file.name))

    u = UniverseAccessor(arctic_server_address, arctic_universe_library)
    with open(temp_file.name, 'r') as f:
        csv_string = f.read()
        print('updating trader host with new universe')
        counter = u.update_from_csv_str(name, csv_string)
    print('finished loading {}, with {} securities loaded'.format(create, str(counter)))


if __name__ == '__main__':
    cli(obj={})
