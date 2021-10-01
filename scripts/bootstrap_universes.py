import sys
import os
import csv
import click
import tempfile
from dataclasses import asdict

# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

import logging
import coloredlogs
from trader.data.universe import Universe, UniverseAccessor
from eoddata_scraper import EodDataScraper
from ib_resolve import main as ib_resolve_main

def build_and_load(
    ib_server_address: str,
    ib_server_port: int,
    arctic_server_address: str,
    arctic_universe_library: str,
    sectype: str,
    exchange: str,
    primary_exchange: str,
    currency: str,
    csv_file: str
):
    print('starting {} universe bootstrap temp file {}'.format(primary_exchange, csv_file))
    n = EodDataScraper()
    result = n.scrape(primary_exchange)
    securities = len(result)
    result.to_csv(csv_file, header=True, index=False)

    print(securities)

    cli = '--ib_server_address {} --full --sectype {} --exchange {} --primary_exchange {} --currency {} --csv_file {}'.format(
        ib_server_address, sectype, exchange, primary_exchange, currency, csv_file
    )

    os.system('python3 ib_resolve.py {}'.format(cli))
    # ib_resolve_main(cli.split(' '))

    u = UniverseAccessor(arctic_server_address, arctic_universe_library)
    print('got here')
    with open(csv_file, 'r') as f:
        csv_string = f.read()
        print('updating trader host with new universe')
        u.update_from_csv_str(primary_exchange, csv_string)
    print('finished {} bootstrap, with {} securities loaded'.format(primary_exchange, str(securities)))


def refresh_bootstrap(arctic_server_address: str, arctic_universe_library: str):
    u = UniverseAccessor(arctic_server_address, arctic_universe_library)
    names = u.list_universes()
    for n in names:
        print('deleting {}'.format(n))
        u.delete(n)


@click.command()
@click.option('--ib_server_address', required=False, default='127.0.0.1', help='127.0.0.1 ib server address')
@click.option('--ib_server_port', required=False, default=7496, help='port for tws server api: 7496')
@click.option('--arctic_server_address', required=False, default='127.0.0.1', help='arctic server address: 127.0.0.1')
@click.option('--arctic_universe_library', required=False, default='Universes', help='arctic universe library: Universes')
@click.option('--bootstrap', required=False, is_flag=True, default=False, help='delete, and re-bootstrap NASDAQ, ASX, LSE startup universes')
@click.option('--list', required=False, is_flag=True, default=False, help='list universes')
@click.option('--get', required=False, help='get all security definitions for given universe')
@click.option('--create_universe', required=False, help='create a new universe and load it from --load_csv file')
@click.option('--load_csv', required=False, help='read csv file containing symbols for universe')
def main(ib_server_address: str,
         ib_server_port: int,
         arctic_server_address: str,
         arctic_universe_library: str,
         bootstrap: bool,
         list: bool,
         get: str,
         create_universe: str,
         load_csv: str):

    if list:
        universes = UniverseAccessor(arctic_server_address, arctic_universe_library).list_universes_count()
        for name, count in universes.items():
            print('universe: {}, symbols {}'.format(name, str(count)))
        return 0

    if get:
        accessor = UniverseAccessor(arctic_server_address, arctic_universe_library)
        universe = accessor.get(get)
        for security in universe.security_definitions:
            print(asdict(security))
        return 0

    if bootstrap:
        refresh_bootstrap(arctic_server_address, arctic_universe_library)

        tf = tempfile.NamedTemporaryFile()
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

        tf = tempfile.NamedTemporaryFile()
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

        tf = tempfile.NamedTemporaryFile()
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


if __name__ == '__main__':
    main()
