
import asyncio
import click
import os
import sys
import tempfile


# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from dataclasses import asdict
from scripts.eoddata_scraper import EodDataScraper
from scripts.ib_instrument_scraper import IBInstrument, scrape_products
from scripts.ib_resolve import IBResolver
from trader.common.command_line import cli, common_options, default_config
from trader.common.helpers import rich_table
from trader.common.logging_helper import setup_logging
from trader.data.universe import SecurityDefinition, Universe, UniverseAccessor
from trader.listeners.ibreactive import IBAIORx
from typing import List, Optional


logging = setup_logging(module_name='cli')

def build_and_load_ib(
    ib_server_address: str,
    ib_server_port: int,
    arctic_server_address: str,
    arctic_universe_library: str,
):
    logging.debug('build_and_load_ib()')

    product_pages = {
        # 'NASDAQ': 'https://www.interactivebrokers.com/en/index.php?f=2222&exch=nasdaq&showcategories=STK',
        # 'NYSE': 'https://www.interactivebrokers.com/en/index.php?f=2222&exch=nyse&showcategories=ETF',
        'LSE': 'https://www.interactivebrokers.com/en/index.php?f=2222&exch=lse&showcategories=ETF',
        'ASX': 'https://www.interactivebrokers.com/en/index.php?f=2222&exch=asx&showcategories=ETF',
    }

    accessor = UniverseAccessor(arctic_server_address, arctic_universe_library)
    client = IBAIORx(ib_server_address, ib_server_port)
    client.connect()
    resolver = IBResolver(client)
    universe: Optional[Universe] = None

    for market, url in product_pages.items():
        instruments: List[IBInstrument] = []
        logging.debug('scrape_products() {}'.format(url))
        instruments = scrape_products(market, url)

        universe = accessor.get(market)

        for instrument in instruments:
            if not universe.find_contract(instrument.to_contract()) and instrument.secType != 'OPT' and instrument.secType != 'WAR':
                contract_details = asyncio.run(resolver.resolve_contract(instrument.to_contract()))
                if contract_details:
                    universe.security_definitions.append(SecurityDefinition.from_contract_details(contract_details))
            else:
                logging.debug('instrument {} already found in a universe {}'.format(instrument, market))

        # update the universe
        accessor.update(universe)
        # weird bug where universe.security_definitions was keeping the previous list object around
        # after calling accessor.get(). bizaare
        universe = None

def build_and_load_deprecated(
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
    click.echo('starting {} universe bootstrap temp file'.format(primary_exchange))
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
        click.echo('updating trader host with new universe')
        u.update_from_csv_str(primary_exchange, csv_string)
    click.echo('finished {} bootstrap, with {} securities loaded'.format(primary_exchange, str(securities)))


@cli.command()
@click.option('--name', required=True, help='Name of universe')
@common_options()
@default_config()
def delete(
    name: str,
    arctic_server_address: str,
    arctic_universe_library: str,
    **args
):
    u = UniverseAccessor(arctic_server_address, arctic_universe_library)
    if name not in u.list_universes():
        click.echo('Universe {} cannot be deleted, not found'.format(name))
    else:
        click.echo('deleting {}'.format(name))
        u.delete(name)


@cli.command()
@common_options()
@default_config()
def bootstrap(
    ib_server_address: str,
    ib_server_port: int,
    arctic_server_address: str,
    arctic_universe_library: str, **args
):
    build_and_load_ib(ib_server_address, ib_server_port, arctic_server_address, arctic_universe_library)


@cli.command('list')
@common_options()
@default_config()
def list_universe(arctic_server_address: str, arctic_universe_library: str, **args):
    universes = UniverseAccessor(arctic_server_address, arctic_universe_library).list_universes_count()
    for name, count in universes.items():
        click.echo('universe: {}, symbols {}'.format(name, str(count)))
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
@click.option('--name', required=True, help='Name of universe')
@common_options()
@default_config()
def destroy(name: str, arctic_server_address: str, arctic_universe_library: str, **args):
    if get:
        accessor = UniverseAccessor(arctic_server_address, arctic_universe_library)
        universe = accessor.delete(name)
        return 0


@cli.command()
@click.option('--name', help='Name of the universe to create')
@click.option('--csv_file', help='csv file of securities to load into universe')
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
    click.echo('resolving to {}'.format(temp_file.name))
    asyncio.run(resolver.fill_csv(csv_file, temp_file.name))

    u = UniverseAccessor(arctic_server_address, arctic_universe_library)
    with open(temp_file.name, 'r') as f:
        csv_string = f.read()
        click.echo('updating trader host with new universe')
        counter = u.update_from_csv_str(name, csv_string)
    click.echo('finished loading {}, with {} securities loaded'.format(create, str(counter)))


if __name__ == '__main__':
    cli(obj={})
