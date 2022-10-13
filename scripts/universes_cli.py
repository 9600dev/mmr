

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
from ib_insync import Contract, ContractDescription
from scripts.ib_instrument_scraper import IBInstrument, scrape_products
from scripts.ib_resolve import IBResolver
from trader.common.command_line import cli, common_options, default_config
from trader.common.helpers import rich_list, rich_table
from trader.common.logging_helper import setup_logging
from trader.data.universe import SecurityDefinition, Universe, UniverseAccessor
from trader.listeners.ibreactive import IBAIORx
from typing import cast, List, Optional, Union


logging = setup_logging(module_name='cli')

def build_and_load_ib(
    ib_server_address: str,
    ib_server_port: int,
    arctic_server_address: str,
    arctic_universe_library: str,
):
    logging.debug('build_and_load_ib()')

    product_pages = {
        # Stocks
        'NASDAQ': 'https://www.interactivebrokers.com/en/index.php?f=2222&exch=nasdaq&showcategories=STK',
        'NYSE': 'https://www.interactivebrokers.com/en/index.php?f=2222&exch=nyse&showcategories=ETF',
        'LSE': 'https://www.interactivebrokers.com/en/index.php?f=2222&exch=lse&showcategories=ETF',
        'ASX': 'https://www.interactivebrokers.com/en/index.php?f=2222&exch=asx&showcategories=ETF',

        # Options, Futures, Agri
        'CFE': 'https://www.interactivebrokers.com/en/index.php?f=2222&exch=cfe&showcategories=FUTGRP',
        'GLOBEX': 'https://www.interactivebrokers.com/en/index.php?f=2222&exch=globex&showcategories=OPTGRP',
        'NYMEX': 'https://www.interactivebrokers.com/en/index.php?f=2222&exch=nymex&showcategories=OPTGRP',
        'NYBOT': 'https://www.interactivebrokers.com/en/index.php?f=2222&exch=nybot&showcategories=OPTGRP',
        'ICEUS': 'https://www.interactivebrokers.com/en/index.php?f=2222&exch=iceus&showcategories=FUTGRP',
        'CBOE': 'https://www.interactivebrokers.com/en/index.php?f=2222&exch=cboe&showcategories=ETF',
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
            if (
                not universe.find_contract(instrument.to_contract())
                # we don't do warrants yet
                and instrument.secType != 'WAR'
            ):
                contract_details = asyncio.run(resolver.resolve_contract(instrument.to_contract()))
                if contract_details:
                    click.echo('adding instrument {} to universe {}'.format(instrument, market))
                    universe.security_definitions.append(SecurityDefinition.from_contract_details(contract_details))
            else:
                logging.debug('instrument {} already found in a universe {}'.format(instrument, market))

        # update the universe
        accessor.update(universe)
        # weird bug where universe.security_definitions was keeping the previous list object around
        # after calling accessor.get(). bizaare
        universe = None


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
    arctic_universe_library: str,
    **args,
):
    build_and_load_ib(ib_server_address, ib_server_port, arctic_server_address, arctic_universe_library)

    macro_defaults = [
        294530233,  # BZ, Brent Crude
        256019308,  # CL, Light Sweet Crude Oil
        457630923,  # GC, Gold
        484743936,  # SI, Silver
        484743956,  # HG, Copper Index
        344273380,  # HH, Natural Gas
        385575948,  # UX, Uranium
        578106878,  # LBR, Lumber Futures
        568549458,  # MNQ, Micro E-Mini Nasdaq 100
        495512551,
    ]

    # add the macro universe
    for conId in macro_defaults:
        add_to_universe_helper(
            name='macro',
            symbol=conId,
            primary_exchange='',
            sec_type='',
            ib_server_address=ib_server_address,
            ib_server_port=ib_server_port,
            arctic_server_address=arctic_server_address,
            arctic_universe_library=arctic_universe_library
        )


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
@click.option('--csv_file', help='optional csv file of securities to load into universe')
@common_options()
@default_config()
def create(
    name: str,
    csv_file: str,
    ib_server_address: str,
    ib_server_port: int,
    arctic_server_address: str,
    arctic_universe_library: str,
    **args,
):
    client = IBAIORx(ib_server_address, ib_server_port)
    client.connect()

    u = UniverseAccessor(arctic_server_address, arctic_universe_library)

    if csv_file:
        resolver = IBResolver(client)
        temp_file = tempfile.NamedTemporaryFile(suffix='.csv')
        click.echo('resolving to {}'.format(temp_file.name))
        asyncio.run(resolver.fill_csv(csv_file, temp_file.name))

        with open(temp_file.name, 'r') as f:
            csv_string = f.read()
            click.echo('updating trader host with new universe')
            counter = u.update_from_csv_str(name, csv_string)
        click.echo('finished loading {}, with {} securities loaded'.format(create, str(counter)))
    else:
        result = u.get(name)
        u.update(result)


def add_to_universe_helper(
    name: str,
    symbol: Union[str, int],
    primary_exchange: str,
    sec_type: str,
    ib_server_address: str,
    ib_server_port: int,
    arctic_server_address: str,
    arctic_universe_library: str,
    **args
):
    client = IBAIORx(ib_server_address, ib_server_port)
    client.connect()

    # conId
    if type(symbol) is int:
        contract_details = client.get_contract_details(Contract(conId=symbol))
        if len(contract_details) > 0:
            logging.debug('found contract details for {}: {}'.format(symbol, contract_details))
            u = UniverseAccessor(arctic_server_address, arctic_universe_library)
            universe = u.get(name)
            # todo using the first element in the list is probably a bug, should fix this
            universe.security_definitions.append(SecurityDefinition.from_contract_details(contract_details[0]))
            u.update(universe)
            logging.debug('adding contract {} to universe: {}'.format(symbol, name))
    # string symbol
    else:
        result = asyncio.run(
            client.get_contract_description(cast(str, symbol), secType=sec_type, primaryExchange=primary_exchange)
        )
        if result is list:
            click.echo('multiple symbols found:')
            rich_list(cast(list, result))
            click.echo('use primary_exchange to be more specific')
        elif type(result) is ContractDescription and result is not None:
            description = cast(ContractDescription, result)

            contract_details = client.get_contract_details(cast(Contract, description.contract))
            logging.debug('found contract details for {}: {}'.format(symbol, contract_details))
            u = UniverseAccessor(arctic_server_address, arctic_universe_library)
            universe = u.get(name)
            # todo using the first element in the list is probably a bug, should fix this
            universe.security_definitions.append(SecurityDefinition.from_contract_details(contract_details[0]))
            u.update(universe)
            logging.debug('adding contract {} to universe: {}'.format(symbol, name))
        else:
            click.echo('no security definition found for {}'.format(symbol))


@cli.command('add-to-universe')
@click.option('--name', help='Name of the universe to add instrument to')
@click.option('--symbol', help='symbol or conId to add to universe')
@click.option('--primary_exchange', default='SMART', help='primary exchange the symbol is listed on default [SMART]')
@click.option('--sec_type', required=True, default='STK', help='security type [default STK]')
@common_options()
@default_config()
def add_to_universe(
    name: str,
    symbol: str,
    primary_exchange: str,
    sec_type: str,
    ib_server_address: str,
    ib_server_port: int,
    arctic_server_address: str,
    arctic_universe_library: str,
    **args
):
    add_to_universe_helper(
        name,
        symbol,
        primary_exchange,
        sec_type,
        ib_server_address,
        ib_server_port,
        arctic_server_address,
        arctic_universe_library,
    )


if __name__ == '__main__':
    cli(obj={})
