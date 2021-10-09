import os
import sys
from prompt_toolkit.shortcuts import prompt
import requests
import click
import pandas as pd
import scripts.universes_cli as universes_cli
import trader.batch.ib_history_queuer as ib_history_queuer
import click_repl

from typing import Type, TypeVar, Dict, Optional, List, cast, Tuple
from trader.container import Container
from trader.data.data_access import SecurityDefinition, TickData, DictData
from trader.data.universe import Universe, UniverseAccessor
from scripts.trader_check import health_check
from ib_insync.ib import IB
from ib_insync.contract import Contract, ContractDescription, Stock
from ib_insync.objects import PortfolioItem

from arctic import Arctic
from trader.common.helpers import rich_table, rich_dict, rich_json, DictHelper
from IPython import get_ipython
from pyfiglet import Figlet
from prompt_toolkit.history import FileHistory, InMemoryHistory
from lightbus import BusPath
from ib_insync.objects import Position
from expression.collections import Seq, seq
from expression import Nothing, Option, Some, effect, option, pipe
from trader.common.command_line import cli, common_options, default_config
from trader.listeners.ibaiorx import IBAIORx
from click_help_colors import HelpColorsGroup
from trader.common.logging_helper import setup_logging, suppress_external
from trader.messaging.bus import *
from trader.common.helpers import *

logging = setup_logging(module_name='cli')
is_repl = False
bus_client: BusPath = bus

@click.group(
    invoke_without_command=True,
    cls=HelpColorsGroup,
    help_headers_color='yellow',
    help_options_color='green')
@click.pass_context
def main(ctx):
    if ctx.invoked_subcommand is None:
        ctx.invoke(repl)


@main.command()
@click.argument('subcommand', required=False)
@click.pass_context
def help(ctx, subcommand):
    subcommand_obj = main.get_command(ctx, subcommand)
    if subcommand_obj is None:
        click.echo(click.get_current_context().find_root().get_help())
    else:
        click.echo(subcommand_obj.get_help(ctx))


@cli.command()
def repl():
    global is_repl

    prompt_kwargs = {
        'history': FileHistory(os.path.expanduser('/tmp/.trader.history')),
        'vi_mode': True,
        'message': 'mmr> '
    }
    is_repl = True
    f = Figlet(font='starwars')
    click.echo(f.renderText('MMR'))
    click.echo(click.get_current_context().find_root().get_help())
    click.echo()
    click.echo('Ctrl-D or \'exit\' to exit')
    click_repl.repl(click.get_current_context(), prompt_kwargs=prompt_kwargs)


@main.group()
def universes():
    pass

universes.add_command(universes_cli.list_universe)
universes.add_command(universes_cli.create)
universes.add_command(universes_cli.bootstrap)
universes.add_command(universes_cli.get)


@main.group()
def batch():
    pass

batch.add_command(ib_history_queuer.ib_history)


@main.command()
def portfolio():
    portfolio: List[Dict] = cast(List[Dict], bus_client.service.get_portfolio())

    def mapper(portfolio_dict: Dict) -> List:
        portfolio = DictHelper[PortfolioItem].to_object(portfolio_dict)
        return [
            portfolio.account,
            portfolio.contract.conId,
            portfolio.contract.localSymbol,
            portfolio.contract.currency,
            portfolio.position,
            portfolio.marketPrice,
            portfolio.marketValue,
            portfolio.averageCost,
            portfolio.unrealizedPNL,
            portfolio.realizedPNL,
        ]

    xs = pipe(
        portfolio,
        seq.map(mapper)
    )

    df = pd.DataFrame(data=list(xs), columns=[
        'account', 'conId', 'localSymbol', 'currency',
        'position', 'marketPrice', 'marketValue', 'averageCost', 'unrealizedPNL', 'realizedPNL'
    ])

    rich_table(df.sort_values(by='currency'), csv=is_repl, financial=True, financial_columns=[
        'marketPrice', 'marketValue', 'averageCost', 'unrealizedPNL', 'realizedPNL'
    ])

    rich_table(df.groupby(by=['currency'])['marketValue'].sum().reset_index(), financial=True, csv=False)
    rich_table(df.groupby(by=['currency'])['unrealizedPNL'].sum().reset_index(), financial=True, csv=False)

@main.command()
def positions():
    positions: List[Dict] = cast(List[Dict], bus_client.service.get_positions())

    def mapper(position_dict: Dict) -> List:
        # this DictHelper thingy is required because lightbus is deserializing
        # RPC calls to Dicts for whatever reason. todo
        position = DictHelper[Position].to_object(position_dict)
        return [
            position.account,
            position.contract.conId,
            position.contract.localSymbol,
            position.contract.exchange,
            position.position,
            position.avgCost,
            position.contract.currency,
            position.position * position.avgCost
        ]

    xs = pipe(
        positions,
        seq.map(mapper)
    )

    df = pd.DataFrame(data=list(xs), columns=['account', 'conId', 'localSymbol', 'exchange', 'position', 'avgCost', 'currency', 'total'])
    rich_table(df.sort_values(by='currency'), financial=True, financial_columns=['total', 'avgCost'], csv=is_repl)
    if is_repl:
        rich_table(df.groupby(by=['currency'])['total'].sum().reset_index(), financial=True)


@main.command()
def reconnect():
    bus_client.service.reconnect()


@main.command()
def clear():
    print(chr(27) + "[2J")


@main.command()
@click.option('--symbol', required=True, help='symbol to resolve to conId')
@common_options()
@default_config()
def resolve(
    symbol: str,
    arctic_server_address: str,
    arctic_universe_library: str,
    **args,
):
    symbol = symbol.lower()
    accessor = UniverseAccessor(arctic_server_address, arctic_universe_library)
    results = []
    for universe in accessor.get_all():
        for definition in universe.security_definitions:
            if symbol in definition.symbol.lower():
                results.append({
                    'universe': universe.name,
                    'symbol': definition.symbol,
                    'exchange': definition.exchange,
                    'primaryExchange': definition.primaryExchange,
                    'currency': definition.currency,
                    'longName': definition.longName,
                    'category': definition.category,
                })
    rich_table(results, False)


@main.group()
def pycron():
    pass


@pycron.command('info')
@default_config()
def pycron_info():
    pycron_server_address = Container().config()['pycron_server_address']
    pycron_server_port = Container().config()['pycron_server_port']
    response = requests.get('http://{}:{}'.format(pycron_server_address, pycron_server_port))
    rich_json(response.json())


@pycron.command('restart')
@click.option('--service', required=True, help='service to restart')
def pycron_restart(
    service: str,
):
    pycron_server_address = Container().config()['pycron_server_address']
    pycron_server_port = Container().config()['pycron_server_port']
    response = requests.get('http://{}:{}/?restart={}'.format(pycron_server_address, pycron_server_port, service))
    print(response.json())
    rich_json(response.json())


@main.command()
@click.option('--symbol', required=True, help='symbol of security')
def company_info(symbol: str):
    symbol = symbol.lower()
    financials = Container().resolve(DictData, arctic_library='HistoricalPolygonFinancials')
    arctic_server_address = Container().config()['arctic_server_address']
    arctic_universe_library = Container().config()['arctic_universe_library']
    accessor = UniverseAccessor(arctic_server_address, arctic_universe_library)

    for universe in accessor.get_all():
        for definition in universe.security_definitions:
            if symbol in definition.symbol.lower():
                info = financials.read(definition)
                if info:
                    rich_table(financials.read(definition).financials, True)  # type: ignore
                    return


@main.command()
def status():
    result = health_check(Container().config_file)
    rich_dict({'status': result})

@main.command('exit')
def def_exit():
    os._exit(os.EX_OK)


container: Container
amd = Contract(symbol='AMD', conId=4391, exchange='SMART', primaryExchange='NASDAQ', currency='USD')
accessor: UniverseAccessor
client: IBAIORx
store: Arctic


def setup_ipython():
    global container
    global accessor
    global client
    global store

    container = Container()
    accessor = container.resolve(UniverseAccessor)
    client = container.resolve(IBAIORx)
    client.connect()
    store = Arctic(mongo_host=container.config()['arctic_server_address'])


if get_ipython().__class__.__name__ == 'TerminalInteractiveShell':  # type: ignore
    setup_ipython()

if __name__ == '__main__':
    main(obj={})

