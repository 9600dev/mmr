from arctic.exceptions import NoDataFoundException
from click_help_colors import HelpColorsGroup
from click_option_group import optgroup, RequiredMutuallyExclusiveOptionGroup
from cloup import option as cloupoption
from cloup import option_group
from cloup.constraints import mutually_exclusive
from expression import pipe
from expression.collections import seq
from ib_insync import TradeLogEntry
from ib_insync.contract import Contract
from ib_insync.objects import PortfolioItem, Position
from ib_insync.order import Order, OrderStatus, Trade
from ib_insync.ticker import Ticker
from scripts.chain import plot_chain
from scripts.trader_check import health_check
from scripts.zmq_pub_listener import ZmqPrettyPrinter
from trader.batch.queuer import Queuer
from trader.cli.cli_renderer import CliRenderer
from trader.cli.command_line import common_options, default_config
from trader.common.exceptions import TraderConnectionException, TraderException
from trader.common.helpers import contract_from_dict, DictHelper, ListHelper
from trader.common.logging_helper import LogLevels, set_log_level, setup_logging
from trader.common.reactivex import SuccessFail
from trader.container import Container as TraderContainer
from trader.data.data_access import DictData, PortfolioSummary, TickData, TickStorage
from trader.data.universe import SecurityDefinition, Universe, UniverseAccessor
from trader.listeners.ibreactive import IBAIORx, WhatToShow
from trader.messaging.clientserver import consume, RPCClient
from trader.messaging.trader_service_api import TraderServiceApi
from trader.objects import BarSize, TradeLogSimple
from trader.trading.strategy import Strategy, StrategyConfig, StrategyState
from typing import Any, Callable, cast, Coroutine, Dict, List, Optional, Tuple, TypeVar, Union

import asyncio
import click
import click._compat as compat
import click.core
import datetime as dt
import os
import pandas as pd
import plotext as plt
import random
import requests
import trader.batch.ib_history_queuer as ib_history_queuer
import trader.cli.universes_cli as universes_cli


global cli_client_id
global remoted_client

cli_client_id = -1

logging = setup_logging(module_name='cli')  # type: ignore
is_repl = False

error_table = {
    'trader.common.exceptions.TraderException': TraderException,
    'trader.common.exceptions.TraderConnectionException': TraderConnectionException
}

invoke_context = None
container = TraderContainer()

remoted_client = RPCClient[TraderServiceApi](
    zmq_server_address=container.config()['zmq_rpc_server_address'],
    zmq_server_port=container.config()['zmq_rpc_server_port'],
    error_table=error_table,
    timeout=5,
)

renderer = CliRenderer()

from click.core import MultiCommand
from io import StringIO


# hijack click.echo to allow redirect to TUI
click_echo_context = click.echo
def monkeypatch_click_echo(stdout):
    global click_echo_context

    def closure(message, color=None, nl=True, err=False, **styles):
        click_echo_context(message=message, file=stdout, color=color, nl=nl, err=err, **styles)

    if not click_echo_context:
        click_echo_context = click.echo
    click.echo = closure
    click.utils.echo = closure
    click.core.echo = closure  # type: ignore


def connect():
    if not remoted_client.is_setup:
        asyncio.get_event_loop().run_until_complete(remoted_client.connect())


def setup_cli(cli_renderer: CliRenderer):
    global cli_client_id
    global renderer
    global remoted_client

    renderer = cli_renderer

    connect()

    try:
        logging.debug('getting client id from trader service')
        cli_client_id = remoted_client.rpc(return_type=int).get_unique_client_id()
    except asyncio.exceptions.TimeoutError:
        click.echo('Could not connect to trader service at {}:{}. Is it running?'.format(
            container.config()['zmq_rpc_server_address'],
            container.config()['zmq_rpc_server_port']
        ))
        cli_client_id = random.randint(50, 100)
        click.echo('using client id {}'.format(cli_client_id))

    return remoted_client, cli_client_id

def resolve_symbol_arctic(
    symbol: Union[int, str],
    arctic_server_address: str,
    arctic_universe_library: str,
) -> Optional[SecurityDefinition]:
    accessor = UniverseAccessor(arctic_server_address, arctic_universe_library)
    return ListHelper.first(accessor.resolve_symbol(symbol))


def resolve_symbol(
    symbol: Union[str, int],
    exchange: str = '',
    universe: str = '',
) -> List[SecurityDefinition]:
    return consume(remoted_client.rpc(return_type=list[SecurityDefinition]).resolve_symbol(symbol, exchange, universe))


def __resolve(
    symbol: Union[str, int],
    arctic_server_address: str,
    arctic_universe_library: str,
    exchange: str = '',
    universe: str = '',
) -> List[Dict[str, Any]]:
    # it's best to call the trader_runtime resolve method, as it can talk to Interactive Brokers if
    # the symbol is not found in any available universes
    temp_results: List[Tuple[str, SecurityDefinition]] = []

    if remoted_client.is_setup:
        temp_results.extend(
            consume(
                remoted_client.rpc(return_type=list[tuple[str, SecurityDefinition]]).resolve_universe(symbol, exchange, universe)
            )
        )
    else:
        accessor = UniverseAccessor(arctic_server_address, arctic_universe_library)
        temp_results.extend(accessor.resolve_universe_name(symbol=symbol, exchange=exchange, universe=universe))

    results: List[Dict] = []
    for universe_name, definition in temp_results:
        results.append({
            'universe': universe_name,
            'conId': definition.conId,
            'symbol': definition.symbol,
            'secType': definition.secType,
            'exchange': definition.exchange,
            'primaryExchange': definition.primaryExchange,
            'currency': definition.currency,
            'longName': definition.longName,
            'category': definition.category,
            'minTick': definition.minTick,
            'bondType': definition.bondType,
            'description': definition.description,
        })
    return results


def __resolve_contract(
    symbol: Union[str, int],
    arctic_server_address: str,
    arctic_universe_library: str,
    primary_exchange: str = ''
) -> List[Contract]:
    results = []
    descriptions = __resolve(symbol, arctic_server_address, arctic_universe_library, primary_exchange)
    for result in descriptions:
        results.append(Contract(
            conId=result['conId'],
            symbol=result['symbol'],
            exchange=result['exchange'],
            currency=result['currency'],
        ))
    return results


def invoke_context_wrapper(ctx):
    global invoke_context
    invoke_context = ctx


@click.group(
    'cli',
    invoke_without_command=True,
    cls=HelpColorsGroup,
    help_headers_color='yellow',
    help_options_color='green')
@click.pass_context
def cli(ctx):
    global invoke_context
    # todo: actually fix this pytz stuff throughout the codebase
    import warnings
    warnings.filterwarnings(
        'ignore',
        message='The zone attribute is specific to pytz\'s interface; please migrate to a new time zone provider. For more details on how to do so, see https://pytz-deprecation-shim.readthedocs.io/en/latest/migration.html'  # noqa: E501
    )

    if ctx.invoked_subcommand is None:
        set_log_level('cli', level=LogLevels.DEBUG)
        ctx.invoke(invoke_context)


@cli.command()
@click.argument('subcommand', required=False)
@click.pass_context
def help(ctx, subcommand):
    subcommand_obj = cli.get_command(ctx, subcommand)
    if subcommand_obj is None:
        click.echo(click.get_current_context().find_root().get_help())
    else:
        click.echo(subcommand_obj.get_help(ctx))


@cli.command()
@click.argument('subcommand', required=False)
@click.pass_context
def ls(ctx, subcommand):
    subcommand_obj = cli.get_command(ctx, subcommand)
    if subcommand_obj is None:
        click.echo(click.get_current_context().find_root().get_help())
    else:
        click.echo(subcommand_obj.get_help(ctx))


@cli.command()
def status():
    result = health_check(TraderContainer().config_file)
    renderer.rich_dict({'status': result})


@cli.command('exit')
def def_exit():
    os._exit(os.EX_OK)


@cli.command()
def pnl():
    result = remoted_client.rpc().get_pnl()
    renderer.rich_table(result)


@cli.group()
def universes():
    pass

universes.add_command(universes_cli.list_universe)
universes.add_command(universes_cli.get)
universes.add_command(universes_cli.destroy)

@universes.command()
@common_options()
@default_config()
def bootstrap(
    ib_server_address: str,
    ib_server_port: int,
    arctic_server_address: str,
    arctic_universe_library: str,
    ib_client_id: Optional[int] = None,
    **args,
):
    universes_cli.build_and_load_ib(
        ib_server_address,
        ib_server_port,
        ib_client_id if ib_client_id else cli_client_id,
        arctic_server_address,
        arctic_universe_library
    )

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
        universes_cli.add_to_universe_helper(
            name='macro',
            symbol=conId,
            primary_exchange='',
            sec_type='',
            ib_server_address=ib_server_address,
            ib_server_port=ib_server_port,
            ib_client_id=ib_client_id if ib_client_id else cli_client_id,
            arctic_server_address=arctic_server_address,
            arctic_universe_library=arctic_universe_library
        )


@universes.command(no_args_is_help=True)
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
    ib_client_id: Optional[int] = None,
    **args,
):
    with (IBAIORx(
        ib_server_address,
        ib_server_port,
        ib_client_id if ib_client_id else cli_client_id
    )) as client:

        u = UniverseAccessor(arctic_server_address, arctic_universe_library)

        if csv_file:
            from scripts.ib_resolve import IBResolver

            import tempfile

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
            logging.debug('created universe {}'.format(name))


@universes.command('add-to-universe', no_args_is_help=True)
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
    ib_client_id: Optional[int] = None,
    **args
):
    universes_cli.add_to_universe_helper(
        name,
        symbol,
        primary_exchange,
        sec_type,
        ib_server_address,
        ib_server_port,
        ib_client_id if ib_client_id else cli_client_id,
        arctic_server_address,
        arctic_universe_library,
    )


@universes.command('remove-from-universe', no_args_is_help=True)
@click.option('--name', help='Name of the universe to remove instrument from')
@click.option('--symbol', help='symbol or conId to add to universe')
@click.option('--primary_exchange', default='SMART', help='primary exchange the symbol is listed on default [SMART]')
@click.option('--sec_type', required=True, default='STK', help='security type [default STK]')
@common_options()
@default_config()
def remove_from_universe(
    name: str,
    symbol: str,
    primary_exchange: str,
    sec_type: str,
    ib_server_address: str,
    ib_server_port: int,
    arctic_server_address: str,
    arctic_universe_library: str,
    ib_client_id: Optional[int] = None,
    **args
):
    universes_cli.remove_from_universe_helper(
        name,
        symbol,
        primary_exchange,
        sec_type,
        ib_server_address,
        ib_server_port,
        ib_client_id if ib_client_id else cli_client_id,
        arctic_server_address,
        arctic_universe_library,
    )


@cli.group()
def history():
    pass

history.add_command(ib_history_queuer.get_universe_history_ib)
history.add_command(ib_history_queuer.get_symbol_history_ib)

@history.command('summary', no_args_is_help=True)
@click.option('--universe', required=True, help='universe to summarize')
@common_options()
@default_config()
def history_summary(
    universe: str,
    arctic_server_address: str,
    arctic_universe_library: str,
    **args,
):
    accessor = UniverseAccessor(arctic_server_address, arctic_universe_library)
    u = accessor.get(universe)

    for barsize_db in TickStorage(arctic_server_address).list_libraries():
        tick_data = TickStorage(arctic_server_address).get_tickdata(BarSize.parse_str(barsize_db))
        tick_data = TickData(arctic_server_address, barsize_db)
        examples: List[str] = tick_data.list_symbols()[0:10]

        result = {
            'universe': universe,
            'arctic_library': barsize_db,
            'bar_size': ' '.join(barsize_db.split('_')[-2:]),
            'security_definition_count': len(u.security_definitions),
            'history_db_symbol_count': len(tick_data.list_symbols()),
            'example_symbols': examples
        }
        renderer.rich_dict(result)


@history.command('read', no_args_is_help=True)
@click.option('--symbol', required=True, help='historical data statistics for symbol')
@click.option('--bar_size', required=True, default='1 min', help='bar size to read')
@common_options()
@default_config()
def history_read(
    symbol: str,
    bar_size: str,
    arctic_server_address: str,
    arctic_universe_library: str,
    **args,
):
    results: List[Contract] = __resolve_contract(symbol, arctic_server_address, arctic_universe_library)
    bar_size_enum = BarSize.parse_str(bar_size)

    if len(results) >= 1:
        data = TickStorage(arctic_server_address).get_tickdata(bar_size_enum).read(results[0])
        renderer.rich_table(data, csv=True, include_index=True)


@history.command('jobs')
@common_options()
@default_config()
def history_jobs(
    **args,
):
    container = TraderContainer()
    queuer = container.resolve(Queuer)
    renderer.rich_dict(queuer.current_queue())


@history.command('security', no_args_is_help=True)
@click.option('--symbol', required=True, help='historical data statistics for symbol')
@click.option('--primary_exchange', required=False, default='NASDAQ', help='exchange for symbol [default: NASDAQ]')
@common_options()
@default_config()
def history_security(
    symbol: str,
    primary_exchange: str,
    arctic_server_address: str,
    arctic_universe_library: str,
    **args,
):
    accessor = UniverseAccessor(arctic_server_address, arctic_universe_library)
    universes = accessor.list_universes()

    # todo: hacky as shit
    def in_universes(library: str):
        for u in universes:
            if u in library:
                return True
        return False

    def get_universe(library: str) -> Optional[Universe]:
        for u in universes:
            if u in library:
                return accessor.get(u)
        return None

    results: List[Dict[str, Any]] = __resolve(symbol, arctic_server_address, arctic_universe_library, primary_exchange)
    history_bar_sizes = TickStorage(arctic_server_address).list_libraries_barsize()
    output = []

    for result in results:
        for history_bar_size in history_bar_sizes:
            tick_data = TickStorage(arctic_server_address).get_tickdata(history_bar_size)

            for dict in results:
                try:
                    start_date, end_date = tick_data.date_summary(int(dict['conId']))
                    output.append({
                        'universe': dict['universe'],
                        'conId': dict['conId'],
                        'symbol': dict['symbol'],
                        'longName': dict['longName'],
                        'history_start': start_date,
                        'history_end': end_date,
                    })
                except NoDataFoundException:
                    pass
    renderer.rich_table(output)


@history.command('bar-sizes', no_args_is_help=True)
def history_bar_sizes():
    renderer.rich_list(BarSize.bar_sizes())


def book_helper() -> pd.DataFrame:
    connect()

    trades_result: dict[int, list[Trade]] = remoted_client.rpc(return_type=dict[int, list[Trade]]).get_trades()
    trades = [sublist[0] for sublist in trades_result.values()]

    def mapper(trade: Trade) -> List:
        return [
            trade.contract.conId,
            trade.contract.symbol,
            trade.order.orderId,
            trade.order.action,
            trade.orderStatus.status,
            trade.orderStatus.filled,
            trade.order.orderType,
            trade.order.lmtPrice,
            trade.order.totalQuantity,
        ]

    xs = pipe(
        trades,
        seq.map(mapper)
    )

    df = pd.DataFrame(data=xs, columns=[
        'conId', 'symbol', 'orderId', 'action', 'status', 'filled', 'orderType', 'lmtPrice', 'totalQuantity'
    ])

    return df.sort_values(by='orderId', ascending=True)


def portfolio_helper() -> pd.DataFrame:
    connect()
    portfolio: list[PortfolioSummary] = remoted_client.rpc(return_type=list[PortfolioSummary]).get_portfolio_summary()

    def mapper(portfolio: PortfolioSummary) -> List:
        # portfolio = PortfolioItem(*portfolio)
        return [
            portfolio.account,
            portfolio.contract.conId,
            portfolio.contract.localSymbol,
            portfolio.dailyPNL,
            portfolio.unrealizedPNL,
            portfolio.realizedPNL,
            portfolio.marketPrice,
            portfolio.contract.currency,
            portfolio.position,
            portfolio.marketValue,
            portfolio.averageCost,
        ]

    xs = pipe(
        portfolio,
        seq.map(mapper)
    )

    df = pd.DataFrame(data=xs, columns=[
        'account', 'conId', 'localSymbol', 'dailyPNL', 'unrealizedPNL', 'marketPrice', 'realizedPNL', 'currency',
        'position', 'marketValue', 'averageCost',
    ])

    return df.sort_values(by='dailyPNL', ascending=False)

def strategy_helper() -> pd.DataFrame:
    connect()
    strategy_list: SuccessFail[List[StrategyConfig]] = consume(
        remoted_client.rpc(return_type=SuccessFail[List[StrategyConfig]]).get_strategies()
    )
    if strategy_list.is_success() and strategy_list.obj:
        result = []
        for s in strategy_list.obj:
            result.append({
                'name': s.name,
                'conids': s.conids if s.conids else '[]',
                'paper': s.paper,
                'state': str(s.state),
                'bar_size': str(s.bar_size),
                'hist_days_prior': s.historical_days_prior,
            })
        return pd.DataFrame(result)
    else:
        return pd.DataFrame()


@cli.command()
def portfolio():
    df = portfolio_helper()
    renderer.rich_table(df, csv=is_repl, financial=True, financial_columns=[
        'marketPrice', 'marketValue', 'averageCost', 'dailyPNL', 'unrealizedPNL', 'realizedPNL', 'dailyPNL'
    ])


def pretty_group(
    name: Optional[str] = None,
    help: Optional[str] = None,
    cls=None,
    **attrs,
):
    return optgroup.group(f'\n{name}', help=f'\b--- {help}\n\n', cls=cls, **attrs)


@cli.command('plot', no_args_is_help=True)
@click.option('--symbol', required=True, help='historical data statistics for symbol')
@pretty_group('Standard plot', help='Plots data over given range and bar_size')
@optgroup.option('--exchange', required=False, default='', help='primary exchange')
@optgroup.option('--bar_size', required=True, default='1 min', help='bar size to read')
@optgroup.option('--prev_days', required=True, type=int, default=1, help='previous days to plot')
@pretty_group('Live plot', help='Subscribes to tick data and plots')
@optgroup.option('--live', is_flag=True, help='start live console plot of symbol')
@optgroup.option('--delayed', is_flag=True, help='use delayed data')
@optgroup.option('--height', default=0, help='height of plot [default: 0 for fullscreen]')
@optgroup.option('--topic', default='ticker', help='\b\nzmq topic, default="ticker"\n\n\n  ')
@common_options()
@default_config()
def plot(
    symbol: str,
    exchange: str,
    bar_size: str,
    prev_days: int,
    live: bool,
    delayed: bool,
    height: int,
    ib_server_address: str,
    ib_server_port: int,
    arctic_server_address: str,
    arctic_universe_library: str,
    zmq_pubsub_server_address: str,
    zmq_pubsub_server_port: int,
    topic: str,
    ib_client_id: Optional[int] = None,
    **args,
):
    results: List[Contract] = __resolve_contract(
        symbol,
        arctic_server_address,
        arctic_universe_library,
        primary_exchange=exchange
    )
    # todo fix all this resolution stuff up
    if len(results) == 0:
        raise click.ClickException('no contracts found for symbol {}, or symbol universe not yet created'.format(symbol))

    contract = results[0]

    if live:
        click.echo('subscribing to {}'.format(contract.symbol))
        remoted_client.rpc().publish_contract(contract, delayed)

        printer = ZmqPrettyPrinter(
            zmq_pubsub_server_address,
            zmq_pubsub_server_port,
            csv=False,
            live_graph=True,
            filter_symbol=symbol,
            height=height,
        )
        asyncio.get_event_loop().run_until_complete(printer.listen(topic))
    else:
        bar_size_enum = BarSize.parse_str(bar_size)
        start_date = dt.datetime.now() - dt.timedelta(days=prev_days)

        with (IBAIORx(
            ib_server_address,
            ib_server_port,
            ib_client_id if ib_client_id else cli_client_id
        )) as client:
            click.echo(ib_client_id)
            history = asyncio.run(client.get_contract_history(
                contract=results[0],
                start_date=start_date,
                bar_size=bar_size_enum,
                what_to_show=WhatToShow.TRADES,
            ))

            plt.clear_data()
            plt.clear_figure()
            plt.theme('dark')
            plt.title(f'{symbol} {bar_size} {prev_days} days')

            if bar_size_enum >= BarSize.Days1:
                plt.date_form('d/m/Y')
            else:
                plt.date_form('d H:M')

            plt.plot(
                plt.datetimes_to_string(history.index),
                history['close'],
                marker='hd')
            plt.show()
            click.getchar()


def positions_helper() -> pd.DataFrame:
    connect()
    positions: list[Position] = remoted_client.rpc(return_type=list[Position]).get_positions()

    def mapper(position: Position) -> List:
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

    df = pd.DataFrame(data=list(xs), columns=[
        'account', 'conId', 'localSymbol', 'exchange', 'position', 'avgCost', 'currency', 'total'
    ])
    return df.sort_values(by='currency')


@cli.command()
def positions():
    df = positions_helper()
    renderer.rich_table(
        df,
        financial=True,
        financial_columns=['total', 'avgCost'],
        csv=is_repl,
    )
    if is_repl:
        renderer.rich_table(
            df.groupby(by=['currency'])['total'].sum().reset_index(),
            financial=True,
        )


@cli.command()
def reconnect():
    connect()


@cli.command()
def clear():
    print(chr(27) + "[2J")


@cli.command(no_args_is_help=True)
@click.option('--symbol', required=True, help='symbol to resolve to conId')
@click.option('--exchange', required=False, help='exchange for symbol [not required]')
@click.option('--universe', required=False, help='universe to check for symbol [not required]')
@click.option('--ib', required=False, default=False, is_flag=True, help='force resolution from IB')
@click.option('--sec_type', required=False, default='STK', help='IB security type [STK is default]')
@click.option('--currency', required=False, default='USD', help='IB security currency')
@common_options()
@default_config()
def resolve(
    symbol: str,
    arctic_server_address: str,
    arctic_universe_library: str,
    exchange: str,
    universe: str,
    ib: bool,
    sec_type: str,
    currency: str,
    **args,
):
    if ib:
        container = TraderContainer()
        with (container.resolve(IBAIORx, ib_client_id=cli_client_id)) as client:
            contract = asyncio.get_event_loop().run_until_complete(client.get_conid(
                symbols=symbol,
                secType=sec_type,
                primaryExchange=exchange,
                currency=currency
            ))
            if contract and type(contract) is list:
                renderer.rich_list(contract)
            elif contract and type(contract) is Contract:
                renderer.rich_dict(contract.__dict__)
    else:
        results = __resolve(symbol, arctic_server_address, arctic_universe_library, exchange, universe)
        if len(results) > 0:
            renderer.rich_table(results, False)
        else:
            click.echo('unable to resolve {}, maybe try the --ib flag to force resolution from IB?'.format(symbol))


@cli.command(no_args_is_help=True)
@click.option('--symbol', required=True, help='symbol to snapshot')
@click.option('--delayed', required=False, default=False, is_flag=True, help='use delayed data?')
@click.option('--primary_exchange', required=False, help='primary exchange for symbol')
@common_options()
@default_config()
def snapshot(
    symbol: str,
    delayed: bool,
    arctic_server_address: str,
    arctic_universe_library: str,
    primary_exchange: str,
    **args,
):
    result = __resolve(symbol, arctic_server_address, arctic_universe_library, primary_exchange)
    if len(result) >= 1:
        r = result[0]
        contract = Contract(
            conId=r['conId'],
            symbol=r['symbol'],
            exchange=r['exchange'],
            primaryExchange=r['primaryExchange'],
            currency=r['currency']
        )

        # awaitable = remoted_client.rpc(return_type=Ticker).get_snapshot(contract, delayed)
        ticker = consume(remoted_client.rpc(return_type=Ticker).get_snapshot(contract, delayed))
        snap = {
            'symbol': ticker.contract.symbol if ticker.contract else '',
            'exchange': ticker.contract.exchange if ticker.contract else '',
            'primaryExchange': ticker.contract.primaryExchange if ticker.contract else '',
            'currency': ticker.contract.currency if ticker.contract else '',
            'time': ticker.time,
            'bid': ticker.bid,
            'bidSize': ticker.bidSize,
            'ask': ticker.ask,
            'askSize': ticker.askSize,
            'last': ticker.last,
            'lastSize': ticker.lastSize,
            'open': ticker.open,
            'high': ticker.high,
            'low': ticker.low,
            'close': ticker.close,
            'halted': ticker.halted
        }
        renderer.rich_dict(snap)
    else:
        click.echo('could not resolve symbol from symbol database')


@cli.group()
def subscribe():
    pass


@subscribe.command('start', no_args_is_help=True)
@click.option('--symbol', required=True, help='symbol to snapshot')
@click.option('--delayed', required=False, default=False, is_flag=True, help='use delayed data?')
@click.option('--primary_exchange', required=False, help='primary exchange for symbol')
@common_options()
@default_config()
def subscribe_start(
    symbol: str,
    delayed: bool,
    arctic_server_address: str,
    arctic_universe_library: str,
    primary_exchange: str,
    **args,
):
    result = __resolve(symbol, arctic_server_address, arctic_universe_library, primary_exchange)
    if len(result) >= 1:
        r = result[0]
        contract = Contract(
            conId=r['conId'],
            symbol=r['symbol'],
            exchange=r['exchange'],
            primaryExchange=r['primaryExchange'],
            currency=r['currency']
        )
        click.echo('subscribing to {}'.format(contract.symbol))
        remoted_client.rpc().publish_contract(contract, delayed)
    else:
        click.echo('no results found')


@subscribe.command('portfolio')
@common_options()
@default_config()
def subscribe_portfolio(
    **args,
):
    portfolio: list[PortfolioItem] = remoted_client.rpc(return_type=list[PortfolioItem]).get_portfolio()
    for portfolio_item in portfolio:
        if not portfolio_item.contract.exchange:
            portfolio_item.contract.exchange = 'SMART'

        click.echo('subscribing to {}'.format(portfolio_item.contract.symbol))
        remoted_client.rpc().publish_contract(portfolio_item.contract, delayed=False)


@subscribe.command('universe', no_args_is_help=True)
@click.option('--name', required=True, help='universe name to tick subscribe')
@click.option('--delayed', required=True, is_flag=True, default=False, help='subscribe to delayed data')
@common_options()
@default_config()
def subscribe_universe(
    name: str,
    delayed: bool,
    arctic_server_address: str,
    arctic_universe_library: str,
    **args,
):
    accessor = UniverseAccessor(arctic_server_address, arctic_universe_library)
    u = accessor.get(name)
    for security_definition in u.security_definitions:
        click.echo('subscribing to {}'.format(security_definition.symbol))
        remoted_client.rpc().publish_contract(u.to_contract(security_definition), delayed=delayed)


@subscribe.command('list')
@common_options()
@default_config()
def subscribe_list(
    **args,
):
    renderer.rich_list(remoted_client.rpc(return_type=list[Contract]).get_published_contracts())


@subscribe.command('listen', no_args_is_help=True)
@click.option('--topic', required=True, default='ticker', help='zmqtopic to listen to')
@common_options()
@default_config()
def subscribe_listen(
    topic: str,
    zmq_pubsub_server_address: str,
    zmq_pubsub_server_port: int,
    **args,
):
    printer = ZmqPrettyPrinter(zmq_pubsub_server_address, zmq_pubsub_server_port, csv=not (is_repl))
    asyncio.get_event_loop().run_until_complete(printer.listen(topic))

@cli.group()
def option():
    pass

@option.command('plot', no_args_is_help=True)
@click.option('--symbol', required=True, help='ticker symbol e.g. FB')
@click.option('--list_dates', required=False, is_flag=True, default=False, help='get the list of expirary dates')
@click.option('--date', required=False, help='option expiry date, format YYYY-MM-DD')
@click.option('--risk_free_rate', required=False, default=0.001, help='risk free rate [default 0.001]')
@common_options()
@default_config()
def options(
    symbol: str,
    list_dates: bool,
    date: str,
    risk_free_rate: float,
    **args,
):
    plot_chain(symbol, list_dates, date, True, risk_free_rate)


# CLI_BOOK
@cli.group()
def book():
    pass


@book.command('trades')
def book_trades():
    trades = remoted_client.rpc(return_type=dict[int, list[Trade]]).get_trades()
    columns = [
        'symbol', 'primaryExchange', 'currency', 'orderId',
        'action', 'status', 'orderType', 'lmtPrice', 'totalQuantity', 'orderRef',
    ]
    table = []
    for trade_id, trade_list in trades.items():
        table.append(DictHelper[str, str].dict_from_object(trade_list[0], columns))
    if table:
        renderer.rich_table(table)
    else:
        renderer.rich_empty_table(message='no trades found')


@book.command('orders')
def book_orders():
    orders: dict[int, list[Order]] = remoted_client.rpc(return_type=dict[int, list[Order]]).get_orders()
    columns = [
        'orderId', 'clientId', 'parentId', 'orderRef', 'action',
        'status', 'orderType', 'allOrNone', 'lmtPrice', 'totalQuantity', 'filledQuantity', 'trailStopPrice', 'trailingPercent'
    ]
    table = []
    for order_id, trade_list in orders.items():
        table.append(DictHelper[str, str].dict_from_object(trade_list[0], columns))
    if table:
        renderer.rich_table(table)
    else:
        renderer.rich_empty_table(message='no orders found')


@book.command('cancel', no_args_is_help=True)
@click.option('--order_id', required=True, type=int, help='order_id to cancel')
def book_order_cancel(order_id: int):
    # todo: untested
    order: SuccessFail[Trade] = remoted_client.rpc(return_type=SuccessFail[Trade]).cancel_order(order_id)
    if order:
        renderer.rich_dict(DictHelper.dict_from_object(order.obj))
    else:
        click.echo('no Trade object returned')


@book.command('log')
def book_log():
    trade_log = remoted_client.rpc(return_type=list[TradeLogSimple]).get_trade_log()
    if len(trade_log) > 0:
        renderer.rich_table(trade_log)
    else:
        renderer.rich_empty_table()


@cli.group()
def strategy():
    pass


@strategy.command('list')
def strategy_list():
    result = strategy_helper()
    if len(result) > 0:
        renderer.rich_table(result)
    else:
        renderer.rich_empty_table(message='no strategies found, or strategy_service not up')


@strategy.command('enable')
@click.option('--name', required=True, help='name of strategy')
@click.option('--paper', required=False, is_flag=True, default=True, help='dont trade, just paper trade it')
def strategy_enable(
    name: str,
    paper: bool,
):
    success_fail = consume(remoted_client.rpc().enable_strategy(name, paper))
    if success_fail.is_success():
        renderer.rich_dict({'state': success_fail.obj})
    else:
        renderer.rich_empty_table(message='strategy enable failed')


@strategy.command('disable')
@click.option('--strategy_name', required=True, help='name of strategy')
def strategy_disable(
    name: str,
):
    success_fail = consume(remoted_client.rpc().disable_strategy(name))
    if success_fail.is_success():
        renderer.rich_dict({'state': success_fail.obj})
    else:
        renderer.rich_empty_table(message='strategy enable failed')


@cli.group()
def pycron():
    pass


@pycron.command('info')
@default_config()
def pycron_info():
    pycron_server_address = TraderContainer().config()['pycron_server_address']
    pycron_server_port = TraderContainer().config()['pycron_server_port']
    response = requests.get('http://{}:{}'.format(pycron_server_address, pycron_server_port))
    renderer.rich_json(response.json())


@pycron.command('restart')
@click.option('--service', required=True, help='service to restart')
def pycron_restart(
    service: str,
):
    pycron_server_address = TraderContainer().config()['pycron_server_address']
    pycron_server_port = TraderContainer().config()['pycron_server_port']
    response = requests.get('http://{}:{}/?restart={}'.format(pycron_server_address, pycron_server_port, service))
    click.echo(response.json())
    renderer.rich_json(response.json())


@cli.command(no_args_is_help=True)
@click.option('--symbol', required=True, help='symbol of security')
def company_info(symbol: str):
    symbol = symbol.lower()
    financials = TraderContainer().resolve(DictData, arctic_library='HistoricalPolygonFinancials')
    arctic_server_address = TraderContainer().config()['arctic_server_address']
    arctic_universe_library = TraderContainer().config()['arctic_universe_library']
    accessor = UniverseAccessor(arctic_server_address, arctic_universe_library)

    for universe in accessor.get_all():
        for definition in universe.security_definitions:
            if symbol in definition.symbol.lower():
                info = financials.read(definition)
                if info:
                    renderer.rich_table(financials.read(definition).financials, True)  # type: ignore
                    return
    renderer.rich_empty_table('no company info found')


# CLI_TRADE
@cli.group()
def trade():
    pass


def __trade_helper(
    buy: bool,
    symbol: str,
    primary_exchange: str,
    market: bool,
    limit: Optional[float],
    equity_amount: Optional[float],
    quantity: Optional[float],
    stop_loss_percentage: float,
    debug: bool,
    arctic_server_address: str,
    arctic_universe_library: str,
    **args,
):
    if limit and limit <= 0.0:
        raise ValueError('limit price can be less than or equal to 0.0: {}'.format(limit))

    definitions = resolve_symbol(symbol)

    if len(definitions) == 0:
        click.echo('no contract found for symbol {}'.format(symbol))
        return
    elif len(definitions) > 1:
        click.echo('multiple security definitions found for symbol {}'.format(symbol))
        for definition in definitions:
            click.echo(definition)
        return

    security = definitions[0]
    contract = Universe.to_contract(security)

    action = 'BUY' if buy else 'SELL'
    trade: SuccessFail[Trade] = consume(remoted_client.rpc(return_type=SuccessFail[Trade]).place_order_simple(
        contract=contract,
        action=action,
        equity_amount=equity_amount,
        quantity=quantity,
        limit_price=limit,
        market_order=market,
        stop_loss_percentage=stop_loss_percentage,
        debug=debug,
    ))

    output_dict = {
        'result': trade.success_fail,
        'error': trade.error,
        'exception': trade.exception,
    }

    if trade.obj:
        output_dict.update(DictHelper.dict_from_object(trade.obj))

    renderer.rich_dict(output_dict)


@trade.command('buy', no_args_is_help=True)
@click.option('--symbol', required=True, type=str, help='IB conId for security')
@click.option('--primary_exchange', required=False, default='', type=str, help='exchange [not required]')
@option_group(
    'trade options',
    cloupoption('--market', is_flag=True, help='market order'),
    cloupoption('--limit', type=float, help='limit price [requires a decimal price]'),
    constraint=mutually_exclusive,
)
@option_group(
    'amount options',
    cloupoption('--equity_amount', type=float, help='total $$ equity amount to buy/sell, eg 1000.0'),
    cloupoption('--quantity', type=float, help='quantity of the underlying, eg 100.0'),
    constraint=mutually_exclusive,
)
@click.option('--stop_loss_percentage', required=False, type=float, default=0.0,
              help='percentage below price to place stop loss order [default=0.0, no stop loss]')
@click.option('--debug', is_flag=True, default=False, help='changes the trade to be + or - 10 percent of submitted limit price.')
@common_options()
@default_config()
def trade_buy(
    symbol: str,
    primary_exchange: str,
    market: bool,
    limit: Optional[float],
    equity_amount: Optional[float],
    quantity: Optional[float],
    stop_loss_percentage: float,
    debug: bool,
    arctic_server_address: str,
    arctic_universe_library: str,
    **args,
):
    __trade_helper(
        buy=True,
        symbol=symbol,
        primary_exchange=primary_exchange,
        market=market,
        limit=limit,
        equity_amount=equity_amount,
        quantity=quantity,
        stop_loss_percentage=stop_loss_percentage,
        debug=debug,
        arctic_server_address=arctic_server_address,
        arctic_universe_library=arctic_universe_library,
        args=args,
    )


@trade.command('sell', no_args_is_help=True)
@click.option('--symbol', required=True, type=str, help='IB conId for security')
@click.option('--primary_exchange', required=False, default='', type=str, help='exchange [not required]')
@option_group(
    'trade options',
    cloupoption('--market', is_flag=True, help='market order'),
    cloupoption('--limit', type=float, help='limit price [requires a decimal price]'),
    constraint=mutually_exclusive,
)
@option_group(
    'amount options',
    cloupoption('--equity_amount', type=float, help='total $$ equity amount to buy/sell, eg 1000.0'),
    cloupoption('--quantity', type=float, help='quantity of the underlying, eg 100.0'),
    constraint=mutually_exclusive,
)
@click.option('--stop_loss_percentage', required=False, type=float, default=0.0,
              help='percentage below price to place stop loss order [default=0.0, no stop loss]')
@click.option('--debug', is_flag=True, default=False, help='changes the trade to be + or - 10 percent of submitted limit price.')
@common_options()
@default_config()
def trade_sell(
    symbol: str,
    primary_exchange: str,
    market: bool,
    limit: Optional[float],
    equity_amount: Optional[float],
    quantity: Optional[float],
    stop_loss_percentage: float,
    debug: bool,
    arctic_server_address: str,
    arctic_universe_library: str,
    **args,
):
    __trade_helper(
        buy=False,
        symbol=symbol,
        primary_exchange=primary_exchange,
        market=market,
        limit=limit,
        equity_amount=equity_amount,
        quantity=quantity,
        stop_loss_percentage=stop_loss_percentage,
        debug=debug,
        arctic_server_address=arctic_server_address,
        arctic_universe_library=arctic_universe_library,
        args=args,
    )


@trade.command('cancel', no_args_is_help=True)
@click.option('--order_id', required=True, type=int, help='order_id for submitted order')
@common_options()
@default_config()
def trade_cancel(
    order_id: int,
    **args,
):
    trade: SuccessFail[Trade] = remoted_client.rpc(return_type=SuccessFail[Trade]).cancel_order(order_id)
    if not trade.is_success():
        renderer.rich_dict(DictHelper.dict_from_object(trade.obj))
    else:
        renderer.rich_empty_table('cancel_all successful')


@trade.command('cancel_all')
@common_options()
@default_config()
def trade_cancel_all(
    **args,
):
    cancelled: SuccessFail[List[int]] = remoted_client.rpc(return_type=SuccessFail[List[int]]).cancel_all()
    if cancelled.is_success():
        renderer.rich_empty_table(message='success')
    else:
        renderer.rich_dict(DictHelper.dict_from_object(cancelled.obj))


@trade.command('update', no_args_is_help=True)
@click.option('--order_id', required=True, type=int, help='order_id for submitted order')
@option_group(
    'trade options',
    cloupoption('--market', is_flag=True, help='market order'),
    cloupoption('--limit', type=float, help='limit price [requires a decimal price]'),
    constraint=mutually_exclusive,
)
@option_group(
    'amount options',
    cloupoption('--equity_amount', type=float, help='total $$ equity amount to buy/sell, eg 1000.0'),
    cloupoption('--quantity', type=float, help='quantity of the underlying, eg 100.0'),
    constraint=mutually_exclusive,
)
@click.option('--stop_loss_percentage', required=False, type=float, default=0.0,
              help='percentage below price to place stop loss order [default=0.0, no stop loss]')
@click.option('--debug', is_flag=True, default=False, help='changes the trade to be + or - 10 percent of submitted limit price.')
@common_options()
@default_config()
def trade_update(
    order_id: int,
    market: bool,
    limit: Optional[float],
    equity_amount: Optional[float],
    quantity: Optional[float],
    stop_loss_percentage: float,
    debug: bool,
    arctic_server_address: str,
    arctic_universe_library: str,
    **args,
):
    # todo: I don't like this api
    def error_out():
        click.echo('order_id {} already filled or cancelled. Aborting'.format(order_id))
    # ib_insync groups talk about modifying the order in place, and resubmitting via place_order_simple
    # but it feels cleaner to cancel the existing order and resubmit in case the user wants to move
    # from limit to market, or whatever.

    # ensure the order id is in the book via trades (Trade is a wrapper around the order, telling you
    # if the trade is submitted, executed, etc)
    trades: dict[int, list[Trade]] = remoted_client.rpc(return_type=dict[int, list[Trade]]).get_trades()
    if order_id in trades and len(trades[order_id]) > 0:
        trade = trades[order_id][0]

        if trade.orderStatus.status in list(OrderStatus.DoneStates):
            error_out()
            return

        cancelled_trade: SuccessFail[Trade] = remoted_client.rpc(return_type=SuccessFail[Trade]).cancel_order(order_id=order_id)
        if not cancelled_trade.is_success():
            error_out()
            return

        __trade_helper(
            buy=trade.order.action == 'BUY',
            symbol=str(trade.contract.conId),
            primary_exchange='',
            market=market,
            limit=limit,
            equity_amount=equity_amount,
            quantity=quantity,
            stop_loss_percentage=stop_loss_percentage,
            debug=debug,
            arctic_server_address=arctic_server_address,
            arctic_universe_library=arctic_universe_library,
            args=args,
        )
