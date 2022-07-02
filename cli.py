import os
from random import randint
import sys
import asyncio
from ib_insync.order import LimitOrder
from prompt_toolkit.shortcuts import prompt
import requests
import click
import pandas as pd
import scripts.universes_cli as universes_cli
import trader.batch.ib_history_queuer as ib_history_queuer
import click_repl
import aioreactive as rx

from aioreactive import AsyncAnonymousObservable, AsyncAwaitableObserver, AsyncAnonymousObserver
from aioreactive.types import AsyncObservable, AsyncObserver
from aioreactive.observers import auto_detach_observer
from typing import Type, TypeVar, Dict, Optional, List, cast, Tuple, Any
from trader.common.reactive import SuccessFailEnum
from trader.container import Container
from trader.data.data_access import SecurityDefinition, TickData, DictData
from trader.data.universe import Universe, UniverseAccessor
from scripts.trader_check import health_check
from scripts.zmq_pub_listener import ZmqPrettyPrinter
from ib_insync.ib import IB
from ib_insync.order import Trade, Order
from ib_insync.contract import Contract, ContractDescription, Stock, Option
from ib_insync.objects import PortfolioItem

from arctic import Arctic
from trader.common.helpers import rich_table, rich_dict, rich_json, DictHelper, rich_list
from IPython import get_ipython
from pyfiglet import Figlet
from prompt_toolkit.history import FileHistory, InMemoryHistory
from ib_insync.objects import Position
from ib_insync.ticker import Ticker
from expression.collections import Seq, seq
from expression import Nothing, Some, effect, pipe
from trader.common.command_line import cli, common_options, default_config
from trader.listeners.ibaiorx import IBAIORx
from click_help_colors import HelpColorsGroup
from trader.common.logging_helper import setup_logging, suppress_external
from trader.listeners.ibaiorx import WhatToShow
from trader.data.market_data import MarketData
from scripts.chain import plot_chain
from trader.common.helpers import contract_from_dict
from trader.messaging.clientserver import RemotedClient, TopicPubSub
from trader.messaging.trader_service_api import TraderServiceApi
from trader.common.exceptions import TraderException, TraderConnectionException
from trader.common.helpers import *


logging = setup_logging(module_name='cli')
is_repl = False


error_table = {
    'trader.common.exceptions.TraderException': TraderException,
    'trader.common.exceptions.TraderConnectionException': TraderConnectionException
}

def connect():
    if not remoted_client.connected:
        asyncio.get_event_loop().run_until_complete(remoted_client.connect())

remoted_client = RemotedClient[TraderServiceApi](error_table=error_table)
connect()

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
    connect()
    portfolio: list[PortfolioItem] = remoted_client.rpc(return_type=list[PortfolioItem]).get_portfolio()

    def mapper(portfolio: PortfolioItem) -> List:
        # portfolio = PortfolioItem(*portfolio)
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

    rich_table(df.sort_values(by='unrealizedPNL', ascending=False), csv=is_repl, financial=True, financial_columns=[
        'marketPrice', 'marketValue', 'averageCost', 'unrealizedPNL', 'realizedPNL'
    ])


@main.command()
def positions():
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
    rich_table(df.sort_values(by='currency'), financial=True, financial_columns=['total', 'avgCost'], csv=is_repl)
    if is_repl:
        rich_table(df.groupby(by=['currency'])['total'].sum().reset_index(), financial=True)


@main.command()
def reconnect():
    connect()


@main.command()
def clear():
    print(chr(27) + "[2J")


def __resolve(
    symbol: str,
    arctic_server_address: str,
    arctic_universe_library: str,
    primary_exchange: Optional[str] = ''
) -> List[Dict[str, Any]]:
    if not primary_exchange: primary_exchange = ''
    symbol = symbol.lower()
    accessor = UniverseAccessor(arctic_server_address, arctic_universe_library)
    results = []
    for universe in accessor.get_all():
        for definition in universe.security_definitions:
            if definition.symbol.lower().startswith(symbol):
                results.append({
                    'universe': universe.name,
                    'conId': definition.conId,
                    'symbol': definition.symbol,
                    'exchange': definition.exchange,
                    'primaryExchange': definition.primaryExchange,
                    'currency': definition.currency,
                    'longName': definition.longName,
                    'category': definition.category,
                    'subcategory': definition.subcategory,
                    'minTick': definition.minTick,
                })
    return [r for r in results if primary_exchange in r['primaryExchange']]


@main.command()
@click.option('--symbol', required=True, help='symbol to resolve to conId')
@click.option('--primary_exchange', required=False, default='NASDAQ', help='exchange for symbol [not required]')
@click.option('--ib', required=False, default=False, is_flag=True, help='force resolution from IB')
@click.option('--sec_type', required=False, default='STK', help='IB security type [STK is default]')
@click.option('--currency', required=False, default='USD', help='IB security currency')
@common_options()
@default_config()
def resolve(
    symbol: str,
    arctic_server_address: str,
    arctic_universe_library: str,
    primary_exchange: str,
    ib: bool,
    sec_type: str,
    currency: str,
    **args,
):
    if ib:
        container = Container()
        client = container.resolve(IBAIORx)
        IBAIORx.client_id_counter = randint(20, 99)
        client.connect()
        contract = asyncio.get_event_loop().run_until_complete(client.get_conid(
            symbols=symbol,
            secType=sec_type,
            primaryExchange=primary_exchange,
            currency=currency
        ))
        if contract and type(contract) is list:
            rich_list(contract)
        elif contract and type(contract) is Contract:
            rich_dict(contract.__dict__)
    else:
        results = __resolve(symbol, arctic_server_address, arctic_universe_library, primary_exchange)
        rich_table(results, False)


@main.command()
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
        ticker = remoted_client.rpc(return_type=Ticker).get_snapshot(contract, delayed)
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
        rich_dict(snap)
    else:
        click.echo('could not resolve symbol from symbol database')



@main.group()
def subscribe():
    pass


@subscribe.command('start')
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
        remoted_client.rpc(return_type=Ticker).publish_contract(contract, delayed)
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
        success_fail = remoted_client.rpc().publish_contract(portfolio_item.contract, delayed=False)
        if success_fail.success_fail == SuccessFailEnum.FAIL:
            click.echo('contract subscription failed on {}'.format(portfolio_item.contract))
            click.echo(success_fail.exception)


@subscribe.command('list')
@common_options()
@default_config()
def subscribe_list(
    **args,
):
    rich_list(remoted_client.rpc(return_type=list[Contract]).get_published_contracts())


@subscribe.command('listen')
@common_options()
@default_config()
def subscribe_listen(
    zmq_pubsub_server_address: str,
    zmq_pubsub_server_port: int,
    **args,
):
    printer = ZmqPrettyPrinter(zmq_pubsub_server_address, zmq_pubsub_server_port, csv=not (is_repl))
    printer.console_listener()


@main.group()
def option():
    pass

@option.command('plot')
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


@main.group()
def loadtest():
    pass


@loadtest.command('start')
def load_test_start():
    remoted_client.rpc().start_load_test()


@loadtest.command('stop')
def load_test_stop():
    remoted_client.rpc().stop_load_test()


@main.group()
def book():
    pass


@book.command('trades')
def book_trades():
    trades = remoted_client.rpc(return_type=dict[int, list[Trade]]).get_trades()
    columns = [
        'symbol', 'primaryExchange', 'currency', 'orderId',
        'action', 'status', 'orderType', 'lmtPrice', 'totalQuantity'
    ]
    table = []
    for trade_id, trade_list in trades.items():
        table.append(DictHelper[str, str].dict_from_object(trade_list[0], columns))
    rich_table(table)


@book.command('orders')
def book_orders():
    orders: dict[int, list[Order]] = remoted_client.rpc(return_type=dict[int, list[Order]]).get_orders()
    columns = [
        'orderId', 'clientId', 'parentId', 'action',
        'status', 'orderType', 'allOrNone', 'lmtPrice', 'totalQuantity'
    ]
    table = []
    for order_id, trade_list in orders.items():
        table.append(DictHelper[str, str].dict_from_object(trade_list[0], columns))
    rich_table(table)


@book.command('cancel')
@click.option('--order_id', required=True, type=int, help='order_id to cancel')
def book_order_cancel(order_id: int):
    # todo: untested
    order: Optional[Trade] = remoted_client.rpc(return_type=Optional[Trade]).cancel_order(order_id)
    if order:
        click.echo(order)
    else:
        click.echo('no Trade object returned')


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
    click.echo(response.json())
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
@click.option('--buy/--sell', default=True, help='buy or sell')
@click.option('--symbol', required=True, type=str, help='IB conId for security')
@click.option('--primary_exchange', required=False, default='', type=str, help='exchange [not required]')
@click.option('--market', default=False, help='accept current market price')
@click.option('--limit', required=True, type=float, help='limit price for security')
@click.option('--amount', required=True, type=float, help='total amount to buy/sell')
@common_options()
@default_config()
def trade(
    buy: bool,
    symbol: str,
    primary_exchange: str,
    market: bool,
    limit: float,
    amount: float,
    arctic_server_address: str,
    arctic_universe_library: str,
    **args,
):
    contracts = __resolve(
        symbol=symbol,
        arctic_server_address=arctic_server_address,
        arctic_universe_library=arctic_universe_library,
        primary_exchange=primary_exchange
    )

    if len(contracts) == 0:
        click.echo('no contract found for symbol {}'.format(symbol))
    elif len(contracts) > 1:
        click.echo('multiple contracts found for symbol {}, aborting'.format(symbol))

    action = 'BUY' if buy else 'SELL'
    trade: Trade = remoted_client.rpc(return_type=Trade).temp_place_order(
        contract=contract_from_dict(contracts[0]),
        action=action,
        equity_amount=amount
    )
    click.echo(trade)


@main.command()
def status():
    result = health_check(Container().config_file)
    rich_dict({'status': result})

@main.command('exit')
def def_exit():
    os._exit(os.EX_OK)


container: Container
amd = Contract(symbol='AMD', conId=4391, exchange='SMART', primaryExchange='NASDAQ', currency='USD')
nvda = Contract(symbol='NVDA', conId=4815747, exchange='SMART', primaryExchange='NASDAQ', currency='USD')
a2m = Contract(symbol='A2M', conId=189114468, exchange='SMART', primaryExchange='ASX', currency='AUD')
marketdata: MarketData
accessor: UniverseAccessor
client: IBAIORx
store: Arctic
bardata: TickData


def setup_ipython():
    global container
    global accessor
    global client
    global store
    global bardata
    global marketdata

    container = Container()
    accessor = container.resolve(UniverseAccessor)
    client = container.resolve(IBAIORx)
    client.connect()
    store = Arctic(mongo_host=container.config()['arctic_server_address'])
    bardata = container.resolve(TickData)
    marketdata = container.resolve(MarketData, **{'client': client})


if get_ipython().__class__.__name__ == 'TerminalInteractiveShell':  # type: ignore
    setup_ipython()

if __name__ == '__main__':
    main(obj={})
