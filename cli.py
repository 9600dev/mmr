from arctic import Arctic
from arctic.exceptions import NoDataFoundException
from click_help_colors import HelpColorsGroup
from cloup import option as cloupoption
from cloup import option_group
from cloup.constraints import mutually_exclusive
from expression import pipe
from expression.collections import seq
from ib_insync.contract import Contract
from ib_insync.objects import PortfolioItem, Position
from ib_insync.order import Order, Trade
from ib_insync.ticker import Ticker
from IPython import get_ipython
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.cursor_shapes import CursorShape
from prompt_toolkit.formatted_text import HTML
from prompt_toolkit.history import FileHistory
from pyfiglet import Figlet
from scripts.chain import plot_chain
from scripts.trader_check import health_check
from scripts.zmq_pub_listener import ZmqPrettyPrinter
from trader.batch.queuer import Queuer
from trader.common.command_line import cli, common_options, default_config
from trader.common.exceptions import TraderConnectionException, TraderException
from trader.common.helpers import contract_from_dict, DictHelper, rich_dict, rich_json, rich_list, rich_table
from trader.common.logging_helper import setup_logging
from trader.common.reactivex import SuccessFail
from trader.container import Container
from trader.data.data_access import DictData, TickData, TickStorage
from trader.data.market_data import MarketData
from trader.data.universe import Universe, UniverseAccessor
from trader.listeners.ibreactive import IBAIORx
from trader.messaging.clientserver import RemotedClient
from trader.messaging.trader_service_api import TraderServiceApi
from trader.objects import BarSize
from typing import Any, Dict, List, Optional, Union

import asyncio
import click
import click_repl
import os
import pandas as pd
import requests
import scripts.universes_cli as universes_cli
import trader.batch.ib_history_queuer as ib_history_queuer


logging = setup_logging(module_name='cli')  # type: ignore
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

cli_client_id = remoted_client.rpc(return_type=int).get_unique_client_id()

@click.group(
    invoke_without_command=True,
    cls=HelpColorsGroup,
    help_headers_color='yellow',
    help_options_color='green')
@click.pass_context
def main(ctx):
    # todo: actually fix this pytz stuff throughout the codebase
    import warnings
    warnings.filterwarnings(
        'ignore',
        message='The zone attribute is specific to pytz\'s interface; please migrate to a new time zone provider. For more details on how to do so, see https://pytz-deprecation-shim.readthedocs.io/en/latest/migration.html'  # noqa: E501
    )

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

    def bottom_toolbar():
        return HTML('MMR statusbar <b><style bg="ansired">[feature not complete]</style></b>.')

    prompt_kwargs = {
        'history': FileHistory(os.path.expanduser('.trader.history')),
        'vi_mode': True,
        'message': 'mmr> ',
        'bottom_toolbar': bottom_toolbar,
        'cursor': CursorShape.BLINKING_BLOCK,
        'auto_suggest': AutoSuggestFromHistory(),
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
universes.add_command(universes_cli.destroy)
universes.add_command(universes_cli.add_to_universe)

@main.group()
def history():
    pass

history.add_command(ib_history_queuer.get_universe_history_ib)
history.add_command(ib_history_queuer.get_symbol_history_ib)

@history.command('summary')
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

    for history_db in TickStorage(arctic_server_address).list_libraries():
        tick_data = TickStorage(arctic_server_address).get_tickdata(history_db)
        tick_data = TickData(arctic_server_address, history_db)
        examples: List[str] = tick_data.list_symbols()[0:10]

        result = {
            'universe': universe,
            'arctic_library': history_db,
            'bar_size': ' '.join(history_db.split('_')[-2:]),
            'security_definition_count': len(u.security_definitions),
            'history_db_symbol_count': len(tick_data.list_symbols()),
            'example_symbols': examples
        }
        rich_dict(result)


@history.command('read')
@click.option('--symbol', required=True, help='historical data statistics for symbol')
@click.option('--universe', required=True, default='NASDAQ', help='exchange for symbol [default: NASDAQ]')
@click.option('--bar_size', required=True, default='1 min', help='bar size to read')
@common_options()
@default_config()
def history_read(
    symbol: str,
    universe: str,
    bar_size: str,
    arctic_server_address: str,
    arctic_universe_library: str,
    **args,
):
    results: List[Contract] = __resolve_contract(symbol, arctic_server_address, arctic_universe_library)
    bar_size_enum = BarSize.parse_str(bar_size)

    if len(results) >= 1:
        data = TickStorage(arctic_server_address).get_tickdata(bar_size_enum).read(results[0])
        rich_table(data, csv=True, include_index=True)


@history.command('jobs')
@common_options()
@default_config()
def history_jobs(
    **args,
):
    container = Container()
    queuer = container.resolve(Queuer)
    rich_dict(queuer.current_queue())


@history.command('security')
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
    rich_table(output)


@history.command('bar-sizes')
def history_bar_sizes():
    rich_list(BarSize.bar_sizes())


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
    symbol: Union[str, int],
    arctic_server_address: str,
    arctic_universe_library: str,
    primary_exchange: Optional[str] = ''
) -> List[Dict[str, Any]]:
    if not primary_exchange: primary_exchange = ''
    accessor = UniverseAccessor(arctic_server_address, arctic_universe_library)
    universe_definitions = accessor.resolve_symbol(symbol)

    results: List[Dict] = []
    for universe, definition in universe_definitions:
        # check to see if there is an existing definition with the same conId
        if not any([x['conId'] == definition.conId for x in results]):
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


def __resolve_contract(
    symbol: Union[str, int],
    arctic_server_address: str,
    arctic_universe_library: str,
    primary_exchange: Optional[str] = ''
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
        client = container.resolve(IBAIORx, extra_args={'ib_client_id': cli_client_id})

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
        if len(results) > 0:
            rich_table(results, False)
        else:
            click.echo('unable to resolve {}, maybe try the --ib flag to force resolution from IB?'.format(symbol))


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


@subscribe.command('universe')
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
    rich_list(remoted_client.rpc(return_type=list[Contract]).get_published_contracts())


@subscribe.command('listen')
@click.option('--topic', required=True, help='zmqtopic to listen to')
@common_options()
@default_config()
def subscribe_listen(
    topic: str,
    zmq_pubsub_server_address: str,
    zmq_pubsub_server_port: int,
    **args,
):
    printer = ZmqPrettyPrinter(zmq_pubsub_server_address, zmq_pubsub_server_port, csv=not (is_repl))
    asyncio.run(printer.listen(topic))


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


## CLI_BOOK
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
def strategy():
    pass


@strategy.command('list')
def strategy_list():
    trades = remoted_client.rpc(return_type=dict[int, list[Trade]]).get_trades()
    columns = [
        'symbol', 'primaryExchange', 'currency', 'orderId',
        'action', 'status', 'orderType', 'lmtPrice', 'totalQuantity'
    ]
    table = []
    for trade_id, trade_list in trades.items():
        table.append(DictHelper[str, str].dict_from_object(trade_list[0], columns))
    rich_table(table)


@strategy.command('enable')
def strategy_enable():
    orders: dict[int, list[Order]] = remoted_client.rpc(return_type=dict[int, list[Order]]).get_orders()
    columns = [
        'orderId', 'clientId', 'parentId', 'action',
        'status', 'orderType', 'allOrNone', 'lmtPrice', 'totalQuantity'
    ]
    table = []
    for order_id, trade_list in orders.items():
        table.append(DictHelper[str, str].dict_from_object(trade_list[0], columns))
    rich_table(table)


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


# CLI_TRADE
@main.group()
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
        return
    elif not symbol.isnumeric():
        click.echo('warning: not using IB conId as symbol identifier could lead to unexpected trade behavior.')

    action = 'BUY' if buy else 'SELL'
    trade: SuccessFail = remoted_client.rpc(return_type=SuccessFail).place_order(
        contract=contract_from_dict(contracts[0]),
        action=action,
        equity_amount=equity_amount,
        quantity=quantity,
        limit_price=limit,
        market_order=market,
        stop_loss_percentage=stop_loss_percentage,
        debug=debug,
    )
    click.echo(trade)


@trade.command('buy')
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


@trade.command('sell')
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


@trade.command('cancel')
@click.option('--order_id', required=True, type=int, help='order_id for submitted order')
@common_options()
@default_config()
def trade_cancel(
    order_id: int,
    **args,
):
    trade: Optional[Trade] = remoted_client.rpc(return_type=SuccessFail).cancel_order(order_id)
    if trade:
        click.echo(trade)
    else:
        click.echo('cancellation unsuccessful; either order_id did not exist or order was already completed')


@trade.command('update')
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
    **args,
):
    click.echo('not implemented')


@main.command()
def status():
    result = health_check(Container().config_file)
    rich_dict({'status': result})

@main.command('exit')
def def_exit():
    os._exit(os.EX_OK)


container: Container
amd = Contract(symbol='AMD', conId=4391, exchange='SMART', primaryExchange='NASDAQ', currency='USD')
tsla = Contract(symbol='TSLA', conId=76792991, exchange='SMART', primaryExchange='NASDAQ', currency='USD')
nvda = Contract(symbol='NVDA', conId=4815747, exchange='SMART', primaryExchange='NASDAQ', currency='USD')
a2m = Contract(symbol='A2M', conId=189114468, exchange='SMART', primaryExchange='ASX', currency='AUD')
cl = Contract(conId=457630923, symbol='CL', secType='FUT', exchange='NYMEX', lastTradeDateOrContractMonth='20221122')

marketdata: MarketData
accessor: UniverseAccessor
client: IBAIORx
store: Arctic
tickstorage: TickStorage


def setup_ipython():
    global container
    global accessor
    global client
    global store
    global bardata
    global marketdata
    global tickstorage

    container = Container()
    accessor = container.resolve(UniverseAccessor)
    client = container.resolve(IBAIORx, extra_args={'ib_client_id': cli_client_id})
    client.connect()
    store = Arctic(mongo_host=container.config()['arctic_server_address'])
    tickstorage = container.resolve(TickStorage)
    marketdata = container.resolve(MarketData, **{'client': client})

    print('Available instance objects:')
    print()
    print(' amd: Contract, nvda: Contract, a2m: Contract, cl: Contract')
    print(' container: Container, accessor: UniverseAccessor, client: IBAIORx')
    print(' store: Arctic, bardata: TickData, marketdata: MarketData')


if get_ipython().__class__.__name__ == 'TerminalInteractiveShell':  # type: ignore
    setup_ipython()

if __name__ == '__main__':
    main(obj={})
