from arctic import Arctic
from arctic.exceptions import NoDataFoundException
from click_help_colors import HelpColorsGroup
from click_option_group import optgroup, RequiredMutuallyExclusiveOptionGroup
from cloup import option as cloupoption
from cloup import option_group
from cloup.constraints import mutually_exclusive
from expression import pipe
from expression.collections import seq
from functools import partial
from ib_insync.contract import Contract
from ib_insync.objects import PortfolioItem, Position
from ib_insync.order import Order, OrderStatus, Trade
from ib_insync.ticker import Ticker
from IPython.core.getipython import get_ipython
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.cursor_shapes import CursorShape
from prompt_toolkit.formatted_text import HTML
from prompt_toolkit.history import FileHistory
from pyfiglet import Figlet
from scripts.chain import plot_chain
from scripts.trader_check import health_check
from scripts.zmq_pub_listener import ZmqPrettyPrinter
from trader.batch.queuer import Queuer
from trader.cli.cli_renderer import ConsoleRenderer
from trader.cli.command_line import cli, common_options, default_config
from trader.cli.commands import *  # NOQA
from trader.cli.commands import cli_client_id, invoke_context_wrapper, setup_cli
from trader.common.exceptions import TraderConnectionException, TraderException
from trader.common.helpers import contract_from_dict, DictHelper, rich_dict, rich_json, rich_list, rich_table
from trader.common.logging_helper import LogLevels, set_log_level, setup_logging
from trader.common.reactivex import AnonymousObserver, SuccessFail
from trader.container import Container
from trader.data.data_access import DictData, TickData, TickStorage
from trader.data.market_data import MarketData
from trader.data.universe import Universe, UniverseAccessor
from trader.listeners.ibreactive import IBAIORx, WhatToShow
from trader.messaging.clientserver import RPCClient
from trader.messaging.strategy_service_api import StrategyServiceApi
from trader.messaging.trader_service_api import TraderServiceApi
from trader.objects import BarSize
from typing import Any, Dict, List, Optional, Union

import asyncio
import click
import click_repl
import datetime as dt
import os
import pandas as pd
import trader.cli.universes_cli as universes_cli


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

    renderer = ConsoleRenderer()

    setup_cli(renderer)

    is_repl = True
    f = Figlet(font='starwars')
    click.echo(f.renderText('MMR'))
    click.echo(click.get_current_context().find_root().get_help())
    click.echo()
    click.echo('Ctrl-D or \'exit\' to exit')
    click_repl.repl(click.get_current_context(), prompt_kwargs=prompt_kwargs)


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
    global cli_client_id
    global remoted_client
    global trader_client
    global strategy_client
    global debug_place_order

    from reactivex import Observer

    renderer = ConsoleRenderer()
    _, cli_client_id = setup_cli(renderer)

    container = Container()
    accessor = container.resolve(UniverseAccessor)
    client = container.resolve(IBAIORx, ib_client_id=cli_client_id)
    client.connect()
    store = Arctic(mongo_host=container.config()['arctic_server_address'])
    tickstorage = container.resolve(TickStorage)
    trader_client = remoted_client
    strategy_client = RPCClient[StrategyServiceApi](
        zmq_server_address=container.config()['zmq_strategy_rpc_server_address'],
        zmq_server_port=container.config()['zmq_strategy_rpc_server_port'],
        timeout=10,
    )

    def lazy_call(func, *args, **kwargs):
        def lazy_func():
            return func(*args, **kwargs)
        return lazy_func

    debug_place_order = lazy_call(
        trader_client.rpc().place_order_simple,
        contract=amd,
        action='BUY',
        equity_amount=None,
        quantity=1.0,
        limit_price=50.0,
        stop_loss_percentage=0.10,
        debug=True,
    )

    print()
    print('Available instance objects:')
    print()
    print(' amd: Contract, nvda: Contract, a2m: Contract, cl: Contract, debug_place_order: Callable')
    print(' container: Container, accessor: UniverseAccessor, client: IBAIORx, store: Arctic')
    print(' tickstorage: TickStorage, trader_client: RPCClient, strategy_client: RPCClient')


if get_ipython().__class__.__name__ == 'TerminalInteractiveShell':  # type: ignore
    setup_ipython()


if __name__ == '__main__':
    invoke_context_wrapper(repl)
    cli(prog_name='cli')
