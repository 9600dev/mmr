from arctic import Arctic
from arctic.exceptions import NoDataFoundException
from click_help_colors import HelpColorsGroup
from click_option_group import optgroup, RequiredMutuallyExclusiveOptionGroup
from cloup import option as cloupoption
from cloup import option_group
from cloup.constraints import mutually_exclusive
from expression import pipe
from expression.collections import seq
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
from rich.syntax import Syntax
from rich.traceback import Traceback
from scripts.chain import plot_chain
from scripts.trader_check import health_check
from scripts.zmq_pub_listener import ZmqPrettyPrinter
from textual import events
from textual.app import App, ComposeResult
from textual.binding import Binding, Bindings
from textual.containers import Container, Grid, Horizontal, Vertical
from textual.css.query import NoMatches
from textual.reactive import var
from textual.screen import Screen
from textual.widgets import Button, DirectoryTree, Footer, Header, Static
from trader.batch.queuer import Queuer
from trader.cli.dialog import Dialog
from trader.common.command_line import cli, common_options, default_config
from trader.common.exceptions import TraderConnectionException, TraderException
from trader.common.helpers import contract_from_dict, DictHelper, rich_dict, rich_json, rich_list, rich_table
from trader.common.logging_helper import LogLevels, set_log_level, setup_logging
from trader.common.reactivex import SuccessFail
from trader.container import Container as TraderContainer
from trader.data.data_access import DictData, TickData, TickStorage
from trader.data.market_data import MarketData
from trader.data.universe import Universe, UniverseAccessor
from trader.listeners.ibreactive import IBAIORx, WhatToShow
from trader.messaging.clientserver import RemotedClient
from trader.messaging.trader_service_api import TraderServiceApi
from trader.objects import BarSize
from typing import Any, Dict, List, Optional, Union

import asyncio
import click
import click_repl
import cloup
import datetime as dt
import os
import pandas as pd
import plotext as plt
import random
import requests
import scripts.universes_cli as universes_cli
import sys
import trader.batch.ib_history_queuer as ib_history_queuer


logging = setup_logging(module_name='cli')  # type: ignore
is_repl = False


# error_table = {
#     'trader.common.exceptions.TraderException': TraderException,
#     'trader.common.exceptions.TraderConnectionException': TraderConnectionException
# }

# container = TraderContainer()

# def connect():
#     if not remoted_client.connected:
#         asyncio.get_event_loop().run_until_complete(remoted_client.connect())

# remoted_client = RemotedClient[TraderServiceApi](
#     zmq_server_address=container.config()['zmq_rpc_server_address'],
#     zmq_server_port=container.config()['zmq_rpc_server_port'],
#     error_table=error_table,
#     timeout=5,
# )
# connect()

# cli_client_id = -1
# try:
#     cli_client_id = remoted_client.rpc(return_type=int).get_unique_client_id()
# except asyncio.exceptions.TimeoutError:
#     click.echo('Could not connect to trader service at {}:{}. Is it running?'.format(
#         container.config()['zmq_rpc_server_address'],
#         container.config()['zmq_rpc_server_port']
#     ))
#     cli_client_id = random.randint(50, 100)
#     click.echo('using client id {}'.format(cli_client_id))


class QuitScreen(Screen):
    BINDINGS = [('c', 'cancel', 'Cancel'), ('q', 'quit', 'Quit')]

    def compose(self) -> ComposeResult:
        yield Grid(
            Static("Are you sure you want to quit?", id="question"),
            Button("Quit", variant="error", id="quit"),
            Button("Cancel", variant="primary", id="cancel"),
            id="dialog",
        )

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "quit":
            await self.action_quit()
        else:
            await self.action_cancel()

    async def action_cancel(self):
        await self.app.pop()  # type: ignore

    async def action_quit(self):
        self.app.exit()


class AsyncCli(App):
    """A Textual app to manage stopwatches."""
    CSS_PATH = 'trader/cli/css.css'
    BINDINGS = [
        Binding('d', 'toggle_dark', 'Toggle dark mode'),
        Binding(
            'q',
            "confirm_y_n('[bold]Quit?[/bold] Y/N', 'quit', 'close_dialog', '[Quit]')",
            'Quit',
        ),
        # Binding('q', 'quit', 'Quit')]
    ]
    SCREENS = {'quit': QuitScreen}

    async def on_mount(self) -> None:
        logging.debug('on_mount')

    def compose(self) -> ComposeResult:

        self.dialog = Dialog(id='modal_dialog')

        yield Container(
            Container(
                Vertical(
                    *[Static(f'Vertical layout, child {number}') for number in range(15)],
                    id='top-left',
                ),
                id='top',
            ),
            Container(
                Static('This'),
                Static('panel'),
                Static('is'),
                Static('using'),
                Static('grid layout!', id='bottom-right-final'),
                id='bottom-left',
            ),
            Container(
                Static("This"),
                Static("panel"),
                Static("is"),
                Static("using"),
                Static("grid layout!", id="bottom-right-final"),
                id="bottom-right",
            ),
            id="app-grid",
        )
        yield Footer()

        yield self.dialog

    def action_confirm_y_n(
        self, message: str, confirm_action: str, noconfirm_action: str, title: str = ""
    ) -> None:
        """Build Yes/No Modal Dialog and process callbacks."""
        dialog = self.query_one("#modal_dialog", Dialog)
        dialog.confirm_action = confirm_action
        dialog.noconfirm_action = noconfirm_action
        dialog.set_message(message)
        dialog.show_dialog()

    def action_toggle_dark(self) -> None:
        """An action to toggle dark mode."""
        self.dark = not self.dark

    async def action_quit(self) -> None:
        """An action to toggle dark mode."""
        # sys.exit(0)
        self.push_screen('quit')

    async def pop(self) -> None:
        self.pop_screen()

    async def on_key(self, event: events.Key) -> None:
        def press(button_id: str) -> None:
            try:
                self.query_one(f'#{button_id}', Button).press()
            except NoMatches:
                pass
        # key = event.key
        # if key == 'c':
        #     press('cancel')
        # elif key == 'q':
        #     press('quit')
        # else:
        #     raise ValueError

async def main():
    app = AsyncCli()
    await app.run_async()


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

    container = TraderContainer()
    accessor = container.resolve(UniverseAccessor)
    client = container.resolve(IBAIORx, ib_client_id=cli_client_id)
    client.connect()
    store = Arctic(mongo_host=container.config()['arctic_server_address'])
    tickstorage = container.resolve(TickStorage)
    marketdata = container.resolve(MarketData, **{'client': client})

    print('Available instance objects:')
    print()
    print(' amd: Contract, nvda: Contract, a2m: Contract, cl: Contract')
    print(' container: Container, accessor: UniverseAccessor, client: IBAIORx')
    print(' store: Arctic, tickstorage: TickStorage, marketdata: MarketData')


if get_ipython().__class__.__name__ == 'TerminalInteractiveShell':  # type: ignore
    setup_ipython()

if __name__ == '__main__':
    asyncio.run(main())
