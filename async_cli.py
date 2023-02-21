from arctic import Arctic
from arctic.exceptions import NoDataFoundException
from click.core import MultiCommand
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
from io import StringIO
from IPython.core.getipython import get_ipython
from prompt_toolkit import prompt
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.cursor_shapes import CursorShape
from prompt_toolkit.formatted_text import HTML
from prompt_toolkit.history import FileHistory
from prompt_toolkit.input import create_input
from prompt_toolkit.keys import Keys
from prompt_toolkit.shortcuts import PromptSession
from pyfiglet import Figlet
from rich.ansi import AnsiDecoder
from rich.console import Console, Group, RenderableType
from rich.jupyter import JupyterMixin
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.syntax import Syntax
from rich.text import Text
from rich.traceback import Traceback
from scripts.chain import plot_chain
from scripts.trader_check import health_check
from scripts.zmq_pub_listener import ZmqPrettyPrinter
from textual import events
from textual.app import App, ComposeResult
from textual.binding import Binding, Bindings
from textual.containers import Container, Content, Grid, Horizontal, Vertical
from textual.css.query import NoMatches
from textual.reactive import Reactive, var
from textual.scroll_view import ScrollView
from textual.widget import Widget
from textual.widgets import Button, DirectoryTree, Footer, Header, Input, Label, Pretty, Static, TextLog
from trader.batch.queuer import Queuer
from trader.cli.command_line import cli, common_options, default_config
from trader.cli.commands import *  # NOQA
from trader.cli.commands import (
    cli_client_id,
    invoke_context_wrapper,
    monkeypatch_click_echo,
    setup_connection
)
from trader.cli.dialog import Dialog
from trader.cli.repl_input import ReplInput
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
import datetime as dt
import os
import pandas as pd
import plotext as plt
import rich as r
import shlex
import sys
import trader.batch.ib_history_queuer as ib_history_queuer
import trader.cli.universes_cli as universes_cli


def make_plot(width, height, phase):
    plt.clf()
    l, frames = 1000, 30
    x = range(1, l + 1)
    # y = plt.sin()
    y = plt.sin(1, length=l, phase=2 * phase / frames)
    plt.scatter(x, y, marker="fhd")
    plt.plotsize(width, height)
    return plt.build()


class PlotextMixin(JupyterMixin):
    def __init__(self, width, height, phase=0):
        self.decoder = AnsiDecoder()
        self.width = width
        self.height = height
        self.phase = 0

    def __rich_console__(self, console, options):
        # self.width = options.max_width or console.width
        # self.height = options.height or console.height
        canvas = make_plot(self.width, self.height, self.phase)
        self.rich_canvas = Group(*self.decoder.decode(canvas))
        yield self.rich_canvas


class InputBox(Widget):
    input_text: Union[Reactive[str], str] = Reactive("")

    def render(self) -> RenderableType:
        return f"[blue]mmr❯[/blue] {self.input_text}"

    def set_input_text(self, input_text: str) -> None:
        self.input_text = input_text

    def on_key(self, event: events.Key):
        if event.character:
            self.input_text += event.character


class ReplWidget(Widget):
    DEFAULT_CSS = """
    #repl-toplevel {
        layout: vertical;
        overflow-y: hidden;
        overflow-x: hidden;
    }

    .repl-container {
        layout: horizontal;
        overflow-x: hidden;
        overflow-y: hidden;
    }

    .box {
        height: 100%;
        content-align: left bottom;
    }

    .line-box {
        height: 1;
        content-align: left bottom;
    }

    .line-input {
        width: 100%;
    }

    .log-box {
        overflow-y: scroll;
    }

    #repl-label {
        width: auto;
    }
    """

    def __init__(
        self,
        output: TextLog,
        group_ctx: Optional[click.core.Context] = None,
        logging: Optional[TextLog] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.output = output
        self.group_ctx = group_ctx
        self.log_box = TextLog(classes='box box-repl log-box')
        self.input_box = ReplInput(classes='line-box line-input')
        self.input_box.focus()
        self.logging = logging

        self.top_container = Container(
            Static('     ', id='repl-label', classes='box'),
            self.log_box,
            classes='repl-container'
        )
        self.bottom_container = Container(
            Static('mmr❯ ', id='repl-label', classes='line-box'),
            self.input_box,
            classes='repl-container line-box',
        )

    def compose(self) -> ComposeResult:
        yield Container(
            self.top_container,
            self.bottom_container,
            id='repl-toplevel',
        )

    def focus(self):
        self.input_box.focus()

    def click_dispatch(self, command: str):
        output = StringIO()

        monkeypatch_click_echo(stdout=output)

        old_ctx = click.get_current_context()
        group_ctx = old_ctx.parent or old_ctx
        group = group_ctx.command
        group.no_args_is_help = True
        args = ''

        repl_command_name = old_ctx.command.name
        if isinstance(group_ctx.command, click.CommandCollection):
            available_commands = {
                cmd_name: cmd_obj
                for source in group_ctx.command.sources
                for cmd_name, cmd_obj in source.commands.items()  # type: ignore
            }
        else:
            available_commands = group_ctx.command.commands  # type: ignore
        available_commands.pop(repl_command_name, None)

        args = shlex.split(command)  # type: ignore

        def echo_reload(message: str):
            print(message)

        try:
            with group.make_context(None, args, parent=group_ctx) as ctx:  # type: ignore
                try:
                    ctx.command.no_args_is_help = True
                    group.invoke(ctx)
                except click.exceptions.Exit:
                    pass
                output.seek(0)
                self.output.write(output.read())
        except click.ClickException as e:
            self.output.write(e.message)

    def write_debug(self, text: str):
        if self.logging:
            self.logging.write(text)

    def write_log(self, text: str):
        self.log_box.write(text)

    def on_repl_input_submitted(self, event: ReplInput.Submitted):
        if not event.value:
            self.write_log(' ')
        else:
            self.write_log(str(event.value))
            self.click_dispatch(event.value)
        self.input_box.value = ''
        self.input_box.focus()

    def on_repl_input_changed(self, event: ReplInput.Changed):
        self.write_debug(str(event))


class AsyncDialog(Widget):
    def __init__(
        self,
        container: Optional[Container] = None,
        width: Optional[int] = 81,
        height: Optional[int] = 21,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        if container:
            self.content = container
        else:
            self.debug = Label('debug')
            self.content = Container(
                self.debug,
                Input(),
                Label("A tool for doing something or other"),
                Label("Copyright (C) 2022 The Copyright Holders"),
                Label("https://www.example.com/"),
                id="mydialog",
            )

        self.console = Console()
        self.width = width
        self.height = height
        self.styles.width = width
        self.styles.height = height
        self.visible = False

    def compose(self) -> ComposeResult:
        yield self.content

    def get_dialog_position(self, dw, dh):
        x = (self.console.width - dw) // 2
        y = (self.console.height - dh) // 2
        return (x, y)

    def show(self):
        self.styles.width = self.width
        self.styles.height = self.height
        self.styles.margin = (1, 1)  # self.get_dialog_position(self.width, self.height)
        self.debug.update(Text(f'w{self.console.width} x h{self.console.height} margin: {self.styles.margin}'))
        self.visible = True

    def hide(self):
        self.visible = False


class AsyncCli(App):
    def __init__(
        self,
        group_ctx: Optional[click.core.Context] = None,
    ):
        super().__init__()
        self.group_ctx = group_ctx

    """A Textual app to manage stopwatches."""
    CSS_PATH = 'trader/cli/css.css'
    BINDINGS = [
        Binding('ctrl+d', 'toggle_dark', 'Toggle dark mode'),
        Binding('ctrl+u', 'dialog', 'Dialog'),
        Binding('ctrl+p', 'plot', 'Plot'),
        Binding('ctrl+t', 'table', 'Table'),
        Binding('ctrl+q', 'quit', 'Quit'),
    ]

    async def on_mount(self) -> None:
        pass

    def compose(self) -> ComposeResult:
        self.text_log = TextLog(id='repl-result2', highlight=False, wrap=True)
        self.top: Container = Container(
            Static('', id='plot', markup=False, expand=True),
            Static('', id='table', markup=False, expand=True),
            id='top',
        )
        self.left_box = TextLog(id='text-box', highlight=False, wrap=True)
        self.right_box = Container(
            Static("This"),
            Static("panel"),
            Static("is"),
            Static("using"),
            Static("grid layout!", id="bottom-right-final"),
            id="bottom-right",
        )
        self.repl = ReplWidget(self.left_box, self.group_ctx, self.text_log)

        yield Container(
            self.text_log,
            self.top,
            self.left_box,
            self.right_box,
            Container(
                self.repl,
                id='repl'
            ),
            id="app-grid",
        )
        yield Footer()

        self.plot = PlotextMixin(self.top.container_size.width, self.top.container_size.height)
        self.dialog = AsyncDialog(id='mydialog')
        self.repl.focus()

        self.setup_tasks()

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
        self.dark = not self.dark
        self.repl.focus()

    def action_dialog(self) -> None:
        if self.dialog.visible:
            self.dialog.hide()
        else:
            self.dialog.show()

    def action_plot(self) -> None:
        self.plot.phase += 1
        self.plot.width = self.top.container_size.width
        self.plot.height = self.top.container_size.height
        text = f'width: {self.top.container_size.width}, height: {self.top.container_size.height}'

        self.query('#top > *').remove()
        self.query_one('#top', Container).mount(Static(self.plot, markup=False, expand=True))

    def action_table(self) -> None:
        container = Container(
            Static('This'),
            Static('panel'),
            Static('is'),
            Static('using'),
        )

        self.query('#top > *').remove()
        self.query_one('#top', Container).mount(container)

    async def on_key(self, event: events.Key) -> None:
        self.text_log.write(f'key: {event.key}, character: {event.character}')
        self.repl.focus()

    def setup_tasks(self):
        self.text_log.write('setup_tasks')
        setup_connection()

app: AsyncCli

async def app_main(click_context):
    global app
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


# hickack the exit command and hook action_quit() instead
@cli.command('exit')
def def_exit():
    global app
    asyncio.run(app.action_quit())


@cli.command()
def repl():
    old_ctx = click.get_current_context()
    group_ctx = old_ctx.parent or old_ctx
    group = group_ctx.command
    args = ''

    repl_command_name = old_ctx.command.name
    if isinstance(group_ctx.command, click.CommandCollection):
        available_commands = {
            cmd_name: cmd_obj
            for source in group_ctx.command.sources
            for cmd_name, cmd_obj in source.commands.items()  # type: ignore
        }
    else:
        available_commands = group_ctx.command.commands  # type: ignore
    available_commands.pop(repl_command_name, None)

    # while True:
    #     command = input('> ')
    #     try:
    #         args = shlex.split(command)
    #         print(args)
    #     except ValueError as e:
    #         click.echo("{}: {}".format(type(e).__name__, e))

    #     try:
    #         with group.make_context(None, args, parent=group_ctx) as ctx:  # type: ignore
    #             group.invoke(ctx)
    #             ctx.exit()
    #     except click.ClickException as e:
    #         e.show()
    #     except SystemExit:
    #         pass

    asyncio.run(app_main(group_ctx))


if __name__ == '__main__':
    invoke_context_wrapper(repl)
    cli(prog_name='cli')

