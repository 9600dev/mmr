from arctic import Arctic
from arctic.exceptions import NoDataFoundException
from click.core import MultiCommand
from click_help_colors import HelpColorsGroup
from click_option_group import optgroup, RequiredMutuallyExclusiveOptionGroup
from cloup import option as cloupoption
from cloup import option_group
from cloup.constraints import mutually_exclusive
from dataclasses import asdict, dataclass
from expression import pipe
from expression.collections import seq
from ib_insync.contract import Contract
from ib_insync.objects import PnLSingle, PortfolioItem, Position
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
from reactivex.observer import AutoDetachObserver
from reactivex.operators import debounce, filter, sample
from reactivex.scheduler.eventloop.asyncioscheduler import AsyncIOScheduler
from reactivex.scheduler.eventloop.asynciothreadsafescheduler import AsyncIOThreadSafeScheduler
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
from textual.coordinate import Coordinate
from textual.css.query import NoMatches
from textual.reactive import reactive, Reactive, var
from textual.scroll_view import ScrollView
from textual.widget import Widget
from textual.widgets import (
    Button,
    DataTable,
    DirectoryTree,
    Footer,
    Header,
    Input,
    Label,
    Pretty,
    Static,
    TextLog
)
from trader.batch.queuer import Queuer
from trader.cli.cli_renderer import TuiRenderer
from trader.cli.command_line import cli, common_options, default_config
from trader.cli.commands import *  # NOQA
from trader.cli.commands import (
    book_helper,
    cli_client_id,
    invoke_context_wrapper,
    monkeypatch_click_echo,
    portfolio_helper,
    resolve_conid_to_security_definition,
    setup_cli,
    strategy_helper
)
from trader.cli.custom_footer import CustomFooter
from trader.cli.dialog import Dialog
from trader.cli.repl_input import ReplInput
from trader.common.dataclass_cache import DataClassEvent, UpdateEvent
from trader.common.exceptions import TraderConnectionException, TraderException
from trader.common.helpers import contract_from_dict, DictHelper, rich_dict, rich_json, rich_list, rich_table
from trader.common.logging_helper import LogLevels, set_log_level, setup_logging
from trader.common.reactivex import SuccessFail
from trader.container import Container as TraderContainer
from trader.data.data_access import DictData, TickData, TickStorage
from trader.data.market_data import MarketData
from trader.data.universe import Universe, UniverseAccessor
from trader.listeners.ibreactive import IBAIORx, WhatToShow
from trader.messaging.clientserver import MessageBusClient, RPCClient, TopicPubSub
from trader.messaging.trader_service_api import TraderServiceApi
from trader.objects import BarSize
from typing import Any, cast, Dict, List, Optional, Union

import asyncio
import click
import datetime as dt
import os
import pandas as pd
import plotext as plt
import shlex
import sys
import trader.batch.ib_history_queuer as ib_history_queuer
import trader.cli.universes_cli as universes_cli


class PlotextMixin(JupyterMixin, Widget):
    update = reactive('update')

    def __init__(self, width, height):
        super().__init__()
        self.decoder = AnsiDecoder()
        self.rich_canvas = Group()
        self.width = width
        self.height = height
        self.y_values = []
        self.filter = 0
        self.update = reactive('update')
        self.dt = dt.datetime.now()

    def filter_plot(self, conId: int):
        self.y_values = []
        self.rich_canvas = Group(*self.decoder.decode('waiting for data...'))
        self.filter = conId
        self.dt = dt.datetime.now()

    def make_plot(self, x, y, width, height):
        plt.clf()
        plt.grid(1, 1)
        plt.title(self.filter)
        plt.xticks([0.0, len(self.y_values) - 1], [self.dt.strftime('%H:%M:%S'), dt.datetime.now().strftime('%H:%M:%S')])
        plt.plot(
            list(range(0, len(self.y_values))),
            self.y_values,
            marker='hd')

        plt.plotsize(width, height)
        return plt.build()

    def ticker(self, ticker: Ticker):
        if ticker.contract and ticker.contract.conId == self.filter:
            last = ticker.last if ticker.last >= 0.0 else ticker.close
            self.y_values.append(last)
            canvas = self.make_plot(range(0, len(self.y_values)), self.y_values, self.width, self.height)
            self.rich_canvas = Group(*self.decoder.decode(canvas))

    def __rich_console__(self, console, options):
        yield self.rich_canvas


class InputBox(Widget):
    input_text: Union[Reactive[str], str] = Reactive("")

    def render(self) -> RenderableType:
        return f"[blue]mmr❯[/blue] {self.input_text}"

    def set_input_text(self, input_text: str) -> None:
        self.input_text = input_text


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
        self.command_list: List[str] = []
        self.command_list_index = 0

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
                except click.exceptions.Exit as ex:
                    self.output.write(ex)
                    pass
                output.seek(0)
                self.output.write(output.read())
        except click.ClickException as e:
            e.show(file=output)
            output.seek(0)
            self.output.write(output.read())

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
        self.command_list.append(event.value)
        self.input_box.value = ''
        self.command_list_index = len(self.command_list) - 1
        self.input_box.focus()

    def on_repl_input_changed(self, event: ReplInput.Changed):
        pass
        # self.write_debug(str(event))

    def on_repl_up_down(self, event: str):
        if event == 'up':
            if self.command_list and self.command_list_index >= 0:
                self.input_box.value = self.command_list[self.command_list_index]
                if self.command_list_index > 0:
                    self.command_list_index -= 1
        elif event == 'down':
            if self.command_list and self.command_list_index < len(self.command_list):
                self.input_box.value = self.command_list[self.command_list_index]
                self.command_list_index += 1
            else:
                self.input_box.value = ''
                self.command_list_index = len(self.command_list) - 1

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
        arctic_server_address: str,
        arctic_universe_library: str,
        zmq_pubsub_server_address: str,
        zmq_pubsub_server_port: int,
        zmq_messagebus_server_address: str,
        zmq_messagebus_server_port: int,
        group_ctx: Optional[click.core.Context] = None,
    ):
        super().__init__()
        self.group_ctx = group_ctx
        self.renderer = TuiRenderer(DataTable())
        self.hidden_container = Container(id='hidden-container')
        self.zmq_ticker = TopicPubSub[Ticker](
            zmq_pubsub_server_address,
            zmq_pubsub_server_port,
        )
        self.zmq_subscriber = TopicPubSub[DataClassEvent](
            zmq_pubsub_server_address,
            zmq_pubsub_server_port + 1,
        )
        self.zmq_messagebus_client = MessageBusClient(zmq_messagebus_server_address, zmq_messagebus_server_port)
        self.remoted_client: RPCClient[TraderServiceApi]
        self.scheduler = AsyncIOScheduler(asyncio.get_running_loop())
        self.arctic_server_address = arctic_server_address
        self.arctic_universe_library = arctic_universe_library

    """A Textual app to manage stopwatches."""
    CSS_PATH = 'trader/cli/css.css'
    BINDINGS = [
        # Binding('ctrl+d', 'toggle_dark', 'Toggle dark mode'),
        # Binding('ctrl+u', 'dialog', 'Dialog'),
        Binding('ctrl+p', 'plot', 'Plot'),
        Binding('ctrl+b', 'book', 'Book'),
        Binding('ctrl+s', 'strategy', 'Strategies'),
        Binding('ctrl+q', 'quit', 'Quit'),
    ]

    async def on_mount(self) -> None:
        pass

    def compose(self) -> ComposeResult:
        self.data_table: DataTable = DataTable()
        self.portfolio_table: DataTable = DataTable()
        self.book_table: DataTable = DataTable()
        self.strategy_table: DataTable = DataTable()

        self.repl_log = TextLog(id='text-box', highlight=False, wrap=True)
        self.text_log = TextLog(id='repl-result2', highlight=False, wrap=True)

        self.top: Container = Container(
            Container(self.data_table, classes='hidden'),
            id='top',
        )

        self.plot = PlotextMixin(self.top.container_size.width, self.top.container_size.height)
        self.plot_static = Static(self.plot, expand=True, markup=False)

        self.right_box = Container(
            Container(self.portfolio_table),
            id='bottom-right',
        )

        self.left_box = Container(
            Container(self.repl_log, id='repl-log'),
            Container(self.book_table, id='book-table', classes='hidden'),
            Container(self.strategy_table, id='strategy-table', classes='hidden'),
            id='bottom-left'
        )

        self.repl = ReplWidget(self.repl_log, self.group_ctx, self.text_log)

        yield Container(
            self.text_log,
            self.top,
            self.left_box,
            # self.repl_log,
            self.right_box,
            Container(
                self.repl,
                id='repl'
            ),
            id="app-grid",
        )

        self.custom_footer = CustomFooter()
        yield self.custom_footer

        self.dialog = AsyncDialog(id='mydialog')
        self.repl.focus()

        self.renderer.set_table(self.data_table)

        # todo
        asyncio.create_task(self.setup_tasks())

        yield self.dialog

    def switch_widget(self, container_id: str, widget_id: str):
        panel = self.query_one(container_id, Container)

        for child_widget in panel.children:
            if not child_widget.has_class('hidden') and child_widget.id != widget_id:
                child_widget.toggle_class('hidden')
            if child_widget.id == widget_id and child_widget.has_class('hidden'):
                child_widget.toggle_class('hidden')

    def replace_top_panel(self, top_widget: Widget):
        top = self.query_one('#top', Container)

        for widget in top.children:
            if not widget.has_class('hidden'):
                widget.toggle_class('hidden')

        # check to see if needed to mount
        mount = True
        for widget in top.children:
            if widget == top_widget:
                mount = False

        if mount:
            self.query_one('#top', Container).mount(top_widget, before=0)

        if top_widget.has_class('hidden'):
            top_widget.toggle_class('hidden')

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
        self.plot.width = self.top.container_size.width
        self.plot.height = self.top.container_size.height
        text = f'width: {self.top.container_size.width}, height: {self.top.container_size.height}'
        self.text_log.write(text)
        self.replace_top_panel(Container(self.plot_static))   # Static(self.plot, markup=False, expand=True)))

    def action_book(self) -> None:
        self.render_book()
        self.switch_widget('#bottom-left', 'book-table')

    def action_strategy(self) -> None:
        self.render_strategies()
        self.switch_widget('#bottom-left', 'strategy-table')

    def render_portfolio(self) -> None:
        df = portfolio_helper()
        df.drop(columns=['account', 'currency', 'averageCost'], inplace=True)
        TuiRenderer(self.portfolio_table).rich_table(df, financial=True, column_key='conId')

        # post the pnl to the footer
        daily_pnl_sum = df['dailyPNL'].sum()
        unrealized_pnl = df['unrealizedPNL'].sum()
        realized_pnl = df['realizedPNL'].sum()
        self.custom_footer.post_message(
            CustomFooter.FooterMessage(daily_pnl_sum, unrealized_pnl, realized_pnl)
        )

    def render_book(self) -> None:
        df = book_helper()
        TuiRenderer(self.book_table).rich_table(df, financial=True, column_key='orderId')

    def render_strategies(self) -> None:
        df = strategy_helper()
        TuiRenderer(self.strategy_table).rich_table(df, financial=True, column_key='name')
        pass

    def start_plot(self, conid: Union[int, str]) -> None:
        if type(conid) == str and str(conid).isnumeric():
            conid = int(conid)

        definition = resolve_conid_to_security_definition(int(conid))
        if definition:
            contract = Universe.to_contract(definition)

            self.plot.filter_plot(int(conid))
            self.action_plot()
            self.remoted_client.rpc().publish_contract(contract=contract, delayed=False)
        else:
            self.text_log.write(f'Could not resolve conid: {conid}')

    def on_tui_message(self, event: TuiRenderer.TuiMessage) -> None:
        if event.table == self.data_table:
            container = Container(self.data_table)
            self.replace_top_panel(container)

    def on_data_table_cell_selected(self, event: DataTable.CellSelected):
        if event.sender == self.portfolio_table:
            if event.cell_key.column_key == 'conId':
                self.start_plot(str(event.value))

    async def on_key(self, event: events.Key) -> None:
        if event.key == 'up' or event.key == 'down':
            self.repl.on_repl_up_down(event.key)
        # self.text_log.write(f'key: {event.key}, character: {event.character}')
        self.repl.focus()

    def on_repl_input_submitted(self, event: ReplInput.Submitted):
        self.switch_widget('#bottom-left', 'repl-log')

    async def ticker_listen(self):
        def on_next(ticker: Ticker):
            symbol = ticker.contract.symbol if ticker.contract else ''
            last = ticker.last if ticker.last >= 0.0 else ticker.close

            self.text_log.write('{} {} {}'.format(dt.datetime.now().strftime('%H:%M:%S'), symbol, last))
            self.plot.ticker(ticker)
            self.plot_static.refresh(repaint=True, layout=True)

        def on_error(error):
            self.text_log.write(f'ticker error: {error}')
            self.plot.filter_plot(0)

        observable = await self.zmq_ticker.subscriber(topic='ticker')
        self.observer = AutoDetachObserver(on_next=on_next, on_error=on_error)
        xs = observable.pipe(
            filter(lambda ticker: ticker.contract is not None and ticker.contract.conId == self.plot.filter),
            sample(sampler=2.0, scheduler=self.scheduler)
        )
        self.subscription = xs.subscribe(self.observer)

    async def setup_tasks(self):
        self.text_log.write('setup_tasks')

        def on_next(data: DataClassEvent):
            self.text_log.write(f'next: {data}')
            if data is type(UpdateEvent) and cast(UpdateEvent, data).item is Trade:
                # trade = cast(Trade, cast(UpdateEvent, data).item)
                # order_status = asdict(trade.orderStatus)
                # TuiRenderer(self.book_table).rich_dict(order_status)
                # bind the trade to the datatable
                pass
            elif type(data) is UpdateEvent and cast(UpdateEvent, data).item is Order:
                # order = cast(Order, cast(UpdateEvent, data).item)
                # order_dict = asdict(order)
                # TuiRenderer(self.book_table).rich_dict(order_dict)
                pass
            elif type(data) is UpdateEvent and type(cast(UpdateEvent, data).item) is PnLSingle:
                pnl_single = cast(PnLSingle, cast(UpdateEvent, data).item)
                # we're not setup to update the portfolio datatable just yet
                pass

        def on_error(error):
            self.text_log.write(f'error: {error}')

        def on_completed():
            pass

        self.remoted_client, _ = setup_cli(self.renderer)

        await self.zmq_messagebus_client.connect()
        self.signal_observer = AutoDetachObserver(on_next=on_next, on_error=on_error, on_completed=on_completed)
        disposable = self.zmq_messagebus_client.subscribe(topic='signal', observer=self.signal_observer)

        observable = await self.zmq_subscriber.subscriber(topic='dataclass')
        self.observer = AutoDetachObserver(on_next=on_next, on_error=on_error, on_completed=on_completed)
        self.subscription = observable.subscribe(self.observer)

        await self.ticker_listen()

        self.render_portfolio()
        self.scheduler.schedule_periodic(3.0, lambda x: self.render_portfolio())
        self.scheduler.schedule_periodic(3.0, lambda x: self.render_book())
        self.scheduler.schedule_periodic(3.0, lambda x: self.render_strategies())

        # setup the plot to start drawing on startup
        cell = self.portfolio_table.get_cell_at(Coordinate(0, 0))
        self.start_plot(str(cell))

app: AsyncCli

async def app_main(click_context):
    global app

    trader_container = TraderContainer()
    app = trader_container.resolve(AsyncCli)
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
