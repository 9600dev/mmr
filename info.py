from expression import pipe
from expression.collections import seq
from ib_insync.contract import Contract
from ib_insync.objects import PortfolioItem
from ib_insync.ticker import Ticker
from reactivex.observer import AutoDetachObserver
from rich.align import Align
from rich.table import Table
from textual.app import App
from textual.widget import Widget
from trader.common.exceptions import TraderConnectionException, TraderException
from trader.common.helpers import rich_tablify
from trader.messaging.clientserver import RemotedClient, TopicPubSub
from trader.messaging.trader_service_api import TraderServiceApi
from typing import Dict, List

import asyncio
import click
import numpy as np
import pandas as pd
import random


class PortfolioTickers(Widget):
    def __init__(
        self,
        name: str,
        zmq_pubsub_server_address: str,
        zmq_pubsub_server_port: int,
        topic: str
    ):
        super().__init__(name)
        self.topic = topic
        self.contract_ticks: Dict[Contract, Ticker] = {}
        self.zmq_subscriber = TopicPubSub[Ticker](
            zmq_pubsub_server_address,
            zmq_pubsub_server_port,
        )

    def get_ticker(self, ticker: Ticker):
        date_time_str = ticker.time.strftime('%H:%M.%S') if ticker.time else ''
        return {
            'symbol': ticker.contract.symbol if ticker.contract else '',
            'primaryExchange': ticker.contract.primaryExchange if ticker.contract else '',
            'currency': ticker.contract.currency if ticker.contract else '',
            'time': date_time_str,
            'bid': '%.2f' % ticker.bid,
            'ask': '%.2f' % ticker.ask,
            'last': '%.2f' % ticker.last,
            'lastSize': int(ticker.lastSize) if not np.isnan(ticker.lastSize) else -1,
            'open': '%.2f' % ticker.open,
            'high': '%.2f' % ticker.high,
            'low': '%.2f' % ticker.low,
            'close': '%.2f' % ticker.close,
            'halted': int(ticker.halted) if not np.isnan(ticker.halted) else -1
        }

    def on_next(self, ticker: Ticker):
        if ticker.contract:
            self.contract_ticks[ticker.contract] = ticker
            self.refresh()

    async def on_mount(self):
        observable = await self.zmq_subscriber.subscriber(topic=self.topic)
        self.observer = AutoDetachObserver(
            on_next=self.on_next,
        )
        self.subscription = observable.subscribe(self.observer)

    def render(self):
        data = [self.get_ticker(ticker) for contract, ticker in self.contract_ticks.items()]
        df = pd.DataFrame(data)

        if len(data) > 0:
            self.table = Table(expand=True)
            for column in df.columns:
                self.table.add_column(column)

            for index, value_list in enumerate(df.values.tolist()):
                row = [str(x) for x in value_list]
                self.table.add_row(*row)

            return self.table
        else:
            return Align.center('no ticker subscriptions', vertical='middle')


class Orders(Widget):
    def __init__(self):
        super().__init__('orders')
        self.body = Table()

    async def render_content(self):
        table = Table(title='Orders', expand=True)

        for i in range(2):
            table.add_column(f"Col {random.randint(0, 4) + 1}", style="magenta")
        for i in range(5):
            table.add_row(*[f"cell asdf {i},{j}" for j in range(2)])
        self.body = table

    async def on_mount(self):
        self.set_interval(1, self.render_content)
        self.set_interval(1, self.refresh)
        await self.render_content()

    def render(self):
        return self.body


class Portfolio(Widget):
    def __init__(self, remoted_client: RemotedClient):
        super().__init__('portfolio')
        self.body = Table()
        self.remoted_client = remoted_client

    def mapper(self, portfolio: PortfolioItem) -> List:
        return [
            portfolio.contract.localSymbol,
            portfolio.marketValue,
            portfolio.unrealizedPNL,
            portfolio.realizedPNL,
        ]

    async def render_content(self):
        portfolio: list[PortfolioItem] = self.remoted_client.rpc(return_type=list[PortfolioItem]).get_portfolio()

        xs = pipe(
            portfolio,
            seq.map(self.mapper)
        )

        if len(portfolio) > 0:
            df = pd.DataFrame(data=list(xs), columns=[
                'sym', 'val', 'unPNL', 'rePNL'
            ])

            table = rich_tablify(df, financial=True, financial_columns=[
                'marketPrice', 'val', 'averageCost', 'unPNL', 'rePNL'
            ])
            table.expand = True
            table.title = 'Positions'
            self.body = table
        else:
            self.body = Table()

    async def on_mount(self):
        self.set_interval(1, self.render_content)
        self.set_interval(1, self.refresh)
        await self.render_content()

    def render(self):
        return self.body


class TraderLog(Widget):
    def __init__(self):
        super().__init__('log')
        self.body = Table()

    async def render_content(self):
        table = Table(title='Trader Log', expand=True)

        for i in range(2):
            table.add_column(f"Col {random.randint(0, 4) + 1}", style="magenta")
        for i in range(5):
            table.add_row(*[f"cell asdf {i},{j}" for j in range(2)])
        self.body = table

    async def on_mount(self):
        self.set_interval(1, self.render_content)
        self.set_interval(1, self.refresh)
        await self.render_content()

    def render(self):
        return self.body


error_table = {
    'trader.common.exceptions.TraderException': TraderException,
    'trader.common.exceptions.TraderConnectionException': TraderConnectionException
}


class MMRViewer(App):
    def __init__(
        self,
        topic: str,
        zmq_pubsub_server_address: str,
        zmq_pubsub_server_port: int,
        screen: bool = True,
        driver_class=None,
        log: str = "",
        log_verbosity: int = 1,
        title: str = "Textual Application",
    ):
        super().__init__(screen, driver_class, log, log_verbosity, title)
        self.tickers = PortfolioTickers('Portfolio', zmq_pubsub_server_address, zmq_pubsub_server_port, topic)
        self.remoted_client = RemotedClient[TraderServiceApi](error_table=error_table)

    def connect(self):
        if not self.remoted_client.connected:
            asyncio.get_event_loop().run_until_complete(self.remoted_client.connect())

    async def on_mount(self) -> None:
        self.connect()
        self.portfolio = Portfolio(self.remoted_client)
        self.orders = Orders()
        self.trader_log = TraderLog()

        grid = await self.view.dock_grid(edge="left", name="left")
        grid.add_column(fraction=1, name="left", min_size=20)
        grid.add_column(fraction=1, name="right")

        grid.add_row(fraction=1, name="top", min_size=10)
        grid.add_row(fraction=1, name="middle")
        grid.add_row(fraction=1, name="bottom")

        grid.add_areas(
            area1="left-start|right-end,top",
            area2="right,top-end|bottom-end",
            area3="left,middle",
            area4="left,bottom",
        )

        grid.place(
            area1=self.tickers,
            area2=self.trader_log,
            area3=self.portfolio,
            area4=self.orders,
        )

        # grid.place(
        #     area1=Placeholder(name="area1"),
        #     area2=Placeholder(name="area2"),
        #     area3=Placeholder(name="area3"),
        #     area4=Placeholder(name="area4"),
        # )


@click.command()
@click.option('--topic', required=True, default='ticker')
@click.option('--zmq_pubsub_server_address', required=True, default='tcp://127.0.0.1')
@click.option('--zmq_pubsub_server_port', required=True, default=42002)
def main(
    topic: str,
    zmq_pubsub_server_address: str,
    zmq_pubsub_server_port: int
):
    viewer = MMRViewer(
        topic=topic,
        zmq_pubsub_server_address=zmq_pubsub_server_address,
        zmq_pubsub_server_port=zmq_pubsub_server_port,
    )

    viewer.run(
        topic=topic,
        zmq_pubsub_server_address=zmq_pubsub_server_address,
        zmq_pubsub_server_port=zmq_pubsub_server_port,
    )

if __name__ == '__main__':
    main()
