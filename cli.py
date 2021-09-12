import inspect
from re import I
import sys
import os
from trader.listeners.ibaiorx import IBAIORx
import requests
import click
import yaml
import json
import pathlib
import pandas as pd
import pandas.util as u
import plotille as plt
import asyncio
import shlex
import io
import time
import logging
import tabview as t
import expression
import locale

from collections import namedtuple
from typing import Type, TypeVar, Dict, Optional, List, cast
from rq import Queue
from rq.job import Job
from redis import Redis
from trader.container import Container
from trader.common.data import TickData, DictData
from trader.common.logging_helper import setup_logging, set_all_log_level
from trader.listeners.ibrx import IBRx
from trader.listeners.polygon_listener import PolygonListener
from trader.batch.polygon_batch import PolygonQueuer
from scripts.trader_check import health_check
from ib_insync.contract import Contract, ContractDescription
from ib_insync import IB, Stock, PortfolioItem
from rx.scheduler.eventloop import AsyncIOThreadSafeScheduler
from tabulate import tabulate
from trader.common.helpers import rich_table, rich_dict, rich_json, DictHelper
from trader.listeners.ibaiorx import WhatToShow
from trader.common.helpers import *
from IPython import get_ipython
from pyfiglet import Figlet
from prompt_toolkit import prompt
from prompt_toolkit.history import FileHistory, InMemoryHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import Completer, Completion, NestedCompleter
from prompt_toolkit import PromptSession
from prompt_toolkit.shortcuts import CompleteStyle
from pygments.lexers.sql import SqlLexer
from lightbus import BusPath
from trader.messaging.bus import *
from ib_insync.objects import Position
from expression.collections import Seq, seq
from expression import Nothing, Option, Some, effect, option, pipe
from types import SimpleNamespace


class CLIDispatcher():
    def __init__(self,
                 root_directory: str,
                 pycron_server_address: str,
                 pycron_server_port: int,
                 config_file: str):
        self.pycron_server_address = pycron_server_address
        self.pycron_server_port = pycron_server_port
        self.config_file = config_file
        self.symbols = pd.read_csv(root_directory + '/data/ib_symbols_nyse_nasdaq.csv')
        self.container = Container(config_file)
        global bus
        self.bus_client: BusPath = bus

    def _bind_client(self):
        self.client = self.container.resolve(IBAIORx)

    def pycron_restart(self, service: str):
        response = requests.get('http://{}:{}/?restart={}'.format(self.pycron_server_address, self.pycron_server_port, service))
        print(response.json())
        rich_json(response.json())

    def pycron_info(self):
        response = requests.get('http://{}:{}'.format(self.pycron_server_address, self.pycron_server_port))
        rich_json(response.json())

    def portfolio(self, csv: bool = False):
        portfolio: List[Dict] = cast(List[Dict], bus.service.get_portfolio())

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
        rich_table(df.sort_values(by='currency'), csv=csv, financial=True, financial_columns=[
            'marketPrice', 'marketValue', 'averageCost', 'unrealizedPNL', 'realizedPNL'
        ])
        if not csv:
            rich_table(df.groupby(by=['currency'])['marketValue'].sum().reset_index(), financial=True)
            rich_table(df.groupby(by=['currency'])['unrealizedPNL'].sum().reset_index(), financial=True)

    def positions(self, csv: bool = False):
        positions: List[Dict] = cast(List[Dict], bus.service.get_positions())

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
        rich_table(df.sort_values(by='currency'), financial=True, financial_columns=['total', 'avgCost'], csv=csv)
        if not csv:
            rich_table(df.groupby(by=['currency'])['total'].sum().reset_index(), financial=True)

    def universe(self, exchange: str = 'NASDAQ'):
        data = container.resolve(TickData, arctic_library='Historical')

    def reconnect(self):
        bus.service.reconnect()

    def exit(self):
        sys.exit(0)

    def clear(self):
        print(chr(27) + "[2J")

    def resolve(self, symbol: str, csv: bool = False):
        result = self.__resolve(symbol)
        rich_table(result, csv)

    def __resolve(self, symbol: str):
        result = self.symbols.loc[self.symbols['symbol'].str.lower() == symbol.lower()]
        if len(result) == 0:  # type: ignore
            try:
                result = self.symbols.loc[self.symbols['conId'] == int(symbol)]
            except Exception as ex:
                pass
        if len(result) == 0:  # type: ignore
            result = self.symbols[self.symbols['company name'].str.lower().str.contains(symbol.lower())]
        return result

    def cinfo(self, symbol: str, csv: bool = False):
        financials = self.container.resolve(DictData, arctic_library='HistoricalPolygonFinancials')
        res = self.__resolve(symbol)
        conId = 0
        if len(res) > 0:
            conId = res.iloc[0].conId
            rich_table(financials.read(financials.symbol_to_contract(conId)).financials, csv)

    # def data(self, symbol: str, csv: bool = False):
    #     data = container.resolve(TickData, arctic_library='Historical')
    #     symbols = {}
    #     if symbol:
    #         contract = data.symbol_to_contract(symbol)
    #         min_date, max_date = data.date_summary(contract)
    #         symbols[contract] =

    def status(self):
        result = health_check(self.config_file)
        rich_dict({'status': result})

    def help(self):
        f = Figlet(font='starwars')
        print(f.renderText('MMR'))
        completions = self.generate_completer()
        print('commands:')
        for key, _ in completions.items():
            print('  {}'.format(key))

    def generate_completer(self):
        def fill_dict(d, keys: List[str]):
            index = d
            for key in keys:
                if key in index and type(index[key]) is dict:
                    index = index[key]
                else:
                    index[key] = {}
                    index = index[key]

        completions = {}
        functions = inspect.getmembers(CLIDispatcher, predicate=inspect.isfunction)
        functions = [(name, f) for (name, f) in functions if '__' not in name and 'generate' not in name]

        for func_name, func in functions:
            if '_' in func_name:
                names = func_name.split('_')
                fill_dict(completions, names)
            else:
                fill_dict(completions, [func_name])
        return completions

    def generate_dispatch(self, input: str):
        def dispatch(func, args: List, live: bool):
            if live:
                while True:
                    try:
                        func(*args)
                        time.sleep(1)
                    except KeyboardInterrupt:
                        return
            else:
                func(*args)

        if not input:
            return

        functions = inspect.getmembers(self, predicate=inspect.ismethod)
        functions = [(name, f) for (name, f) in functions if '__' not in name and 'generate' not in name]
        func_dict = {}
        for key, value in functions:
            func_dict[key] = value

        args = shlex.split(input)
        live = 'live' in args
        if live: args.remove('live')

        # todo: the args need to be coerced from string to the appropriate type for the method
        dispatched = False
        for i in range(1, len(args) + 1):
            function_name = '_'.join(args[:i])
            if function_name in func_dict:
                # dispatch
                dispatch(func_dict[function_name], args[i:], live)
                dispatched = True
        if not dispatched:
            print('no command {} found'.format(input))


def repl(config_file: str = '/home/trader/mmr/configs/trader.yaml'):
    ctrl_count = 0
    container = Container(config_file)
    dispatcher = container.resolve(CLIDispatcher)
    dispatcher.help()
    completer = NestedCompleter.from_nested_dict(dispatcher.generate_completer())

    while True:
        try:
            user_input = prompt('mmr> ',
                                history=FileHistory('data/history.txt'),
                                auto_suggest=AutoSuggestFromHistory(),
                                completer=completer,
                                vi_mode=True,
                                complete_style=CompleteStyle.READLINE_LIKE)
            print(user_input)
            dispatcher.generate_dispatch(user_input)
            ctrl_count = 0
        except KeyboardInterrupt:
            print('Ctrl-C pressed. One more to exit!')
            ctrl_count += 1
            if ctrl_count > 1:
                sys.exit(0)

def command_line(args, config_file: str = '/home/trader/mmr/configs/trader.yaml'):
    user_input = ' '.join(args[1:])
    container = Container(config_file)
    dispatcher = container.resolve(CLIDispatcher)
    dispatcher.generate_dispatch(user_input)


# globals for ipython
setup = False
container: Container = None
dispatch = None
amd = None
symbols = None
symbols_cache: Dict[str, str] = {}
poly = None
t = None
p = None
d = None
f = None
rq = None
ib = None
client = None
arx = None
r = None

def setup_ipython_environment(config_file: str = '/home/trader/mmr/configs/trader.yaml'):
    global setup
    global dispatch
    global container
    global amd
    global symbols
    global symbols_cache
    global poly
    global t, p, d, rq, f, r
    global ib
    global client
    global arx

    if setup:
        return
    else:
        setup = True

    set_all_log_level(logging.INFO)
    # environment variables overrule config_file default's
    container = Container(config_file)
    dispatch = container.resolve(CLIDispatcher)
    amd = Contract(symbol='AMD', secType='STK', exchange='SMART', conId=4391)
    symbols = pd.read_csv(container.config()['root_directory'] + '/data/ib_symbols_nyse_nasdaq.csv')
    for index, value in symbols[['symbol', 'conId']].iterrows():
        symbols_cache[value[0]] = value[1]  # type: ignore
    poly = PolygonListener()
    t = container.resolve(TickData)  # (arctic_server_address=container.config()['arctic_server_address'])
    p = container.resolve(TickData, arctic_library='HistoricalPolygon')
    d = container.resolve(DictData, arctic_library='HistoricalMetadata')
    f = container.resolve(DictData, arctic_library='HistoricalPolygonFinancials')
    r = container.resolve(Redis, host=container.config()['redis_server_address'], port=container.config()['redis_server_port'])
    rq = Queue('history', connection=r)
    ib_server_address = container.config()['ib_server_address']
    ib_server_port = container.config()['ib_server_port']

    ib = IB()
    client = IBRx(ib, scheduler=AsyncIOThreadSafeScheduler(asyncio.get_event_loop()))
    arx = IBAIORx(ib_server_address=ib_server_address, ib_server_port=ib_server_port)
    client.connect(ib_server_address=ib_server_address, ib_server_port=ib_server_port)

def main(args):
    set_all_log_level(logging.INFO)
    if len(args) > 1:
        command_line(args)
    else:
        repl()


if __name__ == '__main__':
    main(sys.argv)


if get_ipython().__class__.__name__ == 'TerminalInteractiveShell':  # type: ignore
    setup_ipython_environment()
