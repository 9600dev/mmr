from bs4 import BeautifulSoup
from collections import deque
from dateutil.tz import gettz, tzlocal
from dateutil.tz.tz import tzfile
from exchange_calendars import ExchangeCalendar
from ib_insync.contract import Contract
from pandas import Timestamp
from pypager.pager import Pager
from pypager.source import GeneratorSource
from rich.console import Console
from rich.table import Table
from rich.text import Text
from scipy.stats import truncnorm
from textual.message import Message
from textual.widgets import DataTable
from textual.widgets.data_table import ColumnKey
from typing import Any, Callable, cast, Dict, Generic, List, Optional, Tuple, TypeVar, Union

import asyncio
import collections
import datetime as dt
import exchange_calendars as ec
import io
import json
import locale
import logging
import numpy as np
import os
import pandas as pd
import tempfile


def which(program):
    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file
    return None


class CliRenderer():
    def rich_json(
        self,
        json_str: str
    ):
        pass

    def rich_table(
        self,
        df,
        csv: bool = False,
        financial: bool = False,
        financial_columns: List[str] = [],
        include_index=False,
    ):
        pass

    def rich_dict(self, d: Dict):
        pass

    def rich_list(self, list_source: List):
        pass


class ConsoleRenderer(CliRenderer):
    def rich_json(self, json_str: str):
        try:
            df = pd.read_json(json.dumps(json_str))
            self.rich_table(df)
        except ValueError as ex:
            self.rich_dict(json_str)  # type: ignore

    def rich_tablify(self, df, financial: bool = False, financial_columns: List[str] = [], include_index=False):
        if type(df) is list:
            df = pd.DataFrame(df)

        if financial:
            locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

        cols: List[str] = list(df.columns)  # type: ignore
        table = Table()
        for column in df.columns:
            table.add_column(str(column))
        for row in df.itertuples():
            r = []
            for i in range(1, len(row)):
                if type(row[i]) is float and not financial:
                    r.append('%.3f' % row[i])
                elif type(row[i]) is float and financial:
                    if len(financial_columns) > 0 and cols[i - 1] in financial_columns:
                        r.append(locale.currency(row[i], grouping=True))
                    elif len(financial_columns) == 0 and financial:
                        r.append(locale.currency(row[i], grouping=True))
                    else:
                        r.append('%.3f' % row[i])
                else:
                    r.append(str(row[i]))
            table.add_row(*r)
        return table

    def rich_table(
        self,
        df,
        csv: bool = False,
        financial: bool = False,
        financial_columns: List[str] = [],
        include_index=False,
    ):
        if type(df) is list:
            df = pd.DataFrame(df)

        if csv:
            if which('vd'):
                temp_file = tempfile.NamedTemporaryFile(suffix='.csv')
                df.to_csv(temp_file.name, index=include_index, float_format='%.2f')
                os.system('vd {}'.format(temp_file.name))
                return None
            else:
                print(df.to_csv(index=False))
            return

        table = self.rich_tablify(df, financial, financial_columns, include_index)

        console = Console()
        console.print(table)

    def rich_dict(self, d: Dict):
        table = Table()
        table.add_column('key')
        table.add_column('value')
        for key, value in d.items():
            table.add_row(str(key), str(value))
        console = Console()
        console.print(table)

    def rich_list(self, list_source: List):
        d = {}
        for counter in range(0, len(list_source)):
            d[counter] = list_source[counter]
        self.rich_dict(d)


class TuiRenderer(CliRenderer):
    def __init__(
        self,
        table: DataTable
    ):
        super().__init__()
        self.table = table

    class TuiMessage(Message, bubble=True):
        def __init__(self, sender) -> None:
            super().__init__(sender)

    def set_table(self, table: DataTable):
        self.table = table

    def clear(self):
        self.table.columns.clear()
        self.table.clear()

    def focus(self):
        self.table.post_message_no_wait(TuiRenderer.TuiMessage(sender=self.table))

    def rich_json(self, json_str: str):
        try:
            df = pd.read_json(json.dumps(json_str))
            self.rich_table(df)
        except ValueError as ex:
            self.rich_dict(json_str)  # type: ignore

    def rich_tablify(self, df, financial: bool = False, financial_columns: List[str] = [], include_index=False):
        self.clear()

        import random
        if type(df) is list:
            df = pd.DataFrame(df)

        if financial:
            locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

        cols: List[str] = list(df.columns)  # type: ignore
        for column in df.columns:
            self.table.add_column(str(column))
        for row in df.itertuples():
            key = random.randint(0, 10000000)
            r = []
            for i in range(1, len(row)):
                if type(row[i]) is float and not financial:
                    r.append('%.3f' % row[i])
                elif type(row[i]) is float and financial:
                    if len(financial_columns) > 0 and cols[i - 1] in financial_columns:
                        r.append(locale.currency(row[i], grouping=True))
                    elif len(financial_columns) == 0 and financial:
                        r.append(locale.currency(row[i], grouping=True))
                    else:
                        r.append('%.3f' % row[i])
                else:
                    r.append(str(row[i]))
            self.table.add_row(*r, key=str(key))
        self.focus()

    def rich_table(
        self,
        df,
        csv: bool = False,
        financial: bool = False,
        financial_columns: List[str] = [],
        include_index=False,
    ):
        if type(df) is list:
            df = pd.DataFrame(df)

        self.clear()
        self.rich_tablify(df, financial, financial_columns, include_index)

    def rich_dict(self, d: Dict):
        self.clear()

        self.table.add_column('key')
        self.table.add_column('value')
        for key, value in d.items():
            self.table.add_row(str(key), str(value), key=str(key))

        self.focus()

    def rich_list(self, list_source: List):
        d = {}
        for counter in range(0, len(list_source)):
            d[counter] = list_source[counter]
        self.rich_dict(d)
