import sys
import os

# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

import coloredlogs
import asyncio
from trader.common.logging_helper import setup_logging, suppress_external
from trader.common.helpers import rich_dict
from trader.data.data_access import IBTradeConfirm
from trader.data.data_access import SecurityDefinition, TickData

logging = setup_logging(module_name='analysis')

import pandas as pd
import datetime as dt
import click
import os
import glob
import vectorbt as vbt

from typing import AsyncIterator, List, Optional
from ib_insync.contract import Contract, ContractDetails
from trader.listeners.ibaiorx import IBAIORx
from ib_insync.flexreport import FlexReport

class Reporting():
    def __init__(
        self,
    ):
        self.trades: pd.DataFrame = pd.DataFrame()
        self.dividends: pd.DataFrame = pd.DataFrame()
        self.fees_taxes: pd.DataFrame = pd.DataFrame()
        self.cash: pd.DataFrame = pd.DataFrame()
        self.interest: pd.DataFrame = pd.DataFrame()

    def load_report(self, flex: FlexReport):
        def date_parser(date_str: str):
            date_str = str(date_str)
            if ';' in date_str:
                return dt.datetime.strptime(date_str, '%Y%m%d;%H%M%S')
            else:
                return dt.datetime.strptime(date_str, '%Y%m%d')

        def add_and_clean(
            current_df: pd.DataFrame,
            additional_df: pd.DataFrame,
            unique_id: str,
            date_time_column: str = 'dateTime'
        ):
            df = pd.concat([current_df, additional_df], ignore_index=True)  # type: ignore
            if unique_id:
                df = df.drop_duplicates(subset=[unique_id])
            df['date_time'] = df[date_time_column].apply(date_parser)
            df = df.sort_values(by='date_time', ascending=True)
            df = df.set_index('date_time')  # type: ignore
            df.index = df.index.tz_localize('America/New_York')  # type: ignore
            return df

        # grab all the orders
        if 'Order' in flex.topics():
            self.trades = add_and_clean(self.trades, flex.df('Order'), 'orderID')  # type: ignore
            self.trades = self.trades.query('assetCategory == "STK"')
            self.trades = self.trades.dropna(subset='price')  # type: ignore

        # grab the account cash inputs/outputs and taxes/fees
        if 'CashTransaction' in flex.topics():
            self.cash = add_and_clean(self.cash, flex.df('CashTransaction'), 'transactionID')  # type: ignore
            self.cash = self.cash.query('type == "Deposits/Withdrawals"')

            self.fees_taxes = add_and_clean(self.fees_taxes, flex.df('CashTransaction'), 'transactionID')  # type: ignore
            self.fees_taxes = self.fees_taxes.query('type != "Deposits/Withdrawals"')
            self.fees_taxes = self.fees_taxes.query('type != "Dividends"')
            self.fees_taxes = self.fees_taxes.query('type != "Interest"')

            self.interest = add_and_clean(self.interest, flex.df('CashTransaction'), 'transactionID')  # type: ignore
            self.interest = self.interest.query('type == "Interest"')

        # grab dividends
        if 'ChangeInDividendAccrual' in flex.topics():
            self.dividends = add_and_clean(self.dividends, flex.df('ChangeInDividendAccrual'), '', 'payDate')  # type: ignore

    def load_xml_reports_glob(self, glob_str: str):
        files = glob.glob(glob_str)
        return self.load_xml_reports(files)

    def load_xml_reports(self, reports: List[str]):
        for report in reports:
            flex = FlexReport(path=report)
            self.load_report(flex)

    def refresh(self):
        raise NotImplementedError('todo')


class TradeAnalysis():
    def __init__(
        self,
        reporting: Reporting,
    ):
        self.reporting = reporting

    def prepare_analysis(self):
        pass

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[df.symbol == 'AMD']  # type: ignore

    def run_analysis(self) -> vbt.Portfolio:
        # init_cash = float(self.reporting.cash.head(1).amount)
        init_cash = 1000000.0
        cash_deposits = self.reporting.cash.amount
        close = self.filter(self.reporting.trades).price
        size = self.filter(self.reporting.trades).quantity
        fees = self.filter(self.reporting.trades).commission * -1


        portfolio = vbt.Portfolio.from_orders(
            init_cash=init_cash,
            close=close,
            size=size,
            fixed_fees=fees,
            direction='both'
        )

        return portfolio

