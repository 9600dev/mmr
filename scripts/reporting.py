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

from typing import AsyncIterator, List, Optional, Dict
from ib_insync.contract import Contract, ContractDetails
from trader.listeners.ibaiorx import IBAIORx
from ib_insync.flexreport import FlexReport
from tabulate import tabulate
from pandas.core.series import Series

def pretty(df):
    if type(df) is Series:
        df = pd.DataFrame(df)

    print(tabulate(df, headers='keys'))

class Reporting():
    def __init__(
        self,
    ):
        self.trades: pd.DataFrame = pd.DataFrame()
        self.dividends: pd.DataFrame = pd.DataFrame()
        self.fees_taxes: pd.DataFrame = pd.DataFrame()
        self.cash: pd.DataFrame = pd.DataFrame()
        self.interest: pd.DataFrame = pd.DataFrame()
        self.corporate_actions: pd.DataFrame = pd.DataFrame()
        self.tables = ['trades', 'dividends', 'fees_taxes', 'cash', 'interest', 'corporate_actions']

    def query(self, query):
        reporting = Reporting()
        for table in self.tables:
            setattr(reporting, table, getattr(self, table).query(query))
        return reporting

    def date_filter(self, start: dt.datetime, end: dt.datetime = dt.datetime.now()):
        reporting = Reporting()
        for table in self.tables:
            setattr(reporting, table, getattr(self, table)[start:end])
        return reporting

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
            df['date'] = df.index
            return df

        logging.debug('load_report() with topics: {}'.format(flex.topics))

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

        # grab stock splits and post-hoc apply them to price and quantity
        # todo: hack
        if 'CorporateAction' in flex.topics():
            self.corporate_actions = add_and_clean(self.corporate_actions, flex.df('CorporateAction'), 'transactionID')  # type: ignore

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

    def group(self, symbols: List[str], report: Reporting, column: str):
        d = {}
        for symbol in symbols:
            if report.trades.symbol.str.contains(symbol).any():
                d[symbol] = report.trades.groupby(['symbol']).get_group(symbol)[column].rename(symbol)

        return pd.concat(list(d.values()), axis=1)

    def run_analysis(self, symbols: List[str]) -> Dict[str, vbt.Portfolio]:
        logging.debug('run_analysis')
        currencies = (self.reporting.trades.currency.unique())
        portfolios = {}

        def build_portfolio(symbols, report):
            # init_cash = float(self.reporting.cash.head(1).amount)
            init_cash = 100000.0
            cash_deposits = self.reporting.cash.amount
            close = self.group(symbols, report, 'price')
            size = self.group(symbols, report, 'quantity')
            fees = self.group(symbols, report, 'commission') * -1

            portfolio = vbt.Portfolio.from_orders(
                # init_cash=init_cash,
                init_cash='auto',
                close=close,
                size=size,
                fixed_fees=fees,
                direction='both',
                cash_sharing=True,
                group_by=True,
            )
            return portfolio

        for currency in currencies:
            report = self.reporting.query('currency == "{}"'.format(currency))
            portfolio = build_portfolio(symbols, report)

            if len(report.corporate_actions) > 0:
                logging.debug('checking for stock splits')
                for index, row in report.corporate_actions.iterrows():
                    date = row['date']
                    symbol = row['symbol']
                    quantity = row['quantity']
                    action_type = row['type']
                    security_id = row['securityID']
                    if 'FS' in action_type and symbol in portfolio.assets().columns:
                        logging.debug('found stock split in {}, warning hacky code here'.format(symbol))
                        # todo: fix the SettingWithCopyWarning problem here
                        view = report.trades[(report.trades.securityID == security_id) & (report.trades.date <= date)].copy()

                        # grab the current asset level
                        previous = pd.DataFrame(portfolio.assets()[symbol])
                        last_quantity = previous[previous.index <= date].iloc[-1].item()
                        multiplier = quantity / last_quantity

                        view['quantity'] = view['quantity'].multiply(multiplier)
                        view['price'] = view['price'].divide(multiplier)
                        report.trades.update(view)
                        portfolio = build_portfolio(symbols, report)

            portfolios[currency] = portfolio
        return portfolios

    def run_analysis_full(self) -> Dict[str, vbt.Portfolio]:
        symbols = list(self.reporting.trades.symbol.unique())
        return self.run_analysis(symbols)
