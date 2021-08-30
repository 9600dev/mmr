# interesting links:
# * https://news.ycombinator.com/item?id=19214650
import os
import sys

PACKAGE_PARENT = '../..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

import pandas as pd
import numpy as np
import datetime as dt
import bisect
import scipy.stats as st
import os
import matplotlib.pyplot as plt
import matplotlib.ticker as plttick
import seaborn as sns
import plotly.graph_objects as go
import plotly.express as px

from abc import ABC, abstractmethod
from typing import List, Tuple, Generator, Optional, Callable, Iterable, Generic, Dict
from trader.common.helpers import best_fit_distribution, fit_distribution, window, reformat_large_tick_values, pct_change_adjust
from trader.common.distributions import Distribution, CsvContinuousDistribution, TestDistribution
from collections import deque
from enum import Enum
from dateutil.relativedelta import relativedelta
from trader.portfolio.quantum_harmonic import QuantumHarmonic
from trader.common.logging_helper import setup_logging
from trader.common.helpers import dateify, date_range

logging = setup_logging(module_name='vector_life')

ROOT = '../../'
SANDP_DISTRIBUTION = ROOT + 'data/sandp2000-2019.csv' if 'finance' in os.getcwd() else 'finance/data/sandp2000-2019.csv'
LIFE_EXPECTANCY = ROOT + 'data/life_expectancy.csv' if 'finance' in os.getcwd() else 'finance/data/life_expectancy.csv'
AUSTRALIA_INFLATION = ROOT + 'data/rba_inflation_data.csv' if 'finance' in os.getcwd() else 'finance/data/rba_inflation_data.csv'
TEST_DISTRIBUTION = ROOT + 'data/model.csv' if 'finance' in os.getcwd() else 'finance/data/model.csv'
QUANTUM_HARMONIC = ROOT + 'data/quantumharmonic.csv' if 'finance' in os.getcwd() else 'finance/data/quantumharmonic.csv'

class AssetType(Enum):
    CASH = 1
    STOCK = 2
    BOND = 4
    PROPERTY = 3
    INCOME = 5

class TransactionType(Enum):
    BUY = 1
    SELL = 2
    YIELDED = 3
    DIVIDEND = 4
    TAX = 5

class TaxType(Enum):
    CAPITAL_GAINS = 1
    INCOME = 2

class Tax():
    def __init__(self, amount: float, tax_type: TaxType):
        self.amount = amount
        self.tax_type = tax_type


class AssetGenerator():
    def __init__(self, asset_type: AssetType):
        self.asset_type: AssetType = asset_type

class SalaryGenerator(AssetGenerator):
    def __init__(self, yearly_salary: float):
        super().__init__(AssetType.INCOME)
        self.yearly_salary: float = yearly_salary

    def generate(self,
                 start_date: dt.datetime,
                 freq: str = 'M',
                 periods: int = 480) -> pd.DataFrame:
        index = pd.date_range(start=start_date, periods=periods, freq=freq)
        data = np.full(periods, self.yearly_salary)
        return pd.DataFrame(index=index, data=data)

class AssetTransaction():
    def __init__(self,
                 transaction_cost: float,
                 transaction_type: TransactionType,
                 amount: float,
                 price: float,
                 date_time: dt.datetime,
                 yield_percentage: Optional[float] = None):
        self.transaction_cost: float = transaction_cost
        self.transaction_type: TransactionType = transaction_type
        self.amount: float = amount
        self.price: float = price
        self.date_time: dt.datetime = date_time
        self.yield_percentage: Optional[float] = yield_percentage

class AssetTick():
    def __init__(self):
        self.value: float = 0.0
        self.asset_transaction: Optional[AssetTransaction] = None
        self.date_time: dt.datetime = dt.datetime(dt.MINYEAR, 1, 1)
        self.tick_yield: float = 0.0
        self.price: float = 0.0

    def __str__(self):
        return 'value: {}, tick_yield: {}, asset_transaction: {}'.format(round(self.value, 2),
                                                                         self.tick_yield,
                                                                         self.asset_transaction)

    def __repr__(self):
        return self.__str__()

    def to_dict(self):
        return {'value': self.value,
                'date_time': self.date_time,
                'tick_yield': self.tick_yield,
                'price': self.price}

class AustralianInflation(Distribution):
    def __init__(self):
        self.distribution = CsvContinuousDistribution(name='australian_inflation',
                                                      csv_file=AUSTRALIA_INFLATION,
                                                      data_column='inflation')

    def sample(self) -> float:
        return self.distribution.sample()

class Asset():
    def __init__(self,
                 name: str,
                 initial_value: float,
                 initial_price: float,
                 asset_type: AssetType,
                 asset_init_date: dt.datetime,
                 capital_gain_applicable: bool,
                 yield_generator: Callable[[AssetTick], float],
                 yield_interval_days: int):
        self.ticks: List[AssetTick] = []
        self.transactions: List[AssetTick] = []  # fast path
        self.name: str = name
        self.initial_value = initial_value
        self.initial_price = initial_price
        self.asset_type: AssetType = asset_type
        self.asset_init_date: dt.datetime = dt.datetime(asset_init_date.year, asset_init_date.month, asset_init_date.day)
        self.capital_gain_applicable = capital_gain_applicable
        self.yield_generator: Callable[[AssetTick], float] = yield_generator
        self.yield_interval_days: int = yield_interval_days

    def init(self) -> None:
        # we need to initialize the first tick, because a buy() will try and call transaction_cost()
        # on the subclass, and it won't be cooked yet if we do it in the constructor
        asset_tick = AssetTick()
        asset_tick.asset_transaction = AssetTransaction(transaction_cost=0.0,
                                                        transaction_type=TransactionType.BUY,
                                                        amount=self.initial_value,
                                                        price=self.initial_price,
                                                        date_time=self.asset_init_date)
        asset_tick.price = self.initial_price
        asset_tick.date_time = self.asset_init_date
        asset_tick.value = self.initial_value
        asset_tick.tick_yield = self.yield_generator(asset_tick)
        self.append_tick(asset_tick)

    def append_tick(self, tick: AssetTick) -> None:
        self.ticks.append(tick)
        if tick.asset_transaction is not None:
            self.transactions.append(tick)

    def generate_tick(self, date_time: dt.datetime) -> Optional[AssetTick]:
        if len(self.ticks) == 0:
            self.init()

        last_transaction_tick: AssetTick
        last_tick: AssetTick
        perc_return: float

        # make sure we're generating ticks after the init date
        if date_time < self.asset_init_date:
            return None

        # grab the last tick
        last_tick = self.ticks[-1]

        trans_filter: Callable[[AssetTransaction], bool] = \
            lambda t: t.transaction_type == TransactionType.BUY or t.transaction_type == TransactionType.YIELDED

        last_transaction_tick = self.get_last_tick_transaction(trans_filter)

        asset_tick = AssetTick()
        asset_tick.date_time = date_time

        # check to see if we need to generate some yield
        if (date_time - last_transaction_tick.date_time).days >= self.yield_interval_days:
            # aggregate ticks in between and apply the rate
            total_yield = self.sum_yield(last_transaction_tick, last_tick)
            # apply that yield to this tick
            yielded_amount = last_tick.value * total_yield
            asset_tick.value = last_tick.value + yielded_amount
            transaction = AssetTransaction(transaction_cost=self.transaction_cost(TransactionType.YIELDED,
                                                                                  yielded_amount,
                                                                                  date_time),
                                           transaction_type=TransactionType.YIELDED,
                                           amount=yielded_amount,
                                           price=self.generate_price(last_tick, date_time),
                                           date_time=date_time,
                                           yield_percentage=total_yield)

            asset_tick.asset_transaction = transaction
        else:
            asset_tick.value = last_tick.value

        asset_tick.tick_yield = self.yield_generator(last_tick)
        self.append_tick(asset_tick)

        return asset_tick

    def __start_fin_year(self, end_fin_year: dt.datetime) -> dt.datetime:
        start_fin_year = end_fin_year - relativedelta(years=1)
        return start_fin_year

    def __filter_fin_year(self, collection: List, end_fin_year: dt.datetime) -> List:
        ret = []
        start_fin_year = self.__start_fin_year(end_fin_year)
        for i in collection:
            if i.date_time >= start_fin_year and i.date_time < end_fin_year:
                ret.append(i)
        return ret

    @abstractmethod
    def transaction_cost(self, transaction_type: TransactionType, amount: float, date_time: dt.datetime):
        pass

    def generate_price(self, last_tick: AssetTick, date_time: dt.datetime):
        pass

    @abstractmethod
    def taxable_income(self, end_fin_year: dt.datetime) -> List[Tax]:
        pass

    def sum_yield(self, start: AssetTick, end: AssetTick) -> float:
        if start == end:
            return start.tick_yield

        start_index = self.ticks.index(start)
        end_index = self.ticks.index(end)

        total_yield = 0.0
        for i in range(start_index, end_index):
            total_yield += self.ticks[i].tick_yield

        return total_yield

    def sum_yield_from_ticks(self, ticks: List[AssetTick]) -> float:
        total_yield = 0.0
        for t in ticks:
            total_yield += t.tick_yield

        return total_yield

    def get_financial_year_ticks(self, end_fin_year: dt.datetime) -> List[AssetTick]:
        start_fin_year = self.__start_fin_year(end_fin_year)

        # let's start from the end, because we're usually calling this during a simulation
        return_ticks = []
        # ticks = [d for d in self.ticks if d.date_time >= start_fin_year and d.date_time < end_fin_year]
        for t in reversed(self.ticks):
            if t.date_time >= start_fin_year and t.date_time < end_fin_year:
                return_ticks.append(t)
            if t.date_time < start_fin_year:
                break
        return list(reversed(return_ticks))

    def get_last_tick_transaction(self,
                                  filter_: Callable[[AssetTransaction], bool] = lambda t: True) -> AssetTick:
        for t in reversed(self.transactions):
            if t.asset_transaction is not None:
                if filter_(t.asset_transaction):
                    return t
        raise ValueError('filter produced no transaction ticks')

    def get_ticks_with_transactions(self,
                                    filter_: Optional[Callable[[AssetTransaction], bool]],
                                    end_fin_year: Optional[dt.datetime] = None) -> List[AssetTick]:
        return_ticks = []
        tick_collection = self.transactions
        if end_fin_year:
            tick_collection = self.__filter_fin_year(tick_collection, end_fin_year)
        for t in tick_collection:
            if t.asset_transaction is not None:
                if filter_ is not None and filter_(t.asset_transaction):
                    return_ticks.append(t)
                elif filter is None:
                    return_ticks.append(t)
        return return_ticks

    def get_value(self) -> float:
        return self.ticks[-1].value

    def perform_buysell(self,
                        amount: float,
                        trans_type: TransactionType,
                        date_time: dt.datetime) -> None:
        if trans_type != TransactionType.SELL and trans_type != TransactionType.BUY:
            raise ValueError('must be a buy or sell transaction')

        if trans_type == TransactionType.SELL and amount > self.get_value():
            raise ValueError('SELL amount is greater than asset value')

        if trans_type == TransactionType.SELL and amount > 0.0:
            amount = amount * -1.0

        last_tick = self.ticks[-1]

        asset_tick = AssetTick()
        asset_tick.asset_transaction = AssetTransaction(transaction_cost=self.transaction_cost(trans_type,
                                                                                               amount,
                                                                                               date_time),
                                                        transaction_type=trans_type,
                                                        amount=amount,
                                                        price=last_tick.price,
                                                        date_time=date_time)
        asset_tick.price = last_tick.price
        asset_tick.date_time = date_time
        asset_tick.value = last_tick.value + amount

        self.append_tick(asset_tick)

    def sell(self, amount: float, date_time: dt.datetime) -> None:
        return self.perform_buysell(amount, TransactionType.SELL, date_time)

    def buy(self, amount: float, date_time: dt.datetime, price: Optional[float]) -> None:
        return self.perform_buysell(amount, TransactionType.BUY, date_time)

    def __str__(self):
        return '{}, len ticks: {}, asset_type: {}'.format(self.name, len(self.ticks), self.asset_type.name)

    def __repr__(self):
        return self.__str__() + '\n' + ',\n'.join([str(a) for a in self.ticks])

class AssetStock(Asset):
    def __init__(self,
                 name: str,
                 initial_value: float,
                 initial_price: float,
                 asset_init_date: dt.datetime):

        super().__init__(name=name,
                         initial_value=initial_value,
                         initial_price=initial_price,
                         asset_type=AssetType.STOCK,
                         asset_init_date=asset_init_date,
                         capital_gain_applicable=True,
                         yield_generator=self.sample_yield,
                         yield_interval_days=1)

        # self.distribution = CsvContinuousDistribution(name=name,
        #                                               csv_file=SANDP_DISTRIBUTION,
        #                                               data_column='adj_close',
        #                                               cache_size=365 * 10,
        #                                               data_column_apply=pct_change_adjust,
        #                                               distribution=st.loglaplace)
        parameters = [0.2, 0.2, 0.086, 0.182, 0.133, 0.928]
        self.distribution = QuantumHarmonic(name=name, csv_file=QUANTUM_HARMONIC, parameters=parameters)

    def sample_yield(self, last_tick: AssetTick) -> float:
        return self.distribution.sample()

    def transaction_cost(self, transaction_type: TransactionType, amount: float, date_time: dt.datetime) -> float:
        return 10.0

    def taxable_income(self, end_fin_year: dt.datetime) -> List[Tax]:
        start_fin_year = end_fin_year - relativedelta(years=1)

        sell_filter: Callable[[AssetTransaction], bool] = \
            lambda t: t.transaction_type == TransactionType.SELL

        buy_filter: Callable[[AssetTransaction], bool] = \
            lambda t: t.transaction_type == TransactionType.BUY

        fin_year_sell_ticks = [t.asset_transaction for t in self.get_ticks_with_transactions(sell_filter, end_fin_year)
                               if t.asset_transaction is not None]
        buys: List[Tuple[dt.datetime, float]]
        buys = [(t.date_time, t.asset_transaction.amount) for t in reversed(self.get_ticks_with_transactions(buy_filter))
                if t.asset_transaction is not None]

        # TODO:// figure out dividend income

        # [capital_gain, income]
        tax = [0.0, 0.0]
        tax_switch = 0
        # for each of the sell's, we need to figure out how long ago they were bought, to calculate
        # capital gain vs. income
        for sell in fin_year_sell_ticks:
            sell_remaining = sell.amount
            for i in range(0, len(buys)):
                if buys[i][0] < (sell.date_time - relativedelta(years=1)):
                    tax_switch = 0
                else:
                    tax_switch = 1

                if buys[i][1] >= sell_remaining:
                    tax[tax_switch] += sell_remaining
                    buys[i] = (buys[i][0], (buys[i][1] - sell_remaining))
                    sell_remaining = 0
                else:
                    tax[tax_switch] += buys[i][1]
                    sell_remaining -= buys[i][1]
                    buys[i] = (buys[i][0], 0)
            # sell_remaining should be zero
            if sell_remaining != 0.0:
                raise ValueError('this should not happen')

        return [Tax(amount=tax[0], tax_type=TaxType.CAPITAL_GAINS),
                Tax(amount=tax[1], tax_type=TaxType.INCOME)]

class AssetCash(Asset):
    def __init__(self,
                 name: str,
                 initial_value: float,
                 asset_init_date: dt.datetime):
        super().__init__(name=name,
                         initial_value=initial_value,
                         initial_price=1.0,
                         asset_type=AssetType.CASH,
                         asset_init_date=asset_init_date,
                         capital_gain_applicable=False,
                         yield_generator=self.sample_yield,
                         yield_interval_days=365)

    def sample_yield(self, last_tick: AssetTick) -> float:
        return 0.028 / self.yield_interval_days

    def transaction_cost(self, transaction_type: TransactionType, amount: float, date_time: dt.datetime) -> float:
        # regardless of BUY, SELL, YIELD, it's all the same
        return 0.0

    def generate_price(self, last_tick: AssetTick, date_time: dt.datetime) -> float:
        return 1.0

    def taxable_income(self, end_fin_year: dt.datetime) -> List[Tax]:
        income: float = 0.0
        trans_filter: Callable[[AssetTransaction], bool] = \
            lambda t: t.transaction_type == TransactionType.YIELDED

        fin_year_ticks = self.get_ticks_with_transactions(filter_=trans_filter,
                                                          end_fin_year=end_fin_year)

        income += sum([t.asset_transaction.amount for t in fin_year_ticks if t.asset_transaction])
        return [Tax(amount=income, tax_type=TaxType.INCOME)]

class TaxReturn():
    def __init__(self):
        self.date: dt.datetime
        self.tax_paid: float
        self.carried_losses: float

class Book():
    def __init__(self,
                 start_date: dt.datetime):
        self.assets: List[Asset] = []
        self.tax_returns: List[TaxReturn] = []
        self.start_date: dt.datetime = start_date

    def calculate_net_worth(self):
        total = 0.0
        for a in self.assets:
            total += a.ticks[-1].value
        return total

    def to_dataframe(self) -> Dict[str, pd.DataFrame]:
        assets = {}
        for a in self.assets:
            tick_dicts = [t.to_dict() for t in a.ticks]
            assets[a.name] = pd.DataFrame(tick_dicts)
            if 'date_time' in tick_dicts[0]:
                assets[a.name]['date_time'] = pd.to_datetime(assets[a.name]['date_time'])
                assets[a.name].set_index('date_time', inplace=True)
        return assets

    def to_dataframe2(self) -> pd.DataFrame:
        tick_dicts = []
        for a in self.assets:
            for t in a.ticks:
                d = t.to_dict()
                d['name'] = a.name
                tick_dicts.append(d)

        df = pd.DataFrame(tick_dicts)
        return df


# https://sixfigureinvesting.com/2016/03/modeling-stock-market-returns-with-laplace-distribution-instead-of-normal/
class LifeSimulator():
    def __init__(self,
                 date_of_birth: dt.datetime,
                 capital: float
                 ):

        self.dob = date_of_birth
        self.days_old = (dt.datetime.now() - date_of_birth).days
        self.capital = capital
        self.d_life_expectancy = pd.read_csv(LIFE_EXPECTANCY)
        self.d_sp_returns: pd.DataFrame
        self.sp_x: List[float] = []
        self.sp_pdf: List[float] = []

    def sample_death_day(self):
        dpy_hist = self.d_life_expectancy.number_of_lives / self.d_life_expectancy.number_of_lives.sum()
        life_days_remaining: int = 0
        try_parse = True
        while life_days_remaining < self.days_old or try_parse:
            try:
                year_of_death = self.dob.year + int(np.random.choice(self.d_life_expectancy.year, p=dpy_hist))  # type: ignore
                month_of_death = np.random.randint(1, 13)  # type: ignore
                day_of_death = np.random.randint(1, 32)  # type: ignore
                life_days_remaining = (dt.datetime(year_of_death, month_of_death, day_of_death) - dt.datetime.now()).days
                try_parse = False
            except Exception:
                try_parse = True
        return life_days_remaining

    def to_plot(self, gen: Generator) -> Tuple[List[int], List[float]]:
        g = list(gen)
        days: int = len(g)
        x = []
        y = []

        counter = 1
        for i in range(days):
            x.append(counter)
            y.append(g[i])
            counter = counter + 1
        return (x, y)

    def tax(self,
            yearly_gain: float,
            tax_brackets: List[float],
            tax_rates: List[float],
            base_tax: List[float]) -> float:
        i = bisect.bisect(tax_brackets, yearly_gain)
        if not i:
            return 0
        rate = tax_rates[i]
        bracket = tax_brackets[i - 1]
        income_in_bracket = yearly_gain - bracket
        tax_in_bracket = income_in_bracket * rate
        total_tax = base_tax[i - 1] + tax_in_bracket
        return total_tax

    def apply_tax(self, book: Book, end_fin_year: dt.datetime) -> float:
        tax_brackets: List[float] = [
            18201.00,
            37001.00,
            90001.00,
            180001.00
        ]

        tax_rates: List[float] = [
            0.0,
            0.19,
            0.325,
            0.37,
            0.45
        ]

        base_tax: List[float] = [
            0.0,        # 18201.00 * 0.0
            3572.0,
            17547.0,
            54547.0
        ]

        # do something simple, like saying "everything is income" for now
        income = 0.0
        capital_gain = 0.0
        # for asset in book.assets:
        #     # sum all the yield transactions
        #     trans_filter: Callable[[AssetTransaction], bool] = \
        #         lambda t: t.transaction_type == TransactionType.YIELDED

        #     fin_year_ticks = asset.get_ticks_with_transactions(filter_=trans_filter,
        #                                                        end_fin_year=end_fin_year)

        #     income += sum([t.asset_transaction.amount for t in fin_year_ticks if t.asset_transaction])

        for asset in book.assets:
            tax_list = asset.taxable_income(end_fin_year)
            for t in filter(lambda x: x.tax_type == TaxType.CAPITAL_GAINS, tax_list):
                capital_gain += t.amount
            for t in filter(lambda x: x.tax_type == TaxType.INCOME, tax_list):
                income += t.amount

        income_tax = self.tax(income, tax_brackets, tax_rates, base_tax)
        capital_gains_tax = capital_gain * 0.45

        # apply the deduction
        income_asset = [a for a in book.assets if a.asset_type == AssetType.CASH][0]
        # capital_asset = [a for a in book.assets if a.asset_type == AssetType.STOCK][0]
        income_asset.ticks[-1].value -= income_tax
        # capital_asset.ticks[tick_index].value -= capital_gains_tax

        return income_tax + capital_gains_tax

    def run_simulation(self,
                       book: Book,
                       apply_tax: bool,
                       apply_inflation: bool,
                       simulation_start_date: dt.datetime = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
                       simulation_end_date: Optional[dt.datetime] = None) -> Book:
        days_remaining: int = 0

        if simulation_end_date:
            days_remaining = (simulation_end_date - simulation_start_date).days
        else:
            days_remaining = self.sample_death_day()

        day_counter = 0
        current_date = simulation_start_date + dt.timedelta(days=1)

        for i in range(days_remaining):
            day_counter += 1

            for asset in book.assets:
                asset.generate_tick(current_date)

            # if it's july first, it's tax time!
            if apply_tax and current_date.month == 7 and current_date.day == 1:
                logger.info('applying tax')
                tax = self.apply_tax(book, current_date)
                day_counter = 0

            # if it's july first, apply inflation discount!
            # if current_date.month == 7 and current_date.day == 1:
            current_date = current_date + dt.timedelta(days=1)

        return book

def init() -> Book:
    logging.info('Starting init()')
    simulator = LifeSimulator(dt.datetime(1981, 4, 13), 1000000.0)
    start_date = dt.datetime(2020, 1, 1)
    end_date = dt.datetime(2040, 1, 1)

    book = Book(start_date)
    book.assets.append(
        AssetCash(name='term_deposit',
                  initial_value=1000000.0,
                  asset_init_date=start_date))

    book.assets.append(
        AssetStock(name='sandp',
                   initial_value=1000000.0,
                   initial_price=100.0,
                   asset_init_date=start_date))
    for i in range(0, 20):
        book.assets.append(
            AssetStock(name='sandp' + str(i),
                       initial_value=1000000.0,
                       initial_price=100.0,
                       asset_init_date=start_date))
    logging.info('Starting simulation')
    simulator.run_simulation(book,
                             False,
                             False,
                             simulation_start_date=start_date,
                             simulation_end_date=end_date)
    logging.info('finished init()')
    return book

