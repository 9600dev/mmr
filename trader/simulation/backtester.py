from dataclasses import dataclass, field
from trader.common.logging_helper import setup_logging
from trader.data.data_access import TickData, TickStorage
from trader.data.event_store import EventType, TradingEvent
from trader.data.market_data import normalize_historical
from trader.data.store import DateRange
from trader.data.universe import UniverseAccessor
from trader.objects import Action, BarSize
from trader.simulation.slippage import FixedBPS, SlippageModel
from trader.trading.risk_gate import RiskGate, RiskLimits
from trader.trading.strategy import Signal, Strategy, StrategyContext, StrategyState
from typing import Dict, List, Optional

import datetime as dt
import importlib
import importlib.util
import inspect
import math
import numpy as np
import os
import pandas as pd
import sys


logging = setup_logging(module_name='backtester')


@dataclass
class BacktestConfig:
    start_date: dt.datetime = field(default_factory=lambda: dt.datetime.now() - dt.timedelta(days=365))
    end_date: dt.datetime = field(default_factory=dt.datetime.now)
    initial_capital: float = 100_000.0
    bar_size: BarSize = BarSize.Mins1
    slippage_bps: float = 1.0
    commission_per_share: float = 0.005
    slippage_model: Optional['SlippageModel'] = None  # overrides slippage_bps when set


@dataclass
class BacktestTrade:
    timestamp: dt.datetime
    conid: int
    action: Action
    quantity: float
    price: float
    commission: float
    signal_probability: float
    signal_risk: float


@dataclass
class BacktestResult:
    signals: List[TradingEvent]
    trades: List[BacktestTrade]
    equity_curve: pd.Series
    total_return: float
    sharpe_ratio: float
    max_drawdown: float
    win_rate: float
    total_trades: int
    start_date: dt.datetime = field(default_factory=dt.datetime.now)
    end_date: dt.datetime = field(default_factory=dt.datetime.now)


class Backtester:
    def __init__(
        self,
        storage: TickStorage,
        config: BacktestConfig,
        risk_limits: Optional[RiskLimits] = None,
    ):
        self.storage = storage
        self.config = config
        self.risk_limits = risk_limits or RiskLimits()

        if self.config.slippage_model is None:
            self.config.slippage_model = FixedBPS(self.config.slippage_bps)

    def _load_strategy_class(self, module_path: str, class_name: str):
        filepath = os.path.abspath(module_path)
        if not os.path.exists(filepath):
            raise FileNotFoundError(f'strategy module not found: {filepath}')

        module_name = os.path.splitext(os.path.basename(filepath))[0]
        spec = importlib.util.spec_from_file_location(module_name, filepath)
        if not spec or not spec.loader:
            raise ImportError(f'cannot load module from {filepath}')

        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)

        class_object = getattr(module, class_name)
        if not (inspect.isclass(class_object) and issubclass(class_object, Strategy)):
            raise TypeError(f'{class_name} is not a subclass of Strategy')
        return class_object

    @staticmethod
    def _bars_per_year(bar_size: BarSize) -> float:
        """Return the approximate number of bars per year for annualizing Sharpe ratio."""
        trading_days = 252
        trading_hours = 6.5  # NYSE regular hours
        trading_minutes = trading_hours * 60  # 390
        trading_seconds = trading_minutes * 60

        mapping = {
            BarSize.Secs1: trading_days * trading_seconds,
            BarSize.Secs5: trading_days * trading_seconds / 5,
            BarSize.Secs10: trading_days * trading_seconds / 10,
            BarSize.Secs15: trading_days * trading_seconds / 15,
            BarSize.Secs30: trading_days * trading_seconds / 30,
            BarSize.Mins1: trading_days * trading_minutes,
            BarSize.Mins2: trading_days * trading_minutes / 2,
            BarSize.Mins3: trading_days * trading_minutes / 3,
            BarSize.Mins5: trading_days * trading_minutes / 5,
            BarSize.Mins10: trading_days * trading_minutes / 10,
            BarSize.Mins15: trading_days * trading_minutes / 15,
            BarSize.Mins20: trading_days * trading_minutes / 20,
            BarSize.Mins30: trading_days * trading_minutes / 30,
            BarSize.Hours1: trading_days * trading_hours,
            BarSize.Hours2: trading_days * trading_hours / 2,
            BarSize.Hours3: trading_days * trading_hours / 3,
            BarSize.Hours4: trading_days * trading_hours / 4,
            BarSize.Hours8: trading_days,  # ~1 bar per day
            BarSize.Days1: trading_days,
            BarSize.Weeks1: 52,
            BarSize.Months1: 12,
        }
        return mapping.get(bar_size, trading_days * trading_minutes)

    def run(
        self,
        strategy: Strategy,
        conids: List[int],
    ) -> BacktestResult:
        logging.info(
            f'starting backtest: {strategy.name}, conids={conids}, '
            f'{self.config.start_date} to {self.config.end_date}'
        )

        signals: List[TradingEvent] = []
        trades: List[BacktestTrade] = []
        equity_values: List[float] = []
        equity_timestamps: List[dt.datetime] = []

        cash = self.config.initial_capital
        positions: Dict[int, float] = {}  # conid -> quantity (positive=long, negative=short)
        position_entry_prices: Dict[int, float] = {}

        date_range = DateRange(start=self.config.start_date, end=self.config.end_date)

        # Load all historical data for each conid
        all_data: Dict[int, pd.DataFrame] = {}
        for conid in conids:
            tickdata = self.storage.get_tickdata(self.config.bar_size)
            try:
                data = tickdata.read(conid, date_range=date_range)
                if data is not None and len(data) > 0:
                    normalized = normalize_historical(data)
                    # Drop rows with NaN close prices (e.g. future dates from API)
                    normalized = normalized.dropna(subset=['close'])
                    if len(normalized) > 0:
                        all_data[conid] = normalized
                    logging.info(f'loaded {len(normalized)} bars for conid {conid}')
                else:
                    logging.warning(f'no data found for conid {conid}')
            except Exception as e:
                logging.warning(f'error loading data for conid {conid}: {e}')

        if not all_data:
            raise ValueError('no historical data available for any conids')

        # Merge all data into a single timeline sorted by timestamp
        combined = pd.concat(
            [df.assign(conid=conid) for conid, df in all_data.items()],
            axis=0
        ).sort_index()

        # Walk forward bar-by-bar, building an expanding window for each conid
        accumulated: Dict[int, pd.DataFrame] = {}
        last_prices: Dict[int, float] = {}

        for timestamp, group in combined.groupby(combined.index):
            for conid in group['conid'].unique():
                bar = group[group['conid'] == conid].drop(columns=['conid'])

                if conid not in accumulated:
                    accumulated[conid] = bar
                else:
                    accumulated[conid] = pd.concat([accumulated[conid], bar], axis=0)

                # Track last price for portfolio valuation
                last_prices[conid] = float(bar['close'].iloc[-1])

            # Call strategy.on_prices() for each conid with accumulated data
            for conid in list(accumulated.keys()):
                signal = strategy.on_prices(accumulated[conid])

                if signal:
                    signal_event = TradingEvent(
                        event_type=EventType.SIGNAL,
                        timestamp=timestamp if isinstance(timestamp, dt.datetime) else dt.datetime.now(),
                        strategy_name=signal.source_name,
                        conid=conid,
                        action=str(signal.action),
                        signal_probability=signal.probability,
                        signal_risk=signal.risk,
                    )
                    signals.append(signal_event)

                    # Simulate execution
                    price = last_prices.get(conid, 0.0)
                    if price <= 0:
                        continue

                    # Determine quantity before slippage (SquareRootImpact needs quantity)
                    quantity = signal.quantity if signal.quantity > 0 else 0
                    if quantity == 0:
                        # Default: use 10% of available capital
                        quantity = math.floor((cash * 0.1) / price) if price > 0 else 0

                    if quantity <= 0:
                        continue

                    # Apply slippage via model
                    bar_row = accumulated[conid].iloc[-1]
                    fill_price = self.config.slippage_model.calculate(price, quantity, signal.action, bar_row)

                    commission = quantity * self.config.commission_per_share

                    if signal.action == Action.BUY:
                        cost = quantity * fill_price + commission
                        if cost > cash:
                            # Not enough capital
                            continue
                        cash -= cost
                        positions[conid] = positions.get(conid, 0) + quantity
                        position_entry_prices[conid] = fill_price
                    elif signal.action == Action.SELL:
                        held = positions.get(conid, 0)
                        if held <= 0:
                            # No position to sell (don't short in backtest)
                            continue
                        sell_qty = min(quantity, held)
                        commission = sell_qty * self.config.commission_per_share
                        proceeds = sell_qty * fill_price - commission
                        cash += proceeds
                        positions[conid] = positions.get(conid, 0) - sell_qty
                        quantity = sell_qty  # Record actual executed quantity
                        if positions[conid] == 0:
                            positions.pop(conid, None)
                            position_entry_prices.pop(conid, None)

                    trades.append(BacktestTrade(
                        timestamp=timestamp if isinstance(timestamp, dt.datetime) else dt.datetime.now(),
                        conid=conid,
                        action=signal.action,
                        quantity=quantity,
                        price=fill_price,
                        commission=commission,
                        signal_probability=signal.probability,
                        signal_risk=signal.risk,
                    ))

            # Track equity (cash + mark-to-market positions)
            portfolio_value = cash
            for conid, qty in positions.items():
                portfolio_value += qty * last_prices.get(conid, 0)

            equity_values.append(portfolio_value)
            equity_timestamps.append(
                timestamp if isinstance(timestamp, dt.datetime) else dt.datetime.now()
            )

        # Build equity curve
        equity_curve = pd.Series(equity_values, index=equity_timestamps)

        # Calculate performance metrics
        total_return = (equity_curve.iloc[-1] / self.config.initial_capital - 1) if len(equity_curve) > 0 else 0.0

        # Sharpe ratio (annualized) — scale factor depends on bar size
        if len(equity_curve) > 1:
            returns = equity_curve.pct_change().dropna()
            if len(returns) > 0 and returns.std() > 0:
                bars_per_year = self._bars_per_year(self.config.bar_size)
                sharpe_ratio = (returns.mean() / returns.std()) * np.sqrt(bars_per_year)
            else:
                sharpe_ratio = 0.0
        else:
            sharpe_ratio = 0.0

        # Max drawdown
        if len(equity_curve) > 0:
            running_max = equity_curve.cummax()
            drawdown = (equity_curve - running_max) / running_max
            max_drawdown = float(drawdown.min())
        else:
            max_drawdown = 0.0

        # Win rate — track weighted average entry price per conid
        avg_entry: Dict[int, float] = {}   # conid -> weighted avg entry price
        avg_qty: Dict[int, float] = {}     # conid -> total held quantity
        winning_trades = 0
        sell_trades = 0
        for trade in trades:
            if trade.action == Action.BUY:
                prev_qty = avg_qty.get(trade.conid, 0.0)
                prev_cost = avg_entry.get(trade.conid, 0.0) * prev_qty
                new_qty = prev_qty + trade.quantity
                if new_qty > 0:
                    avg_entry[trade.conid] = (prev_cost + trade.price * trade.quantity) / new_qty
                    avg_qty[trade.conid] = new_qty
            elif trade.action == Action.SELL:
                sell_trades += 1
                entry_price = avg_entry.get(trade.conid, 0.0)
                if entry_price > 0 and trade.price > entry_price:
                    winning_trades += 1
                # Reduce tracked quantity
                remaining = avg_qty.get(trade.conid, 0.0) - trade.quantity
                if remaining <= 0:
                    avg_entry.pop(trade.conid, None)
                    avg_qty.pop(trade.conid, None)
                else:
                    avg_qty[trade.conid] = remaining

        win_rate = winning_trades / sell_trades if sell_trades > 0 else 0.0

        result = BacktestResult(
            signals=signals,
            trades=trades,
            equity_curve=equity_curve,
            total_return=total_return,
            sharpe_ratio=sharpe_ratio,
            max_drawdown=max_drawdown,
            win_rate=win_rate,
            total_trades=len(trades),
            start_date=self.config.start_date,
            end_date=self.config.end_date,
        )

        logging.info(
            f'backtest complete: {len(trades)} trades, '
            f'return={total_return:.2%}, sharpe={sharpe_ratio:.2f}, '
            f'max_dd={max_drawdown:.2%}, win_rate={win_rate:.2%}'
        )

        return result

    def run_from_module(
        self,
        module_path: str,
        class_name: str,
        conids: List[int],
        universe_accessor: Optional[UniverseAccessor] = None,
    ) -> BacktestResult:
        class_object = self._load_strategy_class(module_path, class_name)
        instance = class_object()

        context = StrategyContext(
            name=class_name,
            bar_size=self.config.bar_size,
            conids=conids,
            universe=None,
            historical_days_prior=0,
            paper=True,
            storage=self.storage,
            universe_accessor=universe_accessor or UniverseAccessor.__new__(UniverseAccessor),
            logger=logging,
            module=module_path,
            class_name=class_name,
        )
        instance.install(context)
        instance.state = StrategyState.RUNNING

        return self.run(instance, conids)
