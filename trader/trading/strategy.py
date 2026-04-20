from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import IntEnum
from logging import Logger
from trader.data.data_access import TickStorage
from trader.data.universe import UniverseAccessor
from trader.objects import Action, BarSize
from typing import Any, Dict, List, Optional

import datetime as dt
import pandas as pd


@dataclass
class Signal():
    source_name: str
    action: Action
    probability: float
    risk: float
    conid: int = 0
    quantity: float = 0.0
    date_time: dt.datetime = field(default_factory=dt.datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    # Time-based exit conditions — only honored in the backtester today
    # (live-runtime treats them as None). A BUY signal with either field
    # set causes the backtester to synthesize a SELL once the condition
    # triggers: N bars elapsed since entry, or the current bar's time-
    # of-day reaches ``close_by_time``. Both are optional; passing both
    # means "whichever triggers first wins." Enables day-trading
    # strategies (open-range breakout, VWAP reversion) to express their
    # "flatten by 15:45 ET" / "bail if stagnant after 20 minutes" rules
    # without every strategy re-implementing the same time-check logic.
    max_hold_bars: Optional[int] = None
    close_by_time: Optional[dt.time] = None


class StrategyState(IntEnum):
    NOT_INSTALLED = 0
    INSTALLED = 1
    WAITING_HISTORICAL_DATA = 2
    RUNNING = 3
    DISABLED = 4
    ERROR = 5


@dataclass
class StrategyContext:
    name: str
    bar_size: BarSize
    conids: List[int]
    universe: Optional[str]
    historical_days_prior: int
    paper: bool
    storage: TickStorage
    universe_accessor: UniverseAccessor
    logger: Logger
    module: Optional[str] = None
    class_name: Optional[str] = None
    runs_when_crontab: Optional[str] = None
    description: Optional[str] = None
    auto_execute: bool = False
    params: Dict[str, Any] = field(default_factory=dict)


class Strategy(ABC):
    def __init__(self):
        self._context: Optional[StrategyContext] = None
        self.state: StrategyState = StrategyState.NOT_INSTALLED

    @property
    def ctx(self) -> StrategyContext:
        if not self._context:
            raise RuntimeError("Strategy not installed")
        return self._context

    # Backward-compatible property accessors that delegate to context
    @property
    def name(self) -> Optional[str]:
        return self._context.name if self._context else None

    @name.setter
    def name(self, value):
        if self._context:
            self._context.name = value

    @property
    def bar_size(self) -> BarSize:
        return self._context.bar_size if self._context else BarSize.Mins1

    @bar_size.setter
    def bar_size(self, value):
        if self._context:
            self._context.bar_size = value

    @property
    def conids(self) -> Optional[List[int]]:
        return self._context.conids if self._context else None

    @conids.setter
    def conids(self, value):
        if self._context:
            self._context.conids = value

    @property
    def universe(self) -> Optional[str]:
        return self._context.universe if self._context else None

    @property
    def module(self) -> Optional[str]:
        return self._context.module if self._context else None

    @module.setter
    def module(self, value):
        if self._context:
            self._context.module = value

    @property
    def class_name(self) -> Optional[str]:
        return self._context.class_name if self._context else None

    @class_name.setter
    def class_name(self, value):
        if self._context:
            self._context.class_name = value

    @property
    def historical_days_prior(self) -> Optional[int]:
        return self._context.historical_days_prior if self._context else None

    @property
    def runs_when_crontab(self) -> Optional[str]:
        return self._context.runs_when_crontab if self._context else None

    @property
    def description(self) -> Optional[str]:
        return self._context.description if self._context else None

    @property
    def paper(self) -> bool:
        return self._context.paper if self._context else True

    @paper.setter
    def paper(self, value):
        if self._context:
            self._context.paper = value

    @property
    def params(self) -> Dict[str, Any]:
        return self._context.params if self._context else {}

    @property
    def storage(self) -> Optional[TickStorage]:
        return self._context.storage if self._context else None

    @property
    def logging(self) -> Optional[Logger]:
        return self._context.logger if self._context else None

    def install(self, context: StrategyContext) -> bool:
        self._context = context
        self.state = StrategyState.INSTALLED
        return True

    def enable(self) -> StrategyState:
        self.state = StrategyState.RUNNING
        return self.state

    def disable(self) -> StrategyState:
        self.state = StrategyState.DISABLED
        return self.state

    def on_prices(self, prices: pd.DataFrame) -> Optional[Signal]:
        """Per-bar signal generation, legacy API.

        Receives the accumulated OHLCV slice up through the current bar.
        Easy to write, but recomputes indicators every bar — O(N²) total
        over a backtest. Fine for pandas ``.rolling()``; too slow for
        vectorbt-backed indicators. For vectorbt / numba strategies, override
        ``precompute()`` + ``on_bar()`` instead.

        Subclasses must implement at least one of ``on_prices`` (legacy) or
        ``on_bar`` (fast path). The default here returns None so strategies
        that only implement ``on_bar`` don't get AttributeError when the
        backtester falls back.
        """
        return None

    # ------------------------------------------------------------------
    # Precompute hook — opt-in O(N) execution path for the backtester.
    # ------------------------------------------------------------------
    #
    # Strategies that use vectorbt / numba indicators should override
    # ``precompute`` and ``on_bar`` instead of ``on_prices``:
    #
    #     def precompute(self, prices):
    #         macd = vbt.MACD.run(prices['close'])
    #         return {'hist': macd.hist}
    #
    #     def on_bar(self, prices, state, index):
    #         if index < 26 or pd.isna(state['hist'].iloc[index]): return None
    #         h, h_prev = state['hist'].iloc[index], state['hist'].iloc[index-1]
    #         if h > 0 and h_prev <= 0:
    #             return Signal(source_name=self.name, action=Action.BUY, probability=0.6, risk=0.4)
    #         return None
    #
    # The backtester calls ``precompute(full_prices)`` ONCE per conid before
    # iterating, so indicator work is amortized over N bars instead of
    # repeated on each of N bars. For live trading the runtime still uses
    # ``on_prices`` — ``on_bar`` is backtest-only today, but the API is
    # symmetric enough that a future live dispatcher can adopt it too.

    def precompute(self, prices: pd.DataFrame) -> Dict[str, Any]:
        """Optional: called once with the FULL OHLCV history before the
        backtester begins iterating bars. Return a dict of precomputed
        indicator arrays (or any state) that ``on_bar`` can consume.

        **Lookahead contract**: every value you store must be aligned 1:1
        with ``prices`` such that index ``i`` depends only on bars
        ``[0..i]`` (inclusive). Rolling/EWM/vectorbt indicators satisfy
        this by construction; ``shift(-1)`` or centered rollings do not.

        Default: returns ``{}``. Strategies that don't need precompute
        can ignore this hook.
        """
        return {}

    def on_bar(
        self,
        prices: pd.DataFrame,
        state: Dict[str, Any],
        index: int,
    ) -> Optional[Signal]:
        """Per-bar hook, fast path.

        ``prices`` is the FULL OHLCV DataFrame (the same object for every
        call — the backtester passes it through by reference, no slicing).
        ``state`` is whatever ``precompute`` returned. ``index`` is the
        0-based position of the current bar.

        Only read ``prices.iloc[:index+1]`` and ``state[key][:index+1]`` —
        reading past ``index`` is lookahead bias.

        Default implementation falls back to ``on_prices(prices.iloc[:index+1])``
        so strategies written against the legacy API keep working.
        """
        return self.on_prices(prices.iloc[:index + 1])

    def on_error(self, error: Exception) -> None:
        self.state = StrategyState.ERROR


class StrategyConfig():
    def __init__(
        self,
        name: str,
        state: StrategyState,
        bar_size: Optional[BarSize] = None,
        conids: Optional[List[int]] = None,
        universe: Optional[str] = None,
        module: Optional[str] = None,
        class_name: Optional[str] = None,
        historical_days_prior: Optional[int] = None,
        runs_when_crontab: Optional[str] = None,
        description: Optional[str] = None,
        paper: bool = True,
        auto_execute: bool = False,
        params: Optional[Dict[str, Any]] = None,
    ):
        self.name = name
        self.bar_size = bar_size
        self.conids = conids
        self.universe = universe
        self.module = module
        self.class_name = class_name
        self.historical_days_prior = historical_days_prior
        self.runs_when_crontab = runs_when_crontab
        self.description = description
        self.state = state
        self.paper = paper
        self.auto_execute = auto_execute
        self.params = params or {}

    @staticmethod
    def from_strategy(strategy: Strategy) -> 'StrategyConfig':
        return StrategyConfig(
            name=strategy.name if strategy.name is not None else 'not_set',
            bar_size=strategy.bar_size,
            conids=strategy.conids,
            universe=strategy.universe,
            module=strategy.module,
            class_name=strategy.class_name,
            historical_days_prior=strategy.historical_days_prior,
            runs_when_crontab=strategy.runs_when_crontab,
            description=strategy.description,
            state=strategy.state,
            paper=strategy.paper,
            auto_execute=strategy._context.auto_execute if strategy._context else False,
            params=strategy.params,
        )
