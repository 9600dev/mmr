from trader.data.store import DateRange
from ib_async import Contract
from trader.trading.strategy import Signal, Strategy, StrategyContext, StrategyState
from trader.objects import Action, BarSize
from typing import Any, Dict, Optional, Tuple

import datetime as dt
import exchange_calendars
import pandas as pd
import trader.strategy.strategy_runtime as runtime
import vectorbt as vbt


class SMICrossOver(Strategy):
    """10/50 moving-average crossover on close prices, powered by vectorbt —
    BUY on fast-MA crossing above slow-MA, SELL on crossing below. Includes
    historical-data readiness gating via the strategy runtime.

    Uses the precompute/on_bar hook so the vectorbt MA is computed once on
    the full series (O(N)) instead of being re-run on every bar (O(N²))."""

    FAST_WINDOW = 10
    SLOW_WINDOW = 50

    def __init__(self):
        super().__init__()
        self.strategy_runtime: Optional[runtime.StrategyRuntime] = None

    def install(self, context: StrategyContext) -> bool:
        super().install(context)
        return True

    def enable(self) -> StrategyState:
        if not self.strategy_runtime:
            raise ValueError('strategy_runtime not set')

        if not self.conids:
            raise ValueError('conids not set')

        date_range: Optional[DateRange] = None
        if self.historical_days_prior:
            date_range = DateRange(start=dt.datetime.now() - dt.timedelta(days=self.historical_days_prior), end=dt.datetime.now())

        # check to see if we have all the available data we need
        for conid in self.conids:
            missing_data = self.ctx.storage.get_tickdata(self.bar_size).missing(
                conid,
                exchange_calendar=exchange_calendars.get_calendar('NASDAQ'),
                pd_offset=None,
                date_range=date_range
            )

            if missing_data:
                self.state = StrategyState.WAITING_HISTORICAL_DATA
                return self.state

        # start subscriptions
        for conid in self.conids:
            self.strategy_runtime.subscribe(self, Contract(conId=conid))

        self.state = StrategyState.RUNNING
        return self.state

    # ----- backtest fast path -------------------------------------------

    def precompute(self, prices: pd.DataFrame) -> Dict[str, Any]:
        """Compute the full MA series once on the entire history. In the
        legacy ``on_prices`` path vectorbt would rebuild Portfolio/Indicator
        objects on every bar; here we pay that cost exactly once."""
        if len(prices) <= self.SLOW_WINDOW:
            return {}
        close = prices['close']
        fast = vbt.MA.run(close, self.FAST_WINDOW)
        slow = vbt.MA.run(close, self.SLOW_WINDOW)
        # Materialize the cross signals to a plain numpy bool array so
        # per-bar access is O(1) and doesn't re-enter vectorbt's wrapper.
        return {
            'entries': fast.ma_crossed_above(slow).to_numpy().astype(bool).ravel(),  # type: ignore
            'exits':   fast.ma_crossed_below(slow).to_numpy().astype(bool).ravel(),  # type: ignore
        }

    def on_bar(self, prices: pd.DataFrame, state: Dict[str, Any], index: int) -> Optional[Signal]:
        entries = state.get('entries')
        exits = state.get('exits')
        if entries is None or exits is None or index < self.SLOW_WINDOW:
            return None
        if entries[index]:
            return Signal('smi_crossover', Action.BUY, 0.0, 0.0)
        if exits[index]:
            return Signal('smi_crossover', Action.SELL, 0.0, 0.0)
        return None

    # ----- live-trading path (unchanged) --------------------------------

    def on_prices(self, prices: pd.DataFrame) -> Optional[Signal]:
        # Only relevant to the live runtime; kept for backwards compatibility.
        # The live runtime is slower but also called once per tick, not per
        # bar-in-history, so the O(N) vs O(N²) gap doesn't matter there.
        if self.state != StrategyState.RUNNING:
            return None
        close = prices['close']
        if len(close) <= self.SLOW_WINDOW:
            return None
        fast_ma = vbt.MA.run(close, self.FAST_WINDOW)
        slow_ma = vbt.MA.run(close, self.SLOW_WINDOW)
        entries = fast_ma.ma_crossed_above(slow_ma)  # type: ignore
        exits = fast_ma.ma_crossed_below(slow_ma)  # type: ignore
        if bool(entries.iloc[-1]):
            return Signal('smi_crossover', Action.BUY, 0.0, 0.0)
        if bool(exits.iloc[-1]):
            return Signal('smi_crossover', Action.SELL, 0.0, 0.0)
        return None

    def on_error(self, error):
        self.state = StrategyState.ERROR
        return super().on_error(error)
