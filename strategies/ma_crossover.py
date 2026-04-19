from trader.trading.strategy import Signal, Strategy
from trader.objects import Action
from typing import Optional

import pandas as pd


class MaCrossover(Strategy):
    """Simple moving-average crossover — BUY when the fast MA crosses above
    the slow MA, SELL on the reverse. Configurable via ``fast_period``
    (default 10) and ``slow_period`` (default 50) params."""

    def __init__(self):
        super().__init__()

    def on_prices(self, prices: pd.DataFrame) -> Optional[Signal]:
        fast_period = self.params.get('fast_period', 10)
        slow_period = self.params.get('slow_period', 50)

        if len(prices) < slow_period:
            return None

        close = prices['close']
        fast = close.rolling(fast_period).mean()
        slow = close.rolling(slow_period).mean()

        if fast.iloc[-1] > slow.iloc[-1] and fast.iloc[-2] <= slow.iloc[-2]:
            return Signal(source_name=self.name, action=Action.BUY, probability=0.7, risk=0.3)
        if fast.iloc[-1] < slow.iloc[-1] and fast.iloc[-2] >= slow.iloc[-2]:
            return Signal(source_name=self.name, action=Action.SELL, probability=0.7, risk=0.3)

        return None
