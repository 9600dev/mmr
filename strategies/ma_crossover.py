from trader.trading.strategy import Signal, Strategy
from trader.objects import Action
from typing import Optional

import pandas as pd


class MaCrossover(Strategy):
    """Strategy: ma_crossover

    Receives accumulated OHLCV DataFrames via on_prices().
    Return a Signal to generate a trade, or None to do nothing.

    DataFrame columns: date (index), open, high, low, close, volume, average, barCount
    """

    def __init__(self):
        super().__init__()

    def on_prices(self, prices: pd.DataFrame) -> Optional[Signal]:
        if len(prices) < 50:
            return None

        close = prices['close']
        fast = close.rolling(10).mean()
        slow = close.rolling(50).mean()

        if fast.iloc[-1] > slow.iloc[-1] and fast.iloc[-2] <= slow.iloc[-2]:
            return Signal(source_name=self.name, action=Action.BUY, probability=0.7, risk=0.3)
        if fast.iloc[-1] < slow.iloc[-1] and fast.iloc[-2] >= slow.iloc[-2]:
            return Signal(source_name=self.name, action=Action.SELL, probability=0.7, risk=0.3)

        return None
