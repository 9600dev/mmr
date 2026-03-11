"""Mean Reversion Strategy using Bollinger Bands.

Buys when price drops below the lower Bollinger Band (oversold).
Sells when price rises above the upper Bollinger Band (overbought).
Uses a 20-period lookback with 2 standard deviations.
"""

from trader.trading.strategy import Signal, Strategy
from trader.objects import Action
from typing import Optional

import pandas as pd


class MeanReversion(Strategy):
    def __init__(self):
        super().__init__()

    def on_prices(self, prices: pd.DataFrame) -> Optional[Signal]:
        if len(prices) < 25:
            return None

        close = prices['close']
        window = 20

        sma = close.rolling(window).mean()
        std = close.rolling(window).std()
        upper = sma + 2 * std
        lower = sma - 2 * std

        last_close = close.iloc[-1]
        prev_close = close.iloc[-2]

        # Buy when price crosses below lower band
        if last_close < lower.iloc[-1] and prev_close >= lower.iloc[-2]:
            return Signal(
                source_name=self.name,
                action=Action.BUY,
                probability=0.65,
                risk=0.35,
            )

        # Sell when price crosses above upper band
        if last_close > upper.iloc[-1] and prev_close <= upper.iloc[-2]:
            return Signal(
                source_name=self.name,
                action=Action.SELL,
                probability=0.65,
                risk=0.35,
            )

        return None
