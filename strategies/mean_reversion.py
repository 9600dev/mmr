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
    """Bollinger-band mean reversion — BUY when price crosses below the lower
    band (oversold), SELL when price crosses above the upper band (overbought).
    Defaults: 20-period window, 2 σ bands."""

    def __init__(self):
        super().__init__()

    def on_prices(self, prices: pd.DataFrame) -> Optional[Signal]:
        window = self.params.get('window', 20)
        num_std = self.params.get('num_std', 2.0)

        if len(prices) < window + 5:
            return None

        close = prices['close']

        sma = close.rolling(window).mean()
        std = close.rolling(window).std()
        upper = sma + num_std * std
        lower = sma - num_std * std

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
