"""RSI (Relative Strength Index) Strategy.

Buys when RSI drops below 30 (oversold) and starts recovering.
Sells when RSI rises above 70 (overbought) and starts declining.
Uses a 14-period RSI.
"""

from trader.trading.strategy import Signal, Strategy
from trader.objects import Action
from typing import Optional

import numpy as np
import pandas as pd


def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """Compute RSI from a price series."""
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)

    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


class RSIStrategy(Strategy):
    def __init__(self):
        super().__init__()

    def on_prices(self, prices: pd.DataFrame) -> Optional[Signal]:
        if len(prices) < 20:
            return None

        rsi = compute_rsi(prices['close'], period=14)

        if len(rsi.dropna()) < 2:
            return None

        current_rsi = rsi.iloc[-1]
        prev_rsi = rsi.iloc[-2]

        if np.isnan(current_rsi) or np.isnan(prev_rsi):
            return None

        # Buy: RSI crosses above 30 from below (recovering from oversold)
        if current_rsi > 30 and prev_rsi <= 30:
            return Signal(
                source_name=self.name,
                action=Action.BUY,
                probability=0.6,
                risk=0.4,
            )

        # Sell: RSI crosses below 70 from above (declining from overbought)
        if current_rsi < 70 and prev_rsi >= 70:
            return Signal(
                source_name=self.name,
                action=Action.SELL,
                probability=0.6,
                risk=0.4,
            )

        return None
