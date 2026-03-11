"""Momentum Strategy using Rate of Change (ROC).

Buys when 10-day momentum crosses above a threshold.
Sells when 10-day momentum crosses below a negative threshold.
Uses volume confirmation to filter low-conviction signals.
"""

from trader.trading.strategy import Signal, Strategy
from trader.objects import Action
from typing import Optional

import numpy as np
import pandas as pd


class Momentum(Strategy):
    def __init__(self):
        super().__init__()

    def on_prices(self, prices: pd.DataFrame) -> Optional[Signal]:
        if len(prices) < 15:
            return None

        close = prices['close']
        volume = prices['volume']

        # Rate of change (10-period)
        roc = close.pct_change(periods=10) * 100

        if len(roc.dropna()) < 2:
            return None

        current_roc = roc.iloc[-1]
        prev_roc = roc.iloc[-2]

        if np.isnan(current_roc) or np.isnan(prev_roc):
            return None

        # Volume confirmation: current volume > 20-day average
        avg_volume = volume.rolling(20).mean().iloc[-1]
        current_volume = volume.iloc[-1]
        volume_confirmed = (
            not np.isnan(avg_volume)
            and not np.isnan(current_volume)
            and current_volume > avg_volume * 0.8
        )

        # Buy: ROC crosses above 5% with volume confirmation
        if current_roc > 5.0 and prev_roc <= 5.0 and volume_confirmed:
            return Signal(
                source_name=self.name,
                action=Action.BUY,
                probability=0.55,
                risk=0.45,
            )

        # Sell: ROC crosses below -5%
        if current_roc < -5.0 and prev_roc >= -5.0:
            return Signal(
                source_name=self.name,
                action=Action.SELL,
                probability=0.55,
                risk=0.45,
            )

        return None
