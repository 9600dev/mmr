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
    """Rate-of-change momentum with volume confirmation — BUY when 10-period
    ROC crosses above +threshold on above-average volume, SELL when it
    crosses below −threshold. Defaults: 10-period ROC, 5 % threshold."""

    def __init__(self):
        super().__init__()

    def on_prices(self, prices: pd.DataFrame) -> Optional[Signal]:
        roc_period = self.params.get('roc_period', 10)
        roc_threshold = self.params.get('roc_threshold', 5.0)
        vol_avg_period = self.params.get('vol_avg_period', 20)

        if len(prices) < max(roc_period + 5, vol_avg_period):
            return None

        close = prices['close']
        volume = prices['volume']

        # Rate of change
        roc = close.pct_change(periods=roc_period) * 100

        if len(roc.dropna()) < 2:
            return None

        current_roc = roc.iloc[-1]
        prev_roc = roc.iloc[-2]

        if np.isnan(current_roc) or np.isnan(prev_roc):
            return None

        # Volume confirmation: current volume > average
        avg_volume = volume.rolling(vol_avg_period).mean().iloc[-1]
        current_volume = volume.iloc[-1]
        volume_confirmed = (
            not np.isnan(avg_volume)
            and not np.isnan(current_volume)
            and current_volume > avg_volume * 0.8
        )

        # Buy: ROC crosses above threshold with volume confirmation
        if current_roc > roc_threshold and prev_roc <= roc_threshold and volume_confirmed:
            return Signal(
                source_name=self.name,
                action=Action.BUY,
                probability=0.55,
                risk=0.45,
            )

        # Sell: ROC crosses below negative threshold
        if current_roc < -roc_threshold and prev_roc >= -roc_threshold:
            return Signal(
                source_name=self.name,
                action=Action.SELL,
                probability=0.55,
                risk=0.45,
            )

        return None
