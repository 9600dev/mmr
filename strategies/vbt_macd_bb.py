"""MACD + Bollinger Bands Strategy powered by vectorbt indicators.

Uses vectorbt's optimized MACD and Bollinger Bands implementations.
Generates BUY when:
  - MACD histogram crosses above zero (bullish momentum)
  - Price is near or below the lower Bollinger Band (cheap entry)
Generates SELL when:
  - MACD histogram crosses below zero (bearish momentum)
  - Price is near or above the upper Bollinger Band (expensive)

This combines momentum (MACD) with mean-reversion (BB) for
higher-conviction entries.
"""

from trader.trading.strategy import Signal, Strategy
from trader.objects import Action
from typing import Optional

import numpy as np
import pandas as pd
import vectorbt as vbt


class VbtMacdBB(Strategy):
    def __init__(self):
        super().__init__()

    def on_prices(self, prices: pd.DataFrame) -> Optional[Signal]:
        if len(prices) < 35:
            return None

        close = prices['close']

        # MACD (12/26/9) via vectorbt
        macd = vbt.MACD.run(close, fast_window=12, slow_window=26, signal_window=9)
        hist = macd.hist

        if len(hist.dropna()) < 2:
            return None

        current_hist = hist.iloc[-1]
        prev_hist = hist.iloc[-2]

        if np.isnan(current_hist) or np.isnan(prev_hist):
            return None

        # Bollinger Bands (20, 2) via vectorbt
        bb = vbt.BBANDS.run(close, window=20, alpha=2)
        upper = bb.upper.iloc[-1]
        lower = bb.lower.iloc[-1]
        middle = bb.middle.iloc[-1]

        if np.isnan(upper) or np.isnan(lower):
            return None

        current_price = close.iloc[-1]

        # Calculate where price sits within the bands (0 = lower, 1 = upper)
        band_width = upper - lower
        if band_width <= 0:
            return None

        band_position = (current_price - lower) / band_width

        # BUY: MACD histogram crosses above zero AND price is in lower half of bands
        if current_hist > 0 and prev_hist <= 0 and band_position < 0.5:
            # Higher probability when price is closer to lower band
            prob = min(0.75, 0.55 + (0.5 - band_position) * 0.4)
            return Signal(
                source_name=self.name,
                action=Action.BUY,
                probability=prob,
                risk=1.0 - prob,
            )

        # SELL: MACD histogram crosses below zero AND price is in upper half of bands
        if current_hist < 0 and prev_hist >= 0 and band_position > 0.5:
            prob = min(0.75, 0.55 + (band_position - 0.5) * 0.4)
            return Signal(
                source_name=self.name,
                action=Action.SELL,
                probability=prob,
                risk=1.0 - prob,
            )

        return None
