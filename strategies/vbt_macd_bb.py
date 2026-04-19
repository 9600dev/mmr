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
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
import vectorbt as vbt


class VbtMacdBB(Strategy):
    """MACD × Bollinger Bands confluence (vectorbt) — BUY when MACD histogram
    crosses above zero AND price is in the lower half of the bands; SELL on
    the opposite. Probability scales with distance from the band midline.

    Uses precompute/on_bar so MACD + BBANDS are computed once on the full
    series rather than re-run on every bar (O(N) vs O(N²))."""

    MACD_FAST = 12
    MACD_SLOW = 26
    MACD_SIGNAL = 9
    BB_WINDOW = 20
    BB_ALPHA = 2
    MIN_BARS = 35

    def __init__(self):
        super().__init__()

    # ----- backtest fast path -------------------------------------------

    def precompute(self, prices: pd.DataFrame) -> Dict[str, Any]:
        if len(prices) < self.MIN_BARS:
            return {}
        close = prices['close']
        macd = vbt.MACD.run(
            close,
            fast_window=self.MACD_FAST,
            slow_window=self.MACD_SLOW,
            signal_window=self.MACD_SIGNAL,
        )
        bb = vbt.BBANDS.run(close, window=self.BB_WINDOW, alpha=self.BB_ALPHA)
        # Materialize to numpy so per-bar reads don't re-enter vectorbt.
        return {
            'hist':  np.asarray(macd.hist).ravel(),
            'upper': np.asarray(bb.upper).ravel(),
            'lower': np.asarray(bb.lower).ravel(),
            'close': close.to_numpy(),
        }

    def on_bar(self, prices: pd.DataFrame, state: Dict[str, Any], index: int) -> Optional[Signal]:
        if not state or index < self.MIN_BARS or index < 1:
            return None

        hist = state['hist']
        upper = state['upper']
        lower = state['lower']
        close = state['close']

        current_hist = hist[index]
        prev_hist = hist[index - 1]
        if np.isnan(current_hist) or np.isnan(prev_hist):
            return None

        u = upper[index]
        lo = lower[index]
        if np.isnan(u) or np.isnan(lo):
            return None

        band_width = u - lo
        if band_width <= 0:
            return None
        band_position = (close[index] - lo) / band_width

        # BUY: MACD hist crosses above zero AND price in lower half of bands
        if current_hist > 0 and prev_hist <= 0 and band_position < 0.5:
            prob = min(0.75, 0.55 + (0.5 - band_position) * 0.4)
            return Signal(
                source_name=self.name,
                action=Action.BUY,
                probability=prob,
                risk=1.0 - prob,
            )

        # SELL: MACD hist crosses below zero AND price in upper half of bands
        if current_hist < 0 and prev_hist >= 0 and band_position > 0.5:
            prob = min(0.75, 0.55 + (band_position - 0.5) * 0.4)
            return Signal(
                source_name=self.name,
                action=Action.SELL,
                probability=prob,
                risk=1.0 - prob,
            )
        return None

    # ----- live-trading path (unchanged) --------------------------------

    def on_prices(self, prices: pd.DataFrame) -> Optional[Signal]:
        if len(prices) < self.MIN_BARS:
            return None

        close = prices['close']

        macd = vbt.MACD.run(close, fast_window=self.MACD_FAST,
                            slow_window=self.MACD_SLOW, signal_window=self.MACD_SIGNAL)
        hist = macd.hist

        if len(hist.dropna()) < 2:
            return None

        current_hist = hist.iloc[-1]
        prev_hist = hist.iloc[-2]
        if np.isnan(current_hist) or np.isnan(prev_hist):
            return None

        bb = vbt.BBANDS.run(close, window=self.BB_WINDOW, alpha=self.BB_ALPHA)
        upper = bb.upper.iloc[-1]
        lower = bb.lower.iloc[-1]
        if np.isnan(upper) or np.isnan(lower):
            return None

        current_price = close.iloc[-1]
        band_width = upper - lower
        if band_width <= 0:
            return None

        band_position = (current_price - lower) / band_width

        if current_hist > 0 and prev_hist <= 0 and band_position < 0.5:
            prob = min(0.75, 0.55 + (0.5 - band_position) * 0.4)
            return Signal(source_name=self.name, action=Action.BUY,
                          probability=prob, risk=1.0 - prob)

        if current_hist < 0 and prev_hist >= 0 and band_position > 0.5:
            prob = min(0.75, 0.55 + (band_position - 0.5) * 0.4)
            return Signal(source_name=self.name, action=Action.SELL,
                          probability=prob, risk=1.0 - prob)

        return None
