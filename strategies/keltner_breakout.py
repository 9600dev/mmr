"""Keltner Channel Breakout — volatility-expansion momentum play.

EMA(20) is the center line; ATR(10) scaled by BAND_MULT forms bands above
and below. BUY when the close crosses above the upper band with volume
confirmation (volatility is expanding in our favour). SELL mirror when
the close crosses below the lower band.

Differs from Bollinger-band mean reversion: Bollinger uses standard
deviation (statistical); Keltner uses ATR (price-range). ATR doesn't
compress as aggressively in low-vol regimes, so Keltner breakouts tend
to fire more often in chop — we add volume confirmation to filter.
"""

from trader.trading.strategy import Signal, Strategy
from trader.objects import Action
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd


class KeltnerBreakout(Strategy):
    """EMA(20) ± 2×ATR(10) breakout, volume-confirmed."""

    EMA_PERIOD = 20
    ATR_PERIOD = 10
    BAND_MULT = 2.0
    VOLUME_MULT = 1.3
    MIN_BARS = 40

    def precompute(self, prices: pd.DataFrame) -> Dict[str, Any]:
        if len(prices) < self.MIN_BARS:
            return {}

        close = prices['close']
        high = prices['high']
        low = prices['low']
        prev_close = close.shift(1)

        # True range — each component is past-only by construction
        tr = pd.concat([
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ], axis=1).max(axis=1)
        atr = tr.rolling(self.ATR_PERIOD).mean()

        ema = close.ewm(span=self.EMA_PERIOD, adjust=False).mean()
        upper = ema + self.BAND_MULT * atr
        lower = ema - self.BAND_MULT * atr
        vol_avg = prices['volume'].rolling(20).mean()

        return {
            'close':   close.to_numpy(),
            'upper':   upper.to_numpy(),
            'lower':   lower.to_numpy(),
            'volume':  prices['volume'].to_numpy(),
            'vol_avg': vol_avg.to_numpy(),
        }

    def on_bar(self, prices: pd.DataFrame, state: Dict[str, Any], index: int) -> Optional[Signal]:
        if not state or index < self.MIN_BARS:
            return None

        close = state['close'][index]
        prev_close = state['close'][index - 1]
        upper = state['upper'][index]
        prev_upper = state['upper'][index - 1]
        lower = state['lower'][index]
        prev_lower = state['lower'][index - 1]
        volume = state['volume'][index]
        vol_avg = state['vol_avg'][index]

        if (np.isnan(upper) or np.isnan(lower) or np.isnan(prev_upper)
                or np.isnan(prev_lower) or np.isnan(vol_avg) or vol_avg <= 0):
            return None

        vol_ok = volume > vol_avg * self.VOLUME_MULT

        # BUY: close crosses above upper band with volume
        if close > upper and prev_close <= prev_upper and vol_ok:
            return Signal(
                source_name=self.name, action=Action.BUY,
                probability=0.60, risk=0.40,
            )
        # SELL: close crosses below lower band (no volume gate — exits faster)
        if close < lower and prev_close >= prev_lower:
            return Signal(
                source_name=self.name, action=Action.SELL,
                probability=0.60, risk=0.40,
            )
        return None
