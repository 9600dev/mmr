"""Regime-Adaptive Strategy: switches between momentum and mean-reversion.

Detects the current market regime using the Hurst exponent:
  - H > 0.55 → trending regime → use momentum (20/50 EMA crossover)
  - H < 0.45 → mean-reverting regime → use Bollinger Band bounce
  - 0.45 ≤ H ≤ 0.55 → random walk → no trade (sit out)

The Hurst exponent is estimated via the rescaled range (R/S) method
over a rolling window.  This gives the strategy an adaptive edge:
it doesn't force momentum trades in choppy markets, and doesn't
fade trends.

Also incorporates volatility-adjusted position sizing via the
signal probability: higher confidence in regime = higher probability.
"""

from trader.trading.strategy import Signal, Strategy
from trader.objects import Action
from typing import Optional

import numpy as np
import pandas as pd


def _hurst_rs(series: np.ndarray, max_lag: int = 20) -> float:
    """Estimate the Hurst exponent using the rescaled range (R/S) method.

    Returns a value in [0, 1]:
      H < 0.5 → mean-reverting
      H = 0.5 → random walk
      H > 0.5 → trending
    """
    n = len(series)
    if n < max_lag * 2:
        return 0.5  # not enough data, assume random walk

    lags = range(2, max_lag + 1)
    rs_values = []

    for lag in lags:
        # Split series into non-overlapping chunks of size `lag`
        n_chunks = n // lag
        if n_chunks < 1:
            continue
        rs_chunk = []
        for i in range(n_chunks):
            chunk = series[i * lag:(i + 1) * lag]
            mean_chunk = np.mean(chunk)
            deviations = chunk - mean_chunk
            cumdev = np.cumsum(deviations)
            r = np.max(cumdev) - np.min(cumdev)
            s = np.std(chunk, ddof=1)
            if s > 1e-10:
                rs_chunk.append(r / s)
        if rs_chunk:
            rs_values.append((np.log(lag), np.log(np.mean(rs_chunk))))

    if len(rs_values) < 3:
        return 0.5

    log_lags, log_rs = zip(*rs_values)
    # Linear regression: log(R/S) = H * log(n) + c
    coeffs = np.polyfit(log_lags, log_rs, 1)
    return float(np.clip(coeffs[0], 0.0, 1.0))


class RegimeAdaptive(Strategy):
    """Switches between momentum and mean-reversion based on Hurst exponent."""

    HURST_WINDOW = 60     # bars for Hurst estimation
    TREND_THRESHOLD = 0.55
    REVERT_THRESHOLD = 0.45

    # Momentum params
    FAST_EMA = 10
    SLOW_EMA = 30

    # Mean-reversion params
    BB_PERIOD = 20
    BB_STD = 2.0

    MIN_BARS = 50

    def __init__(self):
        super().__init__()
        self._last_regime: Optional[str] = None  # 'trending', 'reverting', 'neutral'
        self._position_open = False

    def on_prices(self, prices: pd.DataFrame) -> Optional[Signal]:
        if len(prices) < self.MIN_BARS:
            return None

        close = prices['close']
        returns = close.pct_change(fill_method=None).dropna().values

        if len(returns) < self.HURST_WINDOW:
            return None

        # --- Regime detection ---
        hurst = _hurst_rs(returns[-self.HURST_WINDOW:])

        if hurst > self.TREND_THRESHOLD:
            regime = 'trending'
        elif hurst < self.REVERT_THRESHOLD:
            regime = 'reverting'
        else:
            regime = 'neutral'

        self._last_regime = regime

        # --- Neutral regime: close positions and sit out ---
        if regime == 'neutral':
            if self._position_open:
                self._position_open = False
                return Signal(
                    source_name=self.name,
                    action=Action.SELL,
                    probability=0.6,
                    risk=0.4,
                    metadata={'regime': regime, 'hurst': hurst, 'reason': 'regime_exit'},
                )
            return None

        # --- Trending regime: EMA crossover momentum ---
        if regime == 'trending':
            fast = close.ewm(span=self.FAST_EMA, adjust=False).mean()
            slow = close.ewm(span=self.SLOW_EMA, adjust=False).mean()

            curr_fast, curr_slow = fast.iloc[-1], slow.iloc[-1]
            prev_fast, prev_slow = fast.iloc[-2], slow.iloc[-2]

            # Bullish crossover
            if curr_fast > curr_slow and prev_fast <= prev_slow:
                if not self._position_open:
                    self._position_open = True
                    # Higher Hurst = stronger trend = higher confidence
                    prob = min(0.85, 0.5 + (hurst - 0.5) * 2)
                    return Signal(
                        source_name=self.name,
                        action=Action.BUY,
                        probability=prob,
                        risk=1.0 - prob,
                        metadata={'regime': regime, 'hurst': hurst, 'signal': 'ema_crossover'},
                    )

            # Bearish crossover — exit long
            if curr_fast < curr_slow and prev_fast >= prev_slow:
                if self._position_open:
                    self._position_open = False
                    return Signal(
                        source_name=self.name,
                        action=Action.SELL,
                        probability=0.7,
                        risk=0.3,
                        metadata={'regime': regime, 'hurst': hurst, 'signal': 'ema_exit'},
                    )

        # --- Mean-reverting regime: Bollinger Band bounce ---
        if regime == 'reverting':
            sma = close.rolling(self.BB_PERIOD).mean()
            std = close.rolling(self.BB_PERIOD).std()
            lower = sma - self.BB_STD * std
            upper = sma + self.BB_STD * std

            last_close = close.iloc[-1]
            last_lower = lower.iloc[-1]
            last_upper = upper.iloc[-1]
            last_sma = sma.iloc[-1]

            if np.isnan(last_lower) or np.isnan(last_upper):
                return None

            # Buy when price touches lower band
            if last_close <= last_lower and not self._position_open:
                self._position_open = True
                prob = min(0.85, 0.5 + (0.5 - hurst) * 2)
                return Signal(
                    source_name=self.name,
                    action=Action.BUY,
                    probability=prob,
                    risk=1.0 - prob,
                    metadata={'regime': regime, 'hurst': hurst, 'signal': 'bb_lower_touch'},
                )

            # Sell when price reaches SMA (mean reversion target)
            if last_close >= last_sma and self._position_open:
                self._position_open = False
                return Signal(
                    source_name=self.name,
                    action=Action.SELL,
                    probability=0.7,
                    risk=0.3,
                    metadata={'regime': regime, 'hurst': hurst, 'signal': 'bb_mean_revert'},
                )

        return None
