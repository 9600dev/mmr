"""VWAP Reversion — intraday mean-reversion to the session volume-weighted
average price, confirmed by RSI.

Logic:
  - Session VWAP resets at the start of each trading day (daily normalize).
  - Rolling std of (close − VWAP) gives an intraday price-dispersion scale.
  - BUY when price is ENTRY_STD below VWAP AND RSI < RSI_OVERSOLD (cheap +
    oversold → expect reversion up to fair value).
  - SELL mirror above VWAP + overbought.

Uses the precompute hook — all indicator arrays are computed once on the
full series. Correctness of the session-VWAP reset depends on grouping by
the date portion of the index; we assert no-lookahead in tests.
"""

from trader.trading.strategy import Signal, Strategy
from trader.objects import Action
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd


def _rsi(close: pd.Series, period: int) -> pd.Series:
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


class VwapReversion(Strategy):
    """Intraday VWAP mean reversion with RSI confirmation — resets VWAP daily."""

    ENTRY_STD = 1.5           # σ from session VWAP to trigger entry
    RSI_PERIOD = 14
    RSI_OVERSOLD = 35
    RSI_OVERBOUGHT = 65
    STD_WINDOW = 30           # rolling std of (close − VWAP)
    MIN_BARS = 40

    def precompute(self, prices: pd.DataFrame) -> Dict[str, Any]:
        if len(prices) < self.MIN_BARS:
            return {}

        # Session VWAP: group by calendar date of the index, cumsum
        # price × volume / cumsum volume within each day. When we hand
        # precompute the full history the groupby spans every day, but the
        # cumulative arithmetic within a day only uses bars at or before the
        # current bar — so position i depends only on bars [i within same
        # day, up to i]. No cross-day leakage.
        close = prices['close']
        volume = prices['volume']
        day = prices.index.normalize() if prices.index.tz else prices.index.normalize()
        pv = close * volume
        cum_pv = pv.groupby(day).cumsum()
        cum_v = volume.groupby(day).cumsum()
        # If a bar has zero cumulative volume (rare — day just started with
        # a zero-volume bar), fall back to close so we don't divide by zero.
        vwap = (cum_pv / cum_v.replace(0, np.nan)).fillna(close)

        # Rolling std of (close − VWAP). Within-day would be more precise
        # but a fixed rolling window is cheaper and close enough for 1-min.
        diff = close - vwap
        std = diff.rolling(self.STD_WINDOW).std()

        rsi = _rsi(close, self.RSI_PERIOD)

        return {
            'close': close.to_numpy(),
            'vwap':  vwap.to_numpy(),
            'std':   std.to_numpy(),
            'rsi':   rsi.to_numpy(),
        }

    def on_bar(self, prices: pd.DataFrame, state: Dict[str, Any], index: int) -> Optional[Signal]:
        if not state or index < self.MIN_BARS:
            return None

        close = state['close'][index]
        vwap = state['vwap'][index]
        std = state['std'][index]
        rsi = state['rsi'][index]
        prev_close = state['close'][index - 1]
        prev_vwap = state['vwap'][index - 1]

        if (np.isnan(vwap) or np.isnan(std) or np.isnan(rsi)
                or np.isnan(prev_vwap) or std <= 0):
            return None

        z = (close - vwap) / std

        # BUY: meaningfully below VWAP and RSI oversold (classic dip-buy).
        if z < -self.ENTRY_STD and rsi < self.RSI_OVERSOLD:
            return Signal(
                source_name=self.name, action=Action.BUY,
                probability=0.65, risk=0.35,
            )

        # EXIT LONG / enter-short symmetric: fire SELL when price CROSSES
        # from below VWAP to at-or-above VWAP. This closes any open long
        # near fair value (reversion complete) rather than waiting for the
        # far-side overbought condition that rarely triggers.
        crossed_up = (prev_close < prev_vwap) and (close >= vwap)
        if crossed_up:
            return Signal(
                source_name=self.name, action=Action.SELL,
                probability=0.60, risk=0.40,
            )

        # Far-side overbought — also emits SELL, which would go short in a
        # short-capable backtester. Our backtester rejects short sells
        # (held <= 0 → skip), so this path only takes effect if we already
        # have a long to close; usually crossed_up above closes first.
        if z > self.ENTRY_STD and rsi > self.RSI_OVERBOUGHT:
            return Signal(
                source_name=self.name, action=Action.SELL,
                probability=0.65, risk=0.35,
            )
        return None
