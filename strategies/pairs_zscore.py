"""Pairs Trading / Statistical Arbitrage via Z-Score of price ratio.

Tracks the log price ratio between two instruments, computes a rolling
z-score, and generates buy/sell signals when the spread diverges beyond
a threshold.  Trades the second conid (the one being priced) as the
"cheap leg" — buys when the z-score says it's undervalued relative to
the first conid, sells when the spread reverts.

Requires exactly 2 conids in the strategy config.
"""

from trader.trading.strategy import Signal, Strategy
from trader.objects import Action
from typing import Dict, Optional

import numpy as np
import pandas as pd


class PairsZScore(Strategy):
    """Trade the z-score of the log price ratio between two instruments."""

    LOOKBACK = 60       # rolling window for mean/std of the spread
    ENTRY_Z = 2.0       # open position when |z| > ENTRY_Z
    EXIT_Z = 0.5        # close position when |z| < EXIT_Z
    MIN_BARS = 30       # minimum bars before trading

    def __init__(self):
        super().__init__()
        self._prices: Dict[int, pd.Series] = {}  # conid -> close series
        self._position_side: Optional[str] = None  # 'long' or 'short'

    def on_prices(self, prices: pd.DataFrame) -> Optional[Signal]:
        if len(prices) < 2:
            return None

        # Figure out which conid this call is for by using the
        # last close value to match against stored data
        close = prices['close']

        # Determine conid identity — use ctx.conids ordering
        conids = self.conids or []
        if len(conids) < 2:
            return None

        # Identify which conid we're processing by checking which
        # stored series this close data matches (by length growth)
        current_conid = None
        for cid in conids:
            if cid in self._prices:
                if len(close) > len(self._prices[cid]):
                    current_conid = cid
                    break
            else:
                current_conid = cid
                break

        if current_conid is None:
            # All series same length; assign to first that differs
            current_conid = conids[0] if close.iloc[-1] != self._prices.get(conids[1], pd.Series()).iloc[-1] else conids[1]

        self._prices[current_conid] = close.copy()

        # Only generate signals on the second conid, when both series exist
        if current_conid != conids[1]:
            return None

        # Need both price series with enough data
        s0 = self._prices.get(conids[0])
        s1 = self._prices.get(conids[1])
        if s0 is None or s1 is None:
            return None

        # Align to same length (use the shorter)
        n = min(len(s0), len(s1))
        if n < self.MIN_BARS:
            return None

        p0 = s0.iloc[-n:].values
        p1 = s1.iloc[-n:].values

        # Log price ratio
        with np.errstate(divide='ignore', invalid='ignore'):
            log_ratio = np.log(p0 / p1)

        if np.any(~np.isfinite(log_ratio)):
            return None

        # Rolling z-score
        window = min(self.LOOKBACK, n)
        roll_mean = pd.Series(log_ratio).rolling(window).mean().iloc[-1]
        roll_std = pd.Series(log_ratio).rolling(window).std().iloc[-1]

        if np.isnan(roll_mean) or np.isnan(roll_std) or roll_std < 1e-8:
            return None

        z = (log_ratio[-1] - roll_mean) / roll_std

        # Signal logic
        # High z-score → conid[0] is expensive relative to conid[1] → BUY conid[1]
        # Low z-score  → conid[1] is expensive relative to conid[0] → SELL conid[1]
        if self._position_side is None:
            if z > self.ENTRY_Z:
                self._position_side = 'long'
                prob = min(0.9, 0.5 + abs(z) * 0.1)
                return Signal(
                    source_name=self.name,
                    action=Action.BUY,
                    probability=prob,
                    risk=1.0 - prob,
                    metadata={'z_score': float(z), 'spread_mean': float(roll_mean)},
                )
            elif z < -self.ENTRY_Z:
                # We can't short in the backtester, but signal the desire
                self._position_side = 'short'
                return None  # Can't execute shorts in current backtester
        else:
            if self._position_side == 'long' and z < self.EXIT_Z:
                self._position_side = None
                return Signal(
                    source_name=self.name,
                    action=Action.SELL,
                    probability=0.7,
                    risk=0.3,
                    metadata={'z_score': float(z), 'exit': True},
                )
            elif self._position_side == 'short' and z > -self.EXIT_Z:
                self._position_side = None
                return None  # Was a short, can't exit what we didn't enter

        return None
