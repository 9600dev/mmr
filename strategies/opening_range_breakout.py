"""Opening Range Breakout (ORB) — classic intraday momentum.

Defines the high and low of the first N minutes of each US trading day
(default 30). After the opening range is established, BUY when close
breaks above the range-high with above-average volume; SELL when close
breaks below the range-low. Trades are filtered to regular trading hours
(09:30–16:00 ET); pre-market and after-hours bars don't participate.

Uses the precompute hook so the per-day range and volume average are
computed once over the full series.
"""

from trader.trading.strategy import Signal, Strategy
from trader.objects import Action
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd


class OpeningRangeBreakout(Strategy):
    """30-minute opening-range breakout, volume-confirmed, RTH-only."""

    RANGE_MINUTES = 30
    VOLUME_MULT = 1.5              # current bar volume must exceed this × 20-bar SMA
    RTH_OPEN_MIN = 9 * 60 + 30     # 09:30 ET (in minutes since ET midnight)
    RTH_CLOSE_MIN = 16 * 60        # 16:00 ET
    MIN_BARS = 40

    def precompute(self, prices: pd.DataFrame) -> Dict[str, Any]:
        if len(prices) < self.MIN_BARS:
            return {}

        # Convert to ET so "market open" has a stable definition year-round.
        idx = prices.index
        if idx.tz is None:
            idx = idx.tz_localize('UTC')
        et = idx.tz_convert('America/New_York')

        et_minute = (et.hour * 60 + et.minute).to_numpy()
        et_date = et.date  # python date objects, one per bar

        # Determine the ORB window per day: bars with 9:30 ≤ ET-minute < 9:30 + RANGE_MINUTES
        orb_mask = (et_minute >= self.RTH_OPEN_MIN) & (
            et_minute < self.RTH_OPEN_MIN + self.RANGE_MINUTES
        )
        rth_mask = (et_minute >= self.RTH_OPEN_MIN) & (et_minute < self.RTH_CLOSE_MIN)

        high = prices['high'].to_numpy()
        low = prices['low'].to_numpy()
        close = prices['close'].to_numpy()
        volume = prices['volume'].to_numpy()

        # For each day, compute ORB high/low from the ORB-window bars, then
        # broadcast to every bar of that day. We carry the range forward so
        # a bar AT index i knows the ORB of its containing day provided the
        # ORB window has closed by i's timestamp — otherwise leave NaN so
        # on_bar refuses to trade before the range is complete.
        orb_high = np.full(len(prices), np.nan, dtype=float)
        orb_low = np.full(len(prices), np.nan, dtype=float)

        # Groupby day, compute running max/min over the ORB window, carry
        # forward after the window closes. Simple single pass.
        current_day = None
        running_hi = -np.inf
        running_lo = np.inf
        locked_hi = np.nan
        locked_lo = np.nan
        for i in range(len(prices)):
            d = et_date[i]
            if d != current_day:
                current_day = d
                running_hi, running_lo = -np.inf, np.inf
                locked_hi, locked_lo = np.nan, np.nan

            if orb_mask[i]:
                if high[i] > running_hi: running_hi = high[i]
                if low[i] < running_lo:  running_lo = low[i]
                # Still inside the ORB window — don't publish the range yet
                # (we only act on breakouts AFTER the range is fully formed).
            elif et_minute[i] >= self.RTH_OPEN_MIN + self.RANGE_MINUTES:
                # Window closed for today; freeze the range.
                if running_hi != -np.inf and np.isnan(locked_hi):
                    locked_hi, locked_lo = running_hi, running_lo
                orb_high[i] = locked_hi
                orb_low[i]  = locked_lo

        vol_avg = prices['volume'].rolling(20).mean().to_numpy()

        return {
            'close':     close,
            'volume':    volume,
            'vol_avg':   vol_avg,
            'orb_high':  orb_high,
            'orb_low':   orb_low,
            'rth':       rth_mask.astype(bool),
        }

    def on_bar(self, prices: pd.DataFrame, state: Dict[str, Any], index: int) -> Optional[Signal]:
        if not state or index < self.MIN_BARS:
            return None
        if not state['rth'][index]:
            return None  # pre/post-market — don't trade

        orb_h = state['orb_high'][index]
        orb_l = state['orb_low'][index]
        if np.isnan(orb_h) or np.isnan(orb_l):
            return None  # opening range hasn't closed yet today

        close = state['close'][index]
        prev_close = state['close'][index - 1] if index > 0 else close
        volume = state['volume'][index]
        vol_avg = state['vol_avg'][index]
        if np.isnan(vol_avg) or vol_avg <= 0:
            return None

        vol_ok = volume > vol_avg * self.VOLUME_MULT

        # BUY: close crosses above ORB high with volume
        if close > orb_h and prev_close <= orb_h and vol_ok:
            return Signal(
                source_name=self.name, action=Action.BUY,
                probability=0.60, risk=0.40,
            )
        # SELL: close crosses below ORB low
        if close < orb_l and prev_close >= orb_l and vol_ok:
            return Signal(
                source_name=self.name, action=Action.SELL,
                probability=0.60, risk=0.40,
            )
        return None
