"""Late-Day Momentum — trend-day continuation from 14:30 ET to close.

Thesis: When a stock is in a clear intraday uptrend (price > session
VWAP by >0.5%, intraday high made in the afternoon, range-day expansion
vs prior 5 days), the final 90 minutes often trend into the close as
institutional "close auction" flow accumulates. Enter at ~14:30 ET,
close by 15:55 ET.

Long-only. Gated hard so trades happen on obvious trend days only —
should be 30-80 trades/symbol/year at typical frequency.
"""

from trader.trading.strategy import Signal, Strategy
from trader.objects import Action
from typing import Any, Dict, Optional
from datetime import time as dtime

import numpy as np
import pandas as pd


class LateDayMomentum(Strategy):
    """Afternoon trend-day long continuation, volume + VWAP gated. EOD flat."""

    # Entry window (ET minutes since midnight)
    ENTRY_START_MIN = 14 * 60 + 30    # 14:30
    ENTRY_END_MIN = 15 * 60 + 15      # 15:15 — don't chase too late
    # Trend qualifiers
    VWAP_PREMIUM_PCT = 0.5            # close must be >= 0.5% above VWAP
    VOL_WINDOW = 20
    VOL_MULT = 1.1                    # modest volume confirm on entry bar
    RANGE_EXPANSION = 1.15            # day range so far >= 1.15× mean(prior 5-day ranges)
    # Hold / flat
    MAX_HOLD_BARS = 90                # 15:15 + 90min = 16:45 — EOD will trip first
    EOD_HOUR = 15
    EOD_MINUTE = 55
    MIN_BARS = 120
    RTH_OPEN_MIN = 9 * 60 + 30
    RTH_CLOSE_MIN = 16 * 60

    def precompute(self, prices: pd.DataFrame) -> Dict[str, Any]:
        if len(prices) < self.MIN_BARS:
            return {}

        idx = prices.index
        if idx.tz is None:
            idx = idx.tz_localize("UTC")
        et = idx.tz_convert("America/New_York")

        et_minute = (et.hour * 60 + et.minute).to_numpy()
        et_date_arr = et.date
        et_date = pd.Series(et_date_arr, index=prices.index)
        rth_mask = (et_minute >= self.RTH_OPEN_MIN) & (et_minute < self.RTH_CLOSE_MIN)
        rth_mask_ser = pd.Series(rth_mask, index=prices.index)

        close = prices["close"].to_numpy()
        volume = prices["volume"].to_numpy()

        # Session VWAP
        pv = prices["close"] * prices["volume"]
        cum_pv = pv.groupby(et_date).cumsum()
        cum_v = prices["volume"].groupby(et_date).cumsum()
        vwap = (cum_pv / cum_v.replace(0, np.nan)).fillna(prices["close"]).to_numpy()

        # Daily range (cumulative max-min so far in the day, RTH only)
        rth_high = prices["high"].where(rth_mask_ser)
        rth_low = prices["low"].where(rth_mask_ser)
        day_high_so_far = rth_high.groupby(et_date).cummax()
        day_low_so_far = rth_low.groupby(et_date).cummin()
        day_range_so_far = (day_high_so_far - day_low_so_far).to_numpy()

        # Prior 5 sessions full range — compute per-day full RTH range then shift
        day_full_high = rth_high.groupby(et_date).transform("max")
        day_full_low = rth_low.groupby(et_date).transform("min")
        per_day_range = (day_full_high - day_full_low)
        # Unique per-day value
        daily_rng = per_day_range.groupby(et_date).first()
        # 5-day trailing mean (excluding today)
        prior5_mean = daily_rng.shift(1).rolling(5, min_periods=3).mean()
        prior5_mean_bar = et_date.map(prior5_mean).to_numpy(dtype=float)

        vol_avg = prices["volume"].rolling(self.VOL_WINDOW).mean().to_numpy()

        minutes_since_open = et_minute - self.RTH_OPEN_MIN
        entry_mask = rth_mask & \
            (et_minute >= self.ENTRY_START_MIN) & \
            (et_minute < self.ENTRY_END_MIN)

        return {
            "close": close,
            "vwap": vwap,
            "day_range_so_far": day_range_so_far,
            "prior5_mean": prior5_mean_bar,
            "vol": volume,
            "vol_avg": vol_avg,
            "entry_window": entry_mask,
            "et_minute": et_minute,
        }

    def on_bar(self, prices: pd.DataFrame, state: Dict[str, Any], index: int) -> Optional[Signal]:
        if not state or index < self.MIN_BARS:
            return None
        if not state["entry_window"][index]:
            return None

        close = state["close"][index]
        vwap = state["vwap"][index]
        day_rng = state["day_range_so_far"][index]
        prior5 = state["prior5_mean"][index]
        vol = state["vol"][index]
        vol_avg = state["vol_avg"][index]
        prev_close = state["close"][index - 1]

        if any(np.isnan(x) for x in (vwap, day_rng, prior5, vol_avg)) or vol_avg <= 0 or prior5 <= 0:
            return None

        # Trend qualifiers
        premium = (close - vwap) / vwap * 100.0
        if premium < self.VWAP_PREMIUM_PCT:
            return None
        if day_rng < prior5 * self.RANGE_EXPANSION:
            return None
        if vol < vol_avg * self.VOL_MULT:
            return None
        # Require a small up-tick on the entry bar — don't chase a spike that's reversing
        if close < prev_close:
            return None

        return Signal(
            source_name=self.name, action=Action.BUY,
            probability=0.65, risk=0.35,
            close_by_time=dtime(self.EOD_HOUR, self.EOD_MINUTE),
            max_hold_bars=self.MAX_HOLD_BARS,
        )
