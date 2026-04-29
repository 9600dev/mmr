"""Opening Drive Fade — fade an extreme first-30-min drive back to VWAP.

Thesis: When a liquid name opens with a big directional drive (>1.5
ATR_day from open) in the first 30 RTH minutes and then shows exhaustion
(volume decline + RSI extreme), the move often reverts partway back
toward session VWAP during the rest of the morning. Fade-of-extremes
variant, gated hard on "this has to be a notable move" so trades are rare.

Long-only (backtester rejects shorts), so we only fade gap-DOWN drives:
extreme first-30-min low + RSI oversold + volume exhaustion → BUY for
revert up to VWAP. EOD flat.
"""

from trader.trading.strategy import Signal, Strategy
from trader.objects import Action
from typing import Any, Dict, Optional
from datetime import time as dtime

import numpy as np
import pandas as pd


class OpeningDriveFade(Strategy):
    """Fade an extreme first-30min downside drive on RSI + volume exhaustion."""

    DRIVE_WINDOW_MIN = 30          # first 30 min establishes the drive extreme
    ENTRY_WINDOW_END_MIN = 120     # entries allowed until 11:30 ET
    DRIVE_ATR_MULT = 1.5           # drive must be >= 1.5 × ATR(14-day)
    RSI_PERIOD = 14
    RSI_OVERSOLD = 30
    VOL_EXHAUST_MULT = 0.75        # current bar vol < 0.75× 20-bar avg = exhaustion
    VOL_WINDOW = 20
    MAX_HOLD_BARS = 120            # 2 hours
    EOD_HOUR = 15
    EOD_MINUTE = 45
    MIN_BARS = 60
    RTH_OPEN_MIN = 9 * 60 + 30

    def precompute(self, prices: pd.DataFrame) -> Dict[str, Any]:
        if len(prices) < self.MIN_BARS:
            return {}

        idx = prices.index
        if idx.tz is None:
            idx = idx.tz_localize("UTC")
        et = idx.tz_convert("America/New_York")

        et_minute = (et.hour * 60 + et.minute).to_numpy()
        et_date = pd.Series(et.date, index=prices.index)
        rth_mask = (et_minute >= self.RTH_OPEN_MIN) & (et_minute < 16 * 60)

        close = prices["close"].to_numpy()
        low = prices["low"].to_numpy()
        volume = prices["volume"].to_numpy()

        # Session VWAP
        pv = prices["close"] * prices["volume"]
        cum_pv = pv.groupby(et_date).cumsum()
        cum_v = prices["volume"].groupby(et_date).cumsum()
        vwap = (cum_pv / cum_v.replace(0, np.nan)).fillna(prices["close"]).to_numpy()

        # Day-minutes since RTH open (negative pre-market)
        minutes_since_open = et_minute - self.RTH_OPEN_MIN
        drive_window_mask = rth_mask & (minutes_since_open < self.DRIVE_WINDOW_MIN)
        drive_ser = pd.Series(drive_window_mask, index=prices.index)

        # Per-day: open at RTH, min during drive window
        rth_mask_ser = pd.Series(rth_mask, index=prices.index)
        day_open = prices["open"].where(rth_mask_ser).groupby(et_date).transform("first").to_numpy()
        drive_low = prices["low"].where(drive_ser).groupby(et_date).transform("min").to_numpy()

        # ATR(14-day-worth-of-bars) — for 1-min data, ~14*390 = 5460 bars
        tr1 = prices["high"] - prices["low"]
        tr2 = (prices["high"] - prices["close"].shift()).abs()
        tr3 = (prices["low"] - prices["close"].shift()).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr_bars = 14 * 390  # approximate — 14 trading days of 1-min bars
        atr = tr.rolling(atr_bars, min_periods=390).mean().to_numpy()

        # Drive magnitude: (day_open - drive_low)
        drive_down = day_open - drive_low

        # RSI(14) standard
        delta = prices["close"].diff()
        gain = delta.clip(lower=0).rolling(self.RSI_PERIOD).mean()
        loss = (-delta.clip(upper=0)).rolling(self.RSI_PERIOD).mean()
        rs = gain / loss.replace(0, np.nan)
        rsi = (100.0 - (100.0 / (1.0 + rs))).to_numpy()

        vol_avg = prices["volume"].rolling(self.VOL_WINDOW).mean().to_numpy()

        # Entry window: after drive window ends, up to ENTRY_WINDOW_END_MIN
        entry_mask = rth_mask & \
            (minutes_since_open >= self.DRIVE_WINDOW_MIN) & \
            (minutes_since_open < self.ENTRY_WINDOW_END_MIN)

        return {
            "close": close,
            "low": low,
            "vwap": vwap,
            "day_open": day_open,
            "drive_low": drive_low,
            "drive_down": drive_down,
            "atr": atr,
            "rsi": rsi,
            "vol": volume,
            "vol_avg": vol_avg,
            "entry_window": entry_mask,
        }

    def on_bar(self, prices: pd.DataFrame, state: Dict[str, Any], index: int) -> Optional[Signal]:
        if not state or index < self.MIN_BARS:
            return None
        if not state["entry_window"][index]:
            return None

        close = state["close"][index]
        low = state["low"][index]
        vwap = state["vwap"][index]
        drive_low = state["drive_low"][index]
        drive_down = state["drive_down"][index]
        atr = state["atr"][index]
        rsi = state["rsi"][index]
        vol = state["vol"][index]
        vol_avg = state["vol_avg"][index]

        if any(np.isnan(x) for x in (vwap, drive_low, drive_down, atr, rsi, vol_avg)) or vol_avg <= 0 or atr <= 0:
            return None

        # Must be a notable down-drive — drive magnitude >= DRIVE_ATR_MULT × ATR
        if drive_down < atr * self.DRIVE_ATR_MULT:
            return None

        # Price must still be below VWAP (i.e. the fade target hasn't been hit yet)
        if close >= vwap:
            return None

        # Exhaustion: RSI oversold AND volume declining
        if rsi > self.RSI_OVERSOLD:
            return None
        if vol > vol_avg * self.VOL_EXHAUST_MULT:
            return None

        # And price must be near the drive low (within 0.2 ATR) — the reversion setup
        if low - drive_low > 0.2 * atr:
            return None

        return Signal(
            source_name=self.name, action=Action.BUY,
            probability=0.62, risk=0.38,
            close_by_time=dtime(self.EOD_HOUR, self.EOD_MINUTE),
            max_hold_bars=self.MAX_HOLD_BARS,
        )
