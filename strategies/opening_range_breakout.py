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
    # Session is defined in SESSION_TZ so "market open" is stable year-round and
    # the strategy works on ANY exchange, not just the US. Override per-deployment:
    #   US   (default): SESSION_TZ='America/New_York', open 09:30, close 16:00
    #   ASX          : SESSION_TZ='Australia/Sydney',  open 10:00 (600), close 16:00 (960)
    #   LSE          : SESSION_TZ='Europe/London',     open 08:00 (480), close 16:30 (990)
    SESSION_TZ = 'America/New_York'
    RTH_OPEN_MIN = 9 * 60 + 30     # session open, minutes since SESSION_TZ midnight
    RTH_CLOSE_MIN = 16 * 60        # session close, minutes since SESSION_TZ midnight
    MIN_BARS = 40
    # Exit management. Without these, the ONLY exit is a fresh volume-
    # confirmed cross below the day's range-low — a slow low-volume bleed or
    # an overnight gap below the next day's range never fires it, so losers
    # ride for days (observed live: PLTR −6.7% over 3 sessions, no exit).
    # EOD_FLATTEN emits close_by_time = session close − EOD_FLATTEN_BEFORE_MIN
    # on every BUY, turning the strategy genuinely intraday (matching its
    # docstring). MAX_HOLD_BARS (0 = off) caps hold duration in bars.
    # Defaults preserve historical behaviour — enable per-deployment after
    # validating with `backtest --live-semantics`.
    EOD_FLATTEN = False
    EOD_FLATTEN_BEFORE_MIN = 15    # minutes before session close to flatten
    MAX_HOLD_BARS = 0              # 0 = no bar-count exit

    def _exit_rules(self):
        """(close_by_time, max_hold_bars) for a BUY signal, or (None, None).

        The flatten time is session-relative (RTH_CLOSE_MIN −
        EOD_FLATTEN_BEFORE_MIN) so the same class works for US and ASX
        deployments; both the backtester and the live runtime compare
        close_by_time against session-tz bar timestamps.
        """
        close_by = None
        if self._cfg('EOD_FLATTEN', self.EOD_FLATTEN):
            rth_close = int(self._cfg('RTH_CLOSE_MIN', self.RTH_CLOSE_MIN))
            before = int(self._cfg('EOD_FLATTEN_BEFORE_MIN', self.EOD_FLATTEN_BEFORE_MIN))
            minute = max(0, rth_close - before)
            import datetime as _dt
            close_by = _dt.time(minute // 60, minute % 60)
        max_hold = int(self._cfg('MAX_HOLD_BARS', self.MAX_HOLD_BARS)) or None
        return close_by, max_hold

    def _cfg(self, key: str, default: Any) -> Any:
        """Read a session-config value from live params (``self.params``) with the
        class-attribute as fallback. The live runtime delivers ``--params`` into
        ``self.params`` (it does NOT setattr upper-case class attrs — that's
        backtest-only), so reading here is what makes SESSION_TZ / RTH_OPEN_MIN /
        RTH_CLOSE_MIN / RANGE_MINUTES actually configurable per-deployment live.
        In backtests the value is either setattr'd on the class attr (upper-case
        override) or absent from params, so the fallback still returns the right
        thing."""
        params = getattr(self, 'params', None) or {}
        return params[key] if key in params else default

    def precompute(self, prices: pd.DataFrame) -> Dict[str, Any]:
        if len(prices) < self.MIN_BARS:
            return {}

        session_tz = self._cfg('SESSION_TZ', self.SESSION_TZ)
        rth_open = int(self._cfg('RTH_OPEN_MIN', self.RTH_OPEN_MIN))
        rth_close = int(self._cfg('RTH_CLOSE_MIN', self.RTH_CLOSE_MIN))
        range_min = int(self._cfg('RANGE_MINUTES', self.RANGE_MINUTES))

        # Convert to the session timezone so "market open" has a stable
        # definition year-round and works on any exchange (US/ASX/LSE/…).
        idx = prices.index
        if idx.tz is None:
            idx = idx.tz_localize('UTC')
        et = idx.tz_convert(session_tz)

        et_minute = (et.hour * 60 + et.minute).to_numpy()
        et_date = et.date  # python date objects, one per bar

        # ORB window per day: bars with rth_open ≤ session-minute < rth_open + range
        orb_mask = (et_minute >= rth_open) & (
            et_minute < rth_open + range_min
        )
        rth_mask = (et_minute >= rth_open) & (et_minute < rth_close)

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
            elif et_minute[i] >= rth_open + range_min:
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

    def on_prices(self, prices: pd.DataFrame) -> Optional[Signal]:
        """Live dispatch path (the strategy_runtime calls this per bar).

        The backtester uses precompute()+on_bar() directly for O(N) replay; the
        live runtime only calls on_prices(), so without this the strategy emitted
        NOTHING live. Here we run the same precompute over the accumulated window
        and evaluate on_bar() at the latest bar — identical breakout semantics,
        just driven per-tick instead of by the backtest loop. This is O(N) per
        call, which is fine live (one call per bar on a bounded window) but is
        exactly why the backtester must NOT route through here.
        """
        if prices is None or len(prices) < self.MIN_BARS:
            return None
        state = self.precompute(prices)
        if not state:
            return None
        return self.on_bar(prices, state, len(prices) - 1)

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
            close_by, max_hold = self._exit_rules()
            return Signal(
                source_name=self.name, action=Action.BUY,
                probability=0.60, risk=0.40,
                close_by_time=close_by, max_hold_bars=max_hold,
            )
        # SELL: close crosses below ORB low
        if close < orb_l and prev_close >= orb_l and vol_ok:
            return Signal(
                source_name=self.name, action=Action.SELL,
                probability=0.60, risk=0.40,
            )
        return None
