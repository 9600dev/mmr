"""OpeningRangeBreakout — live (on_prices) path + exchange-awareness.

The live strategy_runtime dispatches only via on_prices(); the backtester uses
precompute()+on_bar(). Before the fix ORB implemented only the latter, so it
emitted nothing live, and its session was hardcoded to US ET hours so it could
never recognise an ASX open. These tests pin both behaviours.
"""
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from trader.objects import Action
from trader.simulation.lookahead_check import assert_no_lookahead

from strategies.opening_range_breakout import OpeningRangeBreakout


def _session_frame(tz_offset_hours: int, open_minute: int, n_pre: int = 25):
    """Build a 1-min OHLCV frame whose bars land at the given session open.

    Bars are timestamped in UTC such that, once converted to a tz `tz_offset_hours`
    ahead of UTC, the first RTH bar is at `open_minute` minutes past midnight.
    Layout: n_pre warmup bars before the open, a 30-bar opening range (~99.5-100.5),
    flat post-range bars, then a clean volume-confirmed breakout to 101.
    """
    # First RTH bar in UTC = open time minus the tz offset.
    open_utc_min = open_minute - tz_offset_hours * 60
    base = pd.Timestamp('2026-07-03 00:00', tz='UTC') + pd.Timedelta(minutes=open_utc_min)
    # Warmup bars sit just before the open (still same UTC day here).
    start = base - pd.Timedelta(minutes=n_pre)

    rows = []
    total = n_pre + 46  # warmup + 30-bar range + 15 post-range + breakout as LAST bar
    for i in range(total):
        rth_i = i - n_pre  # minutes since open (<0 = warmup)
        vol = 1000.0
        if i == total - 1:                     # LAST bar = the breakout we evaluate live
            hi, lo, cl, vol = 101.2, 100.4, 101.0, 6000.0
        elif rth_i < 0:
            hi, lo, cl = 100.1, 99.9, 100.0
        elif rth_i < 30:                       # opening range: 99.5-100.5
            hi = 100.5 if rth_i == 5 else 100.2
            lo = 99.5 if rth_i == 10 else 99.8
            cl = 100.0
        else:                                  # post-range, inside the box
            hi, lo, cl = 100.3, 99.7, 100.0
        rows.append({'open': cl, 'high': hi, 'low': lo, 'close': cl,
                     'volume': vol, 'vwap': cl, 'bar_count': 1})
    idx = pd.date_range(start, periods=total, freq='1min', tz='UTC')
    return pd.DataFrame(rows, index=idx)


def _asx_strategy():
    s = OpeningRangeBreakout()
    s.name = 'orb_test'
    # ASX session params (as they'd be passed via --params on deploy).
    s.SESSION_TZ = 'Australia/Sydney'
    s.RTH_OPEN_MIN = 10 * 60          # 10:00 Sydney
    s.RTH_CLOSE_MIN = 16 * 60
    s.MIN_BARS = 40
    return s


class TestLiveOnPrices:
    def test_asx_breakout_fires_buy_live(self):
        """on_prices() must emit a BUY on the ASX opening-range breakout."""
        s = _asx_strategy()
        # Sydney is UTC+10 in July (AEST). Breakout at index n_pre(25)+45 = 70.
        df = _session_frame(tz_offset_hours=10, open_minute=10 * 60, n_pre=25)
        sig = s.on_prices(df)
        assert sig is not None, 'no signal — opening range never locked for ASX'
        assert sig.action == Action.BUY

    def test_no_signal_during_range_window(self):
        """Mid-range (range not yet closed) must return None."""
        s = _asx_strategy()
        df = _session_frame(tz_offset_hours=10, open_minute=10 * 60, n_pre=25)
        # Last bar at index 50 (rth_i=25) is INSIDE the 30-min range window and
        # past MIN_BARS(40) — so None here means "range not locked", not "too few
        # bars". The range only closes at rth_i>=30.
        mid = df.iloc[:51]
        assert s.on_prices(mid) is None

    def test_us_default_still_works(self):
        """Regression: the default US session still fires a breakout."""
        s = OpeningRangeBreakout()
        s.name = 'orb_us'
        s.MIN_BARS = 40
        # US ET is UTC-4 in July (EDT); open 09:30 = 570.
        df = _session_frame(tz_offset_hours=-4, open_minute=9 * 60 + 30, n_pre=25)
        sig = s.on_prices(df)
        assert sig is not None and sig.action == Action.BUY

    def test_asx_via_live_params_context(self):
        """The REAL live mechanism: session config delivered via self.params
        (from the runtime's StrategyContext), not setattr on class attrs."""
        s = OpeningRangeBreakout()
        s.MIN_BARS = 40
        # name/params/paper_only are all _context-backed properties on Strategy.
        s._context = SimpleNamespace(
            name='orb_params', paper_only=False,
            params={'SESSION_TZ': 'Australia/Sydney', 'RTH_OPEN_MIN': 600, 'RTH_CLOSE_MIN': 960})
        df = _session_frame(tz_offset_hours=10, open_minute=10 * 60, n_pre=25)
        sig = s.on_prices(df)
        assert sig is not None and sig.action == Action.BUY

    def test_live_matches_backtest_at_last_bar(self):
        """on_prices() == on_bar() at the final index (same semantics)."""
        s = _asx_strategy()
        df = _session_frame(tz_offset_hours=10, open_minute=10 * 60, n_pre=25)
        live = s.on_prices(df)
        state = s.precompute(df)
        bt = s.on_bar(df, state, len(df) - 1)
        assert (live is None) == (bt is None)
        if live is not None:
            assert live.action == bt.action


class TestExchangeAwareness:
    def test_us_hardcode_removed_asx_locks_range(self):
        """The ASX opening range must actually lock (orb_high non-NaN post-window)."""
        s = _asx_strategy()
        df = _session_frame(tz_offset_hours=10, open_minute=10 * 60, n_pre=25)
        state = s.precompute(df)
        # After the 30-min window, the range is frozen (not NaN).
        assert not np.isnan(state['orb_high'][-1])
        assert not np.isnan(state['orb_low'][-1])


class TestNoLookahead:
    def test_precompute_is_lookahead_safe_asx(self):
        s = _asx_strategy()
        df = _session_frame(tz_offset_hours=10, open_minute=10 * 60, n_pre=25)
        assert_no_lookahead(s, df)
