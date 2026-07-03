"""resample_ticks_to_bars — the live tick→OHLCV-bar layer that lets bar-based
strategies run on the per-tick live stream (previously they got tick-granular,
cumulative-volume data and couldn't compute correct bars)."""
import numpy as np
import pandas as pd
import pytest

from trader.data.market_data import resample_ticks_to_bars
from trader.objects import BarSize


def _ticks(rows):
    """rows: list of (isots, price, cumulative_volume) → normalized tick frame."""
    idx = pd.DatetimeIndex([pd.Timestamp(t, tz='UTC') for t, _, _ in rows], name='date')
    data = {
        'open': [p for _, p, _ in rows], 'high': [p for _, p, _ in rows],
        'low': [p for _, p, _ in rows], 'close': [p for _, p, _ in rows],
        'volume': [v for _, _, v in rows], 'vwap': [p for _, p, _ in rows],
        'bar_count': [np.nan] * len(rows),
        'bid': [p for _, p, _ in rows], 'ask': [p for _, p, _ in rows],
        'last': [p for _, p, _ in rows], 'last_size': [1] * len(rows),
    }
    return pd.DataFrame(data, index=idx)


BARS = [
    ('2026-07-03 10:00:10', 100.0, 1000),
    ('2026-07-03 10:00:30', 101.0, 1500),
    ('2026-07-03 10:00:50', 100.5, 1800),
    ('2026-07-03 10:01:10', 102.0, 2200),
    ('2026-07-03 10:01:40', 103.0, 2600),
    ('2026-07-03 10:02:05', 104.0, 2900),   # forming bar
]


class TestResample:
    def test_ohlc_per_minute(self):
        bars = resample_ticks_to_bars(_ticks(BARS), '1min')      # drops forming 10:02
        assert list(bars.index.strftime('%H:%M')) == ['10:00', '10:01']
        b0 = bars.iloc[0]
        assert (b0['open'], b0['high'], b0['low'], b0['close']) == (100.0, 101.0, 100.0, 100.5)
        b1 = bars.iloc[1]
        assert (b1['open'], b1['high'], b1['low'], b1['close']) == (102.0, 103.0, 102.0, 103.0)

    def test_per_bar_volume_from_cumulative(self):
        bars = resample_ticks_to_bars(_ticks(BARS), '1min')
        # first bar: volume since session start = its cumulative (1800)
        assert bars.iloc[0]['volume'] == 1800
        # second bar: 2600 - 1800 = 800 (NOT the raw cumulative 2600)
        assert bars.iloc[1]['volume'] == 800

    def test_forming_bar_dropped_by_default(self):
        bars = resample_ticks_to_bars(_ticks(BARS), '1min')
        assert pd.Timestamp('2026-07-03 10:02', tz='UTC') not in bars.index

    def test_forming_bar_kept_when_requested(self):
        bars = resample_ticks_to_bars(_ticks(BARS), '1min', drop_forming=False)
        assert list(bars.index.strftime('%H:%M')) == ['10:00', '10:01', '10:02']
        assert bars.iloc[2]['close'] == 104.0

    def test_day_reset_uses_bar_cumulative(self):
        # cumulative counter resets across a day boundary — the diff would be
        # negative, so the bar's own cumulative is used instead of a bogus value.
        rows = [
            ('2026-07-03 15:59:10', 100.0, 90000),
            ('2026-07-04 10:00:10', 50.0, 500),
            ('2026-07-04 10:00:40', 51.0, 900),
            ('2026-07-04 10:01:10', 52.0, 1300),   # forming
        ]
        bars = resample_ticks_to_bars(_ticks(rows), '1min')
        d2_open = bars[bars.index >= pd.Timestamp('2026-07-04', tz='UTC')].iloc[0]
        assert d2_open['volume'] == 900 and d2_open['volume'] > 0   # not negative

    def test_empty_and_none(self):
        assert resample_ticks_to_bars(pd.DataFrame(), '1min').empty
        out = resample_ticks_to_bars(None, '1min')
        assert out is None or out.empty


class TestTicksToStrategySignal:
    """End-to-end: a raw per-tick stream (cumulative volume) resampled to 1-min
    bars drives OpeningRangeBreakout to a real BUY — the whole point of the fix.
    Before, the strategy got tick-granular cumulative-volume data and never fired."""

    def _session_ticks(self):
        from types import SimpleNamespace
        rows = []
        cum = 0
        base = pd.Timestamp('2026-07-03 09:00', tz='Australia/Sydney')  # pre-open
        # minutes 0-59 pre-open, 60-89 opening range (10:00-10:29), 90-104 inside,
        # 105 breakout, 106 one tick so 105 is a COMPLETED (not forming) bar.
        for m in range(107):
            t0 = base + pd.Timedelta(minutes=m)
            rth_i = m - 60
            if m == 105:
                prices, volstep = [100.0, 101.0, 101.2], 2000     # cross above 100.5, big vol
            elif 0 <= rth_i < 30:
                hi = 100.5 if rth_i == 5 else 100.2
                lo = 99.5 if rth_i == 10 else 100.0
                prices, volstep = [100.0, hi, lo], 300
            else:
                prices, volstep = [100.0, 100.1, 100.0], 300
            secs = (10, 30, 50) if m < 106 else (10,)   # minute 106: single forming tick
            for si, sec in enumerate(secs):
                cum += volstep
                rows.append((t0 + pd.Timedelta(seconds=sec), prices[si % len(prices)], cum))
        idx = pd.DatetimeIndex([t for t, _, _ in rows], name='date').tz_convert('UTC')
        p = [pr for _, pr, _ in rows]
        return pd.DataFrame({'open': p, 'high': p, 'low': p, 'close': p,
                             'volume': [v for _, _, v in rows], 'vwap': p,
                             'bar_count': [np.nan] * len(rows),
                             'bid': p, 'ask': p, 'last': p, 'last_size': [1] * len(rows)}, index=idx)

    def test_resampled_ticks_fire_orb_buy(self):
        from types import SimpleNamespace
        from trader.objects import Action
        from strategies.opening_range_breakout import OpeningRangeBreakout
        ticks = self._session_ticks()
        bars = resample_ticks_to_bars(ticks, '1min')
        # last COMPLETED bar should be the 10:45 breakout (106 is forming, dropped)
        assert bars.index[-1].tz_convert('Australia/Sydney').strftime('%H:%M') == '10:45'
        s = OpeningRangeBreakout(); s.MIN_BARS = 40
        s._context = SimpleNamespace(name='orb', paper_only=False,
                                     params={'SESSION_TZ': 'Australia/Sydney',
                                             'RTH_OPEN_MIN': 600, 'RTH_CLOSE_MIN': 960})
        sig = s.on_prices(bars)
        assert sig is not None and sig.action == Action.BUY

    def test_raw_ticks_would_not_fire(self):
        """Sanity: the SAME ORB on the RAW per-tick stream (no resampling) does
        NOT fire — confirming the resampling is what enables it."""
        from types import SimpleNamespace
        from strategies.opening_range_breakout import OpeningRangeBreakout
        ticks = self._session_ticks()
        s = OpeningRangeBreakout(); s.MIN_BARS = 40
        s._context = SimpleNamespace(name='orb', paper_only=False,
                                     params={'SESSION_TZ': 'Australia/Sydney',
                                             'RTH_OPEN_MIN': 600, 'RTH_CLOSE_MIN': 960})
        # Raw ticks have cumulative volume → volume filter can't confirm → no fire.
        assert s.on_prices(ticks) is None


def test_barsize_to_pandas_freq():
    assert BarSize.to_pandas_freq(BarSize.Mins1) == '1min'
    assert BarSize.to_pandas_freq(BarSize.Mins5) == '5min'
    assert BarSize.to_pandas_freq(BarSize.Days1) == '1D'
    assert BarSize.to_pandas_freq(BarSize.Hours1) == '1h'
