"""Incremental, completed OHLCV bars shared by strategies on one subscription.

Only a new interval materializes a DataFrame. A quote in the forming interval
updates a handful of scalars, independently of the retained tick history.
"""
from __future__ import annotations

import math
import datetime as dt
import pandas as pd

from trader.data.market_data import NORMALIZED_COLUMNS


class DailySessionBuckets:
    """Map real ticks to venue sessions; label every session at midnight UTC.

    Midnight UTC is a date label, not the instant the venue opened. In
    particular an ASX summer session straddles midnight UTC.
    """
    def __init__(self, calendar_name):
        import pandas_market_calendars as mcal
        self.calendar_name = calendar_name
        self.timezone = mcal.get_calendar(calendar_name).tz

    def __call__(self, stamp):
        from trader.data.market_session import session_intervals
        day = stamp.tz_convert(self.timezone).date()
        unknown = False
        for offset in (0, -1, 1):
            session_day = day + dt.timedelta(days=offset)
            lookup = session_intervals(self.calendar_name, session_day)
            unknown |= not lookup.evaluable
            for start, end in lookup.intervals or ((lookup.window,) if lookup.window else ()):
                if start <= stamp < end or (lookup.window and stamp == end == lookup.window[1]):
                    return pd.Timestamp(session_day, tz='UTC')
        # Same explicit fail-open calendar policy as the intraday feed.
        return pd.Timestamp(day, tz='UTC') if unknown else None

    def historical_labels(self, index):
        return pd.DatetimeIndex([
            pd.Timestamp(stamp.date() if stamp == stamp.normalize()
                         else stamp.tz_convert(self.timezone).date(), tz='UTC')
            for stamp in index], name=index.name)


class LiveBarBuffer:
    def __init__(self, ticks: pd.DataFrame | None, freq: str, retention_days: int = 2,
                 daily_sessions: DailySessionBuckets | None = None):
        from trader.data.market_data import resample_ticks_to_bars
        self.freq = freq
        self.retention_days = retention_days
        self.daily_sessions = daily_sessions
        self.revision = 0
        self.last_tick = None
        self.bucket = None
        self.current = None
        self.volume_base = None
        self.last_volume = None
        self.completed = pd.DataFrame(columns=NORMALIZED_COLUMNS)
        if ticks is not None and not ticks.empty:
            ticks = ticks.sort_index()
            if daily_sessions is not None:
                for n in range(len(ticks)):
                    self.append(ticks.iloc[n:n + 1])
                return
            all_bars = resample_ticks_to_bars(ticks, freq, drop_forming=False)
            if not all_bars.empty:
                self.bucket = ticks.index[-1].floor(freq)
                self.completed = all_bars.loc[all_bars.index < self.bucket].copy()
                self.current = (all_bars.loc[self.bucket].to_dict()
                                if self.bucket in all_bars.index else None)
                previous = ticks.loc[ticks.index < self.bucket, 'volume'].dropna()
                first = ticks.loc[ticks.index >= self.bucket, 'volume'].dropna()
                self.volume_base = float(previous.iloc[-1]) if len(previous) else (
                    float(first.iloc[0]) if len(first) else None)
                volumes = ticks['volume'].dropna()
                self.last_volume = float(volumes.iloc[-1]) if len(volumes) else None
            self.last_tick = ticks.index[-1]

    def append(self, normalized: pd.DataFrame) -> bool:
        """Return False for an old tick; it cannot revise a dispatched bar."""
        stamp = normalized.index[-1]
        if self.last_tick is not None and stamp < self.last_tick:
            return False
        row = normalized.iloc[-1]
        bucket = self.daily_sessions(stamp) if self.daily_sessions else stamp.floor(self.freq)
        if bucket is None:
            # The wall-clock watermark may complete a session, but an
            # out-of-session quote must not change its OHLCV.
            self.last_tick = stamp
            return True
        if self.bucket is None or bucket > self.bucket:
            if self.current is not None and self.bucket is not None:
                frame = pd.DataFrame([self.current], index=pd.DatetimeIndex([self.bucket], name='date'))
                self.completed = (frame if self.completed.empty else pd.concat([self.completed, frame]))
                # A weekend/halt may exceed the retained wall-clock window.
                # Always publish the bar being completed before pruning it;
                # otherwise Friday can disappear at Monday's first tick.
                cutoff = min(bucket - pd.Timedelta(days=self.retention_days), self.bucket)
                self.completed = self.completed.loc[self.completed.index >= cutoff]
            self.revision += 1
            self.bucket = bucket
            self.current = None
            self.volume_base = self.last_volume
        self.last_tick = stamp
        price = float(row['close'])
        volume = float(row['volume'])
        if math.isfinite(volume) and volume >= 0:
            if self.volume_base is None:
                self.volume_base = volume
            self.last_volume = volume
        if not math.isfinite(price) or price <= 0:
            return True
        if self.current is None:
            self.current = dict(open=price, high=price, low=price, close=price, bar_count=0)
        current = self.current
        current['high'] = max(float(current['high']), price)
        current['low'] = min(float(current['low']), price)
        current['close'] = price
        current['bar_count'] = float(current.get('bar_count', 0)) + 1
        if self.last_volume is not None and self.volume_base is not None:
            delta = self.last_volume - self.volume_base
            current['volume'] = delta if delta >= 0 else self.last_volume
        else:
            current['volume'] = float('nan')
        for column in ('vwap', 'bid', 'ask', 'last', 'last_size'):
            value = row.get(column)
            if pd.notna(value):
                current[column] = value
        return True
