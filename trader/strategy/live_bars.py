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


def _is_daily(freq: str) -> bool:
    """Whether ``freq`` buckets a whole day (the runtime's own test for it)."""
    try:
        return pd.Timedelta(freq) >= pd.Timedelta(days=1)
    except ValueError:
        return False


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
        # A bucket that is a whole day reports the day's cumulative volume
        # (see ``_day_volume_tick``); an intraday bucket reports what traded
        # in it. ``_counter`` is the cumulative counter as last seen on ANY
        # tick, in or out of session; ``_counter_reset`` says it has started
        # again from zero since the bucket's base was taken.
        self.day_volume = daily_sessions is not None or _is_daily(freq)
        self._counter = None
        self._counter_reset = False
        self.completed = pd.DataFrame(columns=NORMALIZED_COLUMNS)
        if ticks is not None and not ticks.empty:
            ticks = ticks.sort_index()
            if self.day_volume:
                # One rule for the day's volume: replay, do not batch-resample
                # (the batch path differences the counter bar to bar).
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
        volume = float(row['volume'])
        has_volume = math.isfinite(volume) and volume >= 0
        carried = self._counter
        if self.day_volume and has_volume:
            if carried is not None and volume < carried:
                self._counter_reset = True
            self._counter = volume
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
            if self.day_volume:
                # What the counter read before this session's first tick. If
                # it has not restarted, that is the PREVIOUS day's total still
                # showing, not volume traded today. Nothing seen yet (the feed
                # was joined inside the session): the counter is the day's.
                self.volume_base = carried if carried is not None else 0.0
                self.last_volume = None
            else:
                self.volume_base = self.last_volume
        self.last_tick = stamp
        price = float(row['close'])
        if self.day_volume:
            self._day_volume_tick(volume if has_volume else None)
        elif has_volume:
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

    def _day_volume_tick(self, volume: float | None) -> None:
        """Volume bookkeeping for an in-session tick of a DAILY bucket.

        IB's ``volume`` is the cumulative volume for the day and starts again
        from zero each day, so a daily bar's volume is the counter itself.
        Differencing it against the counter at the start of the bar — right
        for a one-minute bar — subtracted YESTERDAY's total from any day that
        traded more than the day before, and reported a day joined part-way
        as the volume since joining (audit 2026-11-14, B3.7).

        The one thing subtracted is a carry-over: until the day's first trade
        the Ticker still shows the previous day's total, and that is not
        volume traded today. The carry-over stops applying the moment the
        counter is seen to go DOWN, on any tick (a restart between sessions
        is seen on out-of-session ticks, which change nothing else here).
        """
        if self._counter_reset:
            self.volume_base = 0.0
            self._counter_reset = False
        if volume is not None:
            self.last_volume = volume
