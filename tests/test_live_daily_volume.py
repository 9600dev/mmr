"""A live DAILY bar carries the day's cumulative volume (audit 2026-11-14, B3.7).

IB's ``volume`` on a live Ticker is the cumulative volume for the day; it
starts again from zero each day. ``LiveBarBuffer`` turned that counter into a
per-bar figure the way a one-minute bar needs — "the counter now, minus the
counter when the bar began" — for daily bars too. For a daily bar the counter
when the bar began is YESTERDAY's total (or, on the first day, whatever the
counter read when the feed was joined), so:

* a day that traded more than the day before it was reported as
  ``today - yesterday`` (3.0M after a 1.0M day read 2.0M);
* a day joined part-way was reported as the volume since joining.

A day that traded LESS than the previous one came out right by accident (the
negative difference was read as a reset), which is why this hid.
"""
import numpy as np
import pandas as pd
import pytest

from trader.data.market_data import NORMALIZED_COLUMNS
from trader.strategy.live_bars import DailySessionBuckets, LiveBarBuffer

NY = 'America/New_York'
#: IB's cumulative day volume for four consecutive NASDAQ sessions.
DAYS = (('2026-11-09', 1_000_000), ('2026-11-10', 3_000_000),
        ('2026-11-11', 500_000), ('2026-11-12', 2_000_000))


def _tick(stamp, volume, price=100.0):
    stamp = pd.Timestamp(stamp)
    stamp = stamp.tz_convert('UTC') if stamp.tzinfo else stamp.tz_localize('UTC')
    row = dict.fromkeys(NORMALIZED_COLUMNS, np.nan)
    row.update(open=price, high=price, low=price, close=price, last=price, volume=volume)
    return pd.DataFrame([row], index=pd.DatetimeIndex([stamp], name='date'), columns=NORMALIZED_COLUMNS)


def _session_ticks(day, total, start='09:30', end='16:00', stale=None):
    """Ticks every ten minutes whose volume climbs to ``total`` by the last one.

    ``stale``: the counter still reads this (yesterday's total) on the first
    two ticks, before the day's first trade resets it.
    """
    stamps = pd.date_range(f'{day} {start}', f'{day} {end}', freq='10min', tz=NY)
    ticks = []
    for k, stamp in enumerate(stamps):
        cumulative = total * (k + 1) / len(stamps)
        if stale is not None and k < 2:
            cumulative = stale
        ticks.append(_tick(stamp, cumulative))
    return ticks


def _daily_buffer():
    return LiveBarBuffer(None, '1D', retention_days=10, daily_sessions=DailySessionBuckets('NASDAQ'))


def _feed(buffer, ticks):
    for tick in ticks:
        buffer.append(tick)
    return buffer


def _volumes(buffer):
    return [float(v) for v in buffer.completed['volume']]


def test_daily_bars_report_each_days_cumulative_volume():
    buffer = _daily_buffer()
    for day, total in DAYS:
        _feed(buffer, _session_ticks(day, total))
    _feed(buffer, _session_ticks('2026-11-13', 1))            # completes the 12th

    assert list(buffer.completed.index) == [pd.Timestamp(day, tz='UTC') for day, _ in DAYS]
    assert _volumes(buffer) == pytest.approx([total for _, total in DAYS])


def test_a_day_busier_than_the_one_before_is_not_reported_net_of_it():
    """The reported shape: 3.0M after a 1.0M day read 2.0M."""
    buffer = _daily_buffer()
    _feed(buffer, _session_ticks('2026-11-09', 1_000_000))
    _feed(buffer, _session_ticks('2026-11-10', 3_000_000))
    assert buffer.current['volume'] == pytest.approx(3_000_000)


def test_a_day_joined_part_way_reports_the_days_volume_not_the_volume_since_joining():
    buffer = _daily_buffer()
    _feed(buffer, [_tick(pd.Timestamp('2026-11-09 13:00', tz=NY), 400_000),
                   _tick(pd.Timestamp('2026-11-09 15:59', tz=NY), 1_000_000)])
    assert buffer.current['volume'] == pytest.approx(1_000_000)


def test_yesterdays_counter_still_showing_before_the_first_trade_is_not_todays_volume():
    """Until the day's first trade the Ticker still carries yesterday's total.

    That reading is not volume traded today: the forming bar says 0, and once
    the counter restarts the bar follows it, past yesterday's total.
    """
    buffer = _daily_buffer()
    _feed(buffer, _session_ticks('2026-11-09', 1_000_000))
    today = _session_ticks('2026-11-10', 3_000_000, stale=1_000_000)

    _feed(buffer, today[:2])
    assert buffer.current['volume'] == 0

    _feed(buffer, today[2:])
    assert buffer.current['volume'] == pytest.approx(3_000_000)
    assert _volumes(buffer) == pytest.approx([1_000_000])


def test_a_reset_seen_only_outside_the_session_still_starts_the_day_from_zero():
    """The counter restarts overnight, between sessions, and the first tick of
    the new session already reads MORE than the whole previous day."""
    buffer = _daily_buffer()
    _feed(buffer, _session_ticks('2026-11-09', 1_000_000))
    _feed(buffer, [_tick(pd.Timestamp('2026-11-10 02:00', tz=NY), 0.0)])      # out of session
    assert buffer.current['volume'] == pytest.approx(1_000_000)               # Monday is untouched
    _feed(buffer, [_tick(pd.Timestamp('2026-11-10 09:30', tz=NY), 1_200_000),
                   _tick(pd.Timestamp('2026-11-10 15:59', tz=NY), 3_000_000)])
    assert _volumes(buffer) == pytest.approx([1_000_000])
    assert buffer.current['volume'] == pytest.approx(3_000_000)


def test_a_quote_outside_the_session_cannot_change_the_days_volume():
    buffer = _daily_buffer()
    _feed(buffer, _session_ticks('2026-11-09', 1_000_000))
    _feed(buffer, [_tick(pd.Timestamp('2026-11-09 23:00', tz=NY), 1_250_000, price=999.0)])
    assert buffer.current['volume'] == pytest.approx(1_000_000)
    assert buffer.current['high'] == 100.0


def test_a_tick_without_volume_leaves_the_days_volume_alone():
    buffer = _daily_buffer()
    _feed(buffer, _session_ticks('2026-11-09', 1_000_000))
    _feed(buffer, [_tick(pd.Timestamp('2026-11-09 16:05', tz=NY), float('nan'), price=101.0)])
    assert buffer.current['volume'] == pytest.approx(1_000_000)
    assert buffer.current['close'] == 101.0


def test_buffer_primed_from_retained_ticks_matches_one_fed_tick_by_tick():
    ticks = [tick for day, total in DAYS for tick in _session_ticks(day, total)]
    primed = LiveBarBuffer(pd.concat(ticks), '1D', retention_days=10,
                           daily_sessions=DailySessionBuckets('NASDAQ'))
    assert _volumes(primed) == pytest.approx([1_000_000, 3_000_000, 500_000])
    assert primed.current['volume'] == pytest.approx(2_000_000)


def test_daily_bars_without_a_session_calendar_use_the_same_rule():
    """No calendar (an unmapped venue): UTC-day buckets, same volume rule,
    whether the buffer is primed from retained ticks or fed one at a time."""
    ticks = []
    for day, total in DAYS:
        stamps = pd.date_range(f'{day} 14:30', f'{day} 21:00', freq='30min', tz='UTC')
        ticks += [_tick(stamp, total * (k + 1) / len(stamps)) for k, stamp in enumerate(stamps)]
    primed = LiveBarBuffer(pd.concat(ticks), '1D', retention_days=10)
    fed = _feed(LiveBarBuffer(None, '1D', retention_days=10), ticks)
    for buffer in (primed, fed):
        assert _volumes(buffer) == pytest.approx([1_000_000, 3_000_000, 500_000])
        assert buffer.current['volume'] == pytest.approx(2_000_000)


def test_intraday_bars_still_report_per_bar_volume():
    """The one-minute rule is unchanged: a bar's volume is what traded in it."""
    buffer = LiveBarBuffer(None, '1min')
    start = pd.Timestamp('2026-11-10 14:30', tz='UTC')
    for k, cumulative in enumerate([1_000_000, 1_000_100, 1_000_400, 1_000_900, 1_001_000]):
        buffer.append(_tick(start + pd.Timedelta(seconds=30 * k), cumulative))
    assert _volumes(buffer) == pytest.approx([100, 800])
