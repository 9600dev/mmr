"""How far behind a bar series is, answered once instead of twice.

`data status` and the sweep freshness guard each computed this separately and
each made the same two mistakes, which is what happens when two call sites
answer one question.

  1. `last.replace(tzinfo=None)` DISCARDS a timezone rather than converting it.
     It keeps the wall clock and silently moves the instant by the offset.
     Daily bars are stored tz-aware (`2026-07-26 21:00:00-07:00`), and one of
     the two call sites then compared that against `datetime.now(timezone.utc)`.

  2. Counting CALENDAR days. Daily bars only exist on trading days, so a series
     current through Friday's close reads as three days stale every Monday
     morning. The sweep guard refuses at three, so this could refuse a sweep on
     data that was completely current.

Neither showed up as a failure. They showed up as a status table that reported
"stale 4" while the underlying data was fine, which is worse: a freshness
signal that cries wolf is one people stop reading.
"""

from __future__ import annotations

import datetime as dt

import pytest

from trader.data.bar_staleness import sessions_behind

UTC = dt.timezone.utc
MINUS_7 = dt.timezone(dt.timedelta(hours=-7))

FRI = dt.datetime(2026, 7, 24, 20, 0, tzinfo=UTC)     # Friday, after the close
SAT = dt.datetime(2026, 7, 25, 12, 0, tzinfo=UTC)
SUN = dt.datetime(2026, 7, 26, 12, 0, tzinfo=UTC)
MON = dt.datetime(2026, 7, 27, 16, 0, tzinfo=UTC)
TUE = dt.datetime(2026, 7, 28, 16, 40, tzinfo=UTC)


class TestWeekendsAreNotStaleness:
    """The Monday-morning false alarm, which is the whole reason this exists."""

    def test_friday_bar_is_current_on_friday(self):
        assert sessions_behind(FRI, dt.datetime(2026, 7, 24, 23, 0, tzinfo=UTC)) == 0

    @pytest.mark.parametrize('when', [SAT, SUN])
    def test_friday_bar_is_current_all_weekend(self, when):
        assert sessions_behind(FRI, when) == 0

    def test_friday_bar_is_one_session_behind_on_monday(self):
        """Three by calendar days, one by trading sessions. The sweep guard
        refuses at three, so the old arithmetic could refuse a sweep on data
        that was entirely up to date."""
        assert sessions_behind(FRI, MON) == 1

    def test_friday_bar_is_two_sessions_behind_on_tuesday(self):
        assert sessions_behind(FRI, TUE) == 2


class TestTheOffsetIsConvertedNotDropped:
    """`replace(tzinfo=None)` keeps the wall clock and moves the instant.
    `astimezone` moves the wall clock and keeps the instant. Only the second is
    a timezone conversion."""

    def test_a_bar_stored_with_an_offset_is_read_as_the_instant_it_names(self):
        # 2026-07-26 21:00-07:00 IS 2026-07-27 04:00Z: Monday's session.
        monday_session = dt.datetime(2026, 7, 26, 21, 0, tzinfo=MINUS_7)
        assert sessions_behind(monday_session, TUE) == 1

    def test_the_same_instant_expressed_two_ways_agrees(self):
        as_offset = dt.datetime(2026, 7, 26, 21, 0, tzinfo=MINUS_7)
        as_utc = as_offset.astimezone(UTC)
        assert sessions_behind(as_offset, TUE) == sessions_behind(as_utc, TUE)

    def test_a_naive_bar_is_read_as_utc_not_local(self):
        """Guessing local would move the instant by the host's offset, which is
        exactly the bug being fixed. Every producer here stores UTC."""
        naive = dt.datetime(2026, 7, 27, 4, 0)
        aware = dt.datetime(2026, 7, 27, 4, 0, tzinfo=UTC)
        assert sessions_behind(naive, TUE) == sessions_behind(aware, TUE)


class TestDegenerateInputs:
    def test_no_bar_is_unknown_not_zero(self):
        """None means "cannot tell", which a caller must not confuse with
        "current". The guard treats unknown as stale."""
        assert sessions_behind(None) is None
        assert sessions_behind(None, TUE) is None

    def test_a_future_bar_is_current_not_negative(self):
        """A session-stamped daily bar can lead the wall clock. Negative
        staleness is not a state anything downstream can use."""
        assert sessions_behind(dt.datetime(2026, 8, 5, 0, 0, tzinfo=UTC), TUE) == 0

    def test_a_non_datetime_is_unknown(self):
        assert sessions_behind('2026-07-27') is None
        assert sessions_behind(12345) is None

    def test_a_pandas_timestamp_is_accepted(self):
        pd = pytest.importorskip('pandas')
        ts = pd.Timestamp('2026-07-24 20:00', tz='UTC')
        assert sessions_behind(ts, MON) == 1


class TestLongGaps:
    def test_a_month_behind_counts_only_weekdays(self):
        june = dt.datetime(2026, 6, 25, 20, 0, tzinfo=UTC)   # the real AAPL case
        behind = sessions_behind(june, TUE)
        elapsed = (TUE.date() - june.date()).days
        assert behind is not None
        assert behind < elapsed, 'weekends were counted as sessions'
        assert behind == 23
