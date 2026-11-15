"""Session gating: which minutes count as market data (AUDIT_ROADMAP G8).

Every case here is anchored to something real — the live bug, the venues the
roster actually trades, and the extended-hours window our own stored history
spans. The central requirement, decided by the operator 2026-07-26:
**US extended-hours bars MUST reach strategies.** This is a session gate, not
an RTH filter, and the tests that would fail if someone "tightened" it to RTH
are the point of the file.
"""
import datetime as dt
import pathlib
from unittest import mock

import pandas as pd
import pytest

from trader.data.market_session import (
    calendar_name_for,
    in_session,
    session_window,
)


def _utc(s):
    return pd.Timestamp(s, tz='UTC')


@pytest.fixture(autouse=True)
def _cold_caches():
    """Every test starts with cold module caches.

    Mutation testing exposed why this matters: `_schedule_cache` is
    module-level, so once any test resolves (calendar, day) the cached window
    answers for every later test — and mutants in `_utc_stamps` / `_calendar` /
    the schedule call survived DIRECT assertions on their output because the
    mutated code never ran. A warm cache turns a behavioural test into a cache
    read. `test_a_failed_lookup_is_not_cached` additionally depends on starting
    cold to mean anything.
    """
    import trader.data.market_session as ms
    with ms._lock:
        ms._schedule_cache.clear()
        ms._regular_cache.clear()
        ms._early_shift_cache.clear()
        ms._calendar_cache.clear()
        ms._warned_unknown.clear()
    yield


class TestExtendedHoursReachStrategies:
    """23% of GOOGL's stored 1-min bars are zero-volume and they ARE the
    extended-hours minutes (0% zero-volume inside RTH, 42-54% outside). The
    backtester sees them, so the live gate must too."""

    @pytest.mark.parametrize('when,label', [
        ('2026-07-24 08:00:00', 'pre-market open, 04:00 ET'),
        ('2026-07-24 11:00:00', 'mid pre-market, 07:00 ET'),
        ('2026-07-24 13:30:00', 'RTH open, 09:30 ET'),
        ('2026-07-24 17:00:00', 'midday RTH'),
        ('2026-07-24 20:00:00', 'RTH close, 16:00 ET'),
        ('2026-07-24 22:00:00', 'post-market, 18:00 ET'),
        ('2026-07-24 23:59:00', 'last post-market minute, 19:59 ET'),
    ])
    def test_us_extended_hours_are_in_session(self, when, label):
        assert in_session(_utc(when), 'SMART', 'NASDAQ') is True, label

    def test_the_us_window_matches_our_stored_history(self):
        """GOOGL's stored 1-min bars run 08:00-23:59 UTC. The gate's window has
        to agree, or live and backtest see different series."""
        lookup = session_window('NASDAQ', dt.date(2026, 7, 24))
        assert lookup.evaluable is True
        assert lookup.window is not None
        start, end = lookup.window
        assert start == _utc('2026-07-24 08:00:00')
        assert end == _utc('2026-07-25 00:00:00')

    @pytest.mark.parametrize('when,label', [
        ('2026-07-24 03:00:00', 'overnight, 23:00 ET — before pre-market'),
        ('2026-07-25 18:00:00', 'Saturday'),
        ('2026-07-26 18:45:00', 'Sunday'),
    ])
    def test_genuinely_closed_minutes_are_out_of_session(self, when, label):
        assert in_session(_utc(when), 'SMART', 'NASDAQ') is False, label


class TestASX:
    def test_the_observed_bug_case_is_refused(self):
        """The live event: three quote ticks for WDS at 11:45 PDT on a SUNDAY
        formed a dispatched bar and dropped bar_age_s from 218,093s to 263s."""
        assert in_session(_utc('2026-07-26 18:45:00'), 'ASX', 'ASX') is False

    def test_the_closing_auction_is_in_session(self):
        """WDS's real last bar is the 06:10 UTC (16:10 AEST) auction print on
        1,379,150 shares. A gate that stopped at the 16:00 close would discard
        the single most important bar of the ASX day."""
        assert in_session(_utc('2026-07-24 06:10:00'), 'ASX', 'ASX') is True

    def test_the_asx_session_body_is_in_session(self):
        assert in_session(_utc('2026-07-24 00:00:00'), 'ASX', 'ASX') is True
        assert in_session(_utc('2026-07-24 03:00:00'), 'ASX', 'ASX') is True

    def test_asx_has_no_extended_hours_so_us_evening_is_closed(self):
        assert in_session(_utc('2026-07-24 12:00:00'), 'ASX', 'ASX') is False


class TestFailsOpen:
    """The asymmetry that shapes this module: the bug costs a polluted moving
    average, a wrong gate costs a strategy that silently stops trading."""

    def test_an_unmapped_venue_is_treated_as_open(self):
        assert in_session(_utc('2026-07-26 18:45:00'), 'NEWEXCHANGE', '') is True

    def test_forex_is_always_open(self):
        assert in_session(_utc('2026-07-26 18:45:00'), 'IDEALPRO', '',
                          sec_type='CASH') is True

    def test_cash_overrides_a_mapped_and_closed_venue(self):
        """The always-open check must decide BEFORE the venue lookup. This
        pins that: sec_type CASH on a venue that is mapped and currently shut
        is still in-session. The forex test above cannot pin it — IDEALPRO is
        unmapped, so the fail-open path gives the same answer even if the
        CASH check is broken (a mutant proved exactly that)."""
        assert in_session(_utc('2026-07-26 18:45:00'), 'ASX', 'ASX',
                          sec_type='CASH') is True

    def test_an_unparseable_timestamp_is_treated_as_open(self):
        assert in_session('not-a-timestamp', 'ASX', 'ASX') is True
        assert in_session(None, 'ASX', 'ASX') is True

    def test_a_naive_timestamp_is_read_as_utc(self):
        assert in_session(pd.Timestamp('2026-07-24 14:00:00'), 'SMART', 'NASDAQ') is True
        assert in_session(pd.Timestamp('2026-07-26 18:45:00'), 'ASX', 'ASX') is False


class TestVenueMapping:
    def test_primary_exchange_wins_over_the_routing_destination(self):
        """IB reports exchange='SMART' (a router, not a venue) with the real
        listing venue in primaryExchange. Reading `exchange` first would map
        every US name through whatever SMART happens to be aliased to."""
        assert calendar_name_for('SMART', 'NASDAQ') == 'NASDAQ'
        assert calendar_name_for('SMART', 'ASX') == 'ASX'

    def test_us_venues_share_the_nyse_session(self):
        for code in ('NYSE', 'ARCA', 'AMEX', 'BATS', 'IEX'):
            assert calendar_name_for(code, '') == 'NYSE', code

    def test_unknown_venue_maps_to_nothing(self):
        assert calendar_name_for('NOPE', '') is None

    def test_mapping_is_case_and_whitespace_insensitive(self):
        assert calendar_name_for(' asx ', '') == 'ASX'

    def test_in_session_resolves_the_venue_from_primary_alone(self):
        """IB can report a routing code we have no mapping for while
        primaryExchange names the real venue. Dropping either argument on the
        way into calendar_name_for silently falls open — Sunday must still be
        refused when only primary maps."""
        assert in_session(_utc('2026-07-26 18:45:00'), 'UNMAPPEDROUTER', 'ASX') is False

    def test_in_session_resolves_the_venue_from_exchange_alone(self):
        assert in_session(_utc('2026-07-26 18:45:00'), 'ASX', '') is False


class TestSessionWindow:
    def test_a_non_session_day_is_evaluable_with_no_window(self):
        """'Shut' and 'unknown' must not look alike — that conflation is what
        made a calendar error suppress every bar."""
        for cal, day in (('ASX', dt.date(2026, 7, 26)),      # Sunday
                         ('NASDAQ', dt.date(2026, 7, 25))):  # Saturday
            lookup = session_window(cal, day)
            assert lookup.window is None
            assert lookup.evaluable is True

    def test_the_asx_window_includes_the_auction(self):
        lookup = session_window('ASX', dt.date(2026, 7, 24))
        assert lookup.window == (_utc('2026-07-24 00:00:00'),
                                 _utc('2026-07-24 06:10:00'))
        assert lookup.evaluable is True


class TestACalendarErrorFailsOpen:
    """Found by mutation testing, 2026-07-26.

    session_window returned a bare Optional, so "shut that day" and "the lookup
    blew up" were the same value, and in_session turned both into False. The log
    said 'treating as open' while the function suppressed the bar — one library
    exception would have starved every strategy of bars, silently. That is the
    exact failure this module's docstring calls worse than the bug it fixes.
    """

    def test_an_exploding_calendar_admits_the_bar(self):
        import trader.data.market_session as ms
        with mock.patch.object(ms, '_calendar', side_effect=RuntimeError('boom')):
            assert ms.in_session(_utc('2026-07-27 01:00:00'), 'ASX', 'ASX') is True

    def test_an_exploding_calendar_reports_not_evaluable(self):
        import trader.data.market_session as ms
        with mock.patch.object(ms, '_calendar', side_effect=RuntimeError('boom')):
            lookup = ms.session_window('ASX', dt.date(2026, 7, 27))
        assert lookup.evaluable is False
        assert lookup.window is None

    def test_a_failed_lookup_is_not_cached(self):
        """Caching a failure pins that day to the wrong answer for the life of
        the process — one transient exception becomes a permanent outage."""
        import trader.data.market_session as ms
        with mock.patch.object(ms, '_calendar', side_effect=RuntimeError('boom')):
            ms.session_window('ASX', dt.date(2026, 7, 27))
        assert ('ASX', dt.date(2026, 7, 27)) not in ms._schedule_cache
        # ...and the very next call, with the library working, gets it right.
        lookup = ms.session_window('ASX', dt.date(2026, 7, 27))
        assert lookup.evaluable is True
        assert lookup.window == (_utc('2026-07-27 00:00:00'),
                                 _utc('2026-07-27 06:10:00'))

    def test_a_session_row_with_unreadable_times_fails_open(self):
        """A schedule that has a row but no usable open/close is not evidence of
        a closed market."""
        import trader.data.market_session as ms
        with mock.patch.object(ms, '_utc_stamps', return_value=[]):
            lookup = ms.session_window('ASX', dt.date(2026, 7, 27))
            assert lookup.evaluable is False
            assert ms.in_session(_utc('2026-07-27 01:00:00'), 'ASX', 'ASX') is True


class TestLivePrimingMatchesTheBacktester:
    """The live frame and the backtest frame must be the same series.

    `Backtester.run` does `normalized.dropna(subset=['close'])` on exactly the
    same DB rows through exactly the same `normalize_historical`. Until
    2026-07-26 the live primer did not, so strategies ran on bars their
    validation never saw — null OHLCV placeholders the provider returns for
    future dates and holidays. Several landed ON a session boundary (GOOGL at
    08:00 UTC = pre-market open, CAT at 13:30 UTC = RTH open), which is where
    ORB reads its opening range.
    """

    def test_null_close_rows_are_dropped_from_the_primed_frame(self, tmp_path):
        import pandas as pd
        from trader.data.market_data import normalize_historical

        raw = pd.DataFrame(
            {
                'open': [10.0, None, 11.0],
                'high': [10.5, None, 11.5],
                'low': [9.5, None, 10.5],
                'close': [10.2, None, 11.2],
                'volume': [1000.0, None, 2000.0],
            },
            index=pd.DatetimeIndex(
                ['2026-07-24 13:30:00', '2026-07-24 13:31:00', '2026-07-24 13:32:00'],
                name='date'),
        )
        # What the primer now does, and what the backtester has always done.
        primed = normalize_historical(raw).dropna(subset=['close'])
        assert len(primed) == 2
        assert not primed['close'].isna().any()

    def test_the_backtester_still_drops_them(self):
        """Guard the other half of the pair: if the backtester's dropna is ever
        removed, this stops being a match and nothing else would notice."""
        source = (pathlib.Path(__file__).resolve().parent.parent
                  / 'trader' / 'simulation' / 'backtester.py').read_text()
        assert "dropna(subset=['close'])" in source

    def test_the_live_primer_still_drops_them(self):
        source = (pathlib.Path(__file__).resolve().parent.parent
                  / 'trader' / 'strategy' / 'strategy_runtime.py').read_text()
        assert "dropna(subset=['close'])" in source


class TestSessionsThatStraddleUtcMidnight:
    """The neighbouring-day loop in in_session is load-bearing, in both
    directions, for the two venues we actually trade.

    +1 is the Australian-summer case: Sydney is UTC+11 under DST, so the ASX
    session for date D opens at 23:00 UTC on D-1. Without the +1 offset, every
    ASX bar between 23:00 UTC and midnight would be wrongly refused for the
    entire southern summer — the gate would eat the first hour of every
    session, and it would look exactly like a slow feed.

    -1 is the US case: NYSE post-market runs to 00:00 UTC of the NEXT day, so
    the final minute of Friday's session carries Saturday's UTC date.
    """

    def test_asx_under_dst_opens_on_the_previous_utc_day(self):
        # Tue 2026-01-06 AEDT session = 23:00 UTC Mon Jan 5 → 05:10 UTC Jan 6.
        lookup = session_window('ASX', dt.date(2026, 1, 6))
        assert lookup.window == (_utc('2026-01-05 23:00:00'),
                                 _utc('2026-01-06 05:10:00'))
        # 23:30 UTC Monday belongs to TUESDAY's session; Monday's own session
        # closed at 05:10 UTC. Only the +1 offset can admit this bar.
        assert in_session(_utc('2026-01-05 23:30:00'), 'ASX', 'ASX') is True

    def test_us_post_market_final_minute_lands_on_the_next_utc_date(self):
        # 00:00 UTC Saturday = 20:00 ET Friday, the post-market close. Only the
        # -1 offset can admit it: Saturday itself has no session.
        assert in_session(_utc('2026-07-25 00:00:00'), 'SMART', 'NASDAQ') is True


class TestCachedAnswersKeepTheirMeaning:
    def test_a_cached_closed_day_still_refuses(self):
        """The cache-hit path must return evaluable=True — a mutant returning a
        falsy evaluable turns every CACHED closed day into fail-open, i.e. the
        gate works exactly once per (venue, day) and then stops. The second call
        here is the cache hit."""
        assert in_session(_utc('2026-07-26 18:45:00'), 'ASX', 'ASX') is False
        assert in_session(_utc('2026-07-26 18:45:00'), 'ASX', 'ASX') is False
        lookup = session_window('ASX', dt.date(2026, 7, 26))
        assert lookup.evaluable is True and lookup.window is None


class TestUtcStampsRowReading:
    def test_a_nat_column_is_skipped_not_terminal(self):
        """A NaT in one schedule column must not stop the read of the next —
        skipping means 'this column has no time today', stopping means the
        whole day reads as unreadable and fails open."""
        from trader.data.market_session import _utc_stamps
        row = pd.Series({'pre': pd.NaT, 'market_open': _utc('2026-07-24 13:30:00')})
        assert _utc_stamps(row, row.index, ('pre', 'market_open')) == \
            [_utc('2026-07-24 13:30:00')]

    def test_a_none_column_is_skipped_not_terminal(self):
        from trader.data.market_session import _utc_stamps
        row = pd.Series({'pre': None, 'market_open': _utc('2026-07-24 13:30:00')},
                        dtype=object)
        assert _utc_stamps(row, row.index, ('pre', 'market_open')) == \
            [_utc('2026-07-24 13:30:00')]

    def test_a_naive_time_is_read_as_utc(self):
        from trader.data.market_session import _utc_stamps
        row = pd.Series({'market_open': pd.Timestamp('2026-07-24 13:30:00')})
        assert _utc_stamps(row, row.index, ('market_open',)) == \
            [_utc('2026-07-24 13:30:00')]


@pytest.mark.parametrize('when,expected', [
    ('2026-09-08 11:59:59', True),
    ('2026-09-08 12:00:00', False),
    ('2026-09-08 12:59:59', False),
    ('2026-09-08 13:00:00', True),
])
def test_hong_kong_lunch_boundaries(when, expected):
    assert in_session(pd.Timestamp(when, tz='Asia/Hong_Kong'), 'SEHK') is expected


def test_session_lookup_exposes_both_hong_kong_trading_intervals():
    from trader.data.market_session import session_intervals
    lookup = session_intervals('HKEX', dt.date(2026, 9, 8))
    assert lookup.evaluable
    assert lookup.intervals == (
        (_utc('2026-09-08 01:30'), _utc('2026-09-08 04:00')),
        (_utc('2026-09-08 05:00'), _utc('2026-09-08 08:00')),
    )


class TestEarlyClose:
    """Strategies carry their own session close; on a half day it is wrong.
    `early_close` is how `mmr verify` / `preflight` say so on the day."""

    def test_the_day_after_us_thanksgiving_closes_at_one(self):
        from trader.data.market_session import early_close
        close = early_close('NYSE', dt.date(2026, 11, 27))
        assert close == pd.Timestamp('2026-11-27 13:00', tz='America/New_York')
        assert str(close.tz) == 'UTC'

    def test_an_ordinary_session_is_not_an_early_close(self):
        from trader.data.market_session import early_close
        assert early_close('NYSE', dt.date(2026, 11, 25)) is None
        assert early_close('ASX', dt.date(2026, 11, 27)) is None

    @pytest.mark.parametrize('day', [dt.date(2026, 11, 26), dt.date(2026, 11, 28)])
    def test_a_day_with_no_session_is_not_an_early_close(self, day):
        """Thanksgiving itself, and the Saturday after it."""
        from trader.data.market_session import early_close
        assert early_close('NYSE', day) is None

    def test_another_venues_early_close_is_in_its_own_time(self):
        from trader.data.market_session import early_close
        close = early_close('ASX', dt.date(2026, 12, 24))
        assert close == pd.Timestamp('2026-12-24 14:10', tz='Australia/Sydney')
        assert early_close('NYSE', dt.date(2026, 12, 24)) == pd.Timestamp('2026-12-24 13:00', tz='America/New_York')

    def test_a_calendar_that_cannot_be_read_raises_instead_of_saying_no(self):
        from trader.data import market_session

        class Broken:
            def schedule(self, **kwargs):
                raise RuntimeError('calendar unavailable')

        with mock.patch.object(market_session, '_calendar', return_value=Broken()):
            with pytest.raises(RuntimeError, match='calendar unavailable'):
                market_session.early_close('NYSE', dt.date(2026, 11, 27))

    def test_only_the_requested_day_is_asked_for(self):
        from trader.data import market_session
        asked = []
        real = market_session._calendar('NYSE')

        class Recording:
            def schedule(self, **kwargs):
                asked.append(kwargs)
                return real.schedule(**kwargs)

            def early_closes(self, schedule):
                return real.early_closes(schedule)

        with mock.patch.object(market_session, '_calendar', return_value=Recording()):
            assert market_session.early_close('NYSE', dt.date(2026, 11, 27)) is not None
        assert asked == [{'start_date': '2026-11-27', 'end_date': '2026-11-27'}]

    def test_a_naive_close_from_the_calendar_is_read_as_utc(self):
        from trader.data import market_session
        frame = pd.DataFrame({'market_close': [pd.Timestamp('2026-11-27 18:00')]},
                             index=pd.DatetimeIndex(['2026-11-27']))

        class Naive:
            def schedule(self, **kwargs):
                return frame

            def early_closes(self, schedule):
                return schedule

        with mock.patch.object(market_session, '_calendar', return_value=Naive()):
            assert market_session.early_close('NYSE', dt.date(2026, 11, 27)) == pd.Timestamp('2026-11-27 18:00', tz='UTC')

    def test_an_early_close_with_no_readable_time_raises(self):
        from trader.data import market_session
        frame = pd.DataFrame({'market_close': [pd.NaT]}, index=pd.DatetimeIndex(['2026-11-27']))

        class Unreadable:
            def schedule(self, **kwargs):
                return frame

            def early_closes(self, schedule):
                return schedule

        with mock.patch.object(market_session, '_calendar', return_value=Unreadable()):
            with pytest.raises(ValueError, match='NYSE reports an early close on 2026-11-27 with no readable time'):
                market_session.early_close('NYSE', dt.date(2026, 11, 27))


# ---------------------------------------------------------------------------
# The REGULAR session — where a market order executes (AUDIT_2026-11-14
# B1.11 / B3.5). Everything above is about which bars a strategy may SEE and
# deliberately includes extended hours; these are about when an order sent now
# would actually trade, and they exclude them.
# ---------------------------------------------------------------------------

def _et(s):
    return pd.Timestamp(s, tz='America/New_York')


def _syd(s):
    return pd.Timestamp(s, tz='Australia/Sydney')


def _hk(s):
    return pd.Timestamp(s, tz='Asia/Hong_Kong')


class _FakeCalendar:
    """A calendar that answers one fixed schedule row, for the shapes no real
    venue will hand us on demand (a break touching the open, a NaT, ...)."""

    tz = 'UTC'
    close_time = dt.time(16, 0)

    def __init__(self, **columns):
        self.asked = []
        self.frame = pd.DataFrame({k: [v] for k, v in columns.items()},
                                  index=pd.DatetimeIndex(['2026-11-25']))

    def schedule(self, **kwargs):
        self.asked.append(kwargs)
        return self.frame

    def early_closes(self, schedule):
        return schedule.iloc[0:0]


class TestRegularSession:
    def test_an_ordinary_us_day_is_half_past_nine_to_four(self):
        from trader.data.market_session import regular_session
        lookup = regular_session('NYSE', dt.date(2026, 11, 25))
        assert lookup.evaluable is True
        assert lookup.window == (_et('2026-11-25 09:30'), _et('2026-11-25 16:00'))
        assert lookup.intervals == (lookup.window,)
        assert [str(stamp.tz) for stamp in lookup.window] == ['UTC', 'UTC']

    def test_it_is_strictly_inside_the_window_the_bar_gate_uses(self):
        """The bar gate admits 04:00-20:00 ET. An order rule that used that
        window would be no rule at all."""
        from trader.data.market_session import regular_session
        day = dt.date(2026, 11, 25)
        regular = regular_session('NYSE', day).window
        extended = session_window('NYSE', day).window
        assert extended[0] < regular[0] and regular[1] < extended[1]

    def test_an_early_close_ends_it_early(self):
        from trader.data.market_session import regular_session
        lookup = regular_session('NYSE', dt.date(2026, 11, 27))
        assert lookup.window == (_et('2026-11-27 09:30'), _et('2026-11-27 13:00'))
        assert lookup.intervals == (lookup.window,)

    @pytest.mark.parametrize('day', [dt.date(2026, 11, 26), dt.date(2026, 11, 28)])
    def test_a_day_with_no_session_is_evaluable_with_no_window(self, day):
        from trader.data.market_session import SessionLookup, regular_session
        assert regular_session('NYSE', day) == SessionLookup(None, True, ())

    def test_the_asx_session_runs_to_the_closing_auction(self):
        from trader.data.market_session import regular_session
        lookup = regular_session('ASX', dt.date(2026, 11, 25))
        assert lookup.window == (_syd('2026-11-25 10:00'), _syd('2026-11-25 16:10'))

    def test_a_lunch_break_is_not_part_of_it(self):
        from trader.data.market_session import regular_session
        lookup = regular_session('HKEX', dt.date(2026, 9, 8))
        assert lookup.window == (_hk('2026-09-08 09:30'), _hk('2026-09-08 16:00'))
        assert lookup.intervals == (
            (_hk('2026-09-08 09:30'), _hk('2026-09-08 12:00')),
            (_hk('2026-09-08 13:00'), _hk('2026-09-08 16:00')),
        )

    def test_only_the_requested_day_is_asked_for(self):
        import trader.data.market_session as ms
        fake = _FakeCalendar(market_open=_utc('2026-11-25 14:30'), market_close=_utc('2026-11-25 21:00'))
        with mock.patch.object(ms, '_calendar', return_value=fake):
            ms.regular_session('NYSE', dt.date(2026, 11, 25))
        assert fake.asked == [{'start_date': '2026-11-25', 'end_date': '2026-11-25'}]

    def test_a_calendar_that_cannot_answer_is_not_evaluable_and_not_cached(self):
        import trader.data.market_session as ms
        day = dt.date(2026, 11, 25)
        with mock.patch.object(ms, '_calendar', side_effect=RuntimeError('boom')):
            assert ms.regular_session('NYSE', day) == ms.SessionLookup(None, False)
        assert ('NYSE', day) not in ms._regular_cache
        # ...and the next call, with the library working, gets it right.
        assert ms.regular_session('NYSE', day).window == (_et('2026-11-25 09:30'), _et('2026-11-25 16:00'))

    def test_an_answer_is_remembered(self):
        import trader.data.market_session as ms
        day = dt.date(2026, 11, 25)
        first = ms.regular_session('NYSE', day)
        assert ms._regular_cache[('NYSE', day)] is first
        with mock.patch.object(ms, '_calendar', side_effect=RuntimeError('boom')):
            assert ms.regular_session('NYSE', day) is first

    def test_the_cache_is_per_venue_and_per_day(self):
        import trader.data.market_session as ms
        ms.regular_session('NYSE', dt.date(2026, 11, 25))
        assert ms.regular_session('NYSE', dt.date(2026, 11, 27)).window[1] == _et('2026-11-27 13:00')
        assert ms.regular_session('ASX', dt.date(2026, 11, 25)).window[0] == _syd('2026-11-25 10:00')

    def test_a_session_row_without_readable_hours_is_not_evaluable(self):
        import trader.data.market_session as ms
        day = dt.date(2026, 11, 25)
        with mock.patch.object(ms, '_utc_stamps', return_value=[]):
            assert ms.regular_session('NYSE', day) == ms.SessionLookup(None, False)
        assert ('NYSE', day) not in ms._regular_cache

    @pytest.mark.parametrize('columns', [
        dict(market_open=pd.NaT, market_close=_utc('2026-11-25 21:00')),
        dict(market_open=_utc('2026-11-25 14:30'), market_close=pd.NaT),
        # A close that is not after the open is not a session.
        dict(market_open=_utc('2026-11-25 21:00'), market_close=_utc('2026-11-25 21:00')),
        dict(market_open=_utc('2026-11-25 21:00'), market_close=_utc('2026-11-25 14:30')),
    ])
    def test_unusable_hours_are_not_evaluable(self, columns):
        import trader.data.market_session as ms
        with mock.patch.object(ms, '_calendar', return_value=_FakeCalendar(**columns)):
            assert ms.regular_session('NYSE', dt.date(2026, 11, 25)) == ms.SessionLookup(None, False)

    def test_a_break_inside_the_session_splits_it(self):
        import trader.data.market_session as ms
        fake = _FakeCalendar(market_open=_utc('2026-11-25 01:00'), market_close=_utc('2026-11-25 07:00'),
                             break_start=_utc('2026-11-25 03:00'), break_end=_utc('2026-11-25 04:00'))
        with mock.patch.object(ms, '_calendar', return_value=fake):
            lookup = ms.regular_session('X', dt.date(2026, 11, 25))
        assert lookup.window == (_utc('2026-11-25 01:00'), _utc('2026-11-25 07:00'))
        assert lookup.intervals == ((_utc('2026-11-25 01:00'), _utc('2026-11-25 03:00')),
                                    (_utc('2026-11-25 04:00'), _utc('2026-11-25 07:00')))

    @pytest.mark.parametrize('break_start, break_end', [
        ('2026-11-25 01:00', '2026-11-25 04:00'),      # starts AT the open
        ('2026-11-25 03:00', '2026-11-25 07:00'),      # ends AT the close
        ('2026-11-25 03:00', '2026-11-25 03:00'),      # empty
        ('2026-11-25 04:00', '2026-11-25 03:00'),      # inverted
        ('2026-11-25 00:30', '2026-11-25 04:00'),      # starts before the open
        ('2026-11-25 03:00', '2026-11-25 07:30'),      # ends after the close
    ])
    def test_a_break_that_is_not_strictly_inside_is_ignored(self, break_start, break_end):
        """Splitting on one would produce an empty or backwards interval."""
        import trader.data.market_session as ms
        fake = _FakeCalendar(market_open=_utc('2026-11-25 01:00'), market_close=_utc('2026-11-25 07:00'),
                             break_start=_utc(break_start), break_end=_utc(break_end))
        with mock.patch.object(ms, '_calendar', return_value=fake):
            lookup = ms.regular_session('X', dt.date(2026, 11, 25))
        assert lookup.intervals == ((_utc('2026-11-25 01:00'), _utc('2026-11-25 07:00')),)

    @pytest.mark.parametrize('columns', [
        dict(break_start=pd.NaT, break_end=_utc('2026-11-25 04:00')),
        dict(break_start=_utc('2026-11-25 03:00'), break_end=pd.NaT),
        dict(break_start=_utc('2026-11-25 03:00')),
        dict(break_end=_utc('2026-11-25 04:00')),
    ])
    def test_half_a_break_is_no_break(self, columns):
        import trader.data.market_session as ms
        fake = _FakeCalendar(market_open=_utc('2026-11-25 01:00'), market_close=_utc('2026-11-25 07:00'), **columns)
        with mock.patch.object(ms, '_calendar', return_value=fake):
            lookup = ms.regular_session('X', dt.date(2026, 11, 25))
        assert lookup == ms.SessionLookup((_utc('2026-11-25 01:00'), _utc('2026-11-25 07:00')), True,
                                          ((_utc('2026-11-25 01:00'), _utc('2026-11-25 07:00')),))


class TestRegularSessionRefusal:
    """`order_at` is the instant the order goes out. Half-open: the open is in,
    the close is out."""

    @pytest.mark.parametrize('calendar_name', [None, ''])
    def test_a_venue_with_no_calendar_is_outside_the_rule(self, calendar_name):
        from trader.data.market_session import regular_session_refusal
        assert regular_session_refusal(_et('2026-11-28 03:00'), calendar_name) is None

    @pytest.mark.parametrize('when, allowed', [
        ('2026-11-25 09:29:59', False),
        ('2026-11-25 09:30:00', True),
        ('2026-11-25 12:00:00', True),
        ('2026-11-25 15:59:59', True),
        ('2026-11-25 16:00:00', False),
        ('2026-11-25 19:00:00', False),      # post-market: in the bar gate, not here
        ('2026-11-25 04:30:00', False),      # pre-market: likewise
    ])
    def test_us_boundaries(self, when, allowed):
        from trader.data.market_session import regular_session_refusal
        assert (regular_session_refusal(_et(when), 'NYSE') is None) is allowed

    @pytest.mark.parametrize('when, allowed', [
        ('2026-11-27 12:59:59', True),
        ('2026-11-27 13:00:00', False),
        ('2026-11-27 15:00:00', False),
    ])
    def test_an_early_close_is_the_close(self, when, allowed):
        from trader.data.market_session import regular_session_refusal
        assert (regular_session_refusal(_et(when), 'NYSE') is None) is allowed

    @pytest.mark.parametrize('when, allowed', [
        ('2026-09-08 11:59:59', True),
        ('2026-09-08 12:00:00', False),
        ('2026-09-08 12:59:59', False),
        ('2026-09-08 13:00:00', True),
    ])
    def test_a_lunch_break_is_outside(self, when, allowed):
        from trader.data.market_session import regular_session_refusal
        assert (regular_session_refusal(_hk(when), 'HKEX') is None) is allowed

    def test_the_reasons_name_the_venue_the_time_and_what_ib_does(self):
        from trader.data.market_session import regular_session_refusal
        assert regular_session_refusal(_et('2026-11-25 08:15'), 'NYSE') == (
            'NYSE regular session has not opened (09:30); '
            'a market order sent at 08:15 America/New_York is held until it does')
        assert regular_session_refusal(_et('2026-11-27 13:30'), 'NYSE') == (
            'NYSE regular session closed at 13:00; '
            'a market order sent at 13:30 America/New_York is held until the next session')
        assert regular_session_refusal(_et('2026-11-26 11:00'), 'NYSE') == (
            'NYSE has no regular session on 2026-11-26: a market order sent now is held until the next one')
        # (the library's Hong Kong calendar names its zone Asia/Shanghai: same clock)
        assert regular_session_refusal(_hk('2026-09-08 12:30'), 'HKEX') == (
            'HKEX is in its trading break at 12:30 Asia/Shanghai; '
            'a market order sent now is held until it ends')

    def test_the_instant_is_what_counts_not_the_zone_it_is_written_in(self):
        from trader.data.market_session import regular_session_refusal
        assert regular_session_refusal(_utc('2026-11-25 15:00'), 'NYSE') is None            # 10:00 ET
        assert regular_session_refusal(_utc('2026-11-25 21:00'), 'NYSE') is not None        # 16:00 ET
        assert regular_session_refusal(_syd('2026-11-26 02:00'), 'NYSE') is None            # 10:00 ET the day before

    def test_the_session_day_is_the_venues_not_utcs(self):
        """23:30 UTC on the 24th is 10:30 on the 25th in Sydney. Asking for the
        24th's session would find it already over."""
        from trader.data.market_session import regular_session_refusal
        assert regular_session_refusal(_utc('2026-11-24 23:30'), 'ASX') is None
        # ...and 21:00 UTC on a Friday is Saturday morning there: no session.
        assert regular_session_refusal(_utc('2026-11-27 23:30'), 'ASX') == (
            'ASX has no regular session on 2026-11-28: a market order sent now is held until the next one')

    def test_a_timestamp_without_a_zone_is_venue_wall_clock(self):
        """The reading `check_time_exit` gives it. Read as UTC, 10:30 would be
        21:30 in Sydney (shut) and 05:30 in New York (shut)."""
        from trader.data.market_session import regular_session_refusal
        assert regular_session_refusal(pd.Timestamp('2026-11-25 10:30'), 'ASX') is None
        assert regular_session_refusal(pd.Timestamp('2026-11-25 10:30'), 'NYSE') is None
        assert regular_session_refusal(pd.Timestamp('2026-11-25 16:00'), 'NYSE') is not None
        assert regular_session_refusal(dt.datetime(2026, 11, 25, 10, 30), 'NYSE') is None
        assert regular_session_refusal('2026-11-25 10:30', 'NYSE') is None

    @pytest.mark.parametrize('order_at', [None, pd.NaT, 'not a time', float('nan')])
    def test_an_unreadable_instant_is_refused(self, order_at):
        from trader.data.market_session import regular_session_refusal
        reason = regular_session_refusal(order_at, 'NYSE')
        assert reason is not None
        assert reason.startswith('regular session not evaluable for NYSE (')
        assert reason.endswith(' — refusing to open (fail-closed)')

    def test_a_calendar_that_cannot_be_loaded_is_refused(self):
        import trader.data.market_session as ms
        with mock.patch.object(ms, '_calendar', side_effect=RuntimeError('boom')):
            assert ms.regular_session_refusal(_et('2026-11-25 10:30'), 'NYSE') == (
                'regular session not evaluable for NYSE (RuntimeError: boom) — refusing to open (fail-closed)')

    def test_a_session_that_cannot_be_read_is_refused(self):
        """The bar gate fails OPEN on this. This rule guards opens only, where
        refusing is the benign direction."""
        import trader.data.market_session as ms
        with mock.patch.object(ms, 'regular_session', return_value=ms.SessionLookup(None, False)) as asked:
            assert ms.regular_session_refusal(_et('2026-11-25 10:30'), 'NYSE') == (
                'regular session not evaluable for NYSE on 2026-11-25 — refusing to open (fail-closed)')
        asked.assert_called_once_with('NYSE', dt.date(2026, 11, 25))


class TestListingCalendar:
    @pytest.mark.parametrize('exchange, primary, expected', [
        ('NASDAQ', '', 'NASDAQ'),
        ('ARCA', '', 'NYSE'),
        ('ASX', '', 'ASX'),
        ('SMART', 'NASDAQ', 'NASDAQ'),
        ('SMART', 'ASX', 'ASX'),
        ('ASX', 'SMART', 'ASX'),             # a router in the primary slot is skipped, not believed
        ('ASX', 'NYSE', 'NYSE'),             # primary wins over the exchange field
        (' asx ', '', 'ASX'),
        ('', ' nasdaq ', 'NASDAQ'),
        ('XYZ', 'ASX', 'ASX'),
        ('ASX', 'XYZ', 'ASX'),               # an unmapped primary falls through to the exchange
    ])
    def test_the_listing_venue_decides(self, exchange, primary, expected):
        from trader.data.market_session import listing_calendar
        assert listing_calendar(exchange, primary) == expected

    @pytest.mark.parametrize('exchange, primary', [
        ('SMART', ''), ('smart', ''), ('', 'SMART'), ('SMART', 'SMART'),
        ('', ''), ('XYZ', ''), ('XYZ', 'QQQ'), (None, None), ('SMART', None),
    ])
    def test_a_router_or_an_unknown_venue_is_no_listing(self, exchange, primary):
        """`calendar_name_for('SMART')` answers NYSE so the bar gate has a
        default it can fail open on. An ASX name routed SMART is not on New
        York hours, and a rule that refuses must not guess."""
        from trader.data.market_session import listing_calendar
        assert listing_calendar(exchange, primary) is None

    def test_smart_is_a_router_here_and_a_default_for_the_bar_gate(self):
        from trader.data.market_session import listing_calendar
        assert calendar_name_for('SMART') == 'NYSE'
        assert listing_calendar('SMART') is None

    def test_every_router_is_skipped_even_if_someone_maps_it(self):
        import trader.data.market_session as ms
        assert ms._ROUTERS == frozenset({'SMART', 'IBKRATS', 'BEST'})
        mapped = dict(ms._EXCHANGE_CALENDARS, IBKRATS='NYSE', BEST='NYSE')
        with mock.patch.object(ms, '_EXCHANGE_CALENDARS', mapped):
            for router in ('SMART', 'IBKRATS', 'BEST'):
                assert ms.listing_calendar(router) is None
                assert ms.listing_calendar(router, 'ASX') == 'ASX'


class TestIntradayOpenRefusal:
    """The order goes out when the bar COMPLETES: the instant judged is the
    bar's start plus its interval."""

    @pytest.mark.parametrize('bar, allowed', [
        ('2026-11-25 09:28', False),     # order at 09:29
        ('2026-11-25 09:29', True),      # order at 09:30:00 — the open
        ('2026-11-25 15:58', True),      # order at 15:59
        ('2026-11-25 15:59', False),     # order at 16:00:00 — the close
        ('2026-11-27 12:58', True),
        ('2026-11-27 12:59', False),     # the half day's last bar
        ('2026-11-27 14:00', False),     # after a 13:00 close, still inside the bar gate
    ])
    def test_one_minute_bars(self, bar, allowed):
        from trader.data.market_session import intraday_open_refusal
        assert (intraday_open_refusal(_et(bar), 60.0, 'NASDAQ') is None) is allowed

    @pytest.mark.parametrize('bar, seconds, allowed', [
        ('2026-11-25 15:50', 300, True),         # order at 15:55
        ('2026-11-25 15:55', 300, False),        # order at 16:00
        ('2026-11-25 14:00', 3600, True),
        ('2026-11-25 15:00', 3600, False),
        ('2026-11-25 09:00', 1800, True),        # a pre-market bar that completes AT the open
        ('2026-11-25 15:59:30', 29.5, True),
        ('2026-11-25 15:59:30', 30, False),
    ])
    def test_the_interval_is_added_to_the_bar(self, bar, seconds, allowed):
        from trader.data.market_session import intraday_open_refusal
        assert (intraday_open_refusal(_et(bar), seconds, 'NYSE') is None) is allowed

    def test_the_reason_is_the_session_rules(self):
        from trader.data.market_session import intraday_open_refusal
        assert intraday_open_refusal(_et('2026-11-27 12:59'), 60, 'NASDAQ') == (
            'NASDAQ regular session closed at 13:00; '
            'a market order sent at 13:00 America/New_York is held until the next session')

    @pytest.mark.parametrize('seconds', [0, 0.0, -60, float('nan'), float('inf'), float('-inf'), None, 'soon', ''])
    def test_an_interval_that_is_not_a_positive_number_is_refused(self, seconds):
        """Without it there is no order instant. In session or not."""
        from trader.data.market_session import intraday_open_refusal
        assert intraday_open_refusal(_et('2026-11-25 11:00'), seconds, 'NASDAQ') == (
            'bar interval unknown — cannot tell when the order would be sent; refusing to open (fail-closed)')

    def test_a_numeric_string_interval_is_a_number(self):
        from trader.data.market_session import intraday_open_refusal
        assert intraday_open_refusal(_et('2026-11-25 15:58'), '60', 'NASDAQ') is None
        assert intraday_open_refusal(_et('2026-11-25 15:59'), '60', 'NASDAQ') is not None

    def test_a_daily_bar_is_outside_the_rule(self):
        """Decided after the close by design; it fills at the next open."""
        from trader.data.market_session import intraday_open_refusal
        saturday = _et('2026-11-28 00:00')
        assert intraday_open_refusal(saturday, 86400, 'NASDAQ') is None
        assert intraday_open_refusal(saturday, 7 * 86400, 'NASDAQ') is None
        assert intraday_open_refusal(saturday, 86399, 'NASDAQ') is not None

    @pytest.mark.parametrize('sec_type', ['CASH', 'CRYPTO', 'cash', ' Crypto '])
    def test_a_round_the_clock_instrument_is_outside_the_rule(self, sec_type):
        from trader.data.market_session import intraday_open_refusal
        assert intraday_open_refusal(_et('2026-11-28 03:00'), 60, 'NASDAQ', sec_type=sec_type) is None

    @pytest.mark.parametrize('sec_type', ['STK', '', None, 'FUT', 'CASHX'])
    def test_everything_else_is_inside_it(self, sec_type):
        from trader.data.market_session import intraday_open_refusal
        assert intraday_open_refusal(_et('2026-11-28 03:00'), 60, 'NASDAQ', sec_type=sec_type) is not None

    @pytest.mark.parametrize('exchange, primary', [('SMART', ''), ('XYZ', ''), ('', ''), ('', 'SMART')])
    def test_an_unknown_listing_is_outside_the_rule(self, exchange, primary):
        from trader.data.market_session import intraday_open_refusal
        assert intraday_open_refusal(_et('2026-11-28 03:00'), 60, exchange, primary) is None

    def test_the_primary_exchange_names_the_venue(self):
        from trader.data.market_session import intraday_open_refusal
        sydney_morning = _syd('2026-11-25 10:30')          # 18:30 the evening before in New York
        assert intraday_open_refusal(sydney_morning, 60, 'SMART', 'ASX') is None
        assert intraday_open_refusal(sydney_morning, 60, 'SMART', 'NASDAQ') is not None
        assert intraday_open_refusal(sydney_morning, 60, 'ASX') is None

    def test_a_bar_without_a_zone_is_venue_wall_clock(self):
        from trader.data.market_session import intraday_open_refusal
        assert intraday_open_refusal(pd.Timestamp('2026-11-25 10:39'), 60.0, 'ASX') is None
        assert intraday_open_refusal(pd.Timestamp('2026-11-25 16:09'), 60.0, 'ASX') is not None

    @pytest.mark.parametrize('bar', ['not a time', object()])
    def test_a_bar_that_is_not_a_time_is_refused(self, bar):
        from trader.data.market_session import intraday_open_refusal
        reason = intraday_open_refusal(bar, 60, 'NASDAQ')
        assert reason is not None
        assert reason.startswith('regular session not evaluable for NASDAQ (')
        assert reason.endswith(' — refusing to open (fail-closed)')

    @pytest.mark.parametrize('bar', [None, pd.NaT])
    def test_a_missing_bar_time_is_refused(self, bar):
        from trader.data.market_session import intraday_open_refusal
        reason = intraday_open_refusal(bar, 60, 'NASDAQ')
        assert reason is not None and 'not evaluable for NASDAQ' in reason


class TestCloseByOn:
    """A holding that asked to be flat by 15:45 asked for fifteen minutes
    before the bell. On a 13:00 close that is 12:45."""

    def test_a_us_half_day_moves_it_by_three_hours(self):
        from trader.data.market_session import close_by_on
        assert close_by_on(dt.time(15, 45), 'NYSE', dt.date(2026, 11, 27)) == dt.time(12, 45)
        assert close_by_on(dt.time(15, 59, 30), 'NASDAQ', dt.date(2026, 11, 27)) == dt.time(12, 59, 30)
        assert close_by_on(dt.time(10, 30), 'NYSE', dt.date(2026, 11, 27)) == dt.time(7, 30)

    def test_another_venue_moves_by_its_own_amount(self):
        """ASX closes 14:10 instead of 16:10 on Christmas Eve."""
        from trader.data.market_session import close_by_on
        assert close_by_on(dt.time(15, 45), 'ASX', dt.date(2026, 12, 24)) == dt.time(13, 45)

    @pytest.mark.parametrize('day', [
        dt.date(2026, 11, 25),       # an ordinary session
        dt.date(2026, 11, 26),       # Thanksgiving: no session
        dt.date(2026, 11, 28),       # Saturday
    ])
    def test_any_other_day_leaves_it_alone(self, day):
        from trader.data.market_session import close_by_on
        assert close_by_on(dt.time(15, 45), 'NYSE', day) == dt.time(15, 45)

    @pytest.mark.parametrize('calendar_name', [None, ''])
    def test_a_venue_with_no_calendar_leaves_it_alone(self, calendar_name):
        import trader.data.market_session as ms
        with mock.patch.object(ms, 'early_close', side_effect=AssertionError('must not be asked')):
            assert ms.close_by_on(dt.time(15, 45), calendar_name, dt.date(2026, 11, 27)) == dt.time(15, 45)

    @pytest.mark.parametrize('close_by, expected', [
        (dt.time(3, 0, 1), dt.time(0, 0, 1)),
        (dt.time(3, 0), dt.time(0, 0)),          # lands exactly on midnight
        (dt.time(2, 59, 59), dt.time(0, 0)),     # would be the day before
        (dt.time(0, 0), dt.time(0, 0)),
    ])
    def test_it_never_moves_before_midnight(self, close_by, expected):
        from trader.data.market_session import close_by_on
        assert close_by_on(close_by, 'NYSE', dt.date(2026, 11, 27)) == expected

    @pytest.mark.parametrize('close_utc', ['2026-11-25 16:00', '2026-11-25 17:30'])
    def test_it_never_moves_later(self, close_utc):
        """A calendar reporting an "early" close at or after the usual one."""
        import trader.data.market_session as ms
        with mock.patch.object(ms, '_calendar', return_value=_FakeCalendar()), \
                mock.patch.object(ms, 'early_close', return_value=_utc(close_utc)):
            assert ms.close_by_on(dt.time(15, 45), 'X', dt.date(2026, 11, 25)) == dt.time(15, 45)

    def test_the_shift_is_measured_on_the_venues_clock(self):
        """Usual close 16:00 in a zone five hours behind UTC; today's is 18:00
        UTC = 13:00 there. Compared without converting, 18:00 looks LATER than
        16:00 and nothing would move."""
        import trader.data.market_session as ms

        class Eastern(_FakeCalendar):
            tz = 'Etc/GMT+5'
            close_time = dt.time(16, 0)

        with mock.patch.object(ms, '_calendar', return_value=Eastern()), \
                mock.patch.object(ms, 'early_close', return_value=_utc('2026-11-27 18:00')):
            assert ms.close_by_on(dt.time(15, 45), 'X', dt.date(2026, 11, 27)) == dt.time(12, 45)

    def test_a_usual_close_that_carries_a_zone_is_read_as_wall_clock(self):
        import zoneinfo
        import trader.data.market_session as ms

        class Aware(_FakeCalendar):
            tz = 'Etc/GMT+5'
            close_time = dt.time(16, 0, tzinfo=zoneinfo.ZoneInfo('Etc/GMT+5'))

        with mock.patch.object(ms, '_calendar', return_value=Aware()), \
                mock.patch.object(ms, 'early_close', return_value=_utc('2026-11-27 18:00')):
            assert ms.close_by_on(dt.time(15, 45), 'X', dt.date(2026, 11, 27)) == dt.time(12, 45)

    def test_the_day_asked_about_is_the_day_given(self):
        import trader.data.market_session as ms
        with mock.patch.object(ms, 'early_close', return_value=None) as asked:
            ms.close_by_on(dt.time(15, 45), 'NYSE', dt.date(2026, 11, 27))
        asked.assert_called_once_with('NYSE', dt.date(2026, 11, 27))

    def test_a_calendar_that_cannot_answer_raises(self):
        """The caller keeps the unshifted time; this function does not pretend
        the day is ordinary."""
        import trader.data.market_session as ms
        with mock.patch.object(ms, 'early_close', side_effect=RuntimeError('calendar unavailable')):
            with pytest.raises(RuntimeError, match='calendar unavailable'):
                ms.close_by_on(dt.time(15, 45), 'NYSE', dt.date(2026, 11, 27))


class TestInRegularSession:
    """The three-valued reading. An exit is delayed only by a session KNOWN to
    be shut, so "unknown" must not come back as "outside"."""

    @pytest.mark.parametrize('when, expected', [
        ('2026-11-25 09:29:59', False),
        ('2026-11-25 09:30:00', True),
        ('2026-11-25 15:59:59', True),
        ('2026-11-25 16:00:00', False),
        ('2026-11-27 13:00:00', False),      # early close
        ('2026-11-26 11:00:00', False),      # holiday
    ])
    def test_it_agrees_with_the_refusal(self, when, expected):
        from trader.data.market_session import in_regular_session, regular_session_refusal
        assert in_regular_session(_et(when), 'NYSE') is expected
        assert (regular_session_refusal(_et(when), 'NYSE') is None) is expected

    def test_a_lunch_break_is_outside(self):
        from trader.data.market_session import in_regular_session
        assert in_regular_session(_hk('2026-09-08 12:30'), 'HKEX') is False
        assert in_regular_session(_hk('2026-09-08 13:00'), 'HKEX') is True

    @pytest.mark.parametrize('calendar_name', [None, ''])
    def test_no_calendar_is_unknown_not_outside(self, calendar_name):
        from trader.data.market_session import in_regular_session
        assert in_regular_session(_et('2026-11-28 03:00'), calendar_name) is None

    @pytest.mark.parametrize('instant', [None, pd.NaT, 'not a time'])
    def test_an_unreadable_instant_is_unknown(self, instant):
        from trader.data.market_session import in_regular_session
        assert in_regular_session(instant, 'NYSE') is None

    def test_a_calendar_that_cannot_answer_is_unknown(self):
        import trader.data.market_session as ms
        with mock.patch.object(ms, '_calendar', side_effect=RuntimeError('boom')):
            assert ms.in_regular_session(_et('2026-11-25 10:30'), 'NYSE') is None
        with mock.patch.object(ms, 'regular_session', return_value=ms.SessionLookup(None, False)):
            assert ms.in_regular_session(_et('2026-11-25 10:30'), 'NYSE') is None

    def test_a_timestamp_without_a_zone_is_venue_wall_clock(self):
        from trader.data.market_session import in_regular_session
        assert in_regular_session(pd.Timestamp('2026-11-25 10:30'), 'ASX') is True
        assert in_regular_session(pd.Timestamp('2026-11-25 16:10'), 'ASX') is False


class TestOrderCalendar:
    @pytest.mark.parametrize('exchange, primary, sec_type, expected', [
        ('SMART', 'NASDAQ', 'STK', 'NASDAQ'),
        ('ASX', 'ASX', 'STK', 'ASX'),
        ('ARCA', '', '', 'NYSE'),
        ('NASDAQ', 'NASDAQ', None, 'NASDAQ'),
        ('NASDAQ', 'NASDAQ', 'FUT', 'NASDAQ'),
        ('SMART', '', 'STK', None),                  # a router is not a listing
        ('XYZ', '', 'STK', None),
        ('NASDAQ', 'NASDAQ', 'CASH', None),          # round the clock, whatever the venue says
        ('NASDAQ', 'NASDAQ', 'CRYPTO', None),
        ('NASDAQ', 'NASDAQ', ' cash ', None),
        ('IDEALPRO', '', 'CASH', None),
    ])
    def test_it_is_the_listing_unless_the_instrument_never_closes(self, exchange, primary, sec_type, expected):
        from trader.data.market_session import order_calendar
        assert order_calendar(exchange, primary, sec_type) == expected

    def test_the_security_type_is_optional(self):
        from trader.data.market_session import order_calendar
        assert order_calendar('ASX') == 'ASX'
        assert order_calendar('SMART', 'ASX') == 'ASX'


class TestBarOpenRefusal:
    """What the executor and the backtester both call, with a calendar name."""

    @pytest.mark.parametrize('bar, seconds, allowed', [
        ('2026-11-25 09:29', 60, True),
        ('2026-11-25 15:58', 60, True),
        ('2026-11-25 15:59', 60, False),
        ('2026-11-25 15:55', 300, False),
        ('2026-11-27 12:59', 60, False),
        ('2026-11-27 12:58', 60, True),
    ])
    def test_the_order_goes_out_when_the_bar_completes(self, bar, seconds, allowed):
        from trader.data.market_session import bar_open_refusal
        assert (bar_open_refusal(_et(bar), seconds, 'NYSE') is None) is allowed

    @pytest.mark.parametrize('calendar_name', [None, ''])
    def test_no_calendar_is_outside_the_rule(self, calendar_name):
        from trader.data.market_session import bar_open_refusal
        assert bar_open_refusal(_et('2026-11-28 03:00'), 60, calendar_name) is None

    @pytest.mark.parametrize('calendar_name', [None, '', 'NYSE'])
    def test_an_unknown_interval_is_refused_whatever_the_venue(self, calendar_name):
        from trader.data.market_session import bar_open_refusal
        assert bar_open_refusal(_et('2026-11-25 11:00'), 0, calendar_name) == (
            'bar interval unknown — cannot tell when the order would be sent; refusing to open (fail-closed)')

    def test_a_daily_bar_is_outside_the_rule(self):
        from trader.data.market_session import bar_open_refusal
        assert bar_open_refusal(_et('2026-11-28 00:00'), 86400, 'NYSE') is None
        assert bar_open_refusal(_et('2026-11-28 00:00'), 86399.5, 'NYSE') is not None

    def test_it_is_what_the_venue_form_resolves_to(self):
        from trader.data.market_session import bar_open_refusal, intraday_open_refusal
        for bar in ('2026-11-25 15:58', '2026-11-25 15:59', '2026-11-27 13:30'):
            assert intraday_open_refusal(_et(bar), 60, 'SMART', 'NASDAQ', 'STK') == \
                bar_open_refusal(_et(bar), 60, 'NASDAQ')


class TestEarlyCloseShift:
    def test_a_us_half_day_ends_three_hours_early(self):
        from trader.data.market_session import early_close_shift
        assert early_close_shift('NYSE', dt.date(2026, 11, 27)) == dt.timedelta(hours=3)
        assert early_close_shift('NASDAQ', dt.date(2026, 12, 24)) == dt.timedelta(hours=3)

    def test_the_asx_christmas_eve_ends_two_hours_early(self):
        from trader.data.market_session import early_close_shift
        assert early_close_shift('ASX', dt.date(2026, 12, 24)) == dt.timedelta(hours=2)

    @pytest.mark.parametrize('day', [dt.date(2026, 11, 25), dt.date(2026, 11, 26), dt.date(2026, 11, 28)])
    def test_any_other_day_is_zero(self, day):
        from trader.data.market_session import early_close_shift
        assert early_close_shift('NYSE', day) == dt.timedelta(0)

    @pytest.mark.parametrize('close_utc', ['2026-11-25 16:00', '2026-11-25 17:30'])
    def test_it_is_never_negative(self, close_utc):
        import trader.data.market_session as ms
        with mock.patch.object(ms, '_calendar', return_value=_FakeCalendar()), \
                mock.patch.object(ms, 'early_close', return_value=_utc(close_utc)):
            assert ms.early_close_shift('X', dt.date(2026, 11, 25)) == dt.timedelta(0)

    def test_an_answer_is_remembered(self):
        """The backtester asks once per bar; the calendar is asked once per day."""
        import trader.data.market_session as ms
        day = dt.date(2026, 11, 27)
        assert ms.early_close_shift('NYSE', day) == dt.timedelta(hours=3)
        assert ms._early_shift_cache[('NYSE', day)] == dt.timedelta(hours=3)
        with mock.patch.object(ms, 'early_close', side_effect=AssertionError('asked twice')):
            assert ms.early_close_shift('NYSE', day) == dt.timedelta(hours=3)
            assert ms.close_by_on(dt.time(15, 45), 'NYSE', day) == dt.time(12, 45)

    def test_an_ordinary_day_is_remembered_too(self):
        import trader.data.market_session as ms
        day = dt.date(2026, 11, 25)
        ms.early_close_shift('NYSE', day)
        assert ms._early_shift_cache[('NYSE', day)] == dt.timedelta(0)

    def test_the_memory_is_per_venue_and_per_day(self):
        import trader.data.market_session as ms
        assert ms.early_close_shift('NYSE', dt.date(2026, 11, 27)) == dt.timedelta(hours=3)
        assert ms.early_close_shift('NYSE', dt.date(2026, 11, 25)) == dt.timedelta(0)
        assert ms.early_close_shift('ASX', dt.date(2026, 11, 27)) == dt.timedelta(0)

    def test_a_failure_raises_and_is_not_remembered(self):
        import trader.data.market_session as ms
        day = dt.date(2026, 11, 27)
        with mock.patch.object(ms, 'early_close', side_effect=RuntimeError('calendar unavailable')):
            with pytest.raises(RuntimeError, match='calendar unavailable'):
                ms.early_close_shift('NYSE', day)
        assert ('NYSE', day) not in ms._early_shift_cache
        assert ms.early_close_shift('NYSE', day) == dt.timedelta(hours=3)


class TestWhatAFailureSays:
    """Found by mutation testing the regular-session code (2026-11-15): the
    decisions were pinned, the words were not. A refusal is read by an
    operator and a failed lookup by whoever is asking why opens stopped."""

    def test_a_failed_lookup_names_the_venue_the_day_and_the_cause(self, caplog):
        import logging as std_logging
        import trader.data.market_session as ms
        with caplog.at_level(std_logging.WARNING), \
                mock.patch.object(ms, '_calendar', side_effect=RuntimeError('boom')):
            ms.regular_session('NYSE', dt.date(2026, 11, 25))
        messages = [record.getMessage() for record in caplog.records if 'regular' in record.getMessage()]
        assert messages == [
            'market_session: could not read the regular NYSE session for 2026-11-25 (boom) — NOT caching']

    def test_unreadable_hours_name_the_columns_that_were_there(self, caplog):
        import logging as std_logging
        import trader.data.market_session as ms
        fake = _FakeCalendar(market_open=pd.NaT, market_close=_utc('2026-11-25 21:00'))
        with caplog.at_level(std_logging.WARNING), mock.patch.object(ms, '_calendar', return_value=fake):
            ms.regular_session('NYSE', dt.date(2026, 11, 25))
        messages = [record.getMessage() for record in caplog.records if 'regular' in record.getMessage()]
        assert messages == [
            "market_session: could not read the regular NYSE session for 2026-11-25 "
            "(unreadable regular hours in ['market_open', 'market_close']) — NOT caching"]

    @pytest.mark.parametrize('instant', [None, pd.NaT])
    def test_a_missing_instant_is_named_as_that(self, instant):
        from trader.data.market_session import regular_session_refusal
        assert regular_session_refusal(instant, 'NYSE') == (
            'regular session not evaluable for NYSE (ValueError: no timestamp) — refusing to open (fail-closed)')

    def test_a_bar_that_is_not_a_time_names_what_went_wrong(self):
        from trader.data.market_session import bar_open_refusal
        try:
            pd.Timestamp('not a time')
        except Exception as ex:                          # whatever pandas calls it in this version
            expected = f'({type(ex).__name__}: {ex})'
        reason = bar_open_refusal('not a time', 60, 'NASDAQ')
        assert reason == f'regular session not evaluable for NASDAQ {expected} — refusing to open (fail-closed)'
