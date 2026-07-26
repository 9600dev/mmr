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

    def _clear(self):
        import trader.data.market_session as ms
        ms._schedule_cache.clear()

    def test_an_exploding_calendar_admits_the_bar(self):
        import trader.data.market_session as ms
        self._clear()
        with mock.patch.object(ms, '_calendar', side_effect=RuntimeError('boom')):
            assert ms.in_session(_utc('2026-07-27 01:00:00'), 'ASX', 'ASX') is True

    def test_an_exploding_calendar_reports_not_evaluable(self):
        import trader.data.market_session as ms
        self._clear()
        with mock.patch.object(ms, '_calendar', side_effect=RuntimeError('boom')):
            lookup = ms.session_window('ASX', dt.date(2026, 7, 27))
        assert lookup.evaluable is False
        assert lookup.window is None

    def test_a_failed_lookup_is_not_cached(self):
        """Caching a failure pins that day to the wrong answer for the life of
        the process — one transient exception becomes a permanent outage."""
        import trader.data.market_session as ms
        self._clear()
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
        self._clear()
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
