"""A daily (or larger) bar for a session that has not closed is never stored.

Field finding 2026-11-05: `mmr history ib --symbol AAPL --bar_size "1 day"
--prev_days 15`, run during regular hours, stored the still-forming session as
that day's complete bar (volume 9.8M at 14:30 ET). missing() then counts the
day as covered, so the truncated bar is never re-fetched — it is permanent.

The guard is at the write chokepoint (Data.write), so these tests drive it
directly AND through each downloader in DataService (IB, Massive, TwelveData)
with the vendor replaced by a fake that, like the real vendors, happily
returns the in-progress session.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import logging

import exchange_calendars
import pandas as pd
import pytest
from ib_async import Contract, ContractDetails

import trader.data.session_guard as session_guard
from trader.data.data_access import SecurityDefinition, TickStorage
from trader.data.session_guard import unclosed_session_mask
from trader.data.store import DateRange
from trader.objects import BarSize

UTC = dt.timezone.utc
DURING = dt.datetime(2026, 11, 5, 19, 30, tzinfo=UTC)   # Thu 14:30 ET
AFTER = dt.datetime(2026, 11, 5, 21, 30, tzinfo=UTC)    # Thu 16:30 ET
BEFORE_OPEN = dt.datetime(2026, 11, 5, 13, 0, tzinfo=UTC)  # Thu 08:00 ET


def _definition(conid=265598, symbol='AAPL', primary='NASDAQ', tz='US/Eastern'):
    sd = SecurityDefinition.from_contract_details(ContractDetails(contract=Contract(
        conId=conid, exchange='SMART', primaryExchange=primary, secType='STK',
        symbol=symbol, currency='USD'), timeZoneId=tz))
    return sd


def _daily(days, tz='America/New_York', bar_size='1 day', volume=30e6):
    idx = pd.DatetimeIndex([pd.Timestamp(d, tz=tz) for d in days])
    df = pd.DataFrame({'open': 100.0, 'high': 101.0, 'low': 99.0, 'close': 100.5,
                       'volume': volume, 'average': 100.2, 'bar_count': 1000,
                       'bar_size': bar_size, 'what_to_show': 1}, index=idx)
    df.index.name = 'date'
    return df


def _stored_days(path, conid, bar_size='1 day'):
    df = TickStorage(path).get_tickdata(BarSize.parse_str(bar_size)).read(
        conid, date_range=DateRange(dt.datetime(2020, 1, 1, tzinfo=UTC),
                                    dt.datetime(2027, 1, 1, tzinfo=UTC)))
    return [] if df.empty else sorted({ts.date().isoformat() for ts in df.index})


@pytest.fixture
def tickdata(tmp_path):
    path = str(tmp_path / 'history.duckdb')
    return path, TickStorage(path).get_tickdata(BarSize.Days1)


class TestChokepoint:
    def test_todays_partial_daily_bar_is_skipped_during_the_session(self, tickdata, caplog):
        """The field case: Nov 5 at 14:30 ET, bar for Nov 5 included."""
        path, td = tickdata
        frame = _daily(['2026-11-03', '2026-11-04', '2026-11-05'])
        frame.loc[frame.index[-1], 'volume'] = 9.8e6
        with caplog.at_level(logging.INFO):
            written = td.write(_definition(), frame, now=DURING)
        assert written == 2
        assert _stored_days(path, 265598) == ['2026-11-03', '2026-11-04']
        assert any('SKIPPED 1 bar' in r.getMessage() and '2026-11-05' in r.getMessage()
                   for r in caplog.records)

    def test_before_the_open_todays_bar_is_skipped_too(self, tickdata):
        path, td = tickdata
        td.write(_definition(), _daily(['2026-11-04', '2026-11-05']), now=BEFORE_OPEN)
        assert _stored_days(path, 265598) == ['2026-11-04']

    def test_after_the_close_the_bar_is_stored(self, tickdata):
        path, td = tickdata
        assert td.write(_definition(), _daily(['2026-11-04', '2026-11-05']), now=AFTER) == 2
        assert _stored_days(path, 265598) == ['2026-11-04', '2026-11-05']

    def test_a_skipped_bar_does_not_count_the_day_as_covered(self, tickdata):
        """The reason it matters: missing() must still ask for Nov 5."""
        path, td = tickdata
        td.write(_definition(), _daily(['2026-11-03', '2026-11-04', '2026-11-05']), now=DURING)
        cal = exchange_calendars.get_calendar('XNYS')
        gaps = td.missing(265598, cal, date_range=DateRange(
            dt.datetime(2026, 11, 3, tzinfo=UTC), dt.datetime(2026, 11, 5, 23, tzinfo=UTC)))
        assert [(str(g.start), str(g.end)) for g in gaps] == [('2026-11-05', '2026-11-05')]

    def test_intraday_bars_for_today_are_unaffected(self, tmp_path):
        path = str(tmp_path / 'h.duckdb')
        td = TickStorage(path).get_tickdata(BarSize.Mins1)
        idx = pd.date_range('2026-11-05 09:30', periods=5, freq='min', tz='America/New_York')
        frame = pd.DataFrame({'open': 1.0, 'high': 1.0, 'low': 1.0, 'close': 1.0,
                              'volume': 1.0, 'bar_size': '1 min'}, index=idx)
        assert td.write(_definition(), frame, now=DURING) == 5

    def test_conid_key_without_calendar_fails_closed(self, tickdata, caplog):
        """Keyed by bare conId there is no venue, so the close is unknown: a
        period reaching today is refused (with a warning), yesterday kept."""
        path, td = tickdata
        with caplog.at_level(logging.WARNING):
            td.write(265598, _daily(['2026-11-04', '2026-11-05']), now=AFTER)
        assert _stored_days(path, 265598) == ['2026-11-04']
        assert any('no exchange calendar' in r.getMessage() for r in caplog.records)

    def test_explicit_calendar_lets_a_conid_key_store_after_the_close(self, tickdata, monkeypatch):
        """`data download` writes by conId; it passes the calendar through."""
        path, td = tickdata
        monkeypatch.setattr(session_guard, '_utcnow', lambda: AFTER)
        cal = exchange_calendars.get_calendar('XNYS')
        n = td.write_resolve_overlap(265598, _daily(['2026-11-04', '2026-11-05']),
                                     exchange_calendar=cal)
        assert n == 2
        assert _stored_days(path, 265598) == ['2026-11-04', '2026-11-05']

    def test_weekend_placeholder_is_left_alone(self, tickdata):
        path, td = tickdata
        frame = _daily(['2026-10-30', '2026-10-31', '2026-11-01'])
        frame.loc[frame.index[1:], ['open', 'high', 'low', 'close', 'volume']] = float('nan')
        assert td.write(_definition(), frame, now=DURING) == 3


class TestMask:
    def _cal(self, name='XNYS'):
        return exchange_calendars.get_calendar(name)

    def test_asx_bar_is_dated_in_sydney(self):
        """ASX session 2026-11-05 runs 10:00-16:00 AEDT = 23:00-05:00 UTC."""
        frame = _daily(['2026-11-05'], tz='Australia/Sydney')
        during = dt.datetime(2026, 11, 5, 2, 0, tzinfo=UTC)    # 13:00 Sydney
        after = dt.datetime(2026, 11, 5, 6, 0, tzinfo=UTC)     # 17:00 Sydney
        cal = self._cal('XASX')
        assert unclosed_session_mask(frame, calendar=cal, now=during).tolist() == [True]
        assert unclosed_session_mask(frame, calendar=cal, now=after).tolist() == [False]

    def test_weekly_bar_waits_for_the_weeks_last_close(self):
        frame = _daily(['2026-11-02'], bar_size='1 week')       # week of Mon 11-02
        cal = self._cal()
        assert unclosed_session_mask(frame, calendar=cal, now=AFTER).tolist() == [True]
        fri_after = dt.datetime(2026, 11, 6, 21, 30, tzinfo=UTC)
        assert unclosed_session_mask(frame, calendar=cal, now=fri_after).tolist() == [False]

    def test_monthly_bar_waits_for_the_months_last_close(self):
        frame = _daily(['2026-11-01'], bar_size='1 month')
        cal = self._cal()
        assert unclosed_session_mask(frame, calendar=cal, now=AFTER).tolist() == [True]
        dec = dt.datetime(2026, 12, 1, 12, tzinfo=UTC)
        assert unclosed_session_mask(frame, calendar=cal, now=dec).tolist() == [False]

    def test_future_dated_rows_are_refused(self):
        frame = _daily(['2026-11-07'])                          # Saturday, after now
        assert unclosed_session_mask(frame, calendar=self._cal(), now=DURING).tolist() == [True]

    def test_old_history_is_untouched(self):
        frame = _daily([d.date().isoformat() for d in pd.bdate_range('2016-01-04', '2026-11-04')])
        assert not unclosed_session_mask(frame, calendar=self._cal(), now=DURING).any()

    def test_bar_size_from_library_when_frame_has_no_column(self):
        frame = _daily(['2026-11-05']).drop(columns='bar_size')
        assert unclosed_session_mask(frame, '1 day', self._cal(), DURING).tolist() == [True]
        assert unclosed_session_mask(frame, '1 min', self._cal(), DURING).tolist() == [False]


# --------------------------------------------------------------------------
# Through each downloader: the vendors return the in-progress session.
# --------------------------------------------------------------------------

@pytest.fixture
def service(tmp_path, monkeypatch):
    from trader.data_service import DataService
    monkeypatch.setattr(session_guard, '_utcnow', lambda: DURING)
    path = str(tmp_path / 'history.duckdb')
    svc = DataService(massive_api_key='k', twelvedata_api_key='k',
                      duckdb_path=str(tmp_path / 'main.duckdb'), history_duckdb_path=path)
    return svc, path


def _partial_frame():
    frame = _daily(['2026-11-03', '2026-11-04', '2026-11-05'])
    frame.loc[frame.index[-1], 'volume'] = 9.8e6
    return frame


def _run_one(svc, path, method, *extra):
    td = TickStorage(path).get_tickdata(BarSize.Days1)
    dr = DateRange(dt.date(2026, 11, 3), dt.date(2026, 11, 5))
    result = asyncio.run(getattr(svc, method)(asyncio.Semaphore(1), _definition(), dr,
                                              BarSize.Days1, td, *extra))
    assert result['ok'], result
    return result


def test_ib_download_skips_the_partial_bar(service, monkeypatch):
    svc, path = service
    import trader.data_service as ds

    class FakeIB:
        def __init__(self, *a, **k):
            pass

        async def connect_async(self):
            return self

        async def get_contract_history(self, **kwargs):
            return _partial_frame()

        def shutdown(self):
            pass

    monkeypatch.setattr(ds, 'IBHistoryWorker', FakeIB)
    _run_one(svc, path, '_download_ib_one', 10)
    assert _stored_days(path, 265598) == ['2026-11-03', '2026-11-04']


@pytest.mark.parametrize('worker_attr,method', [
    ('MassiveHistoryWorker', '_download_massive_one'),
    ('TwelveDataHistoryWorker', '_download_twelvedata_one'),
])
def test_vendor_download_skips_the_partial_bar(service, monkeypatch, worker_attr, method):
    svc, path = service
    import trader.data_service as ds

    class FakeWorker:
        def __init__(self, *a, **k):
            pass

        def get_history(self, **kwargs):
            return _partial_frame()

    monkeypatch.setattr(ds, worker_attr, FakeWorker)
    _run_one(svc, path, method)
    assert _stored_days(path, 265598) == ['2026-11-03', '2026-11-04']
