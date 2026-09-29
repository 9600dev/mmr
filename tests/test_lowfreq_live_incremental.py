"""Incremental modes of scripts/build_pit_universe.py and scripts/fetch_fundamentals.py
(C02 acceptance criterion 1): resume from the last stored session / acceptance
instant, idempotent on re-run, never rewrite older rows. Fake Massive and
sec-api/Polygon clients on a temporary DuckDB."""
from __future__ import annotations

import datetime as dt
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from trader.data.duckdb_store import DuckDBConnection

REPO = Path(__file__).resolve().parents[1]


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, REPO / 'scripts' / f'{name}.py')
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


bpu = _load('build_pit_universe')
ff = _load('fetch_fundamentals')


class FakeMassive:
    """Grouped-daily bars keyed by ISO day; ``scale`` lets a later run serve
    DIFFERENT numbers for old days, which must never reach the table."""
    def __init__(self, days, scale=1.0):
        self.days = list(days)
        self.scale = scale
        self.calls = []

    def get_grouped_daily_aggs(self, day, adjusted=True):
        self.calls.append(day)
        if dt.date.fromisoformat(day) not in self.days:
            return []
        bars = []
        for k, t in enumerate(('AAA', 'BBB', 'CCC', 'ZWZZT')):
            c = (10.0 + k) * self.scale
            bars.append(SimpleNamespace(ticker=t, open=c * 0.99, high=c * 1.01, low=c * 0.98,
                                        close=c, volume=1e6 * (4 - k)))
        return bars


def _sessions(n, start=dt.date(2024, 7, 1)):
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += dt.timedelta(days=1)
    return out


def test_pit_build_resumes_idempotently_and_never_rewrites(tmp_path):
    db = DuckDBConnection(str(tmp_path / 'hist.duckdb'))
    days = _sessions(6)
    first = FakeMassive(days)
    res = bpu.run_build(db, first, days[0], days[2], top=3, log=lambda m: None)
    assert res['sessions'] == 3 and res['rows'] == 9 and res['test_ticker_bars'] == 3
    before = db.execute('SELECT day, ticker, close FROM pit_daily_bars ORDER BY day, ticker', fetch='all')
    assert bpu.last_stored_session(db) == days[2]

    # incremental: resume from the day after the last stored session, with a
    # client that now serves different numbers for the OLD days
    later = FakeMassive(days, scale=2.0)
    start, end = bpu.incremental_window(db, days[5], fallback_start=days[0])
    assert start == days[2] + dt.timedelta(days=1)
    res2 = bpu.run_build(db, later, start, end, top=3, log=lambda m: None)
    assert res2['sessions'] == 3 and res2['skipped'] == 0
    assert all(dt.date.fromisoformat(c) > days[2] for c in later.calls), 'old sessions must not be re-fetched'
    after = db.execute('SELECT day, ticker, close FROM pit_daily_bars ORDER BY day, ticker', fetch='all')
    assert after[:9] == before, 'older rows rewritten'
    assert after[9][2] == pytest.approx(20.0)          # the new sessions carry the new client's numbers

    # re-run over the whole window: every session is already stored -> zero
    # fetches, zero writes, table byte-identical (idempotent)
    again = FakeMassive(days, scale=3.0)
    res3 = bpu.run_build(db, again, days[0], days[5], top=3, log=lambda m: None)
    assert res3['sessions'] == 0 and res3['rows'] == 0 and res3['skipped'] == 6
    assert again.calls == []
    assert db.execute('SELECT day, ticker, close FROM pit_daily_bars ORDER BY day, ticker', fetch='all') == after
    start, end = bpu.incremental_window(db, days[5], fallback_start=days[0])
    assert start > end                                  # "up to date"


def test_pit_build_empty_table_falls_back_to_start(tmp_path):
    db = DuckDBConnection(str(tmp_path / 'hist.duckdb'))
    db.execute(bpu.PIT_TABLE_DDL, fetch='none')
    assert bpu.last_stored_session(db) is None
    assert bpu.incremental_window(db, dt.date(2024, 7, 5), dt.date(2024, 7, 1)) == (dt.date(2024, 7, 1), dt.date(2024, 7, 5))


# ------------------------------------------------------------- fundamentals ---

class FakeResponse:
    def __init__(self, filings):
        self.status_code = 200
        self._f = filings

    def json(self):
        return {'filings': self._f}


class FakeSec:
    """sec-api Query API fake: honours the ``filedAt:{X TO *]`` range clause."""
    def __init__(self, filings):
        self.filings = list(filings)      # dicts with periodOfReport, filedAt, formType, accessionNo
        self.queries = []

    def post(self, url, json, headers, timeout):
        q = json['query']
        self.queries.append(q)
        ticker = q.split('ticker:')[1].split(' ')[0]
        out = [f for f in self.filings if f['ticker'] == ticker]
        if 'filedAt:{' in q:
            after = q.split('filedAt:{')[1].split(' TO')[0]
            out = [f for f in out if f['filedAt'] > after]
        return FakeResponse(out if json['from'] == '0' else [])


class FakePoly:
    def __init__(self, periods):
        self.vx = self
        self.periods = periods

    def list_stock_financials(self, ticker, limit, timeframe):
        return [SimpleNamespace(end_date=p, financials=None) for p in self.periods]


def _filing(ticker, period, filed, acc):
    return {'ticker': ticker, 'periodOfReport': period, 'filedAt': filed, 'formType': '10-Q', 'accessionNo': acc}


def test_fundamentals_incremental_appends_only_new_acceptances(tmp_path):
    db = DuckDBConnection(str(tmp_path / 'main.duckdb'))
    hist = DuckDBConnection(str(tmp_path / 'hist.duckdb'))
    hist.execute(bpu.PIT_TABLE_DDL, fetch='none')
    for d in _sessions(3, dt.date(2024, 6, 26)):
        hist.execute('INSERT INTO pit_daily_bars VALUES (?,?,?,?,?,?,?,?,?)',
                     [d, 'AAA', 10, 11, 9, 10, 1e6, 1e7, 1], fetch='none')
    db.execute(ff.FUNDAMENTALS_DDL, fetch='none')
    sec = FakeSec([_filing('AAA', '2024-03-31', '2024-05-01T06:01:00-04:00', 'acc-1')])
    poly = FakePoly(['2024-03-31', '2024-06-30'])

    res = ff.run_incremental(db, hist, sec, poly, key='k', log=lambda m: None)
    assert res == {'written': 1, 'no_new': 0, 'failed': 0, 'names': 1}
    row0 = db.execute('SELECT ticker, period_end, accession_no, fetched_at FROM fundamentals', fetch='all')
    assert len(row0) == 1 and row0[0][2] == 'acc-1'
    last = ff.last_accepted(db, 'AAA')
    assert last is not None

    # re-run with nothing new: the query carries the resume instant, nothing is written or rewritten
    res2 = ff.run_incremental(db, hist, sec, poly, key='k', log=lambda m: None)
    assert res2['written'] == 0 and res2['no_new'] == 1
    assert 'filedAt:{2024-05-01T10:01:00 TO *]}' in sec.queries[-1]       # UTC of 06:01 EDT
    assert db.execute('SELECT ticker, period_end, accession_no, fetched_at FROM fundamentals', fetch='all') == row0

    # a new filing (and a RESTATEMENT of the old period under a new accession) arrive
    sec.filings += [_filing('AAA', '2024-06-30', '2024-08-01T16:30:00-04:00', 'acc-2'),
                    _filing('AAA', '2024-03-31', '2024-08-02T09:00:00-04:00', 'acc-1a')]
    res3 = ff.run_incremental(db, hist, sec, poly, key='k', log=lambda m: None)
    assert res3['written'] == 2
    rows = db.execute('SELECT period_end, accession_no, fetched_at FROM fundamentals ORDER BY accepted_at', fetch='all')
    assert [r[1] for r in rows] == ['acc-1', 'acc-2', 'acc-1a']
    assert rows[0][2] == row0[0][3], 'the original row must be untouched (append-only)'


def test_fundamentals_incremental_ignore_never_replaces_existing_key(tmp_path):
    db = DuckDBConnection(str(tmp_path / 'main.duckdb'))
    db.execute(ff.FUNDAMENTALS_DDL, fetch='none')
    index = {'2024-03-31': ('2024-05-01T06:01:00-04:00', '10-Q', 'acc-1')}
    ff.ingest_ticker(db, FakePoly(['2024-03-31']), index, '', 'AAA', append_only=True)
    first = db.execute('SELECT fetched_at, accepted_at FROM fundamentals', fetch='all')
    # same key again, even with a changed acceptance instant: INSERT OR IGNORE keeps the stored row
    index2 = {'2024-03-31': ('2024-05-02T06:01:00-04:00', '10-Q', 'acc-1')}
    out = ff.ingest_ticker(db, FakePoly(['2024-03-31']), index2, '', 'AAA', append_only=True)
    assert out['written'] == 1                     # attempted, but ignored by the key
    assert db.execute('SELECT fetched_at, accepted_at FROM fundamentals', fetch='all') == first
