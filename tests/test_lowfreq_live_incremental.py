"""Incremental modes of scripts/build_pit_universe.py and scripts/fetch_fundamentals.py
(C02 acceptance criterion 1): resume from the last stored session / acceptance
instant, idempotent on re-run, never rewrite older rows. Fake Massive and
sec-api / statement-vendor clients on a temporary DuckDB.

The statement vendor is the three `/stocks/financials/v1` endpoints since
2026-11-15: the `/vX` endpoint these tests used to fake was retired, the
ingest swallowed its 404, and every run reported "0 new rows"."""
from __future__ import annotations

import datetime as dt
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
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


_RANGE = __import__('re').compile(r' AND filedAt:\{(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}) TO \*\]$')


class FakeSec:
    """sec-api Query API fake: honours the ``filedAt:{X TO *]`` range clause —
    and, like the real API, answers a MALFORMED range with HTTP 200 and no
    filings. The first version of this fake split on ``filedAt:{`` and took
    whatever followed, so the stray ``}`` the client appended (``... TO *]}``)
    went unnoticed here while the real API returned nothing for every name."""
    def __init__(self, filings):
        self.filings = list(filings)      # dicts with periodOfReport, filedAt, formType, accessionNo
        self.queries = []

    def post(self, url, json, headers, timeout):
        q = json['query']
        self.queries.append(q)
        ticker = q.split('ticker:')[1].split(' ')[0]
        out = [f for f in self.filings if f['ticker'] == ticker]
        if 'filedAt:' in q:
            clause = _RANGE.search(q)
            if clause is None:
                return FakeResponse([])                       # malformed: 200, nothing (observed live)
            after = pd.Timestamp(clause.group(1), tz='UTC')
            out = [f for f in out if pd.Timestamp(f['filedAt']) > after]
        return FakeResponse(out if json['from'] == '0' else [])


class _StatementResponse:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self._body = body

    def json(self):
        return self._body


class FakeStatements:
    """The statement vendor: income statements, balance sheets and cash flow
    statements for (ticker, period), each row naming the ``filing_date`` its
    numbers were taken from - which, as at the real vendor, need not be the
    filing the period was first reported in.

    ``rows`` maps ``(ticker, period)`` to the filing date, or to a dict with
    ``filing_date`` and per-statement overrides (``income`` / ``balance`` /
    ``cashflow``: fields, or None for "the vendor has no such statement").
    """
    INCOME = {'revenue': 1000.0, 'consolidated_net_income_loss': 100.0, 'gross_profit': 400.0,
              'operating_income': 150.0, 'diluted_shares_outstanding': 50.0,
              'net_income_loss_attributable_common_shareholders': 90.0}
    BALANCE = {'total_assets': 5000.0, 'total_liabilities': 3000.0, 'total_equity': 2000.0,
               'total_equity_attributable_to_parent': 1900.0}
    CASHFLOW = {'net_cash_from_operating_activities': 120.0}

    def __init__(self, rows=None, status=200, body=None):
        self.rows = dict(rows or {})
        self.status = status
        self.body = body
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append({'url': url, 'params': dict(params or {}), 'headers': dict(headers or {})})
        if self.status != 200:
            return _StatementResponse(self.status, {'status': 'NOT_FOUND'})
        if self.body is not None:
            return _StatementResponse(200, self.body)
        name = {'income-statements': 'income', 'balance-sheets': 'balance',
                'cash-flow-statements': 'cashflow'}[url.rsplit('/', 1)[1]]
        defaults = {'income': self.INCOME, 'balance': self.BALANCE, 'cashflow': self.CASHFLOW}[name]
        results = []
        for (ticker, period), spec in self.rows.items():
            if ticker != params['tickers']:
                continue
            spec = spec if isinstance(spec, dict) else {'filing_date': spec}
            if name in spec and spec[name] is None:
                continue
            fields = dict(defaults, **(spec.get(name) or {}))
            results.append({'period_end': period, 'tickers': [ticker], 'timeframe': 'quarterly',
                            'filing_date': fields.pop('filing_date', spec['filing_date']), **fields})
        results.sort(key=lambda row: row['period_end'], reverse=True)
        return _StatementResponse(200, {'status': 'OK', 'results': results[:params['limit']]})


FULL_RESULT = {'written': 0, 'no_new': 0, 'failed': 0, 'names': 1, 'vendor_failed': 0,
               'no_statement': 0, 'other_filing': 0, 'aborted': None}


def _stored(db):
    return db.execute('SELECT * FROM fundamentals ORDER BY accepted_at', fetch='df')


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
    poly = FakeStatements({('AAA', '2024-03-31'): '2024-05-01', ('AAA', '2024-06-30'): '2024-08-01'})

    res = ff.run_incremental(db, hist, sec, poly, key='k', log=lambda m: None)
    assert res == dict(FULL_RESULT, written=1)
    row0 = db.execute('SELECT ticker, period_end, accession_no, fetched_at FROM fundamentals', fetch='all')
    assert len(row0) == 1 and row0[0][2] == 'acc-1'
    last = ff.last_accepted(db, 'AAA')
    assert last is not None

    # re-run with nothing new: the query carries the resume instant, nothing is written or rewritten
    res2 = ff.run_incremental(db, hist, sec, poly, key='k', log=lambda m: None)
    assert res2['written'] == 0 and res2['no_new'] == 1
    assert sec.queries[-1].endswith(' AND filedAt:{2024-05-01T10:01:00 TO *]')    # UTC of 06:01 EDT; nothing after the bracket
    assert db.execute('SELECT ticker, period_end, accession_no, fetched_at FROM fundamentals', fetch='all') == row0

    # a new filing (and a RESTATEMENT of the old period under a new accession) arrive;
    # the vendor now serves the restated quarter, dated to the amendment
    sec.filings += [_filing('AAA', '2024-06-30', '2024-08-01T16:30:00-04:00', 'acc-2'),
                    _filing('AAA', '2024-03-31', '2024-08-02T09:00:00-04:00', 'acc-1a')]
    poly.rows[('AAA', '2024-03-31')] = '2024-08-02'
    res3 = ff.run_incremental(db, hist, sec, poly, key='k', log=lambda m: None)
    assert res3['written'] == 2
    rows = db.execute('SELECT period_end, accession_no, fetched_at FROM fundamentals ORDER BY accepted_at', fetch='all')
    assert [r[1] for r in rows] == ['acc-1', 'acc-2', 'acc-1a']
    assert rows[0][2] == row0[0][3], 'the original row must be untouched (append-only)'


def test_fundamentals_incremental_ignore_never_replaces_existing_key(tmp_path):
    db = DuckDBConnection(str(tmp_path / 'main.duckdb'))
    ff.ensure_schema(db)
    vendor = FakeStatements({('AAA', '2024-03-31'): '2024-05-01'})
    index = {'2024-03-31': ('2024-05-01T06:01:00-04:00', '10-Q', 'acc-1')}
    assert ff.ingest_ticker(db, vendor, 'vk', index, '', 'AAA', append_only=True)['written'] == 1
    first = db.execute('SELECT fetched_at, accepted_at FROM fundamentals', fetch='all')
    # same key again, even with a changed acceptance instant: INSERT OR IGNORE keeps the stored row
    index2 = {'2024-03-31': ('2024-05-02T06:01:00-04:00', '10-Q', 'acc-1')}
    out = ff.ingest_ticker(db, vendor, 'vk', index2, '', 'AAA', append_only=True)
    # Regression (2026-10-01): a re-run reported "160 new rows" that INSERT OR IGNORE had skipped.
    # `written` counts rows actually inserted; an ignored row is not new.
    assert out['written'] == 0
    assert db.execute('SELECT fetched_at, accepted_at FROM fundamentals', fetch='all') == first


def test_same_period_multiple_filings_newest_wins_and_rerun_is_idempotent(tmp_path):
    """Two acceptances for one period in a single (newest-first) response: the
    newest must be stored on the FIRST run, so an unchanged re-run has nothing
    newer to add (the previous last-write-wins kept the oldest and the re-run
    then stored the newer one - a second run changing what the first left)."""
    db = DuckDBConnection(str(tmp_path / 'main.duckdb'))
    hist = DuckDBConnection(str(tmp_path / 'hist.duckdb'))
    hist.execute(bpu.PIT_TABLE_DDL, fetch='none')
    hist.execute('INSERT INTO pit_daily_bars VALUES (?,?,?,?,?,?,?,?,?)',
                 [dt.date(2024, 6, 28), 'AAA', 10, 11, 9, 10, 1e6, 1e7, 1], fetch='none')
    db.execute(ff.FUNDAMENTALS_DDL, fetch='none')
    sec = FakeSec([_filing('AAA', '2024-03-31', '2024-05-20T09:00:00-04:00', 'acc-amend'),   # newest first
                   _filing('AAA', '2024-03-31', '2024-05-01T06:01:00-04:00', 'acc-orig')])
    poly = FakeStatements({('AAA', '2024-03-31'): '2024-05-20'})
    index = ff.sec_filing_index(sec, 'AAA', 'k')
    assert index['2024-03-31'][2] == 'acc-amend'
    # response order must not matter
    sec.filings.reverse()
    assert ff.sec_filing_index(sec, 'AAA', 'k')['2024-03-31'][2] == 'acc-amend'
    res = ff.run_incremental(db, hist, sec, poly, key='k', log=lambda m: None)
    assert res['written'] == 1
    first = db.execute('SELECT accession_no, accepted_at, fetched_at FROM fundamentals', fetch='all')
    assert first[0][0] == 'acc-amend'
    res2 = ff.run_incremental(db, hist, sec, poly, key='k', log=lambda m: None)
    assert res2['written'] == 0 and res2['no_new'] == 1
    assert db.execute('SELECT accession_no, accepted_at, fetched_at FROM fundamentals', fetch='all') == first


def test_the_resume_filter_is_a_well_formed_range_and_new_filings_come_back():
    """The clause closed with a stray ``}`` (an f-string escape), sec-api
    answered the malformed range with 200 and no filings, and the incremental
    refresh fetched NOTHING from the day it was written while reporting
    "nothing new" for every name (found 2026-11-15: AAPL's stored filings were
    two quarters behind)."""
    sec = FakeSec([_filing('AAPL', '2026-06-27', '2026-07-31T06:01:02-04:00', 'q3'),
                   _filing('AAPL', '2026-09-26', '2026-11-03T08:05:15-05:00', 'fy')])
    stored = pd.Timestamp('2026-07-31T06:01:02-04:00')
    index = ff.sec_filing_index(sec, 'AAPL', 'k', filed_after=stored)
    query = sec.queries[0]
    assert query == 'ticker:AAPL AND (formType:"10-Q" OR formType:"10-K") AND filedAt:{2026-07-31T10:01:02 TO *]'
    assert query.count('{') == 1 and query.count('}') == 0 and query.count('[') == 0 and query.count(']') == 1
    assert index == {'2026-09-26': ('2026-11-03T08:05:15-05:00', '10-Q', 'fy')}      # only the filing after it


def test_a_malformed_filter_would_have_shown_as_nothing_new():
    """What the fake now does with the old query, so the regression is visible."""
    sec = FakeSec([_filing('AAPL', '2026-09-26', '2026-11-03T08:05:15-05:00', 'fy')])
    response = sec.post('url', json={'query': 'ticker:AAPL AND filedAt:{2026-07-31T10:01:02 TO *]}', 'from': '0'},
                        headers={}, timeout=1)
    assert response.json() == {'filings': []}


def test_without_a_resume_point_there_is_no_date_filter():
    sec = FakeSec([_filing('AAPL', '2026-09-26', '2026-11-03T08:05:15-05:00', 'fy')])
    assert ff.sec_filing_index(sec, 'AAPL', 'k') == {'2026-09-26': ('2026-11-03T08:05:15-05:00', '10-Q', 'fy')}
    assert sec.queries == ['ticker:AAPL AND (formType:"10-Q" OR formType:"10-K")']


# ------------------------------------------- the replacement statement vendor ---
#
# Until 2026-11-15 the numbers came from `/vX/reference/financials`. The vendor
# retired it, the call sat inside `except Exception`, and every run reported
# "0 new rows". What replaced it serves the LATEST version of a period, not the
# original, and share counts on today's split basis - so what may be stored,
# and what a failure looks like, are pinned here.

def _hist_with(tmp_path, *tickers):
    hist = DuckDBConnection(str(tmp_path / 'hist.duckdb'))
    hist.execute(bpu.PIT_TABLE_DDL, fetch='none')
    for rank, ticker in enumerate(tickers, start=1):
        hist.execute('INSERT INTO pit_daily_bars VALUES (?,?,?,?,?,?,?,?,?)',
                     [dt.date(2026, 11, 13), ticker, 10, 11, 9, 10, 1e6, 1e7, rank], fetch='none')
    return hist


@pytest.fixture
def store(tmp_path):
    db = DuckDBConnection(str(tmp_path / 'main.duckdb'))
    ff.ensure_schema(db)
    return db


Q3 = {'2026-09-26': ('2026-11-03T08:05:15-05:00', '10-K', 'acc-fy')}


class TestStatementRequests:
    def test_three_statements_are_asked_for_with_the_key_in_the_header_only(self):
        vendor = FakeStatements({('AAPL', '2026-09-26'): '2026-11-03'})
        out = ff.fetch_statements(vendor, 'vendor-secret', 'AAPL')
        assert [c['url'] for c in vendor.calls] == [
            ff.FINANCIALS_URL + 'income-statements', ff.FINANCIALS_URL + 'balance-sheets',
            ff.FINANCIALS_URL + 'cash-flow-statements']
        for call in vendor.calls:
            assert call['params'] == {'tickers': 'AAPL', 'timeframe': 'quarterly',
                                      'limit': ff.STATEMENT_PERIODS, 'sort': 'period_end.desc'}
            assert call['headers'] == {'Authorization': 'Bearer vendor-secret'}
            assert 'vendor-secret' not in call['url'] and 'vendor-secret' not in str(call['params'])
        assert set(out) == {'income', 'balance', 'cashflow'}
        assert out['income']['2026-09-26']['revenue'] == 1000.0
        assert out['balance']['2026-09-26']['total_assets'] == 5000.0
        assert out['cashflow']['2026-09-26']['net_cash_from_operating_activities'] == 120.0

    @pytest.mark.parametrize('status', [404, 401, 429, 500])
    def test_a_statement_request_that_fails_raises_and_names_it(self, status):
        """404 is what the retired endpoint answered. It was swallowed."""
        with pytest.raises(ff.StatementsUnavailable, match=f'income-statements answered HTTP {status}'):
            ff.fetch_statements(FakeStatements(status=status), 'k', 'AAPL')

    @pytest.mark.parametrize('body', [{}, {'results': None}, {'results': {}}, ['x']])
    def test_a_page_without_a_results_list_raises(self, body):
        with pytest.raises(ff.StatementsUnavailable, match='carries no results list'):
            ff.fetch_statements(FakeStatements(body=body), 'k', 'AAPL')

    def test_a_ticker_the_vendor_has_nothing_for_is_three_empty_statements(self):
        assert ff.fetch_statements(FakeStatements(), 'k', 'NOPE') == {'income': {}, 'balance': {}, 'cashflow': {}}

    def test_rows_without_a_period_are_skipped_and_the_first_seen_period_wins(self):
        body = {'results': [{'period_end': '2026-09-26', 'revenue': 2.0}, {'revenue': 9.0}, 'junk',
                            {'period_end': '2026-09-26', 'revenue': 1.0}]}
        out = ff.fetch_statements(FakeStatements(body=body), 'k', 'AAPL')
        assert out['income'] == {'2026-09-26': {'period_end': '2026-09-26', 'revenue': 2.0}}


class TestWhatIsStored:
    def test_a_row_carries_the_measured_field_mapping_and_its_source(self, store):
        vendor = FakeStatements({('AAPL', '2026-09-26'): '2026-11-03'})
        out = ff.ingest_ticker(store, vendor, 'vk', Q3, '', 'AAPL')
        assert out == {'written': 1, 'no_statement': 0, 'other_filing': 0, 'no_values': 0, 'error': None}
        row = _stored(store).iloc[0]
        assert (row['ticker'], str(row['period_end'])[:10], row['form_type'], row['accession_no']) == (
            'AAPL', '2026-09-26', '10-K', 'acc-fy')
        assert pd.Timestamp(row['accepted_at']) == pd.Timestamp('2026-11-03T08:05:15-05:00')
        assert row['source'] == 'financials_v1'
        # consolidated net income, total equity: what the legacy rows hold (measured)
        assert (row['revenue'], row['net_income'], row['gross_profit'], row['operating_income']) == (
            1000.0, 100.0, 400.0, 150.0)
        assert (row['total_assets'], row['total_liabilities'], row['stockholders_equity']) == (5000.0, 3000.0, 2000.0)
        assert (row['cash_from_operations'], row['shares_diluted']) == (120.0, 50.0)
        assert pd.notna(row['fetched_at'])

    def test_numbers_taken_from_a_later_filing_are_not_stored(self, store):
        """AAPL's quarter ended 2020-06-27 comes back from the vendor dated
        2021-07-28: the comparative column of the 10-Q a year on, recast."""
        index = {'2020-06-27': ('2020-07-31T06:01:00-04:00', '10-Q', 'acc-q3-2020')}
        vendor = FakeStatements({('AAPL', '2020-06-27'): '2021-07-28'})
        out = ff.ingest_ticker(store, vendor, 'vk', index, '', 'AAPL')
        assert (out['written'], out['other_filing'], out['no_statement']) == (0, 1, 0)
        assert len(_stored(store)) == 0

    def test_a_filing_the_vendor_has_not_parsed_yet_is_counted_not_stored(self, store):
        out = ff.ingest_ticker(store, FakeStatements(), 'vk', Q3, '', 'AAPL')
        assert (out['written'], out['no_statement'], out['other_filing']) == (0, 1, 0)
        assert len(_stored(store)) == 0

    def test_a_balance_sheet_from_another_filing_leaves_its_columns_unknown(self, store):
        """The income statement is this filing's; the balance sheet at the
        vendor has already been replaced by a later filing's comparative."""
        vendor = FakeStatements({('AAPL', '2026-09-26'): {
            'filing_date': '2026-11-03', 'balance': {'filing_date': '2027-01-30', 'total_assets': 6000.0}}})
        assert ff.ingest_ticker(store, vendor, 'vk', Q3, '', 'AAPL')['written'] == 1
        row = _stored(store).iloc[0]
        assert row['revenue'] == 1000.0 and row['cash_from_operations'] == 120.0
        assert pd.isna(row['total_assets']) and pd.isna(row['total_liabilities']) and pd.isna(row['stockholders_equity'])

    def test_a_missing_cash_flow_statement_leaves_its_column_unknown(self, store):
        vendor = FakeStatements({('AAPL', '2026-09-26'): {'filing_date': '2026-11-03', 'cashflow': None}})
        assert ff.ingest_ticker(store, vendor, 'vk', Q3, '', 'AAPL')['written'] == 1
        row = _stored(store).iloc[0]
        assert pd.isna(row['cash_from_operations']) and row['total_assets'] == 5000.0

    def test_a_zero_share_count_is_missing_not_zero(self, store):
        """The vendor writes 0.0 for a line the filer did not report."""
        vendor = FakeStatements({('AAPL', '2026-09-26'): {
            'filing_date': '2026-11-03', 'income': {'diluted_shares_outstanding': 0.0, 'revenue': 0.0}}})
        ff.ingest_ticker(store, vendor, 'vk', Q3, '', 'AAPL')
        row = _stored(store).iloc[0]
        assert pd.isna(row['shares_diluted'])
        assert row['revenue'] == 0.0                     # a pre-revenue company really has none

    def test_the_filing_day_is_edgars_not_utcs(self, store):
        """Accepted 2026-11-03 21:30 New York is 2026-11-04 in UTC; the vendor
        dates the filing the 3rd."""
        index = {'2026-09-26': ('2026-11-04T02:30:00+00:00', '10-K', 'acc-fy')}
        vendor = FakeStatements({('AAPL', '2026-09-26'): '2026-10-29'})      # 5 days before the 3rd, 6 before the 4th
        assert ff.ingest_ticker(store, vendor, 'vk', index, '', 'AAPL')['written'] == 1

    def test_full_mode_replaces_and_incremental_mode_never_does(self, store):
        vendor = FakeStatements({('AAPL', '2026-09-26'): '2026-11-03'})
        ff.ingest_ticker(store, vendor, 'vk', Q3, '', 'AAPL', append_only=True)
        vendor.rows[('AAPL', '2026-09-26')] = {'filing_date': '2026-11-03', 'income': {'revenue': 2000.0}}
        assert ff.ingest_ticker(store, vendor, 'vk', Q3, '', 'AAPL', append_only=True)['written'] == 0
        assert _stored(store).iloc[0]['revenue'] == 1000.0
        assert ff.ingest_ticker(store, vendor, 'vk', Q3, '', 'AAPL')['written'] == 1
        assert _stored(store).iloc[0]['revenue'] == 2000.0 and len(_stored(store)) == 1


class TestAVendorThatCannotBeReadIsNotAQuietYear:
    def test_one_name_is_counted_named_and_stores_nothing(self, tmp_path, store):
        hist = _hist_with(tmp_path, 'AAPL')
        sec = FakeSec([_filing('AAPL', '2026-09-26', '2026-11-03T08:05:15-05:00', 'fy')])
        lines = []
        res = ff.run_incremental(store, hist, sec, FakeStatements(status=404), key='k',
                                 log=lines.append, vendor_key='vk')
        assert res == dict(FULL_RESULT, vendor_failed=1)
        assert len(_stored(store)) == 0
        assert '  AAPL: statements unavailable (income-statements answered HTTP 404)' in lines
        assert lines[-1].startswith('done: 0 new rows, 0 names with nothing new, 0 sec-api failures, '
                                    '1 names whose statements could not be read')

    def test_ingest_reports_the_reason(self, store):
        out = ff.ingest_ticker(store, FakeStatements(status=404), 'vk', Q3, '', 'AAPL')
        assert out == {'written': 0, 'no_statement': 0, 'other_filing': 0, 'no_values': 1,
                       'error': 'income-statements answered HTTP 404'}

    def test_a_run_of_failures_stops_the_run(self, tmp_path, store, monkeypatch):
        """Ten in a row is an outage or a retired endpoint, not ten unlucky
        tickers. The rest of the list is not spent finding that out again."""
        monkeypatch.setattr(ff, 'MAX_CONSECUTIVE_VENDOR_ERRORS', 3)
        tickers = [f'T{n:02d}' for n in range(8)]
        hist = _hist_with(tmp_path, *tickers)
        sec = FakeSec([_filing(t, '2026-09-26', '2026-11-03T08:05:15-05:00', f'fy-{t}') for t in tickers])
        vendor = FakeStatements(status=500)
        lines = []
        res = ff.run_incremental(store, hist, sec, vendor, key='k', log=lines.append, vendor_key='vk')
        assert res['vendor_failed'] == 3 and res['names'] == 8
        assert res['aborted'] == ('3 consecutive statement-vendor errors at T02 (3/8), '
                                  'last: income-statements answered HTTP 500')
        assert len(vendor.calls) == 3 and len(sec.queries) == 3
        assert f'ABORT: {res["aborted"]}' in lines

    def test_a_success_in_between_resets_the_count(self, tmp_path, store, monkeypatch):
        monkeypatch.setattr(ff, 'MAX_CONSECUTIVE_VENDOR_ERRORS', 2)
        tickers = ['A1', 'A2', 'A3']
        hist = _hist_with(tmp_path, *tickers)
        sec = FakeSec([_filing(t, '2026-09-26', '2026-11-03T08:05:15-05:00', f'fy-{t}') for t in tickers])

        class Flaky(FakeStatements):
            def get(self, url, params=None, headers=None, timeout=None):
                self.status = 200 if params['tickers'] == 'A2' else 500
                return super().get(url, params=params, headers=headers, timeout=timeout)

        vendor = Flaky({('A2', '2026-09-26'): '2026-11-03'})
        res = ff.run_incremental(store, hist, sec, vendor, key='k', log=lambda m: None, vendor_key='vk')
        assert (res['vendor_failed'], res['written'], res['aborted']) == (2, 1, None)

    def test_the_vendor_key_reaches_the_vendor(self, tmp_path, store):
        hist = _hist_with(tmp_path, 'AAPL')
        sec = FakeSec([_filing('AAPL', '2026-09-26', '2026-11-03T08:05:15-05:00', 'fy')])
        vendor = FakeStatements({('AAPL', '2026-09-26'): '2026-11-03'})
        ff.run_incremental(store, hist, sec, vendor, key='sec-secret', log=lambda m: None, vendor_key='vendor-secret')
        assert {c['headers']['Authorization'] for c in vendor.calls} == {'Bearer vendor-secret'}

    def test_counts_reach_the_run_result(self, tmp_path, store):
        hist = _hist_with(tmp_path, 'AAPL', 'MSFT', 'NVDA')
        sec = FakeSec([_filing('AAPL', '2026-09-26', '2026-11-03T08:05:15-05:00', 'fy-a'),
                       _filing('MSFT', '2026-09-30', '2026-10-29T16:10:00-04:00', 'fy-m'),
                       _filing('NVDA', '2026-07-26', '2026-08-27T16:20:00-04:00', 'q2-n')])
        vendor = FakeStatements({('AAPL', '2026-09-26'): '2026-11-03',           # stored
                                 ('NVDA', '2026-07-26'): '2027-08-26'})          # a later filing's; MSFT: none yet
        res = ff.run_incremental(store, hist, sec, vendor, key='k', log=lambda m: None, vendor_key='vk')
        assert res == dict(FULL_RESULT, names=3, written=1, no_statement=1, other_filing=1)


class TestALateFilingNoticeNeverDatesARow:
    """`formType:"10-Q"` also matches "NT 10-Q": a notice that the report will
    be late, filed BEFORE it. 758 stored rows were dated to one."""

    def _notice(self, ticker, period, filed, form='NT 10-Q'):
        return {'ticker': ticker, 'periodOfReport': period, 'filedAt': filed, 'formType': form,
                'accessionNo': 'acc-nt'}

    def test_a_notice_alone_is_no_filing(self):
        sec = FakeSec([self._notice('LATE', '2026-09-30', '2026-11-10T17:00:00-05:00')])
        assert ff.sec_filing_index(sec, 'LATE', 'k') == {}

    def test_the_report_dates_the_period_not_the_notice_before_it(self):
        sec = FakeSec([self._notice('LATE', '2026-09-30', '2026-11-10T17:00:00-05:00'),
                       _filing('LATE', '2026-09-30', '2026-11-24T16:45:00-05:00', 'acc-real')])
        assert ff.sec_filing_index(sec, 'LATE', 'k') == {
            '2026-09-30': ('2026-11-24T16:45:00-05:00', '10-Q', 'acc-real')}

    def test_a_notice_filed_after_an_amendment_does_not_take_the_period(self):
        """Newest acceptance wins among REPORTS; a notice is not one."""
        sec = FakeSec([_filing('LATE', '2026-09-30', '2026-11-24T16:45:00-05:00', 'acc-real'),
                       self._notice('LATE', '2026-09-30', '2026-12-01T09:00:00-05:00', 'NT 10-K')])
        assert ff.sec_filing_index(sec, 'LATE', 'k')['2026-09-30'][2] == 'acc-real'


class TestSchema:
    def test_a_store_created_before_the_source_column_gains_it(self, tmp_path):
        db = DuckDBConnection(str(tmp_path / 'old.duckdb'))
        db.execute(ff.FUNDAMENTALS_DDL, fetch='none')
        db.execute("INSERT INTO fundamentals VALUES ('', 'AAPL', '2025-09-27', '2025-10-31 06:01:26-04', "
                   "'10-K', 'acc-old', 1, 2, 3, 4, 5, 6, 7, 8, -47029000, now())", fetch='none')
        ff.ensure_schema(db)
        ff.ensure_schema(db)                                              # idempotent
        columns = [r[0] for r in db.execute('DESCRIBE fundamentals', fetch='all')]
        assert columns == list(ff.COLUMNS)
        old = _stored(db).iloc[0]
        assert old['source'] is None or pd.isna(old['source'])             # legacy rows are marked by absence
        assert old['shares_diluted'] == -47029000.0                        # and left exactly as they were

    def test_new_rows_go_into_a_migrated_store_beside_the_old(self, tmp_path):
        db = DuckDBConnection(str(tmp_path / 'old.duckdb'))
        db.execute(ff.FUNDAMENTALS_DDL, fetch='none')
        db.execute("INSERT INTO fundamentals VALUES ('', 'AAPL', '2026-06-27', '2026-07-31 06:01:02-04', "
                   "'10-Q', 'acc-q3', 1, 2, 3, 4, 5, 6, 7, 8, 9, now())", fetch='none')
        ff.ensure_schema(db)
        ff.ingest_ticker(db, FakeStatements({('AAPL', '2026-09-26'): '2026-11-03'}), 'vk', Q3, '', 'AAPL',
                         append_only=True)
        frame = _stored(db)
        assert list(frame['accession_no']) == ['acc-q3', 'acc-fy']
        assert [None if pd.isna(v) else v for v in frame['source']] == [None, 'financials_v1']
