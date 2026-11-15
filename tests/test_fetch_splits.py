"""scripts/fetch_splits.py: the split history, fetched market-wide and loudly.

The script used to ask the vendor ticker by ticker, swallow every failed
request and exit 0 whatever happened. Its table now also feeds the research
path's market caps (trader/data/share_basis.py), where a partial table that
looks complete is a market cap silently wrong.
"""
from __future__ import annotations

import datetime as dt
import importlib.util
import sys
from pathlib import Path

import pytest

from trader.data.duckdb_store import DuckDBConnection

REPO = Path(__file__).resolve().parents[1]
TODAY = dt.date(2026, 11, 15)


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, REPO / 'scripts' / f'{name}.py')
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


fs = _load('fetch_splits')


def _split(ticker, day, split_from, split_to):
    return {'ticker': ticker, 'execution_date': day, 'split_from': split_from, 'split_to': split_to,
            'id': f'E{ticker}{day}'}


class _Response:
    def __init__(self, status_code=200, body=None):
        self.status_code = status_code
        self._body = body

    def json(self):
        return self._body


class FakeVendor:
    """Serves ``rows`` filtered by the requested window, ``page`` at a time,
    with a ``next_url`` the way the real endpoint pages."""

    def __init__(self, rows, page=2, fail_on_call=None, status=500, body_override=None):
        self.rows = sorted(rows, key=lambda r: r['execution_date'])
        self.page = page
        self.calls = []
        self.fail_on_call = fail_on_call
        self.status = status
        self.body_override = body_override
        self._window = None

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append({'url': url, 'params': params, 'headers': dict(headers or {})})
        if self.fail_on_call == len(self.calls):
            return _Response(self.status, {'status': 'ERROR'})
        if self.body_override is not None:
            return _Response(200, self.body_override)
        if params is not None:
            self._window = (params['execution_date.gte'], params['execution_date.lte'])
            start = 0
        else:
            start = int(url.rsplit('cursor=', 1)[1])
        lo, hi = self._window
        matching = [r for r in self.rows if lo <= str(r['execution_date']) <= hi]
        chunk = matching[start:start + self.page]
        body = {'status': 'OK', 'results': chunk}
        if start + self.page < len(matching):
            body['next_url'] = f'{fs.SPLITS_URL}?cursor={start + self.page}'
        return _Response(200, body)


@pytest.fixture
def db(tmp_path):
    return DuckDBConnection(str(tmp_path / 'history.duckdb'))


def _stored(db):
    return db.execute('SELECT ticker, conid, execution_date, split_to, split_from FROM corporate_splits '
                      'ORDER BY execution_date, ticker', fetch='all')


ROWS = [_split('AAPL', '2020-08-31', 1, 4), _split('NVDA', '2021-07-20', 1, 4),
        _split('GE', '2021-08-02', 8, 1), _split('NVDA', '2024-06-10', 1, 10),
        _split('MNST', '2026-08-11', 1, 2)]


class TestFetch:
    def test_every_page_is_followed_and_the_key_is_only_in_the_header(self):
        vendor = FakeVendor(ROWS, page=2)
        rows = fs.fetch_splits(vendor, 'secret-key', dt.date(2020, 1, 1), TODAY)
        assert [r['ticker'] for r in rows] == ['AAPL', 'NVDA', 'GE', 'NVDA', 'MNST']
        assert len(vendor.calls) == 3
        first = vendor.calls[0]
        assert first['url'] == fs.SPLITS_URL
        assert first['params'] == {'execution_date.gte': '2020-01-01', 'execution_date.lte': '2026-11-15',
                                   'limit': 1000, 'order': 'asc', 'sort': 'execution_date'}
        for call in vendor.calls:
            assert call['headers'] == {'Authorization': 'Bearer secret-key'}
            assert 'secret-key' not in call['url'] and 'secret-key' not in str(call['params'])
        # next_url carries the query: no params are sent with it.
        assert [c['params'] for c in vendor.calls[1:]] == [None, None]

    def test_the_window_is_inclusive_at_both_ends(self):
        vendor = FakeVendor(ROWS, page=10)
        rows = fs.fetch_splits(vendor, 'k', dt.date(2021, 7, 20), dt.date(2021, 8, 2))
        assert [(r['ticker'], r['execution_date']) for r in rows] == [('NVDA', '2021-07-20'), ('GE', '2021-08-02')]

    @pytest.mark.parametrize('status', [401, 403, 429, 500, 503])
    def test_a_request_that_fails_raises(self, status):
        vendor = FakeVendor(ROWS, fail_on_call=1, status=status)
        with pytest.raises(fs.SplitsUnavailable, match=f'HTTP {status}'):
            fs.fetch_splits(vendor, 'k', dt.date(2020, 1, 1), TODAY)

    def test_a_later_page_that_fails_raises_too(self):
        """Half a history is not a history."""
        vendor = FakeVendor(ROWS, page=2, fail_on_call=2)
        with pytest.raises(fs.SplitsUnavailable):
            fs.fetch_splits(vendor, 'k', dt.date(2020, 1, 1), TODAY)

    @pytest.mark.parametrize('body', [{}, {'status': 'OK'}, {'results': None}, {'results': 'none'}, ['x'], None])
    def test_a_page_without_a_results_list_raises(self, body):
        vendor = FakeVendor(ROWS, body_override=body if body is not None else 'null')
        with pytest.raises(fs.SplitsUnavailable, match='no results list'):
            fs.fetch_splits(vendor, 'k', dt.date(2020, 1, 1), TODAY)

    def test_an_empty_answer_is_an_answer(self):
        assert fs.fetch_splits(FakeVendor([]), 'k', dt.date(2020, 1, 1), TODAY) == []

    def test_paging_that_never_ends_raises(self, monkeypatch):
        monkeypatch.setattr(fs, 'MAX_PAGES', 2)
        vendor = FakeVendor(ROWS, page=1)
        with pytest.raises(fs.SplitsUnavailable, match='did not end within 2 pages'):
            fs.fetch_splits(vendor, 'k', dt.date(2020, 1, 1), TODAY)
        assert len(vendor.calls) == 2


class TestUsableRows:
    def test_executed_splits_are_kept_as_stored_tuples(self):
        rows, dropped = fs.usable_rows(ROWS, TODAY)
        assert dropped == 0
        assert rows[0] == ('AAPL', dt.date(2020, 8, 31), 4.0, 1.0)             # (ticker, day, split_to, split_from)
        assert rows[2] == ('GE', dt.date(2021, 8, 2), 1.0, 8.0)

    def test_an_announced_split_is_not_stored(self):
        """It has changed no price yet and can still be changed or withdrawn."""
        rows, dropped = fs.usable_rows([_split('X', '2026-11-15', 1, 2), _split('Y', '2026-11-16', 1, 2)], TODAY)
        assert [r[0] for r in rows] == ['X'] and dropped == 1

    @pytest.mark.parametrize('row', [
        _split('', '2020-01-02', 1, 2), _split('X', None, 1, 2), _split('X', '2020-01-02', 0, 2),
        _split('X', '2020-01-02', 1, 0), _split('X', '2020-01-02', 3, 3), 'not a row', None,
    ])
    def test_a_row_that_is_not_a_split_is_dropped_and_counted(self, row):
        rows, dropped = fs.usable_rows([row, _split('OK', '2020-01-02', 1, 2)], TODAY)
        assert [r[0] for r in rows] == ['OK'] and dropped == 1


class TestStore:
    def test_rows_are_stored_with_the_conid_of_a_known_ticker(self, db):
        rows, _ = fs.usable_rows(ROWS[:3], TODAY)
        assert fs.store_splits(db, rows, {'AAPL': '265598'}) == 3
        assert _stored(db) == [('AAPL', '265598', dt.date(2020, 8, 31), 4.0, 1.0),
                               ('NVDA', None, dt.date(2021, 7, 20), 4.0, 1.0),
                               ('GE', None, dt.date(2021, 8, 2), 1.0, 8.0)]

    def test_a_correction_replaces_the_row_for_that_ticker_and_day(self, db):
        fs.store_splits(db, [('X', dt.date(2020, 1, 2), 2.0, 1.0)])
        fs.store_splits(db, [('X', dt.date(2020, 1, 2), 3.0, 1.0)])
        assert _stored(db) == [('X', None, dt.date(2020, 1, 2), 3.0, 1.0)]

    def test_storing_nothing_still_creates_the_table(self, db):
        assert fs.store_splits(db, []) == 0
        assert _stored(db) == []

    def test_the_table_is_the_one_the_audit_and_the_older_script_use(self, db):
        fs.store_splits(db, [])
        columns = [r[0] for r in db.execute('DESCRIBE corporate_splits', fetch='all')]
        assert columns == ['ticker', 'conid', 'execution_date', 'split_to', 'split_from', 'fetched_at']


class TestResumeFrom:
    def test_an_empty_table_starts_at_the_beginning(self, db):
        assert fs.resume_from(db, TODAY) == fs.FIRST_DAY

    def test_it_overlaps_the_newest_stored_split(self, db):
        fs.store_splits(db, [('X', dt.date(2026, 10, 1), 2.0, 1.0), ('Y', dt.date(2026, 11, 5), 2.0, 1.0)])
        assert fs.resume_from(db, TODAY) == dt.date(2026, 11, 5) - dt.timedelta(days=fs.OVERLAP_DAYS)

    def test_a_future_dated_row_left_by_the_older_script_does_not_move_it(self, db):
        """That script stored announced splits; resuming from one would skip
        everything executed between now and then."""
        fs.store_splits(db, [('X', dt.date(2026, 10, 1), 2.0, 1.0), ('Z', dt.date(2026, 12, 7), 2.0, 1.0)])
        assert fs.resume_from(db, TODAY) == dt.date(2026, 10, 1) - dt.timedelta(days=fs.OVERLAP_DAYS)

    def test_it_never_starts_before_the_first_day(self, db):
        fs.store_splits(db, [('X', fs.FIRST_DAY + dt.timedelta(days=3), 2.0, 1.0)])
        assert fs.resume_from(db, TODAY) == fs.FIRST_DAY


class TestRefresh:
    def test_a_first_run_reads_everything_through_today(self, db):
        vendor = FakeVendor(ROWS + [_split('SOON', '2026-12-01', 1, 2)], page=3)
        lines = []
        out = fs.refresh(db, vendor, 'k', today=TODAY, conids={'NVDA': '4815747'}, log=lines.append)
        assert vendor.calls[0]['params']['execution_date.gte'] == fs.FIRST_DAY.isoformat()
        assert vendor.calls[0]['params']['execution_date.lte'] == '2026-11-15'
        assert out == {'listed': 5, 'written': 5, 'dropped': 0, 'withdrawn': 0, 'stored': 5,
                       'since': fs.FIRST_DAY, 'until': TODAY}
        assert [r[0] for r in _stored(db)] == ['AAPL', 'NVDA', 'GE', 'NVDA', 'MNST']
        assert ('NVDA', '4815747', dt.date(2021, 7, 20), 4.0, 1.0) in _stored(db)
        assert lines == ['splits: 5 listed from 2009-01-01 to 2026-11-15, 5 stored, 0 not usable; '
                         '5 executed splits in the table, newest 2026-08-11']

    def test_a_later_run_reads_only_the_overlap_and_changes_nothing_old(self, db):
        vendor = FakeVendor(ROWS, page=10)
        fs.refresh(db, vendor, 'k', today=TODAY, log=lambda m: None)
        before = _stored(db)
        vendor.rows.append(_split('APH', '2026-09-03', 1, 2))
        vendor.rows.sort(key=lambda r: r['execution_date'])
        out = fs.refresh(db, vendor, 'k', today=TODAY, log=lambda m: None)
        assert out['since'] == dt.date(2026, 8, 11) - dt.timedelta(days=fs.OVERLAP_DAYS)
        assert out['listed'] == 2 and out['stored'] == 6                       # MNST again, and APH
        assert [r for r in _stored(db) if r[0] != 'APH'] == before

    def test_full_starts_over(self, db):
        vendor = FakeVendor(ROWS, page=10)
        fs.refresh(db, vendor, 'k', today=TODAY, log=lambda m: None)
        out = fs.refresh(db, vendor, 'k', full=True, today=TODAY, log=lambda m: None)
        assert out['since'] == fs.FIRST_DAY and out['listed'] == 5 and out['stored'] == 5

    def test_a_failed_run_stores_nothing(self, db):
        vendor = FakeVendor(ROWS, page=2, fail_on_call=2)
        with pytest.raises(fs.SplitsUnavailable):
            fs.refresh(db, vendor, 'k', today=TODAY, log=lambda m: None)
        assert _stored(db) == []

    def test_unusable_rows_are_counted_in_the_report(self, db):
        vendor = FakeVendor(ROWS[:2] + [_split('BAD', '2020-09-01', 0, 2)], page=10)
        out = fs.refresh(db, vendor, 'k', today=TODAY, log=lambda m: None)
        assert (out['listed'], out['written'], out['dropped']) == (3, 2, 1)


class TestUniverseConids:
    def test_every_universe_member_is_mapped_by_ticker(self):
        from types import SimpleNamespace

        class Accessor:
            def list_universes(self):
                return ['a', 'b']

            def get(self, name):
                members = {'a': [SimpleNamespace(symbol='AAPL', conId=265598)],
                           'b': [SimpleNamespace(symbol='NVDA', conId=4815747)]}[name]
                return SimpleNamespace(security_definitions=members)

        assert fs.universe_conids(Accessor()) == {'AAPL': '265598', 'NVDA': '4815747'}


class TestWithdrawnSplits:
    """The vendor's adjusted prices follow its CURRENT list. A row it no longer
    lists is in no price it serves and must not be in a share factor - CGNX had
    one dated 2017-11-16 beside the real 2017-12-04, and counted both."""

    def _seed(self, db, vendor_rows):
        vendor = FakeVendor(vendor_rows, page=50)
        fs.refresh(db, vendor, 'k', today=TODAY, log=lambda m: None)
        return vendor

    def _withdrawn(self, db):
        return db.execute('SELECT ticker, execution_date, split_to, split_from FROM corporate_splits_withdrawn '
                          'ORDER BY execution_date, ticker', fetch='all')

    def test_a_row_the_vendor_stops_listing_is_moved_not_deleted(self, db):
        vendor = self._seed(db, ROWS + [_split('CGNX', '2017-11-16', 1, 2), _split('CGNX', '2017-12-04', 1, 2)])
        vendor.rows = [r for r in vendor.rows if not (r['ticker'] == 'CGNX' and r['execution_date'] == '2017-11-16')]
        lines = []
        out = fs.refresh(db, vendor, 'k', full=True, today=TODAY, log=lines.append)
        assert out['withdrawn'] == 1
        assert ('CGNX', None, dt.date(2017, 11, 16), 2.0, 1.0) not in _stored(db)
        assert ('CGNX', None, dt.date(2017, 12, 4), 2.0, 1.0) in _stored(db)
        assert self._withdrawn(db) == [('CGNX', dt.date(2017, 11, 16), 2.0, 1.0)]
        assert lines[-1] == ('splits: 1 stored row(s) the vendor no longer lists moved to '
                             'corporate_splits_withdrawn: CGNX 2017-11-16')

    def test_only_the_window_just_read_is_reconciled(self, db):
        """A default run reads the last 45 days; an older row it did not ask
        about is not "no longer listed"."""
        vendor = self._seed(db, ROWS)
        vendor.rows = [r for r in vendor.rows if r['ticker'] != 'AAPL']          # 2020: outside the overlap
        out = fs.refresh(db, vendor, 'k', today=TODAY, log=lambda m: None)
        assert out['withdrawn'] == 0 and 'AAPL' in [r[0] for r in _stored(db)]

    def test_a_row_dated_after_today_is_outside_every_window(self, db):
        fs.store_splits(db, [('SOON', dt.date(2026, 12, 7), 2.0, 1.0)])           # left by the older script
        fs.refresh(db, FakeVendor(ROWS, page=50), 'k', full=True, today=TODAY, log=lambda m: None)
        assert 'SOON' in [r[0] for r in _stored(db)]

    def test_a_row_from_before_the_first_day_is_left_alone(self, db):
        fs.store_splits(db, [('NVDA', dt.date(2007, 9, 11), 3.0, 2.0)])
        fs.refresh(db, FakeVendor(ROWS, page=50), 'k', full=True, today=TODAY, log=lambda m: None)
        assert ('NVDA', None, dt.date(2007, 9, 11), 3.0, 2.0) in _stored(db)

    def test_a_short_answer_is_not_a_list_of_corrections(self, db, monkeypatch):
        """HTTP 200, a results list, and most of the table missing from it."""
        monkeypatch.setattr(fs, 'WITHDRAW_LIMIT', 2)
        vendor = self._seed(db, ROWS)
        before = _stored(db)
        vendor.rows = vendor.rows[:1]                                            # four of five gone
        with pytest.raises(fs.SplitsUnavailable, match='4 stored rows in that window are not among them') as stop:
            fs.refresh(db, vendor, 'k', full=True, today=TODAY, log=lambda m: None)
        assert 'nothing was changed' in str(stop.value) and '--accept-withdrawals' in str(stop.value)
        assert 'NVDA 2021-07-20' in str(stop.value)
        assert _stored(db) == before

    def test_the_limit_is_inclusive_and_can_be_accepted(self, db, monkeypatch):
        monkeypatch.setattr(fs, 'WITHDRAW_LIMIT', 2)
        vendor = self._seed(db, ROWS)
        vendor.rows = vendor.rows[:3]                                            # two gone: at the limit
        assert fs.refresh(db, vendor, 'k', full=True, today=TODAY, log=lambda m: None)['withdrawn'] == 2
        vendor.rows = vendor.rows[:0]                                            # three more
        with pytest.raises(fs.SplitsUnavailable):
            fs.refresh(db, vendor, 'k', full=True, today=TODAY, log=lambda m: None)
        out = fs.refresh(db, vendor, 'k', full=True, today=TODAY, log=lambda m: None, accept_withdrawals=True)
        assert out['withdrawn'] == 3 and _stored(db) == [] and len(self._withdrawn(db)) == 5

    def test_the_real_limit(self):
        assert fs.WITHDRAW_LIMIT == 25

    def test_no_longer_listed_compares_ticker_and_day(self, db):
        fs.store_splits(db, [('X', dt.date(2026, 10, 1), 2.0, 1.0), ('Y', dt.date(2026, 10, 1), 2.0, 1.0),
                             ('X', dt.date(2026, 10, 9), 3.0, 1.0)])
        listed = [('X', dt.date(2026, 10, 1), 5.0, 1.0)]                         # same key, corrected ratio
        assert fs.no_longer_listed(db, listed, dt.date(2026, 10, 1), dt.date(2026, 10, 9)) == [
            ('Y', dt.date(2026, 10, 1)), ('X', dt.date(2026, 10, 9))]
        assert fs.no_longer_listed(db, listed, dt.date(2026, 10, 2), dt.date(2026, 10, 8)) == []
