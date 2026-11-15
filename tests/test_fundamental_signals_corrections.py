"""scripts/fundamental_signals.py: what stands between the stored fundamentals
rows and a market cap (docs/RESEARCH_LOG.md, 2026-11-15).

The leak probe this script carries moves WHEN a number is known. It cannot see
a number that is wrong, or one on a different basis from the price beside it.
Four such defects sat in the stored rows; `prepare_rows` repairs them and each
repair can be switched off to measure what it was worth.
"""
from __future__ import annotations

import datetime as dt
import importlib.util
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from trader.data.duckdb_store import DuckDBConnection
from trader.data.share_basis import split_events

REPO = Path(__file__).resolve().parents[1]
ALL = frozenset({'notices', 'annual_shares', 'split_basis', 'units', 'periods'})


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, REPO / 'scripts' / f'{name}.py')
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


fs = _load('fundamental_signals')


def _row(ticker, period_end, accepted, form='10-Q', shares=100e6, revenue=1e9, conid='', **extra):
    stamp = pd.Timestamp(accepted, tz='America/New_York')
    row = {'conid': conid, 'ticker': ticker, 'period_end': pd.Timestamp(period_end).date(),
           'accepted_at': stamp, 'known_at': stamp.tz_convert('UTC'), 'form_type': form,
           'revenue': revenue, 'net_income': revenue * 0.1, 'gross_profit': revenue * 0.4,
           'cash_from_operations': revenue * 0.12, 'shares_diluted': shares}
    row.update(extra)
    return row


def _quarters(ticker, n=6, start='2023-03-31', shares=100e6, **kw):
    """n consecutive quarterly filings, each accepted 35 days after period end."""
    rows, end = [], pd.Timestamp(start)
    for q in range(n):
        accepted = (end + pd.Timedelta(days=35)).strftime('%Y-%m-%d') + ' 16:30'
        rows.append(_row(ticker, end, accepted, shares=shares * (1 + 0.01 * q), revenue=1e9 * (1 + 0.02 * q), **kw))
        end = end + pd.offsets.QuarterEnd()
    return rows


def _prepare(rows, corrections=ALL, key='ticker', events=None, **kw):
    if events is None and 'split_basis' in corrections:
        events = {}
    return fs.prepare_rows(pd.DataFrame(rows), key, events=events, corrections=frozenset(corrections), **kw)


class TestEnabledCorrections:
    def test_everything_is_on_by_default(self, monkeypatch):
        monkeypatch.delenv('MMR_FUNDAMENTALS_CORRECTIONS', raising=False)
        assert fs.enabled_corrections() == ALL == frozenset(fs.CORRECTIONS)

    @pytest.mark.parametrize('raw, expected', [
        ('none', frozenset()),
        ('', frozenset()),
        ('notices', {'notices'}),
        (' notices , split_basis ', {'notices', 'split_basis'}),
        ('none,units', {'units'}),
    ])
    def test_the_variable_names_a_subset(self, monkeypatch, raw, expected):
        monkeypatch.setenv('MMR_FUNDAMENTALS_CORRECTIONS', raw)
        assert fs.enabled_corrections() == frozenset(expected)

    def test_a_typo_is_refused_not_ignored(self, monkeypatch):
        """Measuring the wrong thing quietly is the failure this exists to end."""
        monkeypatch.setenv('MMR_FUNDAMENTALS_CORRECTIONS', 'notices,split_bias')
        with pytest.raises(ValueError, match=r"unknown correction\(s\) \['split_bias'\]"):
            fs.enabled_corrections()


class TestRowsAsStored:
    def test_with_nothing_on_the_rows_are_what_they_were(self):
        """The baseline the 2026-09-29 result was measured on, reproducible."""
        rows = _quarters('AAA', 5) + [_row('AAA', '2024-06-30', '2024-07-20 17:00', form='NT 10-Q', shares=-5.0)]
        out = _prepare(rows, corrections=())
        assert len(out) == 6
        assert list(out['shares_diluted'])[-2:] == [pytest.approx(104e6), -5.0]      # sorted by known_at; untouched
        expected = pd.Series([r['revenue'] for r in sorted(rows, key=lambda r: r['known_at'])]).rolling(4).sum()
        assert out['revenue_ttm'].tolist()[3:] == pytest.approx(expected.tolist()[3:])
        assert out['revenue_ttm'].isna().tolist()[:3] == [True, True, True]

    def test_every_flow_gets_a_trailing_sum_and_stocks_do_not(self):
        out = _prepare(_quarters('AAA', 4))
        assert {c for c in out.columns if c.endswith('_ttm')} == {
            'revenue_ttm', 'net_income_ttm', 'gross_profit_ttm', 'cash_from_operations_ttm'}
        assert out['net_income_ttm'].iloc[-1] == pytest.approx(out['revenue_ttm'].iloc[-1] * 0.1)

    def test_names_are_kept_apart(self):
        out = _prepare(_quarters('AAA', 4) + _quarters('BBB', 4, shares=7e6))
        assert out.groupby('ticker')['revenue_ttm'].count().to_dict() == {'AAA': 1, 'BBB': 1}
        assert set(out.loc[out['ticker'] == 'BBB', 'shares_diluted'].round(-5)) <= {7.0e6, 7.1e6, 7.2e6}


class TestLateFilingNotices:
    def test_a_row_dated_to_a_notice_is_dropped(self):
        rows = _quarters('AAA', 4) + [_row('AAA', '2024-03-31', '2024-05-12 17:00', form='NT 10-Q')]
        assert 'NT 10-Q' in set(_prepare(rows, corrections=())['form_type'])
        out = _prepare(rows, corrections={'notices'})
        assert set(out['form_type']) == {'10-Q'} and len(out) == 4


class TestAnnualShareCounts:
    """The retired vendor's fiscal-Q4 "share count" is annual average minus
    mean(Q1..Q3): -47,029,000 for AAPL FY2025; small and POSITIVE for a
    company issuing stock."""

    def _year(self, annual_shares, **annual_kw):
        rows = _quarters('AAA', 3)
        rows.append(_row('AAA', '2023-12-31', '2024-02-10 16:30', form='10-K', shares=annual_shares, **annual_kw))
        rows.append(_row('AAA', '2024-03-31', '2024-05-05 16:30', shares=103e6))
        return rows

    @pytest.mark.parametrize('garbage', [-47_029_000.0, 915_000.0, 0.0])
    def test_the_count_at_a_10k_is_the_last_10qs(self, garbage):
        out = _prepare(self._year(garbage), corrections={'annual_shares'})
        assert out['shares_diluted'].tolist() == pytest.approx([100e6, 101e6, 102e6, 102e6, 103e6])

    def test_without_the_correction_the_garbage_is_the_share_count(self):
        assert _prepare(self._year(915_000.0), corrections=())['shares_diluted'].iloc[3] == 915_000.0

    def test_an_amended_annual_report_is_an_annual_report(self):
        rows = self._year(915_000.0)
        rows[3]['form_type'] = '10-K/A'
        assert _prepare(rows, corrections={'annual_shares'})['shares_diluted'].iloc[3] == pytest.approx(102e6)

    def test_a_10k_with_no_earlier_count_has_none(self):
        """Nothing is carried BACK from a later filing."""
        rows = [_row('AAA', '2023-12-31', '2024-02-10 16:30', form='10-K', shares=915_000.0),
                _row('AAA', '2024-03-31', '2024-05-05 16:30', shares=103e6)]
        out = _prepare(rows, corrections={'annual_shares'})
        assert np.isnan(out['shares_diluted'].iloc[0]) and out['shares_diluted'].iloc[1] == 103e6

    def test_a_fiscal_year_end_row_from_the_current_endpoint_is_a_share_count(self):
        """It reports a real quarterly figure (AAPL FY2026 Q4: 14.63B)."""
        rows = self._year(101.5e6, source='financials_v1', fetched_at=pd.Timestamp('2024-02-12', tz='UTC'))
        for row in rows[:3] + rows[4:]:
            row.update(source=None, fetched_at=pd.Timestamp('2023-06-01', tz='UTC'))
        out = _prepare(rows, corrections={'annual_shares'})
        assert out['shares_diluted'].iloc[3] == pytest.approx(101.5e6)

    @pytest.mark.parametrize('source', [None, '', '  ', float('nan')])
    def test_a_row_with_no_source_is_legacy(self, source):
        rows = self._year(915_000.0, source=source)
        for row in rows[:3] + rows[4:]:
            row['source'] = None
        assert _prepare(rows, corrections={'annual_shares'})['shares_diluted'].iloc[3] == pytest.approx(102e6)


NVDA_EVENTS = split_events([{'ticker': 'NVDA', 'execution_date': '2021-07-20', 'split_from': 1, 'split_to': 4},
                            {'ticker': 'NVDA', 'execution_date': '2024-06-10', 'split_from': 1, 'split_to': 10}])


class TestSplitBasis:
    def _nvda(self):
        return [_row('NVDA', '2021-05-02', '2021-05-26 16:25', shares=632e6),
                _row('NVDA', '2021-08-01', '2021-08-20 16:25', shares=2_532e6),
                _row('NVDA', '2024-07-28', '2024-08-28 16:25', shares=24_848e6)]

    def test_each_count_is_put_on_the_basis_of_the_adjusted_prices(self):
        out = _prepare(self._nvda(), corrections={'split_basis'}, events=NVDA_EVENTS)
        assert out['shares_diluted'].tolist() == pytest.approx([632e6 * 40, 2_532e6 * 10, 24_848e6])

    def test_the_market_cap_is_then_the_true_one(self):
        """2021-07-01: traded at 808, stored (adjusted) close 20.2."""
        out = _prepare(self._nvda(), corrections={'split_basis'}, events=NVDA_EVENTS)
        assert 20.2 * out['shares_diluted'].iloc[0] == pytest.approx(808.0 * 632e6)
        assert 20.2 * 632e6 == pytest.approx(808.0 * 632e6 / 40)               # what it was: a fortieth

    def test_a_split_the_prices_are_not_adjusted_to_is_left_out(self):
        out = _prepare(self._nvda(), corrections={'split_basis'}, events=NVDA_EVENTS,
                       through=dt.date(2024, 6, 9))
        assert out['shares_diluted'].tolist() == pytest.approx([632e6 * 4, 2_532e6, 24_848e6])

    def test_a_split_the_stored_prices_never_carried_is_left_out(self):
        out = _prepare(self._nvda(), corrections={'split_basis'}, events=NVDA_EVENTS,
                       unreflected=frozenset({('NVDA', dt.date(2021, 7, 20))}))
        assert out['shares_diluted'].tolist() == pytest.approx([632e6 * 10, 2_532e6 * 10, 24_848e6])

    def test_a_name_with_no_splits_is_untouched(self):
        out = _prepare(_quarters('AAA', 3), corrections={'split_basis'}, events=NVDA_EVENTS)
        assert out['shares_diluted'].tolist() == pytest.approx([100e6, 101e6, 102e6])

    def test_the_filing_day_decides_not_the_day_it_is_treated_as_known(self):
        """The leak probe delays `known_at`; the count was still filed when it
        was filed."""
        rows = self._nvda()
        for row in rows:
            row['known_at'] = row['known_at'] + pd.Timedelta(days=90)           # past the 2021-07-20 split
        out = _prepare(rows, corrections={'split_basis'}, events=NVDA_EVENTS)
        assert out['shares_diluted'].iloc[0] == pytest.approx(632e6 * 40)

    def test_the_filing_day_is_new_yorks(self):
        """Accepted 2021-07-19 21:00 New York is the 20th in UTC - the split day."""
        rows = [_row('NVDA', '2021-05-02', '2021-07-19 21:00', shares=632e6)]
        out = _prepare(rows, corrections={'split_basis'}, events=NVDA_EVENTS)
        assert out['shares_diluted'].iloc[0] == pytest.approx(632e6 * 40)

    def test_a_row_from_the_current_endpoint_is_on_the_basis_of_its_fetch_day(self):
        """That endpoint serves counts already adjusted to the day it is asked:
        NVDA's 2021-05-02 quarter reads 25.28B, not 632M."""
        rows = [_row('NVDA', '2021-05-02', '2021-05-26 16:25', shares=25_280e6, source='financials_v1',
                     fetched_at=pd.Timestamp('2026-11-15 12:00', tz='UTC')),
                _row('NVDA', '2021-08-01', '2021-08-20 16:25', shares=2_532e6 * 4, source='financials_v1',
                     fetched_at=pd.Timestamp('2024-01-10 12:00', tz='UTC')),       # fetched between the splits
                _row('NVDA', '2021-10-31', '2021-11-18 16:25', shares=2_540e6, source=None,
                     fetched_at=pd.Timestamp('2026-07-29 12:00', tz='UTC'))]       # legacy: as filed
        out = _prepare(rows, corrections={'split_basis'}, events=NVDA_EVENTS)
        assert out['shares_diluted'].tolist() == pytest.approx([25_280e6, 2_532e6 * 4 * 10, 2_540e6 * 10])

    def test_without_split_history_it_refuses(self):
        with pytest.raises(RuntimeError, match="'corporate_splits' table is missing"):
            fs.prepare_rows(pd.DataFrame(self._nvda()), 'ticker', events=None,
                            corrections=frozenset({'split_basis'}))

    def test_it_is_only_asked_for_when_the_correction_is_on(self):
        out = fs.prepare_rows(pd.DataFrame(self._nvda()), 'ticker', events=None, corrections=frozenset())
        assert out['shares_diluted'].iloc[0] == 632e6


class TestUnits:
    def test_a_count_in_millions_is_replaced_by_the_one_before(self):
        rows = _quarters('MCD', 5, shares=730e6)
        rows[3]['shares_diluted'] = 725.9
        out = _prepare(rows, corrections={'units'})
        assert out['shares_diluted'].tolist() == pytest.approx([730e6, 737.3e6, 744.6e6, 744.6e6, 759.2e6])

    def test_a_split_is_not_a_unit_error_once_the_basis_is_right(self):
        """Run after the split basis: 632M then 2,532M is one level, not two."""
        rows = [_row('NVDA', '2021-02-28', '2021-03-10 16:25', shares=630e6),
                _row('NVDA', '2021-05-02', '2021-05-26 16:25', shares=632e6),
                _row('NVDA', '2021-08-01', '2021-08-20 16:25', shares=2_532e6),
                _row('NVDA', '2021-10-31', '2021-11-18 16:25', shares=2_540e6)]
        out = _prepare(rows, corrections={'split_basis', 'units'}, events=NVDA_EVENTS)
        assert out['shares_diluted'].tolist() == pytest.approx([630e6 * 40, 632e6 * 40, 2_532e6 * 10, 2_540e6 * 10])
        # ...and without the basis the same four are a level change, the first of which is held back.
        alone = _prepare(rows, corrections={'units'})
        assert alone['shares_diluted'].tolist() == pytest.approx([630e6, 632e6, 632e6, 2_540e6])

    def test_one_names_error_does_not_touch_another(self):
        rows = _quarters('AAA', 3) + _quarters('BBB', 3, shares=100.0)
        out = _prepare(rows, corrections={'units'})
        assert out.loc[out['ticker'] == 'BBB', 'shares_diluted'].tolist() == pytest.approx([100.0, 101.0, 102.0])


class TestConsecutiveQuarters:
    def test_four_consecutive_quarters_sum(self):
        out = _prepare(_quarters('AAA', 5), corrections={'periods'})
        assert out['revenue_ttm'].notna().tolist() == [False, False, False, True, True]

    def test_four_rows_spanning_a_missing_quarter_do_not(self):
        rows = _quarters('AAA', 6)
        del rows[2]                                                    # 2023-09-30 was never stored
        out = _prepare(rows, corrections={'periods'})
        assert out['revenue_ttm'].notna().tolist() == [False, False, False, False, False]
        loose = _prepare(rows, corrections=())
        assert loose['revenue_ttm'].notna().tolist() == [False, False, False, True, True]

    def test_the_sum_resumes_once_four_in_a_row_exist_again(self):
        rows = _quarters('AAA', 8)
        del rows[2]
        out = _prepare(rows, corrections={'periods'})
        assert out['revenue_ttm'].notna().tolist() == [False] * 5 + [True, True]

    def test_a_53_week_year_is_still_consecutive(self):
        rows = [_row('AAPL', end, acc) for end, acc in (
            ('2022-12-31', '2023-02-03 06:01'), ('2023-04-01', '2023-05-05 06:01'),
            ('2023-07-01', '2023-08-04 06:01'), ('2023-09-30', '2023-11-03 06:01'))]
        assert _prepare(rows, corrections={'periods'})['revenue_ttm'].notna().tolist() == [False, False, False, True]

    def test_a_sixteen_week_quarter_is_still_a_quarter(self):
        """A 16/12/12/12-week year (KR): 112, 84 and 84 days apart."""
        rows = [_row('KR', end, acc) for end, acc in (
            ('2024-02-03', '2024-04-02 16:30'), ('2024-05-25', '2024-06-28 16:30'),
            ('2024-08-17', '2024-09-20 16:30'), ('2024-11-09', '2024-12-13 16:30'))]
        assert (fs.QUARTER_MIN_DAYS, fs.QUARTER_MAX_DAYS) == (70, 120)
        assert _prepare(rows, corrections={'periods'})['revenue_ttm'].notna().tolist() == [False, False, False, True]

    @pytest.mark.parametrize('gap_days, consecutive', [(69, False), (70, True), (120, True), (121, False)])
    def test_the_bounds_on_a_quarter_are_inclusive(self, gap_days, consecutive):
        ends = [pd.Timestamp('2023-03-31')]
        for step in (91, 91, gap_days):
            ends.append(ends[-1] + pd.Timedelta(days=step))
        rows = [_row('AAA', end, (end + pd.Timedelta(days=30)).strftime('%Y-%m-%d') + ' 16:30') for end in ends]
        assert bool(_prepare(rows, corrections={'periods'})['revenue_ttm'].notna().iloc[-1]) is consecutive

    def test_the_same_period_twice_is_not_four_quarters(self):
        """A restatement stored beside the original would be counted twice."""
        rows = _quarters('AAA', 3) + [_row('AAA', '2023-09-30', '2023-12-15 16:30', revenue=1.5e9)]
        out = _prepare(rows, corrections={'periods'})
        assert out['revenue_ttm'].notna().tolist() == [False, False, False, False]
        assert _prepare(rows, corrections=())['revenue_ttm'].notna().tolist() == [False, False, False, True]

    def test_a_restated_quarter_enters_the_sum_once_at_its_latest_version(self):
        rows = _quarters('AAA', 4)                                         # revenue 1.00, 1.02, 1.04, 1.06 (x1e9)
        rows.append(_row('AAA', '2023-09-30', '2024-02-20 16:30', revenue=2e9))      # Q3 restated, known later
        rows.append(_row('AAA', '2024-03-31', '2024-05-05 16:30', revenue=1.08e9))
        out = _prepare(rows, corrections={'periods'})
        sums = out['revenue_ttm'].tolist()
        assert sums[3] == pytest.approx(1e9 + 1.02e9 + 1.04e9 + 1.06e9)             # as first known
        assert sums[4] == pytest.approx(1e9 + 1.02e9 + 2e9 + 1.06e9)                # the restatement, once
        assert sums[5] == pytest.approx(1.02e9 + 2e9 + 1.06e9 + 1.08e9)

    def test_an_unknown_quarter_inside_the_four_makes_the_sum_unknown(self):
        rows = _quarters('AAA', 4)
        rows[1]['revenue'] = None
        assert _prepare(rows, corrections={'periods'})['revenue_ttm'].isna().all()


class TestTogether:
    def test_the_order_is_notices_annual_basis_units_then_carry_forward(self):
        rows = [_row('NVDA', '2021-01-31', '2021-02-26 16:25', form='10-K', shares=-3e6),
                _row('NVDA', '2021-05-02', '2021-05-10 17:00', form='NT 10-Q', shares=1.0),
                _row('NVDA', '2021-05-02', '2021-05-26 16:25', shares=632e6),
                _row('NVDA', '2021-08-01', '2021-08-20 16:25', shares=2_532.0),          # reported in millions
                _row('NVDA', '2021-10-31', '2021-11-18 16:25', shares=2_540e6),
                _row('NVDA', '2022-01-30', '2022-03-18 16:25', form='10-K', shares=11e6)]
        out = _prepare(rows, events=NVDA_EVENTS)
        assert out['form_type'].tolist() == ['10-K', '10-Q', '10-Q', '10-Q', '10-K']
        counts = out['shares_diluted'].tolist()
        assert np.isnan(counts[0])                                     # nothing before it to carry
        assert counts[1:] == pytest.approx([632e6 * 40, 632e6 * 40, 2_540e6 * 10, 2_540e6 * 10])

    def test_keyed_by_conid_too(self):
        rows = _quarters('AAA', 4, conid='1001') + _quarters('BBB', 4, conid='1002', shares=5e6)
        out = _prepare(rows, key='conid')
        assert out.groupby('conid')['revenue_ttm'].count().to_dict() == {'1001': 1, '1002': 1}


# --------------------------------------------------------------------------
# what the store says about its prices
# --------------------------------------------------------------------------

@pytest.fixture
def hist(tmp_path):
    db = DuckDBConnection(str(tmp_path / 'hist.duckdb'))
    db.execute('CREATE TABLE pit_daily_bars(day DATE, ticker VARCHAR, open DOUBLE, high DOUBLE, low DOUBLE, '
               'close DOUBLE, volume DOUBLE, dollar_volume DOUBLE, dv_rank INTEGER)', fetch='none')
    return db


def _splits_table(db, rows):
    db.execute('CREATE TABLE corporate_splits(ticker VARCHAR, conid VARCHAR, execution_date DATE, '
               'split_to DOUBLE, split_from DOUBLE, fetched_at TIMESTAMPTZ, PRIMARY KEY(ticker, execution_date))',
               fetch='none')
    for ticker, day, split_from, split_to in rows:
        db.execute('INSERT INTO corporate_splits VALUES (?, NULL, ?, ?, ?, now())',
                   [ticker, day, split_to, split_from], fetch='none')


def _bars(db, ticker, closes, start='2021-07-12'):
    for n, close in enumerate(closes):
        day = (pd.Timestamp(start) + pd.offsets.BDay(n)).date()
        db.execute('INSERT INTO pit_daily_bars VALUES (?,?,?,?,?,?,?,?,?)',
                   [day, ticker, close, close, close, close, 1e6, close * 1e6, 1], fetch='none')


class TestPriceBasisDay:
    def test_a_store_that_does_not_say_answers_none(self, hist):
        assert fs.price_basis_day(hist) is None

    def test_the_newest_recorded_day_is_the_answer(self, hist):
        hist.execute('CREATE TABLE pit_price_basis(adjusted_through DATE, recorded_at TIMESTAMPTZ, note VARCHAR)',
                     fetch='none')
        assert fs.price_basis_day(hist) is None                        # the table, and nothing in it
        for day in ('2026-07-29', '2026-11-14', '2026-09-01'):
            hist.execute('INSERT INTO pit_price_basis VALUES (?, now(), ?)', [day, 'test'], fetch='none')
        assert fs.price_basis_day(hist) == dt.date(2026, 11, 14)


class TestSplitInputs:
    PX = pd.DataFrame({'NVDA': [1.0, 1.0]}, index=pd.to_datetime(['2021-07-12', '2024-06-28']))

    def _rows(self, *tickers):
        return pd.DataFrame({'ticker': list(tickers), 'conid': [''] * len(tickers)})

    def test_no_split_table_is_no_events(self, hist):
        assert fs._split_inputs(hist, {'NVDA'}, True, self.PX, 'ticker', self._rows('NVDA')) == (
            None, None, frozenset())

    def test_events_are_read_for_the_names_asked_about_only(self, hist):
        _splits_table(hist, [('NVDA', '2021-07-20', 1, 4), ('AAPL', '2020-08-31', 1, 4)])
        events, through, unreflected = fs._split_inputs(hist, {'NVDA'}, True, self.PX, 'ticker', self._rows('NVDA'))
        assert events == {'NVDA': [(dt.date(2021, 7, 20), 4.0)]}
        assert unreflected == frozenset()

    def test_without_a_recorded_basis_the_prices_are_taken_to_be_adjusted_to_their_last_day(self, hist):
        _splits_table(hist, [('NVDA', '2021-07-20', 1, 4)])
        _, through, _ = fs._split_inputs(hist, {'NVDA'}, True, self.PX, 'ticker', self._rows('NVDA'))
        assert through == dt.date(2024, 6, 28)

    def test_a_recorded_basis_wins_for_the_point_in_time_panel(self, hist):
        """A snapshot cut at 2024-06-28 still carries prices adjusted in July 2026."""
        _splits_table(hist, [('NVDA', '2021-07-20', 1, 4)])
        hist.execute('CREATE TABLE pit_price_basis(adjusted_through DATE, recorded_at TIMESTAMPTZ, note VARCHAR)',
                     fetch='none')
        hist.execute("INSERT INTO pit_price_basis VALUES ('2026-07-29', now(), 'build')", fetch='none')
        _, through, _ = fs._split_inputs(hist, {'NVDA'}, True, self.PX, 'ticker', self._rows('NVDA'))
        assert through == dt.date(2026, 7, 29)
        # ...and is about pit_daily_bars only: another panel is adjusted to its own last day.
        _, through, _ = fs._split_inputs(hist, {'NVDA'}, False, self.PX, 'ticker', self._rows('NVDA'))
        assert through == dt.date(2024, 6, 28)

    def test_a_split_the_stored_series_shows_raw_is_unreflected(self, hist):
        """CGNX 2017-12-04: 138.49 then 63.31, a 2:1 the vendor's series missed."""
        _splits_table(hist, [('CGNX', '2021-07-20', 1, 2), ('NVDA', '2021-07-20', 1, 4)])
        _bars(hist, 'CGNX', [138.0, 137.5, 139.0, 138.2, 137.9, 138.49, 63.31, 64.0])        # raw jump on the 20th
        _bars(hist, 'NVDA', [200.0, 201.0, 199.0, 198.0, 200.5, 201.2, 199.9, 202.0])        # adjusted: ordinary day
        px = pd.DataFrame({'CGNX': [1.0, 1.0], 'NVDA': [1.0, 1.0]}, index=pd.to_datetime(['2021-07-12', '2021-07-21']))
        _, _, unreflected = fs._split_inputs(hist, {'CGNX', 'NVDA'}, True, px, 'ticker', self._rows('CGNX', 'NVDA'))
        assert unreflected == frozenset({('CGNX', dt.date(2021, 7, 20))})

    def test_a_split_outside_the_stored_days_cannot_be_checked_and_counts_as_carried(self, hist):
        _splits_table(hist, [('NVDA', '2025-03-03', 1, 2)])
        _bars(hist, 'NVDA', [200.0, 201.0])
        _, _, unreflected = fs._split_inputs(hist, {'NVDA'}, True, self.PX, 'ticker', self._rows('NVDA'))
        assert unreflected == frozenset()

    def test_the_present_day_panel_is_checked_on_its_own_prices(self, hist):
        """Keyed by conId; there is no pit series to read."""
        _splits_table(hist, [('CGNX', '2021-07-20', 1, 2)])
        days = pd.bdate_range('2021-07-12', periods=8)
        px = pd.DataFrame({'555': [138.0, 137.5, 139.0, 138.2, 137.9, 138.49, 63.31, 64.0]}, index=days)
        rows = pd.DataFrame({'ticker': ['CGNX'], 'conid': ['555']})
        _, _, unreflected = fs._split_inputs(hist, {'CGNX'}, False, px, 'conid', rows)
        assert unreflected == frozenset({('CGNX', dt.date(2021, 7, 20))})
