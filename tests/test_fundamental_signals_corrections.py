"""scripts/fundamental_signals.py: what stands between the stored fundamentals
rows and a market cap (docs/RESEARCH_LOG.md, 2026-11-15).

The leak probe this script carries moves WHEN a number is known. It cannot see
a number that is wrong, one on a different basis from the price beside it, or
another company's. Such defects sat in the stored rows; `prepare_rows` and
`build` repair them and each repair can be switched off to measure what it
was worth.
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
ALL = frozenset({'notices', 'annual_shares', 'split_basis', 'units', 'periods', 'continuity', 'turnover'})


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
        assert set(out.loc[out['ticker'] == 'BBB', 'shares_diluted'].dropna().round(-5)) == {7.1e6, 7.2e6}


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


def _counts(out, ticker=None):
    """shares_diluted as a list with None for unknown."""
    frame = out if ticker is None else out[out['ticker'] == ticker]
    return [None if pd.isna(v) else float(v) for v in frame['shares_diluted']]


class TestUnits:
    def test_a_series_has_no_count_until_two_filings_agree(self):
        """TEVA's first stored figure is 2,000,000; every one after is a
        billion."""
        rows = _quarters('TEVA', 3, shares=1_018e6)
        rows[0]['shares_diluted'] = 2_000_000.0
        assert _counts(_prepare(rows, corrections={'units'})) == pytest.approx([None, 1_028.18e6, 1_038.36e6])

    def test_a_count_printed_in_millions_is_read_in_units(self):
        """MCD from 2024: 725.9, 722.0 - quarter after quarter."""
        rows = _quarters('MCD', 5, shares=730e6)
        rows[3]['shares_diluted'] = 751.9
        rows[4]['shares_diluted'] = 759.2
        out = _prepare(rows, corrections={'units'})
        assert _counts(out) == pytest.approx([None, 737.3e6, 744.6e6, 751.9e6, 759.2e6])

    def test_a_number_no_scale_explains_is_replaced_by_the_count_before(self):
        rows = _quarters('STRL', 4, shares=16e6)
        rows[2]['shares_diluted'] = 0.01
        out = _prepare(rows, corrections={'units'})
        assert _counts(out) == pytest.approx([None, 16.16e6, 16.16e6, 16.48e6])

    def test_a_split_is_not_a_unit_error_once_the_basis_is_right(self):
        """Run after the split basis: 632M then 2,532M is one level, not two."""
        rows = [_row('NVDA', '2021-02-28', '2021-03-10 16:25', shares=630e6),
                _row('NVDA', '2021-05-02', '2021-05-26 16:25', shares=632e6),
                _row('NVDA', '2021-08-01', '2021-08-20 16:25', shares=2_532e6),
                _row('NVDA', '2021-10-31', '2021-11-18 16:25', shares=2_540e6)]
        out = _prepare(rows, corrections={'split_basis', 'units'}, events=NVDA_EVENTS)
        assert _counts(out) == pytest.approx([None, 632e6 * 40, 2_532e6 * 10, 2_540e6 * 10])
        # ...and without the basis the same four are a level change, the first of which is held back.
        alone = _prepare(rows, corrections={'units'})
        assert _counts(alone) == pytest.approx([None, 632e6, 632e6, 2_540e6])

    def test_one_names_error_does_not_touch_another(self):
        rows = _quarters('AAA', 3) + _quarters('BBB', 3, shares=100.0)             # BBB prints millions
        out = _prepare(rows, corrections={'units'})
        assert _counts(out, 'AAA') == pytest.approx([None, 101e6, 102e6])
        assert _counts(out, 'BBB') == pytest.approx([None, 101e6, 102e6])


class TestContinuity:
    """A ticker is not one company for ever, and a filing is not current for
    ever. B was Barnes Group (51M shares) until January 2025 and is Barrick
    since May; CTRA was Contura Energy (19M) and is Coterra (800M); EXE's and
    BKR's last filings under an earlier owner are from 2015 and 2013."""

    def _handover(self):
        """Contura's last three quarters, then - fourteen months on - Coterra's."""
        old = _quarters('CTRA', 3, start='2020-03-31', shares=18.2e6)
        new = _quarters('CTRA', 3, start='2022-03-31', shares=814e6)
        return old + new

    def test_filings_further_apart_than_an_issuers_can_be_are_separate_series(self):
        out = _prepare(self._handover(), corrections={'continuity'})
        assert out['series_run'].tolist() == [0, 0, 0, 1, 1, 1]

    def test_the_limit_is_two_hundred_days_and_exactly_that_is_one_series(self):
        from trader.data.share_basis import MAX_DAYS_BETWEEN_FILINGS
        assert MAX_DAYS_BETWEEN_FILINGS == 200
        first = _row('AAA', '2023-03-31', '2023-05-05 16:30')
        # Elapsed time, not wall clock: New York left daylight saving in between, so 200 days after
        # 16:30 on 5 May is 15:30 on 21 November.
        at_limit = _row('AAA', '2023-09-30', '2023-11-21 15:30')
        assert at_limit['known_at'] - first['known_at'] == pd.Timedelta(days=200)
        assert _prepare([first, at_limit], corrections={'continuity'})['series_run'].tolist() == [0, 0]
        beyond = _row('AAA', '2023-09-30', '2023-11-21 15:31')
        assert _prepare([first, beyond], corrections={'continuity'})['series_run'].tolist() == [0, 1]

    def test_a_missing_report_does_not_break_a_series(self):
        """Q3's 10-Q in early November, Q1's in early May, the 10-K missing:
        about 182 days. 1.6% of consecutive filings in the store."""
        rows = [_row('AAA', '2023-09-30', '2023-11-04 16:30'), _row('AAA', '2024-03-31', '2024-05-04 16:30')]
        assert _prepare(rows, corrections={'continuity'})['series_run'].tolist() == [0, 0]

    def test_the_new_companys_count_is_not_measured_against_the_old_ones(self):
        """Without the break, Coterra's first 814M is "an error" beside
        Contura's 18M and the 18M is carried on to it."""
        without = _prepare(self._handover(), corrections={'units'})
        assert _counts(without) == pytest.approx([None, 18.382e6, 18.564e6, 18.564e6, 822.14e6, 830.28e6])
        out = _prepare(self._handover(), corrections={'units', 'continuity'})
        assert _counts(out) == pytest.approx([None, 18.382e6, 18.564e6, None, 822.14e6, 830.28e6])

    def test_a_count_is_not_carried_from_one_series_into_the_next(self):
        """BKR: 9,675,000 in 2013 (another company), then filings from 2020
        with no count at all."""
        rows = _quarters('BKR', 3, start='2012-12-31', shares=9.6e6) + [
            _row('BKR', '2020-03-31', '2020-04-24 07:40', shares=None),
            _row('BKR', '2020-06-30', '2020-07-24 07:40', shares=None)]
        assert _counts(_prepare(rows, corrections={'units'}))[-2:] == pytest.approx([9.792e6, 9.792e6])
        assert _counts(_prepare(rows, corrections={'units', 'continuity'}))[-2:] == [None, None]

    def test_within_a_series_a_count_is_carried_over_two_filings_at_most(self):
        rows = _quarters('AAA', 6)
        for row in rows[2:]:
            row['shares_diluted'] = None
        assert _counts(_prepare(rows, corrections={'units', 'continuity'})) == pytest.approx(
            [None, 101e6, 101e6, 101e6, None, None])
        assert _counts(_prepare(rows, corrections={'units'})) == pytest.approx(
            [None, 101e6, 101e6, 101e6, 101e6, 101e6])

    def test_one_names_gap_does_not_split_another(self):
        rows = self._handover() + _quarters('AAA', 6, start='2020-03-31')
        out = _prepare(rows, corrections={'continuity'})
        assert out.loc[out['ticker'] == 'AAA', 'series_run'].tolist() == [0] * 6
        assert out.loc[out['ticker'] == 'CTRA', 'series_run'].tolist() == [0, 0, 0, 1, 1, 1]

    def test_a_filing_is_the_latest_word_for_two_hundred_days(self):
        rows = _quarters('AAA', 2)
        out = _prepare(rows, corrections={'continuity'})
        assert (out['valid_until'] - out['known_at']).tolist() == [pd.Timedelta(days=200)] * 2
        assert _prepare(rows, corrections=())['valid_until'].isna().all()
        assert _prepare(rows, corrections=())['series_run'].tolist() == [0, 0]

    def test_a_trailing_sum_never_spans_the_handover(self):
        """Already true of `periods` (the quarters are not consecutive); held
        here because a sum of two companies' revenue is the same mistake."""
        out = _prepare(_quarters('CTRA', 4, start='2020-03-31') + _quarters('CTRA', 3, start='2022-03-31'))
        assert out['revenue_ttm'].notna().tolist() == [False, False, False, True, False, False, False]


class TestAsOfFrames:
    """Which filing answers for a session."""

    DAYS = pd.to_datetime(['2023-05-05', '2023-05-08', '2023-08-07', '2023-11-20', '2023-11-21', '2024-03-01'])

    def _px(self, *names):
        return pd.DataFrame(1.0, index=self.DAYS, columns=list(names))

    def test_a_filing_answers_from_the_first_close_after_it(self):
        rows = _prepare([_row('AAA', '2023-03-31', '2023-05-05 16:30', revenue=1e9)], corrections=())
        frames = fs.as_of_frames(rows, 'ticker', self._px('AAA'), fields=('revenue',))
        # accepted 16:30 on the 5th: after that day's close, so known from the 8th - and, unlimited, for ever
        assert frames['revenue']['AAA'].tolist() == pytest.approx([np.nan, 1e9, 1e9, 1e9, 1e9, 1e9], nan_ok=True)

    def test_and_stops_answering_when_it_is_no_longer_the_latest_word(self):
        """B: Barnes Group's last 10-Q, October 2024; the ticker is Barrick's
        from May 2025 and Barrick files no 10-Q."""
        rows = _prepare([_row('AAA', '2023-03-31', '2023-05-05 15:30', revenue=1e9)], corrections={'continuity'})
        # 200 days of elapsed time: 14:30 New York on 21 November (the clocks went back in between)
        assert rows['valid_until'].iloc[0] == pd.Timestamp('2023-11-21 14:30', tz='America/New_York')
        frames = fs.as_of_frames(rows, 'ticker', self._px('AAA'), fields=('revenue',))
        # ...so it answers at the close of the 20th and not at the close of the 21st.
        assert frames['revenue']['AAA'].tolist() == pytest.approx([1e9, 1e9, 1e9, 1e9, np.nan, np.nan], nan_ok=True)

    def test_a_successor_takes_over_and_restarts_the_clock(self):
        rows = _prepare([_row('AAA', '2023-03-31', '2023-05-05 15:30', revenue=1e9),
                         _row('AAA', '2023-06-30', '2023-08-04 16:30', revenue=2e9)], corrections={'continuity'})
        frames = fs.as_of_frames(rows, 'ticker', self._px('AAA'), fields=('revenue',))
        # the second is good until 2024-02-20
        assert frames['revenue']['AAA'].tolist() == pytest.approx([1e9, 1e9, 2e9, 2e9, 2e9, np.nan], nan_ok=True)

    def test_a_name_with_no_filings_and_a_filer_outside_the_panel(self):
        rows = _prepare(_quarters('ZZZ', 2), corrections=())
        frames = fs.as_of_frames(rows, 'ticker', self._px('AAA'), fields=('revenue',))
        assert list(frames['revenue'].columns) == ['AAA'] and frames['revenue']['AAA'].isna().all()

    def test_every_field_the_signals_need_is_placed(self):
        rows = _prepare(_quarters('AAA', 5))
        for field in ('stockholders_equity', 'total_assets'):
            rows[field] = 1.0
        frames = fs.as_of_frames(rows, 'ticker', pd.DataFrame(1.0, index=pd.to_datetime(['2024-06-28']), columns=['AAA']))
        assert set(frames) == set(fs.AS_OF_FIELDS) and 'shares_diluted' in frames
        assert frames['revenue_ttm']['AAA'].iloc[0] == pytest.approx(rows['revenue_ttm'].iloc[-1])


class TestContradictedByTrading:
    """The last check: a market cap smaller than two typical days of the
    name's own trading. TEVA in mid-2018 with a "count" of 2,000,000 was a $48M
    company trading $246M a day."""

    DAYS = pd.bdate_range('2024-01-02', periods=30)

    def _frames(self, shares, volume, price=10.0):
        px = pd.DataFrame({'AAA': price}, index=self.DAYS)
        return (pd.DataFrame({'AAA': shares}, index=self.DAYS, dtype=float), px,
                pd.DataFrame({'AAA': volume}, index=self.DAYS, dtype=float))

    def test_a_cap_the_tape_contradicts_is_unknown(self):
        shares, px, volume = self._frames(2e6, 246e6)                  # cap $20M, $246M a day
        out = fs.contradicted_by_trading(shares, px, volume)
        assert out['AAA'].iloc[:4].tolist() == [2e6] * 4               # fewer than five sessions: no typical day yet
        assert out['AAA'].iloc[4:].isna().all()

    def test_an_ordinary_name_is_untouched(self):
        shares, px, volume = self._frames(1e9, 246e6)                  # cap $10B, 2.5% a day
        assert fs.contradicted_by_trading(shares, px, volume).equals(shares)

    def test_the_threshold_is_half_the_cap_on_a_typical_day(self):
        shares, px, volume = self._frames(1e8, 0.5e9)                  # exactly half of $1B
        assert fs.contradicted_by_trading(shares, px, volume).equals(shares)
        shares, px, volume = self._frames(1e8, 0.5e9 + 1)
        assert fs.contradicted_by_trading(shares, px, volume)['AAA'].iloc[4:].isna().all()

    def test_one_frantic_day_is_not_a_typical_one(self):
        """The median of the window: a squeeze day does not cost a name its cap."""
        volume = [20e6] * 30
        volume[20] = 5e9
        shares, px, frame = self._frames(1e8, volume)
        assert fs.contradicted_by_trading(shares, px, frame).equals(shares)

    def test_it_looks_at_nothing_later_than_the_cell_it_judges(self):
        quiet = [20e6] * 30
        later = [20e6] * 15 + [5e9] * 15
        shares, px, a = self._frames(1e8, quiet)
        _, _, b = self._frames(1e8, later)
        first = fs.contradicted_by_trading(shares, px, a)
        second = fs.contradicted_by_trading(shares, px, b)
        assert first['AAA'].iloc[:15].equals(second['AAA'].iloc[:15])
        assert second['AAA'].iloc[-1:].isna().all()                    # ...and does see it once it is typical

    def test_the_window_is_twenty_one_sessions(self):
        assert fs.TURNOVER_WINDOW == 21
        volume = [5e9] * 9 + [20e6] * 21                              # frantic, then 21 quiet sessions
        shares, px, frame = self._frames(1e8, volume)
        out = fs.contradicted_by_trading(shares, px, frame)
        assert out['AAA'].iloc[4:9].isna().all()
        assert out['AAA'].iloc[-1] == 1e8                              # the frantic days have left the window

    def test_unknown_volume_or_count_contradicts_nothing(self):
        shares, px, volume = self._frames(2e6, float('nan'))
        assert fs.contradicted_by_trading(shares, px, volume).equals(shares)
        shares, px, volume = self._frames(float('nan'), 246e6)
        assert fs.contradicted_by_trading(shares, px, volume)['AAA'].isna().all()

    def test_it_is_the_scalar_rule_cell_for_cell(self):
        """Two implementations of one rule: the frame version must call
        unknown exactly the caps ``share_basis.unbelievable_cap`` does."""
        from trader.data.share_basis import unbelievable_cap
        nan, inf = float('nan'), float('inf')
        cases = [(shares, price, volume)
                 for shares in (2e6, 1e8, 1e9, 0.0, -1e6, nan, inf)
                 for price in (10.0, 0.5, nan)
                 for volume in (246e6, 0.5e9, 0.5e9 + 1, 1e3, 0.0, -5.0, nan, inf)]
        days = pd.bdate_range('2024-01-02', periods=fs.TURNOVER_MIN_SESSIONS)
        names = [f'N{i}' for i in range(len(cases))]
        shares = pd.DataFrame({n: c[0] for n, c in zip(names, cases)}, index=days, dtype=float)
        px = pd.DataFrame({n: c[1] for n, c in zip(names, cases)}, index=days, dtype=float)
        volume = pd.DataFrame({n: c[2] for n, c in zip(names, cases)}, index=days, dtype=float)
        out = fs.contradicted_by_trading(shares, px, volume).iloc[-1]
        removed = 0
        for name, (count, price, vol) in zip(names, cases):
            expected = unbelievable_cap(count * price, vol)
            was_known = not pd.isna(count)
            assert (was_known and pd.isna(out[name])) == expected, (count, price, vol)
            removed += expected
        assert removed >= 6                                            # the grid does exercise the rule

    def test_volume_is_matched_to_the_panel_by_day_and_name(self):
        shares, px, volume = self._frames(2e6, 246e6)
        other = volume.rename(columns={'AAA': 'BBB'})
        assert fs.contradicted_by_trading(shares, px, other).equals(shares)       # BBB's trading says nothing of AAA


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
        assert np.isnan(counts[1])                                     # one filing is not yet a level
        # 2,532.0 is 2,532M printed in millions: read in units and put on the basis, where it agrees with
        # the 632M before the split; the 10-K carries the last 10-Q's
        assert counts[2:] == pytest.approx([2_532e6 * 10, 2_540e6 * 10, 2_540e6 * 10])

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


class TestDollarVolume:
    def test_it_is_the_stored_volume_on_the_panels_shape(self, hist):
        _bars(hist, 'AAA', [10.0, 11.0, 12.0])                         # 2021-07-12 .. 14, $close x 1e6 a day
        _bars(hist, 'ZZZ', [5.0, 5.0, 5.0])                            # stored, not in the panel
        px = pd.DataFrame({'AAA': [1.0, 1.0], 'BBB': [1.0, 1.0]}, index=pd.to_datetime(['2021-07-12', '2021-07-14']))
        out = fs._dollar_volume(hist, px)
        assert list(out.columns) == ['AAA', 'BBB'] and out.index.equals(px.index)
        assert out['AAA'].tolist() == [10e6, 12e6]
        assert out['BBB'].isna().all()                                 # a panel name with no stored bar

    def test_a_day_the_panel_has_and_the_store_does_not_is_unknown(self, hist):
        _bars(hist, 'AAA', [10.0])
        px = pd.DataFrame({'AAA': [1.0, 1.0]}, index=pd.to_datetime(['2021-07-12', '2021-07-13']))
        assert fs._dollar_volume(hist, px)['AAA'].tolist() == pytest.approx([10e6, np.nan], nan_ok=True)


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

    def _basis(self, hist, day='2026-07-29'):
        hist.execute('CREATE TABLE IF NOT EXISTS pit_price_basis(adjusted_through DATE, recorded_at TIMESTAMPTZ, '
                     'note VARCHAR)', fetch='none')
        hist.execute('INSERT INTO pit_price_basis VALUES (?, now(), ?)', [day, 'build'], fetch='none')

    def test_no_split_table_is_no_events(self, hist):
        assert fs._split_inputs(hist, {'NVDA'}, True, self.PX, 'ticker', self._rows('NVDA')) == (
            None, None, frozenset())

    def test_events_are_read_for_the_names_asked_about_only(self, hist):
        _splits_table(hist, [('NVDA', '2021-07-20', 1, 4), ('AAPL', '2020-08-31', 1, 4)])
        self._basis(hist)
        events, through, unreflected = fs._split_inputs(hist, {'NVDA'}, True, self.PX, 'ticker', self._rows('NVDA'))
        assert events == {'NVDA': [(dt.date(2021, 7, 20), 4.0)]}
        assert unreflected == frozenset()

    def test_a_point_in_time_store_that_does_not_say_its_basis_is_refused(self, hist):
        """The last stored day is not it: a snapshot cut at 2024-06-28 still
        carries prices adjusted in July 2026, and reading the basis as the cut
        leaves out two years of splits without a word."""
        _splits_table(hist, [('NVDA', '2021-07-20', 1, 4)])
        with pytest.raises(RuntimeError, match="does not say what day pit_daily_bars is split-adjusted through"):
            fs._split_inputs(hist, {'NVDA'}, True, self.PX, 'ticker', self._rows('NVDA'))
        hist.execute('CREATE TABLE pit_price_basis(adjusted_through DATE, recorded_at TIMESTAMPTZ, note VARCHAR)',
                     fetch='none')
        with pytest.raises(RuntimeError, match='pit_price_basis'):          # the table, and nothing in it
            fs._split_inputs(hist, {'NVDA'}, True, self.PX, 'ticker', self._rows('NVDA'))

    def test_the_recorded_basis_is_the_bound_for_the_point_in_time_panel(self, hist):
        _splits_table(hist, [('NVDA', '2021-07-20', 1, 4)])
        self._basis(hist)
        _, through, _ = fs._split_inputs(hist, {'NVDA'}, True, self.PX, 'ticker', self._rows('NVDA'))
        assert through == dt.date(2026, 7, 29)

    def test_the_present_day_panel_is_adjusted_to_its_own_last_day(self, hist):
        """It is re-fetched whole; `pit_price_basis` is about pit_daily_bars only."""
        _splits_table(hist, [('NVDA', '2021-07-20', 1, 4)])
        _, through, _ = fs._split_inputs(hist, {'NVDA'}, False, self.PX, 'ticker', self._rows('NVDA'))
        assert through == dt.date(2024, 6, 28)
        self._basis(hist)
        _, through, _ = fs._split_inputs(hist, {'NVDA'}, False, self.PX, 'ticker', self._rows('NVDA'))
        assert through == dt.date(2024, 6, 28)

    def test_a_split_the_stored_series_shows_raw_is_unreflected(self, hist):
        """CGNX 2017-12-04: 138.49 then 63.31, a 2:1 the vendor's series missed."""
        _splits_table(hist, [('CGNX', '2021-07-20', 1, 2), ('NVDA', '2021-07-20', 1, 4)])
        _bars(hist, 'CGNX', [138.0, 137.5, 139.0, 138.2, 137.9, 138.49, 63.31, 64.0])        # raw jump on the 20th
        _bars(hist, 'NVDA', [200.0, 201.0, 199.0, 198.0, 200.5, 201.2, 199.9, 202.0])        # adjusted: ordinary day
        px = pd.DataFrame({'CGNX': [1.0, 1.0], 'NVDA': [1.0, 1.0]}, index=pd.to_datetime(['2021-07-12', '2021-07-21']))
        self._basis(hist)
        _, _, unreflected = fs._split_inputs(hist, {'CGNX', 'NVDA'}, True, px, 'ticker', self._rows('CGNX', 'NVDA'))
        assert unreflected == frozenset({('CGNX', dt.date(2021, 7, 20))})

    def test_a_split_outside_the_stored_days_cannot_be_checked_and_counts_as_carried(self, hist):
        _splits_table(hist, [('NVDA', '2025-03-03', 1, 2)])
        _bars(hist, 'NVDA', [200.0, 201.0])
        self._basis(hist)
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


class TestRefusedTickers:
    """`scripts/pit_split_adjust.py` leaves a series it cannot explain exactly
    as it was and names the ticker in `pit_split_refusals`. Its price is on an
    unknown basis, so it has no market cap here."""

    def test_no_table_is_nobody_refused(self, hist):
        assert fs.refused_tickers(hist) == frozenset()

    def test_the_tickers_on_record(self, hist):
        hist.execute('CREATE TABLE pit_split_refusals(ticker VARCHAR, reason VARCHAR, detail VARCHAR, '
                     'unverified_splits VARCHAR, refused_at TIMESTAMPTZ)', fetch='none')
        assert fs.refused_tickers(hist) == frozenset()
        for ticker in ('XRX', 'SDRL', 'XRX'):
            hist.execute("INSERT INTO pit_split_refusals VALUES (?, 'unexplained_quotient', '', '[]', now())",
                         [ticker], fetch='none')
        assert fs.refused_tickers(hist) == frozenset({'XRX', 'SDRL'})

    def test_a_refused_ticker_has_no_share_count_and_so_no_market_cap(self):
        frame = pd.DataFrame({'AAPL': [1.0, 2.0], 'XRX': [3.0, 4.0], 'SDRL': [5.0, 6.0]})
        out = fs.without_refused(frame, frozenset({'XRX', 'SDRL', 'NOT_HELD'}))
        assert out['AAPL'].tolist() == [1.0, 2.0]
        assert out['XRX'].isna().all() and out['SDRL'].isna().all()
        assert frame['XRX'].tolist() == [3.0, 4.0], 'the frame handed in is not changed'

    def test_nobody_refused_is_the_same_frame(self):
        frame = pd.DataFrame({'AAPL': [1.0]})
        assert fs.without_refused(frame, frozenset()) is frame
        assert fs.without_refused(frame, frozenset({'XRX'})) is frame
