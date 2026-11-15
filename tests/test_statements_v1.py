"""trader/data/statements_v1.py: the vendor's replacement statement endpoints,
read point-in-time.

Two properties of those endpoints decide what may be stored, both measured
live on 2026-11-15: an old period comes back as the comparative column of a
LATER filing (AAPL's quarter ended 2020-06-27 is dated 2021-07-28), and share
counts are on today's split basis. The first is refused here; the second is
handled where the rows are read (scripts/fundamental_signals.prepare_rows).
"""

from __future__ import annotations

import datetime as dt

import pytest

from trader.data import statements_v1 as sv
from trader.data.statements_v1 import filed_with, statement_values

ACCEPTED = dt.date(2026, 11, 3)

INCOME = {'filing_date': '2026-11-03', 'revenue': 114_477e6, 'consolidated_net_income_loss': 30_711e6,
          'net_income_loss_attributable_common_shareholders': 30_000e6, 'gross_profit': 54_820e6,
          'operating_income': 35_631e6, 'diluted_shares_outstanding': 14_630_319_000.0,
          'basic_shares_outstanding': 14_570_194_000.0}
BALANCE = {'filing_date': '2026-11-03', 'total_assets': 404_548e6, 'total_liabilities': 292_455e6,
           'total_equity': 112_093e6, 'total_equity_attributable_to_parent': 111_000e6}
CASHFLOW = {'filing_date': '2026-11-03', 'net_cash_from_operating_activities': 28_101e6,
            'cash_from_operating_activities_continuing_operations': 27_000e6}


def _statements(**over):
    out = {'income': dict(INCOME), 'balance': dict(BALANCE), 'cashflow': dict(CASHFLOW)}
    out.update(over)
    return out


class TestFiledWith:
    @pytest.mark.parametrize('filing_date, expected', [
        ('2026-11-03', True),
        ('2026-11-04', True),            # accepted in the evening, dated the next day
        ('2026-11-08', True),            # five days: the edge
        ('2026-11-09', False),
        ('2026-10-29', True),
        ('2026-10-28', False),
        ('2027-01-30', False),           # the next quarter's comparative
        ('2027-11-02', False),           # next year's
    ])
    def test_the_filing_date_must_be_the_acceptance_day_give_or_take(self, filing_date, expected):
        assert filed_with({'filing_date': filing_date}, ACCEPTED) is expected

    @pytest.mark.parametrize('row', [None, {}, {'filing_date': None}, {'filing_date': ''},
                                     {'filing_date': 'soon'}, {'revenue': 1.0}])
    def test_a_row_that_names_no_filing_cannot_be_dated(self, row):
        assert filed_with(row, ACCEPTED) is False

    def test_a_date_object_and_a_timestamp_are_read(self):
        assert filed_with({'filing_date': dt.date(2026, 11, 3)}, ACCEPTED) is True
        assert filed_with({'filing_date': dt.datetime(2026, 11, 3, 16, 30)}, ACCEPTED) is True
        assert filed_with({'filing_date': '2026-11-03T16:30:00Z'}, ACCEPTED) is True

    def test_the_gap_is_a_parameter(self):
        assert filed_with({'filing_date': '2026-11-05'}, ACCEPTED, max_gap_days=1) is False
        assert filed_with({'filing_date': '2026-11-04'}, ACCEPTED, max_gap_days=1) is True
        assert sv.MAX_FILING_GAP_DAYS == 5


class TestStatementValues:
    def test_the_stored_columns_and_where_each_comes_from(self):
        values, reason = statement_values(_statements(), ACCEPTED)
        assert reason == ''
        assert values == {
            'revenue': 114_477e6,
            'net_income': 30_711e6,                  # consolidated, not attributable-to-common
            'gross_profit': 54_820e6,
            'operating_income': 35_631e6,
            'total_assets': 404_548e6,
            'total_liabilities': 292_455e6,
            'stockholders_equity': 112_093e6,        # total equity, not attributable-to-parent
            'cash_from_operations': 28_101e6,        # net, not continuing-operations only
            'shares_diluted': 14_630_319_000.0,      # diluted, not basic
        }

    def test_the_column_set_is_the_stores(self):
        assert list(sv.FIELDS) == ['revenue', 'net_income', 'gross_profit', 'operating_income',
                                   'total_assets', 'total_liabilities', 'stockholders_equity',
                                   'cash_from_operations', 'shares_diluted']
        assert sv.SOURCE == 'financials_v1'

    @pytest.mark.parametrize('income', [None, {}])
    def test_no_income_statement_is_no_row(self, income):
        assert statement_values(_statements(income=income), ACCEPTED) == (None, sv.NO_STATEMENT)
        assert statement_values({}, ACCEPTED) == (None, sv.NO_STATEMENT)

    def test_an_income_statement_from_another_filing_is_no_row(self):
        """The comparative column of a later 10-Q, recast: not what this filing said."""
        later = dict(INCOME, filing_date='2027-11-02', revenue=5.6e9)
        assert statement_values(_statements(income=later), ACCEPTED) == (None, sv.OTHER_FILING)

    def test_an_income_statement_that_names_no_filing_is_no_row(self):
        undated = {k: v for k, v in INCOME.items() if k != 'filing_date'}
        assert statement_values(_statements(income=undated), ACCEPTED) == (None, sv.OTHER_FILING)

    @pytest.mark.parametrize('statement, columns', [
        ('balance', ('total_assets', 'total_liabilities', 'stockholders_equity')),
        ('cashflow', ('cash_from_operations',)),
    ])
    @pytest.mark.parametrize('replacement', [None, {}, 'later'])
    def test_another_statement_missing_or_from_elsewhere_leaves_its_columns_unknown(
            self, statement, columns, replacement):
        base = {'balance': BALANCE, 'cashflow': CASHFLOW}[statement]
        row = dict(base, filing_date='2027-01-30') if replacement == 'later' else replacement
        values, reason = statement_values(_statements(**{statement: row}), ACCEPTED)
        assert reason == '' and values is not None
        for column in sv.FIELDS:
            if column in columns:
                assert values[column] is None, column
            else:
                assert values[column] is not None, column

    @pytest.mark.parametrize('bad', [None, 'n/a', float('nan'), float('inf'), True])
    def test_a_value_that_is_not_a_finite_number_is_unknown(self, bad):
        values, _ = statement_values(_statements(income=dict(INCOME, revenue=bad)), ACCEPTED)
        assert values['revenue'] is None and values['net_income'] == 30_711e6

    def test_a_numeric_string_is_a_number(self):
        values, _ = statement_values(_statements(income=dict(INCOME, revenue='1000.5')), ACCEPTED)
        assert values['revenue'] == 1000.5

    def test_an_absent_field_is_unknown(self):
        income = {k: v for k, v in INCOME.items() if k != 'gross_profit'}
        values, _ = statement_values(_statements(income=income), ACCEPTED)
        assert values['gross_profit'] is None

    @pytest.mark.parametrize('column, statement, field', [
        ('shares_diluted', 'income', 'diluted_shares_outstanding'),
        ('total_assets', 'balance', 'total_assets'),
    ])
    @pytest.mark.parametrize('zero', [0.0, 0, -1.0])
    def test_a_zero_where_zero_cannot_be_a_fact_is_unknown(self, column, statement, field, zero):
        """The vendor writes 0.0 for a line the filer did not report."""
        base = {'income': INCOME, 'balance': BALANCE}[statement]
        values, _ = statement_values(_statements(**{statement: dict(base, **{field: zero})}), ACCEPTED)
        assert values[column] is None

    @pytest.mark.parametrize('column, statement, field', [
        ('revenue', 'income', 'revenue'),                                  # a pre-revenue company
        ('net_income', 'income', 'consolidated_net_income_loss'),
        ('gross_profit', 'income', 'gross_profit'),
        ('operating_income', 'income', 'operating_income'),
        ('total_liabilities', 'balance', 'total_liabilities'),
        ('stockholders_equity', 'balance', 'total_equity'),
        ('cash_from_operations', 'cashflow', 'net_cash_from_operating_activities'),
    ])
    def test_a_zero_or_a_loss_elsewhere_is_a_number(self, column, statement, field):
        base = {'income': INCOME, 'balance': BALANCE, 'cashflow': CASHFLOW}[statement]
        for number in (0.0, -5e6):
            values, _ = statement_values(_statements(**{statement: dict(base, **{field: number})}), ACCEPTED)
            assert values[column] == number

    def test_the_gap_reaches_every_statement(self):
        statements = _statements(income=dict(INCOME, filing_date='2026-11-05'),
                                 balance=dict(BALANCE, filing_date='2026-11-05'))
        assert statement_values(statements, ACCEPTED, max_gap_days=1) == (None, sv.OTHER_FILING)
        values, _ = statement_values(_statements(balance=dict(BALANCE, filing_date='2026-11-05')),
                                     ACCEPTED, max_gap_days=1)
        assert values['total_assets'] is None and values['revenue'] is not None
