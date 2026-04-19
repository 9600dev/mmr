"""Tests for the TwelveData paths added to the SDK.

Covers:
- ``MMR._flatten_td_dict`` — the shared helper that turns TwelveData's nested
  fundamentals payloads into single-level-keyed rows.
- ``MMR._td_fundamentals_to_df`` — payload → DataFrame conversion, ensuring
  the ``fiscal_date`` sort contract holds and nested groups land as dot-keyed
  columns without clobbering each other.
"""

import pandas as pd

from trader.sdk import MMR


class TestFlattenTDDict:

    def test_flat_input_passthrough(self):
        assert MMR._flatten_td_dict({'a': 1, 'b': 'x'}) == {'a': 1, 'b': 'x'}

    def test_nested_dict_becomes_dot_keys(self):
        out = MMR._flatten_td_dict({'a': {'b': 1, 'c': 2}})
        assert out == {'a.b': 1, 'a.c': 2}

    def test_deeply_nested(self):
        out = MMR._flatten_td_dict({
            'valuation_metrics': {
                'trailing_pe': 42.0,
                'price_to_book_mrq': 45.0,
            },
            'financials': {
                'income_statement': {
                    'revenue_ttm': 400_000_000_000,
                },
            },
        })
        assert out['valuation_metrics.trailing_pe'] == 42.0
        assert out['valuation_metrics.price_to_book_mrq'] == 45.0
        assert out['financials.income_statement.revenue_ttm'] == 400_000_000_000

    def test_preserves_none(self):
        """None is a legitimate "unreported" signal from TD — do NOT drop."""
        out = MMR._flatten_td_dict({'a': {'b': None}})
        assert out == {'a.b': None}

    def test_custom_prefix_and_separator(self):
        out = MMR._flatten_td_dict({'x': {'y': 1}}, prefix='ctx', sep='__')
        assert out == {'ctx__x__y': 1}


class TestTDFundamentalsToDF:
    """Assertion style: ``mmr_instance = object()`` — we only exercise the
    helper methods which are effectively static (don't touch ``self`` state).
    Avoids spinning up a full Container just to test a data-shape function."""

    def _mmr(self) -> MMR:
        # Bind-only: __new__ skips __init__ so no Container init required.
        return object.__new__(MMR)

    def test_empty_payload_returns_empty_df(self):
        df = self._mmr()._td_fundamentals_to_df({}, 'balance_sheet')
        assert df.empty

    def test_missing_list_key_returns_empty(self):
        df = self._mmr()._td_fundamentals_to_df({'meta': {'symbol': 'AAPL'}}, 'balance_sheet')
        assert df.empty

    def test_single_period_flattens_to_one_row(self):
        payload = {
            'meta': {'symbol': 'AAPL', 'period': 'Annual'},
            'balance_sheet': [
                {
                    'fiscal_date': '2025-09-30',
                    'year': 2026,
                    'assets': {
                        'current_assets': {
                            'cash': 28_267_000_000,
                            'inventory': 5_718_000_000,
                        },
                    },
                },
            ],
        }
        df = self._mmr()._td_fundamentals_to_df(payload, 'balance_sheet')
        assert len(df) == 1
        assert df['assets.current_assets.cash'].iloc[0] == 28_267_000_000
        assert df['assets.current_assets.inventory'].iloc[0] == 5_718_000_000
        assert df['fiscal_date'].iloc[0] == '2025-09-30'

    def test_multiple_periods_sorted_newest_first(self):
        payload = {
            'meta': {'symbol': 'AAPL'},
            'income_statement': [
                {'fiscal_date': '2024-09-30', 'sales': 383_000_000_000},
                {'fiscal_date': '2025-09-30', 'sales': 416_000_000_000},
                {'fiscal_date': '2023-09-30', 'sales': 383_000_000_000},
            ],
        }
        df = self._mmr()._td_fundamentals_to_df(payload, 'income_statement')
        assert list(df['fiscal_date']) == ['2025-09-30', '2024-09-30', '2023-09-30']

    def test_columns_that_are_entirely_none_are_dropped(self):
        """TD sometimes returns fields as null for every period. Those should
        be pruned so the rendered table doesn't have phantom empty columns."""
        payload = {
            'cash_flow': [
                {'fiscal_date': '2025-09-30', 'net_income': 112_010_000_000, 'deferred_taxes': None},
                {'fiscal_date': '2024-09-30', 'net_income':  94_000_000_000, 'deferred_taxes': None},
            ],
        }
        df = self._mmr()._td_fundamentals_to_df(payload, 'cash_flow')
        assert 'net_income' in df.columns
        assert 'deferred_taxes' not in df.columns
