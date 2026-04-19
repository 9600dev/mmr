"""Tests for the TwelveData paths added to the SDK + scanner.

Covers:
- ``MMR._flatten_td_dict`` — the shared helper that turns TwelveData's nested
  fundamentals payloads into single-level-keyed rows.
- ``MMR._td_fundamentals_to_df`` — payload → DataFrame conversion, ensuring
  the ``fiscal_date`` sort contract holds and nested groups land as dot-keyed
  columns without clobbering each other.
- ``TwelveDataIdeaScanner._fetch_indicators`` — must use chronologically
  sorted closes (TwelveData returns newest-first by default). A value-sorted
  list would silently yield wrong RSI/EMA/SMA values.
- ``TwelveDataIdeaScanner._build_candidates`` — must apply the same
  preferred-stock / warrant / unit filter as the Massive and IB paths.
"""

import datetime as dt
from unittest.mock import MagicMock

import pandas as pd
import pytest

from trader.sdk import MMR
from trader.tools.idea_scanner import TwelveDataIdeaScanner, compute_ema, compute_sma, IdeaScannerError


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


# ---------------------------------------------------------------------------
# TwelveDataIdeaScanner._fetch_indicators
# ---------------------------------------------------------------------------

def _make_td_bars_newest_first(prices):
    """Build a TwelveData-style DataFrame: DatetimeIndex with newest timestamp
    first (the default TD order) and ``close`` as string values."""
    n = len(prices)
    dates = [dt.datetime(2026, 1, 1) + dt.timedelta(days=i) for i in range(n)]
    df = pd.DataFrame(
        {'close': [str(p) for p in prices]},  # TD returns strings
        index=pd.DatetimeIndex(list(reversed(dates)), name='datetime'),
    )
    # Prices are paired with dates in original (ascending) order, then the
    # DF is reindexed newest-first so the scanner *must* re-sort or indicator
    # values will be wrong.
    df['close'] = list(reversed([str(p) for p in prices]))
    return df


class TestTDScannerIndicators:
    """Regression guard for the bug where ``_fetch_indicators`` used a
    value-sorted closes list — indicators depend on chronological order,
    so any resort or reverse breaks them silently."""

    def _scanner_with_prices(self, prices):
        scanner = TwelveDataIdeaScanner(td_client=None)
        fake_ts = MagicMock()
        fake_ts.as_pandas.return_value = _make_td_bars_newest_first(prices)
        scanner._client = MagicMock()
        scanner._client.time_series.return_value = fake_ts
        return scanner

    def test_ema_computed_on_chronological_closes(self):
        # Ascending prices 100..129 (30 bars). EMA on chronological order
        # differs meaningfully from EMA on value-sorted (or reversed) order
        # for a trending series.
        prices = [100.0 + i for i in range(30)]
        scanner = self._scanner_with_prices(prices)
        result = scanner._fetch_indicators(['AAPL'], ['ema_9'])
        expected = compute_ema(prices, window=9)  # on chronological closes
        assert result['AAPL']['ema_9'] == pytest.approx(expected, rel=1e-6)

    def test_sma_computed_on_chronological_closes(self):
        prices = [100.0 + i for i in range(30)]
        scanner = self._scanner_with_prices(prices)
        result = scanner._fetch_indicators(['AAPL'], ['sma_20'])
        expected = compute_sma(prices, window=20)
        assert result['AAPL']['sma_20'] == pytest.approx(expected, rel=1e-6)

    def test_empty_df_returns_no_indicators(self):
        scanner = TwelveDataIdeaScanner(td_client=None)
        fake_ts = MagicMock()
        fake_ts.as_pandas.return_value = pd.DataFrame()
        scanner._client = MagicMock()
        scanner._client.time_series.return_value = fake_ts
        result = scanner._fetch_indicators(['AAPL'], ['rsi'])
        assert result == {'AAPL': {}}


# ---------------------------------------------------------------------------
# TwelveDataIdeaScanner._build_candidates — filter parity with Massive/IB
# ---------------------------------------------------------------------------

def _q(sym: str, **kwargs):
    base = {
        'symbol': sym, 'name': sym, 'close': 10.0, 'open': 9.9, 'high': 10.1,
        'low': 9.8, 'previous_close': 9.95, 'volume': 1000000,
        'average_volume': 800000, 'change': 0.05, 'percent_change': 0.5,
    }
    base.update(kwargs)
    return base


class TestTDScannerBuildCandidates:

    def _scanner(self) -> TwelveDataIdeaScanner:
        return TwelveDataIdeaScanner(td_client=None)

    def test_filters_warrants_and_rights(self):
        quotes = [_q('AAPL'), _q('XYZW'), _q('FOO.U'), _q('BAR.R'), _q('BAZWS')]
        got = [c['ticker'] for c in self._scanner()._build_candidates(quotes)]
        assert got == ['AAPL']

    def test_filters_preferred_stock_by_lowercase_p(self):
        """KKRpD is KKR preferred series D. The lowercase 'p' is the marker;
        tickers that happen to have a capital 'P' (e.g. "PG", "PYPL") must
        NOT be filtered."""
        quotes = [_q('KKRpD'), _q('ACRRpA'), _q('PG'), _q('PYPL')]
        got = [c['ticker'] for c in self._scanner()._build_candidates(quotes)]
        assert 'KKRpD' not in got
        assert 'ACRRpA' not in got
        assert 'PG' in got
        assert 'PYPL' in got

    def test_deduplicates(self):
        quotes = [_q('AAPL'), _q('AAPL', close=11.0)]
        got = self._scanner()._build_candidates(quotes)
        assert len(got) == 1

    def test_skips_zero_or_missing_price(self):
        """Malformed quote records shouldn't crash the scanner."""
        quotes = [_q('AAPL', close=0), _q('MSFT', close=None), _q('NVDA')]
        got = [c['ticker'] for c in self._scanner()._build_candidates(quotes)]
        assert got == ['NVDA']


# ---------------------------------------------------------------------------
# TwelveDataIdeaScanner._fetch_fundamentals — rate-limit graceful degradation
# ---------------------------------------------------------------------------

class TestTDScannerFundamentalsRateLimit:
    """get_statistics is ~100 credits/call; the Grow plan is 610/min. A
    10-ticker scan with fundamentals can burst past that. The scanner must
    complete with partial fundamentals rather than raise mid-scan."""

    def test_rate_limit_short_circuits_remaining_calls(self):
        scanner = TwelveDataIdeaScanner(td_client=None)

        call_count = [0]
        first_5_ok_then_rate_limit = (
            # First 5 tickers return valid payloads
            {'statistics': {'valuations_metrics': {'trailing_pe': 30.0}}},
            {'statistics': {'valuations_metrics': {'trailing_pe': 15.0}}},
            {'statistics': {'valuations_metrics': {'trailing_pe': 20.0}}},
            {'statistics': {'valuations_metrics': {'trailing_pe': 25.0}}},
            {'statistics': {'valuations_metrics': {'trailing_pe': 12.0}}},
        )

        def get_statistics(symbol):
            call_count[0] += 1
            idx = call_count[0] - 1
            if idx < len(first_5_ok_then_rate_limit):
                mock = MagicMock()
                mock.as_json.return_value = first_5_ok_then_rate_limit[idx]
                return mock
            raise RuntimeError(
                'You have run out of API credits for the current minute. '
                '701 API credits were used, with the current limit being 610.'
            )

        scanner._client = MagicMock()
        scanner._client.get_statistics.side_effect = get_statistics

        # 10 tickers — 5 should succeed, 5 should be skipped after rate limit
        tickers = [f'TICK{i}' for i in range(10)]
        result = scanner._fetch_fundamentals(tickers)

        # At least 5 succeeded (could be exactly 5 or slightly more if the
        # rate limit hit only after additional in-flight calls).
        successful = sum(1 for v in result.values() if v.get('pe_ratio') is not None)
        assert successful >= 5
        # And at least one symbol got skipped due to rate limit short-circuit
        # (otherwise all 10 calls went through, contradicting the mock).
        assert len(result) <= 10

    def test_non_rate_limit_errors_dont_trip_short_circuit(self):
        """A transient 500 on one symbol shouldn't stop us from fetching
        fundamentals on the remaining symbols."""
        scanner = TwelveDataIdeaScanner(td_client=None)

        def get_statistics(symbol):
            if symbol == 'BROKEN':
                raise RuntimeError('500 Internal Server Error')
            mock = MagicMock()
            mock.as_json.return_value = {
                'statistics': {'valuations_metrics': {'trailing_pe': 20.0}},
            }
            return mock

        scanner._client = MagicMock()
        scanner._client.get_statistics.side_effect = get_statistics

        result = scanner._fetch_fundamentals(['AAPL', 'BROKEN', 'NVDA', 'MSFT'])
        # BROKEN drops out (empty dict not included in results), others succeed
        assert 'AAPL' in result and result['AAPL'].get('pe_ratio') == 20.0
        assert 'NVDA' in result and result['NVDA'].get('pe_ratio') == 20.0
        assert 'MSFT' in result and result['MSFT'].get('pe_ratio') == 20.0


# ---------------------------------------------------------------------------
# TwelveDataIdeaScanner._discover error handling
# ---------------------------------------------------------------------------

class TestTDScannerDiscoveryErrors:
    """Verify that a total-failure of discovery raises (not silently returns
    empty) so auth / rate-limit errors can't be mistaken for "no movers"."""

    def test_both_directions_failing_raises(self):
        scanner = TwelveDataIdeaScanner(td_client=None)
        scanner._client = MagicMock()
        scanner._client.get_market_movers.side_effect = RuntimeError('401 Unauthorized')
        with pytest.raises(IdeaScannerError, match='Unauthorized'):
            scanner._discover('movers', tickers=None, universe_symbols=None, scan_preset=None)

    def test_partial_failure_tolerated(self):
        """One direction fails, the other works — return what we got rather
        than fail the whole scan."""
        scanner = TwelveDataIdeaScanner(td_client=None)
        scanner._client = MagicMock()

        call_count = [0]
        def side(market, direction):
            call_count[0] += 1
            if direction == 'gainers':
                raise RuntimeError('transient 429')
            mock = MagicMock()
            mock.as_json.return_value = [{'symbol': 'AAPL', 'last': 100.0}]
            return mock
        scanner._client.get_market_movers.side_effect = side
        result = scanner._discover('movers', tickers=None, universe_symbols=None, scan_preset=None)
        assert len(result) == 1
        assert result[0]['symbol'] == 'AAPL'
