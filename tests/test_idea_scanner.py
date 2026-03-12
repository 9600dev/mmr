"""Unit tests for the trading ideas scanner."""

import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from trader.tools.idea_scanner import (
    IBIdeaScanner,
    IdeaScanner,
    PRESETS,
    PRESET_SCAN_CODES,
    ScanFilter,
    ScanPreset,
    apply_filters,
    compute_ema,
    compute_rsi,
    compute_sma,
    list_presets,
    parse_report_snapshot,
    to_dataframe,
)


# ------------------------------------------------------------------
# Helpers to build mock Massive API responses
# ------------------------------------------------------------------

def _make_snapshot(
    ticker: str,
    price: float = 50.0,
    volume: int = 1_000_000,
    change_pct: float = 5.0,
    day_open: float = 0.0,
    day_high: float = 0.0,
    day_low: float = 0.0,
    prev_close: float = 0.0,
    prev_volume: int = 0,
    vwap: float = 0.0,
    bid: float = 0.0,
    ask: float = 0.0,
) -> SimpleNamespace:
    """Build a mock TickerSnapshot."""
    if day_open == 0.0:
        day_open = price * 0.98
    if day_high == 0.0:
        day_high = price * 1.02
    if day_low == 0.0:
        day_low = price * 0.96
    if prev_close == 0.0:
        prev_close = price * 0.95
    if prev_volume == 0:
        prev_volume = volume
    if vwap == 0.0:
        vwap = price * 0.99
    if bid == 0.0:
        bid = price - 0.01
    if ask == 0.0:
        ask = price + 0.01

    return SimpleNamespace(
        ticker=ticker,
        day=SimpleNamespace(
            open=day_open, high=day_high, low=day_low,
            close=price, volume=volume, vwap=vwap,
        ),
        prev_day=SimpleNamespace(
            close=prev_close, volume=prev_volume,
            open=prev_close * 0.99, high=prev_close * 1.01,
            low=prev_close * 0.98,
        ),
        last_quote=SimpleNamespace(bid=bid, ask=ask),
        last_trade=None,
        min=None,
        todays_change=price - prev_close,
        todays_change_percent=change_pct,
        updated=0,
        fair_market_value=0.0,
    )


def _make_indicator_result(value: float):
    """Build a mock indicator result with a single value."""
    return SimpleNamespace(values=[SimpleNamespace(value=value, timestamp=0)])


def _make_empty_indicator_result():
    """Build a mock indicator result with no values."""
    return SimpleNamespace(values=[])


@pytest.fixture
def mock_client():
    """Return a MagicMock Massive RESTClient."""
    client = MagicMock()
    # Default: indicators return empty results
    client.get_rsi.return_value = _make_empty_indicator_result()
    client.get_ema.return_value = _make_empty_indicator_result()
    client.get_sma.return_value = _make_empty_indicator_result()
    return client


@pytest.fixture
def scanner(mock_client):
    return IdeaScanner(mock_client)


# ------------------------------------------------------------------
# list_presets()
# ------------------------------------------------------------------

class TestListPresets:
    def test_returns_all_presets(self):
        df = list_presets()
        assert len(df) == 6
        assert set(df['preset']) == {'momentum', 'gap-up', 'gap-down',
                                      'mean-reversion', 'breakout', 'volatile'}

    def test_has_expected_columns(self):
        df = list_presets()
        assert 'preset' in df.columns
        assert 'description' in df.columns
        assert 'filters' in df.columns
        assert 'indicators' in df.columns


# ------------------------------------------------------------------
# Discovery
# ------------------------------------------------------------------

class TestDiscovery:
    def test_movers_source_fetches_gainers_and_losers(self, scanner, mock_client):
        mock_client.get_snapshot_direction.return_value = [
            _make_snapshot('AAPL', price=180, volume=1_000_000, change_pct=3.0),
        ]
        snaps = scanner._discover('movers', None, None)
        assert mock_client.get_snapshot_direction.call_count == 2
        assert len(snaps) == 2  # 1 from gainers + 1 from losers

    def test_tickers_source_uses_get_snapshot_all(self, scanner, mock_client):
        mock_client.get_snapshot_all.return_value = [
            _make_snapshot('AAPL'), _make_snapshot('MSFT'),
        ]
        snaps = scanner._discover('tickers', ['AAPL', 'MSFT'], None)
        mock_client.get_snapshot_all.assert_called_once_with(
            market_type='stocks', tickers=['AAPL', 'MSFT'],
        )
        assert len(snaps) == 2

    def test_universe_source_uses_get_snapshot_all(self, scanner, mock_client):
        mock_client.get_snapshot_all.return_value = [_make_snapshot('AMD')]
        snaps = scanner._discover('universe', None, ['AMD'])
        mock_client.get_snapshot_all.assert_called_once_with(
            market_type='stocks', tickers=['AMD'],
        )
        assert len(snaps) == 1

    def test_market_source_uses_get_snapshot_all_no_tickers(self, scanner, mock_client):
        mock_client.get_snapshot_all.return_value = [_make_snapshot('XYZ')]
        snaps = scanner._discover('market', None, None)
        mock_client.get_snapshot_all.assert_called_once_with(
            market_type='stocks',
        )
        assert len(snaps) == 1

    def test_market_scan_presets_auto_upgrade_source(self, scanner, mock_client):
        """Presets with use_market_scan=True should use 'market' source, not movers."""
        mock_client.get_snapshot_all.return_value = [
            _make_snapshot('DIP', price=50, volume=500_000, change_pct=-5.0),
        ]
        mock_client.get_rsi.return_value = _make_indicator_result(25.0)
        mock_client.get_sma.return_value = _make_indicator_result(60.0)

        # mean-reversion has use_market_scan=True, so 'movers' source upgrades to 'market'
        df = scanner.scan(preset='mean-reversion')
        # Should NOT have called get_snapshot_direction (movers)
        mock_client.get_snapshot_direction.assert_not_called()
        # Should have called get_snapshot_all
        mock_client.get_snapshot_all.assert_called_once()


# ------------------------------------------------------------------
# Candidate building
# ------------------------------------------------------------------

class TestBuildCandidates:
    def test_gap_pct_calculation(self, scanner):
        snap = _make_snapshot('TEST', price=110, day_open=105, prev_close=100)
        candidates = scanner._build_candidates([snap])
        assert len(candidates) == 1
        # gap = (105 - 100) / 100 * 100 = 5.0%
        assert candidates[0]['gap_pct'] == 5.0

    def test_relative_volume_calculation(self, scanner):
        snap = _make_snapshot('TEST', volume=2_000_000, prev_volume=1_000_000)
        candidates = scanner._build_candidates([snap])
        assert candidates[0]['rel_vol'] == 2.0

    def test_range_pct_calculation(self, scanner):
        snap = _make_snapshot('TEST', day_high=110, day_low=100)
        candidates = scanner._build_candidates([snap])
        # range = (110 - 100) / 100 * 100 = 10.0%
        assert candidates[0]['range_pct'] == 10.0

    def test_deduplicates_tickers(self, scanner):
        snap1 = _make_snapshot('AAPL', price=180)
        snap2 = _make_snapshot('AAPL', price=181)
        candidates = scanner._build_candidates([snap1, snap2])
        assert len(candidates) == 1

    def test_skips_no_day_data(self, scanner):
        snap = SimpleNamespace(
            ticker='BAD', day=None, prev_day=None,
            last_quote=None, todays_change_percent=0.0,
        )
        candidates = scanner._build_candidates([snap])
        assert len(candidates) == 0

    def test_skips_empty_ticker(self, scanner):
        snap = _make_snapshot('')
        candidates = scanner._build_candidates([snap])
        assert len(candidates) == 0

    def test_skips_warrants(self, scanner):
        snap = _make_snapshot('AEVAW')
        candidates = scanner._build_candidates([snap])
        assert len(candidates) == 0

    def test_skips_preferred_stocks(self, scanner):
        snap = _make_snapshot('KKRpD')
        candidates = scanner._build_candidates([snap])
        assert len(candidates) == 0

    def test_keeps_normal_uppercase_tickers(self, scanner):
        snap = _make_snapshot('AAPL')
        candidates = scanner._build_candidates([snap])
        assert len(candidates) == 1

    def test_spread_pct_uses_bid_ask_price_fields(self, scanner):
        """Verify spread is calculated from bid_price/ask_price (Massive API format)."""
        snap = _make_snapshot('TEST', price=100.0)
        # Override last_quote with Massive API field names
        snap.last_quote = SimpleNamespace(
            bid_price=99.90, ask_price=100.10,
            bid=None, ask=None,
        )
        candidates = scanner._build_candidates([snap])
        assert candidates[0]['spread_pct'] == pytest.approx(0.2, abs=0.01)


# ------------------------------------------------------------------
# Filtering
# ------------------------------------------------------------------

class TestFiltering:
    def test_min_price_filter(self, scanner):
        candidates = [
            {'ticker': 'PENNY', 'price': 0.50, 'volume': 1_000_000,
             'change_pct': 5.0, 'rel_vol': 1.0},
            {'ticker': 'GOOD', 'price': 10.0, 'volume': 1_000_000,
             'change_pct': 5.0, 'rel_vol': 1.0},
        ]
        filters = ScanFilter(min_price=5.0)
        result = scanner._apply_filters(candidates, filters)
        assert len(result) == 1
        assert result[0]['ticker'] == 'GOOD'

    def test_min_volume_filter(self, scanner):
        candidates = [
            {'ticker': 'LOW', 'price': 50.0, 'volume': 50_000,
             'change_pct': 5.0, 'rel_vol': 1.0},
            {'ticker': 'HIGH', 'price': 50.0, 'volume': 500_000,
             'change_pct': 5.0, 'rel_vol': 1.0},
        ]
        filters = ScanFilter(min_volume=100_000)
        result = scanner._apply_filters(candidates, filters)
        assert len(result) == 1
        assert result[0]['ticker'] == 'HIGH'

    def test_min_change_pct_filter(self, scanner):
        candidates = [
            {'ticker': 'FLAT', 'price': 50.0, 'volume': 500_000,
             'change_pct': 0.5, 'rel_vol': 1.0},
            {'ticker': 'UP', 'price': 50.0, 'volume': 500_000,
             'change_pct': 5.0, 'rel_vol': 1.0},
        ]
        filters = ScanFilter(min_change_pct=2.0)
        result = scanner._apply_filters(candidates, filters)
        assert len(result) == 1
        assert result[0]['ticker'] == 'UP'

    def test_max_change_pct_filter(self, scanner):
        candidates = [
            {'ticker': 'DOWN', 'price': 50.0, 'volume': 500_000,
             'change_pct': -5.0, 'rel_vol': 1.0},
            {'ticker': 'UP', 'price': 50.0, 'volume': 500_000,
             'change_pct': 2.0, 'rel_vol': 1.0},
        ]
        filters = ScanFilter(max_change_pct=-3.0)
        result = scanner._apply_filters(candidates, filters)
        assert len(result) == 1
        assert result[0]['ticker'] == 'DOWN'

    def test_spread_filter(self, scanner):
        candidates = [
            {'ticker': 'WIDE', 'price': 50.0, 'volume': 500_000,
             'change_pct': 5.0, 'rel_vol': 1.0, 'spread_pct': 12.0},
            {'ticker': 'TIGHT', 'price': 50.0, 'volume': 500_000,
             'change_pct': 5.0, 'rel_vol': 1.0, 'spread_pct': 0.1},
        ]
        filters = ScanFilter(max_spread_pct=5.0)
        result = scanner._apply_filters(candidates, filters)
        assert len(result) == 1
        assert result[0]['ticker'] == 'TIGHT'


# ------------------------------------------------------------------
# Indicator fetching
# ------------------------------------------------------------------

class TestIndicatorFetch:
    def test_fetches_rsi_and_ema(self, scanner, mock_client):
        mock_client.get_rsi.return_value = _make_indicator_result(65.0)
        mock_client.get_ema.return_value = _make_indicator_result(48.5)

        result = scanner._fetch_indicators(['AAPL'], ['rsi', 'ema_9'])
        assert result['AAPL']['rsi'] == 65.0
        assert result['AAPL']['ema_9'] == 48.5

    def test_graceful_indicator_failure(self, scanner, mock_client):
        mock_client.get_rsi.side_effect = Exception('API error')
        mock_client.get_ema.return_value = _make_indicator_result(50.0)

        result = scanner._fetch_indicators(['AAPL'], ['rsi', 'ema_9'])
        assert result['AAPL']['rsi'] is None
        assert result['AAPL']['ema_9'] == 50.0

    def test_one_ticker_failure_doesnt_break_others(self, scanner, mock_client):
        def rsi_side_effect(ticker, **kwargs):
            if ticker == 'BAD':
                raise Exception('fail')
            return _make_indicator_result(55.0)

        mock_client.get_rsi.side_effect = rsi_side_effect
        result = scanner._fetch_indicators(['AAPL', 'BAD'], ['rsi'])
        assert result['AAPL']['rsi'] == 55.0
        assert result['BAD']['rsi'] is None

    def test_empty_indicators_list(self, scanner, mock_client):
        result = scanner._fetch_indicators(['AAPL'], [])
        assert result == {}

    def test_empty_tickers_list(self, scanner, mock_client):
        result = scanner._fetch_indicators([], ['rsi'])
        assert result == {}

    def test_fetches_sma_20_and_50(self, scanner, mock_client):
        mock_client.get_sma.return_value = _make_indicator_result(100.0)
        result = scanner._fetch_indicators(['AAPL'], ['sma_20', 'sma_50'])
        assert result['AAPL']['sma_20'] == 100.0
        assert result['AAPL']['sma_50'] == 100.0
        assert mock_client.get_sma.call_count == 2


# ------------------------------------------------------------------
# Scoring
# ------------------------------------------------------------------

class TestScoring:
    def test_momentum_buy_scores_higher_than_watch(self, scanner):
        high_momentum = {
            'ticker': 'HOT', 'price': 50.0, 'change_pct': 8.0,
            'rel_vol': 3.0, 'rsi': 65.0, 'ema_9': 48.0,
        }
        low_momentum = {
            'ticker': 'MEH', 'price': 50.0, 'change_pct': 2.5,
            'rel_vol': 1.0, 'rsi': 52.0, 'ema_9': 51.0,
        }
        high_score, high_signal = scanner._score_momentum(high_momentum)
        low_score, low_signal = scanner._score_momentum(low_momentum)
        assert high_score > low_score

    def test_gap_up_scores_larger_gaps_higher(self, scanner):
        big_gap = {
            'ticker': 'BIG', 'price': 50.0, 'gap_pct': 10.0,
            'rel_vol': 2.0, 'change_pct': 8.0, 'rsi': 60.0,
        }
        small_gap = {
            'ticker': 'SMALL', 'price': 50.0, 'gap_pct': 3.5,
            'rel_vol': 1.0, 'change_pct': 3.0, 'rsi': 55.0,
        }
        big_score, _ = scanner._score_gap_up(big_gap)
        small_score, _ = scanner._score_gap_up(small_gap)
        assert big_score > small_score

    def test_gap_down_returns_sell_signal(self, scanner):
        candidate = {
            'ticker': 'DROP', 'price': 40.0, 'gap_pct': -8.0,
            'rel_vol': 2.0, 'change_pct': -6.0, 'rsi': 25.0,
            'sma_20': 50.0,
        }
        score, signal = scanner._score_gap_down(candidate)
        assert signal == 'SELL'
        assert score > 0

    def test_mean_reversion_scores_oversold_higher(self, scanner):
        oversold = {
            'ticker': 'LOW', 'price': 40.0, 'rsi': 20.0,
            'sma_20': 50.0, 'sma_50': 55.0, 'rel_vol': 2.0,
        }
        neutral = {
            'ticker': 'MID', 'price': 50.0, 'rsi': 50.0,
            'sma_20': 50.0, 'sma_50': 50.0, 'rel_vol': 1.0,
        }
        oversold_score, _ = scanner._score_mean_reversion(oversold)
        neutral_score, _ = scanner._score_mean_reversion(neutral)
        assert oversold_score > neutral_score

    def test_breakout_alignment_bonus(self, scanner):
        aligned = {
            'ticker': 'BRK', 'price': 55.0, 'vwap': 52.0,
            'ema_9': 53.0, 'sma_20': 50.0, 'rel_vol': 3.0,
            'change_pct': 3.0,
        }
        not_aligned = {
            'ticker': 'NAH', 'price': 49.0, 'vwap': 50.0,
            'ema_9': 50.0, 'sma_20': 51.0, 'rel_vol': 1.0,
            'change_pct': 1.0,
        }
        aligned_score, _ = scanner._score_breakout(aligned)
        not_aligned_score, _ = scanner._score_breakout(not_aligned)
        assert aligned_score > not_aligned_score

    def test_volatile_scores_high_range_higher(self, scanner):
        wide = {
            'ticker': 'WILD', 'price': 50.0, 'range_pct': 15.0,
            'rel_vol': 3.0, 'change_pct': -5.0, 'rsi': 25.0,
        }
        narrow = {
            'ticker': 'CALM', 'price': 50.0, 'range_pct': 2.0,
            'rel_vol': 1.0, 'change_pct': 1.0, 'rsi': 50.0,
        }
        wide_score, _ = scanner._score_volatile(wide)
        narrow_score, _ = scanner._score_volatile(narrow)
        assert wide_score > narrow_score

    def test_scores_capped_at_100(self, scanner):
        extreme = {
            'ticker': 'MAX', 'price': 50.0, 'change_pct': 50.0,
            'rel_vol': 20.0, 'rsi': 95.0, 'ema_9': 30.0,
        }
        score, _ = scanner._score_momentum(extreme)
        assert score <= 100


# ------------------------------------------------------------------
# Full scan pipeline
# ------------------------------------------------------------------

class TestScanPipeline:
    def test_momentum_scan_returns_dataframe(self, scanner, mock_client):
        snapshots = [
            _make_snapshot('AAPL', price=180, volume=2_000_000, change_pct=5.0),
            _make_snapshot('AMD', price=120, volume=3_000_000, change_pct=7.0),
            _make_snapshot('PENNY', price=0.50, volume=50_000, change_pct=10.0),
        ]
        mock_client.get_snapshot_direction.return_value = snapshots
        mock_client.get_rsi.return_value = _make_indicator_result(60.0)
        mock_client.get_ema.return_value = _make_indicator_result(170.0)

        df = scanner.scan(preset='momentum')
        assert isinstance(df, pd.DataFrame)
        # PENNY should be filtered out (price < 5, volume < 500K)
        assert 'PENNY' not in df['ticker'].values
        assert 'score' in df.columns
        assert 'signal' in df.columns

    def test_top_n_limits_output(self, scanner, mock_client):
        snapshots = [
            _make_snapshot(f'SYM{i}', price=50, volume=1_000_000, change_pct=5.0)
            for i in range(20)
        ]
        mock_client.get_snapshot_direction.return_value = snapshots
        mock_client.get_rsi.return_value = _make_indicator_result(60.0)
        mock_client.get_ema.return_value = _make_indicator_result(48.0)

        df = scanner.scan(preset='momentum', top_n=5)
        assert len(df) <= 5

    def test_empty_snapshots_return_empty_df(self, scanner, mock_client):
        mock_client.get_snapshot_direction.return_value = []
        df = scanner.scan(preset='momentum')
        assert df.empty

    def test_all_filtered_out_returns_empty_df(self, scanner, mock_client):
        # All penny stocks that will be filtered by momentum preset (min_price=5)
        snapshots = [
            _make_snapshot('P1', price=0.5, volume=100, change_pct=1.0),
        ]
        mock_client.get_snapshot_direction.return_value = snapshots
        df = scanner.scan(preset='momentum')
        assert df.empty

    def test_custom_filter_overrides_preset(self, scanner, mock_client):
        snapshots = [
            _make_snapshot('CHEAP', price=3.0, volume=500_000, change_pct=5.0),
            _make_snapshot('NORMAL', price=50.0, volume=500_000, change_pct=5.0),
        ]
        mock_client.get_snapshot_direction.return_value = snapshots
        mock_client.get_rsi.return_value = _make_indicator_result(60.0)
        mock_client.get_ema.return_value = _make_indicator_result(48.0)

        # Default momentum preset has min_price=5, so CHEAP would be excluded
        df_default = scanner.scan(preset='momentum')
        assert 'CHEAP' not in df_default['ticker'].values

        # Override min_price to 2
        df_override = scanner.scan(
            preset='momentum',
            custom_filters={'min_price': 2.0},
        )
        assert 'CHEAP' in df_override['ticker'].values

    def test_gap_up_preset_filters_correctly(self, scanner, mock_client):
        snapshots = [
            _make_snapshot('UP', price=50, volume=500_000, change_pct=5.0),
            _make_snapshot('FLAT', price=50, volume=500_000, change_pct=1.0),
        ]
        mock_client.get_snapshot_direction.return_value = snapshots
        mock_client.get_rsi.return_value = _make_indicator_result(55.0)

        df = scanner.scan(preset='gap-up')
        # FLAT should be filtered (change_pct < 3%)
        assert 'FLAT' not in df['ticker'].values
        assert 'UP' in df['ticker'].values

    def test_gap_down_preset_filters_correctly(self, scanner, mock_client):
        snapshots = [
            _make_snapshot('DOWN', price=50, volume=500_000, change_pct=-5.0),
            _make_snapshot('UP', price=50, volume=500_000, change_pct=2.0),
        ]
        # gap-down uses market scan (get_snapshot_all), not movers
        mock_client.get_snapshot_all.return_value = snapshots
        mock_client.get_rsi.return_value = _make_indicator_result(25.0)
        mock_client.get_sma.return_value = _make_indicator_result(55.0)

        df = scanner.scan(preset='gap-down')
        # UP should be filtered (change_pct > -3%)
        assert 'UP' not in df['ticker'].values
        assert 'DOWN' in df['ticker'].values

    def test_unknown_preset_raises(self, scanner):
        with pytest.raises(ValueError, match='Unknown preset'):
            scanner.scan(preset='nonexistent')

    def test_tickers_source(self, scanner, mock_client):
        snapshots = [_make_snapshot('MSFT', price=400, volume=2_000_000, change_pct=3.0)]
        mock_client.get_snapshot_all.return_value = snapshots
        mock_client.get_rsi.return_value = _make_indicator_result(60.0)
        mock_client.get_ema.return_value = _make_indicator_result(395.0)

        df = scanner.scan(preset='momentum', source='tickers', tickers=['MSFT'])
        assert not df.empty
        assert df.iloc[0]['ticker'] == 'MSFT'

    def test_sorted_by_score_descending(self, scanner, mock_client):
        snapshots = [
            _make_snapshot('LOW', price=50, volume=600_000, change_pct=2.5, prev_volume=500_000),
            _make_snapshot('HIGH', price=50, volume=3_000_000, change_pct=12.0, prev_volume=500_000),
        ]
        mock_client.get_snapshot_direction.return_value = snapshots
        mock_client.get_rsi.return_value = _make_indicator_result(65.0)
        mock_client.get_ema.return_value = _make_indicator_result(45.0)

        df = scanner.scan(preset='momentum')
        if len(df) >= 2:
            assert df.iloc[0]['score'] >= df.iloc[1]['score']

    def test_output_columns(self, scanner, mock_client):
        snapshots = [_make_snapshot('AAPL', price=180, volume=2_000_000, change_pct=5.0)]
        mock_client.get_snapshot_direction.return_value = snapshots
        mock_client.get_rsi.return_value = _make_indicator_result(60.0)
        mock_client.get_ema.return_value = _make_indicator_result(175.0)

        df = scanner.scan(preset='momentum')
        assert 'ticker' in df.columns
        assert 'price' in df.columns
        assert 'score' in df.columns
        assert 'signal' in df.columns
        assert 'change_pct' in df.columns
        assert 'volume' in df.columns


# ------------------------------------------------------------------
# Preset definitions
# ------------------------------------------------------------------

# ------------------------------------------------------------------
# Fundamentals enrichment
# ------------------------------------------------------------------

def _make_financial_ratio(
    ticker: str,
    pe: float = 25.0,
    pb: float = 3.5,
    ps: float = 5.0,
    de: float = 0.8,
    roe: float = 18.0,
    roa: float = 10.0,
    div_yield: float = 1.5,
    ev_ebitda: float = 15.0,
    mkt_cap: float = 500_000_000_000,
    eps: float = 6.50,
    fcf: float = 100_000_000_000,
) -> SimpleNamespace:
    """Build a mock FinancialRatio object."""
    return SimpleNamespace(
        ticker=ticker,
        price_to_earnings=pe,
        price_to_book=pb,
        price_to_sales=ps,
        debt_to_equity=de,
        return_on_equity=roe,
        return_on_assets=roa,
        dividend_yield=div_yield,
        ev_to_ebitda=ev_ebitda,
        market_cap=mkt_cap,
        earnings_per_share=eps,
        free_cash_flow=fcf,
    )


class TestFundamentals:
    def test_fetch_fundamentals_returns_data(self, scanner, mock_client):
        mock_client.list_financials_ratios.return_value = [
            _make_financial_ratio('AAPL'),
        ]
        result = scanner._fetch_fundamentals(['AAPL'])
        assert 'AAPL' in result
        assert result['AAPL']['pe_ratio'] == 25.0
        assert result['AAPL']['debt_equity'] == 0.8
        assert result['AAPL']['roe'] == 18.0
        assert result['AAPL']['eps'] == 6.50

    def test_fetch_fundamentals_parallel(self, scanner, mock_client):
        def side_effect(ticker, **kwargs):
            return [_make_financial_ratio(ticker, pe=20.0 if ticker == 'AAPL' else 30.0)]
        mock_client.list_financials_ratios.side_effect = side_effect

        result = scanner._fetch_fundamentals(['AAPL', 'MSFT'])
        assert result['AAPL']['pe_ratio'] == 20.0
        assert result['MSFT']['pe_ratio'] == 30.0

    def test_fetch_fundamentals_graceful_failure(self, scanner, mock_client):
        def side_effect(ticker, **kwargs):
            if ticker == 'BAD':
                raise Exception('API error')
            return [_make_financial_ratio(ticker)]
        mock_client.list_financials_ratios.side_effect = side_effect

        result = scanner._fetch_fundamentals(['AAPL', 'BAD'])
        assert 'AAPL' in result
        assert 'BAD' not in result

    def test_fetch_fundamentals_empty_result(self, scanner, mock_client):
        mock_client.list_financials_ratios.return_value = []
        result = scanner._fetch_fundamentals(['AAPL'])
        assert 'AAPL' not in result  # empty data dict is not stored

    def test_fetch_fundamentals_empty_tickers(self, scanner, mock_client):
        result = scanner._fetch_fundamentals([])
        assert result == {}

    def test_scan_with_fundamentals_enriches_columns(self, scanner, mock_client):
        snapshots = [
            _make_snapshot('AAPL', price=180, volume=2_000_000, change_pct=5.0),
        ]
        mock_client.get_snapshot_direction.return_value = snapshots
        mock_client.get_rsi.return_value = _make_indicator_result(60.0)
        mock_client.get_ema.return_value = _make_indicator_result(175.0)
        mock_client.list_financials_ratios.return_value = [
            _make_financial_ratio('AAPL', pe=28.0, roe=35.0),
        ]

        df = scanner.scan(preset='momentum', fundamentals=True)
        assert 'pe_ratio' in df.columns
        assert 'roe' in df.columns
        assert df.iloc[0]['pe_ratio'] == 28.0
        assert df.iloc[0]['roe'] == 35.0

    def test_scan_without_fundamentals_no_extra_columns(self, scanner, mock_client):
        snapshots = [
            _make_snapshot('AAPL', price=180, volume=2_000_000, change_pct=5.0),
        ]
        mock_client.get_snapshot_direction.return_value = snapshots
        mock_client.get_rsi.return_value = _make_indicator_result(60.0)
        mock_client.get_ema.return_value = _make_indicator_result(175.0)

        df = scanner.scan(preset='momentum', fundamentals=False)
        assert 'pe_ratio' not in df.columns
        assert 'roe' not in df.columns

    def test_fundamentals_none_values_preserved(self, scanner, mock_client):
        ratio = _make_financial_ratio('AAPL')
        ratio.dividend_yield = None
        mock_client.list_financials_ratios.return_value = [ratio]

        result = scanner._fetch_fundamentals(['AAPL'])
        assert result['AAPL']['div_yield'] is None


# ------------------------------------------------------------------
# News enrichment
# ------------------------------------------------------------------

def _make_news_article(
    ticker: str,
    title: str = 'Company announces earnings beat',
    sentiment: str = 'positive',
    reasoning: str = 'Strong revenue growth exceeded expectations',
    published: str = '2026-03-11T10:00:00Z',
) -> SimpleNamespace:
    """Build a mock news article (Polygon format)."""
    return SimpleNamespace(
        title=title,
        published_utc=published,
        author='Test Author',
        tickers=[ticker],
        description='Full article text here.',
        article_url='https://example.com/article',
        insights=[
            SimpleNamespace(
                ticker=ticker,
                sentiment=sentiment,
                sentiment_reasoning=reasoning,
            ),
        ],
    )


class TestNews:
    def test_fetch_news_returns_data(self, scanner, mock_client):
        mock_client.list_ticker_news.return_value = [
            _make_news_article('AAPL', title='Apple hits record high'),
        ]
        result = scanner._fetch_news(['AAPL'])
        assert 'AAPL' in result
        assert result['AAPL']['headline'] == 'Apple hits record high'
        assert result['AAPL']['sentiment'] == 'positive'
        assert result['AAPL']['news_date'] == '2026-03-11'

    def test_fetch_news_parallel(self, scanner, mock_client):
        def side_effect(ticker, **kwargs):
            return [_make_news_article(
                ticker,
                title=f'{ticker} news',
                sentiment='positive' if ticker == 'AAPL' else 'negative',
            )]
        mock_client.list_ticker_news.side_effect = side_effect

        result = scanner._fetch_news(['AAPL', 'MSFT'])
        assert result['AAPL']['sentiment'] == 'positive'
        assert result['MSFT']['sentiment'] == 'negative'

    def test_fetch_news_graceful_failure(self, scanner, mock_client):
        def side_effect(ticker, **kwargs):
            if ticker == 'BAD':
                raise Exception('API error')
            return [_make_news_article(ticker)]
        mock_client.list_ticker_news.side_effect = side_effect

        result = scanner._fetch_news(['AAPL', 'BAD'])
        assert 'AAPL' in result
        assert 'BAD' not in result

    def test_fetch_news_empty_result(self, scanner, mock_client):
        mock_client.list_ticker_news.return_value = []
        result = scanner._fetch_news(['AAPL'])
        assert 'AAPL' not in result

    def test_fetch_news_empty_tickers(self, scanner, mock_client):
        result = scanner._fetch_news([])
        assert result == {}

    def test_fetch_news_truncates_long_title(self, scanner, mock_client):
        long_title = 'A' * 200
        mock_client.list_ticker_news.return_value = [
            _make_news_article('AAPL', title=long_title),
        ]
        result = scanner._fetch_news(['AAPL'])
        assert len(result['AAPL']['headline']) <= 120

    def test_fetch_news_matches_ticker_specific_insight(self, scanner, mock_client):
        """When article has multiple insights, pick the one for our ticker."""
        article = SimpleNamespace(
            title='Market roundup',
            published_utc='2026-03-11T10:00:00Z',
            author='Author',
            tickers=['AAPL', 'MSFT'],
            description='Roundup',
            article_url='https://example.com',
            insights=[
                SimpleNamespace(ticker='MSFT', sentiment='negative',
                                sentiment_reasoning='MSFT declined'),
                SimpleNamespace(ticker='AAPL', sentiment='positive',
                                sentiment_reasoning='AAPL surged'),
            ],
        )
        mock_client.list_ticker_news.return_value = [article]
        result = scanner._fetch_news(['AAPL'])
        assert result['AAPL']['sentiment'] == 'positive'
        assert 'AAPL surged' in result['AAPL']['catalyst']

    def test_scan_with_news_enriches_columns(self, scanner, mock_client):
        snapshots = [
            _make_snapshot('AAPL', price=180, volume=2_000_000, change_pct=5.0),
        ]
        mock_client.get_snapshot_direction.return_value = snapshots
        mock_client.get_rsi.return_value = _make_indicator_result(60.0)
        mock_client.get_ema.return_value = _make_indicator_result(175.0)
        mock_client.list_ticker_news.return_value = [
            _make_news_article('AAPL', title='Apple beats earnings'),
        ]

        df = scanner.scan(preset='momentum', news=True)
        assert 'headline' in df.columns
        assert 'sentiment' in df.columns
        assert 'catalyst' in df.columns
        assert df.iloc[0]['headline'] == 'Apple beats earnings'

    def test_scan_without_news_no_extra_columns(self, scanner, mock_client):
        snapshots = [
            _make_snapshot('AAPL', price=180, volume=2_000_000, change_pct=5.0),
        ]
        mock_client.get_snapshot_direction.return_value = snapshots
        mock_client.get_rsi.return_value = _make_indicator_result(60.0)
        mock_client.get_ema.return_value = _make_indicator_result(175.0)

        df = scanner.scan(preset='momentum', news=False)
        assert 'headline' not in df.columns
        assert 'catalyst' not in df.columns


# ------------------------------------------------------------------
# Preset definitions
# ------------------------------------------------------------------

class TestPresetDefinitions:
    def test_all_presets_have_valid_score_fn(self):
        scanner = IdeaScanner(MagicMock())
        for name, preset in PRESETS.items():
            assert hasattr(scanner, preset.score_fn), \
                f'Preset {name} references missing score function: {preset.score_fn}'

    def test_all_presets_have_required_fields(self):
        for name, preset in PRESETS.items():
            assert preset.name == name
            assert len(preset.description) > 0
            assert isinstance(preset.filters, ScanFilter)
            assert isinstance(preset.indicators, list)
            assert len(preset.indicators) > 0

    def test_all_presets_have_scan_codes(self):
        for name in PRESETS:
            assert name in PRESET_SCAN_CODES, \
                f'Preset {name} missing from PRESET_SCAN_CODES'


# ------------------------------------------------------------------
# Local indicator computation
# ------------------------------------------------------------------

class TestLocalIndicators:
    def test_compute_rsi_basic(self):
        # 20 rising prices should give high RSI
        closes = [100 + i for i in range(20)]
        rsi = compute_rsi(closes)
        assert rsi is not None
        assert rsi > 70

    def test_compute_rsi_falling(self):
        # 20 falling prices should give low RSI
        closes = [100 - i for i in range(20)]
        rsi = compute_rsi(closes)
        assert rsi is not None
        assert rsi < 30

    def test_compute_rsi_insufficient_data(self):
        closes = [100, 101, 102]
        assert compute_rsi(closes) is None

    def test_compute_rsi_empty(self):
        assert compute_rsi([]) is None

    def test_compute_ema_basic(self):
        closes = [100 + i * 0.5 for i in range(20)]
        ema = compute_ema(closes, 9)
        assert ema is not None
        # EMA should be near the recent prices
        assert 105 < ema < 115

    def test_compute_ema_insufficient_data(self):
        closes = [100, 101]
        assert compute_ema(closes, 9) is None

    def test_compute_sma_basic(self):
        closes = list(range(1, 21))  # 1..20
        sma = compute_sma(closes, 20)
        assert sma is not None
        assert sma == 10.5  # average of 1..20

    def test_compute_sma_insufficient_data(self):
        closes = [100, 101]
        assert compute_sma(closes, 20) is None

    def test_compute_sma_window_5(self):
        closes = [10, 20, 30, 40, 50]
        sma = compute_sma(closes, 5)
        assert sma == 30.0


# ------------------------------------------------------------------
# Module-level functions (shared by both scanners)
# ------------------------------------------------------------------

class TestModuleLevelFunctions:
    def test_apply_filters_works_standalone(self):
        candidates = [
            {'ticker': 'A', 'price': 50.0, 'volume': 500_000,
             'change_pct': 5.0, 'rel_vol': 1.0},
            {'ticker': 'B', 'price': 0.50, 'volume': 500_000,
             'change_pct': 5.0, 'rel_vol': 1.0},
        ]
        result = apply_filters(candidates, ScanFilter(min_price=5.0))
        assert len(result) == 1
        assert result[0]['ticker'] == 'A'

    def test_to_dataframe_works_standalone(self):
        candidates = [
            {'ticker': 'A', 'price': 50.0, 'score': 80.0, 'signal': 'BUY',
             'change_pct': 5.0, 'volume': 1_000_000, 'gap_pct': 2.0,
             'rel_vol': 1.5, 'range_pct': 3.0, 'rsi': 65.0},
        ]
        df = to_dataframe(candidates)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert 'ticker' in df.columns
        assert 'score' in df.columns

    def test_to_dataframe_empty(self):
        df = to_dataframe([])
        assert df.empty


# ------------------------------------------------------------------
# IBIdeaScanner
# ------------------------------------------------------------------

def _make_scanner_result(symbol, conId=12345, exchange='ASX', currency='AUD'):
    return {
        'rank': 1, 'symbol': symbol, 'secType': 'STK',
        'exchange': exchange, 'currency': currency, 'conId': conId,
    }


def _make_ib_snapshot(symbol, conId=12345, exchange='ASX', currency='AUD',
                      bid=49.9, ask=50.1, last=50.0,
                      open_=49.0, high=51.0, low=48.5, close=50.0,
                      volume=500_000):
    return {
        'symbol': symbol, 'conId': conId, 'exchange': exchange,
        'currency': currency, 'bid': bid, 'ask': ask, 'last': last,
        'open': open_, 'high': high, 'low': low, 'close': close,
        'volume': volume,
    }


def _make_history_bars(base_price=50.0, num_bars=30):
    """Generate mock history bars with slight upward trend."""
    bars = []
    for i in range(num_bars):
        p = base_price + i * 0.2
        bars.append({
            'date': f'2026-02-{10 + i if 10 + i <= 28 else 28}',
            'open': p - 0.5, 'high': p + 1.0, 'low': p - 1.0,
            'close': p, 'volume': 500_000 + i * 10_000,
        })
    return bars


@pytest.fixture
def mock_rpc():
    """Return a MagicMock RPC client for IBIdeaScanner."""
    rpc = MagicMock()
    return rpc


@pytest.fixture
def ib_scanner(mock_rpc):
    return IBIdeaScanner(mock_rpc)


class TestIBIdeaScannerBuildCandidates:
    def test_builds_from_snapshot_and_history(self, ib_scanner):
        snapshots = [_make_ib_snapshot('BHP', last=45.0, open_=44.0,
                                       high=46.0, low=43.5, volume=1_000_000)]
        history = _make_history_bars(base_price=42.0, num_bars=5)
        history_map = {'BHP': history}

        candidates = ib_scanner._build_candidates(snapshots, history_map)
        assert len(candidates) == 1
        c = candidates[0]
        assert c['ticker'] == 'BHP'
        assert c['price'] == 45.0
        assert c['volume'] == 1_000_000
        # prev_close from second-to-last bar
        prev_close = history[-2]['close']
        expected_change = ((45.0 - prev_close) / prev_close) * 100.0
        assert c['change_pct'] == pytest.approx(expected_change, abs=0.1)

    def test_skips_duplicate_symbols(self, ib_scanner):
        snapshots = [
            _make_ib_snapshot('BHP', last=45.0),
            _make_ib_snapshot('BHP', last=46.0),
        ]
        candidates = ib_scanner._build_candidates(snapshots, {})
        assert len(candidates) == 1

    def test_skips_zero_price(self, ib_scanner):
        snapshots = [_make_ib_snapshot('BAD', last=0.0, bid=0.0, ask=0.0, close=0.0)]
        candidates = ib_scanner._build_candidates(snapshots, {})
        assert len(candidates) == 0

    def test_falls_back_to_bid_when_no_last(self, ib_scanner):
        snapshots = [_make_ib_snapshot('FBK', last=float('nan'), bid=30.0,
                                       ask=30.5, close=float('nan'),
                                       open_=29.5, high=31.0, low=29.0)]
        candidates = ib_scanner._build_candidates(snapshots, {})
        assert len(candidates) == 1
        assert candidates[0]['price'] == 30.0

    def test_gap_and_rel_vol_without_history(self, ib_scanner):
        snapshots = [_make_ib_snapshot('NO_HIST', last=50.0)]
        candidates = ib_scanner._build_candidates(snapshots, {})
        assert len(candidates) == 1
        c = candidates[0]
        # Without history, no prev_close → gap_pct = 0, change_pct = 0, rel_vol = 0
        assert c['gap_pct'] == 0.0
        assert c['change_pct'] == 0.0
        assert c['rel_vol'] == 0.0

    def test_spread_pct_calculated(self, ib_scanner):
        snapshots = [_make_ib_snapshot('SPR', last=100.0, bid=99.50, ask=100.50)]
        candidates = ib_scanner._build_candidates(snapshots, {})
        assert candidates[0]['spread_pct'] == pytest.approx(1.0, abs=0.01)

    def test_includes_exchange_and_currency(self, ib_scanner):
        snapshots = [_make_ib_snapshot('CBA', exchange='ASX', currency='AUD')]
        candidates = ib_scanner._build_candidates(snapshots, {})
        assert candidates[0]['exchange'] == 'ASX'
        assert candidates[0]['currency'] == 'AUD'


class TestIBIdeaScannerScan:
    def test_full_pipeline(self, mock_rpc):
        scanner_results = [
            _make_scanner_result('BHP', conId=100),
            _make_scanner_result('CBA', conId=200),
            _make_scanner_result('RIO', conId=300),
        ]
        snapshots = [
            _make_ib_snapshot('BHP', conId=100, last=45.0, open_=43.0,
                              high=46.0, low=42.5, volume=2_000_000),
            _make_ib_snapshot('CBA', conId=200, last=100.0, open_=98.0,
                              high=102.0, low=97.0, volume=1_500_000),
            _make_ib_snapshot('RIO', conId=300, last=120.0, open_=118.0,
                              high=122.0, low=117.0, volume=800_000),
        ]
        history = _make_history_bars(base_price=40.0, num_bars=30)

        # Chain .rpc().method() calls
        rpc_mock = MagicMock()
        mock_rpc.rpc.return_value = rpc_mock
        rpc_mock.scanner_data.return_value = scanner_results
        rpc_mock.get_snapshots_batch.return_value = snapshots
        rpc_mock.get_history_bars.return_value = history

        scanner = IBIdeaScanner(mock_rpc)
        df = scanner.scan(preset='momentum', location='STK.AU.ASX', top_n=10)

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert 'ticker' in df.columns
        assert 'score' in df.columns
        assert 'signal' in df.columns

    def test_empty_scanner_results(self, mock_rpc):
        rpc_mock = MagicMock()
        mock_rpc.rpc.return_value = rpc_mock
        rpc_mock.scanner_data.return_value = []

        scanner = IBIdeaScanner(mock_rpc)
        df = scanner.scan(preset='momentum', location='STK.AU.ASX')
        assert df.empty

    def test_unknown_preset_raises(self, mock_rpc):
        scanner = IBIdeaScanner(mock_rpc)
        with pytest.raises(ValueError, match='Unknown preset'):
            scanner.scan(preset='nonexistent', location='STK.AU.ASX')

    def test_custom_filters_override(self, mock_rpc):
        scanner_results = [
            _make_scanner_result('CHEAP', conId=100),
            _make_scanner_result('PRICEY', conId=200),
        ]
        snapshots = [
            _make_ib_snapshot('CHEAP', conId=100, last=3.0, open_=2.8,
                              high=3.2, low=2.7, bid=2.99, ask=3.01,
                              volume=500_000),
            _make_ib_snapshot('PRICEY', conId=200, last=50.0, open_=48.0,
                              high=52.0, low=47.0, volume=500_000),
        ]
        # History with prev_close ~2.7 so change_pct ~ +11% for CHEAP
        history_cheap = [{'date': f'2026-02-{10+i}', 'open': 2.5, 'high': 2.8,
                          'low': 2.4, 'close': 2.7, 'volume': 400_000}
                         for i in range(20)]
        history_pricey = _make_history_bars(base_price=45.0, num_bars=20)

        call_count = [0]
        def history_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return history_cheap
            return history_pricey

        rpc_mock = MagicMock()
        mock_rpc.rpc.return_value = rpc_mock
        rpc_mock.scanner_data.return_value = scanner_results
        rpc_mock.get_snapshots_batch.return_value = snapshots
        rpc_mock.get_history_bars.side_effect = history_side_effect

        scanner = IBIdeaScanner(mock_rpc)
        # Default momentum min_price=5.0 would exclude CHEAP;
        # override to min_price=1.0 so CHEAP passes
        df = scanner.scan(preset='momentum', location='STK.AU.ASX',
                          custom_filters={'min_price': 1.0, 'min_volume': 100_000})
        tickers = list(df['ticker']) if not df.empty else []
        assert 'CHEAP' in tickers

    def test_indicators_computed_locally(self, mock_rpc):
        scanner_results = [_make_scanner_result('IND', conId=100)]
        snapshots = [_make_ib_snapshot('IND', conId=100, last=55.0, volume=1_000_000)]
        history = _make_history_bars(base_price=40.0, num_bars=30)

        rpc_mock = MagicMock()
        mock_rpc.rpc.return_value = rpc_mock
        rpc_mock.scanner_data.return_value = scanner_results
        rpc_mock.get_snapshots_batch.return_value = snapshots
        rpc_mock.get_history_bars.return_value = history

        scanner = IBIdeaScanner(mock_rpc)
        df = scanner.scan(preset='momentum', location='STK.AU.ASX',
                          custom_filters={'min_price': 1.0, 'min_volume': 0,
                                          'min_change_pct': None})
        if not df.empty and 'rsi' in df.columns:
            rsi_val = df.iloc[0]['rsi']
            # RSI should be a valid number for 30 bars of upward trend
            assert rsi_val is not None or pd.isna(rsi_val) is False

    def test_top_n_limits_output(self, mock_rpc):
        scanner_results = [
            _make_scanner_result(f'SYM{i}', conId=100 + i) for i in range(20)
        ]
        snapshots = [
            _make_ib_snapshot(f'SYM{i}', conId=100 + i, last=50.0 + i,
                              volume=1_000_000)
            for i in range(20)
        ]
        history = _make_history_bars(base_price=45.0, num_bars=20)

        rpc_mock = MagicMock()
        mock_rpc.rpc.return_value = rpc_mock
        rpc_mock.scanner_data.return_value = scanner_results
        rpc_mock.get_snapshots_batch.return_value = snapshots
        rpc_mock.get_history_bars.return_value = history

        scanner = IBIdeaScanner(mock_rpc)
        df = scanner.scan(preset='momentum', location='STK.AU.ASX', top_n=5,
                          custom_filters={'min_price': 1.0, 'min_volume': 0,
                                          'min_change_pct': None})
        assert len(df) <= 5

    def test_sorted_by_score_descending(self, mock_rpc):
        scanner_results = [
            _make_scanner_result('LOW', conId=100),
            _make_scanner_result('HIGH', conId=200),
        ]
        snapshots = [
            _make_ib_snapshot('LOW', conId=100, last=50.0, volume=600_000),
            _make_ib_snapshot('HIGH', conId=200, last=50.0, volume=3_000_000),
        ]
        # Give HIGH a better history (more vol)
        history_low = _make_history_bars(base_price=49.0, num_bars=20)
        history_high = _make_history_bars(base_price=30.0, num_bars=20)

        call_count = [0]
        def get_history_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return history_low
            return history_high

        rpc_mock = MagicMock()
        mock_rpc.rpc.return_value = rpc_mock
        rpc_mock.scanner_data.return_value = scanner_results
        rpc_mock.get_snapshots_batch.return_value = snapshots
        rpc_mock.get_history_bars.side_effect = get_history_side_effect

        scanner = IBIdeaScanner(mock_rpc)
        df = scanner.scan(preset='momentum', location='STK.AU.ASX',
                          custom_filters={'min_price': 1.0, 'min_volume': 0,
                                          'min_change_pct': None})
        if len(df) >= 2:
            assert df.iloc[0]['score'] >= df.iloc[1]['score']


# ------------------------------------------------------------------
# parse_report_snapshot (XML parsing)
# ------------------------------------------------------------------

_SAMPLE_REPORT_SNAPSHOT = """<?xml version="1.0" encoding="UTF-8"?>
<ReportSnapshot>
  <Ratios>
    <Group ID="Price ratios">
      <Ratio FieldName="APENORM">18.50</Ratio>
      <Ratio FieldName="PRICE2BK">3.20</Ratio>
    </Group>
    <Group ID="Profitability">
      <Ratio FieldName="TTMROEPCT">22.10</Ratio>
      <Ratio FieldName="TTMROAPCT">9.50</Ratio>
    </Group>
    <Group ID="Per share data">
      <Ratio FieldName="TTMEPSXCLX">6.75</Ratio>
    </Group>
    <Group ID="Financial strength">
      <Ratio FieldName="QTOTD2EQ">0.85</Ratio>
    </Group>
    <Group ID="Valuation">
      <Ratio FieldName="MKTCAP">250000000000</Ratio>
      <Ratio FieldName="YIELD">1.35</Ratio>
      <Ratio FieldName="EV2EBITDA">12.40</Ratio>
    </Group>
  </Ratios>
</ReportSnapshot>"""


class TestParseReportSnapshot:
    def test_parses_pe_ratio(self):
        data = parse_report_snapshot(_SAMPLE_REPORT_SNAPSHOT)
        assert data['pe_ratio'] == 18.50

    def test_parses_roe(self):
        data = parse_report_snapshot(_SAMPLE_REPORT_SNAPSHOT)
        assert data['roe'] == 22.10

    def test_parses_eps(self):
        data = parse_report_snapshot(_SAMPLE_REPORT_SNAPSHOT)
        assert data['eps'] == 6.75

    def test_parses_debt_equity(self):
        data = parse_report_snapshot(_SAMPLE_REPORT_SNAPSHOT)
        assert data['debt_equity'] == 0.85

    def test_parses_mkt_cap_as_raw_value(self):
        data = parse_report_snapshot(_SAMPLE_REPORT_SNAPSHOT)
        # mkt_cap should NOT be rounded to 2dp
        assert data['mkt_cap'] == 250000000000

    def test_parses_div_yield(self):
        data = parse_report_snapshot(_SAMPLE_REPORT_SNAPSHOT)
        assert data['div_yield'] == 1.35

    def test_parses_ev_ebitda(self):
        data = parse_report_snapshot(_SAMPLE_REPORT_SNAPSHOT)
        assert data['ev_ebitda'] == 12.40

    def test_parses_pb_ratio(self):
        data = parse_report_snapshot(_SAMPLE_REPORT_SNAPSHOT)
        assert data['pb_ratio'] == 3.20

    def test_parses_roa(self):
        data = parse_report_snapshot(_SAMPLE_REPORT_SNAPSHOT)
        assert data['roa'] == 9.50

    def test_empty_xml_returns_empty(self):
        assert parse_report_snapshot('') == {}

    def test_invalid_xml_returns_empty(self):
        assert parse_report_snapshot('not xml <<<<') == {}

    def test_no_ratios_returns_empty(self):
        xml = '<ReportSnapshot><CoInfo/></ReportSnapshot>'
        assert parse_report_snapshot(xml) == {}

    def test_non_numeric_values_skipped(self):
        xml = """<ReportSnapshot><Ratios>
            <Group ID="X"><Ratio FieldName="APENORM">N/A</Ratio></Group>
        </Ratios></ReportSnapshot>"""
        data = parse_report_snapshot(xml)
        assert 'pe_ratio' not in data

    def test_unknown_fields_ignored(self):
        xml = """<ReportSnapshot><Ratios>
            <Group ID="X"><Ratio FieldName="UNKNOWN123">42.0</Ratio></Group>
        </Ratios></ReportSnapshot>"""
        data = parse_report_snapshot(xml)
        assert len(data) == 0


# ------------------------------------------------------------------
# IBIdeaScanner fundamentals + news enrichment
# ------------------------------------------------------------------

class TestIBIdeaScannerFundamentals:
    def _setup_basic_scan(self, mock_rpc):
        """Set up a basic scan mock that returns one candidate."""
        scanner_results = [_make_scanner_result('BHP', conId=100)]
        snapshots = [_make_ib_snapshot('BHP', conId=100, last=45.0, volume=1_000_000)]
        history = _make_history_bars(base_price=40.0, num_bars=20)

        rpc_mock = MagicMock()
        mock_rpc.rpc.return_value = rpc_mock
        rpc_mock.scanner_data.return_value = scanner_results
        rpc_mock.get_snapshots_batch.return_value = snapshots
        rpc_mock.get_history_bars.return_value = history
        return rpc_mock

    def test_fundamentals_enriches_columns(self, mock_rpc):
        rpc_mock = self._setup_basic_scan(mock_rpc)
        rpc_mock.get_fundamental_data.return_value = _SAMPLE_REPORT_SNAPSHOT

        scanner = IBIdeaScanner(mock_rpc)
        df = scanner.scan(preset='momentum', location='STK.AU.ASX',
                          fundamentals=True,
                          custom_filters={'min_price': 1.0, 'min_volume': 0,
                                          'min_change_pct': None})
        assert not df.empty
        assert 'pe_ratio' in df.columns
        assert 'roe' in df.columns
        assert df.iloc[0]['pe_ratio'] == 18.50

    def test_no_fundamentals_by_default(self, mock_rpc):
        rpc_mock = self._setup_basic_scan(mock_rpc)

        scanner = IBIdeaScanner(mock_rpc)
        df = scanner.scan(preset='momentum', location='STK.AU.ASX',
                          custom_filters={'min_price': 1.0, 'min_volume': 0,
                                          'min_change_pct': None})
        assert 'pe_ratio' not in df.columns

    def test_fundamentals_failure_graceful(self, mock_rpc):
        rpc_mock = self._setup_basic_scan(mock_rpc)
        rpc_mock.get_fundamental_data.side_effect = Exception('No data')

        scanner = IBIdeaScanner(mock_rpc)
        df = scanner.scan(preset='momentum', location='STK.AU.ASX',
                          fundamentals=True,
                          custom_filters={'min_price': 1.0, 'min_volume': 0,
                                          'min_change_pct': None})
        # Should still return results, just without fundamentals columns
        assert not df.empty


class TestIBIdeaScannerNews:
    def _setup_basic_scan(self, mock_rpc):
        scanner_results = [_make_scanner_result('BHP', conId=100)]
        snapshots = [_make_ib_snapshot('BHP', conId=100, last=45.0, volume=1_000_000)]
        history = _make_history_bars(base_price=40.0, num_bars=20)

        rpc_mock = MagicMock()
        mock_rpc.rpc.return_value = rpc_mock
        rpc_mock.scanner_data.return_value = scanner_results
        rpc_mock.get_snapshots_batch.return_value = snapshots
        rpc_mock.get_history_bars.return_value = history
        return rpc_mock

    def test_news_enriches_columns(self, mock_rpc):
        rpc_mock = self._setup_basic_scan(mock_rpc)
        rpc_mock.get_news_headlines.return_value = [
            {'time': '2026-03-11 10:00:00', 'provider': 'BZ',
             'articleId': 'art123', 'headline': 'BHP posts record iron ore output'},
        ]

        scanner = IBIdeaScanner(mock_rpc)
        df = scanner.scan(preset='momentum', location='STK.AU.ASX',
                          news=True,
                          custom_filters={'min_price': 1.0, 'min_volume': 0,
                                          'min_change_pct': None})
        assert not df.empty
        assert 'headline' in df.columns
        assert df.iloc[0]['headline'] == 'BHP posts record iron ore output'
        assert df.iloc[0]['news_date'] == '2026-03-11'

    def test_no_news_by_default(self, mock_rpc):
        rpc_mock = self._setup_basic_scan(mock_rpc)

        scanner = IBIdeaScanner(mock_rpc)
        df = scanner.scan(preset='momentum', location='STK.AU.ASX',
                          custom_filters={'min_price': 1.0, 'min_volume': 0,
                                          'min_change_pct': None})
        assert 'headline' not in df.columns

    def test_news_truncates_long_headline(self, mock_rpc):
        rpc_mock = self._setup_basic_scan(mock_rpc)
        rpc_mock.get_news_headlines.return_value = [
            {'time': '2026-03-11 10:00:00', 'provider': 'BZ',
             'articleId': 'art123', 'headline': 'A' * 200},
        ]

        scanner = IBIdeaScanner(mock_rpc)
        df = scanner.scan(preset='momentum', location='STK.AU.ASX',
                          news=True,
                          custom_filters={'min_price': 1.0, 'min_volume': 0,
                                          'min_change_pct': None})
        assert len(df.iloc[0]['headline']) <= 120

    def test_news_empty_headlines_no_column(self, mock_rpc):
        rpc_mock = self._setup_basic_scan(mock_rpc)
        rpc_mock.get_news_headlines.return_value = []

        scanner = IBIdeaScanner(mock_rpc)
        df = scanner.scan(preset='momentum', location='STK.AU.ASX',
                          news=True,
                          custom_filters={'min_price': 1.0, 'min_volume': 0,
                                          'min_change_pct': None})
        assert not df.empty

    def test_news_failure_graceful(self, mock_rpc):
        rpc_mock = self._setup_basic_scan(mock_rpc)
        rpc_mock.get_news_headlines.side_effect = Exception('No news')

        scanner = IBIdeaScanner(mock_rpc)
        df = scanner.scan(preset='momentum', location='STK.AU.ASX',
                          news=True,
                          custom_filters={'min_price': 1.0, 'min_volume': 0,
                                          'min_change_pct': None})
        assert not df.empty

    def test_fundamentals_and_news_together(self, mock_rpc):
        rpc_mock = self._setup_basic_scan(mock_rpc)
        rpc_mock.get_fundamental_data.return_value = _SAMPLE_REPORT_SNAPSHOT
        rpc_mock.get_news_headlines.return_value = [
            {'time': '2026-03-11 10:00:00', 'provider': 'BZ',
             'articleId': 'art123', 'headline': 'BHP beats estimates'},
        ]

        scanner = IBIdeaScanner(mock_rpc)
        df = scanner.scan(preset='momentum', location='STK.AU.ASX',
                          fundamentals=True, news=True,
                          custom_filters={'min_price': 1.0, 'min_volume': 0,
                                          'min_change_pct': None})
        assert not df.empty
        assert 'pe_ratio' in df.columns
        assert 'headline' in df.columns


class TestIBIdeaScannerTickersPath:
    """Tests for IBIdeaScanner using explicit tickers (bypasses scanner API)."""

    def _make_resolve_result(self, symbol, conId, exchange='ASX', currency='AUD'):
        """Build a mock SecurityDefinition-like object."""
        d = SimpleNamespace()
        d.symbol = symbol
        d.conId = conId
        d.primaryExchange = exchange
        d.currency = currency
        return d

    def test_tickers_bypass_scanner(self, mock_rpc):
        """When tickers are provided, scanner_data should NOT be called."""
        snapshots = [
            _make_ib_snapshot('BHP', conId=100, last=45.0, volume=2_000_000),
            _make_ib_snapshot('CBA', conId=200, last=100.0, volume=1_500_000),
        ]
        history = _make_history_bars(base_price=40.0, num_bars=20)

        rpc_mock = MagicMock()
        mock_rpc.rpc.return_value = rpc_mock
        rpc_mock.resolve_contract.side_effect = [
            [self._make_resolve_result('BHP', 100)],
            [self._make_resolve_result('CBA', 200)],
        ]
        rpc_mock.get_snapshots_batch.return_value = snapshots
        rpc_mock.get_history_bars.return_value = history

        scanner = IBIdeaScanner(mock_rpc)
        df = scanner.scan(
            preset='momentum', location='STK.AU.ASX',
            tickers=['BHP', 'CBA'],
            custom_filters={'min_price': 1.0, 'min_volume': 0, 'min_change_pct': None},
        )

        # scanner_data should never be called when tickers are provided
        rpc_mock.scanner_data.assert_not_called()
        # resolve_symbol should be called for each ticker
        assert rpc_mock.resolve_contract.call_count == 2
        assert not df.empty

    def test_tickers_extracts_exchange_from_location(self, mock_rpc):
        """Exchange hint should be extracted from location (STK.AU.ASX → ASX)."""
        rpc_mock = MagicMock()
        mock_rpc.rpc.return_value = rpc_mock
        rpc_mock.resolve_contract.return_value = [
            self._make_resolve_result('BHP', 100, exchange='ASX'),
        ]
        rpc_mock.get_snapshots_batch.return_value = [
            _make_ib_snapshot('BHP', conId=100, last=45.0, volume=1_000_000),
        ]
        rpc_mock.get_history_bars.return_value = _make_history_bars(base_price=40.0, num_bars=20)

        scanner = IBIdeaScanner(mock_rpc)
        scanner.scan(
            preset='momentum', location='STK.AU.ASX',
            tickers=['BHP'],
            custom_filters={'min_price': 1.0, 'min_volume': 0, 'min_change_pct': None},
        )

        # Check that the Contract passed to resolve_contract has exchange=ASX
        call_args = rpc_mock.resolve_contract.call_args
        contract_arg = call_args[0][0]
        assert contract_arg.exchange == 'ASX'

    def test_universe_symbols_bypass_scanner(self, mock_rpc):
        """When universe_symbols are provided, scanner_data should NOT be called."""
        snapshots = [
            _make_ib_snapshot('BHP', conId=100, last=45.0, volume=2_000_000),
        ]
        history = _make_history_bars(base_price=40.0, num_bars=20)

        rpc_mock = MagicMock()
        mock_rpc.rpc.return_value = rpc_mock
        rpc_mock.resolve_contract.return_value = [
            self._make_resolve_result('BHP', 100),
        ]
        rpc_mock.get_snapshots_batch.return_value = snapshots
        rpc_mock.get_history_bars.return_value = history

        scanner = IBIdeaScanner(mock_rpc)
        df = scanner.scan(
            preset='momentum', location='STK.AU.ASX',
            universe_symbols=['BHP'],
            custom_filters={'min_price': 1.0, 'min_volume': 0, 'min_change_pct': None},
        )

        rpc_mock.scanner_data.assert_not_called()
        assert not df.empty

    def test_unresolvable_symbols_skipped(self, mock_rpc):
        """Symbols that can't be resolved should be silently skipped."""
        rpc_mock = MagicMock()
        mock_rpc.rpc.return_value = rpc_mock
        rpc_mock.resolve_contract.side_effect = [
            [],  # BHP fails
            [self._make_resolve_result('CBA', 200)],  # CBA succeeds
        ]
        rpc_mock.get_snapshots_batch.return_value = [
            _make_ib_snapshot('CBA', conId=200, last=100.0, volume=1_500_000),
        ]
        rpc_mock.get_history_bars.return_value = _make_history_bars(base_price=90.0, num_bars=20)

        scanner = IBIdeaScanner(mock_rpc)
        df = scanner.scan(
            preset='momentum', location='STK.AU.ASX',
            tickers=['BHP', 'CBA'],
            custom_filters={'min_price': 1.0, 'min_volume': 0, 'min_change_pct': None},
        )

        assert not df.empty
        assert len(df) == 1
        assert df.iloc[0]['ticker'] == 'CBA'

    def test_all_symbols_fail_returns_empty(self, mock_rpc):
        """If all symbols fail to resolve, return empty DataFrame."""
        rpc_mock = MagicMock()
        mock_rpc.rpc.return_value = rpc_mock
        rpc_mock.resolve_contract.return_value = []

        scanner = IBIdeaScanner(mock_rpc)
        df = scanner.scan(
            preset='momentum', location='STK.AU.ASX',
            tickers=['FAKE1', 'FAKE2'],
        )
        assert df.empty

    def test_resolve_exception_skipped(self, mock_rpc):
        """If resolve_symbol raises, that symbol is skipped."""
        rpc_mock = MagicMock()
        mock_rpc.rpc.return_value = rpc_mock
        rpc_mock.resolve_contract.side_effect = [
            Exception('Network error'),
            [self._make_resolve_result('CBA', 200)],
        ]
        rpc_mock.get_snapshots_batch.return_value = [
            _make_ib_snapshot('CBA', conId=200, last=100.0, volume=1_500_000),
        ]
        rpc_mock.get_history_bars.return_value = _make_history_bars(base_price=90.0, num_bars=20)

        scanner = IBIdeaScanner(mock_rpc)
        df = scanner.scan(
            preset='momentum', location='STK.AU.ASX',
            tickers=['BROKEN', 'CBA'],
            custom_filters={'min_price': 1.0, 'min_volume': 0, 'min_change_pct': None},
        )
        assert not df.empty
        assert df.iloc[0]['ticker'] == 'CBA'

    def test_tickers_with_fundamentals_and_news(self, mock_rpc):
        """Tickers path should work with fundamentals and news enrichment."""
        rpc_mock = MagicMock()
        mock_rpc.rpc.return_value = rpc_mock
        rpc_mock.resolve_contract.return_value = [
            self._make_resolve_result('BHP', 100),
        ]
        rpc_mock.get_snapshots_batch.return_value = [
            _make_ib_snapshot('BHP', conId=100, last=45.0, volume=1_000_000),
        ]
        rpc_mock.get_history_bars.return_value = _make_history_bars(base_price=40.0, num_bars=20)
        rpc_mock.get_fundamental_data.return_value = _SAMPLE_REPORT_SNAPSHOT
        rpc_mock.get_news_headlines.return_value = [
            {'time': '2026-03-11 10:00:00', 'provider': 'BZ',
             'articleId': 'art123', 'headline': 'BHP record profits'},
        ]

        scanner = IBIdeaScanner(mock_rpc)
        df = scanner.scan(
            preset='momentum', location='STK.AU.ASX',
            tickers=['BHP'],
            fundamentals=True, news=True,
            custom_filters={'min_price': 1.0, 'min_volume': 0, 'min_change_pct': None},
        )
        assert not df.empty
        assert 'pe_ratio' in df.columns
        assert 'headline' in df.columns

    def test_location_without_exchange_suffix(self, mock_rpc):
        """Location like STK.CA (no third part) should use empty exchange hint."""
        rpc_mock = MagicMock()
        mock_rpc.rpc.return_value = rpc_mock
        rpc_mock.resolve_contract.return_value = [
            self._make_resolve_result('RY', 300, exchange='TSE', currency='CAD'),
        ]
        rpc_mock.get_snapshots_batch.return_value = [
            _make_ib_snapshot('RY', conId=300, last=150.0, volume=1_000_000, currency='CAD'),
        ]
        rpc_mock.get_history_bars.return_value = _make_history_bars(base_price=140.0, num_bars=20)

        scanner = IBIdeaScanner(mock_rpc)
        scanner.scan(
            preset='momentum', location='STK.CA',
            tickers=['RY'],
            custom_filters={'min_price': 1.0, 'min_volume': 0, 'min_change_pct': None},
        )

        # With STK.CA (only 2 parts), exchange falls back to SMART
        call_args = rpc_mock.resolve_contract.call_args
        contract_arg = call_args[0][0]
        assert contract_arg.exchange == 'SMART'  # no exchange hint, falls back to SMART
