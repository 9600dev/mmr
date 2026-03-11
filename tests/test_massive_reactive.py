import datetime as dt
from unittest.mock import MagicMock, patch

import pytest
import pytz

from ib_async.ticker import Ticker


# ---------------------------------------------------------------------------
# TestAggToTicker
# ---------------------------------------------------------------------------

class TestAggToTicker:
    def _make_reactive(self):
        from trader.listeners.massive_reactive import MassiveReactive
        return MassiveReactive(
            massive_api_key='test_key',
            massive_feed='stocks',
            massive_delayed=False,
            duckdb_path='',
            universe_library='Universes',
        )

    def _make_equity_agg(self, symbol='AAPL', close=150.0, open_=149.0, high=151.0,
                         low=148.0, volume=1000.0, acc_vol=50000.0, vwap=150.5,
                         end_ts=None):
        agg = MagicMock()
        agg.symbol = symbol
        agg.close = close
        agg.open = open_
        agg.high = high
        agg.low = low
        agg.volume = volume
        agg.accumulated_volume = acc_vol
        agg.vwap = vwap
        agg.end_timestamp = end_ts or int(dt.datetime(2024, 6, 1, 14, 30, tzinfo=dt.timezone.utc).timestamp() * 1000)
        return agg

    def test_agg_to_ticker_basic(self):
        reactive = self._make_reactive()
        agg = self._make_equity_agg()

        ticker = reactive._agg_to_ticker(agg)

        assert isinstance(ticker, Ticker)
        assert ticker.contract.symbol == 'AAPL'
        assert ticker.last == 150.0
        assert ticker.close == 150.0
        assert ticker.open == 149.0
        assert ticker.high == 151.0
        assert ticker.low == 148.0
        assert ticker.volume == 50000.0
        assert ticker.vwap == 150.5
        assert ticker.time is not None

    def test_agg_to_ticker_none_values(self):
        reactive = self._make_reactive()
        agg = self._make_equity_agg(close=None, open_=None, high=None, low=None,
                                     volume=None, acc_vol=None, vwap=None)

        ticker = reactive._agg_to_ticker(agg)

        assert isinstance(ticker, Ticker)
        import math
        assert math.isnan(ticker.last)
        assert math.isnan(ticker.open)
        assert math.isnan(ticker.high)
        assert math.isnan(ticker.low)
        assert math.isnan(ticker.volume)
        assert math.isnan(ticker.vwap)


# ---------------------------------------------------------------------------
# TestTradeToTicker
# ---------------------------------------------------------------------------

class TestTradeToTicker:
    def _make_reactive(self):
        from trader.listeners.massive_reactive import MassiveReactive
        return MassiveReactive(
            massive_api_key='test_key',
            massive_feed='stocks',
            massive_delayed=False,
            duckdb_path='',
            universe_library='Universes',
        )

    def _make_equity_trade(self, symbol='MSFT', price=420.0, size=100, ts_ns=None):
        trade = MagicMock()
        trade.symbol = symbol
        trade.price = price
        trade.size = size
        trade.timestamp = ts_ns or int(dt.datetime(2024, 6, 1, 14, 30, tzinfo=dt.timezone.utc).timestamp() * 1_000_000_000)
        return trade

    def test_trade_to_ticker_basic(self):
        reactive = self._make_reactive()
        trade = self._make_equity_trade()

        ticker = reactive._trade_to_ticker(trade)

        assert isinstance(ticker, Ticker)
        assert ticker.contract.symbol == 'MSFT'
        assert ticker.last == 420.0
        assert ticker.lastSize == 100.0
        assert ticker.time is not None

    def test_trade_to_ticker_none_values(self):
        reactive = self._make_reactive()
        trade = self._make_equity_trade(price=None, size=None)

        ticker = reactive._trade_to_ticker(trade)

        import math
        assert math.isnan(ticker.last)
        assert math.isnan(ticker.lastSize)


# ---------------------------------------------------------------------------
# TestSymbolResolution
# ---------------------------------------------------------------------------

class TestSymbolResolution:
    def test_fallback_contract_when_no_duckdb(self):
        from trader.listeners.massive_reactive import MassiveReactive

        reactive = MassiveReactive(
            massive_api_key='test_key',
            duckdb_path='',
        )

        contract = reactive._get_contract('AAPL')
        assert contract.symbol == 'AAPL'
        assert contract.secType == 'STK'
        assert contract.exchange == 'SMART'
        assert contract.currency == 'USD'
        # conId defaults to 0 when no universe lookup
        assert contract.conId == 0

    def test_symbol_cache(self):
        from trader.listeners.massive_reactive import MassiveReactive

        reactive = MassiveReactive(
            massive_api_key='test_key',
            duckdb_path='',
        )

        # First call caches
        reactive._get_contract('AAPL')
        assert 'AAPL' in reactive._symbol_cache

        # Second call uses cache
        contract = reactive._get_contract('AAPL')
        assert contract.symbol == 'AAPL'
