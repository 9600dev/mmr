"""Unit tests for TwelveDataReactive."""
from __future__ import annotations

import datetime as dt
from typing import Any
from unittest.mock import MagicMock, patch

import pytest


from trader.listeners.twelvedata_reactive import (
    TwelveDataReactive,
    _denormalize_td_symbol,
    _is_forex_pair,
    _normalize_td_symbol,
)


# ---------------------------------------------------------------------------
# Symbol normalization
# ---------------------------------------------------------------------------

class TestSymbolNormalization:
    def test_forex_pair_gets_slash(self):
        assert _normalize_td_symbol("EURUSD") == "EUR/USD"
        assert _normalize_td_symbol("GBPJPY") == "GBP/JPY"
        assert _normalize_td_symbol("eurusd") == "EUR/USD"

    def test_already_normalized_passthrough(self):
        assert _normalize_td_symbol("EUR/USD") == "EUR/USD"

    def test_equity_passthrough(self):
        assert _normalize_td_symbol("AAPL") == "AAPL"
        assert _normalize_td_symbol("SPY") == "SPY"

    def test_crypto_3char_base(self):
        assert _normalize_td_symbol("BTCUSD") == "BTC/USD"
        assert _normalize_td_symbol("ETHUSD") == "ETH/USD"

    def test_crypto_4char_base(self):
        # DOGE + USD = 7 chars; multi-len lookup should handle it
        assert _normalize_td_symbol("DOGEUSD") == "DOGE/USD"

    def test_strip_polygon_prefix(self):
        assert _normalize_td_symbol("C:EURUSD") == "EUR/USD"

    def test_denormalize_strips_slash(self):
        assert _denormalize_td_symbol("EUR/USD") == "EURUSD"
        assert _denormalize_td_symbol("AAPL") == "AAPL"

    def test_is_forex_pair(self):
        assert _is_forex_pair("EURUSD")
        assert _is_forex_pair("EUR/USD")
        assert not _is_forex_pair("AAPL")
        assert not _is_forex_pair("BTCUSD")  # crypto, not forex
        assert not _is_forex_pair("EURXX")


# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------

class TestConstructor:
    def test_requires_api_key(self):
        with pytest.raises(ValueError, match="twelvedata_api_key is required"):
            TwelveDataReactive(twelvedata_api_key="")

    def test_subjects_initialized(self):
        r = TwelveDataReactive(twelvedata_api_key="abc")
        assert r.agg_subject is not None
        assert r.trade_subject is not None
        assert r.quote_subject is not None


# ---------------------------------------------------------------------------
# Event → Ticker translation
# ---------------------------------------------------------------------------

class TestEventTranslation:
    def test_price_event_to_ticker_with_quotes(self):
        r = TwelveDataReactive(twelvedata_api_key="x")
        # mock universe lookup to avoid DuckDB I/O
        r._symbol_cache["AAPL"] = None

        event = {
            "event": "price",
            "symbol": "AAPL",
            "currency": "USD",
            "exchange": "NASDAQ",
            "type": "Common Stock",
            "timestamp": 1700000000,
            "price": 150.25,
            "bid": 150.20,
            "ask": 150.30,
            "day_volume": 12345678,
        }
        ticker = r._event_to_ticker(event)
        assert ticker is not None
        assert ticker.last == 150.25
        assert ticker.bid == 150.20
        assert ticker.ask == 150.30
        assert ticker.volume == 12345678
        assert ticker.contract.symbol == "AAPL"
        assert isinstance(ticker.time, dt.datetime)

    def test_price_event_without_bid_ask(self):
        r = TwelveDataReactive(twelvedata_api_key="x")
        r._symbol_cache["AAPL"] = None
        event = {
            "event": "price",
            "symbol": "AAPL",
            "timestamp": 1700000000,
            "price": 150.0,
        }
        ticker = r._event_to_ticker(event)
        # bid/ask must be NaN (the dashboard NaN-checks them)
        assert ticker.bid != ticker.bid
        assert ticker.ask != ticker.ask

    def test_forex_event_resolves_cash_contract(self):
        r = TwelveDataReactive(twelvedata_api_key="x")
        event = {
            "event": "price",
            "symbol": "EUR/USD",
            "timestamp": 1700000000,
            "price": 1.0876,
            "bid": 1.0875,
            "ask": 1.0877,
        }
        ticker = r._event_to_ticker(event)
        assert ticker.contract.symbol == "EUR"
        assert ticker.contract.secType == "CASH"
        assert ticker.contract.currency == "USD"
        assert ticker.contract.exchange == "IDEALPRO"

    def test_no_symbol_returns_none(self):
        r = TwelveDataReactive(twelvedata_api_key="x")
        assert r._event_to_ticker({"event": "price", "price": 100}) is None


# ---------------------------------------------------------------------------
# Subject fan-out
# ---------------------------------------------------------------------------

class TestFanOut:
    def test_price_with_bid_ask_emits_to_agg_and_quote(self):
        r = TwelveDataReactive(twelvedata_api_key="x")
        r._symbol_cache["AAPL"] = None

        agg_received: list[Any] = []
        quote_received: list[Any] = []
        r.agg_subject.subscribe(on_next=agg_received.append)
        r.quote_subject.subscribe(on_next=quote_received.append)

        r._on_event({
            "event": "price",
            "symbol": "AAPL",
            "timestamp": 1700000000,
            "price": 150.0,
            "bid": 149.99,
            "ask": 150.01,
        })

        assert len(agg_received) == 1
        assert len(quote_received) == 1

    def test_price_without_quotes_emits_only_agg(self):
        r = TwelveDataReactive(twelvedata_api_key="x")
        r._symbol_cache["AAPL"] = None

        agg_received: list[Any] = []
        quote_received: list[Any] = []
        r.agg_subject.subscribe(on_next=agg_received.append)
        r.quote_subject.subscribe(on_next=quote_received.append)

        r._on_event({
            "event": "price",
            "symbol": "AAPL",
            "timestamp": 1700000000,
            "price": 150.0,
        })

        assert len(agg_received) == 1
        assert len(quote_received) == 0

    def test_subscribe_status_event_does_not_fan_out(self):
        r = TwelveDataReactive(twelvedata_api_key="x")
        agg: list[Any] = []
        r.agg_subject.subscribe(on_next=agg.append)

        r._on_event({"event": "subscribe-status", "status": "ok"})
        r._on_event({"event": "heartbeat"})

        assert len(agg) == 0

    def test_handler_swallows_exceptions(self):
        r = TwelveDataReactive(twelvedata_api_key="x")
        # malformed event — must not raise
        r._on_event({"event": "price", "symbol": "AAPL", "timestamp": "garbage"})


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

class TestLifecycle:
    def test_start_normalizes_symbols_and_connects(self):
        r = TwelveDataReactive(twelvedata_api_key="x")
        fake_ws = MagicMock()
        fake_client = MagicMock()
        fake_client.websocket.return_value = fake_ws
        with patch(
            "trader.listeners.twelvedata_reactive.TDClient",
            return_value=fake_client,
        ):
            r.start(symbols=["AAPL", "EURUSD", "BTCUSD"])

        # Verify normalized symbols passed to TD
        called_kwargs = fake_client.websocket.call_args.kwargs
        assert called_kwargs["symbols"] == ["AAPL", "EUR/USD", "BTC/USD"]
        assert callable(called_kwargs["on_event"])
        fake_ws.connect.assert_called_once()

    def test_stop_disconnects_and_completes_subjects(self):
        r = TwelveDataReactive(twelvedata_api_key="x")
        fake_ws = MagicMock()
        r._ws = fake_ws

        r.stop()

        fake_ws.disconnect.assert_called_once()
        assert r._ws is None

    def test_start_failure_raises(self):
        r = TwelveDataReactive(twelvedata_api_key="x")
        fake_ws = MagicMock()
        fake_ws.connect.side_effect = RuntimeError("plan tier insufficient")
        fake_client = MagicMock()
        fake_client.websocket.return_value = fake_ws
        with patch(
            "trader.listeners.twelvedata_reactive.TDClient",
            return_value=fake_client,
        ):
            with pytest.raises(RuntimeError, match="plan tier insufficient"):
                r.start(symbols=["AAPL"])
