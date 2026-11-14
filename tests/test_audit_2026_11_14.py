"""Regressions for the 2026-11-14 audit (docs/AUDIT_2026-11-14.md).

Each class pins one defect found that day — by exercising the paper stack, by
reading the nightly cron logs against the store, or by review — in the form
that failed before its fix.
"""

from __future__ import annotations

import datetime as dt
import logging
import math
from types import SimpleNamespace

import exchange_calendars
import msgpack
import pandas as pd
import pytest

from trader.common.reactivex import SuccessFail
from trader.data.data_access import session_fetch_window, TickStorage
from trader.data.listing_guard import is_us_listing, listing_source_mismatch
from trader.data.market_session import calendar_name_for
from trader.data.session_guard import calendar_for
from trader.data.store import DateRange
from trader.objects import BarSize
from trader.trading.proposal import (
    ExecutionSpec, exit_geometry_errors, protective_trigger_error,
)
from trader.trading.risk_gate import coerce_limit, RiskGate, RiskLimits
from trader.trading.trading_filter import (
    ReloadingTradingFilter, TradingFilter, TradingFilterError,
)

UTC = dt.timezone.utc


# --------------------------------------------------------------------------
# Order safety: exit legs on the wrong side of the entry
# --------------------------------------------------------------------------

class TestExitGeometry:
    """`propose F BUY --limit 5 --stop-loss 50` and `--bracket 3 20` were stored
    PENDING; only "finite and positive" was checked."""

    def test_stop_above_a_buy_limit_is_refused(self):
        spec = ExecutionSpec(order_type='LIMIT', limit_price=5, exit_type='STOP_LOSS',
                             stop_loss_price=50)
        (error,) = spec.validate(action='BUY')
        assert 'at or above the limit entry' in error

    def test_inverted_buy_bracket_is_refused_on_both_legs(self):
        spec = ExecutionSpec(order_type='LIMIT', limit_price=5, exit_type='BRACKET',
                             take_profit_price=3, stop_loss_price=20)
        assert len(spec.validate(action='BUY')) == 2

    def test_short_bracket_is_mirrored(self):
        good = ExecutionSpec(order_type='LIMIT', limit_price=20, exit_type='BRACKET',
                             take_profit_price=10, stop_loss_price=25)
        bad = ExecutionSpec(order_type='LIMIT', limit_price=20, exit_type='BRACKET',
                            take_profit_price=25, stop_loss_price=10)
        assert good.validate(action='SELL') == []
        assert len(bad.validate(action='SELL')) == 2

    def test_correct_geometry_passes(self):
        spec = ExecutionSpec(order_type='LIMIT', limit_price=5, exit_type='BRACKET',
                             take_profit_price=20, stop_loss_price=3)
        assert spec.validate(action='BUY') == []

    def test_market_entry_without_a_quote_only_checks_bracket_order(self):
        stop_only = ExecutionSpec(order_type='MARKET', exit_type='STOP_LOSS', stop_loss_price=50)
        assert stop_only.validate(action='BUY') == []                  # unknown is not wrong
        assert stop_only.validate(action='BUY', reference_price=11.9)  # known and wrong
        inverted = ExecutionSpec(order_type='MARKET', exit_type='BRACKET',
                                 take_profit_price=3, stop_loss_price=20)
        assert any('inverted' in e for e in inverted.validate(action='BUY'))

    def test_without_an_action_nothing_new_is_checked(self):
        spec = ExecutionSpec(order_type='LIMIT', limit_price=5, exit_type='STOP_LOSS',
                             stop_loss_price=50)
        assert spec.validate() == []

    def test_no_exit_type_is_never_refused(self):
        assert exit_geometry_errors('SELL', 'LIMIT', 180, 'NONE', stop_loss_price=170) == []

    def test_trailing_percent_must_be_below_100(self):
        spec = ExecutionSpec(order_type='LIMIT', limit_price=5, exit_type='TRAILING_STOP',
                             trailing_stop_percent=150)
        assert any('below 100' in e for e in spec.validate(action='BUY'))


class TestProtectiveTrigger:
    """`mmr protect IOVA --stop 50` on a 7.72 stock was accepted: a SELL stop
    above the market executes at the next print."""

    def test_sell_stop_above_market_is_refused(self):
        assert 'trigger immediately' in protective_trigger_error('SELL', 'STP', 50, 7.72)

    def test_sell_stop_below_market_is_fine(self):
        assert protective_trigger_error('SELL', 'STP', 2, 7.72) is None

    def test_buy_stop_for_a_short_is_mirrored(self):
        assert protective_trigger_error('BUY', 'STP', 2, 7.72) is not None
        assert protective_trigger_error('BUY', 'STP', 9, 7.72) is None

    def test_take_profit_on_the_wrong_side_is_refused(self):
        assert protective_trigger_error('SELL', 'LMT', 2, 7.72) is not None
        assert protective_trigger_error('SELL', 'LMT', 12, 7.72) is None

    def test_trail_needs_no_market_price_but_must_be_below_100(self):
        assert protective_trigger_error('SELL', 'TRAIL', 30, None) is None
        assert protective_trigger_error('SELL', 'TRAIL', 130, None) is not None

    def test_unknown_market_price_is_a_refusal_not_a_pass(self):
        assert 'no current market price' in protective_trigger_error('SELL', 'STP', 2, None)

    @pytest.mark.parametrize('bad', [float('nan'), 0, -1, None])
    def test_malformed_trigger_is_refused(self, bad):
        assert protective_trigger_error('SELL', 'STP', bad, 7.72) is not None


# --------------------------------------------------------------------------
# Data: the incremental download
# --------------------------------------------------------------------------

def _bars(start: str, periods: int, tz: str, freq: str = 'min') -> pd.DataFrame:
    index = pd.date_range(start, periods=periods, freq=freq, tz=tz)
    return pd.DataFrame({'open': 1.0, 'high': 1.0, 'low': 1.0, 'close': 1.0, 'volume': 1.0,
                         'bar_size': '1 min' if freq == 'min' else '1 day'}, index=index)


NOW = dt.datetime(2026, 11, 14, 18, 0, tzinfo=UTC)          # Saturday
WINDOW = DateRange(dt.datetime(2026, 11, 12, tzinfo=UTC), NOW)


def _gaps(tickdata, conid, calendar, window=WINDOW):
    return [(str(r.start), str(r.end)) for r in tickdata.missing(conid, calendar, date_range=window)]


class TestMissingSessions:
    @pytest.fixture
    def minute(self, tmp_path):
        return TickStorage(str(tmp_path / 'h.duckdb')).get_tickdata(BarSize.Mins1)

    @pytest.fixture
    def daily(self, tmp_path):
        return TickStorage(str(tmp_path / 'd.duckdb')).get_tickdata(BarSize.Days1)

    def test_a_session_truncated_mid_day_is_still_missing(self, minute):
        """PLTR/CAT in the live store stopped at 11:12 ET on a restart day."""
        nys = exchange_calendars.get_calendar('XNYS')
        minute.write(1, _bars('2026-11-12 04:00', 960, 'America/New_York'), now=NOW)
        minute.write(1, _bars('2026-11-13 04:00', 432, 'America/New_York'), now=NOW)   # to 11:11
        assert _gaps(minute, 1, nys) == [('2026-11-13', '2026-11-13')]

    def test_a_newest_session_missing_its_evening_is_still_missing(self, minute):
        nys = exchange_calendars.get_calendar('XNYS')
        minute.write(1, _bars('2026-11-13 04:00', 750, 'America/New_York'), now=NOW)   # to 16:29
        window = DateRange(dt.datetime(2026, 11, 13, tzinfo=UTC), NOW)
        assert _gaps(minute, 1, nys, window) == [('2026-11-13', '2026-11-13')]

    def test_a_complete_extended_session_is_covered(self, minute):
        nys = exchange_calendars.get_calendar('XNYS')
        minute.write(1, _bars('2026-11-12 04:00', 960, 'America/New_York'), now=NOW)
        minute.write(1, _bars('2026-11-13 04:00', 960, 'America/New_York'), now=NOW)
        assert _gaps(minute, 1, nys) == []

    def test_first_hour_of_an_asx_session_is_not_coverage(self, minute):
        asx = exchange_calendars.get_calendar('XASX')
        minute.write(2, _bars('2026-11-12 10:00', 371, 'Australia/Sydney'), now=NOW)
        minute.write(2, _bars('2026-11-13 10:00', 61, 'Australia/Sydney'), now=NOW)
        assert _gaps(minute, 2, asx) == [('2026-11-13', '2026-11-13')]

    def test_asx_daily_bars_are_bucketed_by_sydney_date(self, daily):
        """Keyed at Sydney midnight they are the PREVIOUS day in New York; every
        session looked like its predecessor and the newest always looked missing."""
        asx = exchange_calendars.get_calendar('XASX')
        daily.write(3, _bars('2026-11-09', 5, 'Australia/Sydney', freq='D'),
                    exchange_calendar=asx, now=NOW)
        window = DateRange(dt.datetime(2026, 11, 9, tzinfo=UTC), NOW)
        assert _gaps(daily, 3, asx, window) == []

    def test_a_window_starting_mid_day_does_not_re_request_its_first_session(self, daily):
        nys = exchange_calendars.get_calendar('XNYS')
        daily.write(4, _bars('2026-11-09', 5, 'America/New_York', freq='D'),
                    exchange_calendar=nys, now=NOW)
        tz = dt.timezone(dt.timedelta(hours=-5))
        window = DateRange(dt.datetime(2026, 11, 9, 13, 0, tzinfo=tz), NOW)   # 13:00 ET Monday
        assert _gaps(daily, 4, nys, window) == []


class TestSessionFetchWindow:
    def test_a_one_session_gap_covers_that_whole_session(self):
        """The nightly refresh passed the date itself: midnight, the START of the
        last missing session, so the store ran one session behind every night."""
        gap = DateRange(dt.date(2026, 11, 13), dt.date(2026, 11, 13))
        start, end = session_fetch_window(gap, 'US/Eastern', now=NOW)
        assert (start.hour, start.minute) == (0, 0)
        assert (end.date(), end.hour, end.minute, end.second) == (dt.date(2026, 11, 13), 23, 59, 59)
        assert end - start > dt.timedelta(hours=23)

    def test_end_is_capped_at_now(self):
        friday_evening = dt.datetime(2026, 11, 14, 4, 0, tzinfo=UTC)          # 23:00 ET Friday
        gap = DateRange(dt.date(2026, 11, 13), dt.date(2026, 11, 13))
        _, end = session_fetch_window(gap, 'US/Eastern', now=friday_evening)
        assert end.astimezone(UTC) == friday_evening

    def test_window_is_in_the_venue_timezone(self):
        gap = DateRange(dt.date(2026, 11, 13), dt.date(2026, 11, 13))
        start, _ = session_fetch_window(gap, 'Australia/Sydney', now=NOW)
        assert start.utcoffset() == dt.timedelta(hours=11)

    def test_datetimes_pass_through(self):
        begin = dt.datetime(2026, 11, 1, 9, 30, tzinfo=UTC)
        start, end = session_fetch_window(DateRange(begin, NOW), 'US/Eastern', now=NOW)
        assert start == begin and end == NOW


class TestTwelveDataDailyEnd:
    def test_daily_end_date_is_sent_exclusive(self):
        """TwelveData's end_date excludes that day for daily bars (verified live:
        11-12..11-13 returned 11-12 only), so the day after must be sent."""
        from trader.listeners.twelvedata_history import TwelveDataHistoryWorker

        captured = {}

        class _Series:
            def as_pandas(self):
                return pd.DataFrame()

        worker = TwelveDataHistoryWorker('key')
        worker.client = SimpleNamespace(
            time_series=lambda **kwargs: captured.update(kwargs) or _Series())
        worker.get_history('AAPL', BarSize.Days1,
                           dt.datetime(2026, 11, 12), dt.datetime(2026, 11, 13, 23, 59, 59))
        assert captured['start_date'] == '2026-11-12'
        assert captured['end_date'] == '2026-11-14'


class TestVenueCalendars:
    @pytest.mark.parametrize('primary, expected', [
        ('ARCA', 'XNYS'), ('AMEX', 'XNYS'), ('NASDAQ', 'XNYS'), ('ISLAND', 'XNYS'),
        ('TSE', 'XTSE'), ('VENTURE', 'XTSE'), ('TSEJ', 'XTKS'), ('SEHK', 'XHKG'),
        ('ASX', 'XASX'), ('MEXI', 'XMEX'),
    ])
    def test_ib_venue_codes_resolve_a_calendar(self, primary, expected):
        """37 of 514 refresh-universe members (every ARCA ETF among them) had no
        calendar and re-downloaded 90 days of 1-min bars every night."""
        calendar = calendar_for(SimpleNamespace(primaryExchange=primary, exchange='SMART'))
        assert calendar is not None and calendar.name == expected

    def test_unknown_venue_has_no_calendar(self):
        assert calendar_for(SimpleNamespace(primaryExchange='NOSUCH', exchange='SMART')) is None

    def test_ib_tse_is_toronto_in_the_live_session_gate(self):
        assert calendar_name_for('SMART', 'TSE') == 'TSX'
        assert calendar_name_for('TSEJ', 'TSEJ') == 'XTKS'


class TestListingGuard:
    def _listing(self, primary, currency):
        return SimpleNamespace(symbol='CCL', conId=1, primaryExchange=primary,
                               exchange=primary, currency=currency)

    def test_us_listing_is_accepted_from_us_sources(self):
        assert is_us_listing(self._listing('NYSE', 'USD'))
        assert listing_source_mismatch(self._listing('NYSE', 'USD'), 'twelvedata') == ''
        assert listing_source_mismatch(self._listing('ARCA', 'USD'), 'massive') == ''

    @pytest.mark.parametrize('primary, currency', [('ASX', 'AUD'), ('MEXI', 'MXN'),
                                                   ('TSE', 'CAD'), ('LSEETF', 'USD')])
    def test_foreign_listing_is_refused_from_us_sources(self, primary, currency):
        reason = listing_source_mismatch(self._listing(primary, currency), 'twelvedata')
        assert 'US listing' in reason and primary in reason

    def test_ib_serves_the_listing_it_is_asked_for(self):
        assert listing_source_mismatch(self._listing('ASX', 'AUD'), 'ib') == ''


# --------------------------------------------------------------------------
# Risk gate and trading filter
# --------------------------------------------------------------------------

class TestRiskLimitValidation:
    @pytest.mark.parametrize('key, value', [
        ('max_daily_loss', float('nan')), ('max_daily_loss', 'nan'), ('max_leverage', float('inf')),
        ('max_leverage', -1), ('max_position_size_pct', 10), ('max_open_orders', 15.5),
        ('max_daily_loss', True), ('no_such_limit', 1),
    ])
    def test_nonsense_is_refused(self, key, value):
        """NaN converts cleanly and every comparison with it is False, so
        `max_daily_loss: .nan` switched the check off."""
        with pytest.raises(ValueError):
            coerce_limit(key, value)

    def test_valid_values_keep_their_type(self):
        assert coerce_limit('max_daily_loss', '750') == 750.0
        assert coerce_limit('max_open_orders', 20.0) == 20 and isinstance(
            coerce_limit('max_open_orders', 20.0), int)
        assert coerce_limit('max_position_size_pct', 0.15) == 0.15

    def test_yaml_nan_falls_back_to_the_default(self, tmp_path, caplog):
        config = tmp_path / 'trader.yaml'
        config.write_text('risk_limits:\n  max_daily_loss: .nan\n  max_leverage: 2.0\n')
        with caplog.at_level(logging.ERROR):
            limits = RiskLimits.load(str(config))
        assert limits.max_daily_loss == RiskLimits().max_daily_loss
        assert limits.max_leverage == 2.0
        assert any('max_daily_loss' in r.getMessage() for r in caplog.records)

    def test_unset_margin_fields_are_unevaluable_not_a_crash(self):
        gate = RiskGate(event_store=None, limits=RiskLimits())
        result = gate.check_leverage({'initMarginAfter': None, 'equityWithLoanAfter': None}, 50_000.0)
        assert not result.approved and result.checks['leverage'] == 'unevaluable:margin-data'
        sentinel = gate.check_leverage(
            {'initMarginAfter': 1000.0, 'equityWithLoanAfter': 1.7976931348623157e308}, 50_000.0)
        assert not sentinel.approved and sentinel.checks['margin_cushion'] == 'unevaluable:equity-data'


class TestTradingFilter:
    def test_a_denied_listing_venue_is_denied_when_routed_smart(self):
        """`deny_exchanges: [ASX]` blocked nothing: orders carry exchange=SMART."""
        f = TradingFilter(deny_exchanges=['ASX'])
        assert f.is_allowed('BHP', 'SMART', 'STK', primary_exchange='ASX')[0] is False
        assert f.is_allowed('AAPL', 'SMART', 'STK', primary_exchange='NASDAQ')[0] is True

    def test_an_exchange_allowlist_is_judged_on_the_listing_venue(self):
        f = TradingFilter(exchanges=['NASDAQ', 'NYSE'])
        assert f.is_allowed('AAPL', 'SMART', 'STK', primary_exchange='NASDAQ')[0] is True
        assert f.is_allowed('BHP', 'SMART', 'STK', primary_exchange='ASX')[0] is False

    def test_scalar_or_numeric_list_entries_are_refused_at_load(self, tmp_path):
        path = tmp_path / 'filters.yaml'
        path.write_text('denylist: AAPL\n')
        with pytest.raises(TradingFilterError):
            TradingFilter.load(str(path))
        path.write_text('denylist: [700]\n')
        with pytest.raises(TradingFilterError):
            TradingFilter.load(str(path))

    def test_a_running_service_sees_a_filter_change(self, tmp_path):
        """trader_service loaded the filter once at connect; `mmr filter deny X`
        did not reach the order path until a restart."""
        import os
        path = tmp_path / 'filters.yaml'
        path.write_text('denylist: []\n')
        live = ReloadingTradingFilter(str(path))
        assert live.is_allowed('NKLA', 'SMART', 'STK')[0] is True
        path.write_text('denylist: [NKLA]\n')
        os.utime(path, ns=(1, path.stat().st_mtime_ns + 5_000_000_000))
        assert live.is_allowed('NKLA', 'SMART', 'STK')[0] is False

    def test_a_broken_edit_keeps_the_last_good_filter(self, tmp_path, caplog):
        import os
        path = tmp_path / 'filters.yaml'
        path.write_text('denylist: [NKLA]\n')
        live = ReloadingTradingFilter(str(path))
        path.write_text('denylist: NKLA\n')                      # scalar: does not parse
        os.utime(path, ns=(1, path.stat().st_mtime_ns + 5_000_000_000))
        with caplog.at_level(logging.ERROR):
            assert live.is_allowed('NKLA', 'SMART', 'STK')[0] is False
        assert any('does not parse' in r.getMessage() for r in caplog.records)


# --------------------------------------------------------------------------
# Trader runtime
# --------------------------------------------------------------------------

class TestStaleSubmittedTrades:
    def _trader(self, session_trades):
        from trader.trading.trading_runtime import Trader
        trader = object.__new__(Trader)
        trader.client = SimpleNamespace(ib=SimpleNamespace(trades=lambda: session_trades))
        return trader

    def _trade(self, order_id, status='PreSubmitted'):
        return SimpleNamespace(order=SimpleNamespace(orderId=order_id, clientId=5),
                               orderStatus=SimpleNamespace(status=status))

    def test_a_trade_the_session_no_longer_tracks_is_dropped(self):
        """After a reconnect the object is frozen at PreSubmitted forever and kept
        reserving the shares: the close was refused and the stop already cancelled."""
        frozen, live = self._trade(41), self._trade(42)
        trader = self._trader([live])
        trader._submitted_trades = [frozen, live]
        assert trader._live_submitted_trades() == [live]
        assert trader._submitted_trades == [live]

    def test_terminal_trades_are_dropped(self):
        done = self._trade(7, status='Cancelled')
        trader = self._trader([done])
        trader._submitted_trades = [done]
        assert trader._live_submitted_trades() == []

    def test_an_unreadable_session_keeps_the_entries(self):
        from trader.trading.trading_runtime import Trader
        trader = object.__new__(Trader)

        def unreadable():
            raise RuntimeError('not connected')

        trader.client = SimpleNamespace(ib=SimpleNamespace(trades=unreadable))
        pending = self._trade(9)
        trader._submitted_trades = [pending]
        assert trader._live_submitted_trades() == [pending]


def test_deferred_and_unknown_refusals_are_retryable():
    from trader.trading.trading_runtime import _is_transient_refusal
    assert _is_transient_refusal('DEFERRED: SELL 100 not placed: reserved by working reductions')
    assert _is_transient_refusal('UNKNOWN: working orders unreadable')
    assert not _is_transient_refusal('post-trade leverage 2.31x exceeds limit 2.00x')
    assert not _is_transient_refusal(None)


# --------------------------------------------------------------------------
# RPC replies, logging, configuration
# --------------------------------------------------------------------------

class TestSuccessFailOverRpc:
    def _round_trip(self, value):
        from trader.messaging import clientserver as cs
        blob = msgpack.packb(value, default=cs.ext_pack, use_bin_type=True)
        return msgpack.unpackb(blob, ext_hook=cs.ext_unpack, raw=False)

    def test_a_non_allowlisted_exception_no_longer_poisons_the_reply(self):
        """The client could not decode the reply and reported a TIMEOUT —
        'outcome UNKNOWN' — for a failure the server had stated plainly."""
        import sqlite3
        reply = self._round_trip(SuccessFail.fail(
            error='journal busy', exception=sqlite3.OperationalError('database is locked')))
        assert not reply.is_success() and reply.error == 'journal busy'
        assert isinstance(reply.exception, RuntimeError)
        assert 'OperationalError: database is locked' in str(reply.exception)

    def test_builtin_exceptions_survive_unchanged(self):
        reply = self._round_trip(SuccessFail.fail(error='x', exception=ValueError('plain')))
        assert isinstance(reply.exception, ValueError)

    def test_not_submitted_is_explicit_and_survives_the_wire(self):
        assert SuccessFail.fail(error='x').not_submitted is False
        assert self._round_trip(SuccessFail.never_sent(error='x')).not_submitted is True


def test_log_method_never_logs_a_secret_argument(caplog):
    from trader.common.logging_helper import log_method

    @log_method
    def place(contract, approver_key='', quantity=0):
        return quantity

    with caplog.at_level(logging.DEBUG):
        place('C', approver_key='s3cr3t-approver-key', quantity=3)
    text = ' '.join(r.getMessage() for r in caplog.records)
    assert 's3cr3t' not in text and 'approver_key = <redacted>' in text and 'quantity = 3' in text


class TestTradingMode:
    def _load(self, tmp_path, monkeypatch, mode):
        from trader.config import MMRConfig
        for name in ('IB_ACCOUNT', 'IB_SERVER_PORT', 'TRADING_MODE'):
            monkeypatch.delenv(name, raising=False)
        path = tmp_path / 'trader.yaml'
        path.write_text(
            f'trading_mode: "{mode}"\nib_paper_account: DU111\nib_live_account: U999\n'
            f'ib_paper_port: 4004\nib_live_port: 4003\n')
        return MMRConfig.from_yaml(str(path))

    @pytest.mark.parametrize('mode', ['Paper', 'paper ', 'PAPER'])
    def test_any_spelling_of_paper_is_paper(self, tmp_path, monkeypatch, mode):
        """These all selected the LIVE account and port: the test was == 'paper'
        with live as the else branch."""
        config = self._load(tmp_path, monkeypatch, mode)
        assert config.paper_trading is True and config.ib.account == 'DU111'

    @pytest.mark.parametrize('mode', ['', 'sandbox', 'papr'])
    def test_anything_else_refuses(self, tmp_path, monkeypatch, mode):
        with pytest.raises(ValueError, match="'paper' or 'live'"):
            self._load(tmp_path, monkeypatch, mode)


def test_nan_confidence_does_not_size_as_full_confidence():
    from trader.trading.position_sizing import PortfolioState, PositionSizer, PositionSizingConfig
    sizer = PositionSizer(PositionSizingConfig())
    state = PortfolioState(net_liquidation=100_000.0, gross_position_value=0.0,
                           available_funds=100_000.0, position_count=0)
    unknown = sizer.compute(confidence=float('nan'), portfolio_state=state)
    none = sizer.compute(confidence=0.0, portfolio_state=state)
    full = sizer.compute(confidence=1.0, portfolio_state=state)
    assert math.isclose(unknown.amount_usd, none.amount_usd)
    assert unknown.amount_usd < full.amount_usd


# --------------------------------------------------------------------------
# Manual exits must not depend on market data
# --------------------------------------------------------------------------

class TestCloseWithoutMarketData:
    """`mmr close` / `sell --quantity` fetched two live snapshots before sending
    anything (one of them unused). With data dark — a competing session (10197),
    a delayed-only venue — the exit was never sent."""

    @pytest.mark.asyncio
    async def test_quantity_specified_exit_is_sent_when_snapshots_fail(self):
        from unittest.mock import AsyncMock
        from review.test_review_order_contract import _stock, _trader
        from trader.messaging.trader_service_api import TraderServiceApi

        trader = _trader(held=100, gate_enabled=True)
        trader.client.get_snapshot = AsyncMock(side_effect=Exception(
            'reqId: 77, errorCode: 10197, errorString: No market data during competing live session'))
        try:
            result = await TraderServiceApi(trader).place_order_simple(
                _stock(), 'SELL', equity_amount=None, quantity=100, limit_price=None,
                market_order=True)
            assert result.is_success(), result.error
            assert len(trader.placed) == 1
        finally:
            trader.order_tracker.close(timeout=1)

    @pytest.mark.asyncio
    async def test_the_structural_check_fetches_no_snapshot(self):
        from review.test_review_order_contract import _stock, _trader
        from trader.messaging.trader_service_api import TraderServiceApi

        trader = _trader(held=100, gate_enabled=True)
        try:
            result = await TraderServiceApi(trader).place_order_simple(
                _stock(), 'SELL', equity_amount=None, quantity=100, limit_price=None,
                market_order=True)
            assert result.is_success(), result.error
            assert trader.client.get_snapshot.await_count == 1
        finally:
            trader.order_tracker.close(timeout=1)

    @pytest.mark.asyncio
    async def test_sizing_from_an_amount_still_needs_a_quote(self):
        from unittest.mock import AsyncMock
        from review.test_review_order_contract import _stock, _trader
        from trader.messaging.trader_service_api import TraderServiceApi

        trader = _trader(held=100, gate_enabled=True)
        trader.client.get_snapshot = AsyncMock(side_effect=Exception('no market data'))
        try:
            result = await TraderServiceApi(trader).place_order_simple(
                _stock(), 'SELL', equity_amount=500.0, quantity=None, limit_price=None,
                market_order=True)
            assert not result.is_success()
            assert trader.placed == []
        finally:
            trader.order_tracker.close(timeout=1)


# --------------------------------------------------------------------------
# IB 1101 ("restored, data LOST") is not 1102 ("restored, data maintained")
# --------------------------------------------------------------------------

class TestUpstreamDataLost:
    def _trader(self):
        from trader.trading.trading_runtime import Trader
        trader = object.__new__(Trader)
        trader.reconnects = 0
        trader._invalidate_account_pnl = lambda: None
        trader._ensure_account_pnl_subscription = lambda: None
        trader.reconnect = lambda: setattr(trader, 'reconnects', trader.reconnects + 1)
        trader._ib_upstream_connected = False
        trader._ib_upstream_error = 'lost'
        return trader

    def test_1101_forces_a_resubscribing_reconnect(self):
        """After 1101 every subscription must be resubmitted and the socket never
        dropped, so nothing re-requested them: green flags over frozen data."""
        trader = self._trader()
        trader._on_ib_error(SimpleNamespace(errorCode=1101, errorString='restored - data lost'))
        assert trader._ib_upstream_connected is True
        assert trader.reconnects == 1

    def test_1102_does_not_bounce_the_socket(self):
        trader = self._trader()
        trader._on_ib_error(SimpleNamespace(errorCode=1102, errorString='restored - data maintained'))
        assert trader._ib_upstream_connected is True
        assert trader.reconnects == 0

    def test_a_reconnect_already_in_progress_is_not_doubled(self):
        trader = self._trader()
        trader._reconnecting = True
        trader._on_ib_error(SimpleNamespace(errorCode=1101, errorString='restored - data lost'))
        assert trader.reconnects == 0


# --------------------------------------------------------------------------
# approve() must be able to resolve the conId propose() pinned
# --------------------------------------------------------------------------

class TestPinnedConIdResolution:
    def _definition(self, con_id):
        from trader.data.data_access import SecurityDefinition
        return SecurityDefinition(
            symbol='ZETA', exchange='NYSE', conId=con_id, secType='STK', primaryExchange='NYSE',
            currency='USD', tradingClass='', includeExpired=False, secIdType='', secId='',
            description='', minTick=0.01, orderTypes='', validExchanges='SMART,NYSE',
            priceMagnifier=1, longName='ZETA', category='', subcategory='', tradingHours='',
            timeZoneId='US/Eastern', liquidHours='', stockType='', minSize=1.0,
            sizeIncrement=1.0, suggestedSizeIncrement=1.0, bondType='', couponType='',
            callable=False, putable=False, coupon=0.0, convertable=False, maturity='',
            issueDate='', nextOptionDate='', nextOptionPartial=False, nextOptionType='',
            marketRuleIds='', company_name='')

    def _mmr(self, ib_answer):
        from test_sdk import _make_mmr_with_mock, _make_mock_rpc
        client = _make_mock_rpc()
        client.rpc.return_value.resolve_symbol.return_value = []        # not in a local universe
        client.rpc.return_value.resolve_contract.return_value = ib_answer
        return _make_mmr_with_mock(client), client

    def test_a_conid_outside_the_universes_is_looked_up_exactly_at_ib(self):
        """`propose` pins a conId found by IB discovery; `approve` could only
        resolve conIds from the local universe, so it FAILED for any other symbol."""
        mmr, client = self._mmr([self._definition(555001)])
        contract = mmr._resolve_contract(555001)
        assert contract.conId == 555001
        sent = client.rpc.return_value.resolve_contract.call_args.args[0]
        assert sent.conId == 555001 and not sent.symbol            # an exact-conId question

    def test_a_different_conid_from_ib_is_never_accepted(self):
        mmr, _ = self._mmr([self._definition(999999)])
        with pytest.raises(ValueError):
            mmr._resolve_contract(555001)

    def test_resolve_itself_still_never_asks_ib_about_an_integer(self):
        mmr, client = self._mmr([self._definition(555001)])
        assert mmr.resolve(555001) == []
        client.rpc.return_value.resolve_contract.assert_not_called()


# --------------------------------------------------------------------------
# Order ids are never reused across gateway restarts
# --------------------------------------------------------------------------

class TestOrderIdFloor:
    """A recreated gateway restarted its id counter: 1946 and 1950-1952 were
    issued twice in one day, and journals key physical orders by (client, id)."""

    def _trader(self, top, next_id=1946):
        from trader.trading.trading_runtime import Trader
        trader = object.__new__(Trader)
        state = {'seq': next_id}

        def update(minimum):
            state['seq'] = max(state['seq'], minimum)

        def get():
            state['seq'] += 1
            return state['seq'] - 1

        ib = SimpleNamespace(client=SimpleNamespace(updateReqId=update, getReqId=get))
        trader.client = SimpleNamespace(ib=ib)
        trader.trading_runtime_ib_client_id = 5
        trader.reads = 0

        def journal():
            trader.reads += 1
            if isinstance(top, Exception):
                raise top
            return SimpleNamespace(max_order_id=lambda client_id: top)

        trader.server_order_journal = journal
        return trader, ib

    @pytest.mark.asyncio
    async def test_new_ids_start_above_every_id_ever_reserved(self):
        trader, ib = self._trader(top=1959)
        await trader._floor_order_ids()
        assert ib.client.getReqId() == 1960

    @pytest.mark.asyncio
    async def test_a_higher_gateway_counter_is_left_alone(self):
        trader, ib = self._trader(top=100, next_id=5000)
        await trader._floor_order_ids()
        assert ib.client.getReqId() == 5000

    @pytest.mark.asyncio
    async def test_the_journal_is_read_once_per_connection(self):
        trader, _ = self._trader(top=1959)
        await trader._floor_order_ids()
        await trader._floor_order_ids()
        assert trader.reads == 1
        trader.client = SimpleNamespace(ib=SimpleNamespace(
            client=SimpleNamespace(updateReqId=lambda minimum: None, getReqId=lambda: 1)))
        await trader._floor_order_ids()                       # a new IB connection
        assert trader.reads == 2

    @pytest.mark.asyncio
    async def test_an_unreadable_journal_does_not_block_allocation(self):
        trader, ib = self._trader(top=RuntimeError('database is locked'))
        await trader._floor_order_ids()
        assert ib.client.getReqId() == 1946

    def test_max_order_id_reads_the_reservations(self, tmp_path):
        from trader.data.server_order_journal import ServerOrderJournal
        journal = ServerOrderJournal(str(tmp_path / 'mmr.duckdb'))
        assert journal.max_order_id(5) == 0
        journal.claim('intent-a', 'fp', 'DU1')
        journal.reserve_order('intent-a', 1946, 5)
        journal.reserve_order('intent-a', 1952, 5)
        journal.reserve_order('intent-a', 9000, 7)            # another client
        assert journal.max_order_id(5) == 1952


# --------------------------------------------------------------------------
# Fixed-key ib_async requests: one at a time, never unbounded
# --------------------------------------------------------------------------

class TestSingleFlight:
    """ib_async keeps one future per fixed key ('openOrders', 'positions'); a
    second request replaces it and the first awaiter is never resolved."""

    @pytest.mark.asyncio
    async def test_concurrent_requests_for_one_key_do_not_overlap(self):
        import asyncio
        from trader.listeners.ibreactive import single_flight
        ib = object()
        in_flight, peak = 0, 0

        async def request():
            nonlocal in_flight, peak
            in_flight += 1
            peak = max(peak, in_flight)
            await asyncio.sleep(0.02)
            in_flight -= 1
            return 'orders'

        results = await asyncio.gather(*(single_flight(ib, 'openOrders', request) for _ in range(4)))
        assert results == ['orders'] * 4 and peak == 1

    @pytest.mark.asyncio
    async def test_different_keys_and_connections_do_not_wait_on_each_other(self):
        import asyncio
        from trader.listeners.ibreactive import single_flight
        started = []

        async def request(tag):
            started.append(tag)
            await asyncio.sleep(0.02)

        first, second = object(), object()
        await asyncio.gather(
            single_flight(first, 'openOrders', lambda: request('a')),
            single_flight(first, 'positions', lambda: request('b')),
            single_flight(second, 'openOrders', lambda: request('c')))
        assert sorted(started) == ['a', 'b', 'c']

    @pytest.mark.asyncio
    async def test_a_request_that_never_answers_times_out_and_frees_the_key(self):
        import asyncio
        from trader.listeners.ibreactive import single_flight
        ib = object()

        async def never():
            await asyncio.Event().wait()

        async def answer():
            return 1

        with pytest.raises(asyncio.TimeoutError):
            await single_flight(ib, 'positions', never, timeout=0.05)
        assert await single_flight(ib, 'positions', answer, timeout=0.5) == 1


# --------------------------------------------------------------------------
# A flat account must read as flat once positions have synced
# --------------------------------------------------------------------------

class TestFlatAccountAfterReconnect:
    def _trader(self, live, cached, connected=True):
        from trader.trading.trading_runtime import Trader
        trader = object.__new__(Trader)
        trader.ib_account = 'DU1'
        ib = SimpleNamespace(positions=lambda account=None: list(live),
                             isConnected=lambda: connected)
        trader.client = SimpleNamespace(ib=ib)
        trader.portfolio = SimpleNamespace(get_positions=lambda: list(cached))
        return trader, ib

    def _position(self, quantity):
        return SimpleNamespace(account='DU1', contract=SimpleNamespace(conId=1), position=quantity)

    def test_empty_after_a_sync_is_flat_not_the_stale_cache(self):
        """The cache is never cleared: a position closed while disconnected kept
        reading as held, and a SELL against it went out exit-class (ungated)."""
        trader, ib = self._trader(live=[], cached=[self._position(100)])
        trader._positions_synced_ib = ib
        assert trader.get_positions() == []

    def test_before_the_first_sync_the_cache_still_bridges(self):
        trader, _ = self._trader(live=[], cached=[self._position(100)])
        assert [p.position for p in trader.get_positions()] == [100]

    def test_a_sync_on_an_older_connection_does_not_count(self):
        trader, _ = self._trader(live=[], cached=[self._position(100)])
        trader._positions_synced_ib = object()                 # the previous IB instance
        assert [p.position for p in trader.get_positions()] == [100]

    def test_disconnected_falls_back_to_the_cache(self):
        trader, ib = self._trader(live=[], cached=[self._position(100)], connected=False)
        trader._positions_synced_ib = ib
        assert [p.position for p in trader.get_positions()] == [100]

    def test_live_positions_always_win(self):
        trader, ib = self._trader(live=[self._position(7)], cached=[self._position(100)])
        trader._positions_synced_ib = ib
        assert [p.position for p in trader.get_positions()] == [7]


# --------------------------------------------------------------------------
# A short-opening LIMIT order is valued at the market, not at its own limit
# --------------------------------------------------------------------------

class TestShortOpenValuation:
    """`SELL 400 --limit 1` on a $50 stock was valued at $400 for the
    concentration check and filled as a $20,000 short: a sell limit fills at
    or ABOVE its price, so the client's limit is a floor, not a size."""

    async def _place(self, limit, quote, portfolio_value=100_000):
        from review.test_review_order_contract import _stock, _trader
        trader = _trader(held=0, gate_enabled=True, portfolio_value=portfolio_value)
        ticker = SimpleNamespace(contract=_stock(), last=quote, close=quote, ask=quote, bid=quote)

        async def snapshot(contract, delayed=False):
            if quote is None:
                raise RuntimeError('no market data')
            return ticker

        trader.client.get_snapshot = snapshot
        try:
            return await trader.place_expressive_order(
                _stock(), 'SELL', 400, {'order_type': 'LIMIT', 'limit_price': limit}), trader
        finally:
            trader.order_tracker.close(timeout=1)

    @pytest.mark.asyncio
    async def test_a_lowball_limit_does_not_shrink_the_position(self):
        # 400 x $50 = $20,000 = 20% of a $100k account: over the 10% cap.
        result, trader = await self._place(limit=1.0, quote=50.0)
        assert not result.is_success()
        assert 'position' in str(result.error).lower()
        assert trader.placed == []

    @pytest.mark.asyncio
    async def test_a_genuinely_small_short_still_passes(self):
        # 400 x $10 = $4,000 = 4%.
        result, trader = await self._place(limit=9.5, quote=10.0)
        assert result.is_success(), result.error

    @pytest.mark.asyncio
    async def test_without_a_quote_a_short_open_is_not_evaluable(self):
        result, trader = await self._place(limit=1.0, quote=None)
        assert not result.is_success()
        assert trader.placed == []


# --------------------------------------------------------------------------
# A boolean env var is parsed strictly
# --------------------------------------------------------------------------

class TestBooleanEnvCoercion:
    def _coerce(self, raw):
        import inspect
        from trader.container import Container
        parameter = inspect.Parameter('require_proposal_approval',
                                      inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=bool)
        return Container._coerce_env_value(object.__new__(Container), raw, parameter)

    @pytest.mark.parametrize('raw', ['True ', 'true\r', ' YES', 'on', '1'])
    def test_whitespace_and_case_do_not_turn_true_into_false(self, raw):
        """An env var overrides the YAML: REQUIRE_PROPOSAL_APPROVAL='True\\r'
        used to resolve to False and switch the approval gate off."""
        assert self._coerce(raw) is True

    @pytest.mark.parametrize('raw', ['false', 'No ', '0', 'OFF'])
    def test_explicit_false(self, raw):
        assert self._coerce(raw) is False

    @pytest.mark.parametrize('raw', ['enabled', 'y', 'tru', ''])
    def test_anything_else_refuses(self, raw):
        from trader.container import ContainerResolutionError
        with pytest.raises(ContainerResolutionError):
            self._coerce(raw)


@pytest.mark.asyncio
async def test_direct_path_refuses_a_pure_open_when_opening_is_not_allowed():
    """allow_open=False was only enforced inside the flip branch: an order the
    RPC layer classified as (partly) a reduction, whose position had gone by the
    time the server re-read it, went through as an unapproved open."""
    from review.test_review_order_contract import _stock, _trader
    from trader.objects import Action

    trader = _trader(held=0, gate_enabled=True)           # nothing held any more
    try:
        observable = await trader._place_order_simple(
            _stock(), Action.SELL, equity_amount=None, quantity=50, limit_price=None,
            market_order=True, stop_loss_percentage=0.0, allow_open=False)
        errors = []
        observable.subscribe(on_next=lambda _: None, on_error=errors.append)
        assert len(errors) == 1 and 'approved proposal' in str(errors[0])
        assert trader.placed == []
    finally:
        trader.order_tracker.close(timeout=1)
