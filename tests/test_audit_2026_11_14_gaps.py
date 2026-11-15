"""Regressions for 2026-11-14 fixes that had none.

Found by going back over the audit's fixed list after the mutation pass showed
one fix (the pending-submission leak) was protected by no test at all. Each
class names the audit item it pins.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from trader import mmr_cli as cli
from trader.trading.trading_runtime import Trader


# --------------------------------------------------------------------------
# A3.3 — the whatIf margin request is bounded
# --------------------------------------------------------------------------

class TestMarginCheckIsBounded:
    """`whatIfOrderAsync` resolves only on an order state with margins or a
    non-warning error. A warning-class reply resolved neither, and the await
    ran under the account-wide order lock: one such open wedged every later
    placement — exits and protective stops included."""

    @staticmethod
    def _runtime(what_if, timeout=0.05):
        return SimpleNamespace(client=SimpleNamespace(ib=SimpleNamespace(whatIfOrderAsync=what_if)),
                               WHATIF_TIMEOUT_S=timeout)

    @staticmethod
    def _state(**fields):
        numbers = dict(initMarginBefore=1.0, maintMarginBefore=2.0, equityWithLoanBefore=3.0,
                       initMarginChange=4.0, maintMarginChange=5.0, equityWithLoanChange=6.0,
                       initMarginAfter=7.0, maintMarginAfter=8.0, equityWithLoanAfter=9.0, commission=0.5)
        numbers.update(fields)
        return SimpleNamespace(numeric=lambda digits: SimpleNamespace(**numbers), warningText='careful')

    def test_the_production_bound_is_ten_seconds(self):
        assert Trader.WHATIF_TIMEOUT_S == 10.0

    @pytest.mark.asyncio
    async def test_a_reply_that_never_comes_raises_instead_of_hanging(self):
        started, cancelled = asyncio.Event(), asyncio.Event()

        async def never(contract, order):
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

        runtime = self._runtime(never)
        with pytest.raises(TimeoutError, match='no margin reply within 0.05s'):
            await asyncio.wait_for(Trader.check_order_margin(runtime, object(), object()), timeout=5)
        assert started.is_set() and cancelled.is_set(), 'the abandoned request is cancelled, not leaked'

    @pytest.mark.asyncio
    async def test_a_prompt_reply_is_returned_with_every_margin_field(self):
        async def reply(contract, order):
            return self._state()

        margin = await Trader.check_order_margin(self._runtime(reply), object(), object())
        assert margin == {
            'initMarginBefore': 1.0, 'maintMarginBefore': 2.0, 'equityWithLoanBefore': 3.0,
            'initMarginChange': 4.0, 'maintMarginChange': 5.0, 'equityWithLoanChange': 6.0,
            'initMarginAfter': 7.0, 'maintMarginAfter': 8.0, 'equityWithLoanAfter': 9.0,
            'commission': 0.5, 'warningText': 'careful'}

    @pytest.mark.asyncio
    async def test_a_list_reply_is_its_first_state_and_an_empty_one_is_a_failure(self):
        async def listed(contract, order):
            return [self._state(initMarginAfter=42.0), self._state(initMarginAfter=99.0)]

        async def empty(contract, order):
            return []

        margin = await Trader.check_order_margin(self._runtime(listed), object(), object())
        assert margin['initMarginAfter'] == 42.0
        with pytest.raises(ValueError, match='no order state'):
            await Trader.check_order_margin(self._runtime(empty), object(), object())


# --------------------------------------------------------------------------
# A4.3 — an ad-hoc download resolves to exactly one listing in the source's market
# --------------------------------------------------------------------------

def _listing(con_id, symbol, primary, currency, exchange=None):
    return SimpleNamespace(conId=con_id, symbol=symbol, primaryExchange=primary,
                           exchange=exchange or primary, currency=currency, secType='STK')


class _Accessor:
    def __init__(self, local=(), downloads=None):
        self.local = list(local)
        self.downloads = downloads
        self.updated = []

    def resolve_symbol(self, symbol):
        return list(self.local)

    def get(self, name):
        return self.downloads

    def update(self, universe):
        self.updated.append(universe)


class _Discovery:
    def __init__(self, listings=(), error=None):
        self.listings, self.error, self.calls = list(listings), error, []

    def discover_listings(self, symbol, exchange='', currency=''):
        self.calls.append((symbol, exchange, currency))
        if self.error:
            raise self.error
        return list(self.listings)


US_ORCL = _listing(1447100, 'ORCL', 'NYSE', 'USD', exchange='SMART')
MX_ORCL = _listing(38708990, 'ORCL', 'MEXI', 'MXN')
ASX_CCL = _listing(4036555, 'CCL', 'ASX', 'AUD')
US_CCL = _listing(878372298, 'CCL', 'NYSE', 'USD', exchange='SMART')


class TestDownloadListingResolution:
    """`data download ORCL` took the first local match across every universe,
    then the first row IB returned: 38 of 514 nightly instruments were foreign
    listings receiving the US ticker's bars."""

    def test_one_local_us_listing_is_used_without_asking_ib(self):
        discovery = _Discovery([MX_ORCL])
        found, refusal, registered = cli._resolve_download_listing(
            'ORCL', 'twelvedata', _Accessor([US_ORCL]), discovery)
        assert found is US_ORCL and refusal == '' and registered is False
        assert discovery.calls == []

    def test_a_foreign_local_listing_is_not_a_match_for_a_us_vendor(self):
        """The Mexican row sat in a universe; the vendor serves the US ticker."""
        discovery = _Discovery([US_ORCL])
        accessor = _Accessor([MX_ORCL])
        found, refusal, registered = cli._resolve_download_listing('ORCL', 'twelvedata', accessor, discovery)
        assert found is US_ORCL and refusal == '' and registered is True
        assert discovery.calls == [('ORCL', '', 'USD')]
        assert [sd.conId for sd in accessor.updated[0].security_definitions] == [US_ORCL.conId]

    def test_the_same_conid_in_two_universes_is_one_listing(self):
        found, refusal, _ = cli._resolve_download_listing(
            'ORCL', 'massive', _Accessor([US_ORCL, US_ORCL]), _Discovery())
        assert found is US_ORCL and refusal == ''

    def test_two_us_listings_are_an_ambiguity_never_a_pick(self):
        other = _listing(999001, 'ORCL', 'NASDAQ', 'USD')
        found, refusal, registered = cli._resolve_download_listing(
            'ORCL', 'twelvedata', _Accessor([US_ORCL, other]), _Discovery())
        assert found is None and registered is False
        assert 'ambiguous in the local universes' in refusal
        assert '1447100 (NYSE/USD)' in refusal and '999001 (NASDAQ/USD)' in refusal and '--exchange' in refusal

    def test_ib_returning_two_listings_is_refused_naming_both(self):
        accessor = _Accessor()
        found, refusal, registered = cli._resolve_download_listing(
            'CCL', 'ib', accessor, _Discovery([ASX_CCL, US_CCL]))
        assert found is None and registered is False and accessor.updated == []
        assert 'ambiguous at IB' in refusal and '4036555 (ASX/AUD)' in refusal and '878372298 (NYSE/USD)' in refusal

    def test_an_exchange_states_the_listing_wanted(self):
        discovery = _Discovery([ASX_CCL, US_CCL])
        found, refusal, _ = cli._resolve_download_listing(
            'CCL', 'ib', _Accessor(), discovery, exchange='ASX', currency='AUD')
        assert found is ASX_CCL and refusal == ''
        assert discovery.calls == [('CCL', 'ASX', 'AUD')]

    def test_an_explicit_exchange_lifts_the_us_only_rule_for_the_match(self):
        """The listing guard, not this resolver, then refuses the vendor fill."""
        found, refusal, _ = cli._resolve_download_listing(
            'ORCL', 'twelvedata', _Accessor([MX_ORCL, US_ORCL]), _Discovery(), exchange='MEXI')
        assert found is MX_ORCL and refusal == ''

    def test_nothing_local_and_no_service_says_how_to_add_it(self):
        found, refusal, registered = cli._resolve_download_listing('ORCL', 'twelvedata', _Accessor(), None)
        assert found is None and registered is False
        assert 'trader_service is unreachable' in refusal and 'mmr universe add downloads ORCL' in refusal

    def test_no_us_listing_at_ib_is_a_refusal_not_the_first_foreign_row(self):
        accessor = _Accessor()
        found, refusal, _ = cli._resolve_download_listing('SQ', 'twelvedata', accessor, _Discovery([MX_ORCL]))
        assert found is None and 'a US listing' in refusal and accessor.updated == []

    def test_a_discovery_failure_is_reported_not_raised(self):
        found, refusal, _ = cli._resolve_download_listing(
            'ORCL', 'twelvedata', _Accessor(), _Discovery(error=ConnectionError('RPC timed out')))
        assert found is None and 'RPC timed out' in refusal

    def test_a_discovered_listing_joins_an_existing_downloads_universe_once(self):
        existing = SimpleNamespace(name='downloads', security_definitions=[US_CCL])
        accessor = _Accessor(downloads=existing)
        for _ in range(2):
            found, _refusal, registered = cli._resolve_download_listing(
                'ORCL', 'twelvedata', accessor, _Discovery([US_ORCL]))
            assert found is US_ORCL and registered is True
        assert [sd.conId for sd in existing.security_definitions] == [US_CCL.conId, US_ORCL.conId]

    def test_a_listing_without_a_conid_is_not_a_candidate(self):
        blank = _listing(0, 'ORCL', 'NYSE', 'USD')
        found, refusal, _ = cli._resolve_download_listing('ORCL', 'twelvedata', _Accessor([blank]), None)
        assert found is None and 'could not resolve' in refusal


# --------------------------------------------------------------------------
# A4.9 — the exchange timezone reaches the IB history request on strategy priming
# --------------------------------------------------------------------------

class TestPrimingCarriesTheExchangeTimezone:
    """The contract handed to the IB history worker is a plain Contract with
    no `timeZoneId`, so the worker fell back to US/Eastern and keyed a non-US
    DAILY bar at New York midnight: every ASX session existed under two keys."""

    @staticmethod
    def _security(zone):
        from dataclasses import replace

        from test_data_status_freshness import _definition
        return replace(_definition(4036812, 'BHP', primary='ASX'), timeZoneId=zone, currency='AUD')

    @staticmethod
    def _runtime(missing_ranges):
        from unittest.mock import AsyncMock

        import pandas as pd

        from trader.strategy.strategy_runtime import StrategyRuntime

        runtime = StrategyRuntime.__new__(StrategyRuntime)
        tick_data = SimpleNamespace(missing=lambda security, calendar, date_range: list(missing_ranges),
                                    write=lambda security, frame: len(frame))
        runtime.storage = SimpleNamespace(get_tickdata=lambda bar_size: tick_data)
        runtime._try_get_exchange_calendar = lambda security: object()
        runtime.historical_data_client = SimpleNamespace(
            get_contract_history=AsyncMock(return_value=pd.DataFrame()))
        return runtime

    @pytest.mark.asyncio
    @pytest.mark.parametrize('zone, expected', [('Australia/Sydney', 'Australia/Sydney'), ('', 'US/Eastern')])
    async def test_the_request_names_the_listing_timezone(self, zone, expected):
        import datetime as dt

        from trader.data.store import DateRange
        from trader.objects import BarSize

        gap = DateRange(start=dt.date(2026, 11, 12), end=dt.date(2026, 11, 13))
        runtime = self._runtime([gap])

        await runtime._fetch_history_with_resume(self._security(zone), BarSize.Days1, 5, 'orb_bhp')

        call = runtime.historical_data_client.get_contract_history.await_args.kwargs
        assert call['tz_info'] == expected
        assert call['bar_size'] == BarSize.Days1
        start, end = call['start_date'], call['end_date']
        assert start.tzinfo is not None and end.tzinfo is not None
        assert (start.year, start.month, start.day, start.hour, start.minute) == (2026, 11, 12, 0, 0)
        assert (end.year, end.month, end.day, end.hour, end.minute, end.second) == (2026, 11, 13, 23, 59, 59)

    @pytest.mark.asyncio
    async def test_a_covered_window_asks_ib_for_nothing(self):
        from trader.objects import BarSize

        runtime = self._runtime([])
        await runtime._fetch_history_with_resume(self._security('Australia/Sydney'), BarSize.Mins1, 5, 'orb_bhp')
        runtime.historical_data_client.get_contract_history.assert_not_awaited()


# --------------------------------------------------------------------------
# B3.5 — an early close announces itself in `mmr verify` / `mmr preflight`
# --------------------------------------------------------------------------

class TestEarlyCloseCheck:
    """The intraday strategies carry their own session close (16:00 for the US
    classes). On a 13:00 close nothing in the runtime knows; the stack check
    now says so on the day — FAIL when an intraday strategy is armed."""

    HALF_DAY = __import__('datetime').date(2026, 11, 27)        # the day after US Thanksgiving
    ORDINARY = __import__('datetime').date(2026, 11, 25)

    def test_an_ordinary_day_adds_no_check_row(self):
        assert cli._early_close_check('NYSE,ASX', ['orb_googl'], today=self.ORDINARY) is None
        assert cli._early_close_check('NYSE,ASX', [], today=self.ORDINARY) is None

    def test_a_half_day_with_an_armed_intraday_strategy_fails_and_names_it(self):
        status, detail = cli._early_close_check('NYSE,ASX', ['orb_googl', 'vwap_cat'], today=self.HALF_DAY)
        assert status == 'FAIL'
        assert 'NYSE closes early today at 13:00 America/New_York' in detail and 'ASX' not in detail
        assert '2 armed intraday strategies assume their usual session close (orb_googl, vwap_cat)' in detail
        assert 'held by IB until the next session' in detail and 'Disarm for today' in detail

    def test_one_armed_strategy_reads_in_the_singular(self):
        status, detail = cli._early_close_check('NYSE', ['orb_googl'], today=self.HALF_DAY)
        assert status == 'FAIL' and '1 armed intraday strategy assumes its usual session close (orb_googl)' in detail

    def test_a_half_day_with_nothing_armed_warns(self):
        status, detail = cli._early_close_check('NYSE,ASX', [], today=self.HALF_DAY)
        assert status == 'WARN'
        assert 'NYSE closes early today at 13:00 America/New_York' in detail
        assert 'No intraday strategy is armed; do not arm one today' in detail

    def test_only_watched_venues_count_and_each_in_its_own_time(self):
        christmas_eve = __import__('datetime').date(2026, 12, 24)
        assert cli._early_close_check('ASX', [], today=self.HALF_DAY) is None
        status, detail = cli._early_close_check('nyse, asx', [], today=christmas_eve)
        assert status == 'WARN'
        assert ('ASX closes early today at 14:10 Australia/Sydney; '
                'NYSE closes early today at 13:00 America/New_York') in detail

    def test_an_unmapped_venue_is_ignored_and_no_venues_is_no_row(self):
        assert cli._early_close_check('NOSUCHVENUE', ['orb'], today=self.HALF_DAY) is None
        assert cli._early_close_check('', ['orb'], today=self.HALF_DAY) is None

    def test_an_unreadable_calendar_is_a_warning_never_silence(self, monkeypatch):
        from trader.data import market_session

        def broken(name, day):
            raise RuntimeError('calendar unavailable')

        monkeypatch.setattr(market_session, 'early_close', broken)
        status, detail = cli._early_close_check('NYSE', ['orb_googl'], today=self.HALF_DAY)
        assert status == 'WARN'
        assert 'could not read the session calendar for NYSE (RuntimeError: calendar unavailable)' in detail
        assert 'would go unreported' in detail

    def test_a_real_early_close_outranks_another_venues_unreadable_calendar(self, monkeypatch):
        from trader.data import market_session
        real = market_session.early_close

        def asx_broken(name, day):
            if name == 'ASX':
                raise RuntimeError('calendar unavailable')
            return real(name, day)

        monkeypatch.setattr(market_session, 'early_close', asx_broken)
        status, detail = cli._early_close_check('NYSE,ASX', ['orb_googl'], today=self.HALF_DAY)
        assert status == 'FAIL' and 'NYSE closes early today' in detail

    def test_today_defaults_to_the_venues_own_date(self, monkeypatch):
        """No `today`: each venue is asked about ITS current date."""
        from trader.data import market_session
        asked = []
        monkeypatch.setattr(market_session, 'early_close', lambda name, day: asked.append((name, day)))
        assert cli._early_close_check('NYSE,ASX', []) is None
        import pandas as pd
        assert asked == [('ASX', pd.Timestamp.now(tz='Australia/Sydney').date()),
                         ('NYSE', pd.Timestamp.now(tz='America/New_York').date())]


class TestArmedIntradayStrategies:
    @staticmethod
    def _config(tmp_path, text):
        path = tmp_path / 'strategy_runtime.yaml'
        path.write_text(text)
        return path

    def test_only_armed_strategies_on_an_intraday_bar_size_are_named(self, tmp_path):
        path = self._config(tmp_path, '''
strategies:
  - {name: orb_googl, bar_size: "1 min", auto_execute: true}
  - {name: vwap_cat, bar_size: "5 mins", auto_execute: true}
  - {name: momentum_daily, bar_size: "1 day", auto_execute: true}
  - {name: weekly, bar_size: " 1 Week ", auto_execute: true}
  - {name: orb_off, bar_size: "1 min", auto_execute: false}
  - {name: orb_unset, bar_size: "1 min"}
  - {name: no_bar_size, auto_execute: true}
''')
        assert cli._armed_intraday_strategies(path) == ['orb_googl', 'vwap_cat', 'no_bar_size']

    def test_a_missing_empty_or_malformed_file_is_no_strategies(self, tmp_path):
        assert cli._armed_intraday_strategies(tmp_path / 'absent.yaml') == []
        assert cli._armed_intraday_strategies(self._config(tmp_path, '')) == []
        assert cli._armed_intraday_strategies(self._config(tmp_path, 'strategies: [just-a-string, 7]')) == []
        assert cli._armed_intraday_strategies(self._config(tmp_path, 'strategies: {not: [closed')) == []
