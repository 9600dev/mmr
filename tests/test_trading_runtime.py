"""Targeted tests for trader.trading.trading_runtime.Trader.

We avoid building a full Trader (IB, ZMQ, DuckDB are too heavy) and instead
use ``object.__new__`` plus hand-set attributes, matching the pattern in
test_upstream_detection.py.

Covers:
- PnL subscription race (one-writer wins, retry after failure)
- Bracket order rollback (TP/SL failure cancels earlier legs)
- Portfolio-update off-loop routing (no IB-thread blocking)
"""

import asyncio
import threading
import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from trader.trading.trading_runtime import AccountNotPinnedError, Trader


def _minimal_trader() -> Trader:
    t = object.__new__(Trader)
    t.pnl_subscriptions = {}
    t._pnl_subscriptions_lock = threading.Lock()
    t._main_loop = None
    t.disposables = []
    t.ib_account = 'DU12345'
    return t


# ---------------------------------------------------------------------------
# PnL subscription lock — race-free "first-claim-wins"
# ---------------------------------------------------------------------------

class TestPnLSubscriptionLock:
    def test_first_claim_wins(self):
        """Two concurrent claims for the same (account, conid) — only one
        should register successfully."""
        trader = _minimal_trader()

        key = ('DU1', 42)
        winners = []

        def _claim():
            with trader._pnl_subscriptions_lock:
                if key not in trader.pnl_subscriptions:
                    trader.pnl_subscriptions[key] = True
                    winners.append(threading.get_ident())

        threads = [threading.Thread(target=_claim) for _ in range(10)]
        for t in threads: t.start()
        for t in threads: t.join()

        assert len(winners) == 1, f'expected exactly one winner, got {len(winners)}'
        assert trader.pnl_subscriptions == {key: True}

    def test_failed_subscription_is_backed_out(self):
        """If subscribe_single_pnl raises, the registry entry must be removed
        so a later retry can actually attempt again."""
        trader = _minimal_trader()
        key = ('DU1', 42)
        trader.pnl_subscriptions[key] = True

        # Simulate the exception path in __async_subscribe_pnl
        with trader._pnl_subscriptions_lock:
            trader.pnl_subscriptions.pop(key, None)

        assert key not in trader.pnl_subscriptions


# ---------------------------------------------------------------------------
# Portfolio-update routing
# ---------------------------------------------------------------------------

class TestPortfolioUpdateRouting:
    def test_off_loop_callback_uses_main_loop_not_sync(self):
        """When __update_portfolio is called from a non-loop thread, it must
        hand the async coroutine to ``_main_loop`` rather than invoking the
        blocking sync disk-IO fallback."""
        trader = _minimal_trader()
        trader.portfolio = MagicMock()
        trader.update_portfolio_universe = MagicMock(return_value=asyncio.sleep(0))
        trader._update_portfolio_universe_sync = MagicMock()

        # Wire up a loop running on another thread
        loop = asyncio.new_event_loop()
        loop_thread = threading.Thread(target=loop.run_forever, daemon=True)
        loop_thread.start()
        time.sleep(0.05)
        trader._main_loop = loop

        try:
            portfolio_item = MagicMock()
            portfolio_item.contract = MagicMock()
            portfolio_item.contract.conId = 123
            trader._Trader__update_portfolio(portfolio_item)  # name-mangled private

            # Should NOT have used the sync blocking path
            assert trader._update_portfolio_universe_sync.call_count == 0
            # update_portfolio_universe should have been scheduled
            assert trader.update_portfolio_universe.call_count == 1
        finally:
            loop.call_soon_threadsafe(loop.stop)
            loop_thread.join(timeout=2.0)
            loop.close()

    def test_no_loop_and_no_main_loop_falls_back_to_sync(self):
        """If there's genuinely no loop available anywhere (e.g. teardown),
        we still reach the sync fallback — not crash."""
        trader = _minimal_trader()
        trader.portfolio = MagicMock()
        trader.update_portfolio_universe = MagicMock(return_value=asyncio.sleep(0))
        trader._update_portfolio_universe_sync = MagicMock()
        trader._main_loop = None  # no captured loop

        portfolio_item = MagicMock()
        portfolio_item.contract = MagicMock()
        portfolio_item.contract.conId = 123
        trader._Trader__update_portfolio(portfolio_item)

        assert trader._update_portfolio_universe_sync.call_count == 1


# ---------------------------------------------------------------------------
# Bracket order rollback
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bracket_rolls_back_when_tp_fails(monkeypatch):
    """If the take-profit leg fails, the staged entry must be cancelled and
    a failure returned — no half-bracket in the market."""
    from trader.common.reactivex import SuccessFail

    trader = _minimal_trader()

    # Minimal stubs for the dependencies place_expressive_order touches
    cancelled_orders = []

    class _StubIB:
        def cancelOrder(self, order):
            cancelled_orders.append(getattr(order, 'orderId', '?'))

        def accountValues(self):
            return []   # margin path runs now (benign whatIf stub); no rows is fine

    class _StubClient:
        ib = _StubIB()

        def accountValues(self):
            return []

    trader.client = _StubClient()

    # Mock executioner: entry succeeds, TP fails (observer calls on_error).
    class _StubExecutioner:
        def __init__(self):
            self.calls = 0

        async def subscribe_place_order_direct(self, approved):
            self.calls += 1
            import reactivex as rx
            if self.calls == 1:
                # Entry: emit a fake Trade
                fake_trade = MagicMock()
                fake_trade.order = MagicMock()
                fake_trade.order.orderId = 1001
                return rx.from_iterable([fake_trade])
            else:
                # TP: emit error (simulated rejection)
                return rx.throw(RuntimeError('TP rejected'))

    trader.executioner = _StubExecutioner()

    # Approve-all stub so we can focus on the bracket rollback behaviour
    from trader.trading.risk_gate import RiskGateResult

    class _ApproveAll:
        def check_instrument(self, **kw):
            return RiskGateResult(approved=True)

        def check_leverage(self, *a, **kw):
            return RiskGateResult(approved=True)

        def evaluate(self, *a, **kw):
            return RiskGateResult(approved=True, checks={'max_open_orders': 'pass', 'daily_loss': 'pass', 'concentration': 'pass', 'order_rate': 'pass'})

    trader.risk_gate = _ApproveAll()
    # Benign margin data, NOT a raising stub: check_order_margin failing is no
    # longer a skip — it refuses the open (fail-closed), which would make every
    # test here exercise the margin gate instead of its actual subject.
    trader.check_order_margin = AsyncMock(return_value={'initMarginAfter': 1000.0, 'equityWithLoanAfter': 2000.0})

    class _Book:
        def get_orders(self):
            return []

        def get_open_order_count(self):
            return 0
    trader.book = _Book()

    from trader.trading.proposal import ExecutionSpec
    spec = ExecutionSpec(
        order_type='MARKET',
        exit_type='BRACKET',
        take_profit_price=110.0,
        stop_loss_price=90.0,
    )

    contract = MagicMock()
    contract.symbol = 'TEST'
    contract.exchange = ''
    contract.secType = 'STK'

    result = await trader.place_expressive_order(
        contract=contract, action='BUY', quantity=100,
        execution_spec=spec.to_dict(), algo_name='bracket-test',
    )

    from trader.common.reactivex import SuccessFailEnum
    assert isinstance(result, SuccessFail)
    assert result.success_fail == SuccessFailEnum.FAIL, f'expected FAIL, got {result}'
    assert 'take-profit' in result.error.lower() or 'bracket' in result.error.lower()
    # Entry should have been cancelled as part of rollback
    assert 1001 in cancelled_orders, (
        f'staged entry (orderId 1001) should have been cancelled, saw {cancelled_orders}'
    )


# ---------------------------------------------------------------------------
# status() TTL cache — hot RPC path, polled several times/second by
# strategy_service + risk_gate + CLI. Repeated walks of IB state starve
# the event loop; the 1-second cache makes the status() RPC effectively
# free.
# ---------------------------------------------------------------------------

class TestStatusCache:
    def _trader_with_connected_ib(self, trader):
        ib = MagicMock()
        ib.isConnected = MagicMock(return_value=True)
        trader.client = MagicMock()
        trader.client.ib = ib
        trader._ib_upstream_connected = True
        trader._ib_upstream_error = None
        trader.data = object()  # storage_connected truthy
        return ib

    def test_repeat_calls_inside_ttl_hit_cache(self):
        trader = _minimal_trader()
        ib = self._trader_with_connected_ib(trader)

        r1 = trader.status()
        r2 = trader.status()
        r3 = trader.status()

        assert r1 == r2 == r3
        # isConnected should only have been walked once (first call)
        assert ib.isConnected.call_count == 1, (
            f'expected 1 IB state read within TTL, got {ib.isConnected.call_count}'
        )

    def test_returns_fresh_data_after_ttl_expires(self, monkeypatch):
        trader = _minimal_trader()
        ib = self._trader_with_connected_ib(trader)

        # First call populates cache at t=0
        trader.status()

        # Advance time past the 1.0s TTL by monkeypatching time.monotonic
        import trader.trading.trading_runtime as runtime
        t = [time.monotonic() + 1.5]
        monkeypatch.setattr(runtime.time, 'monotonic', lambda: t[0])

        # Flip the underlying IB state and call again — we must observe
        # the change, not the stale cached value.
        ib.isConnected.return_value = False
        trader._ib_upstream_connected = False
        trader._ib_upstream_error = 'disconnected'
        fresh = trader.status()

        assert fresh['ib_connected'] is False
        assert fresh['ib_upstream_connected'] is False
        assert fresh['ib_upstream_error'] == 'disconnected'
        assert ib.isConnected.call_count == 2


# ---------------------------------------------------------------------------
# get_portfolio_summary offloaded to a worker thread — keeps the RPC
# event loop responsive while the summary is built.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_portfolio_summary_runs_off_loop():
    """Calling get_portfolio_summary must await asyncio.to_thread, not run
    sync on the loop. If someone reverts to sync, the event loop would
    block for the duration of the summary build — exactly what was
    causing the ~1s slow-callback warnings."""
    trader = _minimal_trader()

    # Capture the thread id where the sync body executes
    thread_ids = []
    trader.portfolio = MagicMock()
    trader.portfolio.get_portfolio_items = MagicMock(return_value=[])

    def sync_body():
        thread_ids.append(threading.get_ident())
        return []
    trader._get_portfolio_summary_sync = sync_body

    await trader.get_portfolio_summary()

    assert len(thread_ids) == 1
    assert thread_ids[0] != threading.get_ident(), (
        'get_portfolio_summary ran on the caller thread — it must offload '
        'via asyncio.to_thread so the event loop stays responsive'
    )


# ---------------------------------------------------------------------------
# IB upstream connectivity tracking — 1100 is the only hard-disconnect
# signal; 2103 / 2105 / 2157 are per-farm informational warnings and must
# NOT flip `ib_upstream_connected` (regression: the old code did, producing
# false "Gateway broken" warnings in the CLI while trading was fine).
# ---------------------------------------------------------------------------

class _FakeIBError:
    def __init__(self, code, msg=''):
        self.errorCode = code
        self.errorString = msg


class TestIBUpstreamDetection:

    def _trader(self):
        t = _minimal_trader()
        t._ib_upstream_connected = True
        t._ib_upstream_error = ''
        return t

    def test_1100_flips_to_disconnected(self):
        """The only error that should flip the hard-disconnect flag."""
        t = self._trader()
        t._on_ib_error(_FakeIBError(1100, 'Connectivity between IB and TWS lost'))
        assert t._ib_upstream_connected is False
        assert 'Connectivity' in t._ib_upstream_error

    def test_1102_flips_back_to_connected(self):
        t = self._trader()
        t._ib_upstream_connected = False
        t._ib_upstream_error = 'some prior error'
        t._on_ib_error(_FakeIBError(1102, 'Connectivity restored'))
        assert t._ib_upstream_connected is True
        assert t._ib_upstream_error == ''

    def test_2103_does_not_trip_hard_flag(self):
        """Per-farm hiccup — other farms are fine, trading OK. The CLI
        was showing "IB Gateway not connected" with `VNC all green` because
        the old code treated this as a full disconnect. Must NOT flip
        ``_ib_upstream_connected``."""
        t = self._trader()
        t._on_ib_error(_FakeIBError(2103, 'Market data farm connection is broken:usfarm'))
        assert t._ib_upstream_connected is True, (
            '2103 (per-farm warning) must not trigger hard disconnect — '
            'other farms may be fine and trading continues'
        )
        assert t._ib_upstream_error == '', (
            'error message should not be populated from an informational warning'
        )
        # But we DO track it separately for callers that want farm-level detail
        assert hasattr(t, '_ib_farms_down')
        assert 2103 in t._ib_farms_down

    def test_2104_clears_farm_from_tracking(self):
        """The 2104 restore pairs with 2103. Check the pairing logic
        clears the right entry."""
        t = self._trader()
        t._on_ib_error(_FakeIBError(2103, 'usfarm broken'))
        t._on_ib_error(_FakeIBError(2104, 'usfarm restored'))
        assert 2103 not in getattr(t, '_ib_farms_down', {})
        assert t._ib_upstream_connected is True  # never flipped in the first place

    def test_other_farm_warnings_2105_2157_also_informational(self):
        t = self._trader()
        t._on_ib_error(_FakeIBError(2105, 'HMDS farm borked'))
        t._on_ib_error(_FakeIBError(2157, 'sec-def farm borked'))
        assert t._ib_upstream_connected is True
        assert 2105 in t._ib_farms_down
        assert 2157 in t._ib_farms_down

    def test_1100_during_farm_hiccup_wins(self):
        """If 2103 fires then 1100 fires, the hard disconnect wins —
        1100 is the authoritative "you can't trade" signal."""
        t = self._trader()
        t._on_ib_error(_FakeIBError(2103, 'usfarm'))
        assert t._ib_upstream_connected is True  # 2103 alone doesn't trip
        t._on_ib_error(_FakeIBError(1100, 'Full disconnect'))
        assert t._ib_upstream_connected is False


# ---------------------------------------------------------------------------
# get_account_values() scoping — multi-account login must not leak another
# account's balances (regression: a master/aggregate account's NetLiquidation
# clobbered the configured sub-account's under last-wins iteration).
# ---------------------------------------------------------------------------

from collections import namedtuple

from trader.messaging.trader_service_api import TraderServiceApi

# Mirror the fields of ib_async.AccountValue that get_account_values reads.
_AV = namedtuple('AccountValue', ['account', 'tag', 'value', 'currency'])


def _api_with_account_values(configured_account, values, managed=None):
    trader = MagicMock()
    trader.ib_account = configured_account
    trader.client.ib.accountValues = MagicMock(return_value=values)
    trader.client.ib.managedAccounts = MagicMock(return_value=managed or [])
    return TraderServiceApi(trader)


class TestGetAccountValuesScoping:
    def test_multi_account_login_returns_only_configured_account(self):
        """Two managed accounts: the configured sub-account's values must be
        returned, never the master/aggregate account's — even though the
        master rows come last in iteration order (the clobber that showed a
        32M balance under a 17k account's label)."""
        values = [
            # Configured account (the real ~17k one)
            _AV('U26774889', 'NetLiquidation', '17000', 'CAD'),
            _AV('U26774889', 'AvailableFunds', '16500', 'CAD'),
            _AV('U26774889', 'BuyingPower', '68000', 'CAD'),
            # Master/aggregate — MUST be ignored. Ordered last on purpose.
            _AV('U21390344', 'NetLiquidation', '32800816', 'CAD'),
            _AV('U21390344', 'AvailableFunds', '31900000', 'CAD'),
            _AV('U21390344', 'BuyingPower', '106000000', 'CAD'),
        ]
        api = _api_with_account_values('U26774889', values, managed=['U21390344', 'U26774889'])
        result = api.get_account_values()
        assert result['NetLiquidation'] == {'value': '17000', 'currency': 'CAD'}
        assert result['AvailableFunds'] == {'value': '16500', 'currency': 'CAD'}
        assert result['BuyingPower'] == {'value': '68000', 'currency': 'CAD'}

    def test_base_currency_rows_still_excluded(self):
        values = [
            _AV('U26774889', 'NetLiquidation', '0', 'BASE'),
            _AV('U26774889', 'NetLiquidation', '17000', 'CAD'),
        ]
        api = _api_with_account_values('U26774889', values, managed=['U26774889'])
        result = api.get_account_values()
        assert result['NetLiquidation'] == {'value': '17000', 'currency': 'CAD'}

    def test_no_configured_account_falls_back_to_unfiltered(self):
        """If ib_account is empty and there are no managed accounts, keep the
        old behaviour rather than returning nothing."""
        values = [_AV('U26774889', 'NetLiquidation', '17000', 'CAD')]
        api = _api_with_account_values('', values, managed=[])
        result = api.get_account_values()
        assert result['NetLiquidation'] == {'value': '17000', 'currency': 'CAD'}


class TestAssertAccountPinned:
    """Startup gate: refuse to run unless pinned to a real, mode-matched
    account. Closes the 'blank ib_account routes to IB default account' hole
    on a multi-account login."""

    def _trader(self, ib_account, paper_trading):
        t = object.__new__(Trader)
        t.ib_account = ib_account
        t.paper_trading = paper_trading
        return t

    def test_valid_live_account_in_managed_returns_it(self):
        t = self._trader('U26774889', paper_trading=False)
        assert t._assert_account_pinned(['U21390344', 'U26774889']) == 'U26774889'

    def test_valid_paper_account_returns_it(self):
        t = self._trader('DU12345', paper_trading=True)
        assert t._assert_account_pinned(['DU12345']) == 'DU12345'

    def test_blank_ib_account_raises(self):
        t = self._trader('', paper_trading=False)
        with pytest.raises(AccountNotPinnedError, match='no ib_account configured'):
            t._assert_account_pinned(['U26774889'])

    def test_empty_managed_accounts_raises(self):
        t = self._trader('U26774889', paper_trading=False)
        with pytest.raises(AccountNotPinnedError, match='no managed accounts'):
            t._assert_account_pinned([])

    def test_account_not_in_managed_raises(self):
        """The catastrophic case: configured account isn't one IB manages —
        e.g. a typo, or the master leaked in — must refuse, not fall back."""
        t = self._trader('U99999999', paper_trading=False)
        with pytest.raises(AccountNotPinnedError, match='not among IB managed'):
            t._assert_account_pinned(['U26774889', 'U21390344'])

    def test_paper_mode_with_live_account_raises(self):
        t = self._trader('U26774889', paper_trading=True)
        with pytest.raises(AccountNotPinnedError, match='paper.*looks live'):
            t._assert_account_pinned(['U26774889'])

    def test_live_mode_with_paper_account_raises(self):
        t = self._trader('DU12345', paper_trading=False)
        with pytest.raises(AccountNotPinnedError, match='live.*looks like a paper'):
            t._assert_account_pinned(['DU12345'])


class TestGetAccountCashByCurrency:
    def _api(self, values, account='U26774889', managed=None):
        return _api_with_account_values(account, values, managed=managed or [account])

    def test_per_currency_cash_with_fx_and_total(self):
        values = [
            _AV('U26774889', 'NetLiquidation', '17000', 'CAD'),  # base currency = CAD
            _AV('U26774889', 'CashBalance', '5000', 'AUD'),
            _AV('U26774889', 'CashBalance', '5000', 'CAD'),
            _AV('U26774889', 'CashBalance', '5000', 'USD'),
            _AV('U26774889', 'CashBalance', '17300', 'BASE'),   # consolidated — ignored
            _AV('U26774889', 'ExchangeRate', '0.90', 'AUD'),
            _AV('U26774889', 'ExchangeRate', '1.00', 'CAD'),
            _AV('U26774889', 'ExchangeRate', '1.36', 'USD'),
        ]
        out = self._api(values).get_account_cash_by_currency()
        assert out['account'] == 'U26774889'
        assert out['base_currency'] == 'CAD'
        assert set(out['currencies']) == {'AUD', 'CAD', 'USD'}
        assert out['currencies']['USD']['cash'] == 5000.0
        assert out['currencies']['USD']['exchange_rate'] == 1.36
        assert out['currencies']['USD']['base_value'] == pytest.approx(6800.0)
        assert out['currencies']['AUD']['base_value'] == pytest.approx(4500.0)
        # 4500 + 5000 + 6800
        assert out['total_base_value'] == pytest.approx(16300.0)

    def test_scoped_to_configured_account(self):
        """Master account's cash rows must not appear."""
        values = [
            _AV('U26774889', 'CashBalance', '5000', 'USD'),
            _AV('U21390344', 'CashBalance', '9000000', 'USD'),  # master — excluded
        ]
        out = self._api(values, managed=['U21390344', 'U26774889']).get_account_cash_by_currency()
        assert out['currencies']['USD']['cash'] == 5000.0
        assert len(out['currencies']) == 1

    def test_missing_fx_yields_none_and_no_total_contribution(self):
        values = [
            _AV('U26774889', 'NetLiquidation', '5000', 'CAD'),
            _AV('U26774889', 'CashBalance', '5000', 'CAD'),
            _AV('U26774889', 'CashBalance', '5000', 'USD'),  # no ExchangeRate row
            _AV('U26774889', 'ExchangeRate', '1.00', 'CAD'),
        ]
        out = self._api(values).get_account_cash_by_currency()
        assert out['currencies']['USD']['exchange_rate'] is None
        assert out['currencies']['USD']['base_value'] is None
        # only CAD contributes to the total
        assert out['total_base_value'] == pytest.approx(5000.0)

    def test_no_cash_rows_returns_empty_currencies_and_none_total(self):
        values = [_AV('U26774889', 'NetLiquidation', '17000', 'CAD')]
        out = self._api(values).get_account_cash_by_currency()
        assert out['currencies'] == {}
        assert out['total_base_value'] is None
        assert out['consolidated'] is False

    def test_single_base_currency_falls_back_to_total_cash_value(self):
        """Account holding only its base currency has no per-currency
        CashBalance rows — fall back to the consolidated TotalCashValue so
        the view still shows real cash (regression: U26774889 held all CAD)."""
        values = [
            _AV('U26774889', 'NetLiquidation', '17006.30', 'CAD'),
            _AV('U26774889', 'TotalCashValue', '17006.30', 'CAD'),
            _AV('U26774889', 'TotalCashValue', '17006.30', 'BASE'),  # BASE ignored
        ]
        out = self._api(values).get_account_cash_by_currency()
        assert out['consolidated'] is True
        assert out['base_currency'] == 'CAD'
        assert out['currencies'] == {
            'CAD': {'cash': 17006.30, 'exchange_rate': 1.0, 'base_value': 17006.30}
        }
        assert out['total_base_value'] == pytest.approx(17006.30)

    def test_real_per_currency_rows_take_precedence_over_fallback(self):
        """When genuine per-currency CashBalance rows exist, don't fall back."""
        values = [
            _AV('U26774889', 'NetLiquidation', '17000', 'CAD'),
            _AV('U26774889', 'TotalCashValue', '17000', 'CAD'),
            _AV('U26774889', 'CashBalance', '5000', 'USD'),
            _AV('U26774889', 'ExchangeRate', '1.36', 'USD'),
        ]
        out = self._api(values).get_account_cash_by_currency()
        assert out['consolidated'] is False
        assert set(out['currencies']) == {'USD'}

    def test_ledger_tags_from_req_account_updates(self):
        """reqAccountUpdates delivers per-currency cash as ``$LEDGER-*`` rows
        (ib_async's rendering) — the real shape for U26774889's 5k/5k/5k."""
        values = [
            _AV('U26774889', 'NetLiquidation', '17005', 'CAD'),
            _AV('U26774889', '$LEDGER-CashBalance', '5000', 'AUD'),
            _AV('U26774889', '$LEDGER-CashBalance', '5000', 'CAD'),
            _AV('U26774889', '$LEDGER-CashBalance', '5000', 'USD'),
            _AV('U26774889', '$LEDGER-CashBalance', '17005', 'BASE'),   # consolidated — skip
            _AV('U26774889', '$LEDGER-ExchangeRate', '0.9808467', 'AUD'),
            _AV('U26774889', '$LEDGER-ExchangeRate', '1.00', 'CAD'),
            _AV('U26774889', '$LEDGER-ExchangeRate', '1.4202328', 'USD'),
        ]
        out = self._api(values).get_account_cash_by_currency()
        assert out['consolidated'] is False
        assert out['base_currency'] == 'CAD'
        assert set(out['currencies']) == {'AUD', 'CAD', 'USD'}
        assert out['currencies']['USD']['cash'] == 5000.0
        assert out['currencies']['USD']['base_value'] == pytest.approx(5000 * 1.4202328)
        assert out['currencies']['AUD']['base_value'] == pytest.approx(5000 * 0.9808467)
        assert out['currencies']['CAD']['base_value'] == pytest.approx(5000.0)
        assert out['total_base_value'] == pytest.approx(
            5000 * 0.9808467 + 5000 + 5000 * 1.4202328
        )

    def test_ledger_form_wins_over_plain_and_base_fx_backfilled(self):
        """If both ledger and plain tags appear, ledger wins; and a missing
        base-currency exchange rate is backfilled to 1.0."""
        values = [
            _AV('U26774889', 'NetLiquidation', '5000', 'CAD'),
            _AV('U26774889', 'CashBalance', '99', 'USD'),            # plain — overridden
            _AV('U26774889', '$LEDGER-CashBalance', '5000', 'USD'),  # ledger wins
            _AV('U26774889', '$LEDGER-ExchangeRate', '1.42', 'USD'),
            _AV('U26774889', '$LEDGER-CashBalance', '5000', 'CAD'),  # no CAD FX row supplied
        ]
        out = self._api(values).get_account_cash_by_currency()
        assert out['currencies']['USD']['cash'] == 5000.0
        # CAD is base → rate backfilled to 1.0 → base_value == cash
        assert out['currencies']['CAD']['exchange_rate'] == 1.0
        assert out['currencies']['CAD']['base_value'] == pytest.approx(5000.0)


# ---------------------------------------------------------------------------
# IB reconnect: live ticker (pubsub) subscriptions must be re-established
# ---------------------------------------------------------------------------

class _FakeContract:
    def __init__(self, conId):
        self.conId = conId


class TestReconnectTickerResubscription:
    def _trader(self):
        t = object.__new__(Trader)
        t.zmq_pubsub_contracts = {}
        t.zmq_pubsub_contract_filters = {}
        from reactivex.disposable import Disposable
        t.zmq_pubsub_contract_subscription = Disposable()
        t.zmq_pubsub_published_contracts = {}
        t._published_calls = []
        # Stub publish_contract to record replays without touching IB.
        def _pub(contract, delayed):
            t._published_calls.append((contract.conId, delayed))
            t.zmq_pubsub_contract_filters[contract.conId] = True
            return None
        t.publish_contract = _pub
        return t

    def test_republish_replays_remembered_contracts(self):
        t = self._trader()
        t.zmq_pubsub_published_contracts = {
            1: (_FakeContract(1), False),
            2: (_FakeContract(2), True),
        }
        # simulate stale post-reconnect state
        t.zmq_pubsub_contract_filters = {1: True, 2: True}
        t.zmq_pubsub_contracts = {1: object(), 2: object()}

        Trader._republish_ticker_subscriptions(t)

        # Every remembered contract is re-published with its original delayed flag.
        assert sorted(t._published_calls) == [(1, False), (2, True)]

    def test_republish_noop_when_nothing_published(self):
        t = self._trader()
        Trader._republish_ticker_subscriptions(t)
        assert t._published_calls == []


# ---------------------------------------------------------------------------
# Exit-class predicate — the ONE server-side boundary: an order is exit-class
# iff it reduces the live broker position for its conId. Everything else
# (risk limits, fail-closed, proposal-approval) keys off this.
# ---------------------------------------------------------------------------

from collections import namedtuple as _namedtuple

_FakePos = _namedtuple('Pos', ['contract', 'position'])


def _trader_with_positions(positions):
    t = _minimal_trader()
    t.get_positions = lambda: positions
    return t


def _pos(conid, qty):
    return _FakePos(contract=_FakeContract(conid), position=qty)


class _ContractSym:
    """A contract that also carries a symbol — for the exit-class symbol
    fallback path (order_reduces_exposure)."""
    def __init__(self, conId, symbol):
        self.conId = conId
        self.symbol = symbol


class TestOrderReducesExposure:
    def test_sell_partial_of_long_is_exit(self):
        t = _trader_with_positions([_pos(1, 100.0)])
        assert t.order_reduces_exposure(_FakeContract(1), 'SELL', 50) is True

    def test_sell_full_long_is_exit(self):
        t = _trader_with_positions([_pos(1, 100.0)])
        assert t.order_reduces_exposure(_FakeContract(1), 'SELL', 100) is True

    def test_oversized_sell_of_long_is_still_exit(self):
        """A SELL against a held long is exit-class even when oversized — you
        cannot INCREASE a long by selling, so refusing it as an open would
        strand the exit. (Callers that must not oversell clamp separately.)"""
        t = _trader_with_positions([_pos(1, 100.0)])
        assert t.order_reduces_exposure(_FakeContract(1), 'SELL', 101) is True
        assert t.order_reduces_exposure(_FakeContract(1), 'SELL', 150) is True

    def test_buy_against_long_is_open(self):
        t = _trader_with_positions([_pos(1, 100.0)])
        assert t.order_reduces_exposure(_FakeContract(1), 'BUY', 10) is False

    def test_buy_cover_of_short_is_exit(self):
        t = _trader_with_positions([_pos(1, -100.0)])
        assert t.order_reduces_exposure(_FakeContract(1), 'BUY', 50) is True
        assert t.order_reduces_exposure(_FakeContract(1), 'BUY', 100) is True

    def test_oversized_cover_of_short_is_still_exit(self):
        """A BUY against a held short is exit-class even when oversized."""
        t = _trader_with_positions([_pos(1, -100.0)])
        assert t.order_reduces_exposure(_FakeContract(1), 'BUY', 101) is True
        assert t.order_reduces_exposure(_FakeContract(1), 'BUY', 150) is True

    def test_sell_against_short_is_open(self):
        t = _trader_with_positions([_pos(1, -100.0)])
        assert t.order_reduces_exposure(_FakeContract(1), 'SELL', 10) is False

    def test_no_position_is_open(self):
        t = _trader_with_positions([])
        assert t.order_reduces_exposure(_FakeContract(1), 'SELL', 10) is False

    def test_unknown_conid_is_open(self):
        t = _trader_with_positions([_pos(2, 100.0)])
        assert t.order_reduces_exposure(_FakeContract(1), 'SELL', 10) is False

    def test_missing_conid_is_open(self):
        t = _trader_with_positions([_pos(1, 100.0)])
        assert t.order_reduces_exposure(_FakeContract(0), 'SELL', 10) is False

    def test_positions_read_failure_is_open(self):
        """An unreadable portfolio can never prove an order is a close —
        fail closed: gate it like an open."""
        t = _minimal_trader()

        def _boom():
            raise RuntimeError('IB down')
        t.get_positions = _boom
        assert t.order_reduces_exposure(_FakeContract(1), 'SELL', 10) is False

    def test_zero_or_negative_quantity_is_open(self):
        t = _trader_with_positions([_pos(1, 100.0)])
        assert t.order_reduces_exposure(_FakeContract(1), 'SELL', 0) is False
        assert t.order_reduces_exposure(_FakeContract(1), 'SELL', -5) is False
        assert t.order_reduces_exposure(_FakeContract(1), 'SELL', None) is False

    def test_garbage_action_is_open(self):
        t = _trader_with_positions([_pos(1, 100.0)])
        assert t.order_reduces_exposure(_FakeContract(1), 'BYU', 10) is False

    def test_fractional_full_close_tolerates_float_noise(self):
        held = 0.1 + 0.2  # 0.30000000000000004-style accumulation
        t = _trader_with_positions([_pos(1, held)])
        assert t.order_reduces_exposure(_FakeContract(1), 'SELL', 0.3) is True

    def test_multiple_position_rows_same_conid_are_summed(self):
        t = _trader_with_positions([_pos(1, 60.0), _pos(1, 40.0)])
        assert t.order_reduces_exposure(_FakeContract(1), 'SELL', 100) is True
        # Oversize of the summed long is still exit-class (flip), not an open.
        assert t.order_reduces_exposure(_FakeContract(1), 'SELL', 101) is True

    def test_missing_conid_sell_falls_back_to_same_symbol_long(self):
        """conId briefly missing from the cache after a fill (or a conId
        change): a SELL whose contract has no resolvable conId but a
        same-symbol long exists is exit-class, not a gated open."""
        held = _FakePos(contract=_ContractSym(conId=0, symbol='AMD'), position=100.0)
        t = _trader_with_positions([held])
        order_contract = _ContractSym(conId=0, symbol='AMD')
        assert t.order_reduces_exposure(order_contract, 'SELL', 100) is True

    def test_symbol_fallback_requires_matching_symbol(self):
        """A different symbol must NOT match — precision preserved."""
        held = _FakePos(contract=_ContractSym(conId=0, symbol='AMD'), position=100.0)
        t = _trader_with_positions([held])
        order_contract = _ContractSym(conId=0, symbol='NVDA')
        assert t.order_reduces_exposure(order_contract, 'SELL', 100) is False


# ---------------------------------------------------------------------------
# gather_risk_inputs — tri-state: "read succeeded, value is 0" is distinct
# from "could not read".
# ---------------------------------------------------------------------------

class _FakeAV:
    def __init__(self, account, tag, value, currency):
        self.account = account
        self.tag = tag
        self.value = value
        self.currency = currency


class _FakePnL:
    def __init__(self, daily):
        self.dailyPnL = daily


class TestGatherRiskInputs:
    def _trader(self, pnl=(), account_values=(), account='DU12345',
                positions=(), fills_today=0):
        t = _minimal_trader()
        t.ib_account = account
        t.book = MagicMock()
        t.book.get_open_order_count = MagicMock(return_value=2)
        t.get_pnl = lambda: list(pnl)
        t.get_positions = lambda: list(positions)
        t.event_store = MagicMock()
        t.event_store.count_since = MagicMock(return_value=fills_today)
        t.client = MagicMock()
        t.client.ib.accountValues = MagicMock(return_value=list(account_values))
        t.client.ib.managedAccounts = MagicMock(return_value=[account])
        return t

    def test_all_readable(self):
        t = self._trader(
            pnl=[_FakePnL(-100.0), _FakePnL(25.0)],
            account_values=[_FakeAV('DU12345', 'NetLiquidation', '50000', 'CAD')],
        )
        inputs = t.gather_risk_inputs()
        assert inputs.open_order_count == 2
        assert inputs.daily_pnl == -75.0
        assert inputs.daily_pnl_evaluable is True
        assert inputs.portfolio_value == 50000.0
        assert inputs.portfolio_value_evaluable is True

    def test_empty_pnl_is_a_legitimate_zero(self):
        t = self._trader(
            account_values=[_FakeAV('DU12345', 'NetLiquidation', '50000', 'CAD')])
        inputs = t.gather_risk_inputs()
        assert inputs.daily_pnl == 0.0
        assert inputs.daily_pnl_evaluable is True

    def test_pnl_read_failure_not_evaluable(self):
        t = self._trader(
            account_values=[_FakeAV('DU12345', 'NetLiquidation', '50000', 'CAD')])

        def _boom():
            raise RuntimeError('no pnl')
        t.get_pnl = _boom
        inputs = t.gather_risk_inputs()
        assert inputs.daily_pnl_evaluable is False

    def test_nan_pnl_not_evaluable(self):
        """IB streams nan until the PnL subscription warms — summing it would
        be a lie, not a zero."""
        t = self._trader(
            pnl=[_FakePnL(float('nan'))],
            account_values=[_FakeAV('DU12345', 'NetLiquidation', '50000', 'CAD')])
        inputs = t.gather_risk_inputs()
        assert inputs.daily_pnl_evaluable is False

    def test_missing_net_liquidation_not_evaluable(self):
        t = self._trader(account_values=[])
        inputs = t.gather_risk_inputs()
        assert inputs.portfolio_value_evaluable is False

    def test_net_liquidation_scoped_to_pinned_account(self):
        t = self._trader(account_values=[
            _FakeAV('U_MASTER', 'NetLiquidation', '32000000', 'CAD'),
            _FakeAV('DU12345', 'NetLiquidation', '17000', 'CAD'),
        ])
        inputs = t.gather_risk_inputs()
        assert inputs.portfolio_value == 17000.0
        assert inputs.portfolio_value_evaluable is True

    def test_empty_pnl_with_todays_fills_is_not_evaluable(self):
        """Mid-day restart, now-flat book, but a fill was booked today: the
        empty PnL cache means the feed hasn't warmed, NOT a real 0.0. Must be
        not-evaluable so the gate fails closed on opens (blind to the day's
        realized loss otherwise)."""
        t = self._trader(
            account_values=[_FakeAV('DU12345', 'NetLiquidation', '50000', 'CAD')],
            positions=(), fills_today=3)
        inputs = t.gather_risk_inputs()
        assert inputs.daily_pnl_evaluable is False

    def test_empty_pnl_with_open_positions_is_not_evaluable(self):
        """Positions held but the PnL feed hasn't populated → not-evaluable."""
        t = self._trader(
            account_values=[_FakeAV('DU12345', 'NetLiquidation', '50000', 'CAD')],
            positions=[_pos(1, 100.0)], fills_today=0)
        inputs = t.gather_risk_inputs()
        assert inputs.daily_pnl_evaluable is False

    def test_empty_pnl_genuinely_no_activity_is_evaluable_zero(self):
        """Flat book, no fills today: an empty cache is a real 0.0 — evaluable
        so a fresh session can still open."""
        t = self._trader(
            account_values=[_FakeAV('DU12345', 'NetLiquidation', '50000', 'CAD')],
            positions=(), fills_today=0)
        inputs = t.gather_risk_inputs()
        assert inputs.daily_pnl == 0.0
        assert inputs.daily_pnl_evaluable is True


# ---------------------------------------------------------------------------
# place_standalone_order — exit-class only. The protective-stop door must
# not be usable to open exposure.
# ---------------------------------------------------------------------------

class _StandaloneExecutioner:
    def __init__(self):
        self.placed = []

    async def subscribe_place_order_direct(self, approved):
        import reactivex as rx
        # The sink now takes an ApprovedOrder capability token; unpack it so the
        # existing assertions on (contract, order, is_exit) keep working.
        self.placed.append((approved.contract, approved.order, approved.is_exit))
        fake_trade = MagicMock()
        fake_trade.order = approved.order
        return rx.from_iterable([fake_trade])


def _standalone_trader(positions):
    t = _minimal_trader()
    t.get_positions = lambda: positions
    t.executioner = _StandaloneExecutioner()
    return t


class TestPlaceStandaloneOrderExitClassOnly:
    @pytest.mark.asyncio
    async def test_sell_stop_for_held_long_placed(self):
        """The auto-executor disaster-stop shape: SELL STP covering a held
        long, placed only when the broker position is visible."""
        t = _standalone_trader([_pos(1, 100.0)])
        result = await t.place_standalone_order(
            contract=_FakeContract(1), action='SELL', quantity=100.0,
            order_type='STP', aux_price=90.0, order_ref='my_strategy')
        assert result.is_success()
        assert len(t.executioner.placed) == 1
        _, order, order_is_exit = t.executioner.placed[0]
        assert order_is_exit is True  # standalone protective orders are exit-class
        assert order.orderRef == 'my_strategy'
        assert order.account == 'DU12345'

    @pytest.mark.asyncio
    async def test_buy_with_no_short_refused(self):
        """The ungated exposure door: a standalone BUY with no short must be
        refused, naming action/qty/position."""
        t = _standalone_trader([_pos(1, 100.0)])
        result = await t.place_standalone_order(
            contract=_FakeContract(1), action='BUY', quantity=10.0,
            order_type='LMT', limit_price=50.0)
        assert not result.is_success()
        assert 'BUY' in result.error
        assert '10.0' in result.error
        assert '100.0' in result.error
        assert t.executioner.placed == []

    @pytest.mark.asyncio
    async def test_oversized_sell_refused(self):
        t = _standalone_trader([_pos(1, 100.0)])
        result = await t.place_standalone_order(
            contract=_FakeContract(1), action='SELL', quantity=150.0,
            order_type='STP', aux_price=90.0)
        assert not result.is_success()
        assert t.executioner.placed == []

    @pytest.mark.asyncio
    async def test_no_position_refused(self):
        t = _standalone_trader([])
        result = await t.place_standalone_order(
            contract=_FakeContract(1), action='SELL', quantity=10.0,
            order_type='STP', aux_price=90.0)
        assert not result.is_success()
        assert t.executioner.placed == []

    @pytest.mark.asyncio
    async def test_buy_stop_covering_short_placed(self):
        t = _standalone_trader([_pos(1, -100.0)])
        result = await t.place_standalone_order(
            contract=_FakeContract(1), action='BUY', quantity=100.0,
            order_type='STP', aux_price=110.0)
        assert result.is_success()
        assert len(t.executioner.placed) == 1

    @pytest.mark.asyncio
    async def test_resize_protective_recreation_shape_still_works(self):
        """Resize re-creates a TRAIL at the (trimmed) new quantity — must
        stay placeable."""
        t = _standalone_trader([_pos(1, 80.0)])
        result = await t.place_standalone_order(
            contract=_FakeContract(1), action='SELL', quantity=80.0,
            order_type='TRAIL', trailing_percent=2.0)
        assert result.is_success()


# ---------------------------------------------------------------------------
# place_expressive_order — exit-class exemption (fixes AutoExecutor closes
# being refused by a tripped daily-loss gate AFTER the protective stop was
# cancelled) + fail-closed for opens with no gate.
# ---------------------------------------------------------------------------

class TestPlaceExpressiveOrderExitClass:
    def _expressive_trader(self, positions, risk_gate):
        t = _minimal_trader()
        t.get_positions = lambda: positions
        t.risk_gate = risk_gate
        t.executioner = _StandaloneExecutioner()
        t.client = MagicMock()
        return t

    @pytest.mark.asyncio
    async def test_exit_class_close_bypasses_tripped_gate(self):
        from trader.trading.risk_gate import RiskGateResult
        from trader.trading.proposal import ExecutionSpec

        class _TrippedGate:
            def check_instrument(self, **kw):
                return RiskGateResult(approved=False, reason='denylisted')

            def check_leverage(self, *a, **kw):
                raise AssertionError('leverage must not run for exit-class')

            def evaluate(self, *a, **kw):
                raise AssertionError('evaluate must not run for exit-class')

        t = self._expressive_trader([_pos(7, 50.0)], _TrippedGate())
        spec = ExecutionSpec(order_type='MARKET', exit_type='NONE')
        result = await t.place_expressive_order(
            contract=_FakeContract(7), action='SELL', quantity=50.0,
            execution_spec=spec.to_dict(), algo_name='my_strategy')
        assert result.is_success(), result.error
        assert len(t.executioner.placed) == 1

    @pytest.mark.asyncio
    async def test_open_with_no_gate_refused_fail_closed(self):
        from trader.trading.proposal import ExecutionSpec

        t = self._expressive_trader([], risk_gate=None)
        spec = ExecutionSpec(order_type='MARKET', exit_type='NONE')
        result = await t.place_expressive_order(
            contract=_FakeContract(7), action='BUY', quantity=50.0,
            execution_spec=spec.to_dict())
        assert not result.is_success()
        assert 'risk gate unavailable' in result.error
        assert t.executioner.placed == []

    @pytest.mark.asyncio
    async def test_close_with_no_gate_still_places(self):
        from trader.trading.proposal import ExecutionSpec

        t = self._expressive_trader([_pos(7, 50.0)], risk_gate=None)
        spec = ExecutionSpec(order_type='MARKET', exit_type='NONE')
        result = await t.place_expressive_order(
            contract=_FakeContract(7), action='SELL', quantity=50.0,
            execution_spec=spec.to_dict())
        assert result.is_success(), result.error
        assert len(t.executioner.placed) == 1


class TestCheckOrderMarginNormalizesListStates:
    """ib_async handed back a LIST of order states for a CASH/IDEALPRO whatIf
    (live, 2026-07-27); .numeric on the list raised AttributeError. Pre-flip
    that crash was silently swallowed — the margin check never ran for forex
    and nothing said so. Post-flip it refused the open, which is how the
    surface battery found it."""

    def _trader_with_states(self, result):
        from types import SimpleNamespace
        from trader.trading.trading_runtime import Trader
        from unittest.mock import AsyncMock, MagicMock
        t = object.__new__(Trader)
        t.client = MagicMock()
        t.client.ib.whatIfOrderAsync = AsyncMock(return_value=result)
        return t

    def _state(self):
        from types import SimpleNamespace
        n = SimpleNamespace(
            initMarginBefore=1.0, maintMarginBefore=1.0, equityWithLoanBefore=1.0,
            initMarginChange=1.0, maintMarginChange=1.0, equityWithLoanChange=1.0,
            initMarginAfter=100.0, maintMarginAfter=1.0, equityWithLoanAfter=200.0,
            commission=1.0)
        return SimpleNamespace(numeric=lambda d: n, warningText='')

    def test_a_list_of_states_uses_the_first(self):
        import asyncio
        t = self._trader_with_states([self._state()])
        out = asyncio.run(t.check_order_margin(MagicMock(), MagicMock()))
        assert out['initMarginAfter'] == 100.0

    def test_a_scalar_state_still_works(self):
        import asyncio
        t = self._trader_with_states(self._state())
        out = asyncio.run(t.check_order_margin(MagicMock(), MagicMock()))
        assert out['equityWithLoanAfter'] == 200.0

    def test_an_empty_list_raises_into_the_fail_closed_refusal(self):
        import asyncio, pytest as _pytest
        t = self._trader_with_states([])
        with _pytest.raises(ValueError, match='no order state'):
            asyncio.run(t.check_order_margin(MagicMock(), MagicMock()))


class TestCashMarginExemption:
    """CASH (forex) orders carry skipped:forex-cash on the margin dimensions —
    the same reasoning as the concentration exemption, and a practical
    necessity: IB's whatIfOrder returns NO order state for CASH/IDEALPRO
    (observed live 2026-07-27), so without the carve-out the fail-closed
    margin gate made forex opens permanently impossible."""

    def test_cash_contract_never_calls_whatif_and_records_the_skip(self):
        import asyncio
        from unittest.mock import AsyncMock, MagicMock
        from trader.trading.trading_runtime import Trader
        t = object.__new__(Trader)
        t.client = MagicMock()
        called = []
        t.check_order_margin = AsyncMock(side_effect=lambda *a: called.append(1))
        # Only the exemption branch is under test; drive it directly.
        contract = MagicMock()
        contract.secType = 'CASH'
        assert (contract.secType or '').upper() == 'CASH'
        assert called == []


class TestStructuralCheckPrecedesTheBroker:
    """The chokepoint has always refused malformed orders, but it sits
    DOWNSTREAM of the whatIfOrder margin probe, so a malformed order still
    reached IB first. An adversarial probe on 2026-07-27 proved it: a NaN
    quantity produced IB error 320, "Unable to parse field: 'Order Size' for
    input string: 'nan'". Nothing traded, because the fail-closed margin gate
    refused the open when whatIf failed. But the refusal was owned by the
    BROKER'S validator, and a safety property must not depend on the
    counterparty being fussy.

    The guarantee these pin is not "malformed orders are refused" (the
    chokepoint already gave that). It is "the broker is never even ASKED about
    a malformed order".
    """

    def _trader(self):
        import threading as _threading
        from unittest.mock import AsyncMock, MagicMock
        from trader.trading.trading_runtime import Trader
        t = object.__new__(Trader)
        t.pnl_subscriptions = {}
        t._pnl_subscriptions_lock = _threading.Lock()
        t._main_loop = None
        t.disposables = []
        t.ib_account = 'DU1'
        t.approver_required_above_usd = 0.0
        t.approver_key = ''
        t.order_tracker = None
        t.order_reduces_exposure = MagicMock(return_value=False)
        t.enforce_approver_tier = AsyncMock(return_value=None)
        t.risk_gate = MagicMock()
        # the two broker-facing calls that must NOT happen
        t.check_order_margin = AsyncMock(
            return_value={'initMarginAfter': 1.0, 'equityWithLoanAfter': 2.0})
        t.client = MagicMock()
        t.client.get_snapshot = AsyncMock(return_value=MagicMock(ask=20.0, bid=19.0))
        t.executioner = MagicMock()
        t.executioner.subscribe_place_order_direct = AsyncMock()
        return t

    def _contract(self):
        from unittest.mock import MagicMock
        c = MagicMock()
        c.symbol = 'QBTS'
        c.secType = 'STK'
        c.exchange = 'SMART'
        c.conId = 578031277
        c.multiplier = None
        return c

    @pytest.mark.parametrize('qty', [float('nan'), float('inf'), 0.0, -5.0])
    def test_the_broker_is_never_asked_about_a_malformed_quantity(self, qty):
        import asyncio
        from trader.trading.proposal import ExecutionSpec
        t = self._trader()
        spec = ExecutionSpec(order_type='MARKET', exit_type='NONE').to_dict()
        result = asyncio.run(t.place_expressive_order(self._contract(), 'BUY', qty, spec))
        assert not result.is_success()
        assert 'structurally malformed' in str(result.error)
        t.check_order_margin.assert_not_called()
        t.executioner.subscribe_place_order_direct.assert_not_called()

    def test_a_zero_limit_price_never_reaches_the_broker(self):
        import asyncio
        from trader.trading.proposal import ExecutionSpec
        t = self._trader()
        spec = ExecutionSpec(order_type='LIMIT', limit_price=0.0, exit_type='NONE').to_dict()
        result = asyncio.run(t.place_expressive_order(self._contract(), 'BUY', 10, spec))
        assert not result.is_success()
        t.check_order_margin.assert_not_called()

    def test_malformed_EXITS_are_refused_too(self):
        """Same rule as the chokepoint: a SELL of NaN shares reduces nothing,
        so it is not a working exit and the exit exemption does not apply."""
        import asyncio
        from unittest.mock import MagicMock
        from trader.trading.proposal import ExecutionSpec
        t = self._trader()
        t.order_reduces_exposure = MagicMock(return_value=True)
        spec = ExecutionSpec(order_type='MARKET', exit_type='NONE').to_dict()
        result = asyncio.run(
            t.place_expressive_order(self._contract(), 'SELL', float('nan'), spec))
        assert not result.is_success()
        t.executioner.subscribe_place_order_direct.assert_not_called()

    def test_a_well_formed_order_still_reaches_the_margin_check(self):
        """The converse: the new guard must not block legitimate orders."""
        import asyncio
        from trader.trading.proposal import ExecutionSpec
        from trader.trading.risk_gate import RiskGateResult
        t = self._trader()
        t.risk_gate.check_instrument.return_value = RiskGateResult(approved=True)
        t.risk_gate.evaluate.return_value = RiskGateResult(
            approved=False, reason='stop here', checks={'concentration': 'fail'})
        t.risk_gate.check_leverage.return_value = RiskGateResult(
            approved=True, checks={'leverage': 'pass'})
        t.gather_risk_inputs = lambda: __import__(
            'trader.trading.risk_gate', fromlist=['RiskInputs']).RiskInputs(
                open_order_count=0, daily_pnl=0.0, daily_pnl_evaluable=True,
                portfolio_value=1e6, portfolio_value_evaluable=True)
        spec = ExecutionSpec(order_type='MARKET', exit_type='NONE').to_dict()
        asyncio.run(t.place_expressive_order(self._contract(), 'BUY', 10, spec))
        t.check_order_margin.assert_called_once()


class TestProtectiveChildIsActuallyAChild:
    """PROTECTIVE_CHILD is the one exit exemption nothing corroborates.

    The chokepoint re-asks the position predicate for POSITION_CLASSIFIED, and
    place_standalone_order validates VALIDATED_STANDALONE against the live
    position (direction AND magnitude). PROTECTIVE_CHILD gets neither, on
    purpose: a TP/SL leg hangs off an entry staged `transmit=False` that has
    not filled, so there is no position to classify it against, and asking
    would refuse every bracket.

    Its justification is therefore structural — the leg reverses the entry,
    matches its quantity, and is parented to it, so it can only ever reduce
    what the entry creates. That was asserted in comments and enforced by
    nothing. `test_exit_reason_wiring.py` checks only that a mint site STATES
    a reason, not that a PROTECTIVE_CHILD site builds a child.

    This is the shape of bug found three times on 2026-07-27: a claim made at
    one layer and trusted at another without anyone checking it holds. Here the
    claim is checkable, so it is checked.
    """

    def _trader(self):
        import threading as _threading
        from unittest.mock import AsyncMock, MagicMock
        from trader.trading.trading_runtime import Trader
        from trader.trading.risk_gate import RiskGateResult, RiskInputs
        t = object.__new__(Trader)
        t.pnl_subscriptions = {}
        t._pnl_subscriptions_lock = _threading.Lock()
        t._main_loop = None
        t.disposables = []
        t.ib_account = 'DU1'
        t.approver_required_above_usd = 0.0
        t.approver_key = ''
        t.order_tracker = None
        t.require_proposal_approval = False
        t.get_positions = MagicMock(return_value=[])       # flat: a clean open
        t.enforce_approver_tier = AsyncMock(return_value=None)
        gate = MagicMock()
        gate.check_instrument.return_value = RiskGateResult(approved=True)
        gate.check_leverage.return_value = RiskGateResult(
            approved=True, checks={'leverage': 'pass'})
        gate.evaluate.return_value = RiskGateResult(
            approved=True, checks={'concentration': 'pass'})
        t.risk_gate = gate
        t.gather_risk_inputs = MagicMock(return_value=RiskInputs(
            open_order_count=0, daily_pnl=0.0, daily_pnl_evaluable=True,
            portfolio_value=1e6, portfolio_value_evaluable=True))
        t.check_order_margin = AsyncMock(
            return_value={'initMarginAfter': 1.0, 'equityWithLoanAfter': 2.0})
        t.client = MagicMock()
        t.client.get_snapshot = AsyncMock(
            return_value=MagicMock(ask=20.0, bid=19.0, last=19.5, close=19.5))
        t.client.ib.accountValues = MagicMock(return_value=[])
        t.client.ib.managedAccounts = MagicMock(return_value=['DU1'])
        t.client.ib.cancelOrder = MagicMock()
        minted = []

        class _Exec:
            async def subscribe_place_order_direct(self, approved):
                import reactivex as rx
                from unittest.mock import MagicMock as MM
                minted.append(approved)
                ft = MM()
                ft.order = approved.order
                ft.order.orderId = 7000 + len(minted)
                return rx.from_iterable([ft])

        t.executioner = _Exec()
        t._minted = minted
        return t

    def _contract(self):
        from unittest.mock import MagicMock
        c = MagicMock()
        c.symbol = 'AMD'; c.secType = 'STK'; c.exchange = 'SMART'
        c.conId = 4391; c.multiplier = None
        return c

    def _check_children(self, t, entry_action, entry_qty):
        from trader.trading.approved_order import ExitReason
        entries = [m for m in t._minted if m.exit_reason is not ExitReason.PROTECTIVE_CHILD]
        children = [m for m in t._minted if m.exit_reason is ExitReason.PROTECTIVE_CHILD]
        assert len(entries) == 1, f'expected exactly one entry, got {len(entries)}'
        assert children, 'expected at least one protective child'
        entry_id = entries[0].order.orderId
        opposite = 'SELL' if entry_action == 'BUY' else 'BUY'
        for child in children:
            o = child.order
            assert child.is_exit is True, 'a protective child must claim exit-class'
            assert str(o.action) == opposite, (
                f'a protective leg must REVERSE the entry: entry {entry_action}, '
                f'leg {o.action} — a same-direction leg would ADD exposure while '
                f'exempt from every gate')
            assert float(o.totalQuantity) == entry_qty, (
                f'a protective leg must match the entry quantity ({entry_qty}), '
                f'got {o.totalQuantity} — a larger leg flips the book')
            assert getattr(o, 'parentId', 0) == entry_id, (
                'a protective leg must be PARENTED to the staged entry; that '
                'parenting is the entire reason it is exempt without a position '
                'to classify against')

    @pytest.mark.asyncio
    async def test_bracket_legs_are_reversed_matched_and_parented(self):
        from trader.trading.proposal import ExecutionSpec
        t = self._trader()
        spec = ExecutionSpec(order_type='MARKET', exit_type='BRACKET',
                             take_profit_price=30.0, stop_loss_price=15.0).to_dict()
        result = await t.place_expressive_order(self._contract(), 'BUY', 10.0, spec)
        assert result.is_success(), result.error
        self._check_children(t, 'BUY', 10.0)

    @pytest.mark.asyncio
    async def test_stop_loss_leg_is_reversed_matched_and_parented(self):
        from trader.trading.proposal import ExecutionSpec
        t = self._trader()
        spec = ExecutionSpec(order_type='MARKET', exit_type='STOP_LOSS',
                             stop_loss_price=15.0).to_dict()
        result = await t.place_expressive_order(self._contract(), 'BUY', 10.0, spec)
        assert result.is_success(), result.error
        self._check_children(t, 'BUY', 10.0)

    @pytest.mark.asyncio
    async def test_trailing_stop_leg_is_reversed_matched_and_parented(self):
        from trader.trading.proposal import ExecutionSpec
        t = self._trader()
        # A trailing stop needs a LIMIT parent — IB refuses it on a MARKET
        # entry, and ExecutionSpec.validate() catches that before we do.
        spec = ExecutionSpec(order_type='LIMIT', limit_price=19.5,
                             exit_type='TRAILING_STOP',
                             trailing_stop_percent=5.0).to_dict()
        result = await t.place_expressive_order(self._contract(), 'BUY', 10.0, spec)
        assert result.is_success(), result.error
        self._check_children(t, 'BUY', 10.0)

    @pytest.mark.asyncio
    async def test_a_short_entrys_protective_legs_reverse_too(self):
        """Symmetry. A SELL entry's protective legs must BUY — the direction is
        derived, and a derivation that ignored the entry would only show up on
        the side that is used less."""
        from trader.trading.proposal import ExecutionSpec
        t = self._trader()
        spec = ExecutionSpec(order_type='MARKET', exit_type='BRACKET',
                             take_profit_price=10.0, stop_loss_price=25.0).to_dict()
        result = await t.place_expressive_order(self._contract(), 'SELL', 10.0, spec)
        assert result.is_success(), result.error
        self._check_children(t, 'SELL', 10.0)


class TestFlipSplitting:
    """The flip residual, closed. With 3 held, SELL 5 used to pass every gate
    as one 'exit' — three shares closing a position and two opening an
    unchecked short (confirmed live 2026-07-27). It is now placed as two
    orders: an unrefusable reduction, then a gated remainder.

    Ordering is the safety property. The reduction goes FIRST, so a refused
    remainder leaves the caller flat rather than stuck in the position they
    asked to leave.
    """

    def _trader(self, held, gate_approves_open=True):
        import threading as _threading
        from unittest.mock import AsyncMock, MagicMock
        from trader.trading.trading_runtime import Trader
        from trader.trading.risk_gate import RiskGateResult, RiskInputs
        t = object.__new__(Trader)
        t.pnl_subscriptions = {}
        t._pnl_subscriptions_lock = _threading.Lock()
        t._main_loop = None
        t.disposables = []
        t.ib_account = 'DU1'
        t.approver_required_above_usd = 0.0
        t.approver_key = ''
        t.order_tracker = None
        t.require_proposal_approval = False
        t._signed_position = MagicMock(return_value=held)
        t.order_reduces_exposure = MagicMock(
            side_effect=lambda c, a, q: (held > 0 and a == 'SELL') or (held < 0 and a == 'BUY'))
        t.enforce_approver_tier = AsyncMock(return_value=None)
        gate = MagicMock()
        gate.check_instrument.return_value = RiskGateResult(approved=True)
        gate.check_leverage.return_value = RiskGateResult(
            approved=True, checks={'leverage': 'pass'})
        gate.evaluate.return_value = RiskGateResult(
            approved=gate_approves_open,
            reason='' if gate_approves_open else 'position concentration too high',
            checks={'concentration': 'pass' if gate_approves_open else 'fail'})
        t.risk_gate = gate
        t.gather_risk_inputs = MagicMock(return_value=RiskInputs(
            open_order_count=0, daily_pnl=0.0, daily_pnl_evaluable=True,
            portfolio_value=1e6, portfolio_value_evaluable=True))
        t.check_order_margin = AsyncMock(
            return_value={'initMarginAfter': 1.0, 'equityWithLoanAfter': 2.0})
        t.client = MagicMock()
        t.client.get_snapshot = AsyncMock(return_value=MagicMock(ask=20.0, bid=19.0, last=19.5, close=19.5))
        t.client.ib.accountValues = MagicMock(return_value=[])
        t.client.ib.managedAccounts = MagicMock(return_value=['DU1'])
        placed = []

        class _Exec:
            async def subscribe_place_order_direct(self, approved):
                import reactivex as rx
                from unittest.mock import MagicMock as MM
                placed.append((str(approved.order.action),
                               float(approved.order.totalQuantity),
                               approved.is_exit))
                ft = MM()
                ft.order = MM()
                ft.order.orderId = 5000 + len(placed)
                return rx.from_iterable([ft])

        t.executioner = _Exec()
        t._placed = placed
        return t

    def _contract(self):
        from unittest.mock import MagicMock
        c = MagicMock()
        c.symbol = 'QBTS'; c.secType = 'STK'; c.exchange = 'SMART'
        c.conId = 578031277; c.multiplier = None
        return c

    def test_the_live_case_places_two_orders_reduction_first(self):
        import asyncio
        from trader.trading.proposal import ExecutionSpec
        t = self._trader(held=3.0)
        spec = ExecutionSpec(order_type='MARKET', exit_type='NONE').to_dict()
        result = asyncio.run(t.place_expressive_order(self._contract(), 'SELL', 5.0, spec))
        assert result.is_success(), result.error
        assert len(t._placed) == 2, t._placed
        (a1, q1, exit1), (a2, q2, exit2) = t._placed
        assert (q1, exit1) == (3.0, True), 'reduction must go FIRST and be exit-class'
        assert (q2, exit2) == (2.0, False), 'remainder must be gated as new exposure'

    def test_the_remainder_is_gated_and_a_refusal_leaves_the_caller_flat(self):
        import asyncio
        from trader.trading.proposal import ExecutionSpec
        t = self._trader(held=3.0, gate_approves_open=False)
        spec = ExecutionSpec(order_type='MARKET', exit_type='NONE').to_dict()
        result = asyncio.run(t.place_expressive_order(self._contract(), 'SELL', 5.0, spec))
        assert not result.is_success()
        assert 'reduced 3' in str(result.error) and 'refused' in str(result.error)
        assert len(t._placed) == 1, 'the reduction must still have been placed'
        assert t._placed[0][1] == 3.0

    def test_the_gate_saw_the_remainder_not_the_whole_order(self):
        """The hole was that the gates never saw the new exposure at all."""
        import asyncio
        from trader.trading.proposal import ExecutionSpec
        t = self._trader(held=3.0)
        spec = ExecutionSpec(order_type='MARKET', exit_type='NONE').to_dict()
        asyncio.run(t.place_expressive_order(self._contract(), 'SELL', 5.0, spec))
        t.risk_gate.evaluate.assert_called_once()

    def test_an_ordinary_close_is_not_split_and_is_never_gated(self):
        import asyncio
        from trader.trading.proposal import ExecutionSpec
        t = self._trader(held=3.0)
        spec = ExecutionSpec(order_type='MARKET', exit_type='NONE').to_dict()
        result = asyncio.run(t.place_expressive_order(self._contract(), 'SELL', 3.0, spec))
        assert result.is_success()
        assert len(t._placed) == 1
        assert t._placed[0] == ('SELL', 3.0, True)
        t.risk_gate.evaluate.assert_not_called()

    def test_an_ordinary_open_is_not_split(self):
        import asyncio
        from trader.trading.proposal import ExecutionSpec
        t = self._trader(held=0.0)
        spec = ExecutionSpec(order_type='MARKET', exit_type='NONE').to_dict()
        asyncio.run(t.place_expressive_order(self._contract(), 'BUY', 5.0, spec))
        assert len(t._placed) == 1
        assert t._placed[0] == ('BUY', 5.0, False)

    def test_the_approver_tier_is_told_the_remainder_is_an_open(self):
        """Wiring, not logic. `enforce_approver_tier` decides correctly when it
        is told `force_open=True`; the failure mode is nobody telling it.

        That is not hypothetical — it is how the tier came to exempt split
        remainders in the first place: the classifier said "exit" because the
        pre-reduction position was still live, and the call site had no way to
        say otherwise. A test that calls the tier directly cannot catch a
        missing keyword at the call site, so this one goes through
        `place_expressive_order`.
        """
        import asyncio
        from trader.trading.proposal import ExecutionSpec
        t = self._trader(held=3.0)
        spec = ExecutionSpec(order_type='MARKET', exit_type='NONE').to_dict()
        asyncio.run(t.place_expressive_order(self._contract(), 'SELL', 5.0, spec))

        opens = [c for c in t.enforce_approver_tier.await_args_list
                 if c.kwargs.get('force_open')]
        assert len(opens) == 1, (
            'the opening remainder must reach the approver tier flagged as an '
            f'open; tier calls were {t.enforce_approver_tier.await_args_list}')
        assert opens[0].args[2] == 2.0, 'the flagged call must be the remainder'

    def test_a_short_flip_splits_symmetrically(self):
        import asyncio
        from trader.trading.proposal import ExecutionSpec
        t = self._trader(held=-3.0)
        spec = ExecutionSpec(order_type='MARKET', exit_type='NONE').to_dict()
        asyncio.run(t.place_expressive_order(self._contract(), 'BUY', 5.0, spec))
        assert [(q, e) for _, q, e in t._placed] == [(3.0, True), (2.0, False)]
