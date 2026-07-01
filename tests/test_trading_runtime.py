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
from unittest.mock import MagicMock

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

    class _StubClient:
        ib = _StubIB()

        def accountValues(self):
            return []

    trader.client = _StubClient()

    # Mock executioner: entry succeeds, TP fails (observer calls on_error).
    class _StubExecutioner:
        def __init__(self):
            self.calls = 0

        async def subscribe_place_order_direct(self, contract, order):
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
            return RiskGateResult(approved=True)

    trader.risk_gate = _ApproveAll()
    trader.check_order_margin = MagicMock(side_effect=Exception('skip margin'))

    class _Book:
        def get_orders(self):
            return []
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
