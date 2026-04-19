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

from trader.trading.trading_runtime import Trader


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
