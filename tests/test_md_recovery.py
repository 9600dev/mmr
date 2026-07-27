"""Error 10197 (competing live session) must self-heal, not wait for an operator.

Observed 2026-07-27: a mobile-app login with the trading username at 08:53 made
IB revoke market data on every subscription mid-session. The subscriptions went
silent, contracts_cache still held the dead observables so nothing re-requested,
and the feed stayed dark for 22 minutes until a manual container restart — while
two positions were held (protected only by their broker-side GTC stops, which is
the designed degraded mode, but 22 minutes of operator-dependent recovery is
not). A single fresh reqMktData recovered the feed instantly once the competing
session released the entitlement — which is exactly what the retry loop automates.
"""
import asyncio
import time

from ib_async import Contract

from trader.listeners.ibreactive import IBAIORx


def _rx():
    return IBAIORx('tcp://127.0.0.1', 4002, ib_client_id=99)


class TestRetryDecision:
    def test_idle_when_no_loss_recorded(self):
        rx = _rx()
        assert rx._md_retry_action() == 'idle'

    def test_resubscribe_while_dark(self):
        rx = _rx()
        rx._md_last_tick_at = time.time() - 10
        rx._md_lost_at = time.time()          # loss AFTER the last tick
        assert rx._md_retry_action() == 'resubscribe'

    def test_clear_once_ticks_flow_again(self):
        rx = _rx()
        rx._md_lost_at = time.time() - 10
        rx._md_last_tick_at = time.time()     # tick AFTER the loss
        assert rx._md_retry_action() == 'clear'

    def test_a_tick_before_the_loss_does_not_clear(self):
        """The trap: the feed was alive right up until the competing session
        took it. A stale pre-loss tick must not read as recovery."""
        rx = _rx()
        rx._md_last_tick_at = time.time() - 60
        rx._md_lost_at = time.time() - 30
        assert rx._md_retry_action() == 'resubscribe'


class TestHandlerMarksTheLoss:
    def test_10197_sets_the_loss_timestamp(self):
        rx = _rx()
        assert rx._md_lost_at is None
        asyncio.new_event_loop().run_until_complete(
            rx._IBAIORx__handle_error(1, 10197, 'competing live session', Contract()))
        assert rx._md_lost_at is not None

    def test_10197_starts_the_retry_task(self):
        rx = _rx()

        async def drive():
            await rx._IBAIORx__handle_error(1, 10197, 'competing live session', Contract())
            return rx._md_retry_task

        task = asyncio.new_event_loop().run_until_complete(drive())
        assert task is not None
        task.cancel()

    def test_other_errors_do_not_mark_a_loss(self):
        rx = _rx()
        asyncio.new_event_loop().run_until_complete(
            rx._IBAIORx__handle_error(1, 200, 'no security definition', Contract()))
        assert rx._md_lost_at is None


class TestResubscribeReissuesEveryCachedContract:
    def test_reissues_reqmktdata_per_cached_contract(self):
        rx = _rx()
        calls = []
        rx.ib.reqMktData = lambda **kw: calls.append(kw['contract'].conId)
        # call_event_subscriber_sync needs no live loop for a sync callable
        rx._contracts_source.call_event_subscriber_sync = (
            lambda fn, asend_result=False: fn())
        c1, c2 = Contract(conId=1), Contract(conId=2)
        rx.contracts_cache[c1] = object()
        rx.contracts_cache[c2] = object()
        assert rx._resubscribe_market_data() == 2
        assert sorted(calls) == [1, 2]

    def test_one_failing_contract_does_not_stop_the_rest(self):
        rx = _rx()
        calls = []

        def req(**kw):
            if kw['contract'].conId == 1:
                raise RuntimeError('boom')
            calls.append(kw['contract'].conId)

        rx.ib.reqMktData = req
        rx._contracts_source.call_event_subscriber_sync = (
            lambda fn, asend_result=False: fn())
        rx.contracts_cache[Contract(conId=1)] = object()
        rx.contracts_cache[Contract(conId=2)] = object()
        rx._resubscribe_market_data()
        assert calls == [2]

    def test_liveness_note_updates_the_timestamp(self):
        rx = _rx()
        before = rx._md_last_tick_at
        rx._note_market_data(set())
        assert rx._md_last_tick_at > before
