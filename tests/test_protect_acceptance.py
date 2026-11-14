"""`mmr protect` must never report success while leaving a position naked.

Field incident 2026-11-05 (paper, F 1 sh): `protect F --trail 5` displaced the
working protective stop (reduction coordination cancels the operator's own
competing reduction), the replacement TRAIL was then rejected by IB with 321
"Missing order exchange" — the portfolio-cached contract had a blank exchange —
and the CLI printed success. Pinned here:

  * a blank-exchange stock contract is routed SMART at the chokepoint;
  * an unroutable contract is refused BEFORE any competing order is cancelled;
  * a protective order is only reported placed once the broker accepts it;
  * a rejected replacement restores the displaced protection and fails loudly.
"""
import datetime as dt

import pytest
import reactivex as rx
from ib_async import Stock
from ib_async.objects import TradeLogEntry
from ib_async.order import OrderStatus, Trade

from review.test_review_order_contract import _coordinated_trader, _stock
from trader.trading import order_acceptance as oa


def _log(status, code, msg='m'):
    return TradeLogEntry(dt.datetime.now(dt.timezone.utc), status, msg, code)


def _trade(status, log=()):
    t = Trade(Stock('X', 'SMART', 'USD'), None, OrderStatus(status=status))
    t.log = list(log)
    return t


class TestClassify:
    def test_working_states_accept(self):
        for s in ('Submitted', 'PreSubmitted', 'Filled'):
            assert oa.classify(_trade(s))[0] == oa.ACCEPTED

    def test_dead_states_reject(self):
        for s in ('Cancelled', 'ApiCancelled', 'Inactive'):
            assert oa.classify(_trade(s))[0] == oa.REJECTED

    def test_hard_validation_error_on_new_order_rejects(self):
        verdict, reason = oa.classify(_trade('ValidationError', [
            _log('ValidationError', 321, "Error validating request: Missing order exchange")]))
        assert verdict == oa.REJECTED and 'Missing order exchange' in reason

    def test_informational_warning_alone_is_unconfirmed(self):
        t = _trade('ValidationError', [_log('ValidationError', 2109, 'Outside RTH ignored')])
        assert oa.classify(t)[0] == oa.UNCONFIRMED

    def test_warning_then_working_accepts(self):
        t = _trade('PreSubmitted', [_log('ValidationError', 2109), _log('PreSubmitted', 0)])
        assert oa.classify(t)[0] == oa.ACCEPTED

    @pytest.mark.asyncio
    async def test_await_times_out_unconfirmed(self):
        verdict, _ = await oa.await_acceptance(_trade('PendingSubmit'), timeout=0.1, poll=0.01)
        assert verdict == oa.UNCONFIRMED


def _reject_when(trader, predicate):
    """Make the fake broker reject (321, as IB did) orders matching predicate."""
    original = trader.client.subscribe_place_order.side_effect

    async def place(contract, order):
        obs = await original(contract, order)
        if predicate(contract, order):
            trade = next(t for t in trader.placed if t.order.orderId == order.orderId)
            trade.orderStatus.status = 'ValidationError'
            trade.orderStatus.remaining = 0
            trade.log.append(_log('ValidationError', 321,
                                  "Error validating request.-'PlaceOrder(3)' : cause - Missing order exchange"))
        return obs

    trader.client.subscribe_place_order.side_effect = place


def _fast(trader):
    trader.ACCEPTANCE_TIMEOUT_S = 0.2
    return trader


@pytest.mark.asyncio
async def test_rejected_replacement_restores_the_displaced_stop(tmp_path):
    trader = _fast(_coordinated_trader(tmp_path, held=100))
    stop = await trader.place_standalone_order(_stock(), 'SELL', 100, 'STP', aux_price=8, order_ref='proposal')
    assert stop.is_success(), stop.error
    _reject_when(trader, lambda c, o: o.orderType == 'TRAIL')

    result = await trader.place_standalone_order(_stock(), 'SELL', 100, 'TRAIL', trailing_percent=5)

    assert not result.is_success()
    assert 'REJECTED' in result.error and 'Missing order exchange' in result.error
    assert 're-placed' in result.error and 'UNPROTECTED' not in result.error
    working = [t for t in trader.placed if t.orderStatus.status == 'Submitted']
    assert len(working) == 1, 'exactly one working protective order after the failed swap'
    restored = working[0]
    assert restored.order.orderType == 'STP' and restored.order.auxPrice == 8
    assert restored.order.totalQuantity == 100
    assert restored.order.orderId != stop.obj.order.orderId


@pytest.mark.asyncio
async def test_rejected_replacement_and_failed_restore_says_unprotected(tmp_path):
    trader = _fast(_coordinated_trader(tmp_path, held=100))
    await trader.place_standalone_order(_stock(), 'SELL', 100, 'STP', aux_price=8, order_ref='proposal')
    _reject_when(trader, lambda c, o: True)   # broker now rejects everything
    result = await trader.place_standalone_order(_stock(), 'SELL', 100, 'TRAIL', trailing_percent=5)
    assert not result.is_success()
    assert 'UNPROTECTED' in result.error


@pytest.mark.asyncio
async def test_accepted_replacement_still_succeeds(tmp_path):
    trader = _fast(_coordinated_trader(tmp_path, held=100))
    await trader.place_standalone_order(_stock(), 'SELL', 100, 'STP', aux_price=8, order_ref='proposal')
    result = await trader.place_standalone_order(_stock(), 'SELL', 100, 'TRAIL', trailing_percent=5)
    assert result.is_success(), result.error
    working = [t for t in trader.placed if t.orderStatus.status == 'Submitted']
    assert [t.order.orderType for t in working] == ['TRAIL']


@pytest.mark.asyncio
async def test_blank_exchange_position_contract_is_routed_smart(tmp_path):
    trader = _fast(_coordinated_trader(tmp_path, held=100))
    portfolio_contract = Stock('AUDIT', '', 'USD', conId=100, primaryExchange='NYSE')
    result = await trader.place_standalone_order(portfolio_contract, 'SELL', 100, 'STP', aux_price=8)
    assert result.is_success(), result.error
    assert trader.placed[-1].contract.exchange == 'SMART'
    assert trader.placed[-1].contract.primaryExchange == 'NYSE'


@pytest.mark.asyncio
async def test_blank_exchange_non_us_listing_routes_to_primary_not_smart(tmp_path):
    """Some SEHK listings reject SMART; the primary exchange is always valid."""
    trader = _fast(_coordinated_trader(tmp_path, held=100))
    hk = Stock('AUDIT', '', 'HKD', conId=100, primaryExchange='SEHK')
    result = await trader.place_standalone_order(hk, 'SELL', 100, 'STP', aux_price=8)
    assert result.is_success(), result.error
    assert trader.placed[-1].contract.exchange == 'SEHK'


@pytest.mark.asyncio
async def test_unroutable_contract_refused_before_cancelling_protection(tmp_path):
    trader = _fast(_coordinated_trader(tmp_path, held=100))
    await trader.place_standalone_order(_stock(), 'SELL', 100, 'STP', aux_price=8, order_ref='proposal')
    unroutable = Stock('AUDIT', '', 'USD', conId=100)
    result = await trader.place_standalone_order(unroutable, 'SELL', 100, 'TRAIL', trailing_percent=5)
    assert not result.is_success()
    trader.client.ib.cancelOrder.assert_not_called()
    assert trader.placed[0].orderStatus.status == 'Submitted'


# --------------------------------------------------------------------------
# AUDIT_2026-11-14 B1.9: the displaced stop was put back on ONE failure path
# (IB rejecting the replacement). A replacement that failed any other way left
# the position unprotected, and the error did not say the old stop was gone.
# --------------------------------------------------------------------------

def _fail_placement_when(trader, predicate, error):
    """Make the fake broker raise ``error`` for orders matching predicate."""
    original = trader.client.subscribe_place_order.side_effect

    async def place(contract, order):
        if predicate(contract, order):
            raise error
        return await original(contract, order)

    trader.client.subscribe_place_order.side_effect = place


def _working(trader):
    return [t for t in trader.placed if t.orderStatus.status == 'Submitted']


@pytest.mark.asyncio
async def test_a_send_without_a_receipt_reports_the_cancelled_stop_and_places_nothing_beside_it(tmp_path):
    """The broker call failed after coordination had cancelled the old stop.
    Whether the replacement went out is unknown, so nothing is re-placed (a
    restored stop would cancel the replacement if it IS working) — but the
    failure used to read as a bare "UNKNOWN: ... no receipt", with no word that
    the working stop was gone."""
    trader = _fast(_coordinated_trader(tmp_path, held=100))
    stop = await trader.place_standalone_order(_stock(), 'SELL', 100, 'STP', aux_price=8, order_ref='proposal')
    assert stop.is_success(), stop.error
    _fail_placement_when(trader, lambda c, o: o.orderType == 'TRAIL', ValueError('broker refused the request'))

    result = await trader.place_standalone_order(_stock(), 'SELL', 100, 'TRAIL', trailing_percent=5)

    assert not result.is_success() and 'UNKNOWN' in result.error
    assert f'[{stop.obj.order.orderId}] were CANCELLED' in result.error and 'NOT re-placed' in result.error
    assert 'broker refused the request. Working exit' in result.error        # one sentence each
    assert _working(trader) == []


@pytest.mark.asyncio
async def test_a_replacement_that_provably_was_not_sent_restores_the_displaced_stop(tmp_path):
    """A clean failure after the old stop was cancelled (nothing reached the
    broker): the stop goes back, exactly as it does when IB rejects."""
    trader = _fast(_coordinated_trader(tmp_path, held=100))
    stop = await trader.place_standalone_order(_stock(), 'SELL', 100, 'STP', aux_price=8, order_ref='proposal')
    assert stop.is_success(), stop.error
    stop_trade = stop.obj
    original = trader.executioner.subscribe_place_order_direct

    async def refuse_the_replacement(approved):
        if approved.order.orderType != 'TRAIL':
            return await original(approved)
        # What coordination does before a later, clean refusal of the same order.
        stop_trade.orderStatus.status = 'Cancelled'
        trader.order_tracker.on_trade(stop_trade)                 # the broker's cancel confirmation
        trader.executioner._displaced = [(stop_trade.contract, stop_trade.order, 100.0)]
        return rx.throw(RuntimeError('order journal refused the reservation'))

    trader.executioner.subscribe_place_order_direct = refuse_the_replacement

    result = await trader.place_standalone_order(_stock(), 'SELL', 100, 'TRAIL', trailing_percent=5)

    assert not result.is_success() and 'order journal refused the reservation' in result.error
    assert 're-placed' in result.error and 'UNPROTECTED' not in result.error and 'NOT re-placed' not in result.error
    working = _working(trader)
    assert [(t.order.orderType, t.order.auxPrice, t.order.totalQuantity) for t in working] == [('STP', 8, 100)]
    assert working[0].order.orderId != stop_trade.order.orderId


@pytest.mark.asyncio
async def test_a_failure_with_nothing_displaced_reads_as_before(tmp_path):
    trader = _fast(_coordinated_trader(tmp_path, held=100))
    _fail_placement_when(trader, lambda c, o: True, ValueError('broker refused the request'))
    result = await trader.place_standalone_order(_stock(), 'SELL', 100, 'STP', aux_price=8)
    assert not result.is_success()
    assert 'CANCELLED' not in result.error and 'UNPROTECTED' not in result.error and 're-placed' not in result.error


class TestAccountForDisplaced:
    """The reporting rule on its own: restore only when the replacement
    provably is not working; otherwise report, never re-place."""

    @staticmethod
    def _runtime(restored=(), lost=()):
        from types import SimpleNamespace
        from trader.trading.trading_runtime import Trader

        calls = []

        async def restore(displaced):
            calls.append(list(displaced))
            return list(restored), list(lost)

        runtime = SimpleNamespace(_restore_displaced=restore)
        runtime._account_for_displaced = Trader._account_for_displaced.__get__(runtime)
        return runtime, calls

    @staticmethod
    def _displaced(order_id=41):
        from types import SimpleNamespace
        return [(_stock(), SimpleNamespace(orderId=order_id), 100.0)]

    @pytest.mark.asyncio
    async def test_nothing_displaced_is_no_text_and_no_restore(self):
        runtime, calls = self._runtime()
        assert await runtime._account_for_displaced([], 'X', restore=True) == ''
        assert await runtime._account_for_displaced([], 'X', restore=False) == ''
        assert calls == []

    @pytest.mark.asyncio
    async def test_unknown_outcome_reports_and_never_replaces(self):
        runtime, calls = self._runtime(restored=[99])
        text = await runtime._account_for_displaced(self._displaced(41), 'X', restore=False)
        assert calls == []
        assert '[41]' in text and 'NOT re-placed' in text and 'mmr orders' in text and ' X ' in text

    @pytest.mark.asyncio
    async def test_restore_reports_both_what_came_back_and_what_did_not(self):
        runtime, calls = self._runtime(restored=[99], lost=[42])
        text = await runtime._account_for_displaced(self._displaced(41) + self._displaced(42), 'X', restore=True)
        assert len(calls) == 1 and len(calls[0]) == 2
        assert 're-placed as [99]' in text and '[42] WAS CANCELLED AND COULD NOT BE RESTORED' in text
        assert 'X IS UNPROTECTED' in text


# --------------------------------------------------------------------------
# The manual close path (`mmr close` / `sell`): a close displaces the
# operator's own stop. When the close then failed, the reply named the failure
# and nothing else — position still held, stop gone, operator not told.
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_a_failed_manual_close_says_the_stop_it_cancelled_is_gone(tmp_path):
    from trader.messaging.trader_service_api import TraderServiceApi

    trader = _fast(_coordinated_trader(tmp_path, held=100))
    stop = await trader.place_standalone_order(_stock(), 'SELL', 100, 'STP', aux_price=8)
    assert stop.is_success(), stop.error
    _fail_placement_when(trader, lambda c, o: o.orderType == 'MKT', ValueError('broker refused the request'))
    try:
        result = await TraderServiceApi(trader).place_order_simple(
            _stock(), 'SELL', equity_amount=None, quantity=100, limit_price=None, market_order=True)
    finally:
        trader.order_tracker.close(timeout=1)

    assert not result.is_success()
    assert 'broker refused the request' in result.error
    assert f'[{stop.obj.order.orderId}] were CANCELLED' in result.error
    assert 'NOT re-placed' in result.error and 'mmr orders' in result.error
    assert _working(trader) == []                # and it did not put a second reduction beside an unknown one


@pytest.mark.asyncio
async def test_a_successful_manual_close_still_reports_the_stop_as_a_warning(tmp_path):
    from trader.messaging.trader_service_api import TraderServiceApi

    trader = _fast(_coordinated_trader(tmp_path, held=100))
    await trader.place_standalone_order(_stock(), 'SELL', 100, 'STP', aux_price=8)
    try:
        result = await TraderServiceApi(trader).place_order_simple(
            _stock(), 'SELL', equity_amount=None, quantity=100, limit_price=None, market_order=True)
    finally:
        trader.order_tracker.close(timeout=1)

    assert result.is_success(), result.error
    assert result.warnings and 'cancelled working STP' in result.warnings[0]


@pytest.mark.asyncio
async def test_a_failed_manual_close_that_displaced_nothing_reads_as_before(tmp_path):
    from trader.messaging.trader_service_api import TraderServiceApi

    trader = _fast(_coordinated_trader(tmp_path, held=100))
    _fail_placement_when(trader, lambda c, o: True, ValueError('broker refused the request'))
    try:
        result = await TraderServiceApi(trader).place_order_simple(
            _stock(), 'SELL', equity_amount=None, quantity=100, limit_price=None, market_order=True)
    finally:
        trader.order_tracker.close(timeout=1)

    assert not result.is_success() and 'broker refused the request' in str(result.error)
    assert 'CANCELLED' not in str(result.error) and 're-placed' not in str(result.error)
