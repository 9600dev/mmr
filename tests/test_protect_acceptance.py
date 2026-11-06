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
