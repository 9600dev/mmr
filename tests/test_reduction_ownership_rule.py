"""An exit never displaces another owner's working reduction (review 2026-09-11).

``_coordinate_reduction`` used to cancel EVERY competing same-direction order
for a position-classified exit, including a strategy's disaster stop under a
manual close, and could then refuse the exit (cancel unconfirmed, inventory
unreadable, an unconfirmed earlier send still reserving capacity), leaving the
position naked. The ownership rule now applies to every exit reason: only the
caller's own working reductions may be displaced; a shortfall against orders
owned elsewhere is refused BEFORE any cancel is sent, as a DEFERRED, and the
foreign protection stays working.
"""
import pytest

from review.test_review_order_contract import _coordinated_trader, _stock


def _error_text(result):
    return f'{result.error or ""} {result.exception or ""}'


@pytest.mark.asyncio
async def test_manual_close_does_not_cancel_a_strategy_protective_stop(tmp_path):
    trader = _coordinated_trader(tmp_path, held=100)
    stop = await trader.place_standalone_order(_stock(), 'SELL', 100, 'STP', aux_price=8, order_ref='orb_audit')
    assert stop.is_success(), stop.error

    result = await trader.place_expressive_order(
        _stock(), 'SELL', 100, {'order_type': 'MARKET'}, algo_name='proposal', client_intent_id='manual-close')

    assert not result.is_success()
    assert 'DEFERRED' in _error_text(result) and 'orb_audit' in _error_text(result)
    trader.client.ib.cancelOrder.assert_not_called()
    assert [t.order.orderType for t in trader.placed] == ['STP']
    assert trader.placed[0].orderStatus.status == 'Submitted', 'the strategy stop is still protecting the position'


@pytest.mark.asyncio
async def test_operator_close_still_displaces_the_operators_own_stop(tmp_path):
    """mmr buy/sell stamp 'global', approve stamps 'proposal': both are the
    operator, and the operator may replace the operator's own working stop."""
    trader = _coordinated_trader(tmp_path, held=100)
    stop = await trader.place_standalone_order(_stock(), 'SELL', 100, 'STP', aux_price=8, order_ref='proposal')
    assert stop.is_success(), stop.error

    result = await trader.place_expressive_order(
        _stock(), 'SELL', 100, {'order_type': 'MARKET'}, algo_name='global', client_intent_id='operator-close')

    assert result.is_success(), result.error
    assert trader.client.ib.cancelOrder.call_count == 1
    assert trader.placed[0].orderStatus.status == 'Cancelled'
    assert trader.placed[-1].order.orderType == 'MKT' and trader.placed[-1].order.totalQuantity == 100


@pytest.mark.asyncio
async def test_strategy_close_still_displaces_its_own_protective(tmp_path):
    trader = _coordinated_trader(tmp_path, held=100)
    await trader.place_standalone_order(_stock(), 'SELL', 100, 'STP', aux_price=8, order_ref='orb_audit')

    result = await trader.place_expressive_order(
        _stock(), 'SELL', 100, {'order_type': 'MARKET'}, algo_name='orb_audit', client_intent_id='strategy-close')

    assert result.is_success(), result.error
    assert trader.client.ib.cancelOrder.call_count == 1
    assert trader.placed[-1].order.orderType == 'MKT' and trader.placed[-1].order.totalQuantity == 100


@pytest.mark.asyncio
async def test_manual_close_is_clamped_to_shares_not_reserved_elsewhere(tmp_path):
    trader = _coordinated_trader(tmp_path, held=100)
    await trader.place_standalone_order(_stock(), 'SELL', 60, 'STP', aux_price=8, order_ref='orb_audit')

    result = await trader.place_expressive_order(
        _stock(), 'SELL', 100, {'order_type': 'MARKET'}, algo_name='proposal', client_intent_id='partial-close')

    assert result.is_success(), result.error
    trader.client.ib.cancelOrder.assert_not_called()
    assert trader.placed[0].orderStatus.status == 'Submitted'
    assert trader.placed[-1].order.totalQuantity == 40, 'never a second executable claim on the 60 reserved shares'
