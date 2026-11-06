"""Protective child legs are GTC even when the entry is DAY.

Field finding 2026-11-05: `propose F BUY --market --stop-loss 11` placed the
protective STP child with TIF=DAY (inherited from the entry), so it would
expire at the close and leave the filled position unprotected next session.
"""
from types import SimpleNamespace

import pytest

from review.test_review_order_contract import _coordinated_trader as _base_trader, _stock
from trader.trading.risk_gate import RiskGate, RiskLimits


def _coordinated_trader(tmp_path, held=0):
    trader = _base_trader(tmp_path, held=held)
    trader.risk_gate = RiskGate(RiskLimits(), trader.event_store)
    ticker = SimpleNamespace(contract=_stock(), last=10.0, close=10.0, ask=10.01, bid=9.99)
    trader.client.get_snapshot.return_value = ticker
    trader.client.ib.tickers = lambda: [ticker]
    return trader


@pytest.mark.asyncio
@pytest.mark.parametrize('spec, child_type', [
    ({'order_type': 'MARKET', 'exit_type': 'STOP_LOSS', 'stop_loss_price': 8.0}, 'STP'),
    ({'order_type': 'LIMIT', 'limit_price': 10.0, 'exit_type': 'TRAILING_STOP',
      'trailing_stop_percent': 5.0}, 'TRAIL'),
    ({'order_type': 'LIMIT', 'limit_price': 10.0, 'exit_type': 'BRACKET',
      'take_profit_price': 12.0, 'stop_loss_price': 8.0}, 'STP'),
])
async def test_day_entry_gets_gtc_protective_children(tmp_path, spec, child_type):
    trader = _coordinated_trader(tmp_path, held=0)
    result = await trader.place_expressive_order(_stock(), 'BUY', 10, dict(spec, tif='DAY'),
                                                client_intent_id='tif-test')
    assert result.is_success(), result.error
    entry = trader.placed[0]
    children = trader.placed[1:]
    assert entry.order.tif == 'DAY'
    assert children and all(t.order.tif == 'GTC' for t in children)
    assert any(t.order.orderType == child_type for t in children)


@pytest.mark.asyncio
async def test_gtc_entry_children_stay_gtc(tmp_path):
    trader = _coordinated_trader(tmp_path, held=0)
    result = await trader.place_expressive_order(
        _stock(), 'BUY', 10, {'order_type': 'MARKET', 'exit_type': 'STOP_LOSS',
                              'stop_loss_price': 8.0, 'tif': 'GTC'}, client_intent_id='tif-gtc')
    assert result.is_success(), result.error
    assert [t.order.tif for t in trader.placed] == ['GTC', 'GTC']
