"""The RPC wrapper's placement deadline covers the chokepoint's documented waits.

``place_order_simple`` bounded a path that can legitimately take
8s (own reduction cancel) + 8s (broker receipt) + 5s (submission audit) in a
flat 10s ``wait_for`` and reported "timed out" for orders that were then
placed. The bound now derives from those waits, stays under the SDK's 30s RPC
timeout, and the message says the order MAY have been placed and names the
reconciliation tools.
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
import reactivex as rx
from ib_async import Contract

from trader.messaging.trader_service_api import TraderServiceApi
from trader.trading.executioner import TradeExecutioner


def test_deadline_derives_from_the_documented_waits():
    deadline = TraderServiceApi._placement_deadline_s()
    assert deadline == TradeExecutioner.placement_deadline_s()
    assert deadline > (TradeExecutioner.CANCEL_WAIT_TIMEOUT_S + TradeExecutioner.RECEIPT_TIMEOUT_S
                       + TradeExecutioner.AUDIT_TIMEOUT_S) == 21.0
    assert deadline < 30.0, 'must stay under the SDK RPC client timeout'


@pytest.mark.asyncio
async def test_timeout_says_the_order_may_have_been_placed_and_names_the_tools(monkeypatch):
    trader = MagicMock()
    trader.require_proposal_approval = False
    trader.place_order_simple = AsyncMock(return_value=rx.never())
    api = TraderServiceApi(trader)
    monkeypatch.setattr(TraderServiceApi, '_placement_deadline_s', staticmethod(lambda: 0.05))

    result = await api.place_order_simple(Contract(conId=1, symbol='AMD'), 'SELL', None, 1, None,
                                          market_order=True, client_intent_id='slow-close')

    assert not result.is_success()
    assert result.error.startswith('UNKNOWN:')
    assert 'MAY have been placed' in result.error
    assert 'mmr reservations' in result.error and 'mmr execution-snapshot' in result.error
    assert 'slow-close' in result.error
    assert 'Do not resend' in result.error
