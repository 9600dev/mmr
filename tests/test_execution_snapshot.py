"""Broker replay completeness is separate from local journal durability."""
import datetime as dt
from unittest.mock import AsyncMock, MagicMock

import pytest
from ib_async.objects import CommissionReport, Execution, Fill
from ib_async.order import Order, OrderStatus, Trade

from review.test_review_order_contract import _coordinated_trader, _stock


@pytest.mark.asyncio
async def test_open_orders_alone_do_not_prove_execution_history_complete(tmp_path):
    trader = _coordinated_trader(tmp_path)
    trader._execution_history_ready = False
    trader.client.ib.reqCompletedOrdersAsync = AsyncMock(side_effect=ConnectionError('replay unavailable'))
    snapshot = await trader.execution_snapshot()
    assert snapshot['positions_complete']
    assert snapshot['orders_complete']
    assert not snapshot['executions_complete']
    assert not snapshot['complete']
    assert not trader.order_tracker.health['healthy']
    trader.order_tracker.close()


@pytest.mark.asyncio
async def test_journal_receipt_failure_preserves_confirmed_positions_for_exits(tmp_path):
    trader = _coordinated_trader(tmp_path)
    trader.order_tracker.execution_receipts = MagicMock(side_effect=OSError('journal unreadable'))
    snapshot = await trader.execution_snapshot()
    assert snapshot['complete']
    assert snapshot['positions_complete']
    assert snapshot['positions'][0]['position'] == 100
    assert snapshot['account'] == trader.ib_account
    assert snapshot['account_confirmed']
    assert not snapshot['journal_healthy']
    assert not snapshot['execution_receipts_available']
    trader.order_tracker.close()


@pytest.mark.asyncio
async def test_configured_account_missing_from_broker_is_not_an_authoritative_empty_book(tmp_path):
    trader = _coordinated_trader(tmp_path)
    trader.client.ib.managedAccounts = lambda: ['different-account']
    snapshot = await trader.execution_snapshot()
    assert not snapshot['account_confirmed']
    assert not snapshot['positions_complete']
    assert not snapshot['complete']
    trader.order_tracker.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('has_completed_order', [True, False])
async def test_historical_execution_replay_ingests_partial_fill_without_live_event(tmp_path, has_completed_order):
    trader = _coordinated_trader(tmp_path)
    trader._execution_history_ready = False
    execution = Execution(execId='replayed-fill', orderId=17, clientId=7, permId=1717,
                          acctNumber=trader.ib_account, side='BOT', shares=40, cumQty=40,
                          price=10, avgPrice=10, orderRef='review_strategy|mmr:recovered-intent')
    fill = Fill(_stock(), execution, CommissionReport(), dt.datetime.now(dt.timezone.utc))
    trade = Trade(_stock(), Order(orderId=17, clientId=7, permId=1717,
                                  account=trader.ib_account, totalQuantity=100,
                                  filledQuantity=40,
                                  action='BUY', orderRef=execution.orderRef),
                  OrderStatus(status='Cancelled'))
    trader.client.ib.reqCompletedOrdersAsync = AsyncMock(return_value=[trade] if has_completed_order else [])
    trader.client.ib.reqExecutionsAsync = AsyncMock(return_value=[fill])
    trader.client.ib.trades = lambda: []
    assert await trader._replay_broker_executions()
    rows = trader.order_tracker.snapshot([17])
    assert len(rows) == 1
    assert rows[0]['filled'] == 40
    assert rows[0]['clientIntentId'] == 'recovered-intent'
    assert rows[0]['status'] == ('Cancelled' if has_completed_order else 'Unknown')
    assert trader.order_tracker.health['healthy']
    assert trader.order_tracker.execution_receipts()[0]['execId'] == 'replayed-fill'
    trader.order_tracker.close()
