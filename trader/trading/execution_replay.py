"""Replay IB's available recent execution history without inventing outcomes.

IB does not emit execDetailsEvent for requested historical executions. Returned
fills therefore need explicit ingestion, including fills whose order is no
longer present in the open/completed-order response. IB's history window is
bounded: absence is never proof an older ambiguous intent did not execute.
"""
from __future__ import annotations

import asyncio

from ib_async.order import Order, OrderStatus, Trade


async def replay_execution_history(ib, tracker, timeout: float = 5.0) -> int:
    tracker.begin_replay()
    completed = await asyncio.wait_for(ib.reqCompletedOrdersAsync(False), timeout)
    fills = await asyncio.wait_for(ib.reqExecutionsAsync(), timeout)
    trades = list(ib.trades()) + list(completed)
    completed_ids = {id(trade) for trade in completed}
    by_perm = {trade.order.permId: trade for trade in trades if trade.order.permId}
    by_order = {(trade.order.clientId, trade.order.orderId): trade for trade in trades if trade.order.orderId}
    seen = set()
    for fill in fills:
        execution = fill.execution
        trade = (by_perm.get(execution.permId) or
                 by_order.get((execution.clientId, execution.orderId)))
        if trade is None:
            # A real fill supplies ownership evidence, but no final order
            # status. Keep its outcome unknown until the broker resolves it.
            trade = Trade(fill.contract, Order(
                orderId=execution.orderId, clientId=execution.clientId,
                permId=execution.permId, account=execution.acctNumber,
                orderRef=execution.orderRef,
                action={'BOT': 'BUY', 'SLD': 'SELL'}.get(execution.side, execution.side)),
                OrderStatus(status='Unknown'))
        tracker.on_execution(trade, fill, completed=(id(trade) in completed_ids
            or trade.orderStatus.status in {'Filled', 'Cancelled', 'ApiCancelled', 'Inactive', 'Unknown'}))
        seen.add(id(trade))
    for trade in trades:
        if id(trade) in seen:
            continue
        historical = (id(trade) in completed_ids
                      or trade.orderStatus.status in {'Filled', 'Cancelled', 'ApiCancelled', 'Inactive', 'Unknown'})
        for fill in trade.fills:
            tracker.on_execution(trade, fill, completed=historical)
        if not trade.fills:
            tracker.on_trade(trade, completed=historical)
    return len(fills)
