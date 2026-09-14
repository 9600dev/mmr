from ib_async.contract import Contract
from ib_async.objects import PnLSingle, PortfolioItem, Position
from ib_async.order import Order, Trade
from ib_async.ticker import Ticker
from reactivex.abc import DisposableBase
from reactivex.disposable import Disposable
from reactivex.observer import Observer
from trader.common.logging_helper import setup_logging
from trader.common.reactivex import SuccessFail, SuccessFailEnum
from trader.data.data_access import PortfolioSummary, SecurityDefinition
from trader.data.universe import Universe
from trader.messaging.clientserver import RPCHandler, rpcmethod
from trader.trading.strategy import Strategy, StrategyConfig, StrategyState
from typing import Dict, List, Optional, Tuple, Union

import trader.strategy.strategy_runtime as runtime
import asyncio


logging = setup_logging(module_name='strategy_service_api')


class StrategyServiceApi(RPCHandler):
    def __init__(self, strategy_runtime):
        self.strategy: runtime.StrategyRuntime = strategy_runtime

    @rpcmethod
    async def enable_strategy(self, name: str) -> SuccessFail[StrategyState]:
        try:
            # find the strategy
            strategy = self.strategy.get_strategy(name)
            if strategy:
                state = await asyncio.to_thread(self.strategy.enable_strategy, name)
                if state not in (StrategyState.RUNNING, StrategyState.WAITING_HISTORICAL_DATA):
                    return SuccessFail.fail(error='Strategy could not be enabled; inspect runtime validation errors')
                return SuccessFail.success(state)
            else:
                return SuccessFail.fail(error='Strategy not found or error')
        except Exception as ex:
            return SuccessFail.fail(error=str(ex) or type(ex).__name__, exception=ex)

    @rpcmethod
    async def disable_strategy(self, name: str) -> SuccessFail[StrategyState]:
        try:
            # find the strategy
            strategy = self.strategy.get_strategy(name)
            if strategy:
                state = await asyncio.to_thread(self.strategy.disable_strategy, name)
                return SuccessFail.success(obj=state)
            else:
                return SuccessFail.fail(error='Strategy not found or error')
        except Exception as ex:
            return SuccessFail.fail(error=str(ex) or type(ex).__name__, exception=ex)

    @rpcmethod
    def get_strategies(self) -> List[StrategyConfig]:
        return [StrategyConfig.from_strategy(strategy) for strategy in self.strategy.get_strategies()]

    @rpcmethod
    def runtime_status(self) -> dict:
        """Pipeline-health snapshot (strategy states, ticks_60s per conId,
        dispatched-bar ages, open auto-exec positions). Plain primitives so
        `mmr verify` and container healthchecks can consume it without any
        dataclass deserialization."""
        return self.strategy.runtime_status()

    @rpcmethod
    async def adopt_legacy_holding(self, strategy: str, conid: int,
                                   avg_cost: Optional[float] = None) -> SuccessFail[dict]:
        """Operator-attested ownership for a holding attributed before ownership epochs."""
        executor = getattr(self.strategy, 'auto_executor', None)
        if executor is None:
            return SuccessFail.fail(error='auto-executor is not running in this strategy service')
        try:
            adopted = await asyncio.to_thread(executor.adopt_legacy_holding, strategy, int(conid), avg_cost)
            return SuccessFail.success(obj=adopted)
        except Exception as ex:
            return SuccessFail.fail(error=str(ex) or type(ex).__name__, exception=ex)

    @rpcmethod
    async def list_execution_intents(self, strategy: Optional[str] = None, conid: Optional[int] = None,
                                     active_only: bool = True) -> List[dict]:
        """Executor intents (the SQLite intent journal) as JSON-safe rows.

        Each row carries kind/status/order ids/proposal id, the payload flags
        that matter (attribution_unresolved, never_submitted, operator_resolved,
        deferred_*) and a ``blocking`` sentence saying what the intent holds
        back. Read-only; served even while the worker is busy. An executor-less
        service returns no rows rather than an error so `mmr strategies
        intents` degrades to "nothing to show".
        """
        executor = getattr(self.strategy, 'auto_executor', None)
        if executor is None:
            return []
        return await asyncio.to_thread(executor.list_execution_intents, strategy,
                                       None if conid is None else int(conid), bool(active_only))

    @rpcmethod
    async def resolve_execution_intent(self, intent_id: str, reason: str) -> SuccessFail[dict]:
        """Operator resolution of an intent that has no broker evidence.

        An explicit human act, never invoked automatically. Runs on the
        executor worker; refused (success False, reason in ``error``) when the
        intent recorded order ids not proven terminal or the intent-scoped
        broker snapshot still shows matching orders.
        """
        executor = getattr(self.strategy, 'auto_executor', None)
        if executor is None:
            return SuccessFail.fail(error='auto-executor is not running in this strategy service')
        if not str(reason or '').strip():
            return SuccessFail.fail(error='operator resolution requires a non-empty reason')
        try:
            resolved = await asyncio.to_thread(executor.resolve_execution_intent, str(intent_id), str(reason))
            return SuccessFail.success(obj=resolved)
        except Exception as ex:
            return SuccessFail.fail(error=str(ex) or type(ex).__name__, exception=ex)

    @rpcmethod
    async def reload_strategies(self) -> SuccessFail[List[StrategyConfig]]:
        try:
            # Explicit reload is an application acknowledgment, independent
            # of filesystem timestamp resolution or periodic reconciliation.
            await asyncio.to_thread(self.strategy.config_loader, self.strategy.strategy_config_file)
            await self.strategy._reconcile()
            return SuccessFail.success(
                [StrategyConfig.from_strategy(s) for s in self.strategy.get_strategies()]
            )
        except Exception as ex:
            return SuccessFail.fail(error=str(ex) or type(ex).__name__, exception=ex)
