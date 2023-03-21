from ib_insync.contract import Contract
from ib_insync.objects import PnLSingle, PortfolioItem, Position
from ib_insync.order import Order, Trade
from ib_insync.ticker import Ticker
from reactivex.abc import DisposableBase
from reactivex.disposable import Disposable
from reactivex.observer import Observer
from trader.common.logging_helper import setup_logging
from trader.common.reactivex import SuccessFail, SuccessFailEnum
from trader.data.data_access import PortfolioSummary, SecurityDefinition
from trader.data.universe import Universe
from trader.messaging.clientserver import RPCHandler
from trader.trading.strategy import Strategy, StrategyMetadata, StrategyState
from typing import Dict, List, Optional, Tuple, Union

import trader.strategy.strategy_runtime as runtime


logging = setup_logging(module_name='strategy_service_api')


class StrategyServiceApi(RPCHandler):
    def __init__(self, strategy_runtime):
        self.strategy: runtime.StrategyRuntime = strategy_runtime

    @RPCHandler.rpcmethod
    def enable_strategy(self, strategy_meta: StrategyMetadata) -> SuccessFail[StrategyState]:
        try:
            # find the strategy
            strategy = self.strategy.get_strategy(strategy_meta.name)
            if strategy:
                state = self.strategy.enable_strategy(strategy)
                return SuccessFail.success(state)
            else:
                return SuccessFail.fail(error='Strategy not found or error')
        except Exception as ex:
            return SuccessFail.fail(exception=ex)

    @RPCHandler.rpcmethod
    def disable_strategy(self, strategy_meta: StrategyMetadata) -> SuccessFail[StrategyState]:
        try:
            # find the strategy
            strategy = self.strategy.get_strategy(strategy_meta.name)
            if strategy:
                state = self.strategy.enable_strategy(strategy)
                return SuccessFail.success(obj=state)
            else:
                return SuccessFail.fail(error='Strategy not found or error')
        except Exception as ex:
            return SuccessFail.fail(exception=ex)

    @RPCHandler.rpcmethod
    def get_strategies(self) -> List[StrategyMetadata]:
        return [StrategyMetadata.from_strategy(strategy) for strategy in self.strategy.get_strategies()]
