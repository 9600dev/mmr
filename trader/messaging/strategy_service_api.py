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
from trader.trading.strategy import Strategy, StrategyState
from typing import Dict, List, Optional, Tuple, Union

import asyncio
import trader.strategy.strategy_runtime as runtime


logging = setup_logging(module_name='strategy_service_api')


class StrategyServiceApi(RPCHandler):
    def __init__(self, strategy_runtime):
        self.strategy: runtime.StrategyRuntime = strategy_runtime

    # json can't encode Dict's with keys that aren't primitive and hashable
    # so we often have to convert to weird containers like List[Tuple]
    @RPCHandler.rpcmethod
    def enable_strategy(self, name: str) -> StrategyState:
        return self.strategy.enable_strategy(name)

    @RPCHandler.rpcmethod
    def disable_strategy(self, name: str) -> StrategyState:
        return self.strategy.disable_strategy(name)

    @RPCHandler.rpcmethod
    def list_strategies(self) -> Dict[str, List[int]]:
        result: Dict[str, List[int]] = {}
        for conid, strategy_list in self.strategy.strategies.items():
            for strategy in strategy_list:
                if strategy.name in result:
                    result[strategy.name].append(conid)
                else:
                    result[strategy.name] = [conid]
        return result
