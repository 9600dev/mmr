from trader.trading.strategy import Strategy
from typing import List


class TradeExecutioner():
    def __init__(
        self
    ):
        self.strategies: List[Strategy] = []

    def enable_strategy(self, strategy: Strategy):
        self.strategies.append(strategy)


