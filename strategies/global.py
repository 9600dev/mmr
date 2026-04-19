from trader.trading.strategy import Signal, Strategy, StrategyState
from typing import Optional

import pandas as pd


class Global(Strategy):
    """No-op placeholder strategy — subscribes to the whole portfolio so
    trader_service sees live market data on every held position, without
    generating any trading signals itself. Useful as a universal subscriber."""

    def __init__(self):
        super().__init__()

    def on_prices(self, prices: pd.DataFrame) -> Optional[Signal]:
        return None

    def on_error(self, error):
        return super().on_error(error)
