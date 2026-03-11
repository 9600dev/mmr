from trader.trading.strategy import Signal, Strategy, StrategyState
from typing import Optional

import pandas as pd


class Global(Strategy):
    def __init__(self):
        super().__init__()

    def on_prices(self, prices: pd.DataFrame) -> Optional[Signal]:
        return None

    def on_error(self, error):
        return super().on_error(error)
