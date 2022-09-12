from trader.objects import Action
from trader.trading.strategy import Signal, Strategy
from typing import Optional, Tuple

import pandas as pd
import vectorbt as vbt


class SMICrossOver(Strategy):
    def __init__(
        self,
    ):
        super().__init__()

    def pre_condition(self) -> bool:
        if not super().pre_condition():
            return False

        return True

    def post_condition(self) -> bool:
        if not super().post_condition():
            return False

        return True

    def install(self) -> bool:
        return True

    def signals(self, open_price: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
        fast_ma = vbt.MA.run(open_price, 10)
        slow_ma = vbt.MA.run(open_price, 50)
        entries = fast_ma.ma_crossed_above(slow_ma)  # type: ignore
        exits = fast_ma.ma_crossed_below(slow_ma)  # type: ignore
        return (entries, exits)

    def on_next(self, prices: pd.DataFrame) -> Optional[Signal]:
        result = self.signals(prices)
        if result[0][-1] == 1:
            return Signal(Action.BUY, 0.0)
        else:
            return None
