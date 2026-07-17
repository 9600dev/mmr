"""G6 end-to-end validation strategy — NOT a trading strategy.

Emits a BUY signal on every bar with ``max_hold_bars=2``. Used once, on paper,
to prove the signal → proposal → order → fill → attribution → time-exit-close
pipeline live. The auto-executor's no-pyramiding rule means only the first
BUY opens; the position closes itself two bars later via the time exit; the
per-(strategy, conid) cooldown then keeps it flat for 5 minutes.

Deploy briefly, watch one open/close cycle, then disable and remove.
"""
from typing import Optional

import pandas as pd

from trader.objects import Action
from trader.trading.strategy import Signal, Strategy


class G6Validation(Strategy):
    MIN_BARS = 2

    def on_prices(self, prices: pd.DataFrame) -> Optional[Signal]:
        if prices is None or len(prices) < self.MIN_BARS:
            return None
        return Signal(
            source_name=self.name,
            action=Action.BUY,
            probability=0.60,
            risk=0.40,
            max_hold_bars=2,
        )
