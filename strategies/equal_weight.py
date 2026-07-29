"""Equal-weight buy-and-hold of the whole panel - the benchmark, not a strategy.

Exists so a cross-sectional result can be compared against simply OWNING the
universe, using the identical execution path: same fills at t+1's open, same
slippage, same commission, same marking. A benchmark computed a different way
would differ from the strategy for reasons that have nothing to do with the
signal.

Without this, a long-only momentum book reporting 18.8% CAGR is
uninterpretable. If the universe itself compounded at 18%, the signal added
nothing and the number is beta plus survivorship.
"""

from typing import Any, Dict, Optional

import pandas as pd

from trader.trading.strategy import Strategy


class EqualWeight(Strategy):

    REBALANCE_EVERY = 21     # monthly, to bound turnover
    WARMUP = 252             # match the momentum strategy's start date exactly

    def precompute_panel(self, panel: pd.DataFrame) -> Dict[str, Any]:
        return {'close': panel['close']}

    def on_panel(self, panel, state, index) -> Optional[Dict[int, float]]:
        # Same warm-up as the strategy being benchmarked, so both books start
        # on the same bar and cover the same period.
        if index < self.WARMUP:
            return None
        if self.REBALANCE_EVERY > 1 and index % self.REBALANCE_EVERY:
            return None
        row = state['close'].iloc[index].dropna()
        if len(row) < 2:
            return None
        w = 1.0 / len(row)
        return {int(c): w for c in row.index}
