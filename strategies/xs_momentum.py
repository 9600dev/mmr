"""Cross-sectional 12-1 momentum — long the winners, short the losers.

THE SIGNAL AND WHY THIS ONE
    Jegadeesh-Titman: rank the universe by its return over the last 12 months
    EXCLUDING the most recent month, go long the top slice and short the
    bottom. The one-month skip is not decoration — recent returns carry a
    short-horizon REVERSAL that runs against momentum, and including them
    dilutes the signal with its own opposite.

    Chosen because it is the only signal that survived the panel scan on this
    repo's own data (2026-07-28): IC 0.0181 at a one-day horizon over 2,154
    independent periods, t = 3.45, hit rate 54.5%. Just as importantly its
    rank turnover is 0.036 — the ordering barely moves day to day, which is
    what makes it plausibly tradeable after costs. Short-horizon reversal
    scored a comparable IC at four times the turnover.

WHAT IS BEING CLAIMED, AND WHAT IS NOT
    An IC of 0.018 is small. It says the ranking is slightly better than
    random, not that any individual name is predictable. The entire thesis is
    that a small edge applied across 500 names every day aggregates into
    something, and that only works if costs stay under the edge. Whether they
    do is precisely what the backtest is for — the IC cannot answer it, since
    a rank correlation knows nothing about spreads.

LOOKAHEAD
    `precompute_panel` uses only `shift`, so position i depends on rows
    [0..i-21] alone. Nothing here reads forward.
"""

from typing import Any, Dict, Optional

import pandas as pd

from trader.trading.strategy import Strategy


class XsMomentum(Strategy):

    LOOKBACK = 252          # ~12 months of trading days
    SKIP = 21               # ~1 month, the reversal window to exclude
    LONG_PCT = 0.2          # top quintile
    SHORT_PCT = 0.2         # bottom quintile
    MIN_NAMES = 20          # below this a "quintile" is a handful of names
    REBALANCE_EVERY = 5     # periods between rebalances; 1 = daily
    SHORT_ENABLED = 1       # 0 = long-only (int so it is CLI-tunable)

    def precompute_panel(self, panel: pd.DataFrame) -> Dict[str, Any]:
        close = panel['close']
        # Return from t-LOOKBACK to t-SKIP, known at t. Both ends are shifted,
        # so nothing here can see the future.
        momentum = close.shift(self.SKIP) / close.shift(self.LOOKBACK) - 1.0
        return {'momentum': momentum}

    def on_panel(
        self,
        panel: pd.DataFrame,
        state: Dict[str, Any],
        index: int,
    ) -> Optional[Dict[int, float]]:
        if index < self.LOOKBACK:
            return None            # warm-up: no instruction, NOT flatten
        if self.REBALANCE_EVERY > 1 and index % self.REBALANCE_EVERY:
            return None            # hold the existing book between rebalances

        row = state['momentum'].iloc[index].dropna()
        if len(row) < self.MIN_NAMES:
            return None

        ranked = row.sort_values()
        n_side = max(1, int(len(ranked) * self.LONG_PCT))
        longs = ranked.index[-n_side:]
        shorts = ranked.index[:max(1, int(len(ranked) * self.SHORT_PCT))]

        weights: Dict[int, float] = {}
        # Equal-weight within each leg. Not because it is optimal, but because
        # anything else adds a second thing being tested at the same time, and
        # then a failure cannot be attributed to the signal or the sizing.
        if self.SHORT_ENABLED:
            per_leg = 0.5
            for c in longs:
                weights[int(c)] = per_leg / len(longs)
            for c in shorts:
                weights[int(c)] = -per_leg / len(shorts)
        else:
            for c in longs:
                weights[int(c)] = 1.0 / len(longs)

        # Everything else is explicitly flat. A name that has left the top or
        # bottom slice must be closed, and omitting it would leave the old
        # position in place forever.
        for c in row.index:
            weights.setdefault(int(c), 0.0)
        return weights
