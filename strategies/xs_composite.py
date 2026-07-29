"""Multi-signal cross-sectional book, optionally sector-neutral.

WHY A COMPOSITE
    12-1 momentum alone scores IC 0.018 and converts to Sharpe ~0.31 after
    costs. That is the normal size of a real anomaly in liquid US equities,
    not a failure - and it is why books that work stack many weak,
    weakly-correlated signals instead of hunting for one strong one. Combining
    k signals of average IC c and average pairwise correlation rho gives
    roughly c * sqrt(k / (1 + (k-1) rho)).

    The correlation term does the work. Adding a second momentum variant buys
    almost nothing; adding short-horizon reversal, which is nearly orthogonal
    by construction, buys most of the available uplift.

WHY NEUTRALISE
    Equal-weighting within quantiles makes sector bets nobody chose - a
    momentum book over 2016-2026 was structurally long technology. Removing
    that does not add return, it removes variance nobody was paid for.

    Set SECTOR_NEUTRAL=0 to compare directly against the un-neutralised book;
    the sector map is supplied by the caller through `sector_map`, and an
    empty map makes neutralisation a no-op rather than an error, so the
    strategy runs identically when metadata is unavailable.
"""

from typing import Any, Dict, Optional

import pandas as pd

from trader.simulation.signal_combine import combine, neutralise
from trader.trading.strategy import Strategy


class XsComposite(Strategy):

    LOOKBACK = 252
    SKIP = 21
    REVERSAL_DAYS = 5
    VOL_WINDOW = 63
    LONG_PCT = 0.1
    SHORT_PCT = 0.1
    MIN_NAMES = 20
    REBALANCE_EVERY = 5
    SHORT_ENABLED = 1
    SECTOR_NEUTRAL = 1
    # Membership hysteresis: a name must reach the top EXIT_PCT... see
    # `buffered_membership`. 0 disables (exit bar == entry bar). Attacks the
    # churn a weight-level band could not touch, because in a decile book
    # almost every trade is a whole position opening or closing rather than an
    # adjustment to one being kept.
    EXIT_PCT = 0.0

    # Weights per signal. Deliberately equal by default: IC-weighting fits the
    # weights on the same data the result is read from, which is the selection
    # bias this whole codebase exists to avoid. Equal weights are a
    # pre-registered choice.
    W_MOMENTUM = 1.0
    W_REVERSAL = 1.0
    W_LOWVOL = 0.0        # scored t(NW) = -1.07 in the scan; off by default

    # Loaded from instrument_meta on first use rather than injected by the
    # caller. Setting it on the class from outside does not survive
    # `run_from_module`, which re-imports the module and hands back a FRESH
    # class object - so the injected map silently vanished and neutralisation
    # became a no-op that produced results identical to the un-neutralised
    # book. Owning the lookup here removes the failure mode entirely.
    sector_map: Dict[int, str] = {}

    def _sectors(self) -> Dict[int, str]:
        if self.sector_map:
            return self.sector_map
        try:
            from trader.container import Container
            from trader.data.duckdb_store import DuckDBConnection
            cfg = Container.instance().config()
            db = DuckDBConnection(cfg.get('duckdb_path', ''))
            rows = db.execute(
                'SELECT conid, sic_code FROM instrument_meta '
                'WHERE sic_code IS NOT NULL', fetch='all') or []
            # 2-digit SIC major group. The 174 fine-grained descriptions are
            # mostly groups of one or two, and a group of one cannot be
            # demeaned - it would just delete that name's signal.
            type(self).sector_map = {int(c): str(x)[:2] for c, x in rows
                                     if str(x).strip()}
        except Exception:
            type(self).sector_map = {}
        return self.sector_map

    def precompute_panel(self, panel: pd.DataFrame) -> Dict[str, Any]:
        # Membership carries across bars, so it is instance state rather than
        # precomputed: what the book holds depends on what it held.
        self._held_long: set = set()
        self._held_short: set = set()
        close = panel['close']
        ret1 = close.pct_change()
        signals = {
            'momentum': close.shift(self.SKIP) / close.shift(self.LOOKBACK) - 1.0,
            # Negated so that for EVERY signal, high means expected-high
            # return. Without that the combination would cancel rather than
            # reinforce.
            'reversal': -(close / close.shift(self.REVERSAL_DAYS) - 1.0),
            'lowvol': -ret1.rolling(self.VOL_WINDOW).std(),
        }
        weights = {'momentum': self.W_MOMENTUM, 'reversal': self.W_REVERSAL,
                   'lowvol': self.W_LOWVOL}
        composite = combine(signals, weights=weights)
        if composite is not None and self.SECTOR_NEUTRAL:
            smap = self._sectors()
            if smap:
                composite = neutralise(composite, smap)
        return {'composite': composite}

    def on_panel(self, panel, state, index) -> Optional[Dict[int, float]]:
        comp = state.get('composite')
        if comp is None or index < self.LOOKBACK:
            return None
        if self.REBALANCE_EVERY > 1 and index % self.REBALANCE_EVERY:
            return None
        row = comp.iloc[index].dropna()
        if len(row) < self.MIN_NAMES:
            return None

        if self.EXIT_PCT and self.EXIT_PCT > self.LONG_PCT:
            from trader.simulation.panel import buffered_membership
            pct = row.rank(pct=True)
            held_long = frozenset(self._held_long)
            held_short = frozenset(self._held_short)
            longs = sorted(buffered_membership(
                {int(c): float(v) for c, v in pct.items()},
                held_long, self.LONG_PCT, self.EXIT_PCT))
            # Shorts are the same rule read from the other end: invert the
            # percentile so "most attractive to short" is the top of the scale.
            shorts = sorted(buffered_membership(
                {int(c): 1.0 - float(v) for c, v in pct.items()},
                held_short, self.SHORT_PCT, self.EXIT_PCT))
            self._held_long, self._held_short = set(longs), set(shorts)
            if not longs or (self.SHORT_ENABLED and not shorts):
                return None
        else:
            ranked = row.sort_values()
            n_long = max(1, int(len(ranked) * self.LONG_PCT))
            n_short = max(1, int(len(ranked) * self.SHORT_PCT))
            longs, shorts = list(ranked.index[-n_long:]), list(ranked.index[:n_short])

        weights: Dict[int, float] = {}
        if self.SHORT_ENABLED:
            for c in longs:
                weights[int(c)] = 0.5 / len(longs)
            for c in shorts:
                weights[int(c)] = -0.5 / len(shorts)
        else:
            for c in longs:
                weights[int(c)] = 1.0 / len(longs)
        for c in row.index:
            weights.setdefault(int(c), 0.0)
        return weights
