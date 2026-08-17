#!/usr/bin/env python3
"""Is the size tilt TRADEABLE, or just rank-accurate? (2026-08-17)

THE QUESTION
    The honest-universe decomposition left one live number: 1/marketcap at
    IC 0.0443, t(NW) 3.34 at h=63, WITH the delisted losers present — the
    survivorship explanation for the size effect is falsified. But momentum
    taught the exact gap this script exists to close: its IC also ROSE on the
    honest universe while its traded Sharpe FELL, because the restored names
    are the ones that gap, squeeze and cost the most to trade. Rank accuracy
    and tradeability are different claims. This prices the second one.

PRE-REGISTERED DESIGN (written before the result)
    Two trials, no grid:
      * bottom-marketcap quintile of the covered universe (the size tilt)
      * top sales_to_price quintile (the one ratio that nominally beat both
        of its own parts in the decomposition)
    Monthly rebalance (21 bars), equal weight within the held slice, long
    only. Universe each rebalance = PIT members that day with a computable
    signal (price AND as-of shares from an accepted filing). Benchmarks:
      * equal weight over the SAME covered set — the decisive comparison; a
        tilt that cannot beat owning its own universe is concentration, not
        selection (the decomposition that killed long-only momentum)
      * equal weight over the WHOLE panel — context, so coverage itself is
        visible as a return effect
    Execution identical to the momentum test: `Backtester.run_panel`, fill at
    t+1's open, 5bps slippage, $0.005/share commission.

HONESTY NOTES, WRITTEN BEFORE THE RESULT
    * Two trials are priced here. Upstream choices made after seeing data are
      not: monthly rebalance, quintiles, long-only. The true trial count is
      larger. There is no parameter search to walk-forward — the rule has no
      fitted parameters — so PBO does not apply; the benchmark comparison IS
      the test.
    * 5bps flat slippage understates real small-cap costs. The holdings are
      the SMALLEST names inside a top-500-dollar-volume universe — liquid by
      construction, but the cost model is optimistic and the result should be
      read with that sign in mind.
    * The signal frames are as-of by construction (EDGAR acceptance instant
      strictly before each day's read; validated in the scan phase). Position
      i trades at bar i+1's open, so nothing here reads forward.
    * Names delisted mid-hold: the panel carries their last price forward for
      marking, and the book zeroes every previously-held name explicitly, so
      a name that leaves coverage is closed at the next rebalance rather than
      silently held forever.
"""
from __future__ import annotations

import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import numpy as np      # noqa: E402
import pandas as pd     # noqa: E402

ANN = math.sqrt(252.0)


class SignalQuintilePanel:
    """Hold one quintile of the covered universe, equal-weighted, monthly."""

    REBALANCE_EVERY = 21
    QUANTILE = 0.2
    MIN_NAMES = 50          # a "quintile" of fewer than 10 names is a handful

    def __init__(self, signal: pd.DataFrame, take_bottom: bool):
        self.signal = signal
        self.take_bottom = take_bottom
        self._held: set = set()

    def precompute_panel(self, panel):
        close = panel['close']
        sig = self.signal.reindex(index=close.index, columns=close.columns)
        return {'sig': sig, 'close': close}

    def on_panel(self, panel, state, index):
        if self.REBALANCE_EVERY > 1 and index % self.REBALANCE_EVERY:
            return None
        px = state['close'].iloc[index]
        row = state['sig'].iloc[index][px.notna()].dropna()
        if len(row) < self.MIN_NAMES:
            return None
        ranked = row.sort_values()
        n = max(1, int(len(ranked) * self.QUANTILE))
        held = list(ranked.index[:n] if self.take_bottom
                    else ranked.index[-n:])
        weights = {c: 1.0 / len(held) for c in held}
        # Close everything previously held that was not re-selected — absent
        # keys mean "leave the position alone", and a delisted name would
        # otherwise be carried forever.
        for c in self._held.difference(held):
            weights[c] = 0.0
        for c in row.index:
            weights.setdefault(c, 0.0)
        self._held = set(held)
        return weights


class CoveredEqualWeightPanel(SignalQuintilePanel):
    """Equal weight over every covered name — the decisive benchmark."""

    def on_panel(self, panel, state, index):
        if self.REBALANCE_EVERY > 1 and index % self.REBALANCE_EVERY:
            return None
        px = state['close'].iloc[index]
        row = state['sig'].iloc[index][px.notna()].dropna()
        if len(row) < self.MIN_NAMES:
            return None
        held = list(row.index)
        weights = {c: 1.0 / len(held) for c in held}
        for c in self._held.difference(held):
            weights[c] = 0.0
        self._held = set(held)
        return weights


class WholeUniverseEqualWeightPanel:
    """Equal weight over everything priced — context for coverage effects."""

    REBALANCE_EVERY = 21

    def precompute_panel(self, panel):
        return {'close': panel['close']}

    def on_panel(self, panel, state, index):
        if self.REBALANCE_EVERY > 1 and index % self.REBALANCE_EVERY:
            return None
        row = state['close'].iloc[index].dropna()
        if len(row) < 2:
            return None
        return {c: 1.0 / len(row) for c in row.index}


def _metrics(curve: pd.Series):
    rets = curve.pct_change().dropna()
    if len(rets) < 30:
        return None
    sd = rets.std(ddof=1)
    years = len(rets) / 252.0
    total = float(curve.iloc[-1] / curve.iloc[0] - 1.0)
    running = curve.cummax()
    return {
        'sharpe': float(rets.mean() / sd * ANN) if sd > 0 else 0.0,
        'cagr': float((1.0 + total) ** (1.0 / years) - 1.0),
        'return': total,
        'max_dd': float(((curve - running) / running).min()),
        'n_days': len(rets),
    }


def main() -> int:
    from fundamental_signals import build
    from pit_panel import load_pit_panel
    from trader.objects import BarSize
    from trader.simulation.backtester import Backtester, BacktestConfig

    print('=== loading panel + as-of fundamentals ===', flush=True)
    close, opens = load_pit_panel(top_n=500, verbose=False)
    px, sigs, frames = build(0, pit=True, with_frames=True)
    assert px.index.equals(close.index) and px.columns.equals(close.columns), \
        'panel and signal frames must be the same object shape'

    mcap = (px * frames['shares_diluted']).replace([np.inf, -np.inf], np.nan)
    s2p = sigs['sales_to_price']

    tickers = list(close.columns)
    panel_frames = {}
    for t in tickers:
        df = pd.DataFrame({'open': opens[t], 'close': close[t]}).dropna(
            subset=['close'])
        if len(df) >= 2:
            panel_frames[t] = df

    config = BacktestConfig(
        start_date=close.index[0].to_pydatetime(),
        end_date=close.index[-1].to_pydatetime(),
        initial_capital=1_000_000.0,
        bar_size=BarSize.Days1,
        slippage_bps=5.0,
        commission_per_share=0.005,
    )
    bt = Backtester(config=config, storage=None)

    books = {
        'small-cap quintile (1/mcap)': SignalQuintilePanel(mcap, take_bottom=True),
        'top sales_to_price quintile': SignalQuintilePanel(s2p, take_bottom=False),
        'EW covered set (benchmark)': CoveredEqualWeightPanel(mcap, take_bottom=True),
        'EW whole panel (context)': WholeUniverseEqualWeightPanel(),
    }

    print(f"{'book':<30}{'sharpe':>8}{'CAGR':>8}{'total':>9}{'maxDD':>8}{'days':>7}")
    print('-' * 70)
    results = {}
    for name, strat in books.items():
        result = bt.run_panel(strat, [], frames=panel_frames)
        m = _metrics(result.equity_curve)
        results[name] = m
        if m is None:
            print(f'{name:<30}   (not computable)', flush=True)
            continue
        print(f"{name:<30}{m['sharpe']:>8.2f}{m['cagr']:>8.1%}"
              f"{m['return']:>9.1%}{m['max_dd']:>8.1%}{m['n_days']:>7}",
              flush=True)

    tilt = results.get('small-cap quintile (1/mcap)')
    bench = results.get('EW covered set (benchmark)')
    if tilt and bench:
        print(f"\ntilt minus covered-EW benchmark: "
              f"sharpe {tilt['sharpe'] - bench['sharpe']:+.2f}, "
              f"CAGR {tilt['cagr'] - bench['cagr']:+.1%}")
        print('A tilt that does not beat owning its own universe is '
              'concentration, not selection.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
