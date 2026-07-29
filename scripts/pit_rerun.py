#!/usr/bin/env python3
"""Re-run today's three positive findings on a point-in-time universe.

Each of these was caveated rather than resolved because the universe was
today's liquid names, so membership conditioned on survival. Now membership is
decided monthly using only the prior month's dollar-volume rank, and names that
delisted or were acquired are present for as long as they were members.

Measured with IC rather than a backtest, deliberately: the question is how much
of each signal was survivorship, and IC isolates that without also changing
position sizing, costs and construction. A backtest would move several things
at once and the comparison would be unattributable.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np      # noqa: E402
import pandas as pd     # noqa: E402


def main() -> int:
    from pit_panel import load_pit_panel
    from signal_scan import load_panel
    from trader.simulation.signal_eval import evaluate

    print('=== loading both universes ===', flush=True)
    pit_close, pit_open = load_pit_panel(top_n=500)
    surv_close = load_panel(500)
    surv_close = surv_close[surv_close.index >= '2016-08-01']
    print(f'survivorship panel: {surv_close.shape[1]} names '
          f'(today\'s liquid list)\n', flush=True)

    def signals(px):
        ret1 = px.pct_change()
        return {
            'momentum_12_1': px.shift(21) / px.shift(252) - 1.0,
            'reversal_5d': -(px / px.shift(5) - 1.0),
            'low_vol_63d': -ret1.rolling(63).std(),
            # Size proxy: dollar volume is what we have point-in-time for
            # delisted names (no share counts exist for them). Inverse, so
            # "high signal = small" matches the effect's usual direction.
            'inv_price': 1.0 / px,
        }

    print('=== SAME SIGNALS, TWO UNIVERSES (h=1, non-overlapping) ===')
    print(f"{'signal':<18}{'survivorship IC':>18}{'t':>7}"
          f"{'point-in-time IC':>19}{'t':>7}{'change':>9}")
    print('-' * 78)
    for name in ('momentum_12_1', 'reversal_5d', 'low_vol_63d', 'inv_price'):
        a = evaluate(signals(surv_close)[name], surv_close, horizon=1)
        b = evaluate(signals(pit_close)[name], pit_close, horizon=1)
        if a['mean_ic'] is None or b['mean_ic'] is None:
            print(f'{name:<18}   (not computable)'); continue
        delta = b['mean_ic'] - a['mean_ic']
        print(f"{name:<18}{a['mean_ic']:>18.4f}{a['ic_t_stat']:>7.2f}"
              f"{b['mean_ic']:>19.4f}{b['ic_t_stat']:>7.2f}"
              f"{delta:>+9.4f}", flush=True)

    rng = np.random.default_rng(0)
    ctrl = pd.DataFrame(rng.normal(size=pit_close.shape),
                        index=pit_close.index, columns=pit_close.columns)
    c = evaluate(ctrl, pit_close, horizon=1)
    print(f"{'random_control':<18}{'':>18}{'':>7}"
          f"{c['mean_ic']:>19.4f}{c['ic_t_stat']:>7.2f}")

    print('\n=== the reversal hypothesis, with the dead present ===')
    print('(full point-in-time universe, not tech-restricted: SIC codes are')
    print(' unavailable for delisted names, so a tech subset cannot be built')
    print(' point-in-time without introducing the same bias by the back door)')
    from event_study import study
    print(f"{'thresh':>7}{'h':>4}{'events':>8}{'dates':>7}"
          f"{'excess':>10}{'t(NW)':>8}{'baseline':>10}")
    print('-' * 54)
    for th in (0.05, 0.06, 0.08):
        for h in (1, 5, 21):
            n, nd, ex, t, base, tn = study(pit_close, pit_open, th, h, 'drop')
            if ex is None:
                print(f'{100*th:>6.0f}%{h:>4}{n:>8}{nd:>7}   (too few)'); continue
            flag = ' *' if t is not None and abs(t) >= 2 else ''
            print(f'{100*th:>6.0f}%{h:>4}{n:>8}{nd:>7}{100*ex:>9.2f}%'
                  f'{(t if t else float("nan")):>8.2f}{100*base:>9.2f}%{flag}',
                  flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
