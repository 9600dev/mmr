#!/usr/bin/env python3
"""Wide threshold/horizon sweep over the reversal-vs-continuation question.

WHY THIS PRINTS A MULTIPLE-TESTING LINE FIRST
    A sweep this wide will produce significant-looking cells whether or not an
    effect exists. At 196 independent tests you expect ~10 cells beyond |t|=2
    from noise alone, so "we found a cell at t=2.4" is not a finding - it is
    the arithmetic of testing 196 things. The count of exceedances against its
    own expectation is the result; individual cells are not.

    The cells here are also NOT independent - nested thresholds on nested
    horizons over one panel - which cuts the effective number of tests but also
    means one real effect would light up many neighbouring cells. So the shape
    matters more than the peak: a genuine effect appears as a contiguous
    region, noise appears as scattered singletons.

CONTINUATION IS THE SAME MEASUREMENT, READ WITH THE OPPOSITE SIGN
    Betting on continuation after a drop means SHORTING it. The excess return
    is identical; only the sign of the position changes. So the sweep reports
    the excess once, and the continuation P&L is its negation for drops and
    itself for spikes - no separate experiment needed, and no chance for two
    code paths to disagree.
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np      # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--start', default='2021-07-01')
    args = ap.parse_args()
    from event_study import load, study, study_idiosyncratic

    THRESH = [0.02, 0.03, 0.04, 0.05, 0.06, 0.08, 0.10]
    HORIZ = [1, 3, 5, 10, 21, 42, 63]
    close, opens = load(args.start)
    print(f'tech universe: {close.shape[1]} names x {close.shape[0]} days\n')

    cells = []
    for variant, fn in (('raw', study), ('idio', study_idiosyncratic)):
        for direction in ('drop', 'spike'):
            for th in THRESH:
                for h in HORIZ:
                    out = fn(close, opens, th, h, direction)
                    n, nd, ex, t, base, tn = out
                    if ex is None or t is None:
                        continue
                    cells.append({'variant': variant, 'dir': direction,
                                  'th': th, 'h': h, 'excess': ex, 't': t,
                                  'events': n, 'dates': nd})

    n_tests = len(cells)
    hits = [c for c in cells if abs(c['t']) >= 2.0]
    expected = 0.0455 * n_tests           # two-sided 5% of a t-distribution
    print('=' * 68)
    print(f'MULTIPLE TESTING: {n_tests} cells tested, {len(hits)} beyond |t|=2, '
          f'~{expected:.0f} expected from noise')
    if len(hits) <= expected * 1.5:
        print('  -> the exceedance count is AT OR BELOW chance. No effect here,')
        print('     regardless of what the best individual cell reads.')
    else:
        print('  -> more exceedances than chance; check whether they form a')
        print('     CONTIGUOUS region (an effect) or scattered singletons (noise).')
    print('=' * 68)

    print('\nSIGN CONSISTENCY (a real effect has one sign across its region):')
    for variant in ('raw', 'idio'):
        for direction in ('drop', 'spike'):
            sub = [c for c in cells if c['variant'] == variant
                   and c['dir'] == direction]
            if not sub:
                continue
            pos = sum(1 for c in sub if c['excess'] > 0)
            mean_t = float(np.mean([c['t'] for c in sub]))
            print(f'  {variant:<5} after {direction:<6} {pos:>2}/{len(sub)} cells '
                  f'positive, mean t = {mean_t:+.2f}')

    print('\nCONTINUATION P&L (excess to a position that BETS ON CONTINUATION):')
    print('  after a drop -> SHORT it; after a spike -> LONG it.')
    print(f"{'variant':<7}{'dir':<7}{'thresh':>7}{'h':>4}{'cont.excess':>13}{'t':>8}")
    print('-' * 46)
    for variant in ('raw', 'idio'):
        for direction in ('drop', 'spike'):
            sub = sorted([c for c in cells if c['variant'] == variant
                          and c['dir'] == direction],
                         key=lambda c: -abs(c['t']))[:3]
            for c in sub:
                sign = -1.0 if direction == 'drop' else 1.0
                print(f"{variant:<7}{direction:<7}{100*c['th']:>6.0f}%{c['h']:>4}"
                      f"{100*sign*c['excess']:>12.2f}%{sign*c['t']:>8.2f}")

    top = sorted(cells, key=lambda c: -abs(c['t']))[:5]
    print('\nlargest |t| cells (for completeness - do NOT read these as findings):')
    for c in top:
        print(f"  {c['variant']:<5} {c['dir']:<6} {100*c['th']:.0f}% h={c['h']:<3} "
              f"excess {100*c['excess']:+.2f}%  t={c['t']:+.2f}  "
              f"({c['dates']} dates)")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
