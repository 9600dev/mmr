#!/usr/bin/env python3
"""Score cross-sectional signals over the daily panel.

Loads every instrument with enough daily history into a wide price frame and
runs `signal_eval` over a set of classic academic signals. The point is to
learn whether ANY cross-sectional information exists here before building
portfolio machinery to trade it.

Each signal is computed so that its value at row t uses only data up to and
including t; the evaluator pairs it with the return from t forward. Both
halves of that contract matter and only one of them is enforced here — the
other is pinned in tests/invariants/test_signal_eval.py.
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np      # noqa: E402
import pandas as pd     # noqa: E402


def load_panel(min_bars: int = 500) -> pd.DataFrame:
    from trader.container import Container
    from trader.data.duckdb_store import DuckDBConnection

    cfg = Container.instance().config()
    db = DuckDBConnection(cfg.get('history_duckdb_path', ''))
    rows = db.execute(
        """SELECT symbol, date, close FROM tick_data
           WHERE bar_size = '1 day' AND close IS NOT NULL AND close > 0
             AND symbol IN (SELECT symbol FROM tick_data WHERE bar_size='1 day'
                            GROUP BY symbol HAVING count(*) >= ?)
           ORDER BY date""", [min_bars], fetch='df')
    if rows is None or len(rows) == 0:
        return pd.DataFrame()
    rows['date'] = pd.to_datetime(rows['date'], utc=True).dt.tz_convert('UTC')
    # Normalise to the calendar date: instruments carry different session
    # offsets, and a cross-section must compare the same day across names.
    rows['day'] = rows['date'].dt.date
    wide = rows.pivot_table(index='day', columns='symbol', values='close',
                            aggfunc='last')
    wide.index = pd.to_datetime(wide.index)
    return wide.sort_index()


def build_signals(px: pd.DataFrame) -> dict:
    """Classic cross-sectional signals. Every one is causal at row t."""
    ret1 = px.pct_change(1)
    return {
        # Jegadeesh-Titman: 12-month return skipping the most recent month,
        # because the skip is what separates momentum from short-term reversal.
        'momentum_12_1': px.shift(21) / px.shift(252) - 1.0,
        'momentum_6_1': px.shift(21) / px.shift(126) - 1.0,
        # Short-horizon reversal — negated so "high signal = expected high
        # return" holds for every signal here, making ICs comparable in sign.
        'reversal_5d': -(px / px.shift(5) - 1.0),
        'reversal_21d': -(px / px.shift(21) - 1.0),
        # Low-volatility anomaly: negated realised vol.
        'low_vol_63d': -ret1.rolling(63).std(),
        # Distance below the 52-week high.
        'pct_off_52w_high': px / px.rolling(252).max() - 1.0,
        # A deliberate control: pure noise. Its IC must be indistinguishable
        # from zero, and if it is not, the harness is broken rather than the
        # market being predictable.
        'random_control': pd.DataFrame(
            np.random.default_rng(0).normal(size=px.shape),
            index=px.index, columns=px.columns),
    }


def panel_conids(min_bars: int = 500) -> list:
    """Conids with enough daily history for a panel backtest."""
    from trader.container import Container
    from trader.data.duckdb_store import DuckDBConnection
    cfg = Container.instance().config()
    db = DuckDBConnection(cfg.get('history_duckdb_path', ''))
    rows = db.execute(
        """SELECT symbol FROM tick_data WHERE bar_size='1 day'
           GROUP BY symbol HAVING count(*) >= ? ORDER BY symbol""",
        [min_bars], fetch='all') or []
    out = []
    for (sym,) in rows:
        try:
            out.append(int(sym))
        except (TypeError, ValueError):
            continue
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--horizons', type=int, nargs='+', default=[1, 5, 21])
    ap.add_argument('--min-bars', type=int, default=500)
    ap.add_argument('--buckets', type=int, default=5)
    args = ap.parse_args()

    from trader.simulation.signal_eval import evaluate

    px = load_panel(args.min_bars)
    if px.empty:
        print('no panel data', file=sys.stderr)
        return 1
    print(f'panel: {px.shape[1]} instruments x {px.shape[0]} days '
          f'({px.index[0].date()} -> {px.index[-1].date()}), '
          f'{int(px.notna().sum().sum()):,} observations\n')

    signals = build_signals(px)
    print(f"{'signal':<20}{'h':>3}{'mean IC':>10}{'t(NW)':>8}{'t(raw)':>8}{'hit%':>7}"
          f"{'top-bot':>10}{'turnover':>10}{'periods (have/need)':>22}")
    print('-' * 91)
    for name, sig in signals.items():
        for h in args.horizons:
            r = evaluate(sig, px, horizon=h, n_buckets=args.buckets)
            if r['mean_ic'] is None:
                print(f'{name:<20}{h:>3}   (not computable)')
                continue
            need = r['periods_needed_for_t2']
            need_s = f"{r['n_periods']:,}/{need:,.0f}" if need else f"{r['n_periods']:,}/—"
            t = r['ic_t_stat']
            flag = ' *' if t is not None and abs(t) >= 2 else ''
            print(f"{name:<20}{h:>3}{r['mean_ic']:>10.4f}"
                  f"{(t if t is not None else float('nan')):>8.2f}"
                  f"{(r['ic_t_stat_naive'] if r['ic_t_stat_naive'] is not None else float('nan')):>8.2f}"
                  f"{100*(r['ic_hit_rate'] or 0):>7.1f}"
                  f"{(r['top_minus_bottom'] or float('nan')):>10.4f}"
                  f"{(r['mean_turnover'] or float('nan')):>10.3f}"
                  f"{need_s:>22}{flag}")
    print('\nt(NW) is Newey-West corrected for overlapping forward returns; '
          't(raw) ignores the overlap\n  and is shown only to expose the gap — '
          'at h=21 it inflates t by roughly sqrt(21).')
    print('* = |t(NW)| >= 2. Compare every row against random_control: if the '
          'control is also significant,\n  the harness is wrong, not the '
          'market. periods have/need: if need >> have, the result is\n  '
          'unmeasured rather than weak.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
