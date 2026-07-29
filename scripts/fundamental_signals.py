#!/usr/bin/env python3
"""Build fundamental signals as-of each date, and measure whether they predict.

THE AS-OF RULE
    At each date D, a name's fundamentals are the most recent filing ACCEPTED
    before D's close (16:00 ET). Not the most recent period - the most recently
    KNOWN one. 58.5% of filings in this store land at or after 16:00 ET, so
    more than half are unavailable on their own filing date, and a date-only
    source would trade them a day early.

THE LEAK CHECK
    Fundamentals are where a positive result is most likely to be fake, because
    the lookahead is subtle and produces exactly the clean strong signal that is
    easy to believe. So every signal is also measured with acceptance instants
    shifted FORWARD by a week: a real signal degrades under that shift, a
    leaked one barely moves, because a leak does not depend on when the news
    actually arrived.
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np      # noqa: E402
import pandas as pd     # noqa: E402


def build(delay_days: int = 0):
    """(price panel, {signal name: as-of frame}) with acceptance delayed by
    ``delay_days`` - 0 is the honest read, positive is the leak probe."""
    from signal_scan import load_panel
    from trader.container import Container
    from trader.data.duckdb_store import DuckDBConnection

    px = load_panel(500)
    cfg = Container.instance().config()
    db = DuckDBConnection(cfg.get('duckdb_path', ''))
    rows = db.execute(
        """SELECT conid, period_end, accepted_at, revenue, net_income,
                  gross_profit, total_assets, stockholders_equity,
                  cash_from_operations, shares_diluted
           FROM fundamentals ORDER BY conid, accepted_at""", fetch='df')
    if rows is None or len(rows) == 0:
        return px, {}

    rows['known_at'] = (pd.to_datetime(rows['accepted_at'], utc=True)
                        + pd.Timedelta(days=delay_days))
    # Trailing-twelve-month sums for FLOW items; balance-sheet items are
    # point-in-time and must not be summed.
    rows = rows.sort_values(['conid', 'known_at'])
    for f in ('revenue', 'net_income', 'gross_profit', 'cash_from_operations'):
        rows[f + '_ttm'] = (rows.groupby('conid')[f]
                            .rolling(4, min_periods=4).sum()
                            .reset_index(level=0, drop=True))

    idx = pd.to_datetime(px.index, utc=True) + pd.Timedelta(hours=21)  # 16:00 ET
    out = {}
    fields = ['net_income_ttm', 'stockholders_equity', 'gross_profit_ttm',
              'total_assets', 'cash_from_operations_ttm', 'shares_diluted',
              'revenue_ttm']
    frames = {f: pd.DataFrame(index=px.index, columns=px.columns, dtype=float)
              for f in fields}
    for conid, grp in rows.groupby('conid'):
        col = str(conid)
        if col not in px.columns:
            continue
        g = grp.dropna(subset=['known_at']).sort_values('known_at')
        # searchsorted with side='left' gives the count of filings STRICTLY
        # before each date, which is the as-of rule: a filing accepted at the
        # instant asked about was not actionable at it.
        pos = np.searchsorted(g['known_at'].values, idx.values, side='left') - 1
        valid = pos >= 0
        for f in fields:
            vals = np.full(len(idx), np.nan)
            vals[valid] = g[f].to_numpy()[pos[valid]]
            frames[f][col] = vals

    mcap = px * frames['shares_diluted']
    out['earnings_yield'] = frames['net_income_ttm'] / mcap
    out['book_to_price'] = frames['stockholders_equity'] / mcap
    out['gross_profitability'] = frames['gross_profit_ttm'] / frames['total_assets']
    out['cash_flow_yield'] = frames['cash_from_operations_ttm'] / mcap
    out['sales_to_price'] = frames['revenue_ttm'] / mcap
    # Accruals: earnings not backed by cash. Negated so high = attractive,
    # matching every other signal's orientation.
    out['low_accruals'] = -((frames['net_income_ttm']
                             - frames['cash_from_operations_ttm'])
                            / frames['total_assets'])
    for k in out:
        out[k] = out[k].replace([np.inf, -np.inf], np.nan)
    return px, out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--horizons', type=int, nargs='+', default=[21, 63])
    args = ap.parse_args()
    from trader.simulation.signal_eval import evaluate

    for delay, label in ((0, 'AS-OF (honest)'), (7, 'DELAYED +7d (leak probe)')):
        px, sigs = build(delay)
        if not sigs:
            print('no fundamentals', file=sys.stderr); return 1
        rng = np.random.default_rng(0)
        sigs['random_control'] = pd.DataFrame(
            rng.normal(size=px.shape), index=px.index, columns=px.columns)
        print(f'\n=== {label} ===', flush=True)
        print(f"{'signal':<22}{'h':>4}{'mean IC':>10}{'t(NW)':>8}{'coverage':>10}")
        print('-' * 54)
        for name, s in sigs.items():
            cov = float(s.notna().sum().sum()) / (s.shape[0] * s.shape[1])
            for h in args.horizons:
                r = evaluate(s, px, horizon=h)
                if r['mean_ic'] is None:
                    print(f'{name:<22}{h:>4}   (not computable)'); continue
                t = r['ic_t_stat']
                flag = ' *' if t is not None and abs(t) >= 2 else ''
                print(f"{name:<22}{h:>4}{r['mean_ic']:>10.4f}"
                      f"{(t if t is not None else float('nan')):>8.2f}"
                      f"{100*cov:>9.0f}%{flag}", flush=True)
    print('\n* = |t(NW)| >= 2. A real signal DEGRADES under the +7d delay; a')
    print('  leaked one barely moves, because a leak does not depend on when')
    print('  the news actually arrived.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
