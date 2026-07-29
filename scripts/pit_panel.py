#!/usr/bin/env python3
"""Load a point-in-time panel: membership decided with past information only.

MEMBERSHIP RULE
    A name is in the universe for month M if it was in the top N by dollar
    volume at the END of month M-1. Monthly rather than daily because daily
    re-ranking makes a name hovering at rank 500 flicker in and out, generating
    membership churn that is an artefact of the boundary rather than of the
    market. Lagged by a month so the rule uses only what was known.

THE RESIDUAL BIAS THAT REMAINS, AND WHY IT IS REPORTED NOT FIXED
    When a name stops trading its price series simply ends. A forward return
    spanning that point has no second endpoint, so it comes out NaN and the
    observation drops - which means a company that went to zero contributes no
    loss. That is survivorship again, one level down: fixed at the membership
    level, still present at the return level.

    Fixing it properly needs to distinguish "delisted at ~zero" (return should
    be about -100%) from "acquired at a premium" (return should be the deal
    price), and that needs corporate-action data this loader does not take.
    So instead it COUNTS how often a name vanishes mid-window and prints it,
    because a bias you can size is one you can argue about, and a bias you have
    silently dropped is not.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np      # noqa: E402
import pandas as pd     # noqa: E402


def load_pit_panel(top_n: int = 500, start: str = '2016-08-01',
                   verbose: bool = True):
    """Returns (close, opens) with point-in-time membership applied."""
    from trader.container import Container
    from trader.data.duckdb_store import DuckDBConnection

    cfg = Container.instance().config()
    db = DuckDBConnection(cfg.get('history_duckdb_path', ''))
    df = db.execute(
        """SELECT day, ticker, open, close, dv_rank FROM pit_daily_bars
           WHERE day >= ? ORDER BY day""", [start], fetch='df')
    if df is None or len(df) == 0:
        return None, None
    # NASDAQ/NYSE publish TEST tickers in the consolidated tape - ZWZZT closed
    # at 199,999.00 in this store. They are not securities and would dominate
    # any cross-sectional ranking they touched.
    _TEST = ('ZWZZT', 'ZVZZT', 'ZXZZT', 'ZJZZT', 'ZAZZT', 'ZBZZT', 'ZCZZT',
             'ZTEST', 'IBM.TEST', 'CBO', 'CBX')
    before = df['ticker'].nunique()
    df = df[~df['ticker'].isin(_TEST)]
    df = df[(df['close'] > 0.01) & (df['close'] < 100_000)]
    dropped = before - df['ticker'].nunique()

    df['day'] = pd.to_datetime(df['day'])
    df['month'] = df['day'].dt.to_period('M')

    # Rank at each month end -> membership for the FOLLOWING month.
    month_end = (df.sort_values('day').groupby(['month', 'ticker'])
                 .agg(rank=('dv_rank', 'last')).reset_index())
    member = month_end[month_end['rank'] <= top_n].copy()
    member['effective'] = member['month'] + 1        # lagged: known in advance
    allowed = set(zip(member['effective'].astype(str), member['ticker']))

    df['key'] = list(zip(df['month'].astype(str), df['ticker']))
    df['in_universe'] = [k in allowed for k in df['key']]
    live = df[df['in_universe']]

    close = live.pivot_table(index='day', columns='ticker', values='close',
                             aggfunc='last').sort_index()
    opens = live.pivot_table(index='day', columns='ticker', values='open',
                             aggfunc='last').sort_index()
    opens = opens.reindex(index=close.index, columns=close.columns)

    if verbose:
        n_names = close.shape[1]
        per_day = close.notna().sum(axis=1)
        print(f'point-in-time panel: {n_names} distinct names, '
              f'{close.shape[0]} sessions, {per_day.median():.0f} live per day')
        if dropped:
            print(f'  filtered {dropped} exchange TEST ticker(s) / absurd prices')
        # The honest residual bias. An earlier version of this counted every
        # name whose series ends mid-sample and called it delisted, which came
        # out 3x too high: most had simply fallen out of the liquid set, and a
        # name you would have stopped holding is correctly excluded rather than
        # lost. Only a name that stops while STILL a member is a genuine gap.
        last_rank = (live.sort_values('day').groupby('ticker')['dv_rank']
                     .last())
        last_day = live.groupby('ticker')['day'].max()
        cutoff = close.index[-1] - pd.Timedelta(days=45)
        stopped = last_day < cutoff
        genuine = int((stopped & (last_rank <= top_n)).sum())
        left_first = int((stopped & (last_rank > top_n)).sum())
        print(f'  stopped while still a member : {genuine}  <- genuine gap, '
              f'terminal return unobservable')
        print(f'  stopped after leaving the set: {left_first}  <- correctly '
              f'excluded, not lost')
    return close, opens


if __name__ == '__main__':
    c, o = load_pit_panel()
    print(f'\nclose panel {c.shape}, opens {o.shape}')
    print('sample of names absent from the current liquid list:')
    for t in ('SIVB', 'FRC', 'TWTR', 'ATVI', 'VMW', 'XLNX', 'ZNGA'):
        if t in c.columns:
            s = c[t].dropna()
            print(f'   {t:<6} {len(s):>5} sessions, {s.index[0].date()} -> '
                  f'{s.index[-1].date()}, last close {s.iloc[-1]:.2f}')
