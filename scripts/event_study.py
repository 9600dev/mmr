#!/usr/bin/env python3
"""Does a big one-day move reverse? An event study, not a backtest.

THE HYPOTHESIS UNDER TEST
    After a drop of X% a name climbs back, and after a spike of X% it falls
    back. Tested on tech, last 5 years, at several thresholds and horizons.

WHY AN EVENT STUDY
    A backtest answers "what would this have earned", which mixes the effect
    with position sizing, costs and portfolio construction. The prior question
    is whether the effect EXISTS, and that is a conditional-mean question:
    given a -6% day, is the forward return different from an ordinary day?

THREE THINGS THAT WOULD FAKE THIS RESULT, ALL HANDLED

    1. DRIFT. Tech rose steeply over this window, so ANY forward return looks
       positive. Every number here is therefore an EXCESS over the same
       instruments' unconditional forward return in the same period. Without
       that subtraction the test measures the bull market.

    2. CLUSTERING. Big drops are not independent events - hundreds of tech
       names fall 6% on the same market-wide day. Treating each name-day as an
       observation overstates the sample by orders of magnitude. Statistics
       are computed on PER-DATE means, so the sample size is the number of
       distinct event dates, not the number of events.

    3. SURVIVORSHIP, and this is the one that most threatens THIS hypothesis.
       The universe is today's liquid names, so a stock that fell 6% and kept
       falling until it delisted is absent from the data. That biases
       "drops recover" upward by construction, and no amount of statistical
       care inside the sample can fix it. It is reported, not corrected.

NO LOOKAHEAD
    The event is known at the close of day t. The forward return is measured
    from t+1's OPEN, which is the first price actually tradeable on it.
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np      # noqa: E402
import pandas as pd     # noqa: E402


def tech_conids() -> set:
    """SIC major groups 35 (computer/industrial machinery), 36 (electronics),
    73 (business services, which is where software sits)."""
    from trader.container import Container
    from trader.data.duckdb_store import DuckDBConnection
    cfg = Container.instance().config()
    db = DuckDBConnection(cfg.get('duckdb_path', ''))
    rows = db.execute(
        "SELECT conid, sic_code FROM instrument_meta WHERE sic_code IS NOT NULL",
        fetch='all') or []
    return {str(c) for c, s in rows if str(s)[:2] in ('35', '36', '73')}


def load(start: str):
    """Close and open panels, restricted to tech, from ``start``."""
    from trader.container import Container
    from trader.data.duckdb_store import DuckDBConnection
    from signal_scan import load_panel

    close = load_panel(500)
    close = close[close.index >= start]
    keep = [c for c in close.columns if c in tech_conids()]
    close = close[keep]

    cfg = Container.instance().config()
    db = DuckDBConnection(cfg.get('history_duckdb_path', ''))
    rows = db.execute(
        """SELECT symbol, date, open FROM tick_data
           WHERE bar_size = '1 day' AND open IS NOT NULL AND open > 0
           ORDER BY date""", fetch='df')
    rows['day'] = pd.to_datetime(rows['date'], utc=True).dt.date
    opens = rows.pivot_table(index='day', columns='symbol', values='open',
                             aggfunc='last')
    opens.index = pd.to_datetime(opens.index)
    opens = opens.reindex(index=close.index, columns=close.columns)
    return close, opens


def study_idiosyncratic(close, opens, threshold: float, horizon: int,
                        direction: str, require_market_up: bool = False):
    """The same test on the name's move NET OF THE MARKET.

    A different and more plausible claim than the raw version. A market-wide
    6% selloff is risk being repriced across everything, and there is no reason
    a name should bounce from it. A name that falls 6% while its peers do not
    is a candidate for overreaction, which is what a reversal story actually
    requires.

    Both the event and the outcome are residual: the move is measured against
    the cross-sectional mean, and so is the forward return, because the trade
    that exploits it would be market-hedged. Measuring a residual event with a
    raw outcome would just re-import the drift this whole study exists to
    remove.

    ``require_market_up`` is the strict version - the name fell while the
    market did NOT - which is the cleanest reading of "idiosyncratic" and the
    hardest to explain as beta.
    """
    ret1 = close.pct_change()
    market = ret1.mean(axis=1)                       # equal-weight peer return
    resid = ret1.sub(market, axis=0)

    entry = opens.shift(-1)
    exit_ = close.shift(-(1 + horizon))
    fwd_raw = exit_ / entry - 1.0
    # Forward market return over the same window, from the same panel.
    mkt_entry = opens.mean(axis=1).shift(-1)
    mkt_exit = close.mean(axis=1).shift(-(1 + horizon))
    fwd_mkt = mkt_exit / mkt_entry - 1.0
    fwd = fwd_raw.sub(fwd_mkt, axis=0)               # residual forward return

    if direction == 'drop':
        mask = resid <= -threshold
        if require_market_up:
            mask = mask & (market >= 0).to_numpy()[:, None]
    else:
        mask = resid >= threshold
        if require_market_up:
            mask = mask & (market <= 0).to_numpy()[:, None]

    ev = fwd.where(mask & fwd.notna())
    n_events = int(ev.notna().sum().sum())
    if n_events < 30:
        return n_events, 0, None, None, None, None
    baseline = float(np.nanmean(fwd.to_numpy()))
    per_date = ev.mean(axis=1).dropna()
    n_dates = len(per_date)
    if n_dates < 10:
        return n_events, n_dates, None, None, baseline, None
    excess = per_date - baseline
    from trader.simulation.signal_eval import newey_west_variance
    var = newey_west_variance(tuple(float(v) for v in excess.to_numpy()),
                              max(horizon - 1, 0))
    t = (float(excess.mean()) / np.sqrt(var)) if var else None
    sd = float(excess.std(ddof=1))
    tn = (float(excess.mean()) / (sd / np.sqrt(n_dates))) if sd > 0 else None
    return n_events, n_dates, float(excess.mean()), t, baseline, tn


def study(close, opens, threshold: float, horizon: int, direction: str):
    """Excess forward return after a move beyond ``threshold``.

    Returns (n_events, n_dates, excess_mean, t_stat_on_dates, baseline_mean).
    """
    ret1 = close.pct_change()
    # Entry at t+1 open, exit at t+1+horizon close - the tradeable window.
    entry = opens.shift(-1)
    exit_ = close.shift(-(1 + horizon))
    fwd = (exit_ / entry - 1.0)

    if direction == 'drop':
        mask = ret1 <= -threshold
    else:
        mask = ret1 >= threshold

    ev = fwd.where(mask & fwd.notna())
    n_events = int(ev.notna().sum().sum())
    if n_events < 30:
        return n_events, 0, None, None, None, None

    # Baseline: the SAME instruments' forward return on all days in the same
    # window. Without this the number is the bull market, not the effect.
    baseline = float(np.nanmean(fwd.to_numpy()))

    # Per-date means: big drops cluster on market-wide days, so the unit of
    # observation is the date, not the name-day.
    per_date = ev.mean(axis=1).dropna()
    n_dates = len(per_date)
    if n_dates < 10:
        return n_events, n_dates, None, None, baseline, None
    excess = per_date - baseline
    # Per-date clustering fixes CROSS-SECTIONAL dependence (many names fall on
    # the same day) but not SERIAL dependence: at horizon h, consecutive dates'
    # forward windows share h-1 days, so the t-statistic is inflated by roughly
    # sqrt(h). This is the same defect the pure-noise control caught in the
    # signal scan, in a different costume - and here it is worse, because
    # significance appeared ONLY at h=21, exactly where the overlap is largest.
    from trader.simulation.signal_eval import newey_west_variance
    vals = tuple(float(v) for v in excess.to_numpy())
    var = newey_west_variance(vals, max(horizon - 1, 0))
    t = (float(excess.mean()) / np.sqrt(var)) if var else None
    sd = float(excess.std(ddof=1))
    t_naive = (float(excess.mean()) / (sd / np.sqrt(n_dates))) if sd > 0 else None
    return n_events, n_dates, float(excess.mean()), t, baseline, t_naive


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--start', default='2021-07-01')
    ap.add_argument('--thresholds', type=float, nargs='+',
                    default=[0.04, 0.05, 0.06, 0.08])
    ap.add_argument('--horizons', type=int, nargs='+', default=[1, 3, 5, 10, 21])
    args = ap.parse_args()

    close, opens = load(args.start)
    print(f'tech universe: {close.shape[1]} instruments x {close.shape[0]} days '
          f'({close.index[0].date()} -> {close.index[-1].date()})')
    ret = close.pct_change()
    print(f'unconditional daily mean: {100*float(np.nanmean(ret.to_numpy())):+.3f}%'
          f'   (this is the drift every result must be measured against)\n')

    for direction, arrow in (('drop', 'AFTER A DROP  (expect: bounce)'),
                             ('spike', 'AFTER A SPIKE (expect: fade)')):
        print(f'=== {arrow} ===')
        print(f"{'thresh':>7}{'h':>4}{'events':>8}{'dates':>7}"
              f"{'excess':>10}{'t(NW)':>8}{'t(raw)':>8}{'baseline':>10}")
        print('-' * 62)
        for th in args.thresholds:
            for h in args.horizons:
                n, nd, ex, t, base, tn = study(close, opens, th, h, direction)
                if ex is None:
                    print(f'{100*th:>6.0f}%{h:>4}{n:>8}{nd:>7}   (too few)')
                    continue
                flag = ' *' if t is not None and abs(t) >= 2 else ''
                print(f'{100*th:>6.0f}%{h:>4}{n:>8}{nd:>7}'
                      f'{100*ex:>9.2f}%{(t if t else float("nan")):>8.2f}'
                      f'{(tn if tn else float("nan")):>8.2f}'
                      f'{100*base:>9.2f}%{flag}', flush=True)
        print()
    for strict in (False, True):
        tag = ('IDIOSYNCRATIC + market NOT down' if strict
               else 'IDIOSYNCRATIC (name vs peers)')
        for direction, arrow in (('drop', f'{tag} - AFTER A DROP'),
                                 ('spike', f'{tag} - AFTER A SPIKE')):
            print(f'=== {arrow} ===')
            print(f"{'thresh':>7}{'h':>4}{'events':>8}{'dates':>7}"
                  f"{'excess':>10}{'t(NW)':>8}{'t(raw)':>8}{'baseline':>10}")
            print('-' * 62)
            for th in args.thresholds:
                for h in args.horizons:
                    n, nd, ex, t, base, tn = study_idiosyncratic(
                        close, opens, th, h, direction, require_market_up=strict)
                    if ex is None:
                        print(f'{100*th:>6.0f}%{h:>4}{n:>8}{nd:>7}   (too few)')
                        continue
                    flag = ' *' if t is not None and abs(t) >= 2 else ''
                    print(f'{100*th:>6.0f}%{h:>4}{n:>8}{nd:>7}'
                          f'{100*ex:>9.2f}%{(t if t else float("nan")):>8.2f}'
                          f'{(tn if tn else float("nan")):>8.2f}'
                          f'{100*base:>9.2f}%{flag}', flush=True)
            print()

    print('* = |t(NW)| >= 2. t(raw) ignores that consecutive dates\' forward')
    print('  windows overlap by h-1 days and is shown only to expose the gap.')
    print('  excess is over the same names\' unconditional forward return, so')
    print('  drift is already removed.')
    print('SURVIVORSHIP: names that fell and never recovered are absent from')
    print('  this universe entirely. That biases "drops bounce" upward and')
    print('  cannot be corrected from inside the sample.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
