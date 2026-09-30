"""D07 terminal-loss stress for the insider champion AND its decisive benchmark.

D07: a holding that stops printing is sold at its last stored print, and an
equity family must report a terminal-loss stress on those exits before a
champion is believed. This script re-runs the champion's pre-registered cell
and ``eq_ew_pit`` over the full dev period through the IDENTICAL harness path
(``engine.make_strategy`` / ``engine._config`` / ``Backtester.run_panel``,
same costs, same terminal prints, benchmark on the candidate's published
coverage), identifies every forced exit from the fill log, reconciles the
count and gross value against the backtester's own ``panel_forced_exits`` /
``panel_forced_exit_value`` diagnostics (refusing to write on a mismatch),
and re-marks the exits to ZERO pathwise:

    stressed E'_t = E'_{t-1} * (E_t - P_t) / E_{t-1}

where ``P_t`` is the net proceeds of the forced exits booked on day t, i.e.
the book at close t is worth what it was worth WITHOUT the sale proceeds and
everything after scales proportionally (the cash would have been redeployed
with the rest of the book). Two variants: ``stressed_all`` marks EVERY
forced exit to zero (the bound), ``stressed_collapses`` marks only the exits
whose terminal close is below ``COLLAPSE_FRACTION`` of the name's trailing
252-session high (the "genuine collapses" of D07; an acquisition closing near
its high keeps its proceeds).

Writes ``results/_d07_stress.json``; ``make_report.py`` cross-checks the
unstressed full-period numbers here against ``results/<champion>.json``.

    uv run python research/lowfreq/insider/d07_stress.py [champion_id]
"""
from __future__ import annotations

import datetime as dt
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, cast

import numpy as np
import pandas as pd

from trader.research.lowfreq import config as _cfg
from trader.research.lowfreq import engine, stats
from trader.research.lowfreq.costs import base_costs
from trader.research.lowfreq.loaders import PricePanel, load_equity_panel
from trader.research.lowfreq.registry import default_cell, load_family
from trader.simulation.backtester import Backtester

HERE = Path(__file__).resolve().parent
BENCH = 'eq_ew_pit'
DEFAULT_CHAMPION = 'ins_net_buy_ratio'
COLLAPSE_FRACTION = 0.5      # terminal close < 50 % of the trailing-252-session high = a genuine collapse
TRAILING_SESSIONS = 252
RECONCILE_REL_TOL = 1e-6


# ---------------------------------------------------------------- pure kernel
def stressed_returns(equity: pd.Series, proceeds: pd.Series) -> pd.Series:
    """Daily returns of the equity path with the forced-exit proceeds removed
    on the day they were booked (see module docstring). ``proceeds`` is
    indexed by day (missing = 0). Refuses negative proceeds and proceeds that
    exceed the equity holding them (a book cannot lose more than it has)."""
    eq = equity.astype(float)
    if len(eq) < 2:
        raise ValueError('need at least two equity marks')
    if (eq <= 0).any():
        raise ValueError('equity must be positive everywhere')
    p = proceeds.reindex(eq.index).fillna(0.0).astype(float)
    if (p < 0).any():
        raise ValueError('forced-exit proceeds must be >= 0')
    if (p > eq * (1 + 1e-12)).any():
        raise ValueError('forced-exit proceeds exceed the equity that holds them')
    r = (eq - p) / eq.shift(1) - 1.0
    return r.iloc[1:]


def collapse_flags(closes: pd.DataFrame, exits: pd.DataFrame, fraction: float = COLLAPSE_FRACTION,
                   sessions: int = TRAILING_SESSIONS) -> pd.Series:
    """True for exits whose terminal close is below ``fraction`` of the name's
    highest close over the ``sessions`` sessions ending at the terminal print.
    An exit whose name has no close history is NOT a collapse (unknown, kept)."""
    out: List[bool] = []
    for _, row in exits.iterrows():
        t, ts = row['ticker'], pd.Timestamp(row['date'])
        if t not in closes.columns:
            out.append(False)
            continue
        hist = closes[t].loc[:ts].dropna()
        hist = hist[hist > 0]
        if len(hist) == 0:
            out.append(False)
            continue
        window = hist.iloc[-sessions:]
        out.append(bool(float(hist.iloc[-1]) < fraction * float(window.max())))
    return pd.Series(out, index=exits.index, dtype=bool)


def forced_exits_from_trades(trades, terminal: Dict[Any, pd.Timestamp], closes: pd.DataFrame,
                             slip: float) -> pd.DataFrame:
    """The D07 forced exits in a fill log (``trades`` in booking order).

    The backtester books a forced exit on the name's terminal print AFTER
    every scheduled open fill of that bar, at the bar's CLOSE x (1 - slip).
    So a trade is a forced exit iff it is a SELL on a name whose terminal
    print is that bar, its fill is the close-based one, and no DEFINITELY
    scheduled trade (a BUY, a trade on a non-terminal name, or a SELL priced
    off the open) is booked after it on the same bar. The price test alone
    overcounted the champion by six (names that left the book at the OPEN of
    the very session they last printed) and the ordering rule by one more
    (such a fill on a bar whose open equals its close); the caller still
    reconciles count and gross value against the backtester's own
    diagnostics and refuses on any disagreement."""
    by_ts: Dict[pd.Timestamp, List[Any]] = {}
    for t in trades:
        by_ts.setdefault(pd.Timestamp(t.timestamp), []).append(t)

    def at_close(t, ts) -> bool:
        c = t.conid
        if c not in closes.columns or ts not in closes.index:
            return False
        close_px = float(closes[c].loc[ts])
        return bool(np.isfinite(close_px)) and abs(float(t.price) - close_px * (1.0 - slip)) <= 1e-9 * max(1.0, close_px)

    def may_be_forced(t, ts) -> bool:
        return str(t.action).endswith('SELL') and terminal.get(t.conid) == ts and at_close(t, ts)

    rows = []
    for ts in sorted(by_ts):
        day = by_ts[ts]
        last_scheduled = max([i for i, t in enumerate(day) if not may_be_forced(t, ts)], default=-1)
        chosen: Dict[Any, Any] = {}
        for i, t in enumerate(day):
            if i > last_scheduled and may_be_forced(t, ts):
                chosen[t.conid] = t                      # the last SELL per name wins
        for c in sorted(chosen, key=str):
            t = chosen[c]
            fill = float(t.price)
            rows.append({'date': str(ts.date()), 'ticker': str(c), 'quantity': float(t.quantity), 'fill': fill,
                         'commission': float(t.commission),
                         'net_proceeds': float(t.quantity) * fill - float(t.commission)})
    return pd.DataFrame(rows, columns=['date', 'ticker', 'quantity', 'fill', 'commission', 'net_proceeds'])


def daily_proceeds(exits: pd.DataFrame, mask: Optional[pd.Series] = None) -> pd.Series:
    sel = exits if mask is None else exits[mask.to_numpy()]
    if len(sel) == 0:
        return pd.Series(dtype=float)
    s = sel.groupby(pd.to_datetime(sel['date']))['net_proceeds'].sum()
    return s.astype(float)


def active_block(cand: pd.Series, bench: pd.Series) -> Dict[str, Any]:
    active = (cand - bench.reindex(cand.index).fillna(0.0)).dropna()
    t, lags = stats.newey_west_t(active)
    return {'ann_return': stats.ann_mean(active), 'sharpe': stats.ann_sharpe(active), 't_nw': t, 'nw_lags': lags,
            'max_drawdown_candidate': stats.max_drawdown(cand), 'max_drawdown_benchmark': stats.max_drawdown(bench)}


# ---------------------------------------------------------------- one book
def run_book(entry: Dict[str, Any], cell: Dict[str, Any], panel: PricePanel, data_start, meas_start, end_excl,
             costs, coverage: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
    """Mirror of ``engine.run_window`` that keeps the fill log and the
    backtester diagnostics (which run_window folds into its summary)."""
    window = panel.sliced(data_start, end_excl)
    frames = window.frames()
    wide = engine.wide_panel(frames)
    cov_in = (coverage.reindex(index=wide.index, columns=wide['close'].columns).fillna(False).astype(bool)
              if coverage is not None else None)
    strat = engine.make_strategy(entry, cell, window, coverage=cov_in)
    own_cov = engine.candidate_coverage(engine.make_strategy(entry, cell, window, coverage=cov_in).precompute_panel(wide))
    terminal = engine.terminal_prints(panel)                       # from the FULL loaded panel (D07)
    bt = Backtester(cast(Any, engine._NoStorage()), engine._config(data_start, end_excl, costs, terminal))
    res = bt.run_panel(strat, cast(Any, list(frames.keys())), frames=frames)
    eq = res.equity_curve.astype(float)
    diag = dict(getattr(res, 'diagnostics', None) or {})
    slip = costs.slippage_bps / 10_000.0
    exits = forced_exits_from_trades(res.trades, terminal, window.close, slip)
    gross_from_fills = float((exits['quantity'] * exits['fill'] / (1.0 - slip)).sum()) if len(exits) else 0.0
    rec = {'fills_forced_exits': int(len(exits)), 'diag_forced_exits': diag.get('panel_forced_exits'),
           'fills_gross_value': gross_from_fills, 'diag_gross_value': diag.get('panel_forced_exit_value')}
    ok_count = rec['diag_forced_exits'] == rec['fills_forced_exits']
    dv = rec['diag_gross_value']
    ok_value = (dv is not None and abs(gross_from_fills - float(dv)) <= RECONCILE_REL_TOL * max(1.0, abs(float(dv))))
    if not (ok_count and ok_value):
        raise RuntimeError(f'{entry["id"]}: forced exits from the fill log do not reconcile with the '
                           f'backtester diagnostics: {rec}')
    rec['reconciled'] = True
    ms = pd.Timestamp(meas_start)
    unstressed = eq.pct_change().iloc[1:]
    flags = collapse_flags(window.close, exits)
    exits = exits.assign(collapse=flags.to_numpy() if len(exits) else [])
    all_p = daily_proceeds(exits)
    col_p = daily_proceeds(exits, flags) if len(exits) else pd.Series(dtype=float)
    s_all = stressed_returns(eq, all_p)
    s_col = stressed_returns(eq, col_p)

    def measured(r: pd.Series) -> pd.Series:
        return r[r.index >= ms].dropna()

    cap = engine.INITIAL_CAPITAL
    in_meas = pd.to_datetime(exits['date']) >= ms if len(exits) else pd.Series(dtype=bool)
    return {
        'id': entry['id'], 'cell': dict(cell), 'coverage_published': own_cov is not None,
        'returns': {'unstressed': measured(unstressed), 'stressed_all': measured(s_all),
                    'stressed_collapses': measured(s_col)},
        'summary': {'unstressed': stats.summarise(measured(unstressed)),
                    'stressed_all': stats.summarise(measured(s_all)),
                    'stressed_collapses': stats.summarise(measured(s_col))},
        'forced_exits': int(len(exits)),
        'forced_exit_value_fraction': (float(dv) / cap) if dv is not None else None,
        'forced_exit_net_proceeds_fraction': float(exits['net_proceeds'].sum()) / cap if len(exits) else 0.0,
        'forced_exits_measured_period': int(in_meas.sum()) if len(exits) else 0,
        'collapse_exits': int(flags.sum()) if len(exits) else 0,
        'collapse_net_proceeds_fraction': (float(exits.loc[flags.to_numpy(), 'net_proceeds'].sum()) / cap
                                           if len(exits) else 0.0),
        'reconciliation': rec,
        'exits': exits.to_dict(orient='records'),
        'coverage_frame': own_cov,
    }


def main(argv: Optional[List[str]] = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    champion = argv[0] if argv else DEFAULT_CHAMPION
    t0 = time.time()
    fam = load_family('insider', _cfg.research_root())
    entry, bench = fam.get(champion), fam.get(BENCH)
    if entry['role'] != 'candidate':
        raise SystemExit(f'{champion} is not a candidate-role entry')
    cell = default_cell(entry)
    warm = dt.timedelta(days=fam.warmup_days)
    dev_start, dev_end = fam.dev_start, fam.dev_end
    end_excl = dev_end + dt.timedelta(days=1)
    costs = base_costs(fam.universe)
    panel = load_equity_panel(dev_start - warm, dev_end, top_n=fam.top_n)
    cand = run_book(entry, cell, panel, dev_start - warm, dev_start, end_excl, costs)
    # The benchmark runs on the candidate's published coverage, as in evaluate.py.
    ben = run_book(bench, default_cell(bench), panel, dev_start - warm, dev_start, end_excl, costs,
                   coverage=cand['coverage_frame'])
    active = {k: active_block(cand['returns'][k], ben['returns'][k]) for k in ('unstressed', 'stressed_all', 'stressed_collapses')}

    def book(b: Dict[str, Any]) -> Dict[str, Any]:
        return {k: v for k, v in b.items() if k not in ('returns', 'coverage_frame')}

    out = {
        'generated_at': dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds'),
        'rule': ('D07 terminal-loss stress: every forced exit (a holding sold at its last stored print) is '
                 're-marked to ZERO on the day it was booked, pathwise (E\'_t = E\'_{t-1} (E_t - P_t)/E_{t-1}); '
                 '"stressed_all" marks every forced exit, "stressed_collapses" only those whose terminal close '
                 f'is below {COLLAPSE_FRACTION:.0%} of the trailing-{TRAILING_SESSIONS}-session high. Candidate and '
                 'benchmark are stressed on their OWN forced exits through the identical execution path.'),
        'window': {'start': str(dev_start), 'end': str(dev_end), 'warmup_days': fam.warmup_days,
                   'measured_days': int(len(cand['returns']['unstressed']))},
        'costs': costs.as_dict(),
        'collapse_definition': {'fraction_of_trailing_high': COLLAPSE_FRACTION, 'trailing_sessions': TRAILING_SESSIONS},
        'candidate': book(cand), 'benchmark': book(ben), 'active': active,
        'elapsed_s': round(time.time() - t0, 1),
    }
    path = HERE / 'results' / '_d07_stress.json'
    path.write_text(json.dumps(out, indent=1, sort_keys=True, default=str))
    a = active
    print(f"wrote {path} in {out['elapsed_s']} s")
    print(f"candidate {champion}: forced exits {cand['forced_exits']} ({cand['forced_exit_value_fraction']:.2%} of capital), "
          f"collapses {cand['collapse_exits']}; benchmark {BENCH}: {ben['forced_exits']} "
          f"({ben['forced_exit_value_fraction']:.2%}), collapses {ben['collapse_exits']}")
    for k in ('unstressed', 'stressed_all', 'stressed_collapses'):
        print(f"  {k:19s} cand CAGR {cand['summary'][k]['cagr']:+.2%} bench CAGR {ben['summary'][k]['cagr']:+.2%} "
              f"active {a[k]['ann_return']:+.2%}/yr t(NW) {a[k]['t_nw']:.2f}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
