#!/usr/bin/env python3
"""D07 terminal-loss stress for one evaluated equity candidate (C03, PREREG.md
addendum R2 item 5).

    uv run python research/lowfreq/equity/stress_d07.py <candidate_id>

Re-runs the candidate's OOS fold windows (the fold table of its
``results/<id>.json``) and the decisive benchmark through the SAME engine
path (``engine.make_strategy`` / ``engine._config`` / ``Backtester.run_panel``,
identical costs, D07 terminal prints from the complete loaded panel). A
forced exit is a SELL fill dated at the name's terminal print. The stress
removes the proceeds of every forced exit from equity from that day on - the
name is marked to ZERO instead of its last print - (i) for the candidate
alone and (ii) for candidate and decisive benchmark alike. Writes
``results/<id>__d07_stress.json`` with the event list and stressed metrics.
Read-only: the ledger is not touched (this is not an ``evaluate`` call).
"""
from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, cast

import pandas as pd

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from trader.research.lowfreq import engine, stats                       # noqa: E402
from trader.research.lowfreq.costs import base_costs                     # noqa: E402
from trader.research.lowfreq.loaders import load_equity_panel, to_date   # noqa: E402
from trader.research.lowfreq.registry import default_cell, load_family   # noqa: E402
from trader.simulation.backtester import Backtester                      # noqa: E402


def _run(entry, cell, panel, data_start, end_excl, costs, terminal, coverage=None):
    window = panel.sliced(data_start, end_excl)
    frames = window.frames()
    wide = engine.wide_panel(frames)
    cov_in = (coverage.reindex(index=wide.index, columns=wide['close'].columns).fillna(False).astype(bool)
              if coverage is not None else None)
    strat = engine.make_strategy(entry, cell, window, coverage=cov_in)
    own_cov = engine.candidate_coverage(engine.make_strategy(entry, cell, window, coverage=cov_in).precompute_panel(wide))
    bt = Backtester(cast(Any, engine._NoStorage()), engine._config(to_date(data_start), to_date(end_excl), costs, terminal))
    res = bt.run_panel(strat, cast(Any, list(frames.keys())), frames=frames)
    return res, own_cov


def _forced(res, terminal) -> List[Dict[str, Any]]:
    out = []
    for t in res.trades:
        ts = pd.Timestamp(t.timestamp)
        if str(t.action).endswith('SELL') and terminal.get(t.conid) == ts:
            out.append({'date': str(ts.date()), 'ticker': str(t.conid), 'quantity': float(t.quantity),
                        'price': float(t.price), 'proceeds': float(t.quantity * t.price - t.commission)})
    return out


def _stressed_returns(res, events, meas_start) -> tuple:
    eq = res.equity_curve.astype(float)
    lost = pd.Series(0.0, index=eq.index)
    for e in events:
        lost.loc[pd.Timestamp(e['date']):] += e['proceeds']       # proceeds never received, from that day on
    eq_s = eq - lost
    ms = pd.Timestamp(meas_start)
    raw = eq.pct_change()[eq.index >= ms].dropna()
    stressed = eq_s.pct_change()[eq_s.index >= ms].dropna()
    return raw, stressed


def _m(s: pd.Series) -> Dict[str, Any]:
    t, lags = stats.newey_west_t(s)
    return {'sharpe': stats.ann_sharpe(s), 'ann_mean': stats.ann_mean(s), 't_nw': t, 'nw_lags': lags,
            'max_drawdown': stats.max_drawdown(s), 'days': int(len(s))}


def main(cid: str) -> int:
    fam = load_family('equity', HERE.parent)
    results_path = HERE / 'results' / f'{cid}.json'
    r = json.loads(results_path.read_text())
    entry = fam.get(cid)
    assert engine.load_strategy_class(entry['strategy']['module'], entry['strategy']['class'])
    bench_id = r['decisive_per_fold'][0]
    bench = fam.get(bench_id)
    w = r['window']
    warm = dt.timedelta(days=int(w['warmup_days']))
    dev_start, dev_end = to_date(w['start']), to_date(w['end'])
    costs = base_costs(r['universe'])
    panel = load_equity_panel(dev_start - warm, dev_end, top_n=int(entry.get('top_n', fam.top_n)))
    terminal = engine.terminal_prints(panel)

    cand_raw, cand_s, ben_raw, ben_s, events, per_fold = [], [], [], [], [], []
    for f in r['folds']:
        test_start, test_end = to_date(f['test_start']), to_date(f['test_end'])
        data_start = test_start - warm
        res_c, cov = _run(entry, f['chosen'], panel, data_start, test_end, costs, terminal)
        ev_c = _forced(res_c, terminal)
        res_b, _ = _run(bench, default_cell(bench), panel, data_start, test_end, costs, terminal, coverage=cov)
        ev_b = _forced(res_b, terminal)
        c_raw, c_s = _stressed_returns(res_c, ev_c, test_start)
        b_raw, b_s = _stressed_returns(res_b, ev_b, test_start)
        cand_raw.append(c_raw); cand_s.append(c_s); ben_raw.append(b_raw); ben_s.append(b_s)
        for e in ev_c:
            events.append(dict(e, fold=f['fold'], book='candidate'))
        for e in ev_b:
            events.append(dict(e, fold=f['fold'], book=bench_id))
        per_fold.append({'fold': f['fold'], 'test_start': f['test_start'], 'test_end': f['test_end'],
                         'candidate_forced_exits': len(ev_c), 'candidate_forced_proceeds': sum(e['proceeds'] for e in ev_c),
                         'benchmark_forced_exits': len(ev_b), 'benchmark_forced_proceeds': sum(e['proceeds'] for e in ev_b)})
    cat = lambda parts: pd.concat(parts).sort_index()   # noqa: E731
    c_raw, c_s, b_raw, b_s = cat(cand_raw), cat(cand_s), cat(ben_raw), cat(ben_s)
    b_raw = b_raw.reindex(c_raw.index).fillna(0.0)
    b_s = b_s.reindex(c_s.index).fillna(0.0)
    out = {
        'id': cid, 'decisive_benchmark': bench_id, 'source_results': str(results_path), 'source_verdict': r['verdict'],
        'rule': 'D07 stress: proceeds of every SELL fill dated at a name\'s terminal print are removed from equity '
                'from that day on (the name marked to zero); (i) candidate only, (ii) candidate and benchmark alike',
        'command': f'uv run python research/lowfreq/equity/stress_d07.py {cid}',
        'reproduction_check': {'oos_sharpe_from_results_json': r['oos']['sharpe'],
                               'oos_sharpe_recomputed_unstressed': stats.ann_sharpe(c_raw),
                               'active_ann_return_from_results_json': r['active']['ann_return'],
                               'active_ann_return_recomputed_unstressed': stats.ann_mean(c_raw - b_raw)},
        'per_fold': per_fold,
        'events': events,
        'candidate': {'unstressed': _m(c_raw), 'stressed': _m(c_s)},
        'benchmark': {'unstressed': _m(b_raw), 'stressed': _m(b_s)},
        'active': {'unstressed': _m(c_raw - b_raw), 'i_candidate_stressed_only': _m(c_s - b_raw),
                   'ii_both_stressed': _m(c_s - b_s)},
        'total_candidate_forced_exits': sum(1 for e in events if e['book'] == 'candidate'),
        'total_candidate_forced_proceeds': sum(e['proceeds'] for e in events if e['book'] == 'candidate'),
        'total_benchmark_forced_exits': sum(1 for e in events if e['book'] == bench_id),
        'total_benchmark_forced_proceeds': sum(e['proceeds'] for e in events if e['book'] == bench_id),
        'believed_if': 'active.ii_both_stressed.ann_mean > 0 (PREREG addendum R2 item 5)',
        'passes_stress': bool((_m(c_s - b_s)['ann_mean'] or 0.0) > 0.0),
    }
    path = HERE / 'results' / f'{cid}__d07_stress.json'
    path.write_text(json.dumps(out, indent=1, sort_keys=True, default=str))
    print(json.dumps({k: out[k] for k in ('id', 'reproduction_check', 'candidate', 'benchmark', 'active',
                                          'total_candidate_forced_exits', 'total_candidate_forced_proceeds',
                                          'total_benchmark_forced_exits', 'passes_stress')}, indent=1, default=str))
    print('written', path)
    return 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1]))
