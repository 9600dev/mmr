"""Pre-registration calibration for the insider family (C04): how many covered
names QUALIFY under each registered rule on each monthly rebalance of the
real dev panel, and where the qualifiers sit in the market-cap distribution.

COUNTS ONLY. No return, equity curve or benchmark comparison is computed:
this is what fixes the thresholds in PREREG.md without looking at outcomes.
Writes ``results/_calibration_counts.json`` next to the harness results.

    uv run python research/lowfreq/insider/calibrate_counts.py
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from trader.research.lowfreq import engine  # noqa: E402
from trader.research.lowfreq import insider_signals as ins  # noqa: E402
from trader.research.lowfreq.loaders import load_equity_panel  # noqa: E402
from trader.research.lowfreq.registry import load_family  # noqa: E402
from strategies.lowfreq_insider_base import market_cap  # noqa: E402


def _q(values: List[float], q: float) -> float:
    return float(np.quantile(np.asarray(values, dtype=float), q)) if values else float('nan')


def main(start: str = '2016-08-01', end: str = '2024-06-28') -> Dict[str, Any]:
    t0 = time.time()
    panel = load_equity_panel(start, end)
    t1 = time.time()
    wide = engine.wide_panel(panel.frames())
    close = wide['close'].astype(float)
    fields = {k: v.reindex(index=close.index, columns=close.columns) for k, v in panel.fields.items()}
    mcap = market_cap(close, fields)
    fam = load_family('insider', ROOT / 'research' / 'lowfreq')
    out: Dict[str, Any] = {'panel': {'start': start, 'end': end, 'sessions': int(len(wide)),
                                     'tickers': int(close.shape[1]), 'load_s': round(t1 - t0, 1)},
                           'rules': {}}
    due_idx: List[int] = []
    last: Dict[str, Any] = {}

    def measure(label: str, strat) -> None:
        strat.research_data = {'fields': panel.fields, 'signals': panel.signals, 'universe': panel.universe}
        t2 = time.time()
        state = strat.precompute_panel(wide)
        t3 = time.time()
        last['coverage'] = state['coverage']
        if not due_idx:
            due_idx.extend(i for i in range(len(wide) - 1) if strat.due(i, wide))
        ks: List[float] = []
        ns: List[float] = []
        pct: List[float] = []
        for i in due_idx:
            cov = state['coverage'].iloc[i].astype(bool)
            sig = (state['sig'].iloc[i] > 0) & cov
            ks.append(float(sig.sum()))
            ns.append(float(cov.sum()))
            if sig.any():
                ranks = mcap.iloc[i][cov].rank(pct=True)
                med = ranks.reindex(sig.index[sig]).median()
                if pd.notna(med):
                    pct.append(float(med))
        out['rules'][label] = {
            'precompute_s': round(t3 - t2, 1), 'rebalances': len(ks),
            'K_min': _q(ks, 0.0), 'K_p25': _q(ks, 0.25), 'K_median': _q(ks, 0.5), 'K_p75': _q(ks, 0.75),
            'K_max': _q(ks, 1.0), 'months_below_min_signal': int(sum(k < strat.MIN_SIGNAL_NAMES for k in ks)),
            'N_covered_median': _q(ns, 0.5),
            'qualifier_mcap_percentile_median': (float(np.median(pct)) if pct else None),
        }

    for cid, e in fam.entries.items():
        if e['role'] != 'candidate' or cid.startswith('ins_size_matched_'):
            continue
        measure(cid, engine.make_strategy(e, {}, panel))
    # the H6 floor that was REJECTED at calibration (1 bp), kept for the record
    from strategies.lowfreq_insider_value_to_mcap import ValueToMcapOverlay
    one_bp = type('ValueToMcapAt1bp', (ValueToMcapOverlay,), {'VALUE_TO_MCAP_MIN': 1e-4})
    measure('ins_value_to_mcap@1bp_rejected', one_bp())
    # H6 threshold support: distribution of trailing purchase value / market cap among purchasers
    frames = ins.signal_frames(pd.DatetimeIndex(close.index), [str(c) for c in close.columns], 63)
    ratio = ins.trailing_sum(frames['p_value'], 63) / mcap
    has = ins.trailing_sum(frames['p_filings'], 63) >= 1
    vals = ratio.where(has).iloc[due_idx].stack().dropna()
    out['value_to_mcap_among_purchasers'] = {f'q{int(q * 100):02d}': float(vals.quantile(q))
                                             for q in (0.1, 0.25, 0.5, 0.75, 0.9)}
    out['value_to_mcap_among_purchasers']['n'] = int(len(vals))
    coverage = last['coverage'].astype(bool)
    out['mcap_coverage_share_of_covered'] = float((mcap.notna() & coverage).sum().sum() / max(coverage.sum().sum(), 1))
    out['elapsed_s'] = round(time.time() - t0, 1)
    dest = ROOT / 'research' / 'lowfreq' / 'insider' / 'results' / '_calibration_counts.json'
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(out, indent=1, sort_keys=True))
    print(json.dumps(out, indent=1, sort_keys=True))
    return out


if __name__ == '__main__':
    main(*sys.argv[1:3])
