"""Append-only trial ledger (D02): ``research/lowfreq/<family>/trials.jsonl``.

One schema-complete line per evaluate call (``LEDGER_FIELDS``), written
BEFORE any output, including runs that error or are refused. Trial count for
DSR = distinct registry-entry hashes with role=candidate. Nothing here ever
rewrites or deletes a line.
"""
from __future__ import annotations

import datetime as dt
import json
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional

from trader.research.lowfreq import config as _cfg

HARNESS_VERSION = 2

# Every row carries exactly these keys (None when not applicable), so a
# refused id, an errored run and a completed run are one schema (D02).
LEDGER_FIELDS = ('timestamp', 'git_sha', 'harness_version', 'family', 'id', 'role', 'entry_sha256',
                 'strategy_sha256', 'mode', 'grid', 'fixed_params', 'start', 'end', 'n_trials',
                 'metrics', 'verdict', 'status', 'error', 'elapsed_s', 'results_path')
METRIC_FIELDS = ('oos_sharpe', 'oos_cagr', 'oos_max_drawdown', 'active_ann_return', 'active_t_nw',
                 'oos_active_sharpe', 'active_dsr', 'benchmark_oos_sharpe_max', 'active_ann_return_3x',
                 'pbo', 'selection_stability', 'full_sharpe', 'full_min_cash_fraction',
                 'full_scaled_buy_fills', 'oos_days')
STATUSES = ('ok', 'error', 'refused')


def git_sha() -> Optional[str]:
    try:
        return subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=str(_cfg.repo_root()),
                              capture_output=True, text=True, timeout=10).stdout.strip() or None
    except Exception:            # noqa: BLE001 - a missing git is not a research error
        return None


def compose(row: Dict[str, Any]) -> Dict[str, Any]:
    """The schema-complete row ``append`` would write, without writing it."""
    unknown = set(row) - set(LEDGER_FIELDS)
    if unknown:
        raise ValueError(f'ledger row has keys outside the schema: {sorted(unknown)}')
    if row.get('status') not in STATUSES:
        raise ValueError(f'ledger row status must be one of {STATUSES}, got {row.get("status")!r}')
    out: Dict[str, Any] = {k: None for k in LEDGER_FIELDS}
    out.update(row)
    metrics: Dict[str, Any] = {k: None for k in METRIC_FIELDS}
    metrics.update(row.get('metrics') or {})
    out['metrics'] = metrics
    out['timestamp'] = out['timestamp'] or dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')
    out['git_sha'] = out['git_sha'] or git_sha()
    out['harness_version'] = out['harness_version'] or HARNESS_VERSION
    return out


def append(path: Path, row: Dict[str, Any]) -> Dict[str, Any]:
    row = compose(row)
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(row, sort_keys=True, default=str)
    with open(path, 'a', encoding='utf-8') as fh:
        fh.write(line + '\n')
        fh.flush()
    return row


def read(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    out: List[Dict[str, Any]] = []
    with open(path, 'r', encoding='utf-8') as fh:
        for n, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(f'{path}:{n}: corrupt ledger line: {exc}') from exc
    return out


def candidate_hashes(rows: List[Dict[str, Any]]) -> List[str]:
    """Distinct registry-entry hashes of role=candidate rows, first-seen order."""
    seen: List[str] = []
    for r in rows:
        h = r.get('entry_sha256')
        if r.get('role') == 'candidate' and h and h not in seen:
            seen.append(h)
    return seen


def trial_count(rows: List[Dict[str, Any]], including: Optional[str] = None) -> int:
    hashes = candidate_hashes(rows)
    if including and including not in hashes:
        hashes.append(including)
    return len(hashes)


def trial_sharpes(rows: List[Dict[str, Any]], metric: str = 'oos_active_sharpe') -> Dict[str, float]:
    """Latest recorded ``metric`` per distinct candidate hash (for DSR variance)."""
    out: Dict[str, float] = {}
    for r in rows:
        if r.get('role') != 'candidate' or not r.get('entry_sha256'):
            continue
        v = (r.get('metrics') or {}).get(metric)
        if isinstance(v, (int, float)) and v == v:
            out[r['entry_sha256']] = float(v)
    return out
