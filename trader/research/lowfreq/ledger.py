"""Append-only trial ledger (D02): ``research/lowfreq/<family>/trials.jsonl``.

One line per evaluate call, written BEFORE any output, including runs that
error or are refused. Trial count for DSR = distinct registry-entry hashes with
role=candidate. Nothing here ever rewrites or deletes a line.
"""
from __future__ import annotations

import datetime as dt
import json
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional

from trader.research.lowfreq import config as _cfg

HARNESS_VERSION = 1


def git_sha() -> Optional[str]:
    try:
        return subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=str(_cfg.repo_root()),
                              capture_output=True, text=True, timeout=10).stdout.strip() or None
    except Exception:            # noqa: BLE001 - a missing git is not a research error
        return None


def append(path: Path, row: Dict[str, Any]) -> Dict[str, Any]:
    row = dict(row)
    row.setdefault('timestamp', dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds'))
    row.setdefault('git_sha', git_sha())
    row.setdefault('harness_version', HARNESS_VERSION)
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
