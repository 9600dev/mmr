"""Candidate registry (D02): ``research/lowfreq/<family>/candidates.yaml``.

Schema (one mapping per family)::

    family: xasset
    universe: etf | equity            # which loader (D04 / D05)
    dev_period: {start: 2007-07-01, end: 2024-06-28}
    walk_forward: {train_days: 1826, test_days: 365}   # calendar days
    warmup_days: 400                  # calendar days of history loaded before a window
    top_n: 500                        # equity only
    candidates:
      - id: etf_ew
        role: candidate | benchmark
        hypothesis: ...
        rationale: ...
        strategy: {module: trader.research.lowfreq.reference_strategies, class: EtfEqualWeight}
        params: {LOOKBACK: [126, 252]}   # grid: name -> list of values ({} = no grid)
        default_params: {LOOKBACK: 252}  # the pre-registered cell (full-period report)
        universe: etf                    # optional override
        tickers: [SPY, IEF]              # optional ETF subset (default: all)
        benchmarks: [etf_ew, etf_6040]   # ids of benchmark-role entries
        warmup_days: 400                 # optional override
        registered_at: 2026-09-28

The registry-entry sha256 is the hash of the entry's canonical JSON (sorted
keys), so re-ordering the YAML does not create a new trial while changing any
field does.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import itertools
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from trader.research.lowfreq import config as _cfg

ROLES = ('candidate', 'benchmark')
# One path segment of ``research/lowfreq/<family>/`` or ``results/<id>.json``:
# lower-case letters, digits, underscore. Anything else is refused before any
# filesystem access (D02: the target names files under the family directory).
SEGMENT_RE = re.compile(r'^[a-z0-9_]+$')


def is_safe_segment(s: Any) -> bool:
    return isinstance(s, str) and bool(SEGMENT_RE.match(s))


class RegistryError(ValueError):
    pass


def canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(',', ':'), default=str)


def entry_sha256(entry: Dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json(entry).encode('utf-8')).hexdigest()


@dataclass
class Family:
    name: str
    path: Path
    universe: str
    dev_start: dt.date
    dev_end: dt.date
    train_days: int
    test_days: int
    warmup_days: int
    top_n: int
    raw: Dict[str, Any]
    entries: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    @property
    def ledger_path(self) -> Path:
        return self.path / 'trials.jsonl'

    @property
    def results_dir(self) -> Path:
        return self.path / 'results'

    def get(self, cid: str) -> Dict[str, Any]:
        if cid not in self.entries:
            raise RegistryError(
                f'{cid!r} is not registered in {self.path / "candidates.yaml"}; '
                f'evaluate refuses unregistered ids (D02). Known: {sorted(self.entries)}')
        return self.entries[cid]


def _date(v, what: str) -> dt.date:
    if isinstance(v, dt.datetime):
        return v.date()
    if isinstance(v, dt.date):
        return v
    try:
        return dt.date.fromisoformat(str(v))
    except ValueError as exc:
        raise RegistryError(f'{what}: bad date {v!r}') from exc


def load_family(name: str, root: Optional[Path] = None) -> Family:
    if not is_safe_segment(name):
        raise RegistryError(f'family {name!r} must match {SEGMENT_RE.pattern}')
    root = root or _cfg.research_root()
    path = root / name
    reg = path / 'candidates.yaml'
    if not reg.exists():
        raise RegistryError(f'no registry at {reg}')
    with open(reg, 'r') as fh:
        raw = yaml.safe_load(fh) or {}
    if not isinstance(raw, dict):
        raise RegistryError(f'{reg} must be a mapping')
    universe = str(raw.get('universe', ''))
    if universe not in ('etf', 'equity'):
        raise RegistryError(f'{reg}: universe must be etf or equity, got {universe!r}')
    dev = raw.get('dev_period') or {}
    wf = raw.get('walk_forward') or {}
    fam = Family(
        name=name, path=path, universe=universe,
        dev_start=_date(dev.get('start'), 'dev_period.start'),
        dev_end=_date(dev.get('end'), 'dev_period.end'),
        train_days=int(wf.get('train_days', 0)), test_days=int(wf.get('test_days', 0)),
        warmup_days=int(raw.get('warmup_days', 400)), top_n=int(raw.get('top_n', 500)),
        raw=raw)
    if fam.train_days <= 0 or fam.test_days <= 0:
        raise RegistryError(f'{reg}: walk_forward.train_days/test_days must be positive')
    seen: Dict[str, Dict[str, Any]] = {}
    for e in raw.get('candidates') or []:
        if not isinstance(e, dict) or 'id' not in e:
            raise RegistryError(f'{reg}: every candidate needs an id')
        cid = str(e['id'])
        if cid in seen:
            raise RegistryError(f'{reg}: duplicate id {cid}')
        validate_entry(e, reg)
        seen[cid] = e
    fam.entries = seen
    for cid, e in seen.items():
        for b in e.get('benchmarks') or []:
            if b not in seen or seen[b].get('role') != 'benchmark':
                raise RegistryError(f'{reg}: {cid} names benchmark {b!r} which is not a benchmark-role entry')
    return fam


def validate_entry(e: Dict[str, Any], reg: Path) -> None:
    cid = e['id']
    if not is_safe_segment(cid):
        raise RegistryError(f'{reg}: id {cid!r} must match {SEGMENT_RE.pattern} (it names results/<id>.json)')
    if e.get('role') not in ROLES:
        raise RegistryError(f'{reg}: {cid}: role must be one of {ROLES}')
    strat = e.get('strategy') or {}
    if not strat.get('module') or not strat.get('class'):
        raise RegistryError(f'{reg}: {cid}: strategy needs module and class')
    grid = e.get('params') or {}
    if not isinstance(grid, dict) or any(not isinstance(v, list) or not v for v in grid.values()):
        raise RegistryError(f'{reg}: {cid}: params must map names to non-empty lists')
    if any(not isinstance(k, str) for k in grid):
        raise RegistryError(f'{reg}: {cid}: params keys must be strings (tunable names), got '
                            f'{sorted(repr(k) for k in grid if not isinstance(k, str))}')
    for k in ('hypothesis', 'rationale', 'registered_at'):
        if k not in e:
            raise RegistryError(f'{reg}: {cid}: missing {k}')
    dp = e.get('default_params') or {}
    for k, v in dp.items():
        if k in grid and v not in grid[k]:
            raise RegistryError(f'{reg}: {cid}: default_params.{k}={v!r} is not in its grid')


def expand_grid(entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    grid = entry.get('params') or {}
    if not grid:
        return [{}]
    keys = sorted(grid)
    return [dict(zip(keys, combo)) for combo in itertools.product(*(grid[k] for k in keys))]


def default_cell(entry: Dict[str, Any]) -> Dict[str, Any]:
    cells = expand_grid(entry)
    dp = entry.get('default_params') or {}
    if not dp:
        return cells[0]
    for c in cells:
        if all(c.get(k) == v for k, v in dp.items()):
            return c
    return dict(cells[0], **dp)
