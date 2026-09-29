"""Target weights for one decision session, by the research code path (D08).

The weights a paper sleeve trades must be the weights the harness backtest
would have issued on the same session, or the out-of-sample evidence the
sleeve accumulates is evidence about a different rule. So nothing here
re-implements the strategy: the registry entry is loaded with
``trader.research.lowfreq.registry``, the panel with the harness loaders, the
instance with ``engine.make_strategy``, and the instruction with the same
``precompute_panel`` -> ``on_panel(index of as_of)`` -> ``normalise_weights``
sequence ``Backtester.run_panel`` executes (see ``run_panel`` step 3).

Databases: the harness reads ONLY the databases named by
``LOWFREQ_RESEARCH_CONFIG`` (D03). Inside the container that variable is
pointed at ``~/.config/mmr/trader.yaml`` so the incremental refresh and this
command see the same ``pit_daily_bars`` / ``fundamentals`` tables, read-only.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, cast

import pandas as pd

from trader.research.lowfreq import config as _cfg
from trader.research.lowfreq import engine, loaders, registry
from trader.research.lowfreq.costs import base_costs
from trader.simulation.panel import normalise_weights

SCHEMA = 'mmr.targets.v1'                    # D08
BENCHMARK_SCHEMA = 'mmr.lowfreq_benchmark.v1'
SLEEVE_RE = re.compile(r'^[a-z0-9_]+$')       # D08: lowercase letters, digits, underscore
DEFAULT_TARGETS_DIR = '~/.local/share/mmr/data/targets'
DEFAULT_MAX_FUNDAMENTALS_LAG_DAYS = 7


class TargetsRefused(RuntimeError):
    """The command refuses (non-zero exit, no file written)."""


@dataclass
class TargetsResult:
    document: Dict[str, Any]                       # the D08 file content
    benchmarks: Dict[str, Optional[Dict[str, float]]]   # benchmark id -> weights (positive only); None = it holds
    path: Optional[Path] = None
    benchmark_path: Optional[Path] = None
    diagnostics: Dict[str, Any] = field(default_factory=dict)


def default_targets_path(sleeve: str, as_of: dt.date, targets_dir: Optional[str] = None) -> Path:
    root = Path(os.path.expanduser(targets_dir or DEFAULT_TARGETS_DIR))
    return root / sleeve / f'{as_of.isoformat()}.json'


def benchmark_sidecar_path(targets_path: Path) -> Path:
    return targets_path.with_name(targets_path.stem + '.benchmark.json')


def validate_targets_document(doc: Dict[str, Any]) -> None:
    """D08 schema rules; raises ``ValueError`` naming the first violation."""
    if not isinstance(doc, dict):
        raise ValueError('targets document must be a mapping')
    if doc.get('schema') != SCHEMA:
        raise ValueError(f'schema must be {SCHEMA!r}, got {doc.get("schema")!r}')
    sleeve = doc.get('sleeve')
    if not isinstance(sleeve, str) or not SLEEVE_RE.match(sleeve):
        raise ValueError(f'sleeve {sleeve!r} must match {SLEEVE_RE.pattern}')
    for key in ('as_of', 'prices_as_of'):
        try:
            dt.date.fromisoformat(str(doc.get(key)))
        except (TypeError, ValueError) as exc:
            raise ValueError(f'{key} must be YYYY-MM-DD, got {doc.get(key)!r}') from exc
    gen = doc.get('generated_at')
    if not isinstance(gen, str) or pd.isna(pd.Timestamp(gen)) or pd.Timestamp(gen).tzinfo is None:
        raise ValueError(f'generated_at must be an ISO-8601 UTC instant, got {gen!r}')
    strat = doc.get('strategy')
    if not isinstance(strat, dict):
        raise ValueError('strategy must be a mapping')
    for key in ('family', 'id', 'module', 'class', 'source_sha256'):
        if not isinstance(strat.get(key), str) or not strat[key]:
            raise ValueError(f'strategy.{key} must be a non-empty string')
    if not re.fullmatch(r'[0-9a-f]{64}', strat['source_sha256']):
        raise ValueError('strategy.source_sha256 must be a hex sha256')
    if not isinstance(strat.get('params'), dict):
        raise ValueError('strategy.params must be a mapping')
    cap = doc.get('capital_usd')
    if not isinstance(cap, (int, float)) or isinstance(cap, bool) or not cap > 0 or cap != cap:
        raise ValueError(f'capital_usd must be a positive number, got {cap!r}')
    weights = doc.get('weights')
    if not isinstance(weights, dict) or not weights:
        raise ValueError('weights must be a non-empty mapping')
    total = 0.0
    for ticker, w in weights.items():
        if not isinstance(ticker, str) or not re.fullmatch(r'[A-Z][A-Z0-9.\-]{0,9}', ticker):
            raise ValueError(f'ticker {ticker!r} is not a US symbol')
        if not isinstance(w, (int, float)) or isinstance(w, bool) or not (0.0 < float(w) <= 1.0):
            raise ValueError(f'weight for {ticker} must be in (0, 1], got {w!r}')
        total += float(w)
    if total > 1.0 + 1e-9:
        raise ValueError(f'weights sum to {total:.6f} > 1.0 (D08: the remainder is cash, never leverage)')
    if not isinstance(doc.get('notes'), str):
        raise ValueError('notes must be a string')


def _positive(weights: Optional[Dict[Any, float]]) -> Dict[str, float]:
    if not weights:
        return {}
    return {str(k): float(v) for k, v in weights.items() if float(v) > 0.0}


def load_panel_for(fam: registry.Family, entry: Dict[str, Any], as_of: dt.date) -> loaders.PricePanel:
    """The panel the research path would load for a window ending at ``as_of``:
    ``warmup_days`` of history before it (the harness's warm-up rule), through
    the family's own loader."""
    warm = int(entry.get('warmup_days', fam.warmup_days))
    start = as_of - dt.timedelta(days=warm)
    universe = str(entry.get('universe', fam.universe))
    if universe == 'equity':
        return loaders.load_equity_panel(start=start, end=as_of, top_n=int(entry.get('top_n', fam.top_n)))
    if universe == 'etf':
        return loaders.load_etf_panel(entry.get('tickers'), start=start, end=as_of)
    raise TargetsRefused(f'unknown universe {universe!r}')


def latest_fundamentals_acceptance() -> Optional[pd.Timestamp]:
    """max(accepted_at) in the research main DB's ``fundamentals`` table, UTC
    (None when the table is missing or empty)."""
    con = _cfg.connect_read_only(_cfg.research_db_paths()['main'])
    try:
        try:
            row = con.execute('SELECT max(accepted_at) FROM fundamentals').fetchone()
        except Exception:                                   # noqa: BLE001 - no table
            return None
    finally:
        con.close()
    if not row or row[0] is None:
        return None
    ts = cast(pd.Timestamp, pd.Timestamp(row[0]))
    if pd.isna(ts):
        return None
    return ts.tz_convert('UTC') if ts.tzinfo is not None else ts.tz_localize('UTC')


def check_freshness(panel: loaders.PricePanel, as_of: dt.date, universe: str,
                    max_fundamentals_lag_days: int = DEFAULT_MAX_FUNDAMENTALS_LAG_DAYS) -> Dict[str, Any]:
    """Refuse stale inputs (acceptance criterion 2): the decision session's
    bars must be stored (the panel's last bar IS ``as_of``), and for the equity
    universe the newest stored filing acceptance must be within
    ``max_fundamentals_lag_days`` of ``as_of`` - filings arrive most business
    days across 500 names, so a week-old maximum means the refresh did not run."""
    if len(panel.close.index) == 0:
        raise TargetsRefused('the panel has no bars')
    last_bar = pd.Timestamp(panel.close.index[-1]).date()
    if last_bar != as_of:
        raise TargetsRefused(
            f'STALE BARS: the latest stored session is {last_bar}, but the decision session is {as_of}. '
            f'Refresh the bars (scripts/build_pit_universe.py --incremental) or pass --as-of {last_bar}.')
    out: Dict[str, Any] = {'last_bar': last_bar.isoformat()}
    if universe == 'equity':
        latest = latest_fundamentals_acceptance()
        if latest is None:
            raise TargetsRefused('STALE FUNDAMENTALS: the fundamentals table is empty or missing')
        lag = (pd.Timestamp(as_of) - latest.tz_convert(None).normalize()).days
        out['latest_fundamentals_acceptance'] = latest.isoformat()
        out['fundamentals_lag_days'] = int(lag)
        if lag > max_fundamentals_lag_days:
            raise TargetsRefused(
                f'STALE FUNDAMENTALS: the newest stored filing was accepted {latest.date()}, '
                f'{lag} days before {as_of} (limit {max_fundamentals_lag_days}). '
                f'Refresh with scripts/fetch_fundamentals.py --incremental.')
    return out


def compute_targets(family: str, candidate_id: str, sleeve: str, capital_usd: float,
                    as_of: dt.date, panel: Optional[loaders.PricePanel] = None,
                    params: Optional[Dict[str, Any]] = None, notes: str = '',
                    max_fundamentals_lag_days: int = DEFAULT_MAX_FUNDAMENTALS_LAG_DAYS,
                    check_fresh: bool = True) -> TargetsResult:
    """The D08 document for ``sleeve`` on decision session ``as_of``.

    ``panel`` (tests / parity) replaces the loader; it must end at ``as_of``.
    ``params`` replaces the registry's default cell (the pre-registered one).
    Raises ``TargetsRefused`` when the inputs are stale, the strategy holds on
    ``as_of`` (``on_panel`` returned None: not a rebalance session), or the
    result would violate D08.
    """
    if not SLEEVE_RE.match(sleeve or ''):
        raise TargetsRefused(f'sleeve {sleeve!r} must match {SLEEVE_RE.pattern} (D08)')
    if not capital_usd > 0:
        raise TargetsRefused(f'capital must be positive, got {capital_usd}')
    fam = registry.load_family(family)
    entry = fam.get(candidate_id)
    cell = dict(params) if params is not None else registry.default_cell(entry)
    # A cell outside the registered grid is a different (unregistered) rule:
    # refuse it so a sleeve cannot drift from the entry it claims to trade (D02).
    if params is not None and cell not in registry.expand_grid(entry):
        raise TargetsRefused(f'params {cell} are not a registered grid cell of {candidate_id} '
                             f'(registered: {registry.expand_grid(entry)})')
    universe = str(entry.get('universe', fam.universe))
    if panel is None:
        panel = load_panel_for(fam, entry, as_of)
    diag: Dict[str, Any] = {'universe': universe, 'n_bars': int(len(panel.close.index))}
    if check_fresh:
        diag.update(check_freshness(panel, as_of, universe, max_fundamentals_lag_days))
    elif len(panel.close.index) == 0 or pd.Timestamp(panel.close.index[-1]).date() != as_of:
        raise TargetsRefused(f'the panel must end at {as_of}')

    # ---- EXACTLY the executor's sequence (Backtester.run_panel, step 3) ----
    frames = panel.frames()
    if len(frames) < 2:
        raise TargetsRefused('fewer than 2 instruments with data in the window')
    wide = engine.wide_panel(frames)
    index = len(wide) - 1
    if pd.Timestamp(wide.index[index]).date() != as_of:
        raise TargetsRefused(f'the wide panel ends at {wide.index[index]}, not {as_of}')
    strat = engine.make_strategy(entry, cell, panel)
    state = strat.precompute_panel(wide)
    raw = strat.on_panel(wide, state, index)
    if raw is None:
        raise TargetsRefused(
            f'{candidate_id} issues no instruction on {as_of} (on_panel returned None: not a rebalance '
            f'session for REBALANCE={getattr(strat, "REBALANCE", "?")!r}, or too few covered names). '
            f'The strategy holds; run targets on a rebalance session.')
    costs = base_costs(universe)
    weights = _positive(normalise_weights(raw, max_gross=costs.max_gross))
    if not weights:
        raise TargetsRefused(f'{candidate_id} issued an all-flat instruction on {as_of} (hold nothing)')

    # The decisive benchmark(s) through the identical panel and coverage, so
    # the ledger's virtual benchmark is the one the harness compared against.
    coverage = engine.candidate_coverage(state)
    benches: Dict[str, Optional[Dict[str, float]]] = {}
    for bid in entry.get('benchmarks') or []:
        bentry = fam.get(bid)
        cov = (coverage.reindex(index=wide.index, columns=wide['close'].columns).fillna(False).astype(bool)
               if coverage is not None else None)
        bench = engine.make_strategy(bentry, registry.default_cell(bentry), panel, coverage=cov)
        bstate = bench.precompute_panel(wide)
        braw = bench.on_panel(wide, bstate, index)
        # None = the benchmark holds on this session (e.g. fewer than its MIN_NAMES covered); {} = flatten
        benches[bid] = _positive(normalise_weights(braw, max_gross=costs.max_gross)) if braw is not None else None

    cls = type(strat)
    sha = engine.strategy_source_sha256(cls)
    if not sha:
        raise TargetsRefused(f'cannot hash the source of {cls.__module__}.{cls.__name__}')
    strat_ref = entry['strategy']
    doc: Dict[str, Any] = {
        'schema': SCHEMA,
        'sleeve': sleeve,
        'as_of': as_of.isoformat(),
        'generated_at': dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat(),
        'strategy': {'family': fam.name, 'id': candidate_id, 'module': str(strat_ref['module']),
                     'class': str(strat_ref['class']), 'params': dict(cell), 'source_sha256': sha},
        'capital_usd': float(capital_usd),
        'prices_as_of': pd.Timestamp(wide.index[index]).date().isoformat(),
        'weights': dict(sorted(weights.items())),
        'notes': notes or (f'{len(weights)} names, gross {sum(weights.values()):.6f}; '
                           f'registry entry sha256 {registry.entry_sha256(entry)}; '
                           f'benchmarks {sorted(benches)}'),
    }
    validate_targets_document(doc)
    diag.update({'n_names': len(weights), 'gross': float(sum(weights.values())),
                 'registry_entry_sha256': registry.entry_sha256(entry)})
    return TargetsResult(document=doc, benchmarks=benches, diagnostics=diag)


def write_targets(result: TargetsResult, path: Optional[Path] = None,
                  targets_dir: Optional[str] = None, overwrite: bool = False) -> TargetsResult:
    """Write the D08 file (and the benchmark sidecar next to it). Refuses to
    overwrite an existing file unless asked - a decision session's targets are
    a record, not a scratch buffer."""
    doc = result.document
    validate_targets_document(doc)
    path = Path(path) if path is not None else default_targets_path(
        doc['sleeve'], dt.date.fromisoformat(doc['as_of']), targets_dir)
    if path.exists() and not overwrite:
        raise TargetsRefused(f'{path} exists; pass --overwrite to replace it')
    path.parent.mkdir(parents=True, exist_ok=True)
    side = benchmark_sidecar_path(path)
    side_doc = {'schema': BENCHMARK_SCHEMA, 'sleeve': doc['sleeve'], 'as_of': doc['as_of'],
                'candidate': doc['strategy']['id'], 'capital_usd': doc['capital_usd'],
                'benchmarks': {k: (dict(sorted(v.items())) if v is not None else None)
                               for k, v in result.benchmarks.items()}}
    # Atomic pair: both documents are staged, then the sidecar is published
    # BEFORE the D08 file, so a valid targets file can never exist without the
    # ledger input beside it (a sidecar without its D08 file is inert).
    tmp_side = side.with_suffix('.json.tmp')
    tmp_doc = path.with_suffix('.json.tmp')
    tmp_side.write_text(json.dumps(side_doc, indent=2) + '\n')
    tmp_doc.write_text(json.dumps(doc, indent=2, sort_keys=False) + '\n')
    os.replace(tmp_side, side)
    os.replace(tmp_doc, path)
    result.path, result.benchmark_path = path, side
    return result


def read_targets(path: Path) -> Dict[str, Any]:
    doc = json.loads(Path(path).read_text())
    validate_targets_document(doc)
    return doc


def list_sleeve_targets(sleeve: str, targets_dir: Optional[str] = None) -> List[Path]:
    """Every D08 file of ``sleeve`` in decision-session order."""
    root = Path(os.path.expanduser(targets_dir or DEFAULT_TARGETS_DIR)) / sleeve
    if not root.exists():
        return []
    return sorted(p for p in root.glob('*.json')
                  if not p.name.endswith('.benchmark.json') and re.fullmatch(r'\d{4}-\d{2}-\d{2}\.json', p.name))
