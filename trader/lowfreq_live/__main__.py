"""``python -m trader.lowfreq_live`` - targets / track / rebuild / report / fills.

Exit codes: 0 ok, 2 refused (nothing written), 1 unexpected error.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from typing import Any, Dict, Optional


def _date(s: Optional[str]) -> Optional[dt.date]:
    return dt.date.fromisoformat(s) if s else None


def _container_paths() -> Dict[str, str]:
    """``duckdb_path`` / ``history_duckdb_path`` from the Container config
    (``TRADER_CONFIG`` or ``~/.config/mmr/trader.yaml``)."""
    from trader.container import Container
    cfg = Container.instance().config()
    return {'main': os.path.expanduser(str(cfg.get('duckdb_path', ''))),
            'history': os.path.expanduser(str(cfg.get('history_duckdb_path', '')))}


def cmd_targets(a: argparse.Namespace) -> int:
    from trader.lowfreq_live.targets import TargetsRefused, compute_targets, write_targets
    if '/' not in a.candidate:
        print('candidate must be <family>/<id>', file=sys.stderr)
        return 2
    family, cid = a.candidate.split('/', 1)
    as_of = _date(a.as_of) or dt.date.today()
    params = json.loads(a.params) if a.params else None
    try:
        res = compute_targets(family, cid, a.sleeve, a.capital, as_of, params=params, notes=a.notes or '',
                              max_fundamentals_lag_days=a.max_fundamentals_lag_days)
        write_targets(res, path=a.out, targets_dir=a.targets_dir, overwrite=a.overwrite)
    except TargetsRefused as exc:
        print(f'REFUSED: {exc}', file=sys.stderr)
        return 2
    out: Dict[str, Any] = {'path': str(res.path), 'benchmark_path': str(res.benchmark_path),
                           'n_names': res.diagnostics.get('n_names'), 'gross': res.diagnostics.get('gross'),
                           'as_of': res.document['as_of'], 'source_sha256': res.document['strategy']['source_sha256'],
                           'diagnostics': res.diagnostics}
    if a.json:
        print(json.dumps(out, indent=2, default=str))
    else:
        print(f"wrote {res.path}  ({out['n_names']} names, gross {out['gross']:.4f}, as_of {out['as_of']})")
        print(f"benchmark sidecar {res.benchmark_path}")
        for t, w in res.document['weights'].items():
            print(f'  {t:<8} {w:.6f}')
    return 0


def _store_and_bars(a: argparse.Namespace):
    from trader.lowfreq_live.ledger import DuckDBBars, LedgerStore
    paths = _container_paths() if not (a.ledger_db and a.bars_db) else {}
    ledger_db = a.ledger_db or paths.get('main')
    bars_db = a.bars_db or paths.get('history')
    if not ledger_db or not bars_db:
        raise RuntimeError('cannot resolve --ledger-db / --bars-db')
    return LedgerStore(ledger_db), DuckDBBars(bars_db)


def _ib_positions() -> Dict[str, float]:
    """symbol -> quantity from trader_service (D13 reconciliation source).
    Any failure to read is raised: the caller must refuse, not guess."""
    from trader.sdk import MMR
    df = MMR().positions()
    out: Dict[str, float] = {}
    if df is None or len(df) == 0:
        return out
    for _, row in df.iterrows():
        sym = str(row.get('symbol', '')).upper()
        if sym:
            out[sym] = out.get(sym, 0.0) + float(row.get('position', 0.0) or 0.0)
    return out


def _broker_or_refuse() -> Optional[Dict[str, float]]:
    """IB positions, or None after printing the refusal (D13: no bypass)."""
    try:
        return _ib_positions()
    except Exception as exc:                            # noqa: BLE001 - fail closed
        print(f'REFUSED: cannot read IB positions for reconciliation ({exc}); track/rebuild require it (D13)',
              file=sys.stderr)
        return None


def cmd_track(a: argparse.Namespace) -> int:
    from trader.lowfreq_live.ledger import LedgerDisagreement, ReconciliationError, track
    store, bars = _store_and_bars(a)
    broker = _broker_or_refuse()
    if broker is None:
        return 2
    try:
        res = track(a.sleeve, store, bars, broker, targets_dir=a.targets_dir, through=_date(a.through),
                    benchmark_id=a.benchmark)
    except (ReconciliationError, LedgerDisagreement) as exc:
        print(f'REFUSED: {exc}', file=sys.stderr)
        return 2
    if a.json:
        print(json.dumps(res, indent=2, default=str))
    else:
        print(f"{a.sleeve}: appended {res['appended']} session(s) through {res['through']} "
              f"(benchmark {res['benchmark_id']}, inception {res['inception']}, capital {res['capital_usd']:,.2f}, "
              f"reconciled against IB; benchmark D07 exits since inception: {res['benchmark_forced_exits']})")
    return 0


def cmd_rebuild(a: argparse.Namespace) -> int:
    from trader.lowfreq_live.ledger import ReconciliationError, rebuild
    if not a.attest:
        print('REFUSED: rebuild rewrites the ledger history; pass --attest with --reason', file=sys.stderr)
        return 2
    if not (a.reason or '').strip():
        print('REFUSED: --reason is required', file=sys.stderr)
        return 2
    store, bars = _store_and_bars(a)
    broker = _broker_or_refuse()
    if broker is None:
        return 2
    try:
        res = rebuild(a.sleeve, store, bars, broker, a.reason, targets_dir=a.targets_dir,
                      through=_date(a.through), benchmark_id=a.benchmark)
    except ReconciliationError as exc:
        print(f'REFUSED: {exc}', file=sys.stderr)
        return 2
    if a.json:
        print(json.dumps(res, indent=2, default=str))
    else:
        print(f"{a.sleeve}: rebuilt - dropped {res['rows_dropped']} row(s), wrote {res['rows_written']} through "
              f"{res['through']}; {len(res['disagreements_resolved'])} disagreement(s) resolved; reason: {res['reason']}")
    return 0


def cmd_report(a: argparse.Namespace) -> int:
    from trader.lowfreq_live.ledger import report
    store, _ = _store_and_bars(a)
    r = report(a.sleeve, store)
    if a.json:
        print(json.dumps(r, indent=2, default=str))
        return 0
    print(f"sleeve {r['sleeve']} vs {r['benchmark_id']}  {r['first_session']} -> {r['last_session']} "
          f"({r['sessions']} sessions)")
    print(f"  capital           {r['capital_usd']:>14,.2f}")
    print(f"  sleeve value      {r['sleeve_value']:>14,.2f}   return {100*r['sleeve_return']:+.2f}%")
    print(f"  benchmark value   {r['benchmark_value']:>14,.2f}   return {100*r['benchmark_return']:+.2f}%")
    print(f"  active return     {100*r['active_return']:+.2f}%   (annualised mean {100*r['active_return_annualised_mean']:+.2f}%)")
    print(f"  tracking error    {100*r['tracking_error_annualised']:.2f}% annualised")
    print(f"  holdings {r['n_holdings']} ({r['n_unpriced']} unpriced today), cash {r['cash']:,.2f}")
    if r['rebuilds']:
        print(f"  rebuilds ({len(r['rebuilds'])}; history was rewritten by an attested operator act):")
        for rb in r['rebuilds']:
            print(f"    {rb['rebuilt_at']}  dropped {rb['rows_dropped']} wrote {rb['rows_written']}  {rb['reason']}")
    else:
        print("  rebuilds          none (append-only history)")
    return 0


def cmd_fills(a: argparse.Namespace) -> int:
    store, _ = _store_and_bars(a)
    fills = store.fills(a.sleeve)
    if a.json:
        print(json.dumps([f.__dict__ for f in fills], indent=2, default=str))
        return 0
    for f in fills:
        print(f'{f.trade_date}  {f.quantity:>+10.2f} {f.ticker:<8} @ {f.price:.4f}  comm {f.commission:.2f}  '
              f'exec {f.exec_id} order {f.order_id} proposal {f.proposal_id}')
    print(f'{len(fills)} fill(s) (sleeve_fills, written by trader.rebalance reconcile)')
    return 0


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog='python -m trader.lowfreq_live')
    ap.add_argument('--json', action='store_true')
    sub = ap.add_subparsers(dest='cmd', required=True)

    t = sub.add_parser('targets', help='write the D08 target-weights file for a decision session')
    t.add_argument('candidate', help='<family>/<id> from research/lowfreq/<family>/candidates.yaml')
    t.add_argument('--sleeve', required=True)
    t.add_argument('--capital', type=float, required=True, help='sleeve capital in USD')
    t.add_argument('--as-of', default=None, help='decision session YYYY-MM-DD (default today)')
    t.add_argument('--out', default=None, help='file path (default ~/.local/share/mmr/data/targets/<sleeve>/<as_of>.json)')
    t.add_argument('--targets-dir', default=None)
    t.add_argument('--params', default=None, help='JSON cell overriding the registry default')
    t.add_argument('--notes', default=None)
    t.add_argument('--max-fundamentals-lag-days', type=int, default=7)
    t.add_argument('--overwrite', action='store_true')
    t.set_defaults(func=cmd_targets)

    for name, fn, help_ in (('track', cmd_track, 'append ledger rows through the last stored session '
                                                 '(reconciles against IB first; refuses on any disagreement)'),
                            ('rebuild', cmd_rebuild, 'attested operator act: drop the ledger and replay from inception'),
                            ('report', cmd_report, 'since-inception sleeve vs benchmark'),
                            ('fills', cmd_fills, 'list the sleeve_fills rows of a sleeve (read-only)')):
        p = sub.add_parser(name, help=help_)
        p.add_argument('--sleeve', required=True)
        p.add_argument('--ledger-db', default=None, help='DuckDB file for the ledger (default: trader.yaml duckdb_path)')
        p.add_argument('--bars-db', default=None, help='DuckDB with pit_daily_bars (default: history_duckdb_path)')
        p.add_argument('--targets-dir', default=None)
        if name in ('track', 'rebuild'):
            p.add_argument('--through', default=None, help='last session to record (default: last stored)')
            p.add_argument('--benchmark', default=None, help='benchmark id when the sidecar carries several')
        if name == 'rebuild':
            p.add_argument('--reason', default=None, help='why history is being rewritten (recorded)')
            p.add_argument('--attest', action='store_true', help='confirm the operator reviewed the disagreements')
        p.set_defaults(func=fn)

    return ap


def main(argv=None) -> int:
    a = build_parser().parse_args(argv)
    try:
        return int(a.func(a))
    except RuntimeError as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
