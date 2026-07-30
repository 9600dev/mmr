#!/usr/bin/env python3
"""Fetch split history, so a large jump can be classified instead of accepted.

`unexplained_jumps` reports every large move as a warning and declines to judge,
because telling an unadjusted split from a real re-rating needs a
corporate-actions feed. 799 warnings had been sitting permanently accepted in
the data-quality baseline for want of one. An accepted warning is a warning
nobody reads.
"""
from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def main() -> int:
    import yaml
    from massive import RESTClient
    from trader.container import Container, default_config_path
    from trader.data.duckdb_store import DuckDBConnection
    from trader.data.universe import UniverseAccessor

    ycfg = yaml.safe_load(open(default_config_path())) or {}
    client = RESTClient(os.environ.get('MASSIVE_API_KEY')
                        or ycfg.get('massive_api_key'))
    cfg = Container.instance().config()
    db = DuckDBConnection(cfg.get('history_duckdb_path', ''))
    db.execute("""CREATE TABLE IF NOT EXISTS corporate_splits (
        ticker VARCHAR, conid VARCHAR, execution_date DATE,
        split_to DOUBLE, split_from DOUBLE, fetched_at TIMESTAMPTZ,
        PRIMARY KEY (ticker, execution_date))""", fetch='none')

    ua = UniverseAccessor(cfg.get('duckdb_path', ''),
                          cfg.get('universe_library', 'Universes'))
    mapping = {}
    for name in ua.list_universes():
        for d in getattr(ua.get(name), 'security_definitions', []):
            mapping[d.symbol] = str(d.conId)
    # Also cover the point-in-time universe, which contains delisted names with
    # no conId - their splits matter just as much for classifying a jump.
    pit = db.execute("SELECT DISTINCT ticker FROM pit_daily_bars WHERE "
                     "dv_rank <= 500", fetch='all') or []
    for (t,) in pit:
        mapping.setdefault(t, None)

    print(f'{len(mapping)} tickers', flush=True)
    written = failed = 0
    for i, (ticker, conid) in enumerate(sorted(mapping.items())):
        try:
            splits = list(client.list_splits(ticker=ticker, limit=100))
        except Exception:                                 # noqa: BLE001
            failed += 1
            continue
        for sp in splits:
            try:
                db.execute(
                    'INSERT OR REPLACE INTO corporate_splits VALUES '
                    '(?,?,?,?,?, now())',
                    [ticker, conid, sp.execution_date,
                     float(sp.split_to), float(sp.split_from)], fetch='none')
                written += 1
            except Exception:                             # noqa: BLE001
                continue
        if (i + 1) % 100 == 0:
            print(f'  {i+1}/{len(mapping)} rows={written} failed={failed}',
                  flush=True)
        time.sleep(0.02)
    print(f'done: {written} splits, {failed} tickers failed', flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
