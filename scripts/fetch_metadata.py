#!/usr/bin/env python3
"""Fetch sector / industry / market-cap per instrument, for risk neutralisation.

A cross-sectional book that equal-weights within quantiles makes sector, beta
and size bets nobody chose. A momentum book over 2016-2026 was structurally
long technology, and was paid for that exposure rather than for the signal.
Stripping it needs classifications, which this collects once.

Stored in the object store keyed by conid, not in tick_data: it is static
reference data, not a time series, and putting it in the bar table would mean
every quality rule has to reason about rows that are not bars.
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def main() -> int:
    import yaml
    from massive import RESTClient
    from trader.container import Container, default_config_path
    from trader.data.duckdb_store import DuckDBConnection

    cfg = yaml.safe_load(open(default_config_path()))
    client = RESTClient(os.environ.get('MASSIVE_API_KEY')
                        or cfg.get('massive_api_key'))
    ccfg = Container.instance().config()
    db = DuckDBConnection(ccfg.get('duckdb_path', ''))
    db.execute("""CREATE TABLE IF NOT EXISTS instrument_meta (
        conid VARCHAR PRIMARY KEY, symbol VARCHAR, name VARCHAR,
        sic_code VARCHAR, sic_description VARCHAR, market_cap DOUBLE,
        share_class_shares DOUBLE, list_date VARCHAR, fetched_at TIMESTAMPTZ
    )""", fetch='none')

    # conid -> symbol comes from the universes, which is where the download
    # path registered each resolved contract.
    from trader.data.universe import UniverseAccessor
    ua = UniverseAccessor(ccfg.get('duckdb_path', ''),
                          ccfg.get('universe_library', 'Universes'))
    mapping = {}
    for name in ua.list_universes():
        for d in getattr(ua.get(name), 'security_definitions', []):
            mapping[str(d.conId)] = d.symbol
    if not mapping:
        print('no conid->symbol mapping found in any universe', file=sys.stderr)
        return 1

    done = {r[0] for r in (db.execute(
        'SELECT conid FROM instrument_meta', fetch='all') or [])}
    todo = [(c, s) for c, s in mapping.items() if c not in done]
    print(f'{len(mapping)} instruments known, {len(todo)} to fetch', flush=True)

    ok = fail = 0
    for i, (conid, symbol) in enumerate(todo):
        try:
            d = client.get_ticker_details(symbol)
        except Exception as exc:                          # noqa: BLE001
            fail += 1
            if fail <= 5:
                print(f'  {symbol}: {exc}', flush=True)
            continue
        db.execute(
            """INSERT OR REPLACE INTO instrument_meta VALUES (?,?,?,?,?,?,?,?, now())""",
            [conid, symbol, getattr(d, 'name', None),
             getattr(d, 'sic_code', None), getattr(d, 'sic_description', None),
             float(getattr(d, 'market_cap', 0) or 0),
             float(getattr(d, 'share_class_shares_outstanding', 0) or 0),
             str(getattr(d, 'list_date', '') or '')], fetch='none')
        ok += 1
        if ok % 50 == 0:
            print(f'  {ok}/{len(todo)} fetched', flush=True)
    print(f'done: {ok} fetched, {fail} failed', flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
