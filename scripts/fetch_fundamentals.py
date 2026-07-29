#!/usr/bin/env python3
"""Ingest point-in-time fundamentals: sec-api.io for WHEN, Polygon for WHAT.

THE SPLIT, AND WHY
    Two sources, each used for the thing it is actually authoritative about.

    sec-api.io owns the TIMESTAMPS. Its Query API returns every filing with
    `filedAt` as a full tz-aware instant - `2026-05-01T06:01:00-04:00` - which
    is the only thing that makes a fundamentals backtest honest. Polygon
    exposes a filing DATE on 79% of records and nothing at all on the rest
    (measured: 107 of 499 sampled), and a date cannot distinguish a 06:01
    filing, tradeable at that day's close, from a 16:45 one that is not.

    Polygon owns the NUMBERS. One call per ticker returns years of statements
    already parsed into consistent fields. Getting the same from sec-api means
    one xbrl-to-json conversion per filing - roughly 19,000 calls for this
    universe - to obtain values Polygon already has.

    They join on (ticker, period end), which both report and neither can get
    wrong: it is the period the filing describes, not a judgement about it.

WHAT IS REFUSED
    A period whose acceptance instant cannot be established is DROPPED, not
    estimated. The reporting lag runs 18 to 60 days, so no constant offset is
    within three weeks of right, and being wrong in the optimistic direction
    means trading on results that were not public. Dropped counts are printed
    rather than silently absorbed.
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_SEC_QUERY = 'https://api.sec-api.io'


def _key() -> str:
    """Env first, YAML second - the user's shell is the source of truth and
    keeps the secret out of a bind-mounted config file."""
    import yaml
    from trader.container import default_config_path
    env = os.environ.get('SEC_API_KEY', '').strip()
    if env:
        return env
    cfg = yaml.safe_load(open(default_config_path())) or {}
    return str(cfg.get('sec_api_key', '') or '').strip()


def sec_filing_index(client, ticker: str, key: str, forms=('10-Q', '10-K')):
    """Every 10-Q/10-K for a ticker, with its acceptance instant.

    Returns {period_end_iso: (accepted_at, form_type, accession_no)}. Later
    acceptances overwrite earlier ones for the same period, which is exactly
    the restatement rule: the most recently filed version of a period is the
    current knowledge, and an as-of read filters by acceptance anyway.
    """
    out = {}
    form_clause = ' OR '.join(f'formType:"{f}"' for f in forms)
    for start in (0, 50):
        body = {'query': f'ticker:{ticker} AND ({form_clause})',
                'from': str(start), 'size': '50',
                'sort': [{'filedAt': {'order': 'desc'}}]}
        try:
            r = client.post(_SEC_QUERY, json=body, params={'token': key},
                            timeout=30)
            if r.status_code != 200:
                break
            filings = r.json().get('filings', [])
        except Exception:
            break
        if not filings:
            break
        for f in filings:
            period = f.get('periodOfReport')
            filed = f.get('filedAt')
            if not period or not filed:
                continue
            out[period] = (filed, f.get('formType', ''), f.get('accessionNo', ''))
        if len(filings) < 50:
            break
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--limit', type=int, default=0, help='cap tickers (0 = all)')
    ap.add_argument('--sleep', type=float, default=0.05)
    args = ap.parse_args()

    import httpx
    import yaml
    from massive import RESTClient
    from trader.container import Container, default_config_path
    from trader.data.duckdb_store import DuckDBConnection
    from trader.data.universe import UniverseAccessor
    from trader.data.fundamentals import extract_polygon

    key = _key()
    if not key:
        print('no SEC_API_KEY in env or sec_api_key in config', file=sys.stderr)
        return 1

    cfg = Container.instance().config()
    ycfg = yaml.safe_load(open(default_config_path())) or {}
    poly = RESTClient(os.environ.get('MASSIVE_API_KEY')
                      or ycfg.get('massive_api_key'))
    db = DuckDBConnection(cfg.get('duckdb_path', ''))
    db.execute("""CREATE TABLE IF NOT EXISTS fundamentals (
        conid VARCHAR, ticker VARCHAR, period_end DATE,
        accepted_at TIMESTAMPTZ,          -- when it became public. THE key.
        form_type VARCHAR, accession_no VARCHAR,
        revenue DOUBLE, net_income DOUBLE, gross_profit DOUBLE,
        operating_income DOUBLE, total_assets DOUBLE, total_liabilities DOUBLE,
        stockholders_equity DOUBLE, cash_from_operations DOUBLE,
        shares_diluted DOUBLE, fetched_at TIMESTAMPTZ,
        PRIMARY KEY (conid, period_end, accession_no))""", fetch='none')

    ua = UniverseAccessor(cfg.get('duckdb_path', ''),
                          cfg.get('universe_library', 'Universes'))
    mapping = {}
    for name in ua.list_universes():
        for d in getattr(ua.get(name), 'security_definitions', []):
            mapping[str(d.conId)] = d.symbol
    todo = sorted(mapping.items())
    if args.limit:
        todo = todo[:args.limit]
    print(f'{len(todo)} instruments', flush=True)

    written = no_timestamp = no_values = failed = 0
    with httpx.Client() as client:
        for i, (conid, ticker) in enumerate(todo):
            try:
                index = sec_filing_index(client, ticker, key)
            except Exception:
                failed += 1
                continue
            if not index:
                failed += 1
                continue
            try:
                fins = list(poly.vx.list_stock_financials(
                    ticker=ticker, limit=60, timeframe='quarterly'))
            except Exception:
                no_values += 1
                continue
            for f in fins:
                period = getattr(f, 'end_date', None)
                if not period:
                    no_timestamp += 1
                    continue
                hit = index.get(period)
                if not hit:
                    # No filing carries this period end. It cannot be placed
                    # in time, so it is dropped rather than dated by guesswork.
                    no_timestamp += 1
                    continue
                filed_at, form, acc = hit
                st = getattr(f, 'financials', None)
                vals = extract_polygon(st) if st else {}
                db.execute(
                    'INSERT OR REPLACE INTO fundamentals VALUES '
                    '(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, now())',
                    [conid, ticker, period, filed_at, form, acc,
                     vals.get('revenue'), vals.get('net_income'),
                     vals.get('gross_profit'), vals.get('operating_income'),
                     vals.get('total_assets'), vals.get('total_liabilities'),
                     vals.get('stockholders_equity'),
                     vals.get('cash_from_operations'),
                     vals.get('shares_diluted')], fetch='none')
                written += 1
            if (i + 1) % 25 == 0:
                print(f'  {i+1}/{len(todo)}  rows={written} '
                      f'undated={no_timestamp} novalues={no_values} '
                      f'failed={failed}', flush=True)
            time.sleep(args.sleep)

    print(f'done: {written} rows written, {no_timestamp} periods dropped for '
          f'having no establishable filing time, {no_values} tickers with no '
          f'values, {failed} failed', flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
