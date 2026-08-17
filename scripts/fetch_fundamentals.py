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

    Returns None on a TRANSPORT/HTTP error with nothing yet collected, and {}
    when the API answered cleanly with no filings. The distinction matters at
    scale: an ETF genuinely files no 10-K/Q (an empty answer, skip it), while
    a run of consecutive errors is quota exhaustion or an outage — burning the
    rest of a 2,000-name list recording "failed" on each would spend nothing
    but time and hide the real problem.
    """
    out = {}
    form_clause = ' OR '.join(f'formType:"{f}"' for f in forms)
    for start in (0, 50):
        body = {'query': f'ticker:{ticker} AND ({form_clause})',
                'from': str(start), 'size': '50',
                'sort': [{'filedAt': {'order': 'desc'}}]}
        try:
            # Token in the Authorization header, NOT a query param: httpx logs
            # request URLs at DEBUG, and a token in the URL lands in debug.log.
            r = client.post(_SEC_QUERY, json=body,
                            headers={'Authorization': key}, timeout=30)
            if r.status_code != 200:
                return out if out else None
            filings = r.json().get('filings', [])
        except Exception:
            return out if out else None
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


def pit_membership_todo(db, hist_db, min_months: int):
    """(conid, ticker) work list from POINT-IN-TIME membership, not from
    today's universe — today's universe is the survivorship hole this fetch
    exists to close (the 2026-07 run covered 33.5k member-months; the names
    absent from it hold 39.1k).

    Membership = rank <= 500 by dollar volume at a month-end (the same rule
    `pit_panel.load_pit_panel` trades on). ``min_months`` drops one-month
    transients: at >=3 the list is ~2.3k names carrying ~90% of the missing
    observations. Names already holding ANY fundamentals row are skipped, so
    an interrupted run resumes for the cost of the membership query. Ordered
    longest-membership first: if the API quota dies mid-run, the names that
    carry the most panel weight are already in. Delisted names have no conId
    and never will — rows are written with conid = '' and joined by ticker,
    the same reasoning as `pit_daily_bars` being ticker-keyed.
    """
    import pandas as pd
    from trader.data.bar_quality import EXCHANGE_TEST_TICKERS

    df = hist_db.execute(
        'SELECT day, ticker, dv_rank FROM pit_daily_bars', fetch='df')
    df = df[~df['ticker'].isin(EXCHANGE_TEST_TICKERS)]
    df['day'] = pd.to_datetime(df['day'])
    df['month'] = df['day'].dt.to_period('M')
    month_end = (df.sort_values('day').groupby(['month', 'ticker'])
                 .agg(rank=('dv_rank', 'last')).reset_index())
    months = (month_end[month_end['rank'] <= 500]
              .groupby('ticker').size().sort_values(ascending=False))
    have = {r[0] for r in db.execute(
        'SELECT DISTINCT ticker FROM fundamentals', fetch='all') or []}
    todo = [('', t) for t, n in months.items()
            if n >= min_months and t not in have]
    print(f'pit membership: {len(months)} names >=1 month, '
          f'{len(todo)} to fetch (>={min_months} months, '
          f'{len(have)} already covered)', flush=True)
    return todo


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--limit', type=int, default=0, help='cap tickers (0 = all)')
    ap.add_argument('--sleep', type=float, default=0.05)
    ap.add_argument('--pit-min-months', type=int, default=0,
                    help='fetch by POINT-IN-TIME membership instead of the '
                         'present-day universe: every name with at least this '
                         'many member-months (rank<=500 at a month-end) that '
                         'has no fundamentals yet. 0 = off (legacy universe '
                         'mode).')
    args = ap.parse_args()

    import logging as _logging
    # httpx/httpcore DEBUG chatter is ~50 lines per name; at 2,000+ names it
    # drowns the log files for zero information.
    for noisy in ('httpx', 'httpcore'):
        _logging.getLogger(noisy).setLevel(_logging.WARNING)

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

    if args.pit_min_months > 0:
        hist_db = DuckDBConnection(cfg.get('history_duckdb_path', ''))
        todo = pit_membership_todo(db, hist_db, args.pit_min_months)
    else:
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

    written = no_timestamp = no_values = failed = no_filings = 0
    consecutive_errors = 0
    with httpx.Client() as client:
        for i, (conid, ticker) in enumerate(todo):
            try:
                index = sec_filing_index(client, ticker, key)
            except Exception:
                index = None
            if index is None:
                failed += 1
                consecutive_errors += 1
                if consecutive_errors >= 15:
                    # 15 straight TRANSPORT errors is quota exhaustion or an
                    # outage, not 15 unlucky tickers. Stop spending the list;
                    # the run resumes from here for free (fetched names are
                    # skipped by construction in pit mode).
                    print(f'ABORT: {consecutive_errors} consecutive sec-api '
                          f'errors at {ticker} ({i}/{len(todo)}) — quota '
                          f'exhausted or endpoint down. {written} rows '
                          f'written so far; re-run the same command to '
                          f'resume.', flush=True)
                    return 2
                continue
            consecutive_errors = 0
            if not index:
                # A clean answer with no 10-K/Q: an ETF/trust/unit, not an
                # error. Skip the Polygon call — there is nothing to date.
                no_filings += 1
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
                      f'nofilings={no_filings} failed={failed}', flush=True)
            time.sleep(args.sleep)

    print(f'done: {written} rows written, {no_timestamp} periods dropped for '
          f'having no establishable filing time, {no_values} tickers with no '
          f'values, {no_filings} with no 10-K/Q filings (funds/trusts), '
          f'{failed} failed', flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
