#!/usr/bin/env python3
"""Ingest point-in-time fundamentals: sec-api.io for WHEN, the vendor for WHAT.

THE SPLIT, AND WHY
    Two sources, each used for the thing it is actually authoritative about.

    sec-api.io owns the TIMESTAMPS. Its Query API returns every filing with
    `filedAt` as a full tz-aware instant - `2026-05-01T06:01:00-04:00` - which
    is the only thing that makes a fundamentals backtest honest. The statement
    vendor exposes a filing DATE, and a date cannot distinguish a 06:01
    filing, tradeable at that day's close, from a 16:45 one that is not.

    The vendor (Massive, formerly Polygon) owns the NUMBERS: statements
    already parsed into consistent fields. Getting the same from sec-api means
    one xbrl-to-json conversion per filing.

    They join on (ticker, period end), which both report and neither can get
    wrong: it is the period the filing describes, not a judgement about it.

WHAT IS REFUSED
    A period whose acceptance instant cannot be established is DROPPED, not
    estimated. The reporting lag runs 18 to 60 days, so no constant offset is
    within three weeks of right, and being wrong in the optimistic direction
    means trading on results that were not public. Dropped counts are printed
    rather than silently absorbed.

    A statement whose numbers were taken from a LATER filing is refused too.
    The vendor's current endpoints serve the latest version of a period: an
    old quarter comes back as the comparative column of a filing a year on,
    recast for whatever happened since (trader/data/statements_v1.py). Dated
    to the original filing that would be knowledge the market did not have.
    So this ingest only ever adds what was filed recently - run it often
    enough that a filing is fetched before a later one supersedes it (months,
    not days) - and CANNOT backfill history: a name new to the store starts
    from its most recent filings.

    A late-filing notice (NT 10-K / NT 10-Q) names a period and carries no
    statements; it never dates a row.

THE ENDPOINT CHANGED, AND NOTHING SAID SO
    Until 2026-11-15 the numbers came from `/vX/reference/financials`. The
    vendor retired it (HTTP 404), the call sat inside `except Exception`, and
    every run reported "0 new rows" as if nothing had been filed. A statements
    request that fails is now counted, named in the log, aborts the run after
    `MAX_CONSECUTIVE_VENDOR_ERRORS` in a row, and makes the exit status
    non-zero.
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from trader.data.share_basis import is_late_filing_notice      # noqa: E402
from trader.data import statements_v1                           # noqa: E402

_SEC_QUERY = 'https://api.sec-api.io'
FINANCIALS_URL = 'https://api.massive.com/stocks/financials/v1/'
STATEMENT_PATHS = {'income': 'income-statements', 'balance': 'balance-sheets',
                   'cashflow': 'cash-flow-statements'}
# Quarters asked for per name. Only recent filings can be used (see the module
# docstring), so this is far more than a run ever stores.
STATEMENT_PERIODS = 16
MAX_CONSECUTIVE_VENDOR_ERRORS = 10


class StatementsUnavailable(RuntimeError):
    """The statement vendor did not answer with a usable page."""


def massive_key() -> str:
    """Env first, YAML second; never printed."""
    import yaml
    from trader.container import default_config_path
    env = os.environ.get('MASSIVE_API_KEY', '').strip()
    if env:
        return env
    cfg = yaml.safe_load(open(default_config_path())) or {}
    return str(cfg.get('massive_api_key', '') or '').strip()


def fetch_statements(client, key: str, ticker: str, periods: int = STATEMENT_PERIODS) -> dict:
    """``{'income': {period_end: row}, 'balance': {...}, 'cashflow': {...}}``
    for the newest ``periods`` quarters of ``ticker``.

    ``client`` needs ``get(url, params=..., headers=..., timeout=...)``. Any
    of the three requests not answering HTTP 200 with a ``results`` list
    raises ``StatementsUnavailable`` naming the statement and the status: a
    vendor that cannot be read is not a company that filed nothing.
    """
    headers = {'Authorization': f'Bearer {key}'}     # never in the URL: request URLs are logged
    out = {}
    for name, path in STATEMENT_PATHS.items():
        response = client.get(FINANCIALS_URL + path,
                              params={'tickers': ticker, 'timeframe': 'quarterly',
                                      'limit': periods, 'sort': 'period_end.desc'},
                              headers=headers, timeout=30)
        if response.status_code != 200:
            raise StatementsUnavailable(f'{path} answered HTTP {response.status_code}')
        body = response.json()
        results = body.get('results') if isinstance(body, dict) else None
        if not isinstance(results, list):
            raise StatementsUnavailable(f'{path} response carries no results list')
        rows = {}
        for row in results:
            period = row.get('period_end') if isinstance(row, dict) else None
            if period:
                rows.setdefault(str(period)[:10], row)       # newest first: the first seen wins
        out[name] = rows
    return out


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


def sec_filing_index(client, ticker: str, key: str, forms=('10-Q', '10-K'),
                     filed_after=None):
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
    query = f'ticker:{ticker} AND ({form_clause})'
    if filed_after is not None:
        # Incremental mode (hillclimb C02): only filings accepted strictly
        # after the latest instant already stored for this name. Lucene range
        # on the same field the row is keyed by, so nothing older can come back.
        #
        # The clause is `filedAt:{<instant> TO *]` — exclusive lower bound,
        # open upper. Until 2026-11-15 the f-string closed with `]}}`, which
        # renders a stray `}` after the bracket; sec-api answers a malformed
        # range with HTTP 200 and NO filings, so every name read "nothing
        # new" and the incremental refresh fetched nothing at all, from the
        # day it was written, while reporting success (AAPL's stored filings
        # stopped two quarters back). Built without an f-string so the braces
        # on the page are the braces on the wire.
        query += ' AND filedAt:{' + _iso_instant(filed_after) + ' TO *]'
    for start in (0, 50):
        body = {'query': query,
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
            # The form filter above also matches "NT 10-Q" / "NT 10-K": a
            # notice that the report will be LATE, filed before it. Dating a
            # period to one says its numbers were public before they were
            # (758 stored rows were, found 2026-11-15).
            if is_late_filing_notice(f.get('formType')):
                continue
            # The NEWEST acceptance per period wins, decided by the instant
            # itself rather than by response order: the API pages newest-first,
            # so "last write wins" kept the OLDEST same-period filing and an
            # incremental re-run (which asks for anything after the stored
            # instant) then stored the newer one - a second run changing the
            # table a first run had left (hillclimb C02, round 2).
            prev = out.get(period)
            if prev is None or _later(filed, prev[0]):
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


def _later(a: str, b: str) -> bool:
    """Is acceptance instant ``a`` strictly after ``b``? Compared as instants
    (both tz-aware ISO-8601 from sec-api), never as strings."""
    import pandas as pd
    ta, tb = pd.Timestamp(a), pd.Timestamp(b)
    return bool(ta > tb)


def _iso_instant(t) -> str:
    """A tz-aware instant as sec-api's ``filedAt`` literal (UTC, seconds)."""
    import pandas as pd
    ts = pd.Timestamp(t)
    ts = ts.tz_localize('UTC') if ts.tzinfo is None else ts.tz_convert('UTC')
    return ts.strftime('%Y-%m-%dT%H:%M:%S')


FUNDAMENTALS_DDL = """CREATE TABLE IF NOT EXISTS fundamentals (
        conid VARCHAR, ticker VARCHAR, period_end DATE,
        accepted_at TIMESTAMPTZ,          -- when it became public. THE key.
        form_type VARCHAR, accession_no VARCHAR,
        revenue DOUBLE, net_income DOUBLE, gross_profit DOUBLE,
        operating_income DOUBLE, total_assets DOUBLE, total_liabilities DOUBLE,
        stockholders_equity DOUBLE, cash_from_operations DOUBLE,
        shares_diluted DOUBLE, fetched_at TIMESTAMPTZ,
        PRIMARY KEY (conid, period_end, accession_no))"""


# The stored columns, in order. Every INSERT names them: the table gained
# `source` after it was created, and a positional INSERT into a table with one
# more column than it expects fails (or, worse, shifts).
COLUMNS = ('conid', 'ticker', 'period_end', 'accepted_at', 'form_type', 'accession_no',
           'revenue', 'net_income', 'gross_profit', 'operating_income', 'total_assets',
           'total_liabilities', 'stockholders_equity', 'cash_from_operations',
           'shares_diluted', 'fetched_at', 'source')


def ensure_schema(db) -> None:
    """Create the table, and add what was added since: ``source`` names the
    endpoint a row's numbers came from. NULL = the retired `/vX` endpoint,
    whose fiscal-year-end share counts are not share counts and whose counts
    are on the basis of the FILING day; ``financials_v1`` = the current one,
    whose counts are on the basis of the day they were FETCHED
    (trader/data/statements_v1.py)."""
    db.execute(FUNDAMENTALS_DDL, fetch='none')
    db.execute('ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS source VARCHAR', fetch='none')


def last_accepted(db, ticker: str):
    """Latest stored acceptance instant for ``ticker`` (None when unseen) -
    the incremental resume point."""
    r = db.execute('SELECT max(accepted_at) FROM fundamentals WHERE ticker = ?',
                   [ticker], fetch='one')
    return r[0] if r and r[0] is not None else None


def _accepted_on(filed_at) -> dt.date:
    """The New York calendar day a filing was accepted (EDGAR's clock)."""
    import pandas as pd
    stamp = pd.Timestamp(filed_at)
    stamp = stamp.tz_localize('America/New_York') if stamp.tzinfo is None else stamp.tz_convert('America/New_York')
    return stamp.date()


def ingest_ticker(db, client, key: str, index: dict, conid: str, ticker: str,
                  append_only: bool = False) -> dict:
    """Join the vendor's quarterly statements for ``ticker`` to the acceptance
    index and write them. ``append_only`` (incremental mode) uses
    ``INSERT OR IGNORE``: a row whose (conid, period_end, accession_no) is
    already stored is left exactly as it was - older rows are never rewritten,
    a restatement arrives as a NEW accession number and is simply added.

    Returns counts: ``written``; ``no_statement`` (a dated filing the vendor
    has no income statement for - yet, usually); ``other_filing`` (the vendor's
    numbers for that period come from another filing, usually a later one's
    comparative column, so they are not what this filing said); ``no_values`` 1 when the vendor could not be read, with
    the reason in ``error``.
    """
    counts = {'written': 0, 'no_statement': 0, 'other_filing': 0, 'no_values': 0, 'error': None}
    try:
        statements = fetch_statements(client, key, ticker)
    except StatementsUnavailable as ex:
        counts['no_values'], counts['error'] = 1, str(ex)
        return counts
    verb = 'INSERT OR IGNORE' if append_only else 'INSERT OR REPLACE'
    placeholders = ', '.join('now()' if column == 'fetched_at' else '?' for column in COLUMNS)
    for period, (filed_at, form, acc) in sorted(index.items()):
        values, reason = statements_v1.statement_values(
            {name: statements[name].get(str(period)[:10]) for name in statements_v1.STATEMENTS},
            _accepted_on(filed_at))
        if values is None:
            counts['other_filing' if reason == statements_v1.OTHER_FILING else 'no_statement'] += 1
            continue
        # RETURNING yields only the rows actually written: a key INSERT OR IGNORE
        # skipped is not new (the log once reported 160 "new rows" on a re-run
        # that wrote none, because the sec-api window re-offers the latest day).
        inserted = db.execute(
            f'{verb} INTO fundamentals ({", ".join(COLUMNS)}) VALUES ({placeholders}) RETURNING conid',
            [conid, ticker, period, filed_at, form, acc,
             values.get('revenue'), values.get('net_income'),
             values.get('gross_profit'), values.get('operating_income'),
             values.get('total_assets'), values.get('total_liabilities'),
             values.get('stockholders_equity'),
             values.get('cash_from_operations'),
             values.get('shares_diluted'), statements_v1.SOURCE], fetch='all')
        counts['written'] += len(inserted or [])
    return counts


def incremental_todo(hist_db, months: int = 3) -> list:
    """(conid='', ticker) for every name in the point-in-time top 500 at any
    of the last ``months`` month-ends - the names a live sleeve can hold.
    Unlike ``pit_membership_todo`` it does NOT skip covered names: incremental
    mode asks each for filings after its last stored acceptance."""
    import pandas as pd
    from trader.data.bar_quality import EXCHANGE_TEST_TICKERS
    df = hist_db.execute(
        'SELECT day, ticker, dv_rank FROM pit_daily_bars', fetch='df')
    if df is None or len(df) == 0:
        return []
    df = df[~df['ticker'].isin(EXCHANGE_TEST_TICKERS)]
    df['day'] = pd.to_datetime(df['day'])
    df['month'] = df['day'].dt.to_period('M')
    recent = sorted(df['month'].unique())[-months:]
    month_end = (df[df['month'].isin(recent)].sort_values('day')
                 .groupby(['month', 'ticker']).agg(rank=('dv_rank', 'last')).reset_index())
    names = sorted(month_end[month_end['rank'] <= 500]['ticker'].unique())
    return [('', t) for t in names]


def run_incremental(db, hist_db, client, vendor, key: str, months: int = 3,
                    limit: int = 0, sleep: float = 0.0, log=print, vendor_key: str = '') -> dict:
    """Incremental refresh (hillclimb C02, acceptance criterion 1): for each
    current PIT member ask sec-api only for filings accepted after the latest
    instant stored for that name, and append the dated statements. Idempotent:
    a re-run with nothing new fetches nothing new and writes nothing.

    ``client`` is the sec-api HTTP client, ``vendor`` the statement vendor's
    (the same ``httpx.Client`` serves both). The result's ``vendor_failed``
    counts names whose statements could not be read and ``aborted`` says the
    run stopped early on ``MAX_CONSECUTIVE_VENDOR_ERRORS`` of those in a row -
    an outage or a retired endpoint, not that many unlucky tickers.
    """
    ensure_schema(db)
    todo = incremental_todo(hist_db, months)
    if limit:
        todo = todo[:limit]
    log(f'incremental: {len(todo)} current point-in-time members')
    written = failed = no_new = vendor_failed = other_filing = no_statement = 0
    consecutive = 0
    aborted = None
    for i, (conid, ticker) in enumerate(todo):
        since = last_accepted(db, ticker)
        try:
            index = sec_filing_index(client, ticker, key, filed_after=since)
        except Exception:
            index = None
        if index is None:
            failed += 1
            continue
        if not index:
            no_new += 1
            continue
        res = ingest_ticker(db, vendor, vendor_key, index, conid, ticker, append_only=True)
        written += res['written']
        other_filing += res['other_filing']
        no_statement += res['no_statement']
        if res['no_values']:
            vendor_failed += 1
            consecutive += 1
            log(f'  {ticker}: statements unavailable ({res["error"]})')
            if consecutive >= MAX_CONSECUTIVE_VENDOR_ERRORS:
                aborted = (f'{consecutive} consecutive statement-vendor errors at {ticker} '
                           f'({i + 1}/{len(todo)}), last: {res["error"]}')
                log(f'ABORT: {aborted}')
                break
        else:
            consecutive = 0
        if (i + 1) % 25 == 0:
            log(f'  {i+1}/{len(todo)} rows={written} nonew={no_new} failed={failed} '
                f'vendor_failed={vendor_failed}')
        if sleep:
            time.sleep(sleep)
    log(f'done: {written} new rows, {no_new} names with nothing new, {failed} sec-api failures, '
        f'{vendor_failed} names whose statements could not be read, {no_statement} filings the vendor '
        f'has no statement for yet, {other_filing} whose statement the vendor took from another filing')
    return {'written': written, 'no_new': no_new, 'failed': failed, 'names': len(todo),
            'vendor_failed': vendor_failed, 'no_statement': no_statement,
            'other_filing': other_filing, 'aborted': aborted}


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
    ap.add_argument('--incremental', action='store_true',
                    help='append-only refresh: for every current PIT member '
                         '(top 500 at any of the last --months month-ends) '
                         'fetch only filings accepted after its latest stored '
                         'acceptance instant; stored rows are never rewritten')
    ap.add_argument('--months', type=int, default=3)
    args = ap.parse_args()

    import logging as _logging
    # httpx/httpcore DEBUG chatter is ~50 lines per name; at 2,000+ names it
    # drowns the log files for zero information.
    for noisy in ('httpx', 'httpcore'):
        _logging.getLogger(noisy).setLevel(_logging.WARNING)

    import httpx
    from trader.container import Container
    from trader.data.duckdb_store import DuckDBConnection
    from trader.data.universe import UniverseAccessor

    key = _key()
    if not key:
        print('no SEC_API_KEY in env or sec_api_key in config', file=sys.stderr)
        return 1
    vendor_key = massive_key()
    if not vendor_key:
        print('no MASSIVE_API_KEY in env or massive_api_key in config', file=sys.stderr)
        return 1

    cfg = Container.instance().config()
    db = DuckDBConnection(cfg.get('duckdb_path', ''))
    ensure_schema(db)

    if args.incremental:
        hist_db = DuckDBConnection(cfg.get('history_duckdb_path', ''))
        with httpx.Client() as client:
            res = run_incremental(db, hist_db, client, client, key, months=args.months,
                                  limit=args.limit, sleep=args.sleep,
                                  log=lambda m: print(m, flush=True), vendor_key=vendor_key)
        if res['aborted']:
            return 2
        # Names the vendor or sec-api could not answer for are a FAILED run:
        # "0 new rows" from a dead endpoint read as "nothing was filed" for
        # months.
        return 1 if (res['vendor_failed'] or res['failed']) else 0

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

    written = no_statement = other_filing = no_values = failed = no_filings = 0
    consecutive_errors = consecutive_vendor = 0
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
                # error. Skip the vendor call — there is nothing to date.
                no_filings += 1
                continue
            res = ingest_ticker(db, client, vendor_key, index, conid, ticker)
            written += res['written']
            no_statement += res['no_statement']
            other_filing += res['other_filing']
            if res['no_values']:
                no_values += 1
                consecutive_vendor += 1
                print(f'  {ticker}: statements unavailable ({res["error"]})', flush=True)
                if consecutive_vendor >= MAX_CONSECUTIVE_VENDOR_ERRORS:
                    print(f'ABORT: {consecutive_vendor} consecutive statement-vendor errors at '
                          f'{ticker} ({i}/{len(todo)}), last: {res["error"]}. {written} rows '
                          f'written so far.', flush=True)
                    return 2
            else:
                consecutive_vendor = 0
            if (i + 1) % 25 == 0:
                print(f'  {i+1}/{len(todo)}  rows={written} '
                      f'nostatement={no_statement} other_filing={other_filing} novalues={no_values} '
                      f'nofilings={no_filings} failed={failed}', flush=True)
            time.sleep(args.sleep)

    print(f'done: {written} rows written, {no_statement} filings the vendor has no statement '
          f'for, {other_filing} whose statement the vendor took from another filing (not stored: this ingest '
          f'cannot backfill history), {no_values} tickers whose statements could not be read, '
          f'{no_filings} with no 10-K/Q filings (funds/trusts), {failed} sec-api failures',
          flush=True)
    return 1 if (no_values or failed) else 0


if __name__ == '__main__':
    raise SystemExit(main())
