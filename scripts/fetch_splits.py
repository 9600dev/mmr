#!/usr/bin/env python3
"""Keep the split history (`corporate_splits`, in the history database) current.

WHO READS IT
    * `mmr data audit` — telling an unadjusted split from a real re-rating
      needs a corporate-actions feed. 799 large-jump warnings had been sitting
      permanently accepted in the data-quality baseline for want of one, and
      an accepted warning is a warning nobody reads.
    * the research path (trader/data/share_basis.py) — the point-in-time
      prices are split-ADJUSTED and a filing's share count is AS REPORTED, so
      a market cap needs the splits between the two. Without them every price
      ratio is overstated, by the split ratio, for exactly the stocks that
      went on to split.
    * scripts/pit_split_adjust.py — the same rows say when stored prices have
      fallen onto two different bases.

WHAT A RUN DOES
    Asks the vendor for every split executed in a date window, MARKET-WIDE
    (one request per thousand rows), and upserts them. By default the window
    starts ``OVERLAP_DAYS`` before the newest stored execution date that is
    not in the future, so a split listed late is still picked up; ``--full``
    starts at ``FIRST_DAY``. It ends today: an ANNOUNCED split with a later
    date has changed no price yet and can still be changed or withdrawn.

    Until 2026-11-15 this asked ticker by ticker (6,300 requests, a quarter of
    an hour), swallowed every failed request, exited 0 whatever happened, and
    was run by hand, so the table was as old as the last time someone
    remembered. Rows it stored - back to 1978, and a few with future dates -
    are left as they are; readers compare dates.

WITHDRAWN ROWS
    The vendor's adjusted prices are adjusted by its CURRENT list, so a row it
    no longer lists (a split it re-dated, or recorded by mistake) is in no
    price it serves today and must not be in a share factor either. A stored
    row inside the window just read that the vendor did not return is moved
    to ``corporate_splits_withdrawn`` - moved, not deleted: it is the record
    of what an older price was adjusted by. A response that would withdraw
    more than ``WITHDRAW_LIMIT`` rows is far more likely a short answer than
    that many corrections: the run stops, stores nothing, and lists them;
    ``--accept-withdrawals`` goes ahead once they have been read.

WHAT IS STORED
    ticker, execution date, ``split_to``, ``split_from`` (a 4:1 split is
    from 1 to 4; a 1-for-10 reverse split from 10 to 1; the vendor also lists
    spin-off price adjustments here as odd ratios such as 1000 -> 1398, and
    adjusts its prices by them, so they belong), and the conId when the ticker
    is in a universe. One row per (ticker, execution date), replaced if the
    vendor corrects it.

FAILING
    A request that does not come back 200 with a ``results`` list RAISES:
    nothing from that run is stored and the exit status is 2. A partial table
    that looked complete would be a market cap silently wrong. So does an
    answer that would withdraw an implausible number of stored rows.
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

SPLITS_URL = 'https://api.massive.com/v3/reference/splits'
FIRST_DAY = dt.date(2009, 1, 1)             # before the first stored filing
OVERLAP_DAYS = 45
PAGE_LIMIT = 1000
MAX_PAGES = 500
WITHDRAW_LIMIT = 25

WITHDRAWN_DDL = """CREATE TABLE IF NOT EXISTS corporate_splits_withdrawn (
        ticker VARCHAR, conid VARCHAR, execution_date DATE,
        split_to DOUBLE, split_from DOUBLE, fetched_at TIMESTAMPTZ,
        withdrawn_at TIMESTAMPTZ)"""

SPLITS_DDL = """CREATE TABLE IF NOT EXISTS corporate_splits (
        ticker VARCHAR, conid VARCHAR, execution_date DATE,
        split_to DOUBLE, split_from DOUBLE, fetched_at TIMESTAMPTZ,
        PRIMARY KEY (ticker, execution_date))"""


class SplitsUnavailable(RuntimeError):
    """The vendor did not answer with a usable page of splits."""


def massive_key() -> str:
    """Env first, YAML second; never printed."""
    import yaml
    from trader.container import default_config_path
    env = os.environ.get('MASSIVE_API_KEY', '').strip()
    if env:
        return env
    cfg = yaml.safe_load(open(default_config_path())) or {}
    return str(cfg.get('massive_api_key', '') or '').strip()


def fetch_splits(client, key: str, since: dt.date, until: dt.date) -> list:
    """Every split with ``since <= execution_date <= until``, oldest first.

    ``client`` needs ``get(url, params=..., headers=..., timeout=...)``
    returning an object with ``status_code`` and ``json()`` (an
    ``httpx.Client``). Follows ``next_url`` to the end; a page that is not
    HTTP 200 with a ``results`` list raises ``SplitsUnavailable``.
    """
    headers = {'Authorization': f'Bearer {key}'}     # never in the URL: request URLs are logged
    url = SPLITS_URL
    params = {'execution_date.gte': since.isoformat(), 'execution_date.lte': until.isoformat(),
              'limit': PAGE_LIMIT, 'order': 'asc', 'sort': 'execution_date'}
    rows: list = []
    for _ in range(MAX_PAGES):
        response = client.get(url, params=params, headers=headers, timeout=60)
        if response.status_code != 200:
            raise SplitsUnavailable(f'splits request answered HTTP {response.status_code}')
        body = response.json()
        results = body.get('results') if isinstance(body, dict) else None
        if not isinstance(results, list):
            raise SplitsUnavailable('splits response carries no results list')
        rows.extend(results)
        url = body.get('next_url')
        if not url:
            return rows
        params = None                                 # next_url carries the query
    raise SplitsUnavailable(f'splits did not end within {MAX_PAGES} pages')


def usable_rows(rows, until: dt.date) -> tuple:
    """``(rows to store, how many were dropped)`` as
    ``(ticker, execution_date, split_to, split_from)`` tuples.

    Kept: a ticker, a readable execution date no later than ``until``, and a
    ratio that is a positive finite number other than 1 (the vendor lists a
    few 1:1 "splits"; they change nothing).
    """
    from trader.data.share_basis import split_events

    kept, dropped = [], 0
    for row in rows:
        events = split_events([row]) if isinstance(row, dict) else {}
        if not events:
            dropped += 1
            continue
        (ticker, [(day, _ratio)]), = events.items()
        if day > until:
            dropped += 1
            continue
        kept.append((ticker, day, float(row['split_to']), float(row['split_from'])))
    return kept, dropped


def store_splits(db, rows, conids=None) -> int:
    """Upsert ``(ticker, execution_date, split_to, split_from)`` tuples;
    returns how many were written. ``conids`` maps a ticker to its conId."""
    db.execute(SPLITS_DDL, fetch='none')
    conids = conids or {}
    if not rows:
        return 0

    def _write(conn):
        conn.executemany(
            'INSERT OR REPLACE INTO corporate_splits VALUES (?, ?, ?, ?, ?, now())',
            [[ticker, conids.get(ticker), day, split_to, split_from]
             for ticker, day, split_to, split_from in rows])
    db.execute_atomic(_write)
    return len(rows)


def no_longer_listed(db, rows, since: dt.date, until: dt.date) -> list:
    """``(ticker, execution_date)`` of stored rows dated inside
    ``[since, until]`` that are not among ``rows`` (what the vendor just
    listed for that window), oldest first."""
    db.execute(SPLITS_DDL, fetch='none')
    listed = {(ticker, day) for ticker, day, _to, _from in rows}
    stored = db.execute('SELECT ticker, execution_date FROM corporate_splits '
                        'WHERE execution_date BETWEEN ? AND ? ORDER BY execution_date, ticker',
                        [since, until], fetch='all') or []
    return [(ticker, day.date() if isinstance(day, dt.datetime) else day) for ticker, day in stored
            if (ticker, day.date() if isinstance(day, dt.datetime) else day) not in listed]


def withdraw(db, keys) -> int:
    """Move ``(ticker, execution_date)`` rows to ``corporate_splits_withdrawn``."""
    if not keys:
        return 0
    db.execute(WITHDRAWN_DDL, fetch='none')

    def _move(conn):
        for ticker, day in keys:
            conn.execute('INSERT INTO corporate_splits_withdrawn '
                         'SELECT ticker, conid, execution_date, split_to, split_from, fetched_at, now() '
                         'FROM corporate_splits WHERE ticker = ? AND execution_date = ?', [ticker, day])
            conn.execute('DELETE FROM corporate_splits WHERE ticker = ? AND execution_date = ?', [ticker, day])
    db.execute_atomic(_move)
    return len(keys)


def resume_from(db, today: dt.date) -> dt.date:
    """Where a default run starts: ``OVERLAP_DAYS`` before the newest stored
    execution date that is not in the future, or ``FIRST_DAY`` when there is
    none."""
    db.execute(SPLITS_DDL, fetch='none')
    row = db.execute('SELECT max(execution_date) FROM corporate_splits WHERE execution_date <= ?',
                     [today], fetch='one')
    newest = row[0] if row and row[0] is not None else None
    if newest is None:
        return FIRST_DAY
    if isinstance(newest, dt.datetime):
        newest = newest.date()
    return max(FIRST_DAY, newest - dt.timedelta(days=OVERLAP_DAYS))


def refresh(db, client, key: str, *, full: bool = False, today=None, conids=None, log=print,
            accept_withdrawals: bool = False) -> dict:
    today = today or dt.date.today()
    since = FIRST_DAY if full else resume_from(db, today)
    fetched = fetch_splits(client, key, since, today)
    rows, dropped = usable_rows(fetched, today)
    gone = no_longer_listed(db, rows, since, today)
    if len(gone) > WITHDRAW_LIMIT and not accept_withdrawals:
        shown = ', '.join(f'{ticker} {day}' for ticker, day in gone[:12])
        raise SplitsUnavailable(
            f'the vendor listed {len(fetched)} splits from {since} to {today} and {len(gone)} stored rows in '
            f'that window are not among them ({shown}{", ..." if len(gone) > 12 else ""}). More than '
            f'{WITHDRAW_LIMIT} looks like a short answer, not corrections; nothing was changed. Re-run, or '
            'pass --accept-withdrawals once the list has been read')
    written = store_splits(db, rows, conids)
    withdrawn = withdraw(db, gone)
    total = db.execute('SELECT count(*), max(execution_date) FROM corporate_splits WHERE execution_date <= ?',
                       [today], fetch='one')
    log(f'splits: {len(fetched)} listed from {since} to {today}, {written} stored, {dropped} not usable; '
        f'{total[0]} executed splits in the table, newest {total[1]}')
    if withdrawn:
        log(f'splits: {withdrawn} stored row(s) the vendor no longer lists moved to corporate_splits_withdrawn: '
            + ', '.join(f'{ticker} {day}' for ticker, day in gone[:20]) + (', ...' if withdrawn > 20 else ''))
    return {'listed': len(fetched), 'written': written, 'dropped': dropped, 'withdrawn': withdrawn,
            'stored': int(total[0]), 'since': since, 'until': today}


def universe_conids(accessor) -> dict:
    """``ticker -> conId`` for every instrument in a universe."""
    mapping = {}
    for name in accessor.list_universes():
        for definition in getattr(accessor.get(name), 'security_definitions', []):
            mapping[definition.symbol] = str(definition.conId)
    return mapping


def main() -> int:
    ap = argparse.ArgumentParser(description=(__doc__ or '').split('\n')[0])
    ap.add_argument('--full', action='store_true',
                    help=f're-read everything from {FIRST_DAY} (default: from {OVERLAP_DAYS} days before '
                         f'the newest stored split, or everything when the table is empty)')
    ap.add_argument('--accept-withdrawals', action='store_true',
                    help=f'go ahead when more than {WITHDRAW_LIMIT} stored rows in the window are no longer '
                         'listed by the vendor (they are moved to corporate_splits_withdrawn)')
    args = ap.parse_args()

    import logging as _logging
    for noisy in ('httpx', 'httpcore'):
        _logging.getLogger(noisy).setLevel(_logging.WARNING)

    import httpx
    from trader.container import Container
    from trader.data.duckdb_store import DuckDBConnection
    from trader.data.universe import UniverseAccessor

    key = massive_key()
    if not key:
        print('no MASSIVE_API_KEY in env or massive_api_key in config', file=sys.stderr)
        return 1
    cfg = Container.instance().config()
    db = DuckDBConnection(cfg.get('history_duckdb_path', ''))
    conids = universe_conids(UniverseAccessor(cfg.get('duckdb_path', ''),
                                              cfg.get('universe_library', 'Universes')))
    try:
        with httpx.Client() as client:
            refresh(db, client, key, full=args.full, conids=conids,
                    log=lambda m: print(m, flush=True), accept_withdrawals=args.accept_withdrawals)
    except SplitsUnavailable as ex:
        print(f'FAILED: {ex}; nothing from this run was stored', file=sys.stderr)
        return 2
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
