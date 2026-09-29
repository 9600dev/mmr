#!/usr/bin/env python3
"""Build a point-in-time universe and price panel, including the dead.

WHY THIS IS THE BINDING CONSTRAINT
    Every universe used in this repo so far is "today's top 500 by dollar
    volume", which silently conditions on survival. Names that delisted, were
    acquired, or went to zero are absent, and their absence has flattered three
    separate conclusions in the flattering direction:

      * long-only momentum (CAGR 28.9%): its top decile is loaded with the
        decade's winners because the universe was chosen after they won;
      * the size effect (IC 0.0637): the "small" names are small-and-present
        precisely because they grew INTO a liquidity-ranked list;
      * "drops recover": a stock that fell 6% and kept falling until it
        delisted is not in the sample at all.

    Until membership is decided using only information available at the time,
    "edge" and "we picked the winners" are indistinguishable.

HOW
    Polygon's grouped-daily endpoint returns every US ticker's bar for one
    session. Verified to serve historical sessions and to include names now
    gone - SIVB and FRC (both went to ~zero), TWTR, ATVI, VMW, XLNX. Walking it
    day by day and ranking by that day's dollar volume gives membership decided
    with that day's information, and the price panel falls out of the same
    calls.

WHY THIS TABLE IS TICKER-KEYED, WHICH LOOKS LIKE A RULE VIOLATION
    `tick_data` refuses ticker-keyed rows: a conId must resolve exactly or
    fail. That rule protects the LIVE TRADING path, where acting on an
    under-identified instrument can buy the wrong thing.

    A delisted instrument has no conId and never will, and cannot be traded at
    any price. So the risk the rule guards against does not exist here, while
    the cost of obeying it would be losing exactly the names that make the
    dataset honest. This lives in a SEPARATE table for that reason - so it can
    never be mistaken for tradeable data, and so nothing on the order path can
    reach it.
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


PIT_TABLE_DDL = """CREATE TABLE IF NOT EXISTS pit_daily_bars (
        day DATE, ticker VARCHAR, open DOUBLE, high DOUBLE, low DOUBLE,
        close DOUBLE, volume DOUBLE, dollar_volume DOUBLE, dv_rank INTEGER,
        PRIMARY KEY (day, ticker))"""


def stored_sessions(db) -> set:
    """Every session already holding at least one row."""
    return {r[0] for r in (db.execute(
        'SELECT DISTINCT day FROM pit_daily_bars', fetch='all') or [])}


def last_stored_session(db):
    """The latest stored session (``None`` on an empty table) - the resume
    point of the incremental mode (hillclimb C02, acceptance criterion 1)."""
    r = db.execute('SELECT max(day) FROM pit_daily_bars', fetch='one')
    return r[0] if r and r[0] is not None else None


def run_build(db, client, start: dt.date, end: dt.date, top: int = 800,
              sleep: float = 0.0, log=print) -> dict:
    """Fetch every weekday session in ``[start, end]`` that is NOT already
    stored and write its top-``top`` names.

    Idempotent and append-only by construction: a session with any stored row
    is skipped BEFORE the client is asked, so re-running never re-fetches and
    never rewrites an older row (``INSERT OR REPLACE`` only ever sees new
    sessions). ``client`` needs ``get_grouped_daily_aggs(iso_day, adjusted=True)``
    - the Massive ``RESTClient`` or a test fake. Returns the run's counters.
    """
    import pandas as pd
    from trader.data.bar_quality import impossible_mask, is_test_ticker

    db.execute(PIT_TABLE_DDL, fetch='none')
    done = stored_sessions(db)
    log(f'{start} -> {end}, {len(done)} sessions already stored')

    sessions = empty = rows = n_test = n_impossible = errors = skipped = 0
    day = start
    while day <= end:
        if day.weekday() >= 5:
            day += dt.timedelta(days=1)
            continue
        if day in done:
            skipped += 1
            day += dt.timedelta(days=1)
            continue
        try:
            bars = client.get_grouped_daily_aggs(day.isoformat(), adjusted=True)
        except Exception as exc:                          # noqa: BLE001
            errors += 1
            if errors <= 5:
                log(f'  {day}: {exc}')
            day += dt.timedelta(days=1)
            continue
        if not bars:
            empty += 1                 # holiday, or before coverage begins
            day += dt.timedelta(days=1)
            continue

        scored = []
        for b in bars:
            c, v = getattr(b, 'close', None), getattr(b, 'volume', None)
            t = getattr(b, 'ticker', None)
            if not t or not c or not v or c <= 0 or v <= 0:
                continue
            if is_test_ticker(t):
                n_test += 1
                continue
            scored.append((c * v, b))
        scored.sort(key=lambda x: -x[0])
        keep = scored[:top]

        payload = [(day, b.ticker, getattr(b, 'open', None),
                    getattr(b, 'high', None), getattr(b, 'low', None),
                    b.close, b.volume, dv, i + 1)
                   for i, (dv, b) in enumerate(keep)]

        # Same chokepoint discipline as TickData.write: nothing structurally
        # impossible is stored. Research data earns this as much as trading
        # data does - a high below the close is a row that never happened, and
        # a signal fitted through it has been fitted to fiction. Refused rows
        # are COUNTED, because a filter whose effect you cannot size is a claim
        # rather than a control.
        frame = pd.DataFrame(payload, columns=[
            'day', 'ticker', 'open', 'high', 'low', 'close', 'volume',
            'dollar_volume', 'dv_rank'])
        bad = impossible_mask(frame)
        n_impossible += int(bad.sum())
        payload = [row for row, is_bad in zip(payload, bad) if not is_bad]
        if payload:
            db.execute_atomic(lambda conn, p=payload: conn.executemany(
                'INSERT OR REPLACE INTO pit_daily_bars VALUES (?,?,?,?,?,?,?,?,?)', p))
            rows += len(payload)
            sessions += 1
        if sessions and sessions % 100 == 0:
            log(f'  {day}  sessions={sessions} rows={rows:,}')
        day += dt.timedelta(days=1)
        if sleep:
            time.sleep(sleep)

    log(f'done: {sessions} sessions, {rows:,} rows, {empty} empty days, '
        f'{skipped} already stored, {errors} fetch errors, '
        f'{n_test} test-ticker bars refused, {n_impossible} structurally '
        f'impossible bars refused')
    return {'sessions': sessions, 'rows': rows, 'empty': empty, 'skipped': skipped,
            'errors': errors, 'test_ticker_bars': n_test, 'impossible_bars': n_impossible}


def incremental_window(db, end: dt.date, fallback_start: dt.date):
    """``(start, end)`` for ``--incremental``: the day after the last stored
    session (or ``fallback_start`` on an empty table). Nothing before the
    resume point is ever fetched again."""
    last = last_stored_session(db)
    start = (last + dt.timedelta(days=1)) if last is not None else fallback_start
    return start, end


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--start', default='2016-08-01')
    ap.add_argument('--end', default=None)
    ap.add_argument('--top', type=int, default=800,
                    help='names kept per session (800 gives room to rank a '
                         'top-500 universe without the boundary clipping it)')
    ap.add_argument('--sleep', type=float, default=0.02)
    ap.add_argument('--incremental', action='store_true',
                    help='resume from the day after the last stored session '
                         '(--start is the fallback for an empty table); '
                         'stored sessions are never re-fetched or rewritten')
    args = ap.parse_args()

    import yaml
    from massive import RESTClient
    from trader.container import Container, default_config_path
    from trader.data.duckdb_store import DuckDBConnection

    ycfg = yaml.safe_load(open(default_config_path())) or {}
    client = RESTClient(os.environ.get('MASSIVE_API_KEY')
                        or ycfg.get('massive_api_key'))
    cfg = Container.instance().config()
    db = DuckDBConnection(cfg.get('history_duckdb_path', ''))

    # Deliberately NOT tick_data. See the module docstring: ticker-keyed by
    # necessity, research-only, and unreachable from the order path.
    db.execute(PIT_TABLE_DDL, fetch='none')

    end = dt.date.fromisoformat(args.end) if args.end else \
        dt.date.today() - dt.timedelta(days=1)
    start = dt.date.fromisoformat(args.start)
    if args.incremental:
        start, end = incremental_window(db, end, start)
        if start > end:
            print(f'up to date: last stored session {start - dt.timedelta(days=1)}, '
                  f'nothing to fetch through {end}', flush=True)
            return 0
    run_build(db, client, start, end, top=args.top, sleep=args.sleep,
              log=lambda m: print(m, flush=True))

    # The validation that proves it worked: how much of the historical
    # membership is GONE today? If this is near zero the build is still
    # survivorship-conditioned and nothing has been fixed.
    r = db.execute("""
        WITH hist AS (SELECT DISTINCT ticker FROM pit_daily_bars
                      WHERE dv_rank <= 500),
             recent AS (SELECT DISTINCT ticker FROM pit_daily_bars
                        WHERE dv_rank <= 500
                          AND day >= (SELECT max(day) - 30 FROM pit_daily_bars))
        SELECT (SELECT count(*) FROM hist), (SELECT count(*) FROM recent),
               (SELECT count(*) FROM hist WHERE ticker NOT IN
                    (SELECT ticker FROM recent))""", fetch='one')
    if r:
        print(f'\nhistorical top-500 members ever: {r[0]}')
        print(f'in the top 500 recently:          {r[1]}')
        print(f'GONE from the recent list:        {r[2]} '
              f'({100*r[2]/max(r[0],1):.0f}% of history)')
        print('  Those are the names whose absence biased every prior result.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
