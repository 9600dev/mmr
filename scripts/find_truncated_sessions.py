#!/usr/bin/env python3
"""Report stored 1-minute sessions that stop well short of the close.

A session written while it was still trading (strategy priming on a
mid-session restart, a download run before the close) used to stay truncated
forever: `TickData.missing()` judged coverage by date, so the day counted as
present and nothing fetched the rest. Since 2026-11-14 the newest five stored
sessions are re-checked on every incremental run; this script finds the OLDER
ones, which need a one-off `--force` re-download.

Read-only. Run it where the live DuckDB files are (inside the container):

    python scripts/find_truncated_sessions.py --days 120

It prints the truncated (symbol, session) pairs and, per data source, the
`mmr data download ... --force` command that repairs them.
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
from collections import defaultdict


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    parser.add_argument('--days', type=int, default=120, help='how far back to look (default 120)')
    parser.add_argument('--tolerance-minutes', type=int, default=30,
                        help='a session is truncated when its last bar is earlier than '
                             'the regular close minus this (default 30)')
    args = parser.parse_args()

    import pandas as pd

    from trader.container import Container
    from trader.data.duckdb_store import DuckDBConnection, DuckDBDataStore
    from trader.data.listing_guard import is_us_listing
    from trader.data.session_guard import calendar_for
    from trader.data.universe import UniverseAccessor

    cfg = Container.instance().config()
    main_db = cfg.get('duckdb_path', '')
    history_db = cfg.get('history_duckdb_path', '') or main_db
    accessor = UniverseAccessor(main_db, cfg.get('universe_library', 'Universes'))

    definitions = {}
    for universe in accessor.get_all():
        for sd in universe.security_definitions:
            definitions.setdefault(str(int(sd.conId)), sd)

    by_zone: dict = defaultdict(list)
    calendars = {}
    for key, sd in definitions.items():
        calendar = calendar_for(sd)
        if calendar is None:
            continue
        calendars[key] = calendar
        by_zone[str(calendar.tz)].append(key)

    table = DuckDBDataStore.TABLE_NAME
    db = DuckDBConnection(history_db)
    since = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=args.days)
    tolerance = pd.Timedelta(minutes=args.tolerance_minutes)
    now = pd.Timestamp.now(tz='UTC')

    truncated = []
    for zone, keys in by_zone.items():
        marks = ','.join('?' for _ in keys)
        rows = db.execute(
            f"SELECT symbol, CAST(timezone('{zone}', date) AS DATE) AS day, max(date) AS last, "
            f"count(*) AS bars FROM {table} WHERE bar_size = '1 min' AND close IS NOT NULL "
            f"AND date >= ? AND symbol IN ({marks}) GROUP BY 1, 2",
            [since, *keys], fetch='all') or []
        for symbol, day, last, bars in rows:
            calendar = calendars[symbol]
            try:
                if not calendar.is_session(day.isoformat()):
                    continue
                close = pd.Timestamp(calendar.session_close(day.isoformat()))
            except Exception:
                continue
            if close.tzinfo is None:
                close = close.tz_localize('UTC')
            if close > now:
                continue                      # still trading: not a stored defect
            last_ts = pd.Timestamp(last)
            if last_ts.tzinfo is None:
                last_ts = last_ts.tz_localize('UTC')
            if last_ts < close - tolerance:
                truncated.append((definitions[symbol], day, last_ts.tz_convert(zone), int(bars)))

    truncated.sort(key=lambda item: (item[0].symbol, item[1]))
    print(f'{len(truncated)} truncated 1-min session(s) in the last {args.days} days '
          f'({len(definitions)} instruments checked)')
    for sd, day, last, bars in truncated:
        print(f'  {sd.symbol:7} {sd.conId:>11} {(sd.primaryExchange or sd.exchange):8} {day}  '
              f'last bar {last:%H:%M} {last.tzname()}  {bars} bars')

    if not truncated:
        return 0
    oldest = min(day for _, day, _, _ in truncated)
    span = (dt.date.today() - oldest).days + 2
    us = sorted({sd.symbol for sd, *_ in truncated if is_us_listing(sd)})
    other = sorted({sd.symbol for sd, *_ in truncated if not is_us_listing(sd)})
    print('\nrepair:')
    if us:
        print(f'  mmr data download {" ".join(us)} --bar-size "1 min" --days {span} --force')
    if other:
        print(f'  mmr data download {" ".join(other)} --bar-size "1 min" --days {span} --source ib --force')
    return 3


if __name__ == '__main__':
    sys.exit(main())
