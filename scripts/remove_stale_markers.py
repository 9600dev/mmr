#!/usr/bin/env python3
"""Remove no-data markers that sit beside real bars.

A marker is an all-NULL row in ``tick_data``: "the source had nothing for this
day". It is only true of a day with no bar at all. Until 2026-11-16 the IB
history worker also wrote one for the exchange's CURRENT day when it was asked
before that day had traded (a service restarted before the open); the day's
real bars arrived later and the marker stayed, which the nightly ``mmr data
audit`` reports as an ``empty_bar`` on a trading day.

A marker is STALE when the same instrument has a real bar of the same bar size
on the marker's own calendar day AT ITS VENUE. The venue's day, not "the next
24 hours": older markers are stamped at a time of day rather than at midnight,
and a Sunday marker stamped 04:00 has Monday's pre-market bars within 24 hours
of it while being perfectly true of Sunday. An instrument whose venue is not
known here is not judged at all.

With ``--apply`` the stale markers are deleted and nothing else. Dry run by
default. Run it where the live DuckDB files are (inside the container):

    python scripts/remove_stale_markers.py            # list
    python scripts/remove_stale_markers.py --apply    # delete them
"""

from __future__ import annotations

import argparse
import os
import sys

from typing import Dict, List, Mapping, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

MARKER = 'open IS NULL AND high IS NULL AND low IS NULL AND close IS NULL'
Stale = Tuple[str, str, object, int]        # symbol, bar size, the marker's stamp, real bars that venue day


def venue_day(stamp, timezone: str):
    """``(start, end)`` of the calendar day ``stamp`` falls on in ``timezone``
    (23 or 25 hours long when the clocks change)."""
    import pandas as pd
    local = pd.Timestamp(stamp).tz_convert(timezone)
    start = local.normalize()
    return start, (start + pd.Timedelta(days=1, hours=12)).normalize()


def _find(conn, timezones: Mapping[str, str]) -> Tuple[List[Stale], List[str]]:
    import pandas as pd
    markers = conn.execute(f'SELECT symbol, bar_size, date FROM tick_data WHERE {MARKER}').fetchall()
    unjudged = sorted({str(symbol) for symbol, _, _ in markers if str(symbol) not in timezones})
    rows = []
    for symbol, bar_size, stamp in markers:
        timezone = timezones.get(str(symbol))
        if timezone is None:
            continue
        start, end = venue_day(stamp, timezone)
        rows.append((str(symbol), str(bar_size), pd.Timestamp(stamp), start.tz_convert('UTC'), end.tz_convert('UTC')))
    if not rows:
        return [], unjudged
    marker_days = pd.DataFrame(rows, columns=['symbol', 'bar_size', 'date', 'day_start', 'day_end'])  # noqa: F841
    conn.register('marker_days', marker_days)
    try:
        found = conn.execute('''
            SELECT m.symbol, m.bar_size, m.date, count(*) AS real_bars
            FROM marker_days m JOIN tick_data r
              ON r.symbol = m.symbol AND r.bar_size = m.bar_size AND r.close IS NOT NULL
             AND r.date >= m.day_start AND r.date < m.day_end
            GROUP BY m.symbol, m.bar_size, m.date
            ORDER BY m.date, m.symbol, m.bar_size''').fetchall()
    finally:
        conn.unregister('marker_days')
    return [(s, b, d, int(n)) for s, b, d, n in found], unjudged


def stale_markers(db, timezones: Mapping[str, str]) -> Tuple[List[Stale], List[str]]:
    """``(stale markers, instruments with markers and no known venue)``."""
    return db.execute_atomic(lambda conn: _find(conn, timezones))


def remove_stale_markers(db, timezones: Mapping[str, str]):
    """Delete them; returns ``(stale, unjudged, rows before, rows after)``.
    One transaction (``execute_atomic`` commits on success and rolls back on
    any exception): nothing changes unless exactly the listed markers went."""
    import pandas as pd

    def run(conn):
        stale, unjudged = _find(conn, timezones)
        before = conn.execute('SELECT count(*) FROM tick_data').fetchall()[0][0]
        if stale:
            doomed = pd.DataFrame([(s, b, d) for s, b, d, _ in stale], columns=['symbol', 'bar_size', 'date'])  # noqa: F841
            conn.register('doomed', doomed)
            try:
                expected = conn.execute(f'''
                    SELECT count(*) FROM tick_data t JOIN doomed d
                      ON t.symbol = d.symbol AND t.bar_size = d.bar_size AND t.date = d.date
                    WHERE t.{MARKER.replace(" AND ", " AND t.")}''').fetchall()[0][0]
                conn.execute(f'''
                    DELETE FROM tick_data t USING doomed d
                    WHERE t.symbol = d.symbol AND t.bar_size = d.bar_size AND t.date = d.date
                      AND t.{MARKER.replace(" AND ", " AND t.")}''')
            finally:
                conn.unregister('doomed')
        else:
            expected = 0
        after = conn.execute('SELECT count(*) FROM tick_data').fetchall()[0][0]
        left, _ = _find(conn, timezones)
        if before - after != expected or left:
            raise RuntimeError(f'expected to delete {expected} marker row(s); the table went from {before} to '
                               f'{after} rows and {len(left)} stale marker(s) remain - nothing was changed')
        return stale, unjudged, before, after
    return db.execute_atomic(run)


def venue_timezones(cfg) -> Dict[str, str]:
    """``{conId as stored in tick_data: IANA zone of its listing venue}`` for
    every instrument in a local universe whose venue has a calendar - the
    lookup the data audit uses (``session_guard.calendar_for``)."""
    from trader.data.session_guard import calendar_for
    from trader.data.universe import UniverseAccessor

    zones: Dict[str, str] = {}
    accessor = UniverseAccessor(cfg.get('duckdb_path', ''), cfg.get('universe_library', 'Universes'))
    for universe in accessor.get_all():
        for definition in universe.security_definitions:
            key = str(int(definition.conId))
            if key in zones:
                continue
            calendar = calendar_for(definition)
            if calendar is not None:
                zones[key] = str(calendar.tz)
    return zones


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    parser.add_argument('--apply', action='store_true', help='delete the markers (default: list them)')
    args = parser.parse_args(argv)

    from trader.container import Container
    from trader.data.duckdb_store import DuckDBConnection
    cfg = Container.instance().config()
    db = DuckDBConnection(cfg.get('history_duckdb_path', '') or cfg.get('duckdb_path', ''))
    timezones = venue_timezones(cfg)

    if args.apply:
        stale, unjudged, before, after = remove_stale_markers(db, timezones)
        verb, tail = 'removed     ', f'{len(stale)} stale marker(s) removed; tick_data {before:,} -> {after:,} rows'
    else:
        stale, unjudged = stale_markers(db, timezones)
        verb, tail = 'stale marker', f'{len(stale)} stale marker(s); nothing changed (dry run - pass --apply to delete them)'
    for symbol, bar_size, stamp, real in stale:
        local = venue_day(stamp, timezones[symbol])[0].date()
        print(f'{verb}  {symbol:<12} {bar_size:<8} {local} ({timezones[symbol]})  {real} real bar(s) that day')
    if unjudged:
        print(f'not judged: {len(unjudged)} instrument(s) with markers and no known venue calendar '
              f'({", ".join(unjudged[:12])}{" ..." if len(unjudged) > 12 else ""})')
    print(tail)
    return 0


if __name__ == '__main__':
    sys.exit(main())
