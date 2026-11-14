#!/usr/bin/env python3
"""Repair universe entries that name a FOREIGN listing but hold the US ticker's bars.

Until 2026-11-14 `mmr data download SYMBOL` resolved a name to the first local
match across every universe, then to the first row IB returned. 38 of the 514
members of the nightly refresh universe ended up as foreign listings (ORCL on
the Mexican exchange, CCL = Coles Group on ASX, SU = Schneider Electric in
Paris...), while TwelveData — asked for the bare ticker — kept answering with
the US listing. The US ticker's bars were therefore stored under the foreign
conId. The download path now refuses that (trader/data/listing_guard.py); this
script repairs what was already stored.

For every non-US entry in the universe it:
  1. finds the ONE US listing of the same ticker at IB (refuses on ambiguity);
  2. re-keys the stored bars from the foreign conId to the US conId — only rows
     where the US key has no bar for that (date, bar_size), never overwriting;
  3. deletes what is left under the foreign conId;
  4. replaces the universe entry with the US listing.

Rows are only touched when they are provably the vendor's: a foreign conId that
is ALSO a member of another universe may hold genuine bars for that listing
(IB-sourced), so there only rows carrying the TwelveData signature
(`bar_count = 0 AND average IS NULL`) are moved.

Dry-run by default; pass --apply to change anything. Needs trader_service (IB
discovery) and must run where the live DuckDB files are (inside the container).
Take a backup first: `./docker.sh -B <name>`.
"""

from __future__ import annotations

import argparse
import sys


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    parser.add_argument('--universe', default='downloads')
    parser.add_argument('--apply', action='store_true', help='make the changes (default: report only)')
    args = parser.parse_args()

    from trader.container import Container
    from trader.data.duckdb_store import DuckDBConnection, DuckDBDataStore
    from trader.data.listing_guard import is_us_listing
    from trader.data.universe import UniverseAccessor
    from trader.sdk import MMR

    cfg = Container.instance().config()
    main_db = cfg.get('duckdb_path', '')
    history_db = cfg.get('history_duckdb_path', '') or main_db
    accessor = UniverseAccessor(main_db, cfg.get('universe_library', 'Universes'))
    universe = accessor.get(args.universe)
    if universe is None or not universe.security_definitions:
        print(f'universe {args.universe!r} is empty or missing')
        return 1

    foreign = [sd for sd in universe.security_definitions if not is_us_listing(sd)]
    print(f'{args.universe}: {len(universe.security_definitions)} members, {len(foreign)} not US listings')
    if not foreign:
        return 0

    elsewhere: dict[int, list[str]] = {}
    for other in accessor.get_all():
        if other.name == args.universe:
            continue
        for sd in other.security_definitions:
            elsewhere.setdefault(int(sd.conId), []).append(other.name)

    table = DuckDBDataStore.TABLE_NAME
    db = DuckDBConnection(history_db)

    def count(symbol: str, vendor_only: bool) -> int:
        clause = ' AND bar_count = 0 AND average IS NULL' if vendor_only else ''
        row = db.execute(f'SELECT count(*) FROM {table} WHERE symbol = ?{clause}', [symbol], fetch='one')
        return int(row[0]) if row else 0

    plan, refused = [], []
    with MMR() as mmr:
        for sd in foreign:
            try:
                listings = [c for c in mmr.discover_listings(sd.symbol, currency='USD') if is_us_listing(c)]
            except Exception as ex:
                refused.append((sd, f'IB discovery failed: {ex}'))
                continue
            conids = sorted({int(c.conId) for c in listings})
            if len(conids) != 1:
                refused.append((sd, f'{len(conids)} US listings at IB: {conids}'))
                continue
            us = next(c for c in listings if int(c.conId) == conids[0])
            shared = elsewhere.get(int(sd.conId), [])
            plan.append({
                'old': sd, 'new': us, 'shared': shared,
                'movable': count(str(sd.conId), vendor_only=bool(shared)),
                'total_old': count(str(sd.conId), vendor_only=False),
                'existing_new': count(str(us.conId), vendor_only=False),
            })

    print(f'\n{"symbol":7} {"foreign conId":>13} {"venue":8} {"US conId":>10} {"venue":7} '
          f'{"rows to move":>12} {"rows at old":>11} {"rows at US":>10}  note')
    for item in plan:
        old, new = item['old'], item['new']
        note = f'also in {item["shared"]}: vendor rows only' if item['shared'] else ''
        print(f'{old.symbol:7} {old.conId:>13} {(old.primaryExchange or old.exchange):8} '
              f'{new.conId:>10} {(new.primaryExchange or new.exchange):7} '
              f'{item["movable"]:>12,} {item["total_old"]:>11,} {item["existing_new"]:>10,}  {note}')
    for sd, reason in refused:
        print(f'{sd.symbol:7} {sd.conId:>13} REFUSED - {reason}')

    if not args.apply:
        print('\ndry run - nothing changed. Re-run with --apply (after a backup).')
        return 0 if not refused else 2

    moved = deleted = 0
    for item in plan:
        old_key, new_key = str(item['old'].conId), str(item['new'].conId)
        vendor = ' AND bar_count = 0 AND average IS NULL' if item['shared'] else ''

        def work(conn, old_key=old_key, new_key=new_key, vendor=vendor):
            before = conn.execute(f'SELECT count(*) FROM {table} WHERE symbol = ?', [new_key]).fetchone()[0]
            conn.execute(
                f'UPDATE {table} SET symbol = ? WHERE symbol = ?{vendor} AND NOT EXISTS ('
                f'SELECT 1 FROM {table} AS kept WHERE kept.symbol = ? AND kept.date = {table}.date '
                f'AND kept.bar_size IS NOT DISTINCT FROM {table}.bar_size)',
                [new_key, old_key, new_key])
            after = conn.execute(f'SELECT count(*) FROM {table} WHERE symbol = ?', [new_key]).fetchone()[0]
            left = conn.execute(f'SELECT count(*) FROM {table} WHERE symbol = ?{vendor}', [old_key]).fetchone()[0]
            conn.execute(f'DELETE FROM {table} WHERE symbol = ?{vendor}', [old_key])
            return after - before, left

        gained, dropped = db.execute_atomic(work)
        moved += gained
        deleted += dropped

    by_old = {int(item['old'].conId): item['new'] for item in plan}
    have = {int(sd.conId) for sd in universe.security_definitions}
    repaired = []
    for sd in universe.security_definitions:
        replacement = by_old.get(int(sd.conId))
        if replacement is None:
            repaired.append(sd)
        elif int(replacement.conId) not in have:
            repaired.append(replacement)
            have.add(int(replacement.conId))
    universe.security_definitions = repaired
    accessor.update(universe)

    print(f'\napplied: {len(plan)} entries repaired, {moved:,} rows re-keyed, '
          f'{deleted:,} duplicate rows removed; {len(refused)} refused (left as they were).')
    return 0 if not refused else 2


if __name__ == '__main__':
    sys.exit(main())
