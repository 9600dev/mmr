#!/usr/bin/env python3
"""Put every bar of ``pit_daily_bars`` on one split basis, and keep it there.

WHAT IS WRONG
    ``build_pit_universe.py`` writes each session from the vendor's grouped-
    daily endpoint with ``adjusted=True``: split-adjusted as of the day it was
    FETCHED. Sessions are appended weeks apart, so a split executing between
    two fetches leaves a split-sized jump in the stored series at the junction
    of the two fetches (MNST 1:2, executed 2026-08-11: 97.74 on 2026-07-28,
    48.62 on 2026-07-29). A fake -50% day is a fake return in every backtest,
    benchmark and paper ledger that reads the table. The bulk endpoint also
    never applied a few old splits (WLL 2017-11-09, CGNX 2017-12-04, ESV
    2019-04-11, CLBK 2026-07-21): the same jump, on the execution date.

WHAT THIS DOES
    For every stored ticker that has a split after its first stored day, it
    fetches the vendor's adjusted daily series for that ticker (one request)
    and compares ``stored close / vendor close`` day by day. The rule, its
    tolerance and what it refuses are in ``trader/data/pit_adjustment.py``.
    A stale run of days has its prices divided by the product of the splits
    it lacks and its volume multiplied by it; ``dollar_volume`` and
    ``dv_rank`` are split-invariant and are not touched.

    The default is a DRY RUN: it prints what it would change and what it
    refuses, and writes nothing. ``--apply`` writes, one transaction per
    ticker, and each transaction re-reads the rows it changed and commits
    only if they now agree with the vendor's series.

WHAT IT RECORDS (history DuckDB, created by the first ``--apply``)
    pit_split_adjustments   one row per rescaled run of days: when, which
                            days, the factor, the splits behind it. A second
                            ``--apply`` finds nothing to change; and a run
                            that asks to apply a split to days this table says
                            already received it is REFUSED rather than
                            compounded.
    pit_split_refusals      the tickers currently refused, why, and the
                            splits their prices are not verified to reflect.
                            A reader that needs a uniform basis must leave
                            these tickers out.
    pit_price_basis         append-only ``(adjusted_through, recorded_at,
                            note)``; readers take ``max(adjusted_through)``.
                            Every ticker NOT in pit_split_refusals reflects
                            every split of ``corporate_splits`` executed on or
                            before that day. Written only by a run that looked
                            at every such split (no ``--tickers``; ``--since``
                            only when the basis on record reaches it). The run
                            that FIRST refuses a split does not record a basis
                            past the day before it (``basis_to_record``).

EXIT STATUS
    0 nothing refused; 1 something was refused (or a write failed); 2 usage.

    python scripts/pit_split_adjust.py                      # dry run, everything
    python scripts/pit_split_adjust.py --apply
    python scripts/pit_split_adjust.py --since 2026-11-01 --apply    # nightly
    python scripts/pit_split_adjust.py --tickers MNST APH
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import sys
import time

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from trader.data import pit_adjustment as pa                    # noqa: E402
from trader.data.share_basis import SplitEvent, split_events    # noqa: E402

MARKET_TZ = ZoneInfo('America/New_York')

AUDIT_DDL = """CREATE TABLE IF NOT EXISTS pit_split_adjustments (
        applied_at TIMESTAMPTZ, ticker VARCHAR, first_day DATE, last_day DATE,
        n_rows INTEGER, factor DOUBLE, splits VARCHAR, vendor_lacks VARCHAR,
        basis_date DATE, close_before DOUBLE, close_after DOUBLE)"""
REFUSALS_DDL = """CREATE TABLE IF NOT EXISTS pit_split_refusals (
        ticker VARCHAR, reason VARCHAR, detail VARCHAR, unverified_splits VARCHAR,
        refused_at TIMESTAMPTZ)"""
BASIS_DDL = """CREATE TABLE IF NOT EXISTS pit_price_basis (
        adjusted_through DATE, recorded_at TIMESTAMPTZ, note VARCHAR)"""

_CHUNK = 400


class RowsChanged(RuntimeError):
    """The stored rows are not the ones the plan was made from."""


@dataclass
class RunResult:
    run_date: dt.date
    applied: bool
    stored_tickers: int = 0
    plans: List[pa.TickerPlan] = field(default_factory=list)
    refused: List[pa.TickerPlan] = field(default_factory=list)
    rows_changed: int = 0
    api_calls: int = 0
    basis: Optional[dt.date] = None
    basis_explanation: str = ''
    basis_recorded: bool = False

    @property
    def adjusted(self) -> List[pa.TickerPlan]:
        return [p for p in self.plans if p.verdict == pa.ADJUST]

    @property
    def exit_code(self) -> int:
        return 1 if self.refused else 0

    def as_dict(self) -> Dict[str, Any]:
        return {
            'run_date': self.run_date.isoformat(), 'applied': self.applied,
            'stored_tickers': self.stored_tickers, 'checked': len(self.plans),
            'api_calls': self.api_calls, 'rows_changed': self.rows_changed,
            'adjust': [{
                'ticker': p.ticker,
                'segments': [{'first_day': s.first_day.isoformat(), 'last_day': s.last_day.isoformat(),
                              'rows': s.rows, 'factor': s.factor,
                              'splits': [[d.isoformat(), r] for d, r in s.splits],
                              'vendor_lacks': [[d.isoformat(), r] for d, r in s.vendor_lacks]}
                             for s in p.segments]} for p in self.adjusted],
            'refused': [{'ticker': p.ticker, 'reason': p.reason, 'detail': p.detail,
                         'unverified_splits': [d.isoformat() for d in p.unverified_splits]}
                        for p in self.refused],
            'price_notes': [{'ticker': p.ticker, 'day': n.day.isoformat(), 'stored_close': n.stored_close,
                             'vendor_close': n.reference_close, 'basis_factor': n.factor,
                             'residual': n.residual} for p in self.plans for n in p.price_notes],
            'basis': {'adjusted_through': self.basis.isoformat() if self.basis else None,
                      'recorded': self.basis_recorded, 'explanation': self.basis_explanation},
            'exit_code': self.exit_code,
        }


# -- reading ------------------------------------------------------------------

def table_exists(db, name: str) -> bool:
    row = db.execute('SELECT count(*) FROM information_schema.tables WHERE table_name = ?', [name], fetch='one')
    return bool(row and row[0])


def load_split_rows(db) -> List[Dict[str, Any]]:
    """``corporate_splits`` as rows for ``share_basis.split_events``. Selected
    BY NAME: the table stores split_to before split_from."""
    rows = db.execute('SELECT ticker, execution_date, split_from, split_to FROM corporate_splits',
                      fetch='all') or []
    return [{'ticker': t, 'execution_date': d, 'split_from': f, 'split_to': s} for t, d, f, s in rows]


def splits_fetched_through(db) -> Optional[dt.date]:
    """The market date of the newest ``fetched_at``: what the split table can
    be said to be current to. None when it does not say."""
    row = db.execute('SELECT epoch_ms(max(fetched_at)) FROM corporate_splits', fetch='one')
    if not row or row[0] is None:
        return None
    return dt.datetime.fromtimestamp(row[0] / 1000, tz=MARKET_TZ).date()


def daily_closes(client, ticker: str, start: dt.date, end: dt.date, adjusted: bool) -> Dict[dt.date, float]:
    """``day -> close`` of the vendor's daily series for one ticker. ``client``
    needs ``list_aggs`` (the Massive ``RESTClient``, or a test fake)."""
    out: Dict[dt.date, float] = {}
    for bar in client.list_aggs(ticker, 1, 'day', start.isoformat(), end.isoformat(),
                                adjusted=adjusted, sort='asc', limit=50000):
        stamp, close = getattr(bar, 'timestamp', None), getattr(bar, 'close', None)
        if stamp is None or close is None:
            continue
        out[dt.datetime.fromtimestamp(stamp / 1000, tz=MARKET_TZ).date()] = float(close)
    return out


def _stored_closes(db, tickers: Sequence[str]) -> Dict[str, List[Tuple[dt.date, float]]]:
    out: Dict[str, List[Tuple[dt.date, float]]] = {t: [] for t in tickers}
    for i in range(0, len(tickers), _CHUNK):
        chunk = list(tickers[i:i + _CHUNK])
        marks = ','.join('?' * len(chunk))
        for ticker, day, close in db.execute(
                f'SELECT ticker, day, close FROM pit_daily_bars WHERE ticker IN ({marks}) ORDER BY ticker, day',
                chunk, fetch='all') or []:
            out[ticker].append((day, close))
    return out


def _events_json(events: Iterable[SplitEvent]) -> str:
    return json.dumps([[d.isoformat(), r] for d, r in events])


def _events_from_json(text: Optional[str]) -> Tuple[SplitEvent, ...]:
    return tuple((dt.date.fromisoformat(d), float(r)) for d, r in json.loads(text or '[]'))


def _prior_adjustments(db) -> Dict[str, List[pa.AppliedAdjustment]]:
    out: Dict[str, List[pa.AppliedAdjustment]] = {}
    if not table_exists(db, 'pit_split_adjustments'):
        return out
    for ticker, first, last, splits, applied_at in db.execute(
            'SELECT ticker, first_day, last_day, splits, CAST(applied_at AS VARCHAR) FROM pit_split_adjustments',
            fetch='all') or []:
        out.setdefault(ticker, []).append(pa.AppliedAdjustment(first, last, _events_from_json(splits), applied_at))
    return out


def _standing_refusals(db) -> Dict[str, Tuple[str, str, str]]:
    """``ticker -> (reason, detail, unverified splits as JSON)``."""
    if not table_exists(db, 'pit_split_refusals'):
        return {}
    return {t: (reason, detail, unverified) for t, reason, detail, unverified in db.execute(
        'SELECT ticker, reason, detail, unverified_splits FROM pit_split_refusals', fetch='all') or []}


def recorded_basis(db) -> Optional[dt.date]:
    if not table_exists(db, 'pit_price_basis'):
        return None
    row = db.execute('SELECT max(adjusted_through) FROM pit_price_basis', fetch='one')
    return row[0] if row else None


# -- writing ------------------------------------------------------------------

def _apply_plan(db, plan: pa.TickerPlan, run_date: dt.date,
                recheck: Callable[[List[Tuple[dt.date, float]]], pa.TickerPlan]) -> None:
    """Rescale one ticker's stale days and log it, in ONE transaction that
    commits only if the rows it read are the rows the plan was made from and
    the rows it leaves agree with the vendor's series."""
    def _transaction(conn):
        for seg in plan.segments:
            count, first, last = conn.execute(
                'SELECT count(*), min(day), max(day) FROM pit_daily_bars WHERE ticker = ? AND day BETWEEN ? AND ?',
                [plan.ticker, seg.first_day, seg.last_day]).fetchone()
            close = conn.execute('SELECT close FROM pit_daily_bars WHERE ticker = ? AND day = ?',
                                 [plan.ticker, seg.first_day]).fetchone()
            if (count, first, last) != (seg.rows, seg.first_day, seg.last_day) \
                    or close is None or close[0] != seg.first_close:
                raise RowsChanged(
                    f'{seg.first_day} .. {seg.last_day}: planned {seg.rows} rows starting at close '
                    f'{seg.first_close!r}, found {count} rows ({first} .. {last}) starting at '
                    f'{close[0] if close else None!r}')
            conn.execute(
                'UPDATE pit_daily_bars SET open = open / ?, high = high / ?, low = low / ?, close = close / ?, '
                'volume = volume * ? WHERE ticker = ? AND day BETWEEN ? AND ?',
                [seg.factor] * 5 + [plan.ticker, seg.first_day, seg.last_day])
            conn.execute(
                'INSERT INTO pit_split_adjustments VALUES (now(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                [plan.ticker, seg.first_day, seg.last_day, seg.rows, seg.factor, _events_json(seg.splits),
                 _events_json(seg.vendor_lacks), run_date, seg.first_close, seg.first_close / seg.factor])
        after = recheck(conn.execute(
            'SELECT day, close FROM pit_daily_bars WHERE ticker = ? ORDER BY day', [plan.ticker]).fetchall())
        if after.verdict != pa.CLEAN:
            raise RowsChanged(f'after rescaling, the stored series still does not agree with the vendor\'s '
                              f'({after.verdict}: {after.reason or len(after.segments)} {after.detail})')
    db.execute_atomic(_transaction)


def _record_refusals(db, refused: Sequence[pa.TickerPlan], checked: Set[str],
                     standing: Mapping[str, Tuple[str, str, str]]) -> None:
    """Make ``pit_split_refusals`` say what this run found, touching only rows
    whose content changed (a re-run that finds the same thing writes nothing)."""
    now_refused = {p.ticker: (p.reason, p.detail, json.dumps([d.isoformat() for d in p.unverified_splits]))
                   for p in refused}
    inserts = [(t, *content) for t, content in now_refused.items() if t not in standing]
    updates = [(*content, t) for t, content in now_refused.items() if t in standing and standing[t] != content]
    resolved = [t for t in standing if t in checked and t not in now_refused]
    if not (inserts or updates or resolved):
        return

    def _transaction(conn):
        for row in inserts:
            conn.execute('INSERT INTO pit_split_refusals VALUES (?, ?, ?, ?, now())', list(row))
        for row in updates:
            conn.execute('UPDATE pit_split_refusals SET reason = ?, detail = ?, unverified_splits = ? '
                         'WHERE ticker = ?', list(row))
        for ticker in resolved:
            conn.execute('DELETE FROM pit_split_refusals WHERE ticker = ?', [ticker])
    db.execute_atomic(_transaction)


# -- the run ------------------------------------------------------------------

def _factor_text(factor: float) -> str:
    return f'price / {factor:g}, volume x {factor:g}'


def run(db, split_rows: Iterable[Mapping[str, Any]], client, *, run_date: dt.date,
        apply: bool = False, tickers: Optional[Sequence[str]] = None, since: Optional[dt.date] = None,
        splits_current_through: Optional[dt.date] = None, sleep: float = 0.0,
        log: Callable[[str], None] = print) -> RunResult:
    """Check (and with ``apply`` repair) the split basis of ``pit_daily_bars``.

    ``split_rows``  rows with ``ticker`` / ``execution_date`` / ``split_from`` /
                    ``split_to``; any dated after ``run_date`` is ignored.
    ``client``      ``list_aggs(ticker, 1, 'day', from, to, adjusted=..., ...)``.
    ``tickers``     only these (the basis is then not recorded).
    ``since``       only tickers with a split executed on or after this day.
    """
    result = RunResult(run_date=run_date, applied=apply)
    events_by_ticker = split_events(split_rows)

    wanted = sorted(set(tickers)) if tickers else None
    where, params = '', []
    if wanted:
        where, params = f"WHERE ticker IN ({','.join('?' * len(wanted))})", list(wanted)
    spans = {t: (first, n) for t, first, n in db.execute(
        f'SELECT ticker, min(day), count(*) FROM pit_daily_bars {where} GROUP BY ticker', params,
        fetch='all') or []}
    result.stored_tickers = len(spans)
    for missing in (wanted or []):
        if missing not in spans:
            log(f'SKIP    {missing:<7} not stored in pit_daily_bars')

    selected: Dict[str, List[SplitEvent]] = {}
    for ticker, (first_day, _) in spans.items():
        relevant = pa.relevant_events(events_by_ticker.get(ticker, []), first_day, run_date)
        if not relevant:
            if wanted:
                log(f'SKIP    {ticker:<7} no split after its first stored day ({first_day}): nothing to check')
            continue
        if since is not None and not any(day >= since for day, _ in relevant):
            continue
        selected[ticker] = relevant
    names = sorted(selected)
    scope = f', {len(names)} with a split to check' + (f' executed since {since}' if since else '')
    log(f'{"APPLY" if apply else "DRY RUN"}: run date {run_date}; {len(spans)} stored tickers{scope}')

    stored = _stored_closes(db, names)
    prior = _prior_adjustments(db)
    standing = _standing_refusals(db)
    previous_basis = recorded_basis(db)
    if apply:
        for ddl in (AUDIT_DDL, REFUSALS_DDL, BASIS_DDL):
            db.execute(ddl, fetch='none')

    for index, ticker in enumerate(names, 1):
        bars, events = stored[ticker], selected[ticker]
        unadjusted: Optional[Dict[dt.date, float]] = None
        try:
            result.api_calls += 1
            reference = daily_closes(client, ticker, bars[0][0], run_date, adjusted=True)
            plan = pa.plan_ticker(ticker, bars, events, reference)
            if plan.verdict == pa.NEEDS_UNADJUSTED:
                result.api_calls += 1
                unadjusted = daily_closes(client, ticker, bars[0][0], run_date, adjusted=False)
                plan = pa.plan_ticker(ticker, bars, events, reference, unadjusted=unadjusted)
        except Exception as exc:                                  # noqa: BLE001
            plan = pa.TickerPlan(ticker=ticker, verdict=pa.REFUSED, stored_days=len(bars), reason='fetch_failed',
                                 detail=f'the vendor\'s series could not be read ({type(exc).__name__}: {exc})',
                                 unverified_splits=tuple(d for d, _ in events))
            reference = {}
        plan = pa.refuse_repeat(plan, prior.get(ticker, []))

        if plan.verdict == pa.ADJUST and apply:
            def recheck(rows, _t=ticker, _e=events, _r=reference, _u=unadjusted):
                return pa.plan_ticker(_t, rows, _e, _r, unadjusted=_u)
            try:
                _apply_plan(db, plan, run_date, recheck)
                result.rows_changed += plan.rows_to_change
            except Exception as exc:                              # noqa: BLE001
                plan = pa.TickerPlan(ticker=ticker, verdict=pa.REFUSED, stored_days=len(bars),
                                     reason='write_failed',
                                     detail=f'nothing was written ({type(exc).__name__}: {exc})',
                                     unverified_splits=tuple(sorted({d for s in plan.segments for d, _ in s.splits})))
        result.plans.append(plan)

        for seg in plan.segments:
            lacks = ', '.join(pa.describe_split(e) for e in seg.splits)
            own = (f'  [the vendor\'s own series lacks {", ".join(pa.describe_split(e) for e in seg.vendor_lacks)}]'
                   if seg.vendor_lacks else '')
            log(f'{"ADJUSTED" if apply else "ADJUST  "} {ticker:<7} {seg.first_day} .. {seg.last_day} '
                f'{seg.rows:>5} rows  {_factor_text(seg.factor):<32} lacks {lacks}{own}')
        for note in plan.price_notes:
            log(f'NOTE     {ticker:<7} {note.day} stored close {note.stored_close:g} vs vendor '
                f'{note.reference_close * note.factor:g} ({100 * (math.exp(note.residual) - 1):+.1f}%): '
                'no split explains it; its basis follows its neighbours and the difference is left as it is')
        if plan.verdict == pa.REFUSED:
            result.refused.append(plan)
            log(f'REFUSED  {ticker:<7} [{plan.reason}] {plan.detail}')
        if sleep:
            time.sleep(sleep)
        if index % 250 == 0:
            print(f'  ... {index}/{len(names)} tickers checked', file=sys.stderr, flush=True)

    # -- the basis --------------------------------------------------------------
    new_refused: List[dt.date] = []
    for plan in result.refused:
        known = set(json.loads(standing[plan.ticker][2])) if plan.ticker in standing else set()
        new_refused.extend(d for d in plan.unverified_splits if d.isoformat() not in known)
    claim = run_date
    if splits_current_through is not None and splits_current_through < claim:
        claim = splits_current_through
    scope_gap = ''
    if wanted:
        scope_gap = 'the run was limited to named tickers'
    elif since is not None and (previous_basis is None or previous_basis < since - dt.timedelta(days=1)):
        scope_gap = (f'only splits executed since {since} were considered and '
                     + (f'the basis on record ends {previous_basis}' if previous_basis else 'no basis is on record')
                     + ': run once without --since')
    decision = pa.basis_to_record(claim, previous_basis, new_refused, scope_gap)
    result.basis, result.basis_explanation = decision.adjusted_through, decision.explanation

    refused_now = {p.ticker for p in result.refused}
    if apply:
        _record_refusals(db, result.refused, set(names), standing)
        if decision.adjusted_through is not None:
            # still refused: refused by this run, or by an earlier one and not looked at again
            excluded = sorted(refused_now | (set(standing) - set(names)))
            note = (f'pit_split_adjust: {len(names)} tickers checked, {len(result.adjusted)} rescaled '
                    f'({result.rows_changed} rows)')
            if claim < run_date:
                note += f'; split table fetched through {claim}'
            if excluded:
                note += f'; NOT on this basis (pit_split_refusals): {", ".join(excluded)}'
            if decision.adjusted_through < claim:
                note += f'; {decision.explanation}'
            db.execute('INSERT INTO pit_price_basis VALUES (?, now(), ?)',
                       [decision.adjusted_through, note], fetch='none')
            result.basis_recorded = True

    clean = sum(1 for p in result.plans if p.verdict == pa.CLEAN)
    rows = sum(p.rows_to_change for p in result.adjusted)
    log(f'{len(names)} tickers checked ({result.api_calls} vendor requests): {clean} already uniform, '
        f'{len(result.adjusted)} {"rescaled" if apply else "to rescale"} ({rows} rows), '
        f'{len(result.refused)} refused, {sum(len(p.price_notes) for p in result.plans)} price notes')
    if refused_now:
        log(f'{len(refused_now)} refused ticker(s) are left exactly as they were'
            + (f'; {len(new_refused)} of their splits had not been reported by an earlier run' if new_refused else
               '; all were already on record in pit_split_refusals'))
    if claim < run_date:
        log(f'the split table was last fetched {claim}: nothing can be said about splits after that day')
    if decision.adjusted_through is not None:
        log(f'basis: {decision.explanation} [{"recorded" if result.basis_recorded else "not recorded: dry run"}]')
    else:
        log(f'basis: {decision.explanation}'
            + (f' (on record: {previous_basis})' if previous_basis else ' (none on record)'))
        if not scope_gap and new_refused:
            log('       the refusals above ' + ('are now on record' if apply else 'would be put on record by --apply')
                + '; the NEXT --apply records the basis with those tickers named as exceptions')
    return result


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0],
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--apply', action='store_true', help='write the changes (default: dry run, writes nothing)')
    ap.add_argument('--tickers', nargs='+', metavar='TICKER', help='only these tickers')
    ap.add_argument('--since', metavar='YYYY-MM-DD',
                    help='only tickers with a split executed on or after this day (for a nightly job)')
    ap.add_argument('--run-date', metavar='YYYY-MM-DD',
                    help='the market date of this run (default: today in New York); later-dated splits are ignored')
    ap.add_argument('--db', metavar='PATH', help='history DuckDB (default: history_duckdb_path from the config)')
    ap.add_argument('--splits-json', metavar='PATH',
                    help='read split rows from this JSON file instead of the corporate_splits table')
    ap.add_argument('--sleep', type=float, default=0.0, help='seconds between vendor requests')
    ap.add_argument('--json', action='store_true', help='print one JSON document instead of text')
    args = ap.parse_args(argv)

    try:
        since = dt.date.fromisoformat(args.since) if args.since else None
        run_date = dt.date.fromisoformat(args.run_date) if args.run_date else dt.datetime.now(MARKET_TZ).date()
    except ValueError as exc:
        ap.error(str(exc))

    from massive import RESTClient
    from trader.data.duckdb_store import DuckDBConnection

    key = os.environ.get('MASSIVE_API_KEY')
    db_path = args.db
    if not key or not db_path:
        import yaml
        from trader.container import Container, default_config_path
        if not key:
            key = (yaml.safe_load(open(default_config_path())) or {}).get('massive_api_key')
        if not db_path:
            db_path = Container.instance().config().get('history_duckdb_path', '')
    if not key:
        print('no vendor key: set MASSIVE_API_KEY or massive_api_key in trader.yaml', file=sys.stderr)
        return 2
    if not db_path:
        print('no history database: pass --db or set history_duckdb_path', file=sys.stderr)
        return 2
    if args.db and not os.path.exists(os.path.expanduser(args.db)):
        print(f'{args.db}: no such database', file=sys.stderr)       # never create one by opening it
        return 2

    db = DuckDBConnection(db_path)
    if not table_exists(db, 'pit_daily_bars'):
        print(f'{db_path}: no pit_daily_bars table', file=sys.stderr)
        return 2
    if args.splits_json:
        split_rows = json.load(open(args.splits_json))
        current_through = None
    else:
        if not table_exists(db, 'corporate_splits'):
            print(f'{db_path}: no corporate_splits table (run scripts/fetch_splits.py, or pass --splits-json)',
                  file=sys.stderr)
            return 2
        split_rows = load_split_rows(db)
        current_through = splits_fetched_through(db)

    lines: List[str] = []
    result = run(db, split_rows, RESTClient(key), run_date=run_date, apply=args.apply, tickers=args.tickers,
                 since=since, splits_current_through=current_through, sleep=args.sleep,
                 log=lines.append if args.json else (lambda m: print(m, flush=True)))
    if args.json:
        print(json.dumps(result.as_dict(), indent=1))
    return result.exit_code


if __name__ == '__main__':
    raise SystemExit(main())
