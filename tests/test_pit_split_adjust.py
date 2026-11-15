"""One split basis for ``pit_daily_bars`` (trader/data/pit_adjustment.py,
scripts/pit_split_adjust.py).

The table is written with the vendor's ``adjusted=True`` as of the day each
session was FETCHED, and sessions are appended weeks apart. A split executing
between two fetches left a split-sized jump in the stored series at the
junction of the fetches (MNST 2026-07-28 -> 07-29: 97.74 -> 48.62 for a 1:2
split executed 2026-08-11), which every reader took for a -50% day.

No network: the vendor is ``FakeVendor``; every database is a temporary file.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import json
import math
import sys

from pathlib import Path
from types import SimpleNamespace
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import pytest

from trader.data import pit_adjustment as pa
from trader.data.duckdb_store import DuckDBConnection

REPO = Path(__file__).resolve().parents[1]
ET = ZoneInfo('America/New_York')
D = dt.date


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, REPO / 'scripts' / f'{name}.py')
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


psa = _load('pit_split_adjust')
bpu = _load('build_pit_universe')

SPLITS_DDL = """CREATE TABLE corporate_splits (
        ticker VARCHAR, conid VARCHAR, execution_date DATE,
        split_to DOUBLE, split_from DOUBLE, fetched_at TIMESTAMPTZ,
        PRIMARY KEY (ticker, execution_date))"""

RUN_DATE = D(2026, 10, 1)
BULK = D(2026, 7, 29)        # the day the first batch of sessions was fetched
LATER = D(2026, 9, 25)       # the day the rest was fetched
LAST_BULK_DAY = D(2026, 7, 28)


def sessions(start: dt.date, n: int) -> List[dt.date]:
    out, day = [], start
    while len(out) < n:
        if day.weekday() < 5:
            out.append(day)
        day += dt.timedelta(days=1)
    return out


DAYS = sessions(D(2026, 6, 1), 80)       # 2026-06-01 .. 2026-09-18


def _prod(values: Iterable[float]) -> float:
    out = 1.0
    for v in values:
        out *= v
    return out


class Series:
    """One ticker's true history: a value path that never moves more than a
    couple of percent a day, and the splits on it. ``jumps`` are REAL moves:
    ``{day: multiple}`` applied from that day on."""

    def __init__(self, splits: Sequence[Tuple[dt.date, float]] = (), days: Sequence[dt.date] = DAYS,
                 base: float = 100.0, jumps: Optional[Dict[dt.date, float]] = None):
        self.days = list(days)
        self.splits = sorted(splits)
        self.base = base
        self.jumps = jumps or {}

    def value(self, day: dt.date) -> float:
        i = self.days.index(day)
        v = self.base * (1 + 0.002 * ((i * 7) % 11 - 5))
        return v * _prod(m for d, m in self.jumps.items() if day >= d)

    def raw(self, day: dt.date) -> float:
        """What it traded at: a 1:2 split halves it on the execution date."""
        return self.value(day) / _prod(r for d, r in self.splits if d <= day)

    def _later(self, day, as_of, applied):
        return _prod(r for d, r in self.splits
                     if day < d <= as_of and (applied is None or (d, r) in applied))

    def adjusted(self, day: dt.date, as_of: dt.date, applied=None) -> float:
        """The vendor's adjusted close for ``day`` as served on ``as_of``:
        divided by every split executed after ``day`` and by ``as_of`` (of
        those the endpoint applies at all)."""
        return self.raw(day) / self._later(day, as_of, applied)

    def volume(self, day: dt.date, as_of: dt.date, applied=None) -> float:
        raw = 1_000_000.0 * _prod(r for d, r in self.splits if d <= day)
        return raw * self._later(day, as_of, applied)


def two_fetches(day: dt.date) -> dt.date:
    """Sessions through 2026-07-28 fetched on 07-29, the rest on 09-25."""
    return BULK if day <= LAST_BULK_DAY else LATER


class FakeVendor:
    def __init__(self):
        self.adjusted: Dict[str, Dict[dt.date, float]] = {}
        self.unadjusted: Dict[str, Dict[dt.date, float]] = {}
        self.calls: List[Tuple[str, bool]] = []
        self.fail: Dict[str, Exception] = {}
        self.on_call = None

    def serve(self, ticker: str, series: Series, as_of: dt.date = RUN_DATE, applied=None,
              days: Optional[Sequence[dt.date]] = None):
        days = series.days if days is None else days
        self.adjusted[ticker] = {d: series.adjusted(d, as_of, applied) for d in days}
        self.unadjusted[ticker] = {d: series.raw(d) for d in days}

    def list_aggs(self, ticker, multiplier, timespan, from_, to, adjusted=True, sort='asc', limit=50000):
        assert (multiplier, timespan, sort) == (1, 'day', 'asc')
        self.calls.append((ticker, bool(adjusted)))
        if self.on_call:
            self.on_call(ticker)
        if ticker in self.fail:
            raise self.fail[ticker]
        data = (self.adjusted if adjusted else self.unadjusted).get(ticker, {})
        for day in sorted(data):
            if from_ <= day.isoformat() <= to:
                stamp = int(dt.datetime(day.year, day.month, day.day, tzinfo=ET).timestamp() * 1000)
                yield SimpleNamespace(timestamp=stamp, close=data[day], open=data[day], high=data[day],
                                      low=data[day], volume=1.0)


@pytest.fixture
def db(tmp_path):
    conn = DuckDBConnection(str(tmp_path / 'history.duckdb'))
    conn.execute(bpu.PIT_TABLE_DDL, fetch='none')
    conn.execute(SPLITS_DDL, fetch='none')
    return conn


def store(db, ticker: str, series: Series, fetched=two_fetches, applied=None,
          days: Optional[Sequence[dt.date]] = None, rank: int = 100):
    """Write the ticker as ``build_pit_universe`` would have: every session
    adjusted as of the day IT was fetched."""
    rows = []
    for day in (series.days if days is None else days):
        close = series.adjusted(day, fetched(day), applied)
        volume = series.volume(day, fetched(day), applied)
        rows.append((day, ticker, close * 0.99, close * 1.01, close * 0.98, close, volume,
                     close * volume, rank + series.days.index(day) % 7))
    db.execute_atomic(lambda conn: conn.executemany(
        'INSERT INTO pit_daily_bars VALUES (?,?,?,?,?,?,?,?,?)', rows))


def add_splits(db, ticker: str, splits: Sequence[Tuple[dt.date, float, float]],
               fetched_at: str = '2026-10-01 08:00:00-04:00'):
    """``(execution date, split_from, split_to)`` into corporate_splits - whose
    columns are split_to THEN split_from."""
    for day, split_from, split_to in splits:
        db.execute('INSERT INTO corporate_splits VALUES (?, NULL, ?, ?, ?, CAST(? AS TIMESTAMPTZ))',
                   [ticker, day, split_to, split_from, fetched_at], fetch='none')


def table(db, name: str = 'pit_daily_bars', order: str = 'ALL') -> list:
    """Every row of ``name`` (``[]`` if it does not exist), in a stable order."""
    if not psa.table_exists(db, name):
        return []
    return db.execute(f'SELECT * FROM {name} ORDER BY {order}', fetch='all')


def bars(db, ticker: str) -> Dict[dt.date, tuple]:
    return {r[0]: r[1:] for r in db.execute(
        'SELECT day, open, high, low, close, volume, dollar_volume, dv_rank FROM pit_daily_bars '
        'WHERE ticker = ? ORDER BY day', [ticker], fetch='all')}


def largest_step(db, ticker: str) -> float:
    """The largest close-to-close move in the stored series, in logs."""
    closes = [v[3] for v in bars(db, ticker).values()]
    return max(abs(math.log(b / a)) for a, b in zip(closes, closes[1:]))


def run(db, vendor, **kwargs):
    lines: List[str] = []
    kwargs.setdefault('run_date', RUN_DATE)
    kwargs.setdefault('splits_current_through', psa.splits_fetched_through(db))
    result = psa.run(db, psa.load_split_rows(db), vendor, log=lines.append, **kwargs)
    return result, lines


def closes(series: Series, fetched=two_fetches, applied=None, days=None) -> List[Tuple[dt.date, float]]:
    return [(d, series.adjusted(d, fetched(d), applied)) for d in (series.days if days is None else days)]


def reference(series: Series, applied=None, days=None, as_of: dt.date = RUN_DATE) -> Dict[dt.date, float]:
    return {d: series.adjusted(d, as_of, applied) for d in (series.days if days is None else days)}


FORWARD = (D(2026, 8, 11), 2.0)          # 1:2, executed after the first fetch
REVERSE = (D(2026, 8, 24), 0.1)          # 10:1 reverse


# =============================================================================
# the pure rule
# =============================================================================

class TestJunction:
    def test_the_stale_days_end_at_the_fetch_junction_not_at_the_execution_date(self):
        """Sessions 07-29 .. 08-10 precede the split but were fetched after it:
        they are already adjusted, and dividing them again would halve them."""
        series = Series([FORWARD])
        plan = pa.plan_ticker('MNST', closes(series), [FORWARD], reference(series))
        assert plan.verdict == pa.ADJUST
        [segment] = plan.segments
        assert (segment.first_day, segment.last_day) == (DAYS[0], LAST_BULK_DAY)
        assert segment.rows == sum(1 for d in DAYS if d <= LAST_BULK_DAY)
        assert segment.factor == 2.0 and segment.splits == (FORWARD,) and segment.vendor_lacks == ()
        assert segment.last_day < FORWARD[0] - dt.timedelta(days=10), 'the junction is not the execution date'

    def test_a_reverse_split_is_a_factor_below_one(self):
        """MSTU 10:1 reverse: the stale closes are a tenth of the adjusted ones,
        so dividing by 0.1 multiplies them by ten."""
        series = Series([REVERSE], base=2.0)
        plan = pa.plan_ticker('MSTU', closes(series), [REVERSE], reference(series))
        [segment] = plan.segments
        assert segment.factor == pytest.approx(0.1) and segment.last_day == LAST_BULK_DAY
        assert segment.first_close / segment.factor == pytest.approx(reference(series)[DAYS[0]])

    def test_two_splits_with_two_junctions_give_two_factors(self):
        first, second = (D(2026, 7, 1), 2.0), (D(2026, 9, 1), 3.0)
        series = Series([first, second])

        def fetched(day):                      # three batches: before both, between, after both
            return D(2026, 6, 20) if day <= D(2026, 6, 19) else (D(2026, 8, 15) if day <= D(2026, 8, 14)
                                                                 else LATER)
        plan = pa.plan_ticker('TWO', closes(series, fetched), [first, second], reference(series))
        assert [(s.first_day, s.last_day, s.factor, s.splits) for s in plan.segments] == [
            (DAYS[0], D(2026, 6, 19), 6.0, (first, second)),          # lacks both
            (D(2026, 6, 22), D(2026, 8, 14), 3.0, (second,)),         # lacks only the later one
        ]

    def test_a_series_fetched_in_one_go_is_clean(self):
        series = Series([FORWARD])
        plan = pa.plan_ticker('OK', closes(series, lambda d: LATER), [FORWARD], reference(series))
        assert plan.verdict == pa.CLEAN and plan.segments == () and plan.price_notes == ()

    def test_a_split_the_bulk_endpoint_never_applied_ends_on_its_execution_date(self):
        """CGNX 2017-12-04: fetched long after the split and still unadjusted
        before it, while the vendor's per-ticker series carries it."""
        split = (D(2026, 7, 6), 2.0)
        series = Series([split])
        stored = closes(series, lambda d: LATER, applied=())
        plan = pa.plan_ticker('CGNX', stored, [split], reference(series))
        [segment] = plan.segments
        assert segment.last_day == D(2026, 7, 3) and segment.factor == 2.0 and segment.vendor_lacks == ()
        assert segment.last_day == max(d for d in DAYS if d < split[0]), 'the session before the execution date'


class TestOnlyRecentSplitsCanBeMissing:
    """A day fetched at F lacks the splits executed after F - a suffix of the
    list. Any other combination is not a history that can have happened."""

    def test_an_older_split_missing_while_a_later_one_is_applied_is_refused(self):
        older, later = (D(2026, 7, 6), 0.05), (D(2026, 8, 3), 0.25)
        series = Series([older, later])
        stored = closes(series, lambda d: LATER, applied=(later,))        # WLDS: lacks only the older one
        plan = pa.plan_ticker('WLDS', stored, [older, later], reference(series))
        assert plan.verdict == pa.REFUSED and plan.reason == 'unexplained_quotient'
        assert 'lack 2026-07-06 20:1 reverse while carrying a later split' in plan.detail
        assert plan.unverified_splits == (older[0], later[0])

    def test_a_quotient_only_an_arbitrary_subset_produces_is_refused(self):
        """WHLR: 16 later splits give 65,536 products; 1/72 is 39 of them and
        none is 'the last m'. Accepting subsets would explain any wrong price."""
        splits = [(D(2026, 7, 1) + dt.timedelta(days=7 * i), 1 / r) for i, r in enumerate((2, 3, 4, 9, 6))]
        series = Series(splits)
        stored = [(d, c / 72 if d < splits[0][0] else c) for d, c in closes(series, lambda d: LATER)]
        ref = reference(series)
        assert math.isclose(1 / 72, (1 / 2) * (1 / 4) * (1 / 9)), 'a subset (2, 4, 9) does produce it'
        plan = pa.plan_ticker('WHLR', stored, splits, ref)
        assert plan.verdict == pa.REFUSED and plan.reason == 'unexplained_quotient' and plan.segments == ()

    def test_prices_that_carry_a_split_the_vendor_withdrew_are_refused_and_named(self):
        """XRX: stored 2/3 of the vendor's series on every day, for a 1:1.5
        'split' the vendor no longer lists and its series shows no trace of."""
        phantom = (D(2026, 9, 28), 1.5)
        series = Series([])
        stored = [(d, c / 1.5) for d, c in closes(series)]
        plan = pa.plan_ticker('XRX', stored, [phantom], reference(series))
        assert plan.verdict == pa.REFUSED and plan.reason == 'unexplained_quotient'
        assert 'carry 2026-09-28 1:1.5, which the vendor\'s adjusted series does not' in plan.detail
        assert f'{len(DAYS)} of {len(DAYS)} stored days' in plan.detail

    def test_a_discrepancy_no_split_produces_is_refused(self):
        series = Series([FORWARD])
        stored = [(d, c * 1.37 if d <= LAST_BULK_DAY else c) for d, c in closes(series, lambda d: LATER)]
        plan = pa.plan_ticker('ODD', stored, [FORWARD], reference(series))
        assert plan.verdict == pa.REFUSED and plan.reason == 'unexplained_quotient' and plan.segments == ()
        assert plan.unverified_splits == (FORWARD[0],)

    def test_a_split_and_its_reversal_do_not_ask_for_a_rescale(self):
        """SLG: 1:1.03060254 on 2021-12-14 and 1.0306:1 back on 2022-01-24.
        Their product is 1.0000025, not 1, so 'lacks both' is a candidate a
        hair from 'lacks nothing' - and was the nearer one on 18 days, each of
        which would have been divided by 1.0000025 and logged as a repair."""
        pair = [(D(2026, 7, 6), 1.03060254), (D(2026, 8, 3), 1 / 1.0306)]
        both = pair[0][1] * pair[1][1]
        assert both != 1.0 and abs(math.log(both)) < 1e-5
        series = Series(pair)
        stored = [(d, c * both if d < pair[0][0] else c) for d, c in closes(series, lambda d: LATER)]
        plan = pa.plan_ticker('SLG', stored, pair, reference(series))
        assert plan.verdict == pa.CLEAN and plan.segments == () and plan.price_notes == ()

    @pytest.mark.parametrize('off, explained', [(0.0019, True), (0.0021, False)])
    def test_the_tolerance_is_two_tenths_of_a_percent_in_logs(self, off, explained):
        """Largest disagreement measured on the same bar: 0.00096. A run of
        days further out than twice that is not this split."""
        series = Series([FORWARD])
        stored = [(d, c * math.exp(off) if d <= LAST_BULK_DAY else c) for d, c in closes(series)]
        plan = pa.plan_ticker('TOL', stored, [FORWARD], reference(series))
        assert pa.TOLERANCE == 0.002
        assert (plan.verdict == pa.ADJUST) is explained
        assert (plan.verdict == pa.REFUSED) is not explained


class TestNoReference:
    def test_no_reference_at_all_is_refused_and_every_split_is_unverified(self):
        series = Series([FORWARD])
        plan = pa.plan_ticker('GONE', closes(series), [FORWARD], {})
        assert plan.verdict == pa.REFUSED and plan.reason == 'no_reference'
        assert plan.unverified_splits == (FORWARD[0],) and plan.segments == ()

    def test_a_stored_day_the_reference_has_no_bar_for_is_refused(self):
        series = Series([FORWARD])
        ref = reference(series, days=[d for d in DAYS if d != D(2026, 6, 10)])
        plan = pa.plan_ticker('GAP', closes(series), [FORWARD], ref)
        assert plan.verdict == pa.REFUSED and plan.reason == 'reference_gap'
        assert '1 of 80 stored days' in plan.detail and '2026-06-10' in plan.detail

    def test_a_stored_close_that_is_not_a_price_is_refused(self):
        series = Series([FORWARD])
        stored = [(d, float('nan') if d == DAYS[3] else c) for d, c in closes(series)]
        plan = pa.plan_ticker('NAN', stored, [FORWARD], reference(series))
        assert plan.verdict == pa.REFUSED and plan.reason == 'bad_stored_close'


class TestTheVendorsOwnSeries:
    SPLIT = (D(2026, 8, 11), 2.0)

    def test_a_reference_that_jumps_on_the_execution_date_needs_the_unadjusted_series(self):
        series = Series([self.SPLIT])
        ref = reference(series, applied=())                    # the vendor's series is unadjusted too
        plan = pa.plan_ticker('MISS', closes(series, applied=()), [self.SPLIT], ref)
        assert plan.verdict == pa.NEEDS_UNADJUSTED and plan.needs_unadjusted == (self.SPLIT,)

    def test_a_split_neither_series_carries_is_applied_to_the_days_before_it(self):
        """The raw jump is on the execution date in the reference too, and the
        unadjusted series shows the adjusted one does not apply the split."""
        series = Series([self.SPLIT])
        ref = reference(series, applied=())
        raw = {d: series.raw(d) for d in DAYS}
        plan = pa.plan_ticker('MISS', closes(series, applied=()), [self.SPLIT], ref, unadjusted=raw)
        [segment] = plan.segments
        assert segment.last_day == D(2026, 8, 10) and segment.factor == 2.0
        assert segment.vendor_lacks == (self.SPLIT,)
        # and once applied, the same comparison finds nothing left to do
        fixed = [(d, c / 2.0 if d < self.SPLIT[0] else c) for d, c in closes(series, applied=())]
        assert pa.plan_ticker('MISS', fixed, [self.SPLIT], ref, unadjusted=raw).verdict == pa.CLEAN

    def test_stored_prices_that_carry_a_split_the_vendor_dropped_from_its_series_are_right(self):
        """PSTV: fetched while the vendor applied the 25:1; its series no
        longer does and now jumps 23x on the date. Nothing to change."""
        split = (D(2026, 8, 11), 0.04)
        series = Series([split], base=4.0)
        ref = reference(series, applied=())
        raw = {d: series.raw(d) for d in DAYS}
        plan = pa.plan_ticker('PSTV', closes(series, lambda d: LATER), [split], ref, unadjusted=raw)
        assert plan.verdict == pa.CLEAN and plan.segments == ()

    def test_a_real_move_the_size_of_the_split_on_its_execution_date_is_not_a_missing_split(self):
        """SMX / GCTK: the vendor's series applies the split AND the stock
        doubled that day. The jump test alone calls that 'not carried'; a
        second application would follow on every later run."""
        split = (D(2026, 8, 11), 0.5)
        series = Series([split], jumps={split[0]: 2.0})
        ref = reference(series)
        from trader.data.share_basis import split_reflected_in_prices
        assert split_reflected_in_prices(ref[D(2026, 8, 10)], ref[split[0]], 0.5) is False
        stored = closes(series, lambda d: LATER)
        assert pa.plan_ticker('SMX', stored, [split], ref).verdict == pa.NEEDS_UNADJUSTED
        plan = pa.plan_ticker('SMX', stored, [split], ref, unadjusted={d: series.raw(d) for d in DAYS})
        assert plan.verdict == pa.CLEAN and plan.segments == ()

    def test_an_unadjusted_series_without_the_two_bars_cannot_decide(self):
        series = Series([self.SPLIT])
        ref = reference(series, applied=())
        raw = {d: series.raw(d) for d in DAYS if d != self.SPLIT[0]}
        plan = pa.plan_ticker('MISS', closes(series, applied=()), [self.SPLIT], ref, unadjusted=raw)
        assert plan.verdict == pa.REFUSED and plan.reason == 'undecidable_split'

    def test_an_unadjusted_step_that_is_neither_is_refused(self):
        series = Series([self.SPLIT])
        ref = reference(series, applied=())
        raw = {d: series.raw(d) * (1.3 if d < self.SPLIT[0] else 1.0) for d in DAYS}
        plan = pa.plan_ticker('MISS', closes(series, applied=()), [self.SPLIT], ref, unadjusted=raw)
        assert plan.verdict == pa.REFUSED and plan.reason == 'undecidable_split' and 'neither 1' in plan.detail

    def test_a_split_after_the_references_last_bar_is_tried_both_ways(self):
        """NLTX: 5:1 reverse never applied by the bulk endpoint, then a 4:1 the
        day after the ticker's last session, which nothing applies."""
        missed, trailing = (D(2026, 7, 6), 0.2), (DAYS[-1] + dt.timedelta(days=3), 0.25)
        series = Series([missed])
        stored = closes(series, lambda d: LATER, applied=(), days=DAYS[:10])
        plan = pa.plan_ticker('NLTX', stored, [missed, trailing], reference(series))
        [segment] = plan.segments
        assert segment.factor == pytest.approx(0.2) and segment.splits == (missed,)

    def test_two_readings_of_a_trailing_split_that_disagree_are_refused(self):
        """Read as applied, the early days lack the trailing 4:1 (x0.25); read
        as not applied, they lack the earlier split (x0.2505). Both fit a
        quotient of 0.25025 and they are different divisions."""
        earlier, trailing = (D(2026, 7, 6), 0.2505), (DAYS[-1] + dt.timedelta(days=3), 0.25)
        series = Series([])
        stored = [(d, c * 0.25025 if d < earlier[0] else c) for d, c in closes(series)]
        plan = pa.plan_ticker('BOTH', stored, [earlier, trailing], reference(series))
        assert plan.verdict == pa.REFUSED and plan.reason == 'undecidable_split'
        assert 'executed after the last bar' in plan.detail and plan.segments == ()


class TestOneWrongDay:
    """GOOGL / SMCI / SOXS 2023-05-30: one stored close that is simply not the
    vendor's. It must not block the repair of a real stale segment, and it
    must not be 'repaired' itself."""

    def test_an_isolated_wrong_close_keeps_its_neighbours_basis_and_is_reported(self):
        series = Series([FORWARD])
        off_stale, off_clean = D(2026, 6, 16), D(2026, 9, 2)
        stored = [(d, c * 0.93 if d in (off_stale, off_clean) else c) for d, c in closes(series)]
        plan = pa.plan_ticker('SOXS', stored, [FORWARD], reference(series))
        assert plan.verdict == pa.ADJUST
        [segment] = plan.segments
        assert segment.rows == sum(1 for d in DAYS if d <= LAST_BULK_DAY), 'the wrong day moves with its segment'
        assert [(n.day, n.factor) for n in plan.price_notes] == [(off_stale, 2.0), (off_clean, 1.0)]
        assert all(n.residual == pytest.approx(math.log(0.93)) for n in plan.price_notes)

    def test_two_wrong_days_in_a_row_are_not_isolated(self):
        series = Series([FORWARD])
        wrong = (D(2026, 9, 2), D(2026, 9, 3))
        stored = [(d, c * 0.93 if d in wrong else c) for d, c in closes(series, lambda d: LATER)]
        plan = pa.plan_ticker('RUN', stored, [FORWARD], reference(series))
        assert plan.verdict == pa.REFUSED and plan.reason == 'unexplained_quotient'
        assert plan.unverified_splits == (), 'both days are after the only split: no split is in doubt'
        # with a stale segment as well, the refusal leaves that segment unrepaired: its split IS in doubt
        stale = [(d, c * 0.93 if d in wrong else c) for d, c in closes(series)]
        plan = pa.plan_ticker('RUN', stale, [FORWARD], reference(series))
        assert plan.verdict == pa.REFUSED and plan.segments == () and plan.unverified_splits == (FORWARD[0],)

    def test_a_day_off_by_more_than_ten_percent_is_not_a_price_note(self):
        series = Series([FORWARD])
        stored = [(d, c * 0.85 if d == D(2026, 9, 2) else c) for d, c in closes(series)]
        assert pa.plan_ticker('BIG', stored, [FORWARD], reference(series)).verdict == pa.REFUSED

    def test_a_wrong_day_that_a_small_split_could_explain_is_refused(self):
        """With a 1:1.1 split still to come, a day 3% off is 0.066 from that
        split's basis: too near to call."""
        small = (D(2026, 9, 10), 1.1)
        series = Series([small])
        stored = [(d, c * 1.03 if d == D(2026, 6, 16) else c) for d, c in closes(series, lambda d: LATER)]
        assert pa.plan_ticker('NEAR', stored, [small], reference(series)).verdict == pa.REFUSED


class TestRelevantEvents:
    def test_an_announced_split_and_one_before_the_first_stored_day_are_left_out(self):
        events = [(D(2026, 5, 29), 2.0), (D(2026, 6, 1), 2.0), (D(2026, 8, 11), 2.0),
                  (RUN_DATE, 3.0), (RUN_DATE + dt.timedelta(days=1), 4.0)]
        assert pa.relevant_events(events, D(2026, 6, 1), RUN_DATE) == [(D(2026, 8, 11), 2.0), (RUN_DATE, 3.0)]


class TestRepeatGuard:
    SEGMENT = pa.Segment(D(2026, 6, 1), D(2026, 7, 28), 42, 2.0, (FORWARD,), (), 100.0)

    def test_a_split_already_applied_to_those_days_is_not_applied_again(self):
        prior = [pa.AppliedAdjustment(D(2026, 6, 1), D(2026, 7, 28), (FORWARD,), '2026-10-01')]
        plan = pa.TickerPlan('MNST', pa.ADJUST, 80, segments=(self.SEGMENT,))
        refused = pa.refuse_repeat(plan, prior)
        assert refused.verdict == pa.REFUSED and refused.reason == 'already_applied' and refused.segments == ()
        assert 'would compound' in refused.detail and refused.unverified_splits == (FORWARD[0],)

    @pytest.mark.parametrize('prior', [
        [],
        [pa.AppliedAdjustment(D(2026, 6, 1), D(2026, 7, 28), ((D(2025, 3, 3), 2.0),))],     # another split
        [pa.AppliedAdjustment(D(2026, 7, 29), D(2026, 9, 18), (FORWARD,))],                 # other days
    ])
    def test_a_different_split_or_different_days_is_not_a_repeat(self, prior):
        plan = pa.TickerPlan('MNST', pa.ADJUST, 80, segments=(self.SEGMENT,))
        assert pa.refuse_repeat(plan, prior) is plan


class TestBasisToRecord:
    CLAIM = D(2026, 11, 14)

    def test_a_run_with_nothing_refused_records_the_claim(self):
        assert pa.basis_to_record(self.CLAIM, None, []).adjusted_through == self.CLAIM
        assert pa.basis_to_record(self.CLAIM, D(2026, 11, 13), []).adjusted_through == self.CLAIM

    def test_the_basis_never_moves_backwards_or_repeats(self):
        assert pa.basis_to_record(self.CLAIM, self.CLAIM, []).adjusted_through is None
        assert pa.basis_to_record(self.CLAIM, D(2026, 11, 20), []).adjusted_through is None

    def test_a_first_refusal_holds_the_basis_at_the_day_before_its_split(self):
        decision = pa.basis_to_record(self.CLAIM, D(2026, 11, 1), [D(2026, 11, 12), D(2026, 11, 10)])
        assert decision.adjusted_through == D(2026, 11, 9) and '2026-11-10' in decision.explanation

    def test_a_first_refusal_older_than_the_basis_on_record_records_nothing(self):
        decision = pa.basis_to_record(self.CLAIM, D(2026, 11, 1), [D(2018, 7, 3)])
        assert decision.adjusted_through is None and 'not advanced past 2026-11-01' in decision.explanation

    def test_a_held_back_value_is_never_the_first_basis(self):
        """SDRL's 2018 refusal on the very first run: recording 2018-07-02
        would tell every reader that eight years of splits are in no price."""
        decision = pa.basis_to_record(self.CLAIM, None, [D(2018, 7, 3)])
        assert decision.adjusted_through is None and 'no basis on record' in decision.explanation

    def test_an_incomplete_run_records_nothing(self):
        decision = pa.basis_to_record(self.CLAIM, None, [], scope_gap='the run was limited to named tickers')
        assert decision.adjusted_through is None and 'named tickers' in decision.explanation


# =============================================================================
# the script, on a temporary database
# =============================================================================

def setup_junction(db, vendor, ticker='MNST', split=FORWARD, base=100.0):
    series = Series([split], base=base)
    store(db, ticker, series)
    add_splits(db, ticker, [(split[0], 1.0, split[1])] if split[1] >= 1 else [(split[0], 1 / split[1], 1.0)])
    vendor.serve(ticker, series)
    return series


class TestApply:
    def test_the_junction_jump_is_there_before_and_gone_after(self, db):
        vendor = FakeVendor()
        series = setup_junction(db, vendor)
        before = bars(db, 'MNST')
        assert largest_step(db, 'MNST') == pytest.approx(math.log(2), abs=0.03), 'the defect: a fake -50% day'
        assert before[D(2026, 7, 29)][3] / before[LAST_BULK_DAY][3] == pytest.approx(0.5, abs=0.02)

        result, lines = run(db, vendor, apply=True)

        assert largest_step(db, 'MNST') < 0.03, 'the fake -50% day at the fetch junction is still stored'
        assert result.exit_code == 0 and result.rows_changed == 42 and not result.refused
        after = bars(db, 'MNST')
        for day in DAYS:
            assert after[day][3] == pytest.approx(series.adjusted(day, RUN_DATE), rel=1e-12)
        assert any(line.startswith('ADJUSTED MNST') and '2026-06-01 .. 2026-07-28' in line
                   and 'price / 2, volume x 2' in line and 'lacks 2026-08-11 1:2' in line for line in lines)

    def test_days_fetched_after_the_split_are_not_touched(self, db):
        """07-29 .. 08-10 are before the execution date and already adjusted:
        a repair keyed on the execution date would halve them."""
        vendor = FakeVendor()
        setup_junction(db, vendor)
        before = bars(db, 'MNST')
        run(db, vendor, apply=True)
        after = bars(db, 'MNST')
        untouched = [d for d in DAYS if d > LAST_BULK_DAY]
        assert any(d < FORWARD[0] for d in untouched)
        assert all(after[d] == before[d] for d in untouched)

    def test_prices_are_divided_volume_multiplied_and_the_split_invariant_columns_kept(self, db):
        vendor = FakeVendor()
        setup_junction(db, vendor)
        before = bars(db, 'MNST')
        run(db, vendor, apply=True)
        after = bars(db, 'MNST')
        for day in [d for d in DAYS if d <= LAST_BULK_DAY]:
            o, h, l, c, v, dollar_volume, rank = before[day]
            assert after[day][:4] == (o / 2.0, h / 2.0, l / 2.0, c / 2.0)
            assert after[day][4] == v * 2.0
            assert after[day][5] == dollar_volume and after[day][6] == rank, 'dollar_volume / dv_rank moved'
            assert after[day][3] * after[day][4] == pytest.approx(dollar_volume, rel=1e-12)

    def test_a_reverse_split_multiplies_the_stale_prices(self, db):
        vendor = FakeVendor()
        series = setup_junction(db, vendor, ticker='MSTU', split=REVERSE, base=2.0)
        before = bars(db, 'MSTU')
        assert largest_step(db, 'MSTU') == pytest.approx(math.log(10), abs=0.03)
        result, _ = run(db, vendor, apply=True)
        after = bars(db, 'MSTU')
        assert largest_step(db, 'MSTU') < 0.03, 'the fake 10x day at the fetch junction is still stored'
        assert result.exit_code == 0
        assert after[DAYS[0]][3] == pytest.approx(before[DAYS[0]][3] * 10)
        assert after[DAYS[0]][4] == pytest.approx(before[DAYS[0]][4] / 10)
        assert all(after[d][3] == pytest.approx(series.adjusted(d, RUN_DATE)) for d in DAYS)

    def test_two_splits_with_two_junctions(self, db):
        first, second = (D(2026, 7, 1), 2.0), (D(2026, 9, 1), 3.0)
        series = Series([first, second])

        def fetched(day):
            return D(2026, 6, 20) if day <= D(2026, 6, 19) else (D(2026, 8, 15) if day <= D(2026, 8, 14)
                                                                 else LATER)
        store(db, 'TWO', series, fetched)
        add_splits(db, 'TWO', [(first[0], 1, 2), (second[0], 1, 3)])
        vendor = FakeVendor()
        vendor.serve('TWO', series)
        closes_before = [v[3] for v in bars(db, 'TWO').values()]
        steps = sorted(abs(math.log(b / a)) for a, b in zip(closes_before, closes_before[1:]))[-2:]
        assert steps == pytest.approx([math.log(2), math.log(3)], abs=0.03), 'two fake jumps'

        result, _ = run(db, vendor, apply=True)

        assert largest_step(db, 'TWO') < 0.03, 'a fake jump is still stored at one of the two junctions'
        assert result.exit_code == 0
        audit = db.execute('SELECT first_day, last_day, factor, splits FROM pit_split_adjustments ORDER BY first_day',
                           fetch='all')
        assert [(a, b, f) for a, b, f, _ in audit] == [(DAYS[0], D(2026, 6, 19), 6.0),
                                                      (D(2026, 6, 22), D(2026, 8, 14), 3.0)]
        assert json.loads(audit[0][3]) == [['2026-07-01', 2.0], ['2026-09-01', 3.0]]

    def test_a_split_the_bulk_endpoint_missed_is_repaired_on_its_execution_date(self, db):
        split = (D(2026, 7, 6), 0.25)                         # WLL / ESV 4:1 reverse
        series = Series([split], base=7.0)
        store(db, 'WLL', series, lambda d: LATER, applied=())
        add_splits(db, 'WLL', [(split[0], 4, 1)])
        vendor = FakeVendor()
        vendor.serve('WLL', series)
        raw_jump = bars(db, 'WLL')[split[0]][3] / bars(db, 'WLL')[D(2026, 7, 3)][3]
        assert raw_jump == pytest.approx(4.0, rel=0.03), 'the raw jump sits on the execution date'
        result, _ = run(db, vendor, apply=True)
        assert largest_step(db, 'WLL') < 0.03, 'the raw 4x jump is still stored on the execution date'
        assert result.exit_code == 0
        assert vendor.calls == [('WLL', True)], 'the adjusted series carries the split: one request'

    def test_a_split_the_vendors_own_series_lacks_is_applied_and_then_left_alone(self, db):
        split = (D(2026, 8, 11), 2.0)
        series = Series([split])
        store(db, 'MISS', series, applied=())
        add_splits(db, 'MISS', [(split[0], 1, 2)])
        vendor = FakeVendor()
        vendor.serve('MISS', series, applied=())              # the reference has the raw jump too
        assert largest_step(db, 'MISS') == pytest.approx(math.log(2), abs=0.03)

        result, lines = run(db, vendor, apply=True)

        assert largest_step(db, 'MISS') < 0.03, 'the raw jump is still stored (the vendor\'s series has it too)'
        assert result.exit_code == 0
        assert vendor.calls == [('MISS', True), ('MISS', False)], 'the unadjusted series is asked for only here'
        assert any('the vendor\'s own series lacks 2026-08-11 1:2' in line for line in lines)
        [(lacks,)] = db.execute('SELECT vendor_lacks FROM pit_split_adjustments', fetch='all')
        assert json.loads(lacks) == [['2026-08-11', 2.0]]
        # the reference still jumps; the stored series must not be divided again
        snapshot = table(db)
        again, _ = run(db, vendor, apply=True)
        assert again.exit_code == 0 and again.rows_changed == 0 and table(db) == snapshot

    def test_a_real_doubling_on_the_execution_date_is_not_divided_away(self, db):
        split = (D(2026, 8, 11), 0.5)
        series = Series([split], jumps={split[0]: 2.0})
        store(db, 'SMX', series, lambda d: LATER)
        add_splits(db, 'SMX', [(split[0], 2, 1)])
        vendor = FakeVendor()
        vendor.serve('SMX', series)
        snapshot = table(db)
        result, _ = run(db, vendor, apply=True)
        assert result.exit_code == 0 and result.rows_changed == 0 and table(db) == snapshot
        assert largest_step(db, 'SMX') == pytest.approx(math.log(2), abs=0.03), 'the move is real and stays'

    def test_one_wrong_close_does_not_block_the_repair_and_is_not_repaired(self, db):
        vendor = FakeVendor()
        series = setup_junction(db, vendor, ticker='SOXS')
        off = D(2026, 6, 16)
        db.execute('UPDATE pit_daily_bars SET close = close * 0.93 WHERE ticker = ? AND day = ?', ['SOXS', off],
                   fetch='none')
        wrong = bars(db, 'SOXS')[off][3]
        result, lines = run(db, vendor, apply=True)
        assert result.exit_code == 0 and result.rows_changed == 42
        after = bars(db, 'SOXS')
        assert after[off][3] == wrong / 2.0, 'rescaled with its segment'
        assert after[off][3] / series.adjusted(off, RUN_DATE) == pytest.approx(0.93), 'and still 7% off'
        assert any(line.startswith('NOTE') and '2026-06-16' in line and '(-7.0%)' in line for line in lines)


class TestRefusals:
    def test_a_ticker_with_no_reference_data_is_refused_and_left_alone(self, db):
        series = Series([FORWARD])
        store(db, 'GONE', series)
        add_splits(db, 'GONE', [(FORWARD[0], 1, 2)])
        snapshot = table(db)
        result, lines = run(db, FakeVendor(), apply=True)
        assert result.exit_code == 1 and [p.reason for p in result.refused] == ['no_reference']
        assert table(db) == snapshot and table(db, 'pit_split_adjustments') == []
        assert any(line.startswith('REFUSED  GONE') and '[no_reference]' in line for line in lines)
        [(ticker, reason, _, unverified, _)] = table(db, 'pit_split_refusals', 'ticker')
        assert (ticker, reason, json.loads(unverified)) == ('GONE', 'no_reference', ['2026-08-11'])

    def test_a_vendor_error_is_a_refusal_not_a_crash_and_the_rest_still_runs(self, db):
        vendor = FakeVendor()
        setup_junction(db, vendor, ticker='AAA')
        setup_junction(db, vendor, ticker='BBB')
        vendor.fail['AAA'] = RuntimeError('503')
        before = bars(db, 'AAA')
        result, _ = run(db, vendor, apply=True)
        assert result.exit_code == 1 and [(p.ticker, p.reason) for p in result.refused] == [('AAA', 'fetch_failed')]
        assert bars(db, 'AAA') == before and largest_step(db, 'BBB') < 0.03

    def test_a_discrepancy_no_split_explains_is_refused_and_nothing_is_changed(self, db):
        """Rescaling to the reference would 'fix' it. It is not a split."""
        series = Series([FORWARD])
        store(db, 'ODD', series, lambda d: LATER)
        db.execute('UPDATE pit_daily_bars SET close = close * 1.37 WHERE day <= ?', [LAST_BULK_DAY], fetch='none')
        add_splits(db, 'ODD', [(FORWARD[0], 1, 2)])
        vendor = FakeVendor()
        vendor.serve('ODD', series)
        snapshot = table(db)
        result, lines = run(db, vendor, apply=True)
        assert result.exit_code == 1 and [p.reason for p in result.refused] == ['unexplained_quotient']
        assert table(db) == snapshot and table(db, 'pit_split_adjustments') == []
        assert any('REFUSED  ODD' in line and '42 of 80 stored days' in line and '1.37' in line for line in lines)

    def test_a_refused_ticker_does_not_stop_the_others(self, db):
        vendor = FakeVendor()
        setup_junction(db, vendor, ticker='MNST')
        store(db, 'GONE', Series([FORWARD]))
        add_splits(db, 'GONE', [(FORWARD[0], 1, 2)])
        gone = bars(db, 'GONE')
        result, _ = run(db, vendor, apply=True)
        assert result.exit_code == 1 and [p.ticker for p in result.adjusted] == ['MNST']
        assert largest_step(db, 'MNST') < 0.03 and bars(db, 'GONE') == gone

    def test_rows_that_changed_since_the_plan_are_not_written_over(self, db):
        vendor = FakeVendor()
        setup_junction(db, vendor)

        def someone_else_writes(ticker):
            db.execute('UPDATE pit_daily_bars SET close = close + 1 WHERE ticker = ? AND day = ?',
                       [ticker, DAYS[0]], fetch='none')
        vendor.on_call = someone_else_writes
        result, _ = run(db, vendor, apply=True)
        vendor.on_call = None
        assert result.exit_code == 1 and [p.reason for p in result.refused] == ['write_failed']
        assert table(db, 'pit_split_adjustments') == []
        assert largest_step(db, 'MNST') == pytest.approx(math.log(2), abs=0.03), 'nothing was rescaled'

    def test_a_refusal_that_no_longer_holds_is_taken_off_the_record(self, db):
        series = Series([FORWARD])
        store(db, 'LATE', series)
        add_splits(db, 'LATE', [(FORWARD[0], 1, 2)])
        vendor = FakeVendor()
        first, _ = run(db, vendor, apply=True)
        assert first.exit_code == 1 and len(table(db, 'pit_split_refusals', 'ticker')) == 1
        vendor.serve('LATE', series)                          # the vendor has the ticker after all
        second, _ = run(db, vendor, apply=True)
        assert second.exit_code == 0 and table(db, 'pit_split_refusals', 'ticker') == []
        assert largest_step(db, 'LATE') < 0.03


class TestDryRunAndIdempotence:
    def test_the_default_is_a_dry_run_that_writes_nothing(self, db):
        vendor = FakeVendor()
        setup_junction(db, vendor)
        snapshot = table(db)
        result, lines = run(db, vendor)
        assert result.applied is False and result.rows_changed == 0 and table(db) == snapshot
        assert not any(psa.table_exists(db, t) for t in
                       ('pit_split_adjustments', 'pit_split_refusals', 'pit_price_basis'))
        assert any(line.startswith('ADJUST   MNST') and '42 rows' in line for line in lines)
        assert lines[0].startswith('DRY RUN')
        assert any('not recorded: dry run' in line for line in lines)

    def test_a_second_apply_changes_nothing(self, db):
        vendor = FakeVendor()
        setup_junction(db, vendor)
        setup_junction(db, vendor, ticker='MSTU', split=REVERSE, base=2.0)
        store(db, 'GONE', Series([FORWARD]))
        add_splits(db, 'GONE', [(FORWARD[0], 1, 2)])
        first, _ = run(db, vendor, apply=True)
        assert first.rows_changed == 84
        names = ('pit_daily_bars', 'pit_split_adjustments', 'pit_split_refusals', 'pit_price_basis')
        orders = ('ticker, day', 'ticker, first_day', 'ticker', 'adjusted_through')
        second, _ = run(db, vendor, apply=True)                    # records the basis (the refusal is now known)
        snapshot = [table(db, n, o) for n, o in zip(names, orders)]
        third, lines = run(db, vendor, apply=True)
        assert second.rows_changed == 0 and third.rows_changed == 0
        assert [p.verdict for p in third.plans] == [pa.REFUSED, pa.CLEAN, pa.CLEAN]
        assert [table(db, n, o) for n, o in zip(names, orders)] == snapshot, 'a re-run wrote something'
        assert len(snapshot[1]) == 2 and len(snapshot[3]) == 1

    def test_the_audit_row_says_what_was_done(self, db):
        vendor = FakeVendor()
        setup_junction(db, vendor)
        first_close = bars(db, 'MNST')[DAYS[0]][3]
        run(db, vendor, apply=True)
        [row] = table(db, 'pit_split_adjustments', 'ticker')
        applied_at, ticker, first, last, n_rows, factor, splits, lacks, basis, before, after = row
        assert (ticker, first, last, n_rows, factor) == ('MNST', DAYS[0], LAST_BULK_DAY, 42, 2.0)
        assert json.loads(splits) == [['2026-08-11', 2.0]] and json.loads(lacks) == []
        assert basis == RUN_DATE and (before, after) == (first_close, first_close / 2.0)
        assert applied_at is not None

    def test_a_split_the_audit_log_says_was_applied_is_not_applied_twice(self, db):
        """If the comparison ever asks for the same split on the same days
        again - here the vendor's series halves a second time with no new
        split - the answer is a refusal, not 4x."""
        vendor = FakeVendor()
        series = setup_junction(db, vendor)
        run(db, vendor, apply=True)
        snapshot = table(db)
        vendor.adjusted['MNST'] = {d: c / 2.0 if d <= LAST_BULK_DAY else c
                                   for d, c in vendor.adjusted['MNST'].items()}
        result, lines = run(db, vendor, apply=True)
        assert result.exit_code == 1 and [p.reason for p in result.refused] == ['already_applied']
        assert table(db) == snapshot and len(table(db, 'pit_split_adjustments', 'ticker')) == 1
        assert bars(db, 'MNST')[DAYS[0]][3] == pytest.approx(series.adjusted(DAYS[0], RUN_DATE))


class TestScope:
    def test_an_announced_split_is_not_looked_at(self, db):
        vendor = FakeVendor()
        series = Series([])
        store(db, 'SOON', series, lambda d: LATER)
        add_splits(db, 'SOON', [(RUN_DATE + dt.timedelta(days=5), 1, 2)])
        vendor.serve('SOON', series)
        result, _ = run(db, vendor, apply=True)
        assert result.plans == [] and vendor.calls == [] and result.exit_code == 0

    def test_only_stored_tickers_with_a_split_cost_a_request(self, db):
        vendor = FakeVendor()
        setup_junction(db, vendor, ticker='MNST')
        store(db, 'PLAIN', Series([]), lambda d: LATER)                # stored, never split
        add_splits(db, 'ELSEWHERE', [(FORWARD[0], 1, 2)])              # split, not stored
        add_splits(db, 'OLD', [(D(2020, 1, 2), 1, 2)])
        store(db, 'OLD', Series([]), lambda d: LATER)                  # split before its first stored day
        run(db, vendor)
        assert vendor.calls == [('MNST', True)]

    def test_since_selects_tickers_by_their_newest_split_and_checks_all_of_it(self, db):
        vendor = FakeVendor()
        setup_junction(db, vendor, ticker='RECENT')
        old = (D(2026, 6, 15), 2.0)
        series = Series([old])
        store(db, 'EARLIER', series, lambda d: D(2026, 6, 10) if d <= D(2026, 6, 9) else LATER)
        add_splits(db, 'EARLIER', [(old[0], 1, 2)])
        vendor.serve('EARLIER', series)
        result, _ = run(db, vendor, apply=True, since=D(2026, 8, 1))
        assert [p.ticker for p in result.plans] == ['RECENT'] and vendor.calls == [('RECENT', True)]
        assert largest_step(db, 'RECENT') < 0.03
        assert largest_step(db, 'EARLIER') == pytest.approx(math.log(2), abs=0.03), 'out of scope: untouched'

    def test_named_tickers_only(self, db):
        vendor = FakeVendor()
        setup_junction(db, vendor, ticker='AAA')
        setup_junction(db, vendor, ticker='BBB')
        result, lines = run(db, vendor, apply=True, tickers=['BBB', 'NOPE'])
        assert [p.ticker for p in result.plans] == ['BBB'] and vendor.calls == [('BBB', True)]
        assert largest_step(db, 'AAA') == pytest.approx(math.log(2), abs=0.03)
        assert any(line.startswith('SKIP') and 'NOPE' in line for line in lines)

    def test_split_rows_are_read_by_column_name(self, db):
        """corporate_splits stores split_to BEFORE split_from; read positionally
        a 1:2 split becomes a 2:1 reverse and the factor inverts."""
        add_splits(db, 'MNST', [(FORWARD[0], 1.0, 2.0)])
        [(positional_fourth,)] = db.execute('SELECT split_to FROM corporate_splits', fetch='all')
        assert positional_fourth == 2.0
        [row] = psa.load_split_rows(db)
        assert (row['split_from'], row['split_to']) == (1.0, 2.0)


class TestBasis:
    def basis(self, db):
        return [(r[0], r[2]) for r in table(db, 'pit_price_basis', 'adjusted_through')]

    def test_a_complete_apply_with_nothing_refused_records_the_run_date(self, db):
        vendor = FakeVendor()
        setup_junction(db, vendor)
        result, _ = run(db, vendor, apply=True)
        assert result.basis == RUN_DATE and result.basis_recorded
        [(through, note)] = self.basis(db)
        assert through == RUN_DATE and '1 rescaled (42 rows)' in note and 'NOT on this basis' not in note
        assert psa.recorded_basis(db) == RUN_DATE

    def test_the_basis_is_no_later_than_the_split_table_is_current_to(self, db):
        vendor = FakeVendor()
        series = Series([FORWARD])
        store(db, 'MNST', series)
        add_splits(db, 'MNST', [(FORWARD[0], 1, 2)], fetched_at='2026-09-28 23:40:00-04:00')
        vendor.serve('MNST', series)
        assert psa.splits_fetched_through(db) == D(2026, 9, 28)
        result, lines = run(db, vendor, apply=True)
        assert result.basis == D(2026, 9, 28)
        assert any('last fetched 2026-09-28' in line for line in lines)

    def test_the_run_that_first_refuses_records_no_basis_and_the_next_one_does(self, db):
        vendor = FakeVendor()
        setup_junction(db, vendor)
        store(db, 'GONE', Series([FORWARD]))
        add_splits(db, 'GONE', [(FORWARD[0], 1, 2)])
        first, lines = run(db, vendor, apply=True)
        assert first.exit_code == 1 and first.basis is None and self.basis(db) == []
        assert any('refused for the first time' in line for line in lines)
        second, lines = run(db, vendor, apply=True)
        assert second.refused and second.unreported == []
        assert second.exit_code == 0, 'a refusal already on record is a known exception, not a failed run'
        assert any('all were already on record in pit_split_refusals' in line for line in lines)
        [(through, note)] = self.basis(db)
        assert through == RUN_DATE and 'NOT on this basis (pit_split_refusals): GONE' in note

    def test_accepting_the_refusals_records_the_basis_in_the_same_run(self, db):
        """The operator has read the dry run; one apply instead of two."""
        vendor = FakeVendor()
        setup_junction(db, vendor)
        store(db, 'GONE', Series([FORWARD]))
        add_splits(db, 'GONE', [(FORWARD[0], 1, 2)])
        dry, _ = run(db, vendor, accept_refusals=True)
        assert dry.basis == RUN_DATE and not dry.basis_recorded and self.basis(db) == []
        result, _ = run(db, vendor, apply=True, accept_refusals=True)
        assert result.exit_code == 1, 'accepted is not the same as not refused'
        [(through, note)] = self.basis(db)
        assert through == RUN_DATE and 'NOT on this basis (pit_split_refusals): GONE' in note
        assert '--accept-refusals' in note
        assert [r[0] for r in table(db, 'pit_split_refusals')] == ['GONE']

    def test_a_new_refusal_holds_an_existing_basis_at_the_day_before_its_split(self, db):
        vendor = FakeVendor()
        setup_junction(db, vendor)
        run(db, vendor, apply=True, run_date=D(2026, 8, 1), splits_current_through=None)
        assert psa.recorded_basis(db) == D(2026, 8, 1)
        store(db, 'GONE', Series([(D(2026, 9, 1), 2.0)]))
        add_splits(db, 'GONE', [(D(2026, 9, 1), 1, 2)])
        result, _ = run(db, vendor, apply=True)
        assert result.exit_code == 1 and result.basis == D(2026, 8, 31)
        assert psa.recorded_basis(db) == D(2026, 8, 31)

    def test_named_tickers_never_record_a_basis(self, db):
        vendor = FakeVendor()
        setup_junction(db, vendor)
        result, lines = run(db, vendor, apply=True, tickers=['MNST'])
        assert result.rows_changed == 42 and result.basis is None and self.basis(db) == []
        assert any('limited to named tickers' in line for line in lines)

    def test_since_records_a_basis_only_when_the_one_on_record_reaches_it(self, db):
        vendor = FakeVendor()
        setup_junction(db, vendor)
        gap, lines = run(db, vendor, apply=True, since=D(2026, 8, 1))
        assert gap.rows_changed == 42 and gap.basis is None and self.basis(db) == []
        assert any('run once without --since' in line for line in lines)
        db.execute('INSERT INTO pit_price_basis VALUES (?, now(), ?)', [D(2026, 7, 31), 'earlier full run'],
                   fetch='none')
        reached, _ = run(db, vendor, apply=True, since=D(2026, 8, 1))
        assert reached.basis == RUN_DATE and psa.recorded_basis(db) == RUN_DATE


class TestCommandLine:
    @pytest.fixture
    def cli(self, db, monkeypatch, tmp_path):
        vendor = FakeVendor()
        monkeypatch.setenv('MASSIVE_API_KEY', 'not-a-key')
        import massive
        monkeypatch.setattr(massive, 'RESTClient', lambda key: vendor)
        return SimpleNamespace(db=db, vendor=vendor, path=db.db_path, tmp=tmp_path)

    def test_dry_run_then_apply_then_nothing(self, cli, capsys):
        setup_junction(cli.db, cli.vendor)
        argv = ['--db', cli.path, '--run-date', RUN_DATE.isoformat(), '--json']
        assert psa.main(argv) == 0
        plan = json.loads(capsys.readouterr().out)
        assert plan['applied'] is False and plan['adjust'][0]['ticker'] == 'MNST'
        assert plan['adjust'][0]['segments'][0] == {
            'first_day': '2026-06-01', 'last_day': '2026-07-28', 'rows': 42, 'factor': 2.0,
            'splits': [['2026-08-11', 2.0]], 'vendor_lacks': []}
        assert largest_step(cli.db, 'MNST') > 0.6, 'a dry run'
        assert psa.main(argv + ['--apply']) == 0
        assert json.loads(capsys.readouterr().out)['rows_changed'] == 42
        assert largest_step(cli.db, 'MNST') < 0.03
        assert psa.main(argv + ['--apply']) == 0
        again = json.loads(capsys.readouterr().out)
        assert again['rows_changed'] == 0 and again['adjust'] == []

    def test_a_refusal_is_a_non_zero_exit(self, cli, capsys):
        store(cli.db, 'GONE', Series([FORWARD]))
        add_splits(cli.db, 'GONE', [(FORWARD[0], 1, 2)])
        argv = ['--db', cli.path, '--run-date', RUN_DATE.isoformat()]
        assert psa.main(argv) == 1
        assert 'REFUSED  GONE' in capsys.readouterr().out
        assert psa.main(argv + ['--apply']) == 1 and psa.recorded_basis(cli.db) is None

    def test_accept_refusals_is_wired_through(self, cli, capsys):
        store(cli.db, 'GONE', Series([FORWARD]))
        add_splits(cli.db, 'GONE', [(FORWARD[0], 1, 2)])
        argv = ['--db', cli.path, '--run-date', RUN_DATE.isoformat(), '--apply', '--accept-refusals']
        assert psa.main(argv) == 1 and psa.recorded_basis(cli.db) == RUN_DATE

    def test_split_rows_can_come_from_a_json_file(self, cli, capsys):
        series = Series([FORWARD])
        store(cli.db, 'MNST', series)
        cli.vendor.serve('MNST', series)
        path = cli.tmp / 'splits.json'
        path.write_text(json.dumps([{'ticker': 'MNST', 'execution_date': '2026-08-11',
                                     'split_from': 1, 'split_to': 2}]))
        assert psa.main(['--db', cli.path, '--run-date', RUN_DATE.isoformat(), '--splits-json', str(path),
                         '--apply']) == 0
        assert largest_step(cli.db, 'MNST') < 0.03

    def test_a_database_path_that_does_not_exist_is_not_created(self, cli, capsys):
        missing = cli.tmp / 'nope.duckdb'
        assert psa.main(['--db', str(missing)]) == 2
        assert not missing.exists() and 'no such database' in capsys.readouterr().err

    def test_without_a_split_table_it_says_so(self, cli, capsys):
        cli.db.execute('DROP TABLE corporate_splits', fetch='none')
        assert psa.main(['--db', cli.path]) == 2
        assert 'no corporate_splits table' in capsys.readouterr().err


# ---------------------------------------------------------------------------
# A nightly job: which refusals fail the run, and which splits a run looks at
# ---------------------------------------------------------------------------

class TestWhatFailsARun:
    """Ten tickers are refused in the live table and always will be (SDRL: a
    2018 restructuring that is not a split). A job that exits 1 on them every
    night is a job nobody reads; one that exits 0 on a NEW refusal is worse."""

    def _refused(self, db, ticker='GONE', split=FORWARD):
        store(db, ticker, Series([split]))
        add_splits(db, ticker, [(split[0], 1, split[1])])

    def test_a_first_refusal_fails_the_run_and_names_the_ticker(self, db):
        vendor = FakeVendor()
        self._refused(db)
        result, lines = run(db, vendor, apply=True, accept_refusals=True)
        assert result.unreported == ['GONE'] and result.exit_code == 1
        assert any('NOT already on record (this is why the exit status is 1): GONE' in line for line in lines)

    def test_the_same_refusal_the_next_night_does_not(self, db):
        vendor = FakeVendor()
        self._refused(db)
        run(db, vendor, apply=True, accept_refusals=True)
        again, lines = run(db, vendor, apply=True, accept_refusals=True)
        assert [p.ticker for p in again.refused] == ['GONE'], 'still refused, still printed'
        assert again.unreported == [] and again.exit_code == 0
        assert any(line.startswith('REFUSED  GONE') for line in lines)

    def test_a_dry_run_does_not_put_a_refusal_on_record(self, db):
        vendor = FakeVendor()
        self._refused(db)
        for _ in range(2):
            result, _ = run(db, vendor)
            assert result.exit_code == 1 and result.unreported == ['GONE']

    def test_a_new_split_on_a_ticker_already_refused_fails_the_run_again(self, db):
        vendor = FakeVendor()
        store(db, 'GONE', Series([FORWARD, (D(2026, 9, 15), 3.0)]))
        add_splits(db, 'GONE', [(FORWARD[0], 1, 2)])
        run(db, vendor, apply=True, accept_refusals=True)
        add_splits(db, 'GONE', [(D(2026, 9, 15), 1, 3)])
        result, _ = run(db, vendor, apply=True, accept_refusals=True)
        assert result.unreported == ['GONE'] and result.exit_code == 1

    def test_a_vendor_failure_is_never_already_known(self, db):
        """It is put on record like any refusal - and reported every night it
        happens, because tomorrow's request may succeed."""
        vendor = FakeVendor()
        setup_junction(db, vendor)
        vendor.fail['MNST'] = RuntimeError('503')
        for _ in range(2):
            result, _ = run(db, vendor, apply=True, accept_refusals=True)
            assert [p.reason for p in result.refused] == ['fetch_failed']
            assert result.unreported == ['MNST'] and result.exit_code == 1
        del vendor.fail['MNST']
        fixed, _ = run(db, vendor, apply=True, accept_refusals=True)
        assert fixed.exit_code == 0 and fixed.rows_changed == 42 and table(db, 'pit_split_refusals') == []

    def test_one_new_refusal_among_standing_ones_is_the_one_named(self, db):
        vendor = FakeVendor()
        self._refused(db, 'OLD1')
        self._refused(db, 'OLD2', REVERSE)
        run(db, vendor, apply=True, accept_refusals=True)
        self._refused(db, 'NEW')
        result, _ = run(db, vendor, apply=True, accept_refusals=True)
        assert sorted(p.ticker for p in result.refused) == ['NEW', 'OLD1', 'OLD2']
        assert result.unreported == ['NEW'] and result.exit_code == 1

    def test_the_json_document_carries_them(self, db):
        vendor = FakeVendor()
        self._refused(db)
        result, _ = run(db, vendor)
        document = result.as_dict()
        assert document['unreported_refusals'] == ['GONE'] and document['exit_code'] == 1


class TestTheDefaultWindowFollowsTheBasis:
    def test_no_basis_on_record_is_everything(self):
        assert psa.default_since(None) is None

    def test_with_a_basis_it_reaches_back_past_it(self):
        assert psa.OVERLAP_DAYS == 45
        assert psa.default_since(D(2026, 10, 1)) == D(2026, 8, 17)

    @pytest.fixture
    def cli(self, db, monkeypatch, tmp_path):
        vendor = FakeVendor()
        monkeypatch.setenv('MASSIVE_API_KEY', 'not-a-key')
        import massive
        monkeypatch.setattr(massive, 'RESTClient', lambda key: vendor)
        return SimpleNamespace(db=db, vendor=vendor, path=db.db_path, tmp=tmp_path)

    def _old_and_new(self, cli):
        """OLD split executed 2026-06-15 (long before any basis), NEW after it."""
        old = Series([(D(2026, 6, 15), 2.0)])
        store(cli.db, 'OLD', old, fetched=lambda day: RUN_DATE)            # fetched in one go: clean
        add_splits(cli.db, 'OLD', [(D(2026, 6, 15), 1, 2)])
        cli.vendor.serve('OLD', old)
        setup_junction(cli.db, cli.vendor, 'NEW', (D(2026, 9, 20), 2.0))

    def test_the_first_run_looks_at_every_split(self, cli, capsys):
        self._old_and_new(cli)
        assert psa.main(['--db', cli.path, '--run-date', RUN_DATE.isoformat(), '--apply']) == 0
        assert sorted({t for t, _ in cli.vendor.calls}) == ['NEW', 'OLD']
        assert psa.recorded_basis(cli.db) == RUN_DATE

    def test_a_later_run_looks_only_at_splits_since_before_the_basis(self, cli, capsys):
        self._old_and_new(cli)
        argv = ['--db', cli.path, '--apply']
        psa.main(argv + ['--run-date', RUN_DATE.isoformat()])
        cli.vendor.calls.clear()
        capsys.readouterr()
        # the nightly order: the split table is refreshed first (fetch_splits.py) ...
        cli.db.execute("UPDATE corporate_splits SET fetched_at = CAST('2026-10-02 08:00:00-04:00' AS TIMESTAMPTZ)",
                       fetch='none')
        assert psa.main(argv + ['--run-date', '2026-10-02']) == 0
        assert sorted({t for t, _ in cli.vendor.calls}) == ['NEW']          # 09-20 is inside 45 days; 06-15 is not
        out = capsys.readouterr().out
        assert 'looking at splits executed since 2026-08-17 (45 days before the basis on record)' in out
        assert psa.recorded_basis(cli.db) == D(2026, 10, 2), 'the window reaches the basis, so it advances'

    def test_the_basis_does_not_pass_the_day_the_split_table_was_last_fetched(self, cli, capsys):
        """Nothing can be said about splits after that day, whatever the window."""
        self._old_and_new(cli)
        argv = ['--db', cli.path, '--apply']
        psa.main(argv + ['--run-date', RUN_DATE.isoformat()])
        assert psa.main(argv + ['--run-date', '2026-10-09']) == 0
        assert psa.recorded_basis(cli.db) == RUN_DATE
        assert 'the split table was last fetched 2026-10-01' in capsys.readouterr().out

    def test_however_long_the_job_was_down_the_window_reaches_the_basis(self, cli, capsys):
        """A fixed "last N days" would leave the splits in between unexamined
        and the basis stuck. The window starts from the basis itself."""
        self._old_and_new(cli)
        argv = ['--db', cli.path, '--apply']
        psa.main(argv + ['--run-date', RUN_DATE.isoformat()])
        late = Series([(D(2026, 12, 1), 2.0)])
        store(cli.db, 'LATE', late, fetched=lambda day: D(2026, 11, 1))     # fetched before its split
        add_splits(cli.db, 'LATE', [(D(2026, 12, 1), 1, 2)], fetched_at='2027-05-01 08:00:00-04:00')
        cli.vendor.serve('LATE', late, as_of=D(2027, 5, 1))
        cli.vendor.calls.clear()
        assert psa.main(argv + ['--run-date', '2027-05-01']) == 0
        assert 'LATE' in {t for t, _ in cli.vendor.calls}
        assert largest_step(cli.db, 'LATE') < 0.03
        assert psa.recorded_basis(cli.db) == D(2027, 5, 1)

    def test_full_looks_at_everything_whatever_is_on_record(self, cli, capsys):
        self._old_and_new(cli)
        argv = ['--db', cli.path, '--apply', '--run-date']
        psa.main(argv + [RUN_DATE.isoformat()])
        cli.vendor.calls.clear()
        assert psa.main(argv + ['2026-10-02', '--full']) == 0
        assert sorted({t for t, _ in cli.vendor.calls}) == ['NEW', 'OLD']

    @pytest.mark.parametrize('extra', [['--since', '2026-09-01'], ['--tickers', 'NEW']])
    def test_full_cannot_be_narrowed(self, cli, capsys, extra):
        with pytest.raises(SystemExit) as stop:
            psa.main(['--db', cli.path, '--full'] + extra)
        assert stop.value.code == 2

    def test_an_explicit_since_is_used_as_given(self, cli, capsys):
        self._old_and_new(cli)
        psa.main(['--db', cli.path, '--apply', '--run-date', RUN_DATE.isoformat()])
        cli.vendor.calls.clear()
        psa.main(['--db', cli.path, '--run-date', '2026-10-02', '--since', '2026-06-01'])
        assert sorted({t for t, _ in cli.vendor.calls}) == ['NEW', 'OLD']
