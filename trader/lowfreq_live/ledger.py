"""Paper ledger: a sleeve's daily value against its virtual decisive benchmark.

Both sides are valued from the SAME stored bars (``pit_daily_bars``, the bars
the research path reads), and both are REPLAYED from inception on every
``track`` call from durable inputs only - the sleeve's recorded fills, the
sleeve's D08 target files (whose ``.benchmark.json`` sidecars carry the
benchmark weights ``targets`` computed on the identical panel and coverage),
and the bars. The ledger table is the OUTPUT: one row per session, appended
once and never rewritten, so a later data correction shows up as a
disagreement the operator can see rather than a silently changed history.

Benchmark rules (D05 decisive benchmark, D01 execution): the virtual book
fills its new weights at the OPEN of the first stored session after each
decision session (t+1 open, as ``Backtester.run_panel`` does), pays the
family's base costs (5 bps slippage + $0.005/share for equities), holds
fractional shares (a rule, not orders), and is marked at every session's
close; a holding without a print that day carries its last close forward and
is counted in ``benchmark_n_unpriced``. Cash earns zero (D03).
"""
from __future__ import annotations

import datetime as dt
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Protocol, Tuple, cast

import numpy as np
import pandas as pd

from trader.data.duckdb_store import DuckDBConnection
from trader.lowfreq_live.targets import (BENCHMARK_SCHEMA, benchmark_sidecar_path, list_sleeve_targets,
                                         read_targets)
from trader.research.lowfreq.costs import Costs, base_costs

FILLS_DDL = """CREATE TABLE IF NOT EXISTS lowfreq_sleeve_fills (
    id INTEGER PRIMARY KEY, sleeve VARCHAR NOT NULL, fill_date DATE NOT NULL,
    ticker VARCHAR NOT NULL, side VARCHAR NOT NULL, quantity DOUBLE NOT NULL,
    price DOUBLE NOT NULL, commission DOUBLE NOT NULL, source VARCHAR,
    recorded_at TIMESTAMP NOT NULL)"""
FILLS_SEQ = 'CREATE SEQUENCE IF NOT EXISTS lowfreq_sleeve_fills_id_seq START 1'
LEDGER_DDL = """CREATE TABLE IF NOT EXISTS lowfreq_paper_ledger (
    sleeve VARCHAR NOT NULL, session DATE NOT NULL,
    holdings_value DOUBLE, cash DOUBLE, sleeve_value DOUBLE, n_holdings INTEGER, n_unpriced INTEGER,
    benchmark_id VARCHAR, benchmark_value DOUBLE, benchmark_cash DOUBLE,
    benchmark_n_holdings INTEGER, benchmark_n_unpriced INTEGER,
    capital_usd DOUBLE, recorded_at TIMESTAMP NOT NULL,
    PRIMARY KEY (sleeve, session))"""


class BarSource(Protocol):
    """Session bars for a set of tickers over an inclusive date window:
    ``(opens, closes)`` frames indexed by session (tz-naive midnight) with one
    column per ticker (NaN where a name had no print)."""
    def bars(self, tickers: Iterable[str], start: dt.date, end: dt.date) -> Tuple[pd.DataFrame, pd.DataFrame]: ...
    def sessions(self, start: dt.date, end: dt.date) -> pd.DatetimeIndex: ...
    def last_session(self) -> Optional[dt.date]: ...


class DuckDBBars:
    """``pit_daily_bars`` in the history DB, opened read-only (the bars the
    research path trades on; exchange test tickers and absurd prices blanked
    with the loader's rule)."""
    def __init__(self, history_db_path: str):
        self.path = str(Path(history_db_path).expanduser())

    def _con(self):
        import duckdb
        return duckdb.connect(self.path, read_only=True)

    def bars(self, tickers, start, end):
        names = sorted(set(tickers))
        if not names:
            idx = self.sessions(start, end)
            return pd.DataFrame(index=idx), pd.DataFrame(index=idx)
        con = self._con()
        try:
            df = con.execute('SELECT day, ticker, open, close FROM pit_daily_bars WHERE day >= ? AND day <= ? '
                             'AND ticker IN (' + ','.join('?' * len(names)) + ')', [start, end, *names]).fetchdf()
        finally:
            con.close()
        df['day'] = pd.to_datetime(df['day'])
        bad = ~((df['close'] > 0.01) & (df['close'] < 100_000))
        df.loc[bad, ['open', 'close']] = np.nan
        idx = self.sessions(start, end)
        opens = df.pivot_table(index='day', columns='ticker', values='open', aggfunc='last')
        closes = df.pivot_table(index='day', columns='ticker', values='close', aggfunc='last')
        return (opens.reindex(index=idx, columns=names), closes.reindex(index=idx, columns=names))

    def sessions(self, start, end):
        con = self._con()
        try:
            rows = con.execute('SELECT DISTINCT day FROM pit_daily_bars WHERE day >= ? AND day <= ? ORDER BY day',
                               [start, end]).fetchall()
        finally:
            con.close()
        return pd.DatetimeIndex([pd.Timestamp(r[0]) for r in rows])

    def last_session(self):
        con = self._con()
        try:
            r = con.execute('SELECT max(day) FROM pit_daily_bars').fetchone()
        finally:
            con.close()
        return r[0] if r and r[0] is not None else None


class FrameBars:
    """A ``BarSource`` over in-memory frames (tests)."""
    def __init__(self, opens: pd.DataFrame, closes: pd.DataFrame):
        self.opens, self.closes = opens.sort_index(), closes.sort_index()

    def bars(self, tickers, start, end):
        idx = self.sessions(start, end)
        names = sorted(set(tickers))
        return (self.opens.reindex(index=idx, columns=names), self.closes.reindex(index=idx, columns=names))

    def sessions(self, start, end):
        idx = pd.DatetimeIndex(self.closes.index)
        return idx[(idx >= pd.Timestamp(start)) & (idx <= pd.Timestamp(end))]

    def last_session(self):
        return pd.Timestamp(self.closes.index[-1]).date() if len(self.closes.index) else None


@dataclass(frozen=True)
class Fill:
    fill_date: dt.date
    ticker: str
    side: str            # BUY | SELL
    quantity: float
    price: float
    commission: float = 0.0
    source: str = 'operator'


class LedgerStore:
    """The fill journal and the ledger table in one DuckDB file."""
    def __init__(self, db_path: str):
        self.db = DuckDBConnection(str(Path(db_path).expanduser()))
        self.db.execute(FILLS_SEQ, fetch='none')
        self.db.execute(FILLS_DDL, fetch='none')
        self.db.execute(LEDGER_DDL, fetch='none')

    def record_fill(self, sleeve: str, fill: Fill) -> None:
        if fill.side not in ('BUY', 'SELL'):
            raise ValueError(f'side must be BUY or SELL, got {fill.side!r}')
        if not (fill.quantity > 0 and math.isfinite(fill.quantity)):
            raise ValueError(f'quantity must be positive and finite, got {fill.quantity!r}')
        if not (fill.price > 0 and math.isfinite(fill.price)):
            raise ValueError(f'price must be positive and finite, got {fill.price!r}')
        if fill.commission < 0 or not math.isfinite(fill.commission):
            raise ValueError(f'commission must be >= 0, got {fill.commission!r}')
        self.db.execute(
            "INSERT INTO lowfreq_sleeve_fills VALUES (nextval('lowfreq_sleeve_fills_id_seq'), ?,?,?,?,?,?,?,?, now())",
            [sleeve, fill.fill_date, fill.ticker.upper(), fill.side, float(fill.quantity), float(fill.price),
             float(fill.commission), fill.source], fetch='none')

    def fills(self, sleeve: str) -> List[Fill]:
        rows = self.db.execute('SELECT fill_date, ticker, side, quantity, price, commission, source FROM '
                               'lowfreq_sleeve_fills WHERE sleeve = ? ORDER BY fill_date, id', [sleeve], fetch='all') or []
        return [Fill(r[0], r[1], r[2], float(r[3]), float(r[4]), float(r[5]), r[6] or '') for r in rows]

    def ledger(self, sleeve: str) -> pd.DataFrame:
        df = self.db.execute('SELECT * FROM lowfreq_paper_ledger WHERE sleeve = ? ORDER BY session', [sleeve], fetch='df')
        if df is None or len(df) == 0:
            return pd.DataFrame()
        df['session'] = pd.to_datetime(df['session'])
        return df.set_index('session')

    def append_rows(self, rows: List[Dict[str, Any]]) -> int:
        """Insert rows for sessions not yet in the table; existing rows are
        never touched (append-only ledger)."""
        n = 0
        for r in rows:
            exists = self.db.execute('SELECT 1 FROM lowfreq_paper_ledger WHERE sleeve = ? AND session = ?',
                                     [r['sleeve'], r['session']], fetch='one')
            if exists:
                continue
            self.db.execute(
                'INSERT INTO lowfreq_paper_ledger VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?, now())',
                [r['sleeve'], r['session'], r['holdings_value'], r['cash'], r['sleeve_value'], r['n_holdings'],
                 r['n_unpriced'], r['benchmark_id'], r['benchmark_value'], r['benchmark_cash'],
                 r['benchmark_n_holdings'], r['benchmark_n_unpriced'], r['capital_usd']], fetch='none')
            n += 1
        return n


# ---------------------------------------------------------------- replay ---

@dataclass
class SleeveInputs:
    sleeve: str
    capital_usd: float                        # inception capital: the FIRST D08 file's capital_usd
    inception: dt.date                        # the first decision session
    benchmark_id: str
    benchmark_weights: Dict[dt.date, Dict[str, float]]   # decision session -> weights
    fills: List[Fill]


def load_inputs(sleeve: str, store: LedgerStore, targets_dir: Optional[str] = None,
                benchmark_id: Optional[str] = None) -> SleeveInputs:
    paths = list_sleeve_targets(sleeve, targets_dir)
    if not paths:
        raise RuntimeError(f'no targets files for sleeve {sleeve!r} under {targets_dir or "the default targets dir"}; '
                           f'run `targets` first')
    first = read_targets(paths[0])
    bench: Dict[dt.date, Dict[str, float]] = {}
    bid = benchmark_id
    for p in paths:
        side = benchmark_sidecar_path(p)
        if not side.exists():
            raise RuntimeError(f'{p} has no benchmark sidecar {side.name}; regenerate targets')
        doc = json.loads(side.read_text())
        if doc.get('schema') != BENCHMARK_SCHEMA:
            raise RuntimeError(f'{side}: unexpected schema {doc.get("schema")!r}')
        benches = doc.get('benchmarks') or {}
        if bid is None:
            if len(benches) != 1:
                raise RuntimeError(f'{side} carries {len(benches)} benchmarks; pass --benchmark <id>')
            bid = next(iter(benches))
        if bid not in benches:
            raise RuntimeError(f'{side} has no benchmark {bid!r}')
        if benches[bid] is None:
            continue                                    # the benchmark held on that session: no rebalance
        benchmarks_w = {str(k): float(v) for k, v in benches[bid].items() if float(v) > 0}
        bench[dt.date.fromisoformat(doc['as_of'])] = benchmarks_w
    return SleeveInputs(sleeve=sleeve, capital_usd=float(first['capital_usd']),
                        inception=dt.date.fromisoformat(first['as_of']), benchmark_id=str(bid),
                        benchmark_weights=bench, fills=store.fills(sleeve))


def _mark(qty: Dict[str, float], closes_row: pd.Series, last: Dict[str, float]) -> Tuple[float, int, int]:
    value, n, unpriced = 0.0, 0, 0
    for t, q in qty.items():
        if abs(q) < 1e-12:
            continue
        px = closes_row.get(t, np.nan) if len(closes_row) else np.nan
        if px is not None and np.isfinite(px) and px > 0:
            last[t] = float(px)
        else:
            unpriced += 1
        p = last.get(t)
        if p is not None:
            value += q * p
        n += 1
    return value, n, unpriced


def replay(inputs: SleeveInputs, bars: BarSource, through: dt.date, costs: Optional[Costs] = None) -> pd.DataFrame:
    """One row per session in (inception, through]: sleeve holdings x closes +
    cash from the fill journal; the virtual benchmark filled at t+1 open with
    costs and marked at close."""
    costs = costs or base_costs('equity')
    names = set(f.ticker for f in inputs.fills)
    for w in inputs.benchmark_weights.values():
        names |= set(w)
    start = inputs.inception
    opens, closes = bars.bars(names, start, through)
    sessions = bars.sessions(start, through)
    slip = costs.slippage_bps / 10_000.0

    fills_by_day: Dict[dt.date, List[Fill]] = {}
    for f in inputs.fills:
        fills_by_day.setdefault(f.fill_date, []).append(f)
    pending_bench: Dict[dt.date, Dict[str, float]] = dict(inputs.benchmark_weights)

    s_qty: Dict[str, float] = {}
    s_cash = float(inputs.capital_usd)
    s_last: Dict[str, float] = {}
    b_qty: Dict[str, float] = {}
    b_cash = float(inputs.capital_usd)
    b_last: Dict[str, float] = {}
    rows: List[Dict[str, Any]] = []
    for ts in sessions:
        day = cast(pd.Timestamp, pd.Timestamp(ts)).date()
        # sleeve: fills dated this session settle at their recorded prices
        for f in fills_by_day.get(day, []):
            sgn = 1.0 if f.side == 'BUY' else -1.0
            s_cash -= sgn * f.quantity * f.price + f.commission
            s_qty[f.ticker] = s_qty.get(f.ticker, 0.0) + sgn * f.quantity
        # benchmark: every decision session strictly before this one that is
        # still pending fills now, at this session's open (t+1 open)
        due = sorted(d for d in pending_bench if d < day)  # type: ignore[operator]
        if due:
            weights = pending_bench[due[-1]]
            for d in due:
                pending_bench.pop(d, None)
            open_row = opens.loc[ts] if ts in opens.index else pd.Series(dtype=float)
            value = b_cash + sum(q * (float(open_row.get(t)) if np.isfinite(open_row.get(t, np.nan)) else b_last.get(t, 0.0))
                                 for t, q in b_qty.items())
            targets_q: Dict[str, float] = {}
            for t, w in weights.items():
                px = open_row.get(t, np.nan)
                if np.isfinite(px) and px > 0:
                    targets_q[t] = w * value / float(px)
            deltas: Dict[str, float] = {}
            for t in set(b_qty) | set(targets_q):
                px = open_row.get(t, np.nan)
                if not (np.isfinite(px) and px > 0):
                    continue                      # no print: cannot trade it today, hold
                delta = targets_q.get(t, 0.0) - b_qty.get(t, 0.0)
                if abs(delta) >= 1e-12:
                    deltas[t] = delta

            def _fill(t: str, delta: float) -> None:
                nonlocal b_cash
                px = float(open_row.get(t))
                fill = px * (1 + slip) if delta > 0 else px * (1 - slip)
                b_cash -= delta * fill + abs(delta) * costs.commission_per_share
                b_qty[t] = b_qty.get(t, 0.0) + delta
                if abs(b_qty[t]) < 1e-12:
                    b_qty.pop(t)

            for t, delta in deltas.items():           # sells first (run_panel order)
                if delta < 0:
                    _fill(t, delta)
            buy_cost = sum(d * float(open_row.get(t)) * (1 + slip) + d * costs.commission_per_share
                           for t, d in deltas.items() if d > 0)
            # D06: buys are scaled pro rata to the cash available after the sells, so
            # costs on a fully invested book can never overdraw the virtual account.
            scale = 1.0 if buy_cost <= max(b_cash, 0.0) else (max(b_cash, 0.0) / buy_cost if buy_cost > 0 else 0.0)
            for t, delta in deltas.items():
                if delta > 0 and scale > 0:
                    _fill(t, delta * scale)
            if b_cash < -1e-6:
                raise RuntimeError(f'virtual benchmark cash {b_cash:.2f} < 0 on {day} (bug: the D06 guard failed)')
        close_row = closes.loc[ts] if ts in closes.index else pd.Series(dtype=float)
        hv, hn, hu = _mark(s_qty, close_row, s_last)
        bv, bn, bu = _mark(b_qty, close_row, b_last)
        rows.append({'sleeve': inputs.sleeve, 'session': day, 'holdings_value': hv, 'cash': s_cash,
                     'sleeve_value': hv + s_cash, 'n_holdings': hn, 'n_unpriced': hu,
                     'benchmark_id': inputs.benchmark_id, 'benchmark_value': bv + b_cash, 'benchmark_cash': b_cash,
                     'benchmark_n_holdings': bn, 'benchmark_n_unpriced': bu, 'capital_usd': inputs.capital_usd})
    df = pd.DataFrame(rows)
    if len(df):
        df['session'] = pd.to_datetime(df['session'])
        df = df.set_index('session')
    return df


def track(sleeve: str, store: LedgerStore, bars: BarSource, targets_dir: Optional[str] = None,
          through: Optional[dt.date] = None, benchmark_id: Optional[str] = None) -> Dict[str, Any]:
    """Append one ledger row per session not yet recorded, through ``through``
    (default: the last stored session). Existing rows are never rewritten; a
    replayed value that disagrees with a stored row is reported, not applied."""
    inputs = load_inputs(sleeve, store, targets_dir, benchmark_id)
    end = through or bars.last_session()
    if end is None:
        raise RuntimeError('no stored sessions')
    frame = replay(inputs, bars, end)
    existing = store.ledger(sleeve)
    disagreements = []
    if len(existing) and len(frame):
        common = frame.index.intersection(existing.index)
        for ts in common:
            for col in ('sleeve_value', 'benchmark_value'):
                a, b = float(frame.at[ts, col]), float(existing.at[ts, col])
                if abs(a - b) > 1e-6 * max(1.0, abs(b)):
                    disagreements.append({'session': ts.date().isoformat(), 'column': col, 'stored': b, 'replayed': a})
    new_rows = [dict(r, session=cast(pd.Timestamp, pd.Timestamp(cast(Any, ts))).date()) for ts, r in frame.iterrows()
                if not len(existing) or ts not in existing.index]
    n = store.append_rows(new_rows)
    return {'sleeve': sleeve, 'appended': n, 'through': end.isoformat(), 'sessions_replayed': int(len(frame)),
            'disagreements': disagreements, 'benchmark_id': inputs.benchmark_id,
            'inception': inputs.inception.isoformat(), 'capital_usd': inputs.capital_usd}


def report(sleeve: str, store: LedgerStore) -> Dict[str, Any]:
    """Since-inception sleeve vs benchmark return, active return, tracking
    error (annualised std of daily active returns), from the ledger rows."""
    df = store.ledger(sleeve)
    if len(df) == 0:
        raise RuntimeError(f'no ledger rows for sleeve {sleeve!r}; run track first')
    cap = float(df['capital_usd'].iloc[0])
    sv = df['sleeve_value'].astype(float)
    bv = df['benchmark_value'].astype(float)
    s_ret = float(sv.iloc[-1] / cap - 1.0)
    b_ret = float(bv.iloc[-1] / cap - 1.0)
    s_daily = pd.concat([pd.Series([cap]), sv]).pct_change().dropna()
    b_daily = pd.concat([pd.Series([cap]), bv]).pct_change().dropna()
    active = (s_daily.to_numpy() - b_daily.to_numpy())
    te = float(np.std(active, ddof=1) * np.sqrt(252)) if len(active) > 1 else float('nan')
    days = int(len(df))
    return {'sleeve': sleeve, 'benchmark_id': str(df['benchmark_id'].iloc[-1]), 'sessions': days,
            'first_session': cast(pd.Timestamp, df.index[0]).date().isoformat(),
            'last_session': cast(pd.Timestamp, df.index[-1]).date().isoformat(),
            'capital_usd': cap, 'sleeve_value': float(sv.iloc[-1]), 'benchmark_value': float(bv.iloc[-1]),
            'sleeve_return': s_ret, 'benchmark_return': b_ret, 'active_return': s_ret - b_ret,
            'tracking_error_annualised': te,
            'active_return_annualised_mean': float(np.mean(active) * 252) if len(active) else float('nan'),
            'n_holdings': int(df['n_holdings'].iloc[-1]), 'n_unpriced': int(df['n_unpriced'].iloc[-1]),
            'cash': float(df['cash'].iloc[-1])}
