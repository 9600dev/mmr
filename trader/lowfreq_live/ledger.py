"""Paper ledger: a sleeve's daily value against its virtual decisive benchmark.

Both sides are valued from the SAME stored bars (``pit_daily_bars``, the bars
the research path reads) and both are REPLAYED from inception on every
``track`` call from durable inputs only: the sleeve's fills (the D13
``sleeve_fills`` journal C01 writes from IB executions - this module never
writes it), the sleeve's D08 target files (whose ``.benchmark.json`` sidecars
carry the benchmark weights ``targets`` computed on the identical panel and
coverage), and the bars. The ledger table is the OUTPUT: one row per session.
A row is PROVISIONAL until ``SILENCE_SESSIONS`` later sessions are stored (a
D07 exit ON it may still be booked with hindsight - see below) and SETTLED
after that, never rewritten, so a late fill or a data correction shows up as
a DISAGREEMENT the operator sees rather than a silently changed history.

The virtual benchmark is executed by ``Backtester.run_panel`` itself - the
executor the harness scores every candidate and benchmark with (D01) - fed a
strategy that replays the sidecar weights on their decision sessions. So its
whole-share sizing off the decision close, t+1-open fills, D06 pro-rata cash
guard, D07 terminal-print exits and carry-forward marks are the harness's by
construction, not a re-implementation. Cash earns zero (D03).

Holdings (D13): per-ticker sums of the sleeve's signed fill quantities,
reconciled against IB positions when a broker read is supplied - a sleeve may
never own more than IB shows, and a discrepancy REFUSES the run.
"""
from __future__ import annotations

import datetime as dt
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol, Tuple, cast

import numpy as np
import pandas as pd

from trader.data.duckdb_store import DuckDBConnection
from trader.lowfreq_live.targets import (BENCHMARK_SCHEMA, benchmark_sidecar_path, list_sleeve_targets,
                                         read_targets)
from trader.objects import BarSize
from trader.research.lowfreq import engine
from trader.research.lowfreq.costs import Costs, base_costs
from trader.research.lowfreq.loaders import PricePanel
from trader.simulation.backtester import BacktestConfig, Backtester, BacktestResult
from trader.trading.strategy import Strategy

# D13 schema (C01 writes it; C02 reads it; tests create it from here).
SLEEVE_FILLS_DDL = """CREATE TABLE IF NOT EXISTS sleeve_fills (
    sleeve VARCHAR NOT NULL, ticker VARCHAR NOT NULL, trade_date DATE NOT NULL,
    quantity DOUBLE NOT NULL, price DOUBLE NOT NULL, commission DOUBLE NOT NULL,
    exec_id VARCHAR PRIMARY KEY, order_id VARCHAR, proposal_id BIGINT, recorded_at TIMESTAMPTZ)"""
LEDGER_DDL = """CREATE TABLE IF NOT EXISTS lowfreq_paper_ledger (
    sleeve VARCHAR NOT NULL, session DATE NOT NULL,
    holdings_value DOUBLE, cash DOUBLE, sleeve_value DOUBLE, n_holdings INTEGER, n_unpriced INTEGER,
    benchmark_id VARCHAR, benchmark_value DOUBLE, benchmark_cash DOUBLE,
    benchmark_n_holdings INTEGER, benchmark_n_unpriced INTEGER,
    capital_usd DOUBLE, recorded_at TIMESTAMP NOT NULL, provisional BOOLEAN NOT NULL,
    PRIMARY KEY (sleeve, session))"""
# D07, exactly: a stopped name is exited ON the session of its LAST STORED
# PRINT, at that close (less slippage and commission) - the fill the harness
# books. The harness knows a print was the last with hindsight over the
# complete dataset; the ledger only learns it once the name has been silent
# for SILENCE_SESSIONS consecutive stored sessions. So a session is SETTLED
# once SILENCE_SESSIONS later sessions are stored (every D07 exit on or before
# it is then decided) and PROVISIONAL until then: a provisional row may be
# revised - in the benchmark columns only, when an exit is booked with
# hindsight - and `track` reports each revision; a settled row never changes
# except through an attested `rebuild`. Applied per episode: a name that
# stops, re-enters through a later rebalance and stops again is exited at the
# last print of each episode, and a gap before the benchmark first held it
# changes nothing (the executor sells only what it holds). A name that prints
# again after SILENCE_SESSIONS silent sessions (a long halt) has been exited
# at its last print and re-enters only through the next sidecar's weights -
# the one departure from the hindsight rule, which would have carried it.
SILENCE_SESSIONS = 5
# Settled rows are prefix-stable, so a stored row and its replay must agree to
# floating-point precision; anything larger is a late fill or a data
# correction and REFUSES the run (an attested `rebuild` rewrites history).
DISAGREEMENT_TOLERANCE = 1e-6
SLEEVE_COLUMNS = ('sleeve_value', 'cash')                   # never revised silently, provisional or not
BENCHMARK_COLUMNS = ('benchmark_value', 'benchmark_cash')   # revisable while provisional (D07 hindsight)
REBUILDS_DDL = """CREATE TABLE IF NOT EXISTS lowfreq_ledger_rebuilds (
    sleeve VARCHAR NOT NULL, rebuilt_at TIMESTAMP NOT NULL, reason VARCHAR NOT NULL,
    rows_dropped INTEGER NOT NULL, rows_written INTEGER NOT NULL, disagreements VARCHAR)"""


class ReconciliationError(RuntimeError):
    """The sleeve's journaled holdings exceed what the broker shows (D13)."""


class LedgerDisagreement(RuntimeError):
    """A replay differs from stored rows; nothing was appended."""
    def __init__(self, sleeve: str, disagreements: List[Dict[str, Any]]):
        self.disagreements = disagreements
        shown = '; '.join(f"{d['session']} {d['column']} stored {d['stored']:.2f} replayed {d['replayed']:.2f}"
                          for d in disagreements[:5])
        super().__init__(f'{sleeve}: replay disagrees with {len(disagreements)} stored value(s) - {shown}'
                         f'{" ..." if len(disagreements) > 5 else ""}. Nothing appended; investigate (a late '
                         f'fill? corrected bars?) and run `rebuild --sleeve {sleeve} --reason ... --attest`.')


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
        idx = self.sessions(start, end)
        if not names:
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
        return cast(pd.Timestamp, pd.Timestamp(self.closes.index[-1])).date() if len(self.closes.index) else None


@dataclass(frozen=True)
class Fill:
    """One row of ``sleeve_fills`` (D13): ``quantity`` is SIGNED (+ buy, - sell)."""
    trade_date: dt.date
    ticker: str
    quantity: float
    price: float
    commission: float = 0.0
    exec_id: str = ''
    order_id: str = ''
    proposal_id: Optional[int] = None


class LedgerStore:
    """The ledger table (written here) and the D13 fills journal (read only)
    in the MAIN DuckDB."""
    def __init__(self, db_path: str):
        self.db = DuckDBConnection(str(Path(db_path).expanduser()))
        self.db.execute(LEDGER_DDL, fetch='none')
        self.db.execute(REBUILDS_DDL, fetch='none')

    def rebuild_atomic(self, sleeve: str, rows: List[Dict[str, Any]], reason: str,
                       disagreements: list) -> Tuple[int, int]:
        """Drop the sleeve's rows, write the replay and record the audit row in
        ONE transaction (``execute_atomic`` commits or rolls the whole callback
        back), so a failure part-way leaves the table exactly as it was - never
        a half-written history, and never a rewritten one without its audit
        entry. Returns ``(rows_dropped, rows_written)``."""
        def _do(con):
            n = con.execute('SELECT count(*) FROM lowfreq_paper_ledger WHERE sleeve = ?', [sleeve]).fetchone()
            dropped = int(n[0]) if n else 0
            con.execute('DELETE FROM lowfreq_paper_ledger WHERE sleeve = ?', [sleeve])
            written = 0
            for r in rows:
                con.execute('INSERT INTO lowfreq_paper_ledger VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?, now(), ?)',
                            [sleeve, r['session'], r['holdings_value'], r['cash'], r['sleeve_value'], r['n_holdings'],
                             r['n_unpriced'], r['benchmark_id'], r['benchmark_value'], r['benchmark_cash'],
                             r['benchmark_n_holdings'], r['benchmark_n_unpriced'], r['capital_usd'],
                             bool(r['provisional'])])
                written += 1
            con.execute('INSERT INTO lowfreq_ledger_rebuilds VALUES (?, now(), ?, ?, ?, ?)',
                        [sleeve, reason, dropped, written, json.dumps(disagreements, default=str)])
            return dropped, written
        return self.db.execute_atomic(_do)

    def rebuilds(self, sleeve: str) -> List[Dict[str, Any]]:
        rows = self.db.execute('SELECT rebuilt_at, reason, rows_dropped, rows_written FROM lowfreq_ledger_rebuilds '
                               'WHERE sleeve = ? ORDER BY rebuilt_at', [sleeve], fetch='all') or []
        return [{'rebuilt_at': str(r[0]), 'reason': r[1], 'rows_dropped': int(r[2]), 'rows_written': int(r[3])}
                for r in rows]

    def fills(self, sleeve: str) -> List[Fill]:
        """Every ``sleeve_fills`` row of the sleeve in trade order. Refuses
        (rather than assuming an empty book) when C01 has not created the
        table: a missing journal is not a flat sleeve."""
        try:
            rows = self.db.execute(
                'SELECT trade_date, ticker, quantity, price, commission, exec_id, order_id, proposal_id '
                'FROM sleeve_fills WHERE sleeve = ? ORDER BY trade_date, recorded_at, exec_id', [sleeve], fetch='all') or []
        except Exception as exc:                          # noqa: BLE001 - DuckDB CatalogException
            raise RuntimeError(f'sleeve_fills is not readable ({exc}); C01\'s `trader.rebalance reconcile` '
                               f'creates and fills it (D13)') from exc
        out = []
        for r in rows:
            q, p, c = float(r[2]), float(r[3]), float(r[4] or 0.0)
            if not (math.isfinite(q) and q != 0 and math.isfinite(p) and p > 0 and math.isfinite(c) and c >= 0):
                raise RuntimeError(f'sleeve_fills row {r[5]!r} for {sleeve} is malformed: qty={q} price={p} comm={c}')
            out.append(Fill(r[0], str(r[1]).upper(), q, p, c, str(r[5] or ''), str(r[6] or ''),
                            int(r[7]) if r[7] is not None else None))
        return out

    def ledger(self, sleeve: str) -> pd.DataFrame:
        df = self.db.execute('SELECT * FROM lowfreq_paper_ledger WHERE sleeve = ? ORDER BY session', [sleeve], fetch='df')
        if df is None or len(df) == 0:
            return pd.DataFrame()
        df['session'] = pd.to_datetime(df['session'])
        df['provisional'] = df['provisional'].astype(bool)
        return df.set_index('session')

    _VALUE_COLUMNS = ('holdings_value', 'cash', 'sleeve_value', 'n_holdings', 'n_unpriced', 'benchmark_value',
                      'benchmark_cash', 'benchmark_n_holdings', 'benchmark_n_unpriced')

    def apply_replay(self, sleeve: str, rows: List[Dict[str, Any]]) -> Dict[str, int]:
        """Write a replay in ONE transaction: sessions not yet stored are
        inserted; a stored PROVISIONAL row is rewritten when its values changed
        (a D07 exit booked with hindsight) or when it settles; a stored SETTLED
        row is never touched (``_compare`` has already refused any difference
        on it). Returns the counts of rows inserted / revised / settled."""
        cols = list(self._VALUE_COLUMNS)

        def _do(con):
            stored = {r[0]: (bool(r[1]), r[2:]) for r in con.execute(
                'SELECT session, provisional, ' + ', '.join(cols) + ' FROM lowfreq_paper_ledger WHERE sleeve = ?',
                [sleeve]).fetchall()}
            inserted = revised = settled = 0
            for r in rows:
                cur = stored.get(r['session'])
                if cur is None:
                    con.execute('INSERT INTO lowfreq_paper_ledger VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?, now(), ?)',
                                [sleeve, r['session'], r['holdings_value'], r['cash'], r['sleeve_value'], r['n_holdings'],
                                 r['n_unpriced'], r['benchmark_id'], r['benchmark_value'], r['benchmark_cash'],
                                 r['benchmark_n_holdings'], r['benchmark_n_unpriced'], r['capital_usd'],
                                 bool(r['provisional'])])
                    inserted += 1
                    continue
                was_provisional, values = cur
                if not was_provisional:
                    continue                                          # settled rows are immutable
                changed = any(abs(float(r[c]) - float(v)) > DISAGREEMENT_TOLERANCE * max(1.0, abs(float(v)))
                              for c, v in zip(cols, values))
                settles = not bool(r['provisional'])
                if changed or settles:
                    con.execute('UPDATE lowfreq_paper_ledger SET ' + ', '.join(f'{c} = ?' for c in cols)
                                + ', provisional = ?, recorded_at = now() WHERE sleeve = ? AND session = ?',
                                [*(r[c] for c in cols), bool(r['provisional']), sleeve, r['session']])
                    revised += int(changed)
                    settled += int(settles)
            return {'inserted': inserted, 'revised': revised, 'settled': settled}
        return self.db.execute_atomic(_do)


# ---------------------------------------------------------------- inputs ---

@dataclass
class SleeveInputs:
    sleeve: str
    capital_usd: float                        # inception capital: the FIRST D08 file's capital_usd
    inception: dt.date                        # the first decision session
    benchmark_id: str
    benchmark_weights: Dict[dt.date, Dict[str, float]]   # decision session -> weights (sessions it held are absent)
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
        pair = read_targets(p)
        for key, want in (('sleeve', pair['sleeve']), ('as_of', pair['as_of']),
                          ('candidate', pair['strategy']['id']), ('capital_usd', pair['capital_usd'])):
            if doc.get(key) != want:
                raise RuntimeError(f'{side}: {key}={doc.get(key)!r} does not match its D08 file ({want!r})')
        if pair['strategy']['id'] != first['strategy']['id'] or pair['sleeve'] != sleeve:
            raise RuntimeError(f'{p}: candidate/sleeve differ from the inception file')
        benches = doc.get('benchmarks') or {}
        if bid is None:
            if len(benches) != 1:
                raise RuntimeError(f'{side} carries {len(benches)} benchmarks; pass --benchmark <id>')
            bid = next(iter(benches))
        if bid not in benches:
            raise RuntimeError(f'{side} has no benchmark {bid!r}')
        if benches[bid] is None:
            continue                                    # the benchmark held on that session: no rebalance
        bench[dt.date.fromisoformat(doc['as_of'])] = {str(k): float(v) for k, v in benches[bid].items() if float(v) > 0}
    return SleeveInputs(sleeve=sleeve, capital_usd=float(first['capital_usd']),
                        inception=dt.date.fromisoformat(first['as_of']), benchmark_id=str(bid),
                        benchmark_weights=bench, fills=store.fills(sleeve))


def holdings_from_fills(fills: Iterable[Fill], through: Optional[dt.date] = None) -> Dict[str, float]:
    """Per-ticker signed sums of the fills dated on or before ``through`` (D13)."""
    qty: Dict[str, float] = {}
    for f in fills:
        if through is not None and f.trade_date > through:
            continue
        qty[f.ticker] = qty.get(f.ticker, 0.0) + f.quantity
    return {t: q for t, q in qty.items() if abs(q) > 1e-9}


def reconcile_holdings(holdings: Mapping[str, float], broker: Mapping[str, float]) -> None:
    """D13: a sleeve may never own more than IB shows. Raises on the first
    violation; a short journaled quantity is refused outright (long-only)."""
    problems = []
    for t, q in sorted(holdings.items()):
        if q < -1e-9:
            problems.append(f'{t}: journaled quantity {q} is negative')
            continue
        have = float(broker.get(t, 0.0))
        if have + 1e-9 < q:
            problems.append(f'{t}: sleeve journal says {q:g}, IB shows {have:g}')
    if problems:
        raise ReconciliationError('holdings exceed IB positions - ' + '; '.join(problems))


# ------------------------------------------------------ benchmark replay ---

class _NoStorage:
    def get_tickdata(self, bar_size):
        raise RuntimeError('the ledger injects frames; tick_data is not read')


class _WeightsReplay(Strategy):
    """Replays recorded target weights on their decision sessions; holds otherwise."""
    def __init__(self, schedule: Dict[dt.date, Dict[str, float]]):
        super().__init__()
        self._schedule = dict(schedule)

    def precompute_panel(self, panel: pd.DataFrame) -> Dict[str, Any]:
        return {}

    def on_panel(self, panel, state, index) -> Optional[Dict[Any, float]]:
        day = cast(pd.Timestamp, pd.Timestamp(panel.index[index])).date()
        w = self._schedule.get(day)
        if w is None:
            return None
        out: Dict[Any, float] = {c: 0.0 for c in panel['close'].columns}   # explicit flat elsewhere
        out.update(w)
        return out


def backtest_config(start: dt.date, end_excl: dt.date, capital: float, costs: Costs,
                    terminal: Mapping[Any, Any]) -> BacktestConfig:
    """Field-for-field the harness's ``engine._config`` with the sleeve's capital
    (pinned by a test against ``engine._config``). ``terminal`` maps a name to
    its terminal print (the harness) or to every recognised silence (the live
    rule); ``run_panel`` accepts both."""
    return BacktestConfig(
        start_date=dt.datetime.combine(start, dt.time()), end_date=dt.datetime.combine(end_excl, dt.time()),
        initial_capital=capital, bar_size=BarSize.Days1,
        slippage_bps=costs.slippage_bps, commission_per_share=costs.commission_per_share,
        short_borrow_bps_annual=costs.short_borrow_bps_annual, panel_max_gross=costs.max_gross,
        fill_policy='next_open', panel_terminal_prints=dict(terminal))


def terminal_prints_live(panel: PricePanel, silence: int = SILENCE_SESSIONS) -> Dict[Any, List[pd.Timestamp]]:
    """D07 without hindsight: ticker -> the session of EVERY print that is
    followed by at least ``silence`` consecutive stored sessions without a
    print (one entry per stopped episode). ``Backtester.run_panel`` sells the
    holding ON that bar at its close - the harness's own D07 fill - if the name
    is held then, and does nothing when it is not. Decided from the panel's
    bars only, so an entry never changes once made; a print followed by fewer
    than ``silence`` silent sessions is still undecided and lies inside the
    provisional window (``settled_cutoff``)."""
    close = panel.close.replace([np.inf, -np.inf], np.nan)
    idx = pd.DatetimeIndex(close.index)
    out: Dict[Any, List[pd.Timestamp]] = {}
    for t in close.columns:
        printed = (close[t] > 0).to_numpy()
        last_print: Optional[int] = None
        run = 0
        for i, has in enumerate(printed):
            if has:
                last_print, run = i, 0
                continue
            if last_print is None:
                continue
            run += 1
            if run == silence:
                out.setdefault(t, []).append(cast(pd.Timestamp, idx[last_print]))
    return out


def settled_cutoff(sessions: pd.DatetimeIndex, silence: int = SILENCE_SESSIONS) -> Optional[pd.Timestamp]:
    """The last SETTLED session of a replay over ``sessions``: the latest one
    with ``silence`` stored sessions after it, so every D07 exit on or before
    it is decided. None while nothing is settled."""
    n = len(sessions)
    return cast(pd.Timestamp, sessions[n - silence - 1]) if n > silence else None


def run_benchmark(inputs: SleeveInputs, bars: BarSource, through: dt.date,
                  costs: Optional[Costs] = None) -> Tuple[BacktestResult, PricePanel, pd.DatetimeIndex]:
    """Execute the virtual benchmark with ``Backtester.run_panel`` over the
    stored bars in [inception, through]. Returns the executor's result (trade
    log and diagnostics, including ``panel_forced_exits``), the price panel it
    ran on and the stored sessions."""
    costs = costs or base_costs('equity')
    names: set = set()
    for w in inputs.benchmark_weights.values():
        names |= set(w)
    sessions = bars.sessions(inputs.inception, through)
    if len(sessions) == 0 or cast(pd.Timestamp, sessions[0]).date() != inputs.inception:
        raise RuntimeError(f'inception {inputs.inception} is not a stored session')
    if len(names) < 2:
        raise RuntimeError('the benchmark names fewer than 2 instruments; nothing to replay')
    opens, closes = bars.bars(names, inputs.inception, through)
    panel = PricePanel(open=opens, close=closes)
    frames = panel.frames()
    if len(frames) < 2:
        raise RuntimeError('fewer than 2 benchmark names have stored bars in the window')
    terminal = terminal_prints_live(panel)      # D07: last print of every stopped episode decided so far
    cfg = backtest_config(inputs.inception, through + dt.timedelta(days=1), inputs.capital_usd, costs, terminal)
    res = Backtester(cast(Any, _NoStorage()), cfg).run_panel(
        _WeightsReplay(inputs.benchmark_weights), cast(Any, list(frames.keys())), frames=frames)
    return res, panel, sessions


def replay_benchmark(inputs: SleeveInputs, bars: BarSource, through: dt.date,
                     costs: Optional[Costs] = None) -> pd.DataFrame:
    """The virtual benchmark's per-session value/cash/holdings, executed by
    ``Backtester.run_panel`` over the stored bars. The executor's ``panel_*``
    diagnostics (forced exits, D06 scaling) ride along in ``.attrs['diagnostics']``."""
    res, panel, sessions = run_benchmark(inputs, bars, through, costs)
    closes = panel.close
    cols = ['benchmark_value', 'benchmark_cash', 'benchmark_n_holdings', 'benchmark_n_unpriced']
    equity = res.equity_curve.astype(float)
    equity.index = pd.DatetimeIndex(equity.index)
    equity = equity.reindex(sessions).ffill().fillna(inputs.capital_usd)
    # cash and positions per session, rebuilt from the executor's own fill log
    by_ts: Dict[Any, list] = {}
    for t in res.trades:
        by_ts.setdefault(cast(Any, pd.Timestamp(t.timestamp)), []).append(t)
    cash = float(inputs.capital_usd)
    qty: Dict[str, float] = {}
    out = []
    for ts in sessions:
        for t in by_ts.get(pd.Timestamp(ts), []):
            sgn = 1.0 if str(t.action).endswith('BUY') else -1.0
            cash -= sgn * t.quantity * t.price + t.commission
            qty[t.conid] = qty.get(t.conid, 0.0) + sgn * t.quantity
            if abs(qty[t.conid]) < 1e-9:
                qty.pop(t.conid)
        row = closes.loc[ts] if ts in closes.index else pd.Series(dtype=float)
        unpriced = sum(1 for t in qty if not (np.isfinite(row.get(t, np.nan)) and row.get(t, 0) > 0))
        out.append({'benchmark_value': float(equity.loc[ts]), 'benchmark_cash': cash,
                    'benchmark_n_holdings': len(qty), 'benchmark_n_unpriced': unpriced})
    df = pd.DataFrame(out, index=sessions, columns=cols)
    df.attrs['diagnostics'] = {k: v for k, v in res.diagnostics.items() if str(k).startswith('panel_')}
    return df


def _mark(qty: Dict[str, float], closes_row: pd.Series, last: Dict[str, float]) -> Tuple[float, int, int]:
    value, n, unpriced = 0.0, 0, 0
    for t, q in qty.items():
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
    """One row per session in [inception, through]: sleeve holdings x closes +
    cash from the fills journal, beside the run_panel-executed benchmark."""
    bench = replay_benchmark(inputs, bars, through, costs)
    sessions = bench.index
    names = {f.ticker for f in inputs.fills}
    _, closes = bars.bars(names, inputs.inception, through)
    fills_by_day: Dict[dt.date, List[Fill]] = {}
    for f in inputs.fills:
        fills_by_day.setdefault(f.trade_date, []).append(f)
    qty: Dict[str, float] = {}
    cash = float(inputs.capital_usd)
    last: Dict[str, float] = {}
    rows: List[Dict[str, Any]] = []
    cutoff = settled_cutoff(pd.DatetimeIndex(sessions))
    for ts in sessions:
        day = cast(pd.Timestamp, pd.Timestamp(ts)).date()
        provisional = cutoff is None or pd.Timestamp(ts) > cutoff
        for f in fills_by_day.get(day, []):
            cash -= f.quantity * f.price + f.commission
            qty[f.ticker] = qty.get(f.ticker, 0.0) + f.quantity
            if abs(qty[f.ticker]) < 1e-9:
                qty.pop(f.ticker)
        row = closes.loc[ts] if ts in closes.index else pd.Series(dtype=float)
        hv, hn, hu = _mark(qty, row, last)
        b = bench.loc[ts]
        rows.append({'sleeve': inputs.sleeve, 'session': day, 'holdings_value': hv, 'cash': cash,
                     'sleeve_value': hv + cash, 'n_holdings': hn, 'n_unpriced': hu,
                     'benchmark_id': inputs.benchmark_id, 'benchmark_value': float(b['benchmark_value']),
                     'benchmark_cash': float(b['benchmark_cash']),
                     'benchmark_n_holdings': int(b['benchmark_n_holdings']),
                     'benchmark_n_unpriced': int(b['benchmark_n_unpriced']), 'capital_usd': inputs.capital_usd,
                     'provisional': provisional})
    df = pd.DataFrame(rows)
    if len(df):
        df['session'] = pd.to_datetime(df['session'])
        df = df.set_index('session')
    df.attrs['benchmark_diagnostics'] = dict(bench.attrs.get('diagnostics', {}))
    return df


def _compare(frame: pd.DataFrame, existing: pd.DataFrame) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """``(disagreements, revisions)``. A difference on a SETTLED stored row, or
    in a sleeve-side column of any row, is a disagreement (a late fill,
    corrected bars: the run is refused). A benchmark-side difference on a
    PROVISIONAL stored row is a revision - a D07 exit booked with hindsight -
    which is allowed and reported."""
    dis: List[Dict[str, Any]] = []
    rev: List[Dict[str, Any]] = []
    if not len(existing) or not len(frame):
        return dis, rev
    for ts in frame.index.intersection(existing.index):
        provisional = bool(existing.at[ts, 'provisional'])
        for col in SLEEVE_COLUMNS + BENCHMARK_COLUMNS:
            a, b = float(frame.at[ts, col]), float(existing.at[ts, col])
            if abs(a - b) > DISAGREEMENT_TOLERANCE * max(1.0, abs(b)):
                item = {'session': cast(pd.Timestamp, ts).date().isoformat(), 'column': col, 'stored': b, 'replayed': a}
                (rev if provisional and col in BENCHMARK_COLUMNS else dis).append(item)
    return dis, rev


def _prepare(sleeve: str, store: LedgerStore, bars: BarSource, targets_dir: Optional[str],
             through: Optional[dt.date], benchmark_id: Optional[str],
             broker_positions: Mapping[str, float]) -> Tuple[SleeveInputs, dt.date, Dict[str, float], pd.DataFrame]:
    if broker_positions is None:                      # the CLI reads IB; tests pass fakes
        raise ReconciliationError('broker positions are required (D13): a sleeve may never own more than IB shows')
    inputs = load_inputs(sleeve, store, targets_dir, benchmark_id)
    end = through or bars.last_session()
    if end is None:
        raise RuntimeError('no stored sessions')
    holdings = holdings_from_fills(inputs.fills, end)
    reconcile_holdings(holdings, broker_positions)
    return inputs, end, holdings, replay(inputs, bars, end)


def track(sleeve: str, store: LedgerStore, bars: BarSource, broker_positions: Mapping[str, float],
          targets_dir: Optional[str] = None, through: Optional[dt.date] = None,
          benchmark_id: Optional[str] = None) -> Dict[str, Any]:
    """Record the replay through ``through`` (default: the last stored
    session): one row per session not yet recorded, and the revision of
    PROVISIONAL rows on which a D07 exit has since been decided (benchmark
    columns only; each revision is returned in ``revisions``).

    ``broker_positions`` (symbol -> quantity from IB) is REQUIRED: the
    journaled holdings are reconciled first and a discrepancy refuses the run
    (D13). The replay must agree with every SETTLED stored row, and with the
    sleeve-side columns of every row; any disagreement raises
    ``LedgerDisagreement`` BEFORE anything is written, so the table never holds
    rows from two incompatible histories. ``rebuild`` is the only way to
    rewrite settled rows, and it is attested and audited."""
    inputs, end, holdings, frame = _prepare(sleeve, store, bars, targets_dir, through, benchmark_id, broker_positions)
    existing = store.ledger(sleeve)
    disagreements, revisions = _compare(frame, existing)
    if disagreements:
        raise LedgerDisagreement(sleeve, disagreements)
    rows = [dict(r, session=cast(pd.Timestamp, pd.Timestamp(cast(Any, ts))).date()) for ts, r in frame.iterrows()]
    counts = store.apply_replay(sleeve, rows)
    diag = frame.attrs.get('benchmark_diagnostics', {})
    return {'sleeve': sleeve, 'appended': counts['inserted'], 'revised': counts['revised'],
            'settled': counts['settled'], 'provisional_sessions': int(frame['provisional'].sum()),
            'revisions': revisions, 'through': end.isoformat(), 'sessions_replayed': int(len(frame)),
            'benchmark_id': inputs.benchmark_id, 'inception': inputs.inception.isoformat(),
            'capital_usd': inputs.capital_usd, 'reconciled': True, 'holdings': holdings,
            'benchmark_forced_exits': int(diag.get('panel_forced_exits', 0)),          # D07 live-rule exits
            'benchmark_forced_exit_value': float(diag.get('panel_forced_exit_value', 0.0))}


def rebuild(sleeve: str, store: LedgerStore, bars: BarSource, broker_positions: Mapping[str, float],
            reason: str, targets_dir: Optional[str] = None, through: Optional[dt.date] = None,
            benchmark_id: Optional[str] = None) -> Dict[str, Any]:
    """Operator act: drop the sleeve's ledger rows and replay from inception.
    Requires a non-empty ``reason``; the act, the reason, the counts and the
    disagreements it resolved are recorded in ``lowfreq_ledger_rebuilds``."""
    if not (reason or '').strip():
        raise RuntimeError('rebuild requires a reason')
    inputs, end, holdings, frame = _prepare(sleeve, store, bars, targets_dir, through, benchmark_id, broker_positions)
    disagreements, _revisions = _compare(frame, store.ledger(sleeve))
    rows = [dict(r, session=cast(pd.Timestamp, pd.Timestamp(cast(Any, ts))).date()) for ts, r in frame.iterrows()]
    dropped, written = store.rebuild_atomic(sleeve, rows, reason.strip(), disagreements)
    diag = frame.attrs.get('benchmark_diagnostics', {})
    return {'sleeve': sleeve, 'rows_dropped': dropped, 'rows_written': written, 'through': end.isoformat(),
            'disagreements_resolved': disagreements, 'reason': reason.strip(), 'holdings': holdings,
            'benchmark_forced_exits': int(diag.get('panel_forced_exits', 0))}


def report(sleeve: str, store: LedgerStore) -> Dict[str, Any]:
    """Since-inception sleeve vs benchmark return, active return, tracking
    error (annualised std of daily active returns), from the ledger rows."""
    df = store.ledger(sleeve)
    if len(df) == 0:
        raise RuntimeError(f'no ledger rows for sleeve {sleeve!r}; run track first')
    cap = float(df['capital_usd'].iloc[0])
    prov = df['provisional'].astype(bool)
    settled = df.index[~prov]
    sv = df['sleeve_value'].astype(float)
    bv = df['benchmark_value'].astype(float)
    s_ret = float(sv.iloc[-1] / cap - 1.0)
    b_ret = float(bv.iloc[-1] / cap - 1.0)
    s_daily = pd.concat([pd.Series([cap]), sv]).pct_change().dropna()
    b_daily = pd.concat([pd.Series([cap]), bv]).pct_change().dropna()
    active = (s_daily.to_numpy() - b_daily.to_numpy())
    te = float(np.std(active, ddof=1) * np.sqrt(252)) if len(active) > 1 else float('nan')
    return {'sleeve': sleeve, 'benchmark_id': str(df['benchmark_id'].iloc[-1]), 'sessions': int(len(df)),
            'first_session': cast(pd.Timestamp, df.index[0]).date().isoformat(),
            'last_session': cast(pd.Timestamp, df.index[-1]).date().isoformat(),
            'capital_usd': cap, 'sleeve_value': float(sv.iloc[-1]), 'benchmark_value': float(bv.iloc[-1]),
            'sleeve_return': s_ret, 'benchmark_return': b_ret, 'active_return': s_ret - b_ret,
            'tracking_error_annualised': te,
            'active_return_annualised_mean': float(np.mean(active) * 252) if len(active) else float('nan'),
            'n_holdings': int(df['n_holdings'].iloc[-1]), 'n_unpriced': int(df['n_unpriced'].iloc[-1]),
            'cash': float(df['cash'].iloc[-1]), 'rebuilds': store.rebuilds(sleeve),
            'provisional_sessions': int(prov.sum()),          # trailing rows a D07 exit may still revise
            'settled_through': cast(pd.Timestamp, settled[-1]).date().isoformat() if len(settled) else None}
