"""Run one registered candidate cell over one window through ``Backtester.run_panel``.

Every number the harness reports - candidate, benchmark, stressed, per fold -
comes out of this one function, so strategy and benchmark cannot differ for
reasons unrelated to the signal (D01: identical execution path and costs).
Fills are at t+1's open; None = hold, {} = flatten; cash earns zero (D03).
"""
from __future__ import annotations

import datetime as dt
import hashlib
import importlib
import importlib.util
import inspect
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, cast

import numpy as np
import pandas as pd

from trader.objects import BarSize
from trader.research.lowfreq import config as _cfg
from trader.research.lowfreq.costs import Costs
from trader.research.lowfreq.loaders import PricePanel, to_date
from trader.simulation.backtester import BacktestConfig, Backtester
from trader.simulation.lookahead_check import assert_no_panel_lookahead
from trader.strategy.parameters import apply_param_overrides
from trader.trading.strategy import Strategy, StrategyContext, StrategyState

INITIAL_CAPITAL = 1_000_000.0
PANEL_FIELDS = ('open', 'high', 'low', 'close', 'volume')
# Lookahead sampling (orchestrator ruling, round 3): prefixes are replayed with
# FRESH state at the first/last EDGE decision indices and in PROBE_WINDOWS runs
# of PROBE_WINDOW_LEN consecutive indices spread over the panel. A run is longer
# than any calendar month of sessions, so a monthly schedule's due bar lies
# inside every run and a weekly one several times.
PROBE_WINDOWS = 3
PROBE_WINDOW_LEN = 23
PROBE_EDGE = 3


class _NoStorage:
    """run_panel receives injected frames; nothing may read the store."""
    def get_tickdata(self, bar_size):
        raise RuntimeError('the lowfreq harness injects frames; tick_data is not read (D03)')


@dataclass
class WindowResult:
    returns: pd.Series
    equity: pd.Series
    n_trades: int
    turnover_per_year: Optional[float]
    cell: Dict[str, Any]
    median_holding_adv_usd: Optional[float] = None   # D05 capacity: median 21d $ADV of held names
    median_names_held: Optional[float] = None
    min_cash_fraction: Optional[float] = None         # min cash / initial capital (< 0 = implicit leverage)
    frozen_value_fraction_end: Optional[float] = None  # value of unpriceable holdings at window end / equity
    n_frozen_holdings_end: Optional[int] = None
    scaled_buy_fills: Optional[int] = None            # D06: BUY fills the cash guard scaled down
    scaled_rebalances: Optional[int] = None           # D06: fill dates on which the guard acted
    forced_exits: Optional[int] = None                # D07: holdings sold at their last stored print
    forced_exit_value_fraction: Optional[float] = None  # D07: their gross value / initial capital
    coverage: Optional[pd.DataFrame] = None           # D05: coverage THIS run published, on ITS bars

    def metrics(self) -> Dict[str, Any]:
        from trader.research.lowfreq import stats
        m = stats.summarise(self.returns)
        m.update({'n_trades': self.n_trades, 'turnover_per_year': self.turnover_per_year,
                  'median_holding_adv_usd': self.median_holding_adv_usd,
                  'median_names_held': self.median_names_held,
                  'min_cash_fraction': self.min_cash_fraction,
                  'frozen_value_fraction_end': self.frozen_value_fraction_end,
                  'n_frozen_holdings_end': self.n_frozen_holdings_end,
                  'scaled_buy_fills': self.scaled_buy_fills,
                  'scaled_rebalances': self.scaled_rebalances,
                  'forced_exits': self.forced_exits,
                  'forced_exit_value_fraction': self.forced_exit_value_fraction})
        return m


def load_strategy_class(module: str, class_name: str) -> type:
    """``module`` is a dotted name (``strategies.lowfreq_xasset_x``) or a path
    ending in ``.py`` relative to the repo root."""
    if module.endswith('.py'):
        path = _cfg.repo_root() / module
        if not path.exists():
            raise FileNotFoundError(f'strategy module {path} does not exist')
        key = 'lowfreq_' + hashlib.sha256(str(path).encode()).hexdigest()[:12]
        spec = importlib.util.spec_from_file_location(key, str(path))
        if spec is None or spec.loader is None:
            raise ImportError(f'cannot load {path}')
        mod = importlib.util.module_from_spec(spec)
        sys.modules[key] = mod
        spec.loader.exec_module(mod)
    else:
        root = str(_cfg.repo_root())
        if root not in sys.path:
            sys.path.insert(0, root)
        mod = importlib.import_module(module)
    cls = getattr(mod, class_name, None)
    if cls is None or not inspect.isclass(cls) or not issubclass(cls, Strategy):
        raise TypeError(f'{module}.{class_name} is not a trader.trading.strategy.Strategy subclass')
    return cls


def strategy_source_sha256(cls: type) -> Optional[str]:
    try:
        src = inspect.getsource(sys.modules[cls.__module__])
    except (OSError, TypeError, KeyError):
        return None
    return hashlib.sha256(src.encode('utf-8')).hexdigest()


def make_strategy(entry: Dict[str, Any], cell: Dict[str, Any], panel: PricePanel,
                  coverage: Optional[pd.DataFrame] = None) -> Strategy:
    """Construct, parametrise and install one strategy instance.

    ``coverage`` is the CANDIDATE's boolean (time x ticker) coverage frame; a
    benchmark that defines ``set_coverage`` receives it so it owns exactly the
    names the candidate could have chosen from (D05 decisive benchmark).
    """
    strat = entry['strategy']
    cls = load_strategy_class(str(strat['module']), str(strat['class']))
    inst = cls()
    from trader.data.universe import UniverseAccessor
    ctx = StrategyContext(
        name=str(entry['id']), bar_size=BarSize.Days1, conids=[], universe=None,
        historical_days_prior=0, paper_only=True, storage=cast(Any, None),
        universe_accessor=UniverseAccessor.__new__(UniverseAccessor), logger=cast(Any, None),
        module=str(strat['module']), class_name=str(strat['class']))
    inst.install(ctx)
    inst.state = StrategyState.RUNNING
    apply_param_overrides(inst, dict(cell) if cell else None)
    setter = getattr(inst, 'set_research_data', None)
    if callable(setter):
        setter({'fields': panel.fields, 'signals': panel.signals, 'universe': panel.universe})
    cov_setter = getattr(inst, 'set_coverage', None)
    if coverage is not None and callable(cov_setter):
        cov_setter(coverage)
    return inst


def candidate_coverage(state: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """The candidate's published ``coverage`` frame, if its precompute state
    carries one (boolean, time x ticker); None otherwise."""
    cov = state.get('coverage') if isinstance(state, dict) else None
    if isinstance(cov, pd.DataFrame) and cov.shape[1] > 0:
        return cov.fillna(False).astype(bool)
    return None


def _holdings_capacity(trades, adv: Optional[pd.DataFrame], meas_start, index) -> tuple:
    """Median (over holdings snapshots) of the median 21-day $ADV of held names,
    and the median count of names held. Holdings are rebuilt from the fill log;
    snapshots are taken at the first measured bar (the book carried in from
    warm-up) and at every fill date inside the measured window."""
    if not trades:
        return None, None
    qty: Dict[Any, float] = {}
    by_ts: Dict[Any, list] = {}
    for t in trades:
        by_ts.setdefault(pd.Timestamp(t.timestamp), []).append(t)

    def apply(ts):
        for t in by_ts[ts]:
            signed = t.quantity if str(t.action).endswith('BUY') else -t.quantity
            qty[t.conid] = qty.get(t.conid, 0.0) + signed

    def held():
        return [c for c, q in qty.items() if q > 1e-9]

    ms = pd.Timestamp(meas_start)
    ordered = sorted(by_ts)
    snapshots = []
    for ts in ordered:
        if ts < ms:
            apply(ts)
    measured = [ts for ts in pd.DatetimeIndex(index) if ts >= ms]
    if measured:
        snapshots.append((measured[0], held()))
    for ts in ordered:
        if ts >= ms:
            apply(ts)
            snapshots.append((ts, held()))
    counts = [len(h) for _, h in snapshots]
    advs = []
    if adv is not None:
        for ts, h in snapshots:
            if h and ts in adv.index:
                row = adv.loc[ts].reindex(h).dropna()
                if len(row):
                    advs.append(float(row.median()))
    return (float(np.median(advs)) if advs else None), (float(np.median(counts)) if counts else None)


def wide_panel(frames: Dict[Any, pd.DataFrame]) -> pd.DataFrame:
    """The (field, ticker) panel exactly as ``_run_panel_frames`` builds it."""
    return pd.concat(
        {f: pd.DataFrame({c: df[f] for c, df in frames.items() if f in df.columns})
         for f in PANEL_FIELDS}, axis=1).sort_index()


def _config(start: dt.date, end_excl: dt.date, costs: Costs) -> BacktestConfig:
    return BacktestConfig(
        start_date=dt.datetime.combine(start, dt.time()), end_date=dt.datetime.combine(end_excl, dt.time()),
        initial_capital=INITIAL_CAPITAL, bar_size=BarSize.Days1,
        slippage_bps=costs.slippage_bps, commission_per_share=costs.commission_per_share,
        short_borrow_bps_annual=costs.short_borrow_bps_annual, panel_max_gross=costs.max_gross,
        fill_policy='next_open',
        panel_exit_at_last_print=True)     # D07: a stopped name exits at its last stored print


def _buys_outside(trades, member: pd.DataFrame, index: pd.Index) -> list:
    """BUY fills whose DECISION bar (the bar before the fill) was outside the
    boolean (time x ticker) mask - point-in-time membership or a published
    coverage frame (D05)."""
    idx = pd.DatetimeIndex(index)
    pos = {ts: i for i, ts in enumerate(idx)}
    out = []
    for t in trades:
        if not str(t.action).endswith('BUY'):
            continue
        i = pos.get(pd.Timestamp(t.timestamp))
        if i is None or i == 0:
            continue
        decision = idx[i - 1]
        ok = bool(member.at[decision, t.conid]) if (t.conid in member.columns and decision in member.index) else False
        if not ok:
            out.append((str(pd.Timestamp(t.timestamp).date()), t.conid, float(t.quantity)))
    return out


def _cash_and_frozen(trades, closes: pd.DataFrame, equity: pd.Series, capital: float) -> tuple:
    """(min cash / capital, frozen value at end / final equity, n frozen).
    Cash is rebuilt from the fill log - independently of the backtester's own
    ledger, so it also checks that the D06 guard held (a long-only book must
    never show it below zero); a holding is frozen when it printed no close in
    the window's last 21 bars, so its mark is a stale carry-forward."""
    if not trades or len(equity) == 0:
        return None, None, None
    cash = capital
    qty: Dict[Any, float] = {}
    min_cash = cash
    by_day: Dict[Any, list] = {}
    for t in trades:
        by_day.setdefault(pd.Timestamp(t.timestamp), []).append(t)
    for ts in sorted(by_day):                 # cash is judged at END of day: a rebalance
        for t in by_day[ts]:                  # day's buys and sells settle together
            sgn = 1.0 if str(t.action).endswith('BUY') else -1.0
            cash -= sgn * t.quantity * t.price + t.commission
            qty[t.conid] = qty.get(t.conid, 0.0) + sgn * t.quantity
        min_cash = min(min_cash, cash)
    tail = closes.iloc[-21:]
    frozen_value = 0.0
    n_frozen = 0
    for c, q in qty.items():
        if q <= 1e-9 or c not in closes.columns:
            continue
        if tail[c].notna().any():
            continue
        last = closes[c].dropna()
        if len(last):
            frozen_value += q * float(last.iloc[-1])
            n_frozen += 1
    final_eq = float(equity.iloc[-1])
    return (min_cash / capital), (frozen_value / final_eq if final_eq > 0 else None), n_frozen


def run_window(entry: Dict[str, Any], cell: Dict[str, Any], panel: PricePanel,
               data_start, meas_start, end_excl, costs: Costs,
               coverage: Optional[pd.DataFrame] = None) -> WindowResult:
    """Backtest ``cell`` on ``[data_start, end_excl)`` and report the daily
    returns of ``[meas_start, end_excl)``; the difference is warm-up."""
    window = panel.sliced(data_start, end_excl)
    frames = window.frames()
    if len(frames) < 2:
        raise ValueError(f'{entry["id"]}: fewer than 2 instruments with data in '
                         f'[{data_start}, {end_excl})')
    wide = wide_panel(frames)
    # D05: a received coverage frame is sliced to THIS window's bars before it
    # is installed - never the unsliced full-period frame.
    cov_in = (coverage.reindex(index=wide.index, columns=wide['close'].columns).fillna(False).astype(bool)
              if coverage is not None else None)
    strat = make_strategy(entry, cell, window, coverage=cov_in)
    # The coverage THIS run publishes, derived from THIS window's data by a
    # separate instance (the executing one must enter the backtest unreplayed).
    own_cov = candidate_coverage(make_strategy(entry, cell, window, coverage=cov_in).precompute_panel(wide))
    bt = Backtester(cast(Any, _NoStorage()), _config(to_date(data_start), to_date(end_excl), costs))
    # run_panel is typed for conId keys; the research panels are ticker-keyed
    # (delisted names have no conId - see the PIT universe notes in CLAUDE.md).
    res = bt.run_panel(strat, cast(Any, list(frames.keys())), frames=frames)
    eq = res.equity_curve.astype(float)
    member = window.fields.get('member')
    if member is not None:                     # D05: opens only inside the point-in-time universe
        bad = _buys_outside(res.trades, member, eq.index)
        if bad:
            raise ValueError(f'{entry["id"]}: universe violation - {len(bad)} BUY fill(s) in names '
                             f'outside point-in-time membership on the decision bar, e.g. {bad[:3]}')
    check_cov = own_cov if own_cov is not None else cov_in
    if check_cov is not None:                  # D05: opens only inside the coverage that was published
        bad = _buys_outside(res.trades, check_cov, eq.index)
        if bad:
            raise ValueError(f'{entry["id"]}: coverage violation - {len(bad)} BUY fill(s) in names outside '
                             f'the published coverage frame on the decision bar, e.g. {bad[:3]}')
    rets = eq.pct_change()
    ms = pd.Timestamp(meas_start)
    measured = rets[(rets.index >= ms)].dropna()
    eq_meas = eq[eq.index >= ms]
    notional = float(sum(abs(t.quantity) * t.price for t in res.trades
                         if pd.Timestamp(t.timestamp) >= ms))
    years = max(len(measured) / 252.0, 1e-9)
    turnover = (notional / max(float(eq_meas.mean()), 1e-9) / years) if len(eq_meas) else None
    adv_med, n_held = _holdings_capacity(res.trades, window.fields.get('adv_usd_21d'), ms, eq.index)
    min_cash, frozen, n_frozen = _cash_and_frozen(res.trades, window.close, eq, INITIAL_CAPITAL)
    diag = getattr(res, 'diagnostics', None) or {}      # D06: how often the cash guard acted
    return WindowResult(returns=measured, equity=eq_meas, n_trades=len(res.trades),
                        turnover_per_year=turnover, cell=dict(cell),
                        median_holding_adv_usd=adv_med, median_names_held=n_held,
                        min_cash_fraction=min_cash, frozen_value_fraction_end=frozen,
                        n_frozen_holdings_end=n_frozen,
                        scaled_buy_fills=diag.get('panel_scaled_buy_fills'),
                        scaled_rebalances=diag.get('panel_scaled_rebalances'),
                        forced_exits=diag.get('panel_forced_exits'),
                        forced_exit_value_fraction=(float(diag['panel_forced_exit_value']) / INITIAL_CAPITAL
                                                    if diag.get('panel_forced_exit_value') is not None else None),
                        coverage=own_cov)


def _weights_equal(a: Optional[Dict[Any, float]], b: Optional[Dict[Any, float]], tol: float = 1e-9) -> bool:
    """Exact instruction semantics: None (hold) != {} (flatten) != {x: 0.0}
    (explicit flat), and an OMITTED key is not an explicit 0 - the panel
    executor treats them differently, so the checker must too."""
    if a is None or b is None:
        return a is None and b is None
    if set(a) != set(b):
        return False
    for k in a:
        x, y = float(a[k]), float(b[k])
        if not (abs(x - y) <= tol or (np.isnan(x) and np.isnan(y))):
            return False
    return True


def _values_equal(full, trunc, rows: int, tol: float = 1e-9) -> bool:
    """Do the first ``rows`` rows of a precomputed value agree? Frames/Series
    aligned with the panel and 1-d arrays of panel length are compared
    element-wise (NaN == NaN); scalars by equality; anything else is not
    comparable and is skipped."""
    if isinstance(full, (pd.DataFrame, pd.Series)) and isinstance(trunc, (pd.DataFrame, pd.Series)):
        a, b = full.iloc[:rows], trunc.iloc[:rows]
        if a.shape != b.shape:
            return False
        try:
            x = a.to_numpy(dtype=float), b.to_numpy(dtype=float)
        except (TypeError, ValueError):
            return bool(a.astype(object).equals(b.astype(object)))
        return bool(np.all((np.abs(x[0] - x[1]) <= tol) | (np.isnan(x[0]) & np.isnan(x[1]))))
    if isinstance(full, np.ndarray) and isinstance(trunc, np.ndarray) and full.ndim >= 1:
        a, b = full[:rows], trunc[:rows]
        if a.shape != b.shape:
            return False
        if a.dtype.kind in 'fiub' and b.dtype.kind in 'fiub':
            x, y = a.astype(float), b.astype(float)
            return bool(np.all((np.abs(x - y) <= tol) | (np.isnan(x) & np.isnan(y))))
        return bool(np.array_equal(a, b))
    if isinstance(full, (int, float, str, bool, type(None))) and isinstance(trunc, (int, float, str, bool, type(None))):
        return full == trunc or (isinstance(full, float) and isinstance(trunc, float) and np.isnan(full) and np.isnan(trunc))
    return True


def sampled_prefixes(n: int, windows: int = PROBE_WINDOWS, window_len: int = PROBE_WINDOW_LEN,
                     edge: int = PROBE_EDGE) -> List[int]:
    """Prefix end-indices ``i`` (0 <= i <= n-2, the indices the executor asks
    ``on_panel`` about) that ``lookahead_check`` replays with fresh state: the
    first and last ``edge`` of them, plus ``windows`` runs of ``window_len``
    consecutive indices spread over the panel. Every index when the panel is
    short enough for that to be cheap; never empty for n >= 2."""
    last = n - 2
    if last < 0:
        return []
    picks = set(range(0, min(edge, last + 1)))
    picks |= set(range(max(0, last - edge + 1), last + 1))
    for k in range(windows):
        centre = int(round((k + 1) * (last + 1) / (windows + 1)))
        start = max(0, min(centre - window_len // 2, last - window_len + 1))
        picks |= set(range(start, min(start + window_len, last + 1)))
    return sorted(picks)


def lookahead_check(entry: Dict[str, Any], cell: Dict[str, Any], panel: PricePanel,
                    coverage: Optional[pd.DataFrame] = None,
                    prefixes: Optional[List[int]] = None) -> Dict[str, Any]:
    """Refuse a strategy that reads forward, BEFORE any metric is computed.
    Returns the full-panel precompute state (never replayed), so the caller
    can read a published ``coverage`` frame without recomputing it.

    (1) ``assert_no_panel_lookahead``: hiding the last bars must not change any
        past precomputed value.
    (2) A REFERENCE run replays the strategy exactly as the executor does: one
        ``precompute_panel`` on the full panel, then ``on_panel`` at every index
        in order, sharing one state - so an honest STATEFUL strategy (one that
        counts its own rebalances in ``state``) is replayed on its own terms.
    (3) For every sampled prefix ``i`` (``sampled_prefixes``: boundaries plus
        runs of consecutive indices, or the caller's list) a FRESH instance is
        built on the panel truncated to bars <= i, ``precompute_panel`` is run
        on that truncated panel (its values over those bars must equal the
        full-panel values) and ``on_panel`` is replayed sequentially over
        0..i: every instruction must equal the reference run's under exact
        key semantics (``_weights_equal``). Nothing is skipped where the full
        run returned None - a t+1-derived state that SUPPRESSES an
        instruction on full data and emits one on the honest prefix fails
        here, as does a direct ``iloc[index + 1]`` read (IndexError).
    A benchmark is checked in the form it is executed: ``coverage`` installs
    the candidate's frame first. There is no short-panel bypass.
    """
    frames = panel.frames()
    if len(frames) < 2:
        raise ValueError('lookahead check needs at least two instruments')
    wide = wide_panel(frames)
    idx = wide.index
    n = len(wide)
    if n < 2:
        raise ValueError('lookahead check needs at least two bars')

    def fresh(upto: Optional[int] = None) -> Strategy:
        if upto is None:
            return make_strategy(entry, cell, panel, coverage=coverage)
        sub = panel.sliced(idx[0], idx[upto] + pd.Timedelta(days=1))
        cov = coverage.loc[coverage.index <= idx[upto]] if coverage is not None else None
        return make_strategy(entry, cell, sub, coverage=cov)

    assert_no_panel_lookahead(fresh(), wide)                       # probe (1)
    snapshot = fresh().precompute_panel(wide)                      # pre-replay reference values
    if not isinstance(snapshot, dict):
        raise TypeError(f'{entry["id"]}: precompute_panel must return a dict, got {type(snapshot).__name__}')
    ref = fresh()                                                  # probe (2): the executor's sequence
    ref_state = ref.precompute_panel(wide)
    ref_w: List[Optional[Dict[Any, float]]] = [ref.on_panel(wide, ref_state, i) for i in range(n - 1)]

    picks = sorted(set(prefixes)) if prefixes is not None else sampled_prefixes(n)
    for i in picks:                                                # probe (3)
        if not 0 <= i <= n - 2:
            raise ValueError(f'prefix index {i} outside 0..{n - 2}')
        trunc = wide.iloc[:i + 1]
        s_i = fresh(i)
        try:
            st = s_i.precompute_panel(trunc)
        except (IndexError, KeyError) as exc:
            raise AssertionError(f'LOOKAHEAD: precompute_panel on bars <= {idx[i].date()} failed: '
                                 f'{type(exc).__name__}: {exc}') from exc
        if set(st) != set(snapshot):
            raise AssertionError(f'LOOKAHEAD: precompute state keys differ on bars <= {idx[i].date()}: '
                                 f'{sorted(set(st) ^ set(snapshot))}')
        for k in st:
            if not _values_equal(snapshot[k], st[k], i + 1):
                raise AssertionError(f'LOOKAHEAD: precomputed {k!r} over bars <= {idx[i].date()} changes when '
                                     f'later bars are hidden')
        for j in range(i + 1):
            try:
                w = s_i.on_panel(trunc, st, j)
            except (IndexError, KeyError) as exc:
                raise AssertionError(f'LOOKAHEAD: on_panel at index {j} ({idx[j].date()}) failed once bars '
                                     f'after {idx[i].date()} were hidden: {type(exc).__name__}: {exc}') from exc
            if not _weights_equal(w, ref_w[j]):
                raise AssertionError(f'LOOKAHEAD: the instruction at {idx[j].date()} (index {j}) differs when '
                                     f'bars after {idx[i].date()} (index {i}) are hidden - it depends on them')
    return snapshot
