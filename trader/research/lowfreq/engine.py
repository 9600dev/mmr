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
from typing import Any, Dict, Optional, cast

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
                  'scaled_rebalances': self.scaled_rebalances})
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
        fill_policy='next_open')


def _non_member_buys(trades, member: pd.DataFrame, index: pd.Index) -> list:
    """BUY fills whose DECISION bar (the bar before the fill) was outside the
    point-in-time membership - a universe violation under D05."""
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
    strat = make_strategy(entry, cell, window, coverage=coverage)
    bt = Backtester(cast(Any, _NoStorage()), _config(to_date(data_start), to_date(end_excl), costs))
    # run_panel is typed for conId keys; the research panels are ticker-keyed
    # (delisted names have no conId - see the PIT universe notes in CLAUDE.md).
    res = bt.run_panel(strat, cast(Any, list(frames.keys())), frames=frames)
    eq = res.equity_curve.astype(float)
    member = window.fields.get('member')
    if member is not None:                     # D05: opens only inside the point-in-time universe
        bad = _non_member_buys(res.trades, member, eq.index)
        if bad:
            raise ValueError(f'{entry["id"]}: universe violation - {len(bad)} BUY fill(s) in names '
                             f'outside point-in-time membership on the decision bar, e.g. {bad[:3]}')
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
                        scaled_rebalances=diag.get('panel_scaled_rebalances'))


def lookahead_check(entry: Dict[str, Any], cell: Dict[str, Any], panel: PricePanel,
                    probe_indices: int = 3) -> Dict[str, Any]:
    """Refuse a strategy that reads forward, BEFORE any metric is computed.
    Returns the full-panel precompute state (so the caller can read a
    published ``coverage`` frame without recomputing it).

    Two probes. (1) ``assert_no_panel_lookahead``: hiding the last bars must
    not change any past precomputed value. (2) ``on_panel`` at a few indices
    must return the same weights whether or not bars after ``index`` exist -
    which catches a strategy that indexes ``panel.iloc[index + 1]`` directly,
    something the precompute probe cannot see.
    """
    frames = panel.frames()
    if len(frames) < 2:
        raise ValueError('lookahead check needs at least two instruments')
    wide = wide_panel(frames)
    strat = make_strategy(entry, cell, panel)
    assert_no_panel_lookahead(strat, wide)
    n = len(wide)
    full_state = strat.precompute_panel(wide)
    if n < 8:
        return full_state
    for i in np.linspace(n // 2, n - 3, probe_indices).astype(int):
        i = int(i)
        full_w = make_strategy(entry, cell, panel).on_panel(wide, full_state, i)
        trunc = wide.iloc[:i + 1]
        trunc_strat = make_strategy(entry, cell, panel.sliced(wide.index[0], wide.index[i] + pd.Timedelta(days=1)))
        try:
            trunc_w = trunc_strat.on_panel(trunc, trunc_strat.precompute_panel(trunc), i)
        except (IndexError, KeyError) as exc:
            raise AssertionError(f'LOOKAHEAD: on_panel at index {i} failed without future bars: {exc}') from exc
        if (full_w is None) != (trunc_w is None):
            raise AssertionError(f'LOOKAHEAD: on_panel at index {i} returned an instruction only when future bars were present')
        if full_w is not None and trunc_w is not None:
            keys = set(full_w) | set(trunc_w)
            for k in keys:
                a, b = float(full_w.get(k, 0.0) or 0.0), float(trunc_w.get(k, 0.0) or 0.0)
                if not (abs(a - b) <= 1e-9 or (np.isnan(a) and np.isnan(b))):
                    raise AssertionError(f'LOOKAHEAD: on_panel weight for {k!r} at index {i} depends on bars after {i} ({a} vs {b})')
    return full_state
