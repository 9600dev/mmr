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
from trader.research.lowfreq.loaders import PricePanel
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

    def metrics(self) -> Dict[str, Any]:
        from trader.research.lowfreq import stats
        m = stats.summarise(self.returns)
        m.update({'n_trades': self.n_trades, 'turnover_per_year': self.turnover_per_year})
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


def make_strategy(entry: Dict[str, Any], cell: Dict[str, Any], panel: PricePanel) -> Strategy:
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
    return inst


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


def run_window(entry: Dict[str, Any], cell: Dict[str, Any], panel: PricePanel,
               data_start, meas_start, end_excl, costs: Costs) -> WindowResult:
    """Backtest ``cell`` on ``[data_start, end_excl)`` and report the daily
    returns of ``[meas_start, end_excl)``; the difference is warm-up."""
    window = panel.sliced(data_start, end_excl)
    frames = window.frames()
    if len(frames) < 2:
        raise ValueError(f'{entry["id"]}: fewer than 2 instruments with data in '
                         f'[{data_start}, {end_excl})')
    strat = make_strategy(entry, cell, window)
    bt = Backtester(cast(Any, _NoStorage()), _config(pd.Timestamp(data_start).date(),
                                                      pd.Timestamp(end_excl).date(), costs))
    res = bt.run_panel(strat, list(frames.keys()), frames=frames)
    eq = res.equity_curve.astype(float)
    rets = eq.pct_change()
    ms = pd.Timestamp(meas_start)
    measured = rets[(rets.index >= ms)].dropna()
    eq_meas = eq[eq.index >= ms]
    notional = float(sum(abs(t.quantity) * t.price for t in res.trades
                         if pd.Timestamp(t.timestamp) >= ms))
    years = max(len(measured) / 252.0, 1e-9)
    turnover = (notional / max(float(eq_meas.mean()), 1e-9) / years) if len(eq_meas) else None
    return WindowResult(returns=measured, equity=eq_meas, n_trades=len(res.trades),
                        turnover_per_year=turnover, cell=dict(cell))


def lookahead_check(entry: Dict[str, Any], cell: Dict[str, Any], panel: PricePanel,
                    probe_indices: int = 3) -> None:
    """Refuse a strategy that reads forward, BEFORE any metric is computed.

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
    if n < 8:
        return
    full_state = strat.precompute_panel(wide)
    for i in np.linspace(n // 2, n - 3, probe_indices).astype(int):
        i = int(i)
        full_w = make_strategy(entry, cell, panel).on_panel(wide, full_state, i)
        trunc = wide.iloc[:i + 1]
        trunc_strat = make_strategy(entry, cell, panel.sliced(panel.close.index[0], panel.close.index[i] + pd.Timedelta(days=1)))
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
