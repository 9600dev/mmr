"""Deterministic synthetic 1-minute OHLCV fixtures for backtester tests.

Shared by the backtester result-pin tests and the backtest profiling
harness. Everything is seeded, so the same arguments always produce the
byte-identical frame — which is what lets a test pin a backtest's exact
trade list and equity curve.

The frames mimic what ``DuckDBDataStore.read`` hands the backtester: a UTC
``DatetimeIndex`` named ``date`` and the raw ``tick_data`` columns
(``open/high/low/close/volume/average/bar_count/bar_size/what_to_show``).
"""
from __future__ import annotations

import hashlib
from typing import Dict, Iterable, Optional

import numpy as np
import pandas as pd


def make_minute_bars(
    n_days: int = 62,
    seed: int = 7,
    start: str = '2024-01-02',
    session_start: str = '08:00',
    session_end: str = '17:30',
    base_price: float = 100.0,
    drop_frac: float = 0.0,
    duplicate_every: int = 0,
) -> pd.DataFrame:
    """Build ``n_days`` weekdays of 1-min bars between session_start/end ET.

    The default (62 days x 570 bars) is ~35k bars — the size of a 90-day
    1-min backtest. Prices follow a random walk with per-day drift regimes
    and occasional volume spikes so breakout strategies actually trade.

    Args:
        n_days: Number of weekday sessions.
        seed: RNG seed (determinism).
        start: First calendar date (weekends are skipped).
        session_start: First bar time, America/New_York.
        session_end: Last bar time (exclusive), America/New_York.
        base_price: Starting price.
        drop_frac: Fraction of bars randomly removed (gappy data).
        duplicate_every: If > 0, every Nth bar is duplicated with a
            different OHLC (same timestamp) — the ugly-vendor-data shape.

    Returns:
        DataFrame indexed by UTC ``date`` with tick_data columns.
    """
    rng = np.random.default_rng(seed)
    days = pd.bdate_range(start=start, periods=n_days)
    stamps = []
    for d in days:
        day = d.strftime('%Y-%m-%d')
        rng_idx = pd.date_range(f'{day} {session_start}', f'{day} {session_end}',
                                freq='1min', inclusive='left', tz='America/New_York')
        stamps.append(rng_idx)
    idx = stamps[0].append(stamps[1:]) if len(stamps) > 1 else stamps[0]
    n = len(idx)
    # Per-day drift regime: some days trend up, some down, some chop.
    day_codes = pd.factorize(idx.date)[0]
    drift_by_day = rng.choice([-0.02, 0.0, 0.0, 0.025], size=n_days)
    drift = drift_by_day[day_codes]
    rets = drift + rng.normal(0.0, 0.08, n)
    close = base_price + np.cumsum(rets)
    close = np.maximum(close, 1.0)
    open_ = np.concatenate([[close[0]], close[:-1]]) + rng.normal(0, 0.02, n)
    high = np.maximum(open_, close) + np.abs(rng.normal(0, 0.05, n))
    low = np.minimum(open_, close) - np.abs(rng.normal(0, 0.05, n))
    volume = rng.integers(500, 2000, n).astype(float)
    spikes = rng.random(n) < 0.08
    volume[spikes] *= rng.uniform(2.0, 5.0, spikes.sum())
    df = pd.DataFrame({
        'open': open_, 'high': high, 'low': low, 'close': close,
        'volume': volume,
        'average': (high + low + close) / 3.0,
        'bar_count': rng.integers(10, 200, n),
        'bar_size': '1 min',
        'what_to_show': 1,
    }, index=idx.tz_convert('UTC'))
    df.index.name = 'date'
    if drop_frac > 0:
        keep = rng.random(n) >= drop_frac
        df = df[keep]
    if duplicate_every > 0:
        dup = df.iloc[::duplicate_every].copy()
        dup['open'] = dup['open'] + 0.03
        dup['close'] = dup['close'] - 0.04
        dup['high'] = np.maximum(dup['high'], dup[['open', 'close']].max(axis=1))
        dup['low'] = np.minimum(dup['low'], dup[['open', 'close']].min(axis=1))
        df = pd.concat([df, dup]).sort_index(kind='stable')
    return df


class _FrameTickData:
    def __init__(self, frames: Dict[int, pd.DataFrame]):
        self._frames = frames

    def read(self, contract, date_range=None) -> pd.DataFrame:
        conid = int(contract)
        df = self._frames.get(conid)
        if df is None:
            return pd.DataFrame()
        return df.copy()


class FrameStorage:
    """Minimal ``TickStorage`` stand-in serving in-memory frames by conid."""

    def __init__(self, frames: Dict[int, pd.DataFrame]):
        self._frames = frames

    def get_tickdata(self, bar_size):
        return _FrameTickData(self._frames)


def _f(x) -> str:
    """Exact float rendering (repr round-trips binary64)."""
    return repr(float(x))


def result_fingerprint(result) -> Dict[str, object]:
    """Canonical, exact fingerprint of a BacktestResult.

    Every float is rendered with ``repr`` (round-trip exact), so two results
    hash equal only if trades, signals and every equity point match bit for
    bit.
    """
    trades = [
        '|'.join([str(t.timestamp), str(int(t.conid)), str(t.action), _f(t.quantity),
                  _f(t.price), _f(t.commission), _f(t.signal_probability), _f(t.signal_risk)])
        for t in result.trades
    ]
    signals = [
        '|'.join([str(s.timestamp), str(s.strategy_name), str(int(s.conid)), str(s.action),
                  _f(s.signal_probability), _f(s.signal_risk)])
        for s in result.signals
    ]
    eq = result.equity_curve
    equity = [f'{ts}|{_f(v)}' for ts, v in zip(eq.index, eq.to_numpy())]
    metrics = '|'.join(_f(getattr(result, k)) for k in (
        'total_return', 'sharpe_ratio', 'max_drawdown', 'win_rate', 'sortino_ratio',
        'calmar_ratio', 'profit_factor', 'expectancy_bps', 'time_in_market_pct'))

    def h(lines: Iterable[str]) -> str:
        return hashlib.sha256('\n'.join(lines).encode()).hexdigest()

    return {
        'n_trades': len(trades),
        'n_signals': len(signals),
        'n_equity': len(equity),
        'trades_sha': h(trades),
        'signals_sha': h(signals),
        'equity_sha': h(equity),
        'metrics': metrics,
        'final_equity': _f(eq.iloc[-1]) if len(eq) else None,
    }
