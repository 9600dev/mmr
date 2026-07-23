"""Synthetic OHLCV generators — deterministic "nasty market" frames.

These began life as pytest fixtures in ``tests/conftest.py`` (the edge-case
shapes used to harden indicators / position sizing against realistic-ugly
data). They're lifted here so non-test code — the strategy gauntlet in
particular — can run the same battery. The conftest fixtures now delegate
to these functions, so the frames are byte-identical to what the existing
test-suite has always seen (same seeds, same shapes).

Every frame follows the project's standard OHLCV convention: columns
``open, high, low, close, volume``, a UTC ``DatetimeIndex`` named ``date``.
All generators are seeded — two calls return identical data.
"""

from typing import Dict

import numpy as np
import pandas as pd


def _finish(df: pd.DataFrame) -> pd.DataFrame:
    df.index.name = "date"
    return df


def ohlcv_with_gaps() -> pd.DataFrame:
    """OHLCV with NaN rows (simulates market halts / future-date API padding)."""
    n = 50
    rng = np.random.default_rng(7)
    close = 100 + np.cumsum(rng.normal(0, 0.3, n))
    dates = pd.date_range("2024-01-02 09:30", periods=n, freq="1min", tz="UTC")
    df = pd.DataFrame({
        "open": close,
        "high": close + 0.2,
        "low": close - 0.2,
        "close": close,
        "volume": rng.integers(100, 10000, n).astype(float),
    }, index=dates)
    # Inject gaps: rows 10-12 and 30 are NaN
    df.iloc[10:13] = np.nan
    df.iloc[30] = np.nan
    return _finish(df)


def ohlcv_high_volatility() -> pd.DataFrame:
    """High-ATR synthetic OHLCV — tests position sizing / volatility adjustment."""
    n = 60
    rng = np.random.default_rng(13)
    # Daily swings of ~5% — large ATR
    close = 100 + np.cumsum(rng.normal(0, 5.0, n))
    dates = pd.date_range("2024-01-02", periods=n, freq="1D", tz="UTC")
    df = pd.DataFrame({
        "open": close,
        "high": close + np.abs(rng.normal(3, 1, n)),
        "low": close - np.abs(rng.normal(3, 1, n)),
        "close": close,
        "volume": rng.integers(1_000_000, 10_000_000, n).astype(float),
    }, index=dates)
    return _finish(df)


def ohlcv_zero_volume() -> pd.DataFrame:
    """OHLCV with zero-volume bars — simulates pre/post-market or illiquid
    periods that must not divide-by-zero or get flagged as opportunities."""
    n = 40
    rng = np.random.default_rng(23)
    close = 100 + np.cumsum(rng.normal(0, 0.1, n))
    dates = pd.date_range("2024-01-02 09:30", periods=n, freq="1min", tz="UTC")
    volume = rng.integers(1000, 5000, n).astype(float)
    # First 10 and last 5 bars have zero volume (pre/post-market)
    volume[:10] = 0.0
    volume[-5:] = 0.0
    df = pd.DataFrame({
        "open": close, "high": close + 0.05, "low": close - 0.05,
        "close": close, "volume": volume,
    }, index=dates)
    return _finish(df)


def ohlcv_halted() -> pd.DataFrame:
    """ATR-zero scenario: price stuck at a halt price for many bars."""
    n = 30
    dates = pd.date_range("2024-01-02 09:30", periods=n, freq="1min", tz="UTC")
    halt_price = 42.0
    df = pd.DataFrame({
        "open": [halt_price] * n,
        "high": [halt_price] * n,
        "low": [halt_price] * n,
        "close": [halt_price] * n,
        "volume": [0.0] * n,
    }, index=dates)
    return _finish(df)


def ohlcv_nan_rows() -> pd.DataFrame:
    """Trailing all-NaN rows — the Massive/Polygon API pads future dates and
    market holidays with null bars, and a strategy reading ``iloc[-1]``
    without a NaN check trades on garbage. Distinct from ``ohlcv_with_gaps``
    (interior gaps): here the *newest* bars are the broken ones."""
    n = 40
    rng = np.random.default_rng(31)
    close = 100 + np.cumsum(rng.normal(0, 0.2, n))
    dates = pd.date_range("2024-01-02 09:30", periods=n, freq="1min", tz="UTC")
    df = pd.DataFrame({
        "open": close,
        "high": close + 0.1,
        "low": close - 0.1,
        "close": close,
        "volume": rng.integers(100, 5000, n).astype(float),
    }, index=dates)
    df.iloc[-5:] = np.nan
    return _finish(df)


def ohlcv_trending(n: int = 240, seed: int = 101) -> pd.DataFrame:
    """Clean persistent uptrend with mild noise. Long enough (240 bars) for
    typical indicator warm-up, which the lookahead checker needs."""
    rng = np.random.default_rng(seed)
    close = 100 + 0.05 * np.arange(n) + np.cumsum(rng.normal(0, 0.2, n))
    dates = pd.date_range("2024-01-02 09:30", periods=n, freq="1min", tz="UTC")
    df = pd.DataFrame({
        "open": close - rng.uniform(0, 0.1, n),
        "high": close + rng.uniform(0.05, 0.3, n),
        "low": close - rng.uniform(0.05, 0.3, n),
        "close": close,
        "volume": rng.integers(500, 20000, n).astype(float),
    }, index=dates)
    return _finish(df)


def ohlcv_choppy(n: int = 240, seed: int = 202) -> pd.DataFrame:
    """Rangebound oscillation — mean-reverting chop with no exploitable
    drift. The counterpart series for the lookahead checker."""
    rng = np.random.default_rng(seed)
    close = 100 + 2.0 * np.sin(np.arange(n) * 2 * np.pi / 20) + rng.normal(0, 0.15, n)
    dates = pd.date_range("2024-01-02 09:30", periods=n, freq="1min", tz="UTC")
    df = pd.DataFrame({
        "open": close - rng.uniform(0, 0.1, n),
        "high": close + rng.uniform(0.05, 0.3, n),
        "low": close - rng.uniform(0.05, 0.3, n),
        "close": close,
        "volume": rng.integers(500, 20000, n).astype(float),
    }, index=dates)
    return _finish(df)


def battery() -> Dict[str, pd.DataFrame]:
    """Every synthetic frame, keyed by name — the gauntlet's S3 input set."""
    return {
        "with_gaps": ohlcv_with_gaps(),
        "high_volatility": ohlcv_high_volatility(),
        "zero_volume": ohlcv_zero_volume(),
        "halted": ohlcv_halted(),
        "nan_rows": ohlcv_nan_rows(),
        "trending": ohlcv_trending(),
        "choppy": ohlcv_choppy(),
    }
