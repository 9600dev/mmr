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


def driftless_session_bars(
    days: int = 250,
    seed: int = 0,
    start_price: float = 100.0,
    annual_vol: float = 0.32,
    annual_drift: float = 0.0,
    session_tz: str = 'America/New_York',
    open_minute: int = 9 * 60 + 30,
    close_minute: int = 16 * 60,
    start_date: str = '2025-07-01',
) -> pd.DataFrame:
    """A year of 1-minute bars containing, by construction, NOTHING to find.

    This is the NEGATIVE CONTROL instrument. Prices follow a driftless random
    walk: every minute's log return is drawn i.i.d. from a zero-mean Gaussian,
    so no rule computable from bars 0..t carries any information about bar t+1.
    Overnight gaps are drawn the same way. A strategy that scores well on this
    has not found an edge, because there is no edge here to find — it has
    measured its own search.

    Why a control is needed at all: the whole pipeline (sweep, rank by
    composite score, keep the top cells) is a machine for finding the best of N
    draws, and the best of N draws from noise looks good. Reading a backtest
    without knowing what the same machine reports on nothing is like reading a
    scale without knowing its zero.

    What is deliberately REAL here — session boundaries, the 09:30-16:00
    trading day, weekday-only dates, plausible per-minute volatility — is
    structure the strategies need in order to run at all. An opening-range
    breakout requires an opening range to exist. What is deliberately ABSENT
    is any relationship between one bar and the next beyond the random walk
    itself: no drift, no mean reversion, no volatility clustering, no
    intraday U-shape. Adding those would make the data more realistic and the
    control weaker, because then a strategy could profit from a real pattern
    and the result would no longer be unambiguously manufactured.

    ``annual_vol`` defaults to 32%, roughly a liquid US large-cap.

    ``annual_drift`` defaults to 0 — the pure null. Set it to the realised
    market return over the comparison window to answer a SHARPER question. A
    long-biased strategy on real equities earns the market's rise for free, so
    a zero-drift control understates what a no-skill strategy would have made
    and flatters the real result by the whole equity risk premium. With drift
    matched, anything the real data still shows above the control is not beta.
    Drift is added to the mean of the SAME i.i.d. process, so it introduces a
    trend without introducing any predictability: no rule computable from bars
    0..t forecasts bar t+1 any better than the constant.
    """
    rng = np.random.default_rng(seed)

    sessions = pd.bdate_range(start=start_date, periods=days, tz=session_tz)
    per_session = close_minute - open_minute
    # Per-minute sigma from the annualised figure: 252 sessions of
    # `per_session` minutes each.
    sigma = annual_vol / np.sqrt(252.0 * per_session)

    index_parts = []
    for day in sessions:
        base = day.normalize() + pd.Timedelta(minutes=open_minute)
        index_parts.append(pd.date_range(base, periods=per_session, freq='1min'))
    index = pd.DatetimeIndex(np.concatenate([p.values for p in index_parts]))
    index = pd.DatetimeIndex(index).tz_localize('UTC').tz_convert(session_tz) \
        if index.tz is None else index
    n = len(index)

    # Close-to-close log returns, zero mean. Overnight boundaries get the same
    # distribution scaled up a little, which is the only concession to realism
    # and cannot create predictability (it is still zero-mean and independent).
    mu = annual_drift / (252.0 * per_session)
    steps = rng.normal(mu, sigma, n)
    session_start = np.zeros(n, dtype=bool)
    session_start[::per_session] = True
    steps[session_start] *= 3.0
    close = start_price * np.exp(np.cumsum(steps))

    # Open is the prior close (or the first close for bar 0). High/low straddle
    # the open-close body by a nonnegative random amount, so every bar is
    # structurally coherent by construction — `bar_quality.impossible_mask`
    # must accept all of them, and the write path enforces that.
    open_ = np.empty(n)
    open_[0] = start_price
    open_[1:] = close[:-1]
    body_hi = np.maximum(open_, close)
    body_lo = np.minimum(open_, close)
    wick = np.abs(rng.normal(0.0, sigma, n)) * close
    high = body_hi + wick
    low = np.maximum(body_lo - np.abs(rng.normal(0.0, sigma, n)) * close, 1e-6)

    volume = rng.lognormal(mean=7.0, sigma=0.7, size=n).round()

    df = pd.DataFrame({
        'open': open_, 'high': high, 'low': low, 'close': close,
        'volume': volume,
        'average': (high + low + close) / 3.0,
        'bar_count': rng.integers(5, 200, n).astype(float),
    }, index=index)
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
