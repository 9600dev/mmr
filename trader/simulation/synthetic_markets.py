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
    """The original nasty-shape frames, keyed by name.

    NOTE what these are NOT: every intraday frame here starts 09:30 **UTC**
    (04:30 New York) and is at most 240 bars long, so none of them contains a
    single regular-session bar. A strategy that only acts inside its session
    sees nothing on them — see ``session_battery`` for frames it can act on.
    """
    return {
        "with_gaps": ohlcv_with_gaps(),
        "high_volatility": ohlcv_high_volatility(),
        "zero_volume": ohlcv_zero_volume(),
        "halted": ohlcv_halted(),
        "nan_rows": ohlcv_nan_rows(),
        "trending": ohlcv_trending(),
        "choppy": ohlcv_choppy(),
    }


# ---------------------------------------------------------------------------
# Session-stamped frames
# ---------------------------------------------------------------------------
#
# The frames above were written to break indicator maths, and for that the
# timestamp did not matter. It matters for a session-gated strategy: an
# opening-range breakout needs an opening range, and a frame stamped 04:30-08:29
# New York has none, so every one of its arrays is NaN and every check on it
# passes by having nothing to look at (docs/AUDIT_2026-11-14.md B4.3).
#
# These generators produce consecutive WEEKDAY sessions of 1-minute bars whose
# wall-clock times are the caller's session (default 09:30-16:00
# America/New_York), on the module's usual UTC index. Unlike
# ``driftless_session_bars`` they are not a null: they carry trends, gaps,
# reversion and volume bursts on purpose, because their job is to make a
# strategy DO something so that what it does can be inspected.

SESSION_TZ = 'America/New_York'
SESSION_OPEN_MINUTE = 9 * 60 + 30
SESSION_CLOSE_MINUTE = 16 * 60

# A Wednesday in January: five sessions then span a weekend (Wed, Thu, Fri,
# Mon, Tue), and no zone changes its clocks inside the window.
_SESSION_START_DATE = '2024-01-03'


def _session_grid(days: int, session_tz: str, open_minute: int, close_minute: int,
                  pre_minutes: int, post_minutes: int, start_date: str):
    """``(index, session_no, since_open)`` for ``days`` weekday sessions.

    ``since_open`` is each bar's minute offset from its session open: negative
    before the open, ``>= close_minute - open_minute`` after the close.
    """
    if days < 1:
        raise ValueError(f'days must be >= 1, got {days}')
    if not 0 <= open_minute < close_minute <= 24 * 60:
        raise ValueError(
            f'session minutes must satisfy 0 <= open < close <= 1440, got '
            f'open={open_minute} close={close_minute}')
    # Extended hours never spill past the session's own calendar day.
    pre = min(max(int(pre_minutes), 0), open_minute)
    post = min(max(int(post_minutes), 0), 24 * 60 - close_minute)
    per_day = pre + (close_minute - open_minute) + post

    # Built as naive wall-clock minutes and localized once, so 09:30 means
    # 09:30 in the session's zone whatever its UTC offset is that day.
    wall = pd.DatetimeIndex(np.concatenate([
        pd.date_range(day + pd.Timedelta(minutes=open_minute - pre),
                      periods=per_day, freq='1min').to_numpy()
        for day in pd.bdate_range(start=start_date, periods=days)
    ]))
    index = wall.tz_localize(session_tz).tz_convert('UTC')
    since_open = np.tile(np.arange(-pre, per_day - pre), days)
    session_no = np.repeat(np.arange(days), per_day)
    return index, session_no, since_open


def _coherent_ohlcv(index, close: np.ndarray, volume: np.ndarray, wick: np.ndarray,
                    rng, start_price: float) -> pd.DataFrame:
    """OHLCV whose every bar is structurally possible: open is the prior
    close, and high/low straddle the open-close body."""
    n = len(close)
    open_ = np.empty(n)
    open_[0] = start_price
    open_[1:] = close[:-1]
    high = np.maximum(open_, close) + np.abs(rng.normal(0.0, 1.0, n)) * wick * close
    low = np.minimum(open_, close) - np.abs(rng.normal(0.0, 1.0, n)) * wick * close
    return _finish(pd.DataFrame({
        'open': open_, 'high': high, 'low': np.maximum(low, 1e-6),
        'close': close, 'volume': volume,
    }, index=index))


def _session_volume(rng, since_open: np.ndarray, session_len: int,
                    steps: np.ndarray, sigma: float) -> np.ndarray:
    """U-shaped intraday volume that bursts on large moves (so a volume-
    confirmed breakout can exist), thin and often zero outside the session."""
    in_session = (since_open >= 0) & (since_open < session_len)
    frac = np.clip(since_open / max(session_len - 1, 1), 0.0, 1.0)
    profile = 0.6 + 1.6 * (2.0 * frac - 1.0) ** 2
    burst = 1.0 + np.minimum(np.abs(steps) / sigma, 4.0)
    volume = 4000.0 * profile * burst * rng.lognormal(0.0, 0.5, len(steps))
    thin = rng.integers(0, 400, len(steps)).astype(float)
    thin[rng.random(len(steps)) < 0.5] = 0.0
    return np.where(in_session, volume, thin).round()


def session_trending(days: int = 5, seed: int = 303, *,
                     session_tz: str = SESSION_TZ,
                     open_minute: int = SESSION_OPEN_MINUTE,
                     close_minute: int = SESSION_CLOSE_MINUTE,
                     pre_minutes: int = 0, post_minutes: int = 0,
                     start_date: str = _SESSION_START_DATE) -> pd.DataFrame:
    """Sessions that each gap at the open and then trend all day, so the
    price leaves its opening range — in both directions and by different
    amounts: two modest up days, a down day, then a large up day and a large
    down day (repeating). The uneven sizes are deliberate: a "range
    expansion" or "trend day" rule compares today with the days before it and
    never fires when every day looks alike. The session-stamped counterpart
    of ``ohlcv_trending``."""
    rng = np.random.default_rng(seed)
    index, session_no, since_open = _session_grid(
        days, session_tz, open_minute, close_minute, pre_minutes, post_minutes, start_date)
    session_len = close_minute - open_minute
    in_session = (since_open >= 0) & (since_open < session_len)
    sigma = 0.0006
    # Each session's total drift, as a log move from open to close.
    session_move = np.array([0.010, 0.006, -0.010, 0.035, -0.025])[session_no % 5]

    steps = rng.normal(0.0, sigma, len(index))
    steps = np.where(in_session, steps + session_move / session_len, steps * 0.3)
    steps[since_open == 0] += np.sign(session_move[since_open == 0]) * 0.004
    close = 100.0 * np.exp(np.cumsum(steps))
    volume = _session_volume(rng, since_open, session_len, steps, sigma)
    wick = np.where(in_session, sigma, sigma * 0.3)
    return _coherent_ohlcv(index, close, volume, wick, rng, 100.0)


def session_choppy(days: int = 5, seed: int = 404, *,
                   session_tz: str = SESSION_TZ,
                   open_minute: int = SESSION_OPEN_MINUTE,
                   close_minute: int = SESSION_CLOSE_MINUTE,
                   pre_minutes: int = 0, post_minutes: int = 0,
                   start_date: str = _SESSION_START_DATE) -> pd.DataFrame:
    """Rangebound sessions: each opens away from an oscillating anchor
    (alternately below and above it) and is pulled back, crossing it
    repeatedly. The session-stamped counterpart of ``ohlcv_choppy``."""
    rng = np.random.default_rng(seed)
    index, session_no, since_open = _session_grid(
        days, session_tz, open_minute, close_minute, pre_minutes, post_minutes, start_date)
    session_len = close_minute - open_minute
    in_session = (since_open >= 0) & (since_open < session_len)
    sigma = 0.0009
    n = len(index)

    noise = rng.normal(0.0, sigma, n)
    anchor = 0.004 * np.sin(2.0 * np.pi * np.clip(since_open, 0, session_len) / 120.0)
    log_price = np.empty(n)
    level = 0.0
    for i in range(n):
        if since_open[i] == 0:
            # Open 0.6% away from the anchor, alternating sides by session.
            level = anchor[i] + (-0.006 if session_no[i] % 2 == 0 else 0.006)
        elif in_session[i]:
            level += 0.03 * (anchor[i] - level) + noise[i]
        else:
            level += 0.3 * noise[i]
        log_price[i] = level
    steps = np.diff(log_price, prepend=0.0)
    close = 100.0 * np.exp(log_price)
    volume = _session_volume(rng, since_open, session_len, steps, sigma)
    wick = np.where(in_session, sigma, sigma * 0.3)
    return _coherent_ohlcv(index, close, volume, wick, rng, 100.0)


def session_extended(days: int = 3, seed: int = 505, *,
                     session_tz: str = SESSION_TZ,
                     open_minute: int = SESSION_OPEN_MINUTE,
                     close_minute: int = SESSION_CLOSE_MINUTE,
                     start_date: str = _SESSION_START_DATE) -> pd.DataFrame:
    """Choppy sessions WITH extended hours: two hours of pre-market and one
    of post-market around each, thin and often zero-volume. This is the shape
    the live feed and the stored history actually have, and the only frame
    here on which an in-session mask is not constant."""
    return session_choppy(days, seed, session_tz=session_tz, open_minute=open_minute,
                          close_minute=close_minute, pre_minutes=120, post_minutes=60,
                          start_date=start_date)


def session_halts(days: int = 3, seed: int = 606, *,
                  session_tz: str = SESSION_TZ,
                  open_minute: int = SESSION_OPEN_MINUTE,
                  close_minute: int = SESSION_CLOSE_MINUTE,
                  start_date: str = _SESSION_START_DATE) -> pd.DataFrame:
    """The nasty shapes of ``battery()``, placed INSIDE sessions where a
    session-gated strategy will actually meet them: a mid-session run of
    all-NaN rows, a stretch stuck at one price on zero volume, and trailing
    all-NaN rows on the newest bars."""
    df = session_trending(days, seed, session_tz=session_tz, open_minute=open_minute,
                          close_minute=close_minute, start_date=start_date)
    session_len = close_minute - open_minute
    if session_len < 30 or len(df) < 60:
        return df
    # First session: a 10% slice of the session goes missing.
    a = session_len // 4
    df.iloc[a:a + max(session_len // 10, 3)] = np.nan
    # Second session (or the same one when there is only one): price stuck.
    b = (session_len if days > 1 else 0) + session_len // 2
    c = b + max(session_len // 10, 3)
    stuck = float(df['close'].iloc[b - 1])
    df.iloc[b:c, df.columns.get_indexer(['open', 'high', 'low', 'close'])] = stuck
    df.iloc[b:c, df.columns.get_loc('volume')] = 0.0
    df.iloc[-5:] = np.nan
    return df


def daily_trending(n: int = 400, seed: int = 707) -> pd.DataFrame:
    """Business-day bars with persistent trends that reverse twice (up, down,
    up), long enough for a 200-day or 12-month indicator to warm up."""
    rng = np.random.default_rng(seed)
    index = pd.bdate_range(start='2022-01-03', periods=n, tz='UTC')
    sigma = 0.012
    phase = np.arange(n) * 3 // max(n, 1)
    steps = rng.normal(0.0, sigma, n) + np.where(phase == 1, -0.0025, 0.0025)
    close = 100.0 * np.exp(np.cumsum(steps))
    volume = (2_000_000.0 * (1.0 + np.minimum(np.abs(steps) / sigma, 4.0))
              * rng.lognormal(0.0, 0.4, n)).round()
    return _coherent_ohlcv(index, close, volume, np.full(n, sigma * 0.5), rng, 100.0)


def daily_choppy(n: int = 400, seed: int = 808) -> pd.DataFrame:
    """Business-day bars oscillating around a flat level — no drift to ride."""
    rng = np.random.default_rng(seed)
    index = pd.bdate_range(start='2022-01-03', periods=n, tz='UTC')
    sigma = 0.012
    log_price = (0.06 * np.sin(np.arange(n) * 2.0 * np.pi / 40.0)
                 + rng.normal(0.0, sigma, n))
    steps = np.diff(log_price, prepend=0.0)
    close = 100.0 * np.exp(log_price)
    volume = (2_000_000.0 * (1.0 + np.minimum(np.abs(steps) / sigma, 4.0))
              * rng.lognormal(0.0, 0.4, n)).round()
    return _coherent_ohlcv(index, close, volume, np.full(n, sigma * 0.5), rng, 100.0)


def session_battery(session_tz: str = SESSION_TZ,
                    open_minute: int = SESSION_OPEN_MINUTE,
                    close_minute: int = SESSION_CLOSE_MINUTE) -> Dict[str, pd.DataFrame]:
    """Session-stamped 1-minute frames for a strategy that trades
    ``open_minute``-``close_minute`` in ``session_tz``."""
    return {
        name: generator(session_tz=session_tz, open_minute=open_minute,
                        close_minute=close_minute)
        for name, generator in (
            ("session_trending", session_trending),
            ("session_choppy", session_choppy),
            ("session_extended", session_extended),
            ("session_halts", session_halts),
        )
    }


def daily_battery() -> Dict[str, pd.DataFrame]:
    """Daily frames long enough for slow indicators (``battery()`` has one
    daily frame, 60 bars)."""
    return {
        "daily_trending": daily_trending(),
        "daily_choppy": daily_choppy(),
    }
