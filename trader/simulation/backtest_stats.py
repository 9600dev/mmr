"""Statistical-confidence tests for completed backtests.

These answer the question *"is this edge real or noise?"* — beyond what the
headline Sharpe / profit_factor / return numbers can tell you on their own.

Five tests, computed from persisted run data:

1. **Probabilistic Sharpe Ratio** (López de Prado 2012) — probability that
   the true Sharpe of the return process exceeds a benchmark (default 0),
   adjusted for sample size AND the actual skew/kurtosis of the returns.
   This is the single highest-signal "is it real" test — a raw Sharpe of 3
   on 30 bars behaves very differently to the same number on 3000 bars.

2. **t-test of mean per-trade P&L ≠ 0** — the classical frequentist test
   for whether the observed average trade P&L is statistically different
   from zero. p-value < 0.05 means "unlikely to be random walk".
   A "trade" here, and in tests 3–5, is an EXIT: the P&L one closing order
   realized, summed over every lot it closed (``exit_pnls``). Lots closed
   by one exit are not independent observations.

3. **Bootstrap 95% CI on total return and Sharpe** — nonparametric
   resampling (5000 replicates) of the per-trade P&L / bar-return series.
   A strategy that shows "+8% return, CI [-3%, +19%]" is far less
   trustworthy than "+8% return, CI [+5%, +11%]".

4. **P&L distribution shape (skew + excess kurtosis)** — negative skew +
   high kurt is the classic "picking up pennies in front of a steamroller"
   signature that Sharpe misses. Fat tails are a direct warning.

5. **Longest losing streak vs Monte Carlo expectation** — compares the
   realised streak against the 95th percentile of streaks you'd expect by
   randomly reordering the same P&L sequence. Worse-than-random means
   losses cluster (auto-correlated P&L, regime-dependent edge).

All functions are pure; they take numpy arrays (or lists) and return either
a primitive or a typed dataclass. The UI layer in ``mmr_cli`` orchestrates
fetching the persisted trades/equity curve and calling ``compute_all``.

Three different things are called "trades" around a backtest; ``TradeCounts``
names them: FILLS (``BacktestResult.total_trades`` — every BUY and SELL),
FIFO LOTS (``round_trip_pnls``) and EXITS (``exit_pnls`` — closing orders).
They are equal in pairs only for a strategy that never stacks.
"""

import math
from collections import deque
from dataclasses import dataclass
from typing import Callable, List, Optional, Tuple

import numpy as np


@dataclass
class BacktestStats:
    """Bundle of statistical-confidence outputs for a single backtest run.

    Fields are ``Optional`` because each test has its own minimum-sample
    requirement — e.g. PSR is undefined for < 3 observations, bootstrap
    needs ≥ 10 data points to be meaningful, etc. ``None`` means "not
    enough data to compute this", not "zero".
    """
    n_trades: int              # sample size of the per-trade stats: EXITS (== n_exits)
    n_bar_returns: int         # bar-return count used for Sharpe-family stats

    psr: Optional[float]                     # probability, [0, 1]
    t_stat: Optional[float]                  # per-trade P&L, H0: mean = 0
    p_value: Optional[float]                 # two-sided p-value

    return_ci_lo: Optional[float]            # bootstrap CI on mean per-trade P&L ($)
    return_ci_hi: Optional[float]
    return_ci_n_boot: int                    # sample size (for disclosure)

    sharpe_ci_lo: Optional[float]            # bootstrap CI on annualised Sharpe
    sharpe_ci_hi: Optional[float]
    sharpe_ci_n_boot: int

    pnl_skew: Optional[float]                # per-trade P&L skewness
    pnl_excess_kurtosis: Optional[float]     # excess kurtosis (0 = normal)

    losing_streak_actual: Optional[int]
    losing_streak_mc_95: Optional[float]     # 95th pct under random reordering

    # The three counts by name (see ``TradeCounts``). ``n_trades`` above is
    # kept and equals ``n_exits``; until the 2026-11-14 audit (B4.5) it was
    # the lot count, which differs whenever one exit closes several lots.
    n_exits: int = 0           # closing orders that realized P&L
    n_lots: int = 0            # FIFO-matched (BUY fill, SELL fill) pairs
    n_fills: int = 0           # every usable BUY and SELL fill


def round_trip_pnls(trades: List[dict]) -> List[float]:
    """Pair BUY fills against subsequent SELL fills (FIFO per conId) to
    produce the realized P&L of each matched LOT. The per-trade statistics
    do NOT use this series directly — see ``exit_pnls``.

    Unmatched BUYs (still-open positions at end of backtest) are ignored —
    they're unrealized, not round-trips. Partial closes produce one P&L
    entry per matched lot. Commissions are apportioned per-share and
    subtracted from each matched lot's P&L.

    Action strings accept both ``'BUY'``/``'SELL'`` and ``'Action.BUY'``/
    ``'Action.SELL'`` — the persisted JSON uses whichever ``str()`` of the
    Action enum produces at write time.
    """
    return [pnl for pnl, _, _ in round_trip_lots(trades)]


def _usable_fill(t: dict) -> Optional[Tuple[int, float, float, float, bool]]:
    """``(conid, quantity, price, commission, is_buy)`` for a fill the pairing
    can use, else ``None`` (unparseable, non-positive quantity or price, or an
    action that is neither BUY nor SELL). The one rule for "is this a fill"."""
    try:
        conid = int(t.get('conid', 0))
        qty = float(t.get('quantity', 0))
        price = float(t.get('price', 0))
        comm = float(t.get('commission', 0.0))
    except (TypeError, ValueError):
        return None
    if qty <= 0 or price <= 0:
        return None
    action_raw = str(t.get('action', '')).upper()
    if 'BUY' in action_raw:
        return conid, qty, price, comm, True
    if 'SELL' in action_raw:
        return conid, qty, price, comm, False
    return None


def round_trip_lots(trades: List[dict]) -> List[Tuple[float, dict, dict]]:
    """The pairing behind ``round_trip_pnls``, with each lot's provenance.

    One entry per matched lot: ``(realized P&L, the BUY fill that opened it,
    the SELL fill that closed it)`` — the two dicts are the caller's own, so
    any extra key it put on a fill comes back. That is what lets a caller
    measuring a WINDOW of a longer run keep only the lots opened inside it
    without a second copy of the FIFO rule (``Backtester._summarise``).
    """
    lots: dict = {}  # conid -> deque of [qty_remaining, buy_price, buy_comm_per_share, buy fill]
    out: List[Tuple[float, dict, dict]] = []
    for t in sorted(trades, key=lambda x: x.get('timestamp', '')):
        fill = _usable_fill(t)
        if fill is None:
            continue
        conid, qty, price, comm, is_buy = fill
        comm_per_share = comm / qty
        if is_buy:
            lots.setdefault(conid, deque()).append([qty, price, comm_per_share, t])
        else:
            remaining = qty
            while remaining > 0 and lots.get(conid):
                lot = lots[conid][0]
                matched = min(lot[0], remaining)
                realized = (
                    matched * (price - lot[1])
                    - matched * lot[2]       # buy-side commission
                    - matched * comm_per_share   # sell-side commission
                )
                out.append((realized, lot[3], t))
                lot[0] -= matched
                remaining -= matched
                if lot[0] <= 1e-12:
                    lots[conid].popleft()
    return out


def _exit_pnls(lots: List[Tuple[float, dict, dict]]) -> List[float]:
    """Sum matched-lot P&Ls per closing order, in order of first appearance.

    A closing order is identified from the fills themselves: the SELL's
    instrument and timestamp. The backtester emits one SELL fill per exit;
    partial fills of one closing order share both and are one exit. A SELL
    without a timestamp is its own exit (nothing says it belongs with another).
    """
    totals: dict = {}
    for pnl, _, closed in lots:
        stamp = closed.get('timestamp')
        key = (closed.get('conid'), stamp) if stamp not in (None, '') else id(closed)
        totals[key] = totals.get(key, 0.0) + pnl
    return list(totals.values())


def exit_pnls(trades: List[dict]) -> List[float]:
    """Realized P&L per EXIT — the unit of every per-trade statistic here.

    ``round_trip_pnls`` yields one P&L per FIFO lot. When a position was
    built from several BUYs (pyramiding) one SELL closes several lots, and
    those P&Ls share the exit price and most of the holding period: they are
    one outcome observed several times, not several outcomes. Fed to a t-test
    as independent draws they shrink the standard error by about sqrt(lots per
    exit) — on driftless random walks a three-lot stack was "significant at
    p < 0.05" in 22% of runs, a five-lot stack in 36% (audit 2026-11-14,
    B4.5). Summed per closing order the rate is the nominal 5%.

    For a strategy that never stacks (one BUY, one SELL) this is exactly
    ``round_trip_pnls``.
    """
    return _exit_pnls(round_trip_lots(trades))


@dataclass(frozen=True)
class TradeCounts:
    """The three things a backtest calls "trades", by their own names."""
    n_fills: int    # every usable BUY and SELL fill; a pyramid add is a fill.
    #                 ``BacktestResult.total_trades`` of a per-instrument run.
    n_lots: int     # FIFO-matched (BUY fill, SELL fill) pairs: ``round_trip_pnls``.
    n_exits: int    # closing orders that realized P&L: ``exit_pnls``. The
    #                 sample size of the per-trade tests, and what win rate /
    #                 profit factor / expectancy are counted over.


def trade_counts(trades: Optional[List[dict]]) -> TradeCounts:
    """Fills, FIFO lots and exits of a trade log (``None`` / empty = zeros)."""
    if not trades:
        return TradeCounts(0, 0, 0)
    lots = round_trip_lots(trades)
    return TradeCounts(
        n_fills=sum(1 for t in trades if _usable_fill(t) is not None),
        n_lots=len(lots),
        n_exits=len(_exit_pnls(lots)),
    )


def probabilistic_sharpe(
    returns: np.ndarray,
    benchmark_sharpe: float = 0.0,
) -> Optional[float]:
    """Probabilistic Sharpe Ratio (López de Prado 2012).

    Probability that the **true** Sharpe of the process generating
    ``returns`` exceeds ``benchmark_sharpe``, given the observed sample
    size and higher moments:

        PSR(SR*) = Φ[ (SR - SR*) · √(n-1) /
                      √(1 - γ₃·SR + (γ₄ - 1)/4 · SR²) ]

    Where γ₃ is skew and γ₄ is (non-excess) kurtosis. Using the observed
    skew/kurt in the denominator corrects the classical-normal Sharpe
    t-test for fat tails and asymmetry — otherwise you get spuriously
    confident PSRs on real return series.

    Returns ``None`` when n < 3 (higher moments undefined) or 0.5 when
    the sample has zero variance (no information to go on).
    """
    from scipy.stats import norm, skew as _skew, kurtosis as _kurt

    r = np.asarray(returns, dtype=float)
    r = r[np.isfinite(r)]
    n = len(r)
    if n < 3:
        return None

    sd = r.std(ddof=1)
    if sd == 0.0:
        return 0.5
    sr_hat = r.mean() / sd
    gamma3 = float(_skew(r, bias=False))
    # López de Prado's formula uses **non-excess** kurtosis (normal = 3).
    gamma4 = float(_kurt(r, fisher=False, bias=False))

    # Clamp denominator to avoid sqrt of near-zero / negative.
    var_term = max(
        1e-12,
        1.0 - gamma3 * sr_hat + (gamma4 - 1.0) * sr_hat ** 2 / 4.0,
    )
    z = (sr_hat - benchmark_sharpe) * math.sqrt(n - 1) / math.sqrt(var_term)
    return float(norm.cdf(z))


def t_test_mean_zero(values: np.ndarray) -> Tuple[Optional[float], Optional[float]]:
    """Two-sided one-sample t-test of H₀: mean(values) == 0.

    Returns ``(t_stat, p_value)``; ``(None, None)`` if n < 2 or zero
    variance (test is undefined).
    """
    from scipy.stats import t

    v = np.asarray(values, dtype=float)
    v = v[np.isfinite(v)]
    n = len(v)
    if n < 2:
        return None, None
    sd = v.std(ddof=1)
    if sd == 0.0:
        return None, None
    t_stat = v.mean() / (sd / math.sqrt(n))
    p_value = 2.0 * (1.0 - t.cdf(abs(t_stat), df=n - 1))
    return float(t_stat), float(p_value)


def bootstrap_ci(
    values: np.ndarray,
    statistic: Callable[[np.ndarray], float],
    n_boot: int = 5000,
    alpha: float = 0.05,
    seed: int = 42,
) -> Optional[Tuple[float, float]]:
    """Nonparametric percentile-bootstrap CI.

    Resamples ``values`` with replacement ``n_boot`` times, applies
    ``statistic`` to each resample, and returns the
    (``alpha/2``, ``1-alpha/2``) quantiles of the bootstrap distribution.

    Returns ``None`` for n < 10 — bootstrap on tiny samples is
    misleading (the resamples are nearly identical to the original).
    """
    v = np.asarray(values, dtype=float)
    v = v[np.isfinite(v)]
    n = len(v)
    if n < 10:
        return None
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, n, size=(n_boot, n))
    resamples = v[idx]
    # Vectorise where we can; fall back to row-by-row for custom stats.
    try:
        stats = statistic(resamples)  # type: ignore[arg-type]
        stats = np.asarray(stats, dtype=float)
        if stats.shape != (n_boot,):
            raise ValueError('stat returned wrong shape for vectorised path')
    except Exception:
        stats = np.array([statistic(row) for row in resamples], dtype=float)
    stats = stats[np.isfinite(stats)]
    if len(stats) == 0:
        return None
    return (
        float(np.quantile(stats, alpha / 2)),
        float(np.quantile(stats, 1 - alpha / 2)),
    )


def _longest_negative_run(pnls: np.ndarray) -> int:
    """Length of the longest consecutive run of strictly-negative values."""
    longest = 0
    current = 0
    for x in pnls:
        if x < 0:
            current += 1
            if current > longest:
                longest = current
        else:
            current = 0
    return longest


def longest_losing_streak(pnls) -> int:
    """Convenience alias — accepts list or array."""
    return _longest_negative_run(np.asarray(pnls, dtype=float))


def mc_losing_streak_p95(
    pnls: np.ndarray,
    n_sim: int = 2000,
    seed: int = 42,
) -> Optional[float]:
    """95th-percentile longest-losing-streak under random reordering.

    Answers: given the *same set* of per-trade P&Ls (preserving win-rate
    and magnitudes) but in a random order, what streak length would you
    see ~5% of the time?

    If the actual streak substantially exceeds this MC 95%, losses
    *cluster* — a red flag for auto-correlated returns or regime-
    dependent edge that won't survive the next regime shift.
    """
    v = np.asarray(pnls, dtype=float)
    v = v[np.isfinite(v)]
    if len(v) < 5:
        return None
    rng = np.random.default_rng(seed)
    streaks = np.empty(n_sim, dtype=int)
    for i in range(n_sim):
        streaks[i] = _longest_negative_run(rng.permutation(v))
    return float(np.quantile(streaks, 0.95))


def compute_all(
    trades: Optional[List[dict]],
    bar_returns: Optional[np.ndarray],
    bars_per_year: float = 252.0,
) -> BacktestStats:
    """Orchestrator — computes the full stats bundle from whatever data
    is available. Missing inputs produce ``None`` on the affected fields;
    callers should render those as "—" or "insufficient data".

    ``bars_per_year`` is used only for annualising the bootstrap Sharpe CI
    so it's on the same scale as the run's headline ``sharpe_ratio``. For
    raw (per-period) CIs, pass ``1.0``.
    """
    # --- per-trade P&L series: one per EXIT, not one per FIFO lot. Lots
    # closed by one exit share its price and most of the holding period;
    # treating them as independent draws inflated every test below.
    lots = round_trip_lots(trades) if trades else []
    pnls_list = _exit_pnls(lots)
    pnls = np.array(pnls_list, dtype=float) if pnls_list else np.array([])
    n_trades = len(pnls)
    n_fills = sum(1 for t in trades if _usable_fill(t) is not None) if trades else 0

    # --- per-bar return series ---
    if bar_returns is not None:
        bar_r = np.asarray(bar_returns, dtype=float)
        bar_r = bar_r[np.isfinite(bar_r)]
    else:
        bar_r = np.array([])
    n_bar = len(bar_r)

    # PSR on bar returns (definitional basis)
    psr = probabilistic_sharpe(bar_r) if n_bar >= 3 else None

    # t-test on per-trade P&L when available (directly testable trade
    # edge), else fall back to bar returns so users see *some* signal.
    if n_trades >= 2:
        t_stat, p_value = t_test_mean_zero(pnls)
    elif n_bar >= 2:
        t_stat, p_value = t_test_mean_zero(bar_r)
    else:
        t_stat, p_value = None, None

    # Bootstrap CIs
    return_ci = bootstrap_ci(pnls, lambda s: np.mean(s, axis=-1)) if n_trades >= 10 else None

    ann_factor = math.sqrt(bars_per_year)

    def _sharpe_vec(s: np.ndarray) -> np.ndarray:
        # Vectorised Sharpe on bootstrap rows — shape (n_boot, n).
        mu = s.mean(axis=-1)
        sd = s.std(axis=-1, ddof=1)
        with np.errstate(divide='ignore', invalid='ignore'):
            sr = np.where(sd > 0, mu / sd, 0.0) * ann_factor
        return sr

    sharpe_ci = bootstrap_ci(bar_r, _sharpe_vec) if n_bar >= 30 else None

    # Distribution shape (per-trade P&L)
    if n_trades >= 3:
        from scipy.stats import skew as _skew, kurtosis as _kurt
        pnl_skew = float(_skew(pnls, bias=False))
        pnl_kurt = float(_kurt(pnls, fisher=True, bias=False))  # excess
    else:
        pnl_skew, pnl_kurt = None, None

    # Losing streak vs MC
    if n_trades >= 5:
        losing_actual = longest_losing_streak(pnls)
        losing_mc_95 = mc_losing_streak_p95(pnls)
    else:
        losing_actual, losing_mc_95 = None, None

    return BacktestStats(
        n_trades=n_trades,
        n_bar_returns=n_bar,
        psr=psr,
        t_stat=t_stat,
        p_value=p_value,
        return_ci_lo=return_ci[0] if return_ci else None,
        return_ci_hi=return_ci[1] if return_ci else None,
        return_ci_n_boot=n_trades,
        sharpe_ci_lo=sharpe_ci[0] if sharpe_ci else None,
        sharpe_ci_hi=sharpe_ci[1] if sharpe_ci else None,
        sharpe_ci_n_boot=n_bar,
        pnl_skew=pnl_skew,
        pnl_excess_kurtosis=pnl_kurt,
        losing_streak_actual=losing_actual,
        losing_streak_mc_95=losing_mc_95,
        n_exits=n_trades,
        n_lots=len(lots),
        n_fills=n_fills,
    )
