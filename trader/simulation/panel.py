"""Cross-sectional (panel) backtesting — the pure kernel.

WHY THIS EXISTS
    Every strategy in this codebase is time-series: it looks at ONE instrument
    and asks "is this going up". The backtester enforces that shape — it
    dispatches per-conid, handing the strategy a single instrument's frame, so
    nothing can see all instruments at a moment and compare them.

    That restriction is what capped the 2026-07 research at 20-44 trades per
    ten-month walk-forward. At 44 trades you can only detect a per-trade Sharpe
    above 0.30, which is enormous — nothing that large survives in liquid US
    large-caps. The experiments could not have found a realistic edge even if
    one existed.

    A cross-sectional strategy ranks N instruments each period and holds the
    extremes. With 500 names over 10 years that is ~1.25M instrument-days and
    ~2,500 rebalances instead of 44 trades, and each portfolio observation
    averages away idiosyncratic noise. It is a different question ("which of
    these is cheap RELATIVE to the others") answered with orders of magnitude
    more evidence.

WHAT IS PURE HERE
    The translation from target WEIGHTS to target SHARE COUNTS, and from
    target share counts to orders. Both are arithmetic with sharp edges —
    fractional shares, unpriceable instruments, weights that sum past 1 — and
    getting them wrong silently changes leverage rather than raising. They are
    contracted and property-tested for that reason; the orchestration that
    calls them lives in the Backtester.
"""

from __future__ import annotations

import math

from typing import Dict, Mapping, Optional

import deal


@deal.pure
@deal.pre(lambda weights, equity, prices, allow_fractional=False:
          equity >= 0.0)
@deal.ensure(
    lambda _: all(isinstance(q, float) for q in _.result.values()),
    message='quantities must be numeric')
def target_positions(
    weights: Mapping[int, float],
    equity: float,
    prices: Mapping[int, float],
    allow_fractional: bool = False,
) -> Dict[int, float]:
    """Turn target portfolio weights into target share counts.

    A conid with no usable price is OMITTED, not zeroed. Those are different
    instructions: zero means "hold nothing", omission means "this instrument
    could not be valued". Treating the second as the first would silently
    liquidate a position because a single price was missing — the most
    expensive kind of no-op.

    Shares are truncated TOWARD ZERO by default so a long is never rounded up
    into more exposure than the weight asked for and a short is never rounded
    more negative. Rounding to nearest would breach the weight in whichever
    direction the fraction fell, which over 500 names compounds into leverage
    nobody chose.
    """
    out: Dict[int, float] = {}
    if not (math.isfinite(equity) and equity > 0):
        return out
    for conid, w in weights.items():
        if w is None or not math.isfinite(w) or w == 0.0:
            out[conid] = 0.0
            continue
        px = prices.get(conid)
        if px is None or not math.isfinite(px) or px <= 0:
            continue                      # unpriceable: no instruction, not zero
        raw = (w * equity) / px
        out[conid] = raw if allow_fractional else float(math.trunc(raw))
    return out


@deal.pure
def rebalance_orders(
    current: Mapping[int, float],
    target: Mapping[int, float],
    min_shares: float = 0.0,
) -> Dict[int, float]:
    """Signed share deltas to move from ``current`` to ``target``.

    Only conids named in ``target`` are considered, plus any currently-held
    conid that ``target`` explicitly sets. A held position the strategy did
    not mention is LEFT ALONE — see `target_positions`: a missing entry means
    "no instruction", and a rebalancer that liquidates everything it was not
    told about turns one unpriceable instrument into a portfolio-wide sell.

    ``min_shares`` suppresses dust trades. Below the threshold the delta is
    dropped rather than rounded up, so a churn floor cannot itself become a
    source of turnover.
    """
    out: Dict[int, float] = {}
    for conid, tgt in target.items():
        cur = current.get(conid, 0.0)
        if not math.isfinite(tgt) or not math.isfinite(cur):
            continue
        delta = tgt - cur
        if abs(delta) <= min_shares:
            continue
        out[conid] = delta
    return out


@deal.pure
@deal.ensure(
    lambda _: _.result is None or _.result >= 0.0,
    message='gross exposure is a magnitude')
def gross_exposure(weights: Mapping[int, float]) -> Optional[float]:
    """Sum of |weight|. 1.0 is fully invested; 2.0 is 2x levered.

    Returned rather than enforced, because what counts as too much is a policy
    question and this module does not make policy. The caller that funds the
    trade does.
    """
    total = 0.0
    for w in weights.values():
        if w is None or not math.isfinite(w):
            return None
        total += abs(w)
    return total


@deal.pure
def normalise_weights(
    weights: Mapping[int, float],
    max_gross: float = 1.0,
) -> Dict[int, float]:
    """Scale weights down so gross exposure does not exceed ``max_gross``.

    Scales DOWN only. A book that is 40% invested is a deliberate choice —
    perhaps the signal only fired on a few names — and scaling it up to
    fill the budget would invent conviction the strategy never expressed.
    """
    clean = {c: w for c, w in weights.items()
             if w is not None and math.isfinite(w)}
    gross = gross_exposure(clean)
    if gross is None or gross <= 0 or not (math.isfinite(max_gross)
                                           and max_gross > 0):
        return dict(clean)
    if gross <= max_gross:
        return dict(clean)
    scale = max_gross / gross
    return {c: w * scale for c, w in clean.items()}
