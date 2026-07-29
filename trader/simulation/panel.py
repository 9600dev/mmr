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
@deal.pre(lambda ranks, held, enter_pct, exit_pct:
          0.0 < enter_pct <= 1.0 and enter_pct <= exit_pct <= 1.0)
def buffered_membership(
    ranks: Mapping[int, float],
    held: frozenset,
    enter_pct: float,
    exit_pct: float,
) -> frozenset:
    """Which names the book should hold, with a buffer against boundary churn.

    A decile book re-sorts its universe every period, and a name sitting near
    the tenth-percentile line crosses it constantly - generating a full entry
    and a full exit each time for a signal that never really changed. Measured
    on this repo: a weight-level no-trade band moved turnover from 60.3x to
    59.0x, i.e. barely at all, because the turnover was never in the weights.
    It was in MEMBERSHIP. Almost every trade was a whole position opening or
    closing.

    So the buffer goes on membership instead. A name must rank in the top
    ``enter_pct`` to be bought, but is only sold once it falls out of the top
    ``exit_pct``. With 0.10 and 0.25 a name entering at the 9th percentile has
    to decay all the way to the 25th before the book pays to leave, instead of
    being traded twice a week while it hovers.

    ``ranks`` maps conid to percentile rank in [0, 1], where 1.0 is the most
    attractive. ``held`` is what the book currently owns. The asymmetry is the
    whole mechanism: ``exit_pct >= enter_pct`` is a precondition, because the
    reverse would eject names faster than it admitted them and churn MORE.
    """
    keep: set = set()
    for conid, r in ranks.items():
        if r is None or not math.isfinite(r):
            continue
        if r >= 1.0 - enter_pct:
            keep.add(conid)                   # clears the entry bar
        elif conid in held and r >= 1.0 - exit_pct:
            keep.add(conid)                   # inside the buffer: hold, do not pay
    return frozenset(keep)


@deal.pure
@deal.pre(lambda current, target, band, prices=None: band >= 0.0)
def apply_no_trade_band(
    current: Mapping[int, float],
    target: Mapping[int, float],
    band: float,
    prices: Optional[Mapping[int, float]] = None,
) -> Dict[int, float]:
    """Drop rebalances too small to be worth their cost.

    A cross-sectional book re-sorts its whole universe every period, and most
    of the resulting trades are tiny adjustments to positions it is keeping.
    Measured on this repo: adding a second signal took turnover from 14x to
    62x a year and cost 4.7pp of return to capture a signal worth 0.52 Sharpe
    gross - we banked 0.19. The trades were not wrong; most of them were not
    worth making.

    ``band`` is the minimum change, as a FRACTION of the target position, that
    justifies trading. A name whose target moved less than that keeps its
    current position. Entries and exits are never suppressed: going from zero
    to a position, or to zero from one, is a decision rather than a drift, and
    banding it would leave the book unable to act on its own signal.

    ``prices`` is optional and, when given, makes the band notional rather
    than share-count based - the right basis, since cost scales with dollars
    traded and not with share counts. Without it the comparison is on shares,
    which is a reasonable approximation only when prices are similar.
    """
    out: Dict[int, float] = {}
    for conid, tgt in target.items():
        cur = current.get(conid, 0.0)
        if not (math.isfinite(tgt) and math.isfinite(cur)):
            continue
        # Never band an entry or an exit.
        if cur == 0.0 or tgt == 0.0 or (cur > 0) != (tgt > 0):
            out[conid] = tgt
            continue
        delta = abs(tgt - cur)
        scale = abs(tgt)
        if prices is not None:
            px = prices.get(conid)
            if px is not None and math.isfinite(px) and px > 0:
                delta *= px
                scale *= px
        if scale > 0 and (delta / scale) < band:
            out[conid] = cur          # hold: the move is not worth its cost
        else:
            out[conid] = tgt
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
