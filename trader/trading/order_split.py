"""Pure decomposition of a position-flipping order into its two real parts.

THE PROBLEM THIS CLOSES
    Exit-class classification is direction-aware and NOT size-clamped: a SELL
    against ANY net-long is an exit, whatever the quantity. That is deliberate,
    because the alternative (clamping the test) makes an oversized close
    REFUSABLE, and refusing a reduction is worse than any limit the refusal
    would enforce.

    The cost is the documented "flip residual". With 3 shares held, ``SELL 5``
    is labelled an exit, so all five shares pass every gate: the trading
    filter, the leverage check, the risk gate, the approval requirement and the
    approver notional tier. Three of them close a position. The other two open
    a SHORT that nothing checked. Confirmed live 2026-07-27: the order was
    accepted and submitted with no refusal from anything.

THE FIX, AND WHY IT IS NOT "REFUSE THE ORDER"
    The order was always two economically different things wearing one label.
    Split it and each part gets the treatment it deserves:

        held +3, SELL 5   ->   reduce 3 (exit-class, ungated, never refusable)
                             + open   2 (new short exposure, fully gated)

    The reduction is never blocked, which preserves the rule that matters. The
    new exposure faces every check, which closes the hole. Nothing here can
    refuse a close; the worst case is that the OPENING half is refused and the
    caller ends up flat instead of short, which is the safe direction.

WHAT THIS FUNCTION IS NOT
    It does not decide whether the opening half is allowed. It decides only how
    many shares belong to each half. The gates decide the rest, which keeps
    this function pure and total.
"""

import math

from typing import NamedTuple

import deal

from trader.trading.exit_class import reduces_exposure


class SplitPlan(NamedTuple):
    """How one requested order divides into reduction and new exposure."""
    reduce_qty: float      # shares that close existing position (exit-class)
    open_qty: float        # shares that create new exposure (gated)

    @property
    def is_flip(self) -> bool:
        """True when the order crosses zero, so both halves are non-empty."""
        return self.reduce_qty > 0 and self.open_qty > 0


def _as_finite(x) -> float:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return 0.0
    return v if math.isfinite(v) else 0.0


@deal.has()  # side-effect free: no I/O, no global mutation
@deal.pure
@deal.ensure(
    lambda _: _.result.reduce_qty >= 0.0 and _.result.open_qty >= 0.0,
    message='a split half cannot be negative')
@deal.ensure(
    lambda _: (_.result.reduce_qty + _.result.open_qty
               == _as_finite(_.quantity) if _as_finite(_.quantity) > 0 else True),
    message='the split lost or invented shares — halves must sum to the request')
@deal.ensure(
    lambda _: _.result.reduce_qty <= abs(_as_finite(_.held)),
    message='the reduction half exceeds the position it claims to reduce')
def split_order(action: str, held: float, quantity: float) -> SplitPlan:
    """Divide ``quantity`` into the part that reduces ``held`` and the part
    that opens new exposure.

    ``held`` is the SIGNED broker position: > 0 long, < 0 short, 0 flat.

    Three shapes, and only the third is a flip:

    * not a reduction at all (wrong direction, or flat) -> all opening
    * a reduction that fits inside the position          -> all reduction
    * a reduction LARGER than the position               -> both halves

    Degenerate inputs collapse to ``(0, 0)``: a non-positive or non-finite
    quantity is not an order, and the structural check refuses it upstream.
    """
    qty = _as_finite(quantity)
    if qty <= 0:
        return SplitPlan(0.0, 0.0)

    position = _as_finite(held)
    if not reduces_exposure(action, position, qty):
        # Wrong direction or flat: every share is new exposure. This includes
        # the case the gates already handled correctly before this module
        # existed, so the behaviour there is unchanged.
        return SplitPlan(0.0, qty)

    available = abs(position)
    if qty <= available:
        return SplitPlan(qty, 0.0)

    return SplitPlan(available, qty - available)
