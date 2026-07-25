"""Pure exit-class decision — the trust boundary, extracted so it can be verified.

``Trader.order_reduces_exposure`` is the single predicate that decides whether an
order is EXIT-CLASS. A True answer exempts the order from the trading filter, the
leverage check, the risk gate, ``require_proposal_approval`` AND the approver
notional tier. It is therefore the highest-consequence boolean in the system: a
predicate that wrongly answers True turns every one of those gates off at once.

WHY THIS MODULE EXISTS
    The decision used to live inline in ``trading_runtime`` (a ~2000-line module
    outside the mutation scope), and the invariants spec exercised it only
    through a ``MagicMock(return_value=True)``. The properties therefore proved
    "given the classifier says exit, the gates do not refuse" and never "the
    classifier is right". A targeted backdoor — ``if qty > 1000: return True``,
    which permits unlimited naked shorts through every gate — passed all 41
    invariants tests and all 1882 suite tests.

    Splitting the *decision* from the *position lookup* fixes that: the decision
    is now pure (no self, no I/O), so it is contract-checked by ``deal`` at
    runtime, symbolically checked by CrossHair, mutation-tested, and pinned by a
    real Hypothesis property in ``tests/invariants/test_exit_class.py`` — instead
    of being the one thing the whole toolchain could not see.

SEMANTICS (deliberate, and NOT what a size-clamped reading would assume)
    Direction-aware, NOT size-clamped: a SELL against ANY net-long is exit-class
    whatever the quantity, so an oversized "flip" that crosses zero is still
    exit-class. You cannot INCREASE a long by selling, and refusing an exit is
    worse than any limit it could breach. The flip's net-new opening remainder is
    a documented residual (see docs/SAFETY_ROADMAP.md), closed by turnover caps
    or order-splitting — never by refusing the reduction. Callers that must not
    OVERSELL (protective orders) clamp the quantity separately.

    FAIL CLOSED (return False, i.e. "treat as an open and gate it") for anything
    unreadable: no position, a flat position, a non-BUY/SELL action, or a
    non-positive/non-finite quantity.
"""

import math

import deal


@deal.has()  # side-effect free: no I/O, no global mutation
@deal.pure
def reduces_exposure(action: str, held: float, quantity: float) -> bool:
    """True iff ``action quantity`` reduces a live signed position of ``held``.

    ``held`` is the SIGNED broker position for the instrument: > 0 long,
    < 0 short, 0.0 flat. ``quantity`` is the absolute order size.

    The result is deliberately INDEPENDENT of ``quantity`` (beyond requiring it
    to be a sane positive number) — that independence is the property that makes
    a size-triggered backdoor impossible to hide here.
    """
    try:
        qty = float(quantity)
        position = float(held)
    except (TypeError, ValueError):
        return False
    if not math.isfinite(qty) or qty <= 0:
        return False
    if not math.isfinite(position) or position == 0.0:
        return False
    act = str(action).strip().upper()
    if act == 'SELL':
        return position > 0
    if act == 'BUY':
        return position < 0
    return False
