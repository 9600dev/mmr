"""Pure structural validation of an order — the last thing between us and IB.

WHY THIS MODULE EXISTS
    ``OrderValidator._check_order`` already held this decision, and held it
    correctly. The problem was WHERE it was wired: ``TradeExecutioner.place_order``
    runs it under ``ExecutorCondition.SANITY_CHECK``, and the only caller that
    reaches ``place_order`` is ``place_order_simple`` — the manual ``mmr buy`` /
    ``mmr sell`` path. Every AUTOMATED order (propose → approve, the
    AutoExecutor's opens and closes, every bracket leg, the protective stop)
    goes through ``place_expressive_order`` → ``subscribe_place_order_direct``
    and never saw it.

    So the structurally-sane check guarded the path with a human watching, and
    not the path that trades unattended. This module is that decision, extracted
    pure so it can be contracted, symbolically checked, mutation-tested and
    pinned — and then wired at the CHOKEPOINT, which every order path shares.

    What could reach IB before that wiring: a LIMIT order with
    ``limit_price=0.0`` (``ExecutionSpec.validate`` only rejects ``None``), a
    STOP with a zero stop price, or any quantity ``place_expressive_order``
    was handed — it takes ``quantity: float`` from the client and never checked
    it was positive or finite. The server-side notional tier exists precisely
    because the server must not trust client-supplied numbers; this closes the
    same hole for the structural ones.

SCOPE — deliberately conservative
    It rejects only orders that are DEFINITELY malformed: a non-positive or
    non-finite quantity, a bad action, a priced order type missing its price.
    It does NOT second-guess price LEVELS — a wide limit is a legitimate order
    and blocking it would cost real fills. TRAIL orders are checked for action
    and quantity only; their trailing parameters are not modelled here.

ON REFUSING EXITS
    The standing rule is that an exit is never refused, and this check applies
    to exits too. That is not a contradiction: a structurally malformed order is
    not a working exit. A SELL of NaN shares, or a stop with no stop price, does
    not reduce a position — IB rejects it and the operator gets a cryptic error
    from the far side of the wire. Refusing it here, loudly and by name, is
    strictly better than sending garbage and calling it protection.
"""

import math

from typing import Optional

import deal


# Order types that carry a limit price / a stop price, per IB's ``orderType``.
_NEEDS_LIMIT = ('LMT', 'STP LMT')
_NEEDS_STOP = ('STP', 'STP LMT')


def _finite_positive(x) -> bool:
    """True iff *x* coerces to a finite, strictly positive float."""
    try:
        v = float(x)
    except (TypeError, ValueError):
        return False
    return math.isfinite(v) and v > 0


def _normalize(text) -> str:
    return str(text or '').strip().upper()


@deal.has()  # side-effect free: no I/O, no global mutation
@deal.pure
# A reason must be non-empty. Call sites branch on `if reason is not None`, but a
# falsy reason would still print as nothing and read as "no problem" in a log.
@deal.ensure(
    lambda _: _.result is None or len(_.result) > 0,
    message='a rejection reason must never be the empty string')
# The acceptance conditions, stated as contracts rather than left implicit in the
# control flow. Each says: accepting IMPLIES this held. A mutant that loosens the
# body into accepting something malformed violates one of these at runtime.
@deal.ensure(
    lambda _: _.result is not None or _normalize(_.action) in ('BUY', 'SELL'),
    message='accepted an order whose action is neither BUY nor SELL')
@deal.ensure(
    lambda _: _.result is not None or _finite_positive(_.quantity),
    message='accepted an order with a non-positive or non-finite quantity')
@deal.ensure(
    lambda _: (_.result is not None
               or _normalize(_.order_type) not in _NEEDS_LIMIT
               or _finite_positive(_.limit_price)),
    message='accepted a limit-priced order type with no usable limit price')
@deal.ensure(
    lambda _: (_.result is not None
               or _normalize(_.order_type) not in _NEEDS_STOP
               or _finite_positive(_.stop_price)),
    message='accepted a stop-priced order type with no usable stop price')
def structural_rejection(
    action: str,
    quantity: float,
    order_type: str,
    limit_price: float,
    stop_price: float,
) -> Optional[str]:
    """Return why this order is structurally malformed, or None if it is sane.

    ``limit_price`` / ``stop_price`` are only consulted for the order types that
    require them, so a MARKET order may pass any values (ib_async leaves them at
    ``UNSET_DOUBLE``, which is finite and positive and must not be interpreted).
    """
    act = _normalize(action)
    if act not in ('BUY', 'SELL'):
        return f'invalid action {act!r}'

    if not _finite_positive(quantity):
        return f'non-positive or non-finite quantity {quantity!r}'

    otype = _normalize(order_type)
    if otype in _NEEDS_LIMIT and not _finite_positive(limit_price):
        return f'{otype} order requires a positive limit price (got {limit_price!r})'
    if otype in _NEEDS_STOP and not _finite_positive(stop_price):
        return f'{otype} order requires a positive stop price (got {stop_price!r})'
    return None


def rejection_for_order(order) -> Optional[str]:
    """Adapter: read the structural fields off an ib_async ``Order``.

    Kept separate from the decision so the decision stays pure and scalar (and
    therefore CrossHair-checkable). Attribute reads are defensive because this
    runs at the placement chokepoint, where an ``AttributeError`` would turn a
    malformed order into a crash instead of a refusal.
    """
    return structural_rejection(
        action=getattr(order, 'action', ''),
        quantity=getattr(order, 'totalQuantity', 0),
        order_type=getattr(order, 'orderType', ''),
        limit_price=getattr(order, 'lmtPrice', 0),
        stop_price=getattr(order, 'auxPrice', 0),
    )
