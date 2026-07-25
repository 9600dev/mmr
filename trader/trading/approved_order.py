"""The ``ApprovedOrder`` capability token.

This turns "every order placement went through the risk gate" from a
call-graph *convention* into a *type-checked invariant*. The single IB
chokepoint — ``TradeExecutioner.subscribe_place_order_direct`` — accepts
**only** an ``ApprovedOrder``, and an ``ApprovedOrder`` can only be
constructed by ``mint_approved_order`` (a module-private sentinel guards the
sole constructor). So a code path that never reached the gate cannot construct
the argument the placement function requires, and ``ty`` flags the attempt.

The token *carries and records* the tranche-1 decision (exit-class predicate,
risk-gate result); it does **not** replace it. The gate/exit-class logic runs
exactly as before at each authorization point, and mints a token only on the
APPROVE branch. On a refuse, no token is minted — so no placement is possible.

**Why not a Pydantic model.** An earlier cut used ``pydantic.BaseModel``; the
Phase-1 adversarial review showed its *public* constructors
(``model_construct``, ``model_copy``) bypass ``__init__`` entirely, forging a
sink-accepted, type-clean token with no sentinel — defeating both the runtime
guard and ``ty``. A plain frozen ``__slots__`` class has no such bypass
constructor: the sentinel-guarded ``__init__`` is the *only* way in, frozen
attributes block re-binding, and ``__reduce__`` refuses serialization (so no
pickle/copy reconstruction path recreates a token off the wire).

**Threat model (documented residual).** The guard makes *accidental* or
*refactor-introduced* construction fail loudly — the honest-mistake case this
phase targets — and there is no type-clean public bypass. It is still **not** a
defense against a malicious in-process actor that reflects out the module
sentinel; true unforgeability against a hostile strategy is Phase 3's
subprocess isolation (SAFETY_ROADMAP tranche 3). Do not over-engineer that
here.

The ``contract``/``order`` payload is stored as-is (no isinstance validation):
the invariant is *possession of a minted token*, and the trusted mint sites
always pass real ib_async objects, so revalidating at the chokepoint would only
add a new runtime failure mode (and break the duck-typed test idiom).
"""
from __future__ import annotations

import enum

from ib_async import Contract, Order

# Deliberately NOT extended when ExitReason was added: tests/invariants/
# test_approved_order.py pins this export surface to exactly these two names, and
# that property is human-owned. ExitReason is imported by name where needed —
# __all__ only governs `import *`, so nothing is lost by respecting the pin.
__all__ = ['ApprovedOrder', 'mint_approved_order']


class ExitReason(enum.Enum):
    """WHY a token claims to be exit-class.

    ``is_exit=True`` exempts an order from the gate-record requirement at the
    placement chokepoint, so it is the one field a caller can get wrong in a way
    that puts an ungated order on the wire. It cannot be verified by re-reading
    the position, because that is only valid for SOME exits — see below. So it is
    at least made ATTRIBUTABLE: every claim names the rule that justifies it, a
    new mint site cannot pass a bare bool (tests/test_exit_reason_wiring.py
    fails), and the chokepoint checks the one category that is checkable.
    """

    POSITION_CLASSIFIED = 'position_classified'
    """``Trader.order_reduces_exposure`` said so against the LIVE position.
    Corroboratable: the chokepoint re-asks the predicate and logs loudly on
    disagreement (which means the position moved between classification and
    placement). Observability only — it never refuses, because refusing an exit
    is worse than the stale classification it would be protecting against."""

    PROTECTIVE_CHILD = 'protective_child'
    """A bracket/protective leg (take-profit, stop-loss, trailing stop) attached
    to an entry. Exit-class BY CONSTRUCTION, not by position: the entry is staged
    with ``transmit=False`` and has not filled, so the position legitimately does
    not exist yet and ``order_reduces_exposure`` would correctly answer False.
    NOT corroboratable — asking the predicate here would refuse every bracket's
    protection, or (observability-only) alarm on every single bracket."""

    VALIDATED_STANDALONE = 'validated_standalone'
    """``Trader.place_standalone_order``, which validates exit-class itself
    before minting and refuses anything else (it was once an ungated exposure
    door). The validation is the caller's, immediately upstream of the mint."""

# Module-private sentinel. NEVER exported (absent from __all__) and never
# passed out of this module: it is the only key that unlocks construction.
_MINT_KEY = object()


class ApprovedOrder:
    """A frozen, mint-only capability token proving an order was authorized.

    Construct it exclusively via :func:`mint_approved_order` — the gate is the
    only sanctioned minter. Any other construction raises. Not a Pydantic
    model by design: there must be no ``model_construct``/``model_copy``-style
    public constructor that skips the sentinel.
    """

    # Declared for the type-checker; ``__slots__`` makes the token frozen-able
    # and keeps it allocation-light on the order hot path.
    contract: Contract
    order: Order
    is_exit: bool
    checks: dict[str, str]
    exit_reason: ExitReason | None

    __slots__ = ('contract', 'order', 'is_exit', 'checks', 'exit_reason')

    def __init__(
        self,
        _key: object = None,
        /,
        *,
        contract: Contract,
        order: Order,
        is_exit: bool = False,
        checks: dict[str, str] | None = None,
        exit_reason: ExitReason | None = None,
    ) -> None:
        # Positional-only, sentinel-guarded. Accidental/direct construction
        # (``ApprovedOrder(contract=..., order=...)`` or with a wrong first
        # arg) fails loudly instead of forging a capability token.
        if _key is not _MINT_KEY:
            raise RuntimeError(
                'ApprovedOrder is mint-only — construct it via the gate '
                '(trader.trading.approved_order.mint_approved_order)')
        object.__setattr__(self, 'contract', contract)
        object.__setattr__(self, 'order', order)
        object.__setattr__(self, 'is_exit', is_exit)
        object.__setattr__(self, 'checks', {} if checks is None else checks)
        object.__setattr__(self, 'exit_reason', exit_reason)

    # --- frozen: a leg's authorization decision cannot be mutated post-mint ---
    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError('ApprovedOrder is frozen')

    def __delattr__(self, name: str) -> None:
        raise AttributeError('ApprovedOrder is frozen')

    # --- not serializable: no pickle/copy path may reconstruct a token off the
    # wire, bypassing the mint guard. Tokens live and die inside one server
    # process, synchronously between authorization and placement. ---
    def __reduce__(self):
        raise TypeError('ApprovedOrder is not serializable (mint-only capability token)')

    def __repr__(self) -> str:
        sym = getattr(self.contract, 'symbol', '?')
        act = getattr(self.order, 'action', '?')
        qty = getattr(self.order, 'totalQuantity', '?')
        if self.is_exit:
            kind = f'exit:{self.exit_reason.value if self.exit_reason else "UNATTRIBUTED"}'
        else:
            kind = 'open'
        return f'ApprovedOrder({act} {qty} {sym}, {kind})'


def mint_approved_order(
    contract: Contract,
    order: Order,
    *,
    is_exit: bool,
    checks: dict[str, str] | None = None,
    exit_reason: ExitReason | None = None,
) -> ApprovedOrder:
    """The sanctioned constructor, for code that has completed the gate /
    exit-class decision for ``(contract, order)`` and reached its APPROVE branch.

    Minting itself performs NO verification, so on its own the type only ever
    proved "somebody called mint" — not "the gate ran". A new code path could
    mint without gating and the type-checker would be perfectly happy, which made
    the capability a convention wearing a type's clothes.

    Minting stays permissive BY DESIGN — tests/invariants/test_approved_order.py
    pins that a token may be constructed with an empty checks record, and that
    property is human-owned. The authorization evidence is instead enforced
    where the token is SPENT: ``TradeExecutioner.subscribe_place_order_direct``
    refuses a non-exit token that carries no passing gate record. Validating at
    the consumption point is also the better boundary — it covers every future
    mint site automatically, including ones nobody remembered to audit.

    NOT verified anywhere, deliberately: that ``is_exit`` matches the live
    position. Corroborating it needs a position read, and ``enforce_approver_tier``
    documents why a second read on this path is unsafe — it can race and mis-gate
    a genuine exit. ``is_exit`` is checked at its SOURCE instead, in
    ``trader.trading.exit_class``.
    """
    return ApprovedOrder(
        _MINT_KEY,
        contract=contract,
        order=order,
        is_exit=is_exit,
        checks=checks,
        exit_reason=exit_reason,
    )
