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

from ib_async import Contract, Order

__all__ = ['ApprovedOrder', 'mint_approved_order']

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

    __slots__ = ('contract', 'order', 'is_exit', 'checks')

    def __init__(
        self,
        _key: object = None,
        /,
        *,
        contract: Contract,
        order: Order,
        is_exit: bool = False,
        checks: dict[str, str] | None = None,
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
        kind = 'exit' if self.is_exit else 'open'
        return f'ApprovedOrder({act} {qty} {sym}, {kind})'


def mint_approved_order(
    contract: Contract,
    order: Order,
    *,
    is_exit: bool,
    checks: dict[str, str] | None = None,
) -> ApprovedOrder:
    """The sanctioned constructor. Callable only from code that has completed
    the gate/exit-class decision for ``(contract, order)`` and reached its
    APPROVE branch. Refusing paths never call this, so they can never obtain a
    token to hand the placement chokepoint.
    """
    return ApprovedOrder(
        _MINT_KEY,
        contract=contract,
        order=order,
        is_exit=is_exit,
        checks=checks,
    )
