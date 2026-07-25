"""Invariant of record: the ApprovedOrder capability token.

The single IB placement chokepoint — ``TradeExecutioner.subscribe_place_order_
direct`` — accepts ONLY an ``ApprovedOrder``. An ``ApprovedOrder`` can only be
constructed by ``mint_approved_order`` (a module-private sentinel guards the
sole constructor; it is a frozen ``__slots__`` class, NOT a pydantic model, so
there is no ``model_construct``/``model_copy`` bypass). Together these turn
"every order went through the risk
gate" from a call-graph convention into a checked invariant: a code path that
never reached the gate cannot construct the argument the sink requires.

Properties stated here:
  * direct/accidental construction of ApprovedOrder RAISES (mint-only);
  * mint_approved_order produces a usable, frozen token that carries is_exit +
    the tri-state gate record;
  * the token is immutable (field assignment raises) — a leg's authorization
    decision cannot be mutated after minting;
  * the sink is unusable without a token: at runtime a non-token argument
    fails (no .contract), and at the type level ``ty`` rejects a bare
    contract/order (asserted via a real ty run when the dev toolchain is
    present).

Residual (documented, not a gap in THIS phase): the mint guard defeats
accidental/refactor construction, not a malicious in-process actor that
reflects out the sentinel. True unforgeability is Phase 3 subprocess
isolation (SAFETY_ROADMAP tranche 2/3).
"""
from __future__ import annotations

import asyncio
import shutil
import subprocess
import textwrap
from pathlib import Path

import copy
import pickle

import pytest

from trader.trading import approved_order as ao
from trader.trading.approved_order import ApprovedOrder, mint_approved_order


class _Contract:
    """Duck contract — the token skips isinstance validation by design, so a
    lightweight stand-in is sufficient to exercise the invariant."""
    def __init__(self, conId=1, symbol='AMD'):
        self.conId = conId
        self.symbol = symbol


class _Order:
    def __init__(self, action='BUY', account='DU1'):
        self.action = action
        self.account = account
        self.totalQuantity = 10


# ---------------------------------------------------------------------------
# (a) direct construction raises — the token is mint-only
# ---------------------------------------------------------------------------

def test_direct_construction_without_key_raises():
    """The common accidental form — ApprovedOrder(contract=.., order=..) —
    must not forge a token. It fails loudly (missing sentinel)."""
    with pytest.raises((TypeError, RuntimeError)):
        ApprovedOrder(contract=_Contract(), order=_Order())


def test_direct_construction_with_wrong_key_raises_runtime_error():
    """A caller that passes SOME first arg but not the real sentinel gets the
    explicit mint-only RuntimeError."""
    with pytest.raises(RuntimeError, match='mint-only'):
        ApprovedOrder(object(), contract=_Contract(), order=_Order())


def test_sentinel_is_not_exported():
    """The sentinel is the only key; it must never leave the module (absent
    from __all__), or the capability could be forged trivially."""
    assert '_MINT_KEY' not in ao.__all__
    assert set(ao.__all__) == {'ApprovedOrder', 'mint_approved_order'}


# ---------------------------------------------------------------------------
# (b) mint produces a usable token carrying the authorization decision
# ---------------------------------------------------------------------------

def test_mint_produces_usable_token():
    c, o = _Contract(conId=42), _Order(action='SELL')
    tok = mint_approved_order(c, o, is_exit=True, checks={'daily_loss': 'pass'})
    assert isinstance(tok, ApprovedOrder)
    assert tok.contract is c
    assert tok.order is o
    assert tok.is_exit is True
    assert tok.checks == {'daily_loss': 'pass'}


def test_mint_defaults_checks_empty_and_isolated():
    """The default checks record is empty and NOT shared between tokens (a
    shared mutable default would let one leg's record bleed into another)."""
    a = mint_approved_order(_Contract(), _Order(), is_exit=False)
    b = mint_approved_order(_Contract(), _Order(), is_exit=False)
    assert a.checks == {} and b.checks == {}
    assert a.checks is not b.checks


# ---------------------------------------------------------------------------
# (c) the token is frozen
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('field', ['is_exit', 'contract', 'order', 'checks'])
def test_token_is_frozen(field):
    tok = mint_approved_order(_Contract(), _Order(), is_exit=False)
    with pytest.raises(AttributeError):
        setattr(tok, field, object())


# ---------------------------------------------------------------------------
# (c') no bypass constructor — the Phase-1 review finding, pinned as regression
#
# The original cut used a pydantic BaseModel whose PUBLIC constructors
# (model_construct / model_copy) skipped the sentinel-guarded __init__,
# forging a sink-accepted, ty-clean token with the gate never consulted. The
# token must therefore NOT be a pydantic model, and no construction path
# (replace / copy / deepcopy / pickle) may recreate a token off an existing
# one or off the wire.
# ---------------------------------------------------------------------------

def test_no_pydantic_bypass_constructors():
    """The forge vector that defeated the first cut must not exist: no
    model_construct / model_copy / model_validate on the token type."""
    for backdoor in ('model_construct', 'model_copy', 'model_validate',
                     'model_validate_json', '__fields_set__'):
        assert not hasattr(ApprovedOrder, backdoor), (
            f'ApprovedOrder exposes {backdoor!r} — a pydantic bypass '
            f'constructor that forges a token without the gate')


def test_copy_and_deepcopy_cannot_forge():
    """copy/deepcopy must not yield a mutable clone that re-binds the order
    (they route through __reduce__, which refuses)."""
    tok = mint_approved_order(_Contract(), _Order(), is_exit=True)
    with pytest.raises(TypeError):
        copy.copy(tok)
    with pytest.raises(TypeError):
        copy.deepcopy(tok)


def test_pickle_cannot_reconstruct_a_token():
    """A token must not be reconstructable from serialized state (which would
    bypass the mint guard). __reduce__ refuses."""
    tok = mint_approved_order(_Contract(), _Order(), is_exit=False)
    with pytest.raises((TypeError, pickle.PicklingError)):
        pickle.dumps(tok)


# ---------------------------------------------------------------------------
# (d) the sink is unusable without a token
# ---------------------------------------------------------------------------

def test_sink_rejects_non_token_at_runtime():
    """Passing a bare order (not a token) to the chokepoint fails: the sink
    reads approved.contract first, and a non-token lacks it. This is the
    runtime shadow of the type-level guarantee."""
    from trader.trading.executioner import TradeExecutioner
    ex = TradeExecutioner()

    with pytest.raises(AttributeError):
        asyncio.run(ex.subscribe_place_order_direct(_Order()))  # type: ignore[arg-type]


_TY_PROBE = textwrap.dedent(
    """
    from ib_async import Contract, Order
    from trader.trading.executioner import TradeExecutioner
    from trader.trading.approved_order import mint_approved_order

    async def bad(ex: TradeExecutioner):
        # WRONG — bare contract/order, no token. Must be a type error.
        await ex.subscribe_place_order_direct(Contract(), Order())

    async def good(ex: TradeExecutioner):
        tok = mint_approved_order(Contract(), Order(), is_exit=False)
        await ex.subscribe_place_order_direct(tok)  # OK
    """
)


@pytest.mark.skipif(shutil.which('uv') is None, reason='ty dev toolchain (uv) not present')
def test_ty_rejects_bare_contract_order_to_sink(tmp_path):
    """The load-bearing property of this phase: ``ty`` flags a placement that
    skips the gate. We run the real type-checker on a probe that calls the
    sink with a bare contract/order and assert it is reported as a type error,
    while the minted-token call is clean.
    """
    repo = Path(__file__).resolve().parents[2]
    probe = tmp_path / 'ty_probe_approved_order.py'
    probe.write_text(_TY_PROBE)
    proc = subprocess.run(
        ['uv', 'run', 'ty', 'check', str(probe),
         '--output-format', 'concise', '--exit-zero'],
        cwd=repo, capture_output=True, text=True, timeout=180,
    )
    out = proc.stdout + proc.stderr
    # The gate-skipping call must be flagged: Expected `ApprovedOrder`, found a
    # bare `Contract`. (Line-number-independent — dedent adds a leading blank.)
    assert any(
        'invalid-argument-type' in ln and 'Expected `ApprovedOrder`' in ln
        and 'found `Contract`' in ln
        for ln in out.splitlines()
    ), f'ty did not flag the gate-skipping call as a type error.\n{out}'
    # The minted-token `good` path must be clean — nothing was flagged as
    # having wrongly received an ApprovedOrder.
    assert not any('found `ApprovedOrder`' in ln for ln in out.splitlines()), (
        f'ty wrongly flagged the minted-token call.\n{out}'
    )
