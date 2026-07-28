"""SPEC: the proposal gate and the order splitter answer the SAME question.

WHY THIS FILE EXISTS
    The flip residual survived its own fix, and this is the property that
    would have caught it.

    ``order_split.py`` decomposes a position-crossing order into an
    unrefusable reduction and a gated remainder. That was correct, tested,
    CrossHair-clean and 95% mutation-killed. It still did not close the hole
    on the direct path, because a DIFFERENT gate sat upstream of it: the RPC
    layer asked ``order_reduces_exposure(contract, action, FULL quantity)``,
    which is direction-aware and not size-clamped, so ``SELL 3`` against 1
    held answered "this is an exit" and the whole order was exempted from
    ``require_proposal_approval``. The splitter downstream then dutifully
    placed the opening half — past a gate that had already waved it through.

    Found live on 2026-07-27: ``mmr sell GOOGL --limit 900 --quantity 3``
    against 1 held produced TWO submitted orders, while the control (a plain
    open short on a flat symbol, same path, same session) was correctly
    refused. Same account, same config, minutes apart.

    The lesson is not "the splitter was wrong". It is that a decomposition is
    only as good as the agreement between everyone who classifies the order.
    Two components each asking a reasonable question, in the wrong order, put
    the hole back.

WHAT IS PINNED
    A behavioural property over the whole direct path, not the arithmetic
    (``tests/invariants/test_order_split.py`` owns the arithmetic):

      1. With ``require_proposal_approval`` on, NO quantity that opens
         exposure is ever placed on the direct path. For any held position and
         any order size.
      2. A reduction is still never blocked. Even in the flip case, where the
         opening half is refused, the closing half goes through.

    Property 1 alone is satisfiable by refusing everything, which would block
    closes — the failure the exit-class rule exists to prevent. Property 2
    alone is satisfiable by today's bug. Both together are the spec.
"""

import asyncio
import threading

from unittest.mock import AsyncMock, MagicMock

from hypothesis import given, settings, strategies as st

import pytest

import reactivex as rx


def _api(held, require_approval=True):
    """A Trader shell with a live position of ``held``, wired to the real
    ``split_for_order`` / ``place_order_simple`` and a recording executioner.

    Everything stubbed here is I/O (broker reads, snapshots, margin). The code
    under test — the gate decision, the split, and which orders reach the
    executioner — is real.
    """
    from trader.messaging.trader_service_api import TraderServiceApi
    from trader.trading.trading_runtime import Trader

    t = object.__new__(Trader)
    t.pnl_subscriptions = {}
    t._pnl_subscriptions_lock = threading.Lock()
    t._main_loop = None
    t.disposables = []
    t.ib_account = 'DU1'
    t.approver_required_above_usd = 0.0
    t.approver_key = ''
    t.order_tracker = None
    t.require_proposal_approval = require_approval

    position = MagicMock()
    position.contract = MagicMock()
    position.contract.conId = 578031277
    position.contract.symbol = 'QBTS'
    position.position = held
    t.get_positions = MagicMock(return_value=[position] if held else [])

    t.client = MagicMock()
    t.client.get_snapshot = AsyncMock(
        return_value=MagicMock(ask=20.0, bid=19.0, last=19.5, close=19.5))

    placed = []

    class _Exec:
        def helper_create_order(self, contract, action, tick, amount, qty,
                                limit_price, market_order, stop_pct,
                                algo_name='global', debug=False):
            o = MagicMock()
            o.contract = contract
            o.order = MagicMock()
            o.order.action = str(action)
            o.order.totalQuantity = qty
            return o

        async def place_order(self, contract_order, condition,
                              position_value_hint=None, approver_key='',
                              force_open=False, skip_risk_gate=False):
            placed.append((str(contract_order.order.action),
                           float(contract_order.order.totalQuantity),
                           force_open))
            ft = MagicMock()
            ft.order = MagicMock()
            ft.order.orderId = 6000 + len(placed)
            return rx.from_iterable([ft])

    t.executioner = _Exec()

    api = TraderServiceApi(t)
    api._placed = placed
    return api


def _contract():
    c = MagicMock()
    c.symbol = 'QBTS'
    c.secType = 'STK'
    c.exchange = 'SMART'
    c.conId = 578031277
    c.multiplier = None
    return c


def _run(api, action, quantity):
    return asyncio.run(api.place_order_simple(
        contract=_contract(), action=action, equity_amount=None,
        quantity=quantity, limit_price=19.4, market_order=False))


def _opening_quantity(placed, action, held):
    """Shares among ``placed`` that INCREASE exposure, from the position the
    order started against. Computed here independently of the production
    splitter on purpose: a test that reuses the code under test to decide what
    the right answer is cannot fail when that code is wrong.
    """
    reducible = abs(held) if (
        (action == 'SELL' and held > 0) or (action == 'BUY' and held < 0)) else 0.0
    total = sum(q for _, q, _ in placed)
    return max(0.0, total - reducible)


class TestApprovalGateNeverLetsOpeningQuantityThrough:

    @settings(max_examples=200, deadline=None)
    @given(
        held=st.floats(min_value=-500, max_value=500, allow_nan=False,
                       allow_infinity=False),
        quantity=st.floats(min_value=0.01, max_value=1000, allow_nan=False,
                           allow_infinity=False),
        action=st.sampled_from(['BUY', 'SELL']),
    )
    def test_no_opening_quantity_is_ever_placed(self, held, quantity, action):
        """THE property. With approval required, the direct path may reduce a
        position but may never open one — at any size, from any position.

        This fails on the pre-fix code for exactly the live case: held=+1,
        SELL 3 placed 3 shares against a 1-share position, so 2 opened a short.
        """
        api = _api(held=held)
        _run(api, action, quantity)
        opening = _opening_quantity(api._placed, action, held)
        assert opening == pytest.approx(0.0, abs=1e-9), (
            f'held={held} {action} {quantity} placed {api._placed}: '
            f'{opening} shares of NEW exposure got past require_proposal_approval')

    @settings(max_examples=200, deadline=None)
    @given(
        held=st.floats(min_value=-500, max_value=500, allow_nan=False,
                       allow_infinity=False),
        quantity=st.floats(min_value=0.01, max_value=1000, allow_nan=False,
                           allow_infinity=False),
        action=st.sampled_from(['BUY', 'SELL']),
    )
    def test_a_reduction_is_never_blocked(self, held, quantity, action):
        """The other half of the spec, and the reason 'refuse the whole order'
        is not an acceptable fix. Whatever the gate does to the opening half,
        the shares that close an existing position must still be placed."""
        api = _api(held=held)
        _run(api, action, quantity)
        expected_reduction = min(
            quantity,
            abs(held) if ((action == 'SELL' and held > 0)
                          or (action == 'BUY' and held < 0)) else 0.0)
        placed_total = sum(q for _, q, _ in api._placed)
        assert placed_total >= expected_reduction - 1e-9, (
            f'held={held} {action} {quantity}: a reduction of {expected_reduction} '
            f'was owed but only {placed_total} was placed — a close was blocked')

    def test_the_exact_live_case(self):
        """held +1, SELL 3, approval required. Pinned as the regression it is:
        this produced orders 1009 (SELL 1) and 1011 (SELL 2) in the paper
        account, and 1011 should never have existed."""
        api = _api(held=1.0)
        _run(api, 'SELL', 3.0)
        assert [(q, force_open) for _, q, force_open in api._placed] == [(1.0, False)], (
            f'expected only the 1-share reduction, got {api._placed}')

    def test_the_control_that_exposed_it_still_refuses(self):
        """The comparison that made the bug visible: the same path, the same
        session, a plain open short on a flat symbol. It was correctly refused
        while the flip was not. It must stay refused."""
        api = _api(held=0.0)
        result = _run(api, 'SELL', 2.0)
        assert not result.is_success()
        assert 'require_proposal_approval' in str(result.error)
        assert api._placed == []

    def test_an_ordinary_close_still_goes_through_untouched(self):
        api = _api(held=3.0)
        result = _run(api, 'SELL', 3.0)
        assert result.is_success()
        assert [(q, f) for _, q, f in api._placed] == [(3.0, False)]

    def test_with_approval_off_the_remainder_is_gated_not_dropped(self):
        """Turning the gate off must not turn the SPLIT off. The opening half
        still goes to the executioner as a forced open, where the risk gate
        and trading filter see it — it is gated, not exempted."""
        api = _api(held=1.0, require_approval=False)
        _run(api, 'SELL', 3.0)
        assert [(q, f) for _, q, f in api._placed] == [(1.0, False), (2.0, True)], (
            f'expected reduction then a force_open remainder, got {api._placed}')


class TestTheApproverTierDoesNotExemptTheRemainderEither:
    """The same hole, in a third layer, found by auditing rather than probing.

    After fixing the RPC proposal gate, the lesson written down was "look for
    the OTHER askers before declaring a classification bug fixed". Applying it
    immediately found `enforce_approver_tier` doing the same thing: it exempts
    any order that `order_reduces_exposure` calls a reduction, and a split
    flip's opening half still reads as one, because the reduction it follows
    may not have filled yet. So the notional tier handed the remainder exactly
    the exemption the split exists to remove.

    Latent rather than live — the tier is off by default
    (`approver_required_above_usd = 0`) — but it is the control that is
    supposed to stand between a compromised proposer context and a large
    position, which is precisely where a silent exemption matters.

    `force_open` is what keeps the exemption honest, and it must not cost the
    genuine exits their exemption; all three cases are pinned together, because
    fixing this by simply recomputing the opening remainder here would gate a
    real exit whenever the position read failed.
    """

    def _tier(self, held, force_open, action='SELL', qty=2.0):
        from trader.trading.trading_runtime import Trader
        t = object.__new__(Trader)
        t.approver_required_above_usd = 1000.0     # tier ON
        t.approver_key = 'secret'
        p = MagicMock()
        p.contract = MagicMock()
        p.contract.conId = 1
        p.contract.symbol = 'X'
        p.position = held
        t.get_positions = MagicMock(return_value=[p] if held else [])
        t._tier_notional = AsyncMock(return_value=(50_000.0, True))
        c = MagicMock()
        c.conId = 1
        c.symbol = 'X'
        c.multiplier = None
        return asyncio.run(t.enforce_approver_tier(
            c, action, qty, 'MARKET', None, '', force_open=force_open))

    def test_a_split_remainder_is_gated_by_the_tier(self):
        """held +1, SELL 2 arriving as the opening half. $50k, no key."""
        assert self._tier(held=1.0, force_open=True) is not None

    def test_the_identical_order_from_flat_is_also_gated(self):
        """The control. If this passed while the remainder was exempted, the
        difference would be the bug rather than the position."""
        assert self._tier(held=0.0, force_open=False) is not None

    def test_a_genuine_pure_exit_is_still_never_gated(self):
        """SELL 2 against a held 5 needs no approver key, and must not acquire
        one from this fix. Refusing an exit is worse than any limit."""
        assert self._tier(held=5.0, force_open=False) is None

    def test_a_short_side_flip_remainder_is_gated_symmetrically(self):
        assert self._tier(held=-1.0, force_open=True, action='BUY') is not None


class TestOneResolverForBothQuestions:
    """The structural fix behind the behavioural one: there is a single place
    that decides how much of an order opens exposure. Two implementations
    drifting apart is what produced the bug, so the tier's accessor and the
    splitter must be the same computation by construction."""

    @settings(max_examples=200, deadline=None)
    @given(
        held=st.floats(min_value=-500, max_value=500, allow_nan=False,
                       allow_infinity=False),
        quantity=st.floats(min_value=0.01, max_value=1000, allow_nan=False,
                           allow_infinity=False),
        action=st.sampled_from(['BUY', 'SELL']),
    )
    def test_the_tier_and_the_splitter_never_disagree(self, held, quantity, action):
        api = _api(held=held)
        plan = api.trader.split_for_order(_contract(), action, quantity)
        tier_view = api.trader._opening_exposure_quantity(_contract(), action, quantity)
        assert tier_view == plan.open_qty

    def test_an_unreadable_position_is_all_opening(self):
        """Fail-closed. If the broker read fails we cannot prove any share
        reduces anything, so every share is treated as new exposure and faces
        every gate."""
        api = _api(held=5.0)
        api.trader.get_positions = MagicMock(side_effect=RuntimeError('IB down'))
        plan = api.trader.split_for_order(_contract(), 'SELL', 3.0)
        assert (plan.reduce_qty, plan.open_qty) == (0.0, 3.0)

    def test_an_unreadable_position_refuses_the_order_rather_than_opening(self):
        api = _api(held=5.0)
        api.trader.get_positions = MagicMock(side_effect=RuntimeError('IB down'))
        result = _run(api, 'SELL', 3.0)
        assert not result.is_success()
        assert api._placed == []
