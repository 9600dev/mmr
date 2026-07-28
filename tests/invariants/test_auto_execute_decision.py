"""Invariant of record: the autonomous execution decision.

``decide_signal`` is the single gate between a strategy emitting a signal and
real money moving without a human in the loop. Every safety rail the
auto-executor advertises — the kill switch, the live double-arm, per-bar dedup,
the stale-bar gate, the cooldown, the pyramiding bound, long-only semantics —
is a branch in this one function.

WHY THIS FILE EXISTS
    The function is well-shaped (pure, module-level, plain values in and out) and
    has 84 unit tests, but until 2026-07-25 it carried NO contract, NO property,
    and was outside the mutation scope — so the strength of those 84 tests was
    unmeasured. Measuring it found 79.2% (32 survivors), and among the survivors
    were mutants that change real behaviour in the most dangerous direction:

      * `live_armed: bool = False` -> `True` — the LIVE DOUBLE-ARM defaulting to
        ARMED. Nothing asserted that omitting the argument means disarmed.
      * `bar_age_seconds is not None` -> `is None` — inverting the stale-bar gate.
      * `stale_bar_multiple * bar_size` -> `/` — destroying the stale threshold.
      * `held_qty > 0` -> `> 1` (twice) — a fractional holding pyramids again, or
        loses its disarmed-close exemption.

    Example tests missed these because they used tidy inputs: armed sessions,
    whole-share positions, bars that were either fresh or absurdly old.

THE PROPERTIES
    Stated as things that must NEVER happen, because that is the direction that
    costs money. A wrongly-refused signal is a missed trade; a wrongly-permitted
    one is an unattended live order.
"""

import datetime as dt

import pytest
from hypothesis import given, settings, strategies as st

from trader.objects import Action
from trader.strategy.auto_executor import (
    SignalWork, accept_empty_broker_read, decide_signal,
)

_SETTINGS = settings(max_examples=300, deadline=None)

_BAR = dt.datetime(2026, 7, 24, 10, 30)


def _work(action=Action.BUY, **kw):
    base = dict(strategy_name='s', conid=1, action=action, bar_ts=_BAR,
                bar_size_seconds=60.0,
                auto_execute=True, state_running=True)
    base.update(kw)
    return SignalWork(**base)


# Every knob that could plausibly vary, so a property covers the whole cube
# rather than the corner an example author had in mind.
_FLAGS = dict(
    kill_switch=st.booleans(),
    paper_trading=st.booleans(),
    held_qty=st.floats(min_value=-1e6, max_value=1e6, allow_nan=False, allow_infinity=False),
    already_executed_bar=st.booleans(),
    cooldown_active=st.booleans(),
    bar_age_seconds=st.one_of(st.none(), st.floats(min_value=0.0, max_value=1e6,
                                                   allow_nan=False, allow_infinity=False)),
    live_armed=st.booleans(),
    held_lots=st.integers(min_value=0, max_value=10),
)


class TestAbsoluteRefusals:
    """Gates that must dominate every other consideration."""

    @_SETTINGS
    @given(action=st.sampled_from([Action.BUY, Action.SELL]), auto_execute=st.booleans(),
           paper_only=st.booleans(), running=st.booleans(), **_FLAGS)
    def test_kill_switch_always_skips(self, action, auto_execute, paper_only, running,
                                      kill_switch, **flags):
        """The kill switch is the operator's stop button. If it is set, NOTHING
        else in the input space may produce an order."""
        work = _work(action, auto_execute=auto_execute, paper_only=paper_only,
                     state_running=running, bar_size_seconds=60.0)
        d = decide_signal(work, kill_switch=True, **flags)
        assert d.kind == 'skip', f'kill switch bypassed: {d}'

    @_SETTINGS
    @given(action=st.sampled_from([Action.BUY, Action.SELL]), **_FLAGS)
    def test_already_executed_bar_always_skips(self, action, already_executed_bar, **flags):
        """Per-bar dedup: one execution per (strategy, conid, bar), ever. Without
        this a re-delivered bar doubles a position."""
        flags['kill_switch'] = False
        work = _work(action, bar_size_seconds=60.0)
        d = decide_signal(work, already_executed_bar=True, **flags)
        assert d.kind == 'skip', f'dedup bypassed: {d}'


class TestLiveDoubleArm:
    """Real money requires BOTH live mode and an explicit arming flag."""

    @_SETTINGS
    @given(**{k: v for k, v in _FLAGS.items() if k != 'live_armed'})
    def test_unarmed_live_never_opens(self, **flags):
        """THE real-money guard. In live mode without arming, no BUY may open —
        whatever the position, the bar age, or the cooldown."""
        flags['kill_switch'] = False
        flags['paper_trading'] = False
        work = _work(Action.BUY, bar_size_seconds=60.0)
        d = decide_signal(work, live_armed=False, **flags)
        assert d.kind != 'open', f'unarmed live session opened a position: {d}'

    def test_the_DEFAULT_is_disarmed(self):
        """Kills the mutant that flipped `live_armed: bool = False` to `True`.

        The code default must be DISARMED: a caller that forgets the argument
        must get the safe answer, not an armed one. Nothing asserted this."""
        d = decide_signal(
            _work(Action.BUY), kill_switch=False, paper_trading=False,
            held_qty=0.0, already_executed_bar=False, cooldown_active=False)
        assert d.kind == 'skip'
        assert 'not armed' in d.reason

    def test_arming_alone_is_not_enough_in_paper(self):
        """The converse sanity check: paper mode does not need the flag, so the
        gate must be about LIVE mode specifically."""
        d = decide_signal(
            _work(Action.BUY), kill_switch=False, paper_trading=True,
            held_qty=0.0, already_executed_bar=False, cooldown_active=False)
        assert d.kind == 'open'


class TestExitsAreNeverBlocked:
    """The mirror of the exit-class rule: closing must not be gated."""

    @_SETTINGS
    @given(cooldown=st.booleans(),
           bar_age=st.floats(min_value=0.0, max_value=1e6, allow_nan=False, allow_infinity=False),
           held=st.floats(min_value=0.01, max_value=1e6, allow_nan=False, allow_infinity=False),
           armed=st.booleans(), paper=st.booleans())
    def test_a_held_position_can_always_be_closed(self, cooldown, bar_age, held, armed, paper):
        """A SELL against an attributed holding closes regardless of cooldown, a
        stale bar, or the arming flag. Being unable to exit is strictly more
        dangerous than exiting on imperfect data."""
        work = _work(Action.SELL, bar_size_seconds=60.0)
        d = decide_signal(work, kill_switch=False, paper_trading=paper, held_qty=held,
                          already_executed_bar=False, cooldown_active=cooldown,
                          bar_age_seconds=bar_age, live_armed=armed)
        assert d.kind == 'close', f'an exit was blocked: {d}'
        assert d.quantity == held

    def test_disarming_does_not_strand_a_position(self):
        """auto_execute=False stops OPENS, not closes — otherwise disarming a
        strategy would orphan whatever it already holds."""
        work = _work(Action.SELL, auto_execute=False)
        d = decide_signal(work, kill_switch=False, paper_trading=True, held_qty=10.0,
                          already_executed_bar=False, cooldown_active=False)
        assert d.kind == 'close'

    def test_a_fractional_holding_is_still_closable_when_disarmed(self):
        """Kills the `held_qty > 0` -> `> 1` mutant in the disarm exemption: a
        0.5-lot position (forex/crypto) must not lose the right to close."""
        work = _work(Action.SELL, auto_execute=False)
        d = decide_signal(work, kill_switch=False, paper_trading=True, held_qty=0.5,
                          already_executed_bar=False, cooldown_active=False)
        assert d.kind == 'close', 'a fractional position was stranded by disarming'


class TestLongOnlySemantics:
    @_SETTINGS
    @given(held=st.floats(min_value=-1e6, max_value=0.0, allow_nan=False, allow_infinity=False),
           **{k: v for k, v in _FLAGS.items() if k not in ('held_qty', 'kill_switch')})
    def test_sell_while_flat_never_trades(self, held, **flags):
        """The executor is long-only: a SELL with nothing held must be a no-op,
        never a short. This is what stops an unbounded-loss position class from
        existing at all."""
        d = decide_signal(_work(Action.SELL, bar_size_seconds=60.0),
                          kill_switch=False, held_qty=held, **flags)
        assert d.kind == 'skip', f'shorting from flat: {d}'


class TestPyramidingBound:
    def test_no_pyramiding_by_default(self):
        """Default is single-lot: holding anything refuses a second open."""
        d = decide_signal(_work(Action.BUY), kill_switch=False, paper_trading=True,
                          held_qty=100.0, already_executed_bar=False, cooldown_active=False)
        assert d.kind == 'skip'
        assert 'no pyramiding' in d.reason

    def test_a_fractional_holding_still_blocks_a_second_open(self):
        """Kills the `held_qty > 0` -> `> 1` mutant: a 0.5-lot position counts as
        held, so the single-lot rule still applies."""
        d = decide_signal(_work(Action.BUY), kill_switch=False, paper_trading=True,
                          held_qty=0.5, already_executed_bar=False, cooldown_active=False)
        assert d.kind == 'skip', 'a fractional holding was treated as flat and re-opened'

    @_SETTINGS
    @given(max_adds=st.integers(min_value=1, max_value=5),
           held_lots=st.integers(min_value=0, max_value=12))
    def test_stack_never_exceeds_the_declared_bound(self, max_adds, held_lots):
        """With pyramiding enabled, the stack tops out at 1 + max_adds lots."""
        d = decide_signal(_work(Action.BUY, pyramid_max_adds=max_adds),
                          kill_switch=False, paper_trading=True, held_qty=100.0,
                          already_executed_bar=False, cooldown_active=False,
                          held_lots=held_lots)
        if held_lots > max_adds:
            assert d.kind == 'skip', f'stack grew past the bound: {d}'


class TestStaleBarGate:
    """Opening at current market off a stale bar is trading on garbage."""

    def test_a_stale_bar_refuses_the_open(self):
        """Kills the `is not None` -> `is None` inversion."""
        d = decide_signal(_work(Action.BUY, bar_size_seconds=60.0),
                          kill_switch=False, paper_trading=True, held_qty=0.0,
                          already_executed_bar=False, cooldown_active=False,
                          bar_age_seconds=500.0)          # > 3 x 60
        assert d.kind == 'skip'
        assert 'stale_bar' in d.reason

    def test_a_fresh_bar_still_opens(self):
        """Kills the `multiple * bar_size` -> `/` mutant: under division the
        threshold becomes 0.05s and every bar looks stale, so a healthy 100s-old
        bar on a 60s interval must still open."""
        d = decide_signal(_work(Action.BUY, bar_size_seconds=60.0),
                          kill_switch=False, paper_trading=True, held_qty=0.0,
                          already_executed_bar=False, cooldown_active=False,
                          bar_age_seconds=100.0)          # < 3 x 60
        assert d.kind == 'open', 'a fresh bar was rejected as stale'

    def test_the_DEFAULT_multiple_is_three(self):
        """Kills the `stale_bar_multiple: float = 3.0` -> `4.0` mutant. A bar
        3.5x its interval is stale under the real default and fresh under the
        mutant, and no caller-supplied value hides the difference here."""
        d = decide_signal(_work(Action.BUY, bar_size_seconds=60.0),
                          kill_switch=False, paper_trading=True, held_qty=0.0,
                          already_executed_bar=False, cooldown_active=False,
                          bar_age_seconds=210.0)          # 3.5 x 60
        assert d.kind == 'skip', 'the default stale multiple is no longer 3x'

    def test_sub_second_bars_are_still_gated(self):
        """Kills the `bar_size_seconds > 0` -> `> 1` mutant: a 0.5s bar size must
        not switch the stale gate off entirely."""
        d = decide_signal(_work(Action.BUY, bar_size_seconds=0.5),
                          kill_switch=False, paper_trading=True, held_qty=0.0,
                          already_executed_bar=False, cooldown_active=False,
                          bar_age_seconds=100.0)          # >> 3 x 0.5
        assert d.kind == 'skip', 'the stale gate switched off for sub-second bars'


class TestCooldown:
    @_SETTINGS
    @given(paper=st.booleans(), armed=st.booleans())
    def test_cooldown_blocks_opens_only(self, paper, armed):
        d = decide_signal(_work(Action.BUY, bar_size_seconds=60.0),
                          kill_switch=False, paper_trading=paper, held_qty=0.0,
                          already_executed_bar=False, cooldown_active=True,
                          live_armed=armed)
        assert d.kind != 'open'


class TestEmptyBrokerReadIsNotEvidenceOfAFlatBook:
    """Reconciliation marks attributed positions absent at the broker as
    CLOSED_EXTERNALLY and cancels their protective stops. Done wrongly, that is
    the worst outcome in this module: the position is still there, no strategy
    will ever close it (attribution is gone) and nothing protects it (the stop
    is cancelled) — permanently, because nothing re-attributes.

    An empty read is exactly the input where "absent" is least trustworthy:
    ``get_positions`` falls back to MMR's own portfolio cache when
    ``ib.positions()`` is empty, and both are empty for the first moments after
    trader_service connects — which is when strategy_service starts pushing bars
    and the executor does its first-work reconcile.
    """

    def test_a_first_empty_read_is_never_believed(self):
        """The startup race, stated directly: with no prior empty read there is
        no evidence, and no elapsed time can manufacture any."""
        assert accept_empty_broker_read(None, now=1e9, grace_seconds=0.0) is False
        assert accept_empty_broker_read(None, now=1e9, grace_seconds=120.0) is False

    @_SETTINGS
    @given(
        first=st.floats(min_value=0.0, max_value=2e9, allow_nan=False,
                        allow_infinity=False),
        delta=st.floats(min_value=0.0, max_value=1e6, allow_nan=False,
                        allow_infinity=False),
        grace=st.floats(min_value=0.0, max_value=1e4, allow_nan=False,
                        allow_infinity=False),
    )
    def test_belief_requires_the_full_grace_period(self, first, delta, grace):
        """Compared against the MEASURED elapsed time, not the nominal delta:
        (first + delta) - first is not delta for a real epoch timestamp, and
        the guarantee is about what the clock actually showed."""
        now = first + delta
        measured = now - first
        assert accept_empty_broker_read(first, now, grace) == (measured >= grace)

    @_SETTINGS
    @given(
        first=st.floats(min_value=0.0, max_value=2e9, allow_nan=False,
                        allow_infinity=False),
        grace=st.floats(min_value=1.0, max_value=1e4, allow_nan=False,
                        allow_infinity=False),
    )
    def test_belief_is_monotone_in_elapsed_time(self, first, grace):
        """Waiting longer never un-believes an empty book — the guard delays a
        reconcile, it must not be able to prevent one indefinitely."""
        assert accept_empty_broker_read(first, first + grace * 0.5, grace) is False
        assert accept_empty_broker_read(first, first + grace * 2.0 + 1.0, grace) is True

    @_SETTINGS
    @given(bad=st.sampled_from([None, 'soon', float('nan'), object()]))
    def test_an_unreadable_clock_does_not_close_anything(self, bad):
        """Fail toward NOT reconciling. A stale attribution blocks new opens,
        which is loud and recoverable; a wrongly-cancelled disaster stop on a
        live position is silent and is not."""
        assert accept_empty_broker_read(bad, now=1e9, grace_seconds=1.0) is False
        assert accept_empty_broker_read(1e9, now=bad, grace_seconds=1.0) is False
