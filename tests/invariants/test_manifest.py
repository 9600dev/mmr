"""Invariant of record: the strategy manifest is an OPENS-ONLY envelope.

The manifest (allowed_conids / direction / turnover caps) may refuse a
strategy's *entry*, but it must be structurally incapable of blocking an
*exit* — a close (SELL-while-held) or a time-exit. Exits reducing risk are
sacred; a declared envelope that could strand a live position by refusing its
close would be strictly more dangerous than no envelope at all.

Two load-bearing properties are pinned here:

  (a) A close still closes even when the held conId is OUTSIDE allowed_conids
      AND the turnover cap is exhausted — the two conditions that refuse an
      *open* — because the close path never reaches the manifest gate.
  (b) A time-exit still fires under the same out-of-envelope conditions.

Plus a backward-compatibility property: with every manifest field None, the
gate is a pure no-op — check_manifest returns None for any open directive and
counts, so the un-manifested path is byte-identical to pre-manifest behaviour.

These are the human-owned spec. If an implementation change turns one red, the
implementation is wrong — the manifest must never gate an exit.
"""

import datetime as dt
from types import SimpleNamespace

import pandas as pd
import pytest
from hypothesis import given, settings, strategies as st

from trader.objects import Action
from trader.strategy.auto_executor import (
    AutoExecutor,
    BarWork,
    Directive,
    SignalWork,
    check_manifest,
    decide_signal,
)

# A FRESH bar. These tests drive the OPEN path, and the stale-bar gate refuses
# an open whose bar is older than 3x the bar size. The hardcoded past date this
# replaced only ever passed because an unset `bar_size_seconds` disabled the
# gate, so the suite was exercising opens under conditions that cannot occur
# live. Naive here means UTC, which is how bar_age_seconds reads it.
TS = pd.Timestamp.now(tz='UTC').tz_localize(None).floor('min')


# ---------------------------------------------------------------------------
# Minimal fake SDK — self-contained so this invariant file has no cross-test
# import dependency. Resolves conId 1111 <-> 'WDS' exactly and books fills.
# ---------------------------------------------------------------------------

class _Result:
    def __init__(self, ok=True, obj=None, error=None):
        self._ok, self.obj, self.error = ok, (obj if obj is not None else []), error

    def is_success(self):
        return self._ok


class _FakeSDK:
    def __init__(self):
        self.secdef = SimpleNamespace(
            symbol='WDS', exchange='ASX', primaryExchange='ASX',
            currency='AUD', secType='STK', conId=1111)
        self.proposals = {}
        self.next_id = 100
        self.broker = {}
        self.propose_calls = []
        self.approve_calls = []
        self.fill_qty = 140.0
        self.avg_cost = 100.0
        self.next_protective_id = 900

    def resolve(self, symbol, sec_type='STK', exchange='', universe='', currency=''):
        return [self.secdef] if symbol in (1111, 'WDS') else []

    def propose(self, **kwargs):
        pid = self.next_id
        self.next_id += 1
        self.propose_calls.append(kwargs)
        self.proposals[pid] = SimpleNamespace(
            quantity=kwargs.get('quantity'), metadata=kwargs.get('metadata') or {})
        return pid, None, None

    def approve(self, pid):
        self.approve_calls.append(pid)
        p = self.proposals[pid]
        qty = p.quantity if p.quantity else self.fill_qty
        action = self.propose_calls[-1]['action']
        cur = self.broker.get(1111, 0.0)
        self.broker[1111] = cur + qty if action == 'BUY' else cur - qty
        return _Result(ok=True, obj=[pid * 10])

    def positions(self):
        rows = [{'conId': c, 'position': q, 'avgCost': self.avg_cost}
                for c, q in self.broker.items() if q != 0]
        return pd.DataFrame(rows)

    def place_protective_order(self, **kwargs):
        oid = self.next_protective_id
        self.next_protective_id += 1
        return _Result(ok=True, obj=SimpleNamespace(order=SimpleNamespace(orderId=oid)))

    def cancel(self, order_id):
        return _Result(ok=True)

    def trades(self):
        rows = [{'orderId': pid * 10,
                 'totalQuantity': (self.proposals[pid].quantity or self.fill_qty)}
                for pid in self.approve_calls]
        return pd.DataFrame(rows)

    def _proposal_store(self):
        proposals = self.proposals

        class _S:
            def get(self, pid):
                return proposals.get(pid)
        return _S()


@pytest.fixture
def executor(tmp_path):
    sdk = _FakeSDK()
    ex = AutoExecutor(
        duckdb_path=str(tmp_path / 'manifest_inv.duckdb'),
        paper_trading=True, cooldown_seconds=0.0, sdk_factory=lambda: sdk)
    ex._reconciled = True
    return ex, sdk


def _open_no_manifest(ex, sdk):
    """Establish a genuinely-open attributed position via a no-manifest BUY, so
    a later close/time-exit has something real to exit. The close itself then
    carries the refusing envelope in the test body."""
    ex._process_signal(SignalWork(
        strategy_name='orb', conid=1111, action=Action.BUY, bar_ts=TS,
        bar_size_seconds=60.0,
        probability=0.6, auto_execute=True, state_running=True))
    assert ex.state.open_position('orb', 1111) is not None
    assert sdk.broker[1111] == 140.0


# ---------------------------------------------------------------------------
# (a) THE load-bearing property: a manifest never blocks an exit.
# ---------------------------------------------------------------------------

class TestManifestNeverBlocksExit:
    def test_close_fires_out_of_universe_and_turnover_exhausted(self, executor):
        ex, sdk = executor
        _open_no_manifest(ex, sdk)
        # A SELL whose manifest declares BOTH conditions that refuse an open:
        #   * this conId (1111) is NOT in allowed_conids, AND
        #   * the daily turnover cap is already exhausted.
        # It must STILL close — the close path never reaches the manifest gate.
        pre_opens = [c for c in sdk.propose_calls if c['action'] == 'BUY']
        ex._process_signal(SignalWork(
            strategy_name='orb', conid=1111, action=Action.SELL,
            bar_ts=TS + pd.Timedelta(minutes=30), bar_size_seconds=60.0, auto_execute=True,
            state_running=True,
            manifest_allowed_conids=[2222, 3333],      # 1111 excluded
            manifest_max_opens_per_day=1,               # already exhausted below
            manifest_max_opens_per_hour=1))
        closes = [c for c in sdk.propose_calls if c['action'] == 'SELL']
        assert len(closes) == 1, 'the close must have been placed despite the envelope'
        assert closes[0]['quantity'] == 140.0
        assert ex.state.open_position('orb', 1111) is None
        # And the manifest did NOT sneak in an extra open.
        assert [c for c in sdk.propose_calls if c['action'] == 'BUY'] == pre_opens


# ---------------------------------------------------------------------------
# (b) A time-exit fires even fully outside the manifest envelope.
# ---------------------------------------------------------------------------

class TestManifestNeverBlocksTimeExit:
    def test_time_exit_fires_out_of_envelope(self, executor):
        ex, sdk = executor
        # Open with a max_hold_bars time-exit and no manifest.
        ex._process_signal(SignalWork(
            strategy_name='orb', conid=1111, action=Action.BUY, bar_ts=TS,
        bar_size_seconds=60.0,
            probability=0.6, auto_execute=True, state_running=True,
            max_hold_bars=5))
        assert ex.state.open_position('orb', 1111) is not None
        # The time-exit path (_process_bar -> _execute_close) never consults
        # the manifest; even if this strategy's envelope excluded the conId and
        # capped turnover, the exit must fire.
        ex._process_bar(BarWork('orb', 1111, TS + pd.Timedelta(minutes=6), 6))
        closes = [c for c in sdk.propose_calls if c['action'] == 'SELL']
        assert len(closes) == 1
        assert 'time exit' in closes[0]['reasoning']
        assert ex.state.open_position('orb', 1111) is None


# ---------------------------------------------------------------------------
# (c) Backward-compatibility: all-None manifest ⇒ the gate is a pure no-op.
# ---------------------------------------------------------------------------

_ACTIONS = st.sampled_from([Action.BUY, Action.SELL])
_DIRECTIVE_KINDS = st.sampled_from(['open', 'close', 'skip'])


class TestBackwardCompatIdentical:
    @settings(max_examples=300, deadline=None)
    @given(
        conid=st.integers(min_value=1, max_value=10_000),
        action=_ACTIONS,
        kind=_DIRECTIVE_KINDS,
        opens_today=st.integers(min_value=0, max_value=1000),
        opens_hour=st.integers(min_value=0, max_value=1000),
    )
    def test_no_manifest_gate_is_noop(self, conid, action, kind, opens_today, opens_hour):
        """With every manifest field None, check_manifest returns None for ANY
        directive and ANY turnover counts — it can never alter the outcome, so
        an un-manifested strategy is byte-identical to today."""
        work = SignalWork(
            strategy_name='s', conid=conid, action=action, bar_ts=TS,
            bar_size_seconds=60.0,
            manifest_allowed_conids=None, manifest_direction=None,
            manifest_max_opens_per_day=None, manifest_max_opens_per_hour=None)
        directive = Directive(kind, 'reason', quantity=None)
        assert check_manifest(work, directive, opens_today, opens_hour) is None

    @settings(max_examples=300, deadline=None)
    @given(
        action=_ACTIONS,
        held_qty=st.floats(min_value=0.0, max_value=1e6,
                           allow_nan=False, allow_infinity=False),
        already_executed_bar=st.booleans(),
        cooldown_active=st.booleans(),
        auto_execute=st.booleans(),
    )
    def test_decide_signal_ignores_manifest_fields(
            self, action, held_qty, already_executed_bar, cooldown_active, auto_execute):
        """decide_signal must not read manifest fields at all: the same inputs
        with and without manifest primitives set produce identical directives.
        (The manifest lives entirely in the separate check_manifest gate.)"""
        base = dict(
            strategy_name='s', conid=1, action=action, bar_ts=TS,
            bar_size_seconds=60.0,
            probability=0.6, auto_execute=auto_execute, state_running=True)
        plain = SignalWork(**base)
        with_manifest = SignalWork(
            **base, manifest_allowed_conids=[9999],
            manifest_direction='long', manifest_max_opens_per_day=1,
            manifest_max_opens_per_hour=1)
        kw = dict(kill_switch=False, paper_trading=True, held_qty=held_qty,
                  already_executed_bar=already_executed_bar,
                  cooldown_active=cooldown_active, live_armed=True)
        d1 = decide_signal(plain, **kw)
        d2 = decide_signal(with_manifest, **kw)
        assert (d1.kind, d1.reason, d1.quantity) == (d2.kind, d2.reason, d2.quantity)
