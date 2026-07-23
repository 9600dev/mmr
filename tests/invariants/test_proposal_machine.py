"""Invariant of record: the proposal state machine, as documented.

A Hypothesis RuleBasedStateMachine drives a REAL ProposalStore on a temporary
DuckDB file through legal AND illegal operation sequences, checking the store
against a trivial in-memory model after every step.

The transition table below is written out LITERALLY — deliberately not
imported from trader.data.proposal_store — so a change to the implementation's
table cannot silently rewrite this spec into a tautology. If the two diverge,
this file fails, and that is the point.

Documented machine (CLAUDE.md + proposal_store docstrings):
    PENDING  → APPROVED | REJECTED | EXPIRED | FAILED
    APPROVED → EXECUTED | FAILED | REJECTED
    EXECUTED | REJECTED | EXPIRED | FAILED → (terminal: nothing, ever)
Plus:
    * same-status "transitions" always raise (double-approve defence),
    * terminal rows refuse metadata mutation,
    * terminal rows refuse delete without force=True,
    * proposals are born PENDING — add() refuses any other initial status.
"""

import os
import shutil
import tempfile
from uuid import uuid4

import pytest
from hypothesis import HealthCheck, settings, strategies as st
from hypothesis.stateful import (
    Bundle,
    RuleBasedStateMachine,
    consumes,
    invariant,
    rule,
)

from trader.data.duckdb_store import DuckDBConnection
from trader.data.proposal_store import InvalidProposalTransition, ProposalStore
from trader.trading.proposal import ExecutionSpec, TradeProposal


# The documented table — the spec, not the implementation's copy.
DOCUMENTED_TRANSITIONS = {
    'PENDING': {'APPROVED', 'REJECTED', 'EXPIRED', 'FAILED'},
    'APPROVED': {'EXECUTED', 'FAILED', 'REJECTED'},
    'EXECUTED': set(),
    'REJECTED': set(),
    'EXPIRED': set(),
    'FAILED': set(),
}
TERMINAL = {'EXECUTED', 'REJECTED', 'EXPIRED', 'FAILED'}
ALL_STATUSES = sorted(DOCUMENTED_TRANSITIONS)


def _make_proposal(symbol='AMD', status='PENDING'):
    return TradeProposal(
        symbol=symbol,
        action='BUY',
        quantity=10.0,
        execution=ExecutionSpec(),
        reasoning='invariant machine',
        status=status,
    )


class ProposalMachine(RuleBasedStateMachine):
    """Real ProposalStore on a tmp DuckDB vs. a dict model of pid -> status."""

    proposals = Bundle('proposals')

    def __init__(self):
        super().__init__()
        # Mirror tests/conftest.py's tmp-DuckDB idiom, but per machine
        # instance: Hypothesis builds many machines per test, while pytest
        # fixtures (including the DuckDBConnection._instances-clearing
        # autouse) only reset between tests — so each machine gets a unique
        # path and clears its own connection-cache entry in teardown().
        self._tmpdir = tempfile.mkdtemp(prefix='mmr_invariant_proposals_')
        self._db_path = os.path.join(self._tmpdir, f'test_{uuid4().hex[:8]}.duckdb')
        self.store = ProposalStore(self._db_path)
        self.model: dict[int, str] = {}

    def teardown(self):
        DuckDBConnection._instances.pop(self._db_path, None)
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    # -- creation ----------------------------------------------------------

    @rule(target=proposals, symbol=st.sampled_from(['AMD', 'AAPL', 'MSFT', 'NVDA']))
    def add_pending(self, symbol):
        pid = self.store.add(_make_proposal(symbol=symbol))
        assert self.store.get(pid).status == 'PENDING', 'proposals are born PENDING'
        self.model[pid] = 'PENDING'
        return pid

    @rule(status=st.sampled_from(sorted(TERMINAL | {'APPROVED'}) + ['BOGUS']))
    def add_non_pending_refused(self, status):
        """add() refuses any initial status other than PENDING."""
        before = {pid for pid in self.model}
        with pytest.raises(InvalidProposalTransition):
            self.store.add(_make_proposal(status=status))
        # Nothing was created behind the refusal.
        assert {p.id for p in self.store.query(limit=10_000)} == before

    # -- transitions -------------------------------------------------------

    @rule(pid=proposals, to=st.sampled_from(ALL_STATUSES))
    def update_status(self, pid, to):
        """Exactly the documented table: legal transitions succeed; anything
        else (including same-status no-ops) raises and changes nothing."""
        current = self.model[pid]
        if to in DOCUMENTED_TRANSITIONS[current]:
            self.store.update_status(pid, to)
            self.model[pid] = to
        else:
            with pytest.raises(InvalidProposalTransition):
                self.store.update_status(pid, to)
        assert self.store.get(pid).status == self.model[pid]

    @rule(pid=proposals)
    def update_unknown_status_refused(self, pid):
        with pytest.raises(InvalidProposalTransition):
            self.store.update_status(pid, 'BOGUS')
        assert self.store.get(pid).status == self.model[pid]

    @rule(
        pid=proposals,
        frm=st.sampled_from(ALL_STATUSES),
        to=st.sampled_from(ALL_STATUSES),
    )
    def cas_transition(self, pid, frm, to):
        """try_transition: statically-illegal pairs raise; legal pairs win
        iff the row is actually in `frm` (exactly-one-winner CAS)."""
        if to not in DOCUMENTED_TRANSITIONS[frm]:
            with pytest.raises(InvalidProposalTransition):
                self.store.try_transition(pid, frm, to)
        else:
            won = self.store.try_transition(pid, frm, to)
            assert won is (self.model[pid] == frm)
            if won:
                self.model[pid] = to
        assert self.store.get(pid).status == self.model[pid]

    # -- terminal immutability beyond status -------------------------------

    @rule(pid=proposals, value=st.integers())
    def metadata_merge(self, pid, value):
        """Metadata merges freely on live rows; terminal rows are immutable —
        the merge raises and the stored metadata is byte-identical after."""
        if self.model[pid] in TERMINAL:
            before = self.store.get(pid).metadata
            with pytest.raises(InvalidProposalTransition):
                self.store.update_metadata(pid, {'poke': value})
            assert self.store.get(pid).metadata == before
        else:
            self.store.update_metadata(pid, {'poke': value})
            assert self.store.get(pid).metadata.get('poke') == value

    @rule(pid=proposals)
    def delete_terminal_without_force_refused(self, pid):
        """Audit-trail protection: terminal rows survive delete() without
        force, and the refusal changes nothing."""
        if self.model[pid] not in TERMINAL:
            return
        with pytest.raises(InvalidProposalTransition):
            self.store.delete(pid)
        after = self.store.get(pid)
        assert after is not None
        assert after.status == self.model[pid]

    @rule(pid=consumes(proposals))
    def delete_row(self, pid):
        """Non-terminal rows delete freely; terminal rows only with force."""
        if self.model[pid] in TERMINAL:
            assert self.store.delete(pid, force=True) is True
        else:
            assert self.store.delete(pid) is True
        assert self.store.get(pid) is None
        del self.model[pid]

    # -- global invariant --------------------------------------------------

    @invariant()
    def store_matches_model(self):
        for pid, status in self.model.items():
            row = self.store.get(pid)
            assert row is not None, f'proposal {pid} vanished'
            assert row.status == status, (
                f'proposal {pid}: store says {row.status!r}, model says {status!r}'
            )


ProposalMachine.TestCase.settings = settings(
    max_examples=15,
    stateful_step_count=30,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)

TestProposalMachine = ProposalMachine.TestCase
