"""Pure proposal-state-machine predicates — no I/O, no DuckDB, no dill.

The proposal state machine is pure data (a transition table) plus pure
predicates over it. They live here, split off from ``ProposalStore``'s DuckDB
plumbing, so:

  * CrossHair can import and symbolically verify them without dragging in the
    duckdb/dill import chain (dill runs a ``tempfile`` side effect at import,
    which CrossHair's audit wall — correctly — refuses to load); and
  * the recognized-status set and the legal-edge table have exactly one home.

``trader.data.proposal_store`` re-exports these and delegates its guards to
them. The table is ALSO written out literally in
``tests/invariants/test_proposal_machine.py`` (the human-owned spec); a change
here that diverges from that spec fails there — that is the point.
"""

from typing import Set

import deal

from trader.trading.proposal import ProposalStatus


# Valid state transitions:
#   PENDING  → APPROVED | REJECTED | EXPIRED | FAILED
#   APPROVED → EXECUTED | FAILED | REJECTED
# Terminal states (EXECUTED, REJECTED, EXPIRED, FAILED) admit NO further
# transitions — including same-status no-ops — and their metadata is immutable.
# This exact table is pinned by tests/invariants/test_proposal_machine.py; a
# change here without changing that human-owned spec is a bug.
_TERMINAL: Set[str] = {
    ProposalStatus.EXECUTED.value,
    ProposalStatus.REJECTED.value,
    ProposalStatus.EXPIRED.value,
    ProposalStatus.FAILED.value,
}

_ALLOWED_TRANSITIONS = {
    ProposalStatus.PENDING.value: {
        ProposalStatus.APPROVED.value,
        ProposalStatus.REJECTED.value,
        ProposalStatus.EXPIRED.value,
        ProposalStatus.FAILED.value,
    },
    ProposalStatus.APPROVED.value: {
        ProposalStatus.EXECUTED.value,
        ProposalStatus.FAILED.value,
        ProposalStatus.REJECTED.value,
    },
    ProposalStatus.EXECUTED.value: set(),
    ProposalStatus.REJECTED.value: set(),
    ProposalStatus.EXPIRED.value: set(),
    ProposalStatus.FAILED.value: set(),
}


@deal.pure
@deal.ensure(lambda _: _.result == (_.status in _ALLOWED_TRANSITIONS or _.status in _TERMINAL))
def is_known_status(status: str) -> bool:
    """Pure predicate: is ``status`` one the state machine recognizes?

    The DuckDB-facing guards (update_status / try_transition) delegate their
    "unknown status" check here so the recognized-status set lives in exactly
    one contracted place.
    """
    return status in _ALLOWED_TRANSITIONS or status in _TERMINAL


@deal.pure
@deal.ensure(lambda _: not _.result or _.from_status not in _TERMINAL)
@deal.ensure(lambda _: not _.result or _.to_status in _ALLOWED_TRANSITIONS)
def is_valid_transition(from_status: str, to_status: str) -> bool:
    """Pure transition-validity predicate for the proposal state machine.

    Returns True iff moving ``from_status`` → ``to_status`` is one of the edges
    in the documented table. Two invariants — enforced as ``deal`` contracts and
    symbolically checked by CrossHair — follow from the table's shape:

      * a legal transition never ORIGINATES from a terminal state — terminal
        states (EXECUTED / REJECTED / EXPIRED / FAILED) have no outgoing edges;
      * a legal transition always LANDS on a recognized status.

    This is the single source of truth for the DuckDB-backed guards, which stay
    pure-predicate + I/O rather than re-implementing the table inline.
    """
    return to_status in _ALLOWED_TRANSITIONS.get(from_status, set())
