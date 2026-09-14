"""A retained exit can act only on the entry identities it actually owns."""

import datetime as dt

import pytest

from trader.strategy.auto_executor import AutoExecutor
from trader.strategy.execution_intents import IntentStore, timestamp_text


ENTRY = dt.datetime(2024, 1, 3, 15, tzinfo=dt.timezone.utc)
NEXT_ENTRY = ENTRY + dt.timedelta(minutes=1)


@pytest.fixture
def authority(tmp_path):
    executor = AutoExecutor.__new__(AutoExecutor)
    executor.intents = IntentStore(str(tmp_path / "authority.db"))
    return executor


def _position(entry, proposal=71):
    return {"entry_bar_ts": entry, "proposal_id": proposal}


def _close(executor, **payload):
    return executor.intents.create(
        "authority_test", 101, "CLOSE",
        {"bar_ts": timestamp_text(NEXT_ENTRY), "reason": "time exit",
         "quantity": 10.0, "exit_request_active": True, **payload},
        status="WAITING",
    )


@pytest.mark.parametrize(("entry", "expected"), [
    (ENTRY, True),
    (NEXT_ENTRY, False),
    (None, False),
])
def test_initial_timer_binding_cannot_match_a_later_entry(authority, entry, expected):
    # Initial TIMER payloads have a policy binding and no separate residual
    # request binding. Absence of the latter must not erase the former.
    intent = _close(authority, policy_entry_bar_ts=timestamp_text(ENTRY))
    position = _position(entry) if entry is not None else None

    assert "request_entry_bar_ts" not in intent["payload"]
    assert authority._close_entry_matches(intent, position) is expected


@pytest.mark.parametrize("policy", [None, NEXT_ENTRY])
def test_matching_concrete_request_survives_an_older_explicit_scope(authority, policy):
    scope = {"positions": [{"entry_bar_ts": timestamp_text(ENTRY),
                             "proposal_id": 70}], "openings": {}}
    intent = _close(
        authority, explicit_exit_scope=scope,
        request_entry_bar_ts=timestamp_text(NEXT_ENTRY),
        policy_entry_bar_ts=timestamp_text(policy) if policy is not None else None,
    )

    assert authority._close_entry_matches(intent, _position(NEXT_ENTRY)) is True


@pytest.mark.parametrize(("policy", "requested"), [
    (ENTRY, NEXT_ENTRY),
    (NEXT_ENTRY, ENTRY),
])
def test_both_concrete_bindings_must_match_the_current_entry(authority, policy, requested):
    intent = _close(
        authority, policy_entry_bar_ts=timestamp_text(policy),
        request_entry_bar_ts=timestamp_text(requested),
    )

    assert authority._close_entry_matches(intent, _position(NEXT_ENTRY)) is False


def test_unbound_explicit_request_does_not_gain_an_unrelated_position(authority):
    scope = {"positions": [{"entry_bar_ts": timestamp_text(ENTRY),
                             "proposal_id": 70}], "openings": {}}
    intent = _close(authority, explicit_exit_scope=scope, policy_entry_bar_ts=None)

    assert authority._close_entry_matches(intent, _position(NEXT_ENTRY)) is False
    assert authority._close_entry_matches(intent, _position(ENTRY, proposal=70)) is True


@pytest.mark.parametrize(("pending", "position_entry", "expected"), [
    (False, None, False),
    (True, None, True),
    (True, NEXT_ENTRY, False),
])
def test_pending_captured_opening_matches_only_while_owner_is_flat(
        authority, pending, position_entry, expected):
    openings = {}
    if pending:
        opening = authority.intents.create(
            "authority_test", 101, "OPEN",
            {"bar_ts": timestamp_text(ENTRY)}, status="CREATED",
        )
        openings[opening["intent_id"]] = {
            "entry_bar_ts": timestamp_text(ENTRY), "proposal_id": None,
        }
    intent = _close(authority, explicit_exit_scope={"positions": [], "openings": openings},
                    policy_entry_bar_ts=None)
    position = _position(position_entry) if position_entry is not None else None

    assert authority._close_entry_matches(intent, position) is expected
