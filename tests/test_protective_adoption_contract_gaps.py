"""Protective observations acquire only scoped, proven holding identity.

Native-shaped observations are injected at the snapshot boundary. Real intent
and attribution stores retain the resulting identity; no broker is contacted.
"""

import datetime as dt
import math
from types import SimpleNamespace

import pandas as pd
import pytest

from trader.strategy.auto_executor import AutoExecutor, AutoExecState
from trader.strategy.execution_intents import IntentStore, timestamp_text


OWNER, CONID = "adoption_owner", 101
ENTRY = dt.datetime(2024, 1, 3, 15, tzinfo=dt.timezone.utc)


@pytest.fixture
def adoption(tmp_path, monkeypatch):
    path = str(tmp_path / "adoption.duckdb")
    executor = AutoExecutor.__new__(AutoExecutor)
    executor.state = AutoExecState(path)
    executor.intents = IntentStore(path)
    executor.state.record_open(OWNER, CONID, 10, ENTRY, 51, None, None)
    rows = []
    monkeypatch.setattr(executor, "_execution_snapshot", lambda: {
        "complete": True, "orders": rows,
    })
    return SimpleNamespace(executor=executor, rows=rows, path=path)


def _row(ctx, *, owner=OWNER, conid=CONID, order_id=71, **changes):
    owned = ctx.executor.state.open_position(owner, conid)
    return {
        "account": "paper-contract", "clientId": 7, "permId": 1000 + order_id,
        "orderId": order_id, "orderRef": owner,
        "clientIntentId": f"native-stop-{order_id}", "conId": conid,
        "action": "SELL", "orderType": "STP", "status": "Submitted",
        "filled": 0, "totalQuantity": 10, "fillQuantityKnown": True,
        "brokerIntentCreatedAt": owned["ownership_started_at"] + 1.0,
        **changes,
    }


@pytest.mark.parametrize(("strategy", "conid", "expected"), [
    (OWNER, CONID, {(OWNER, CONID)}),
    (OWNER, None, {(OWNER, CONID), (OWNER, 202)}),
    (None, CONID, {(OWNER, CONID), ("other_owner", CONID)}),
])
def test_adoption_filters_preserve_exact_owner_and_instrument(adoption, strategy, conid, expected):
    ctx = adoption
    ctx.executor.state.record_open(OWNER, 202, 10, ENTRY, 52, None, None)
    ctx.executor.state.record_open("other_owner", CONID, 10, ENTRY, 53, None, None)
    ctx.rows.extend([
        _row(ctx), _row(ctx, conid=202, order_id=72),
        _row(ctx, owner="other_owner", order_id=73),
    ])

    ctx.executor._adopt_observed_protectives(strategy, conid)

    assert {(item["strategy"], item["conid"]) for item in ctx.executor.intents.all()} == expected


@pytest.mark.parametrize("status", ["Submitted", "PendingCancel", "Filled", "Cancelled", "ApiCancelled", "Inactive"])
def test_owned_working_and_terminal_stop_observations_keep_native_identity(adoption, status):
    ctx = adoption
    row = _row(ctx, status=status, filled=10 if status == "Filled" else 0)
    ctx.rows.append(row)

    ctx.executor._adopt_observed_protectives(OWNER, CONID)

    intent, = ctx.executor.intents.all()
    position = ctx.executor.state.open_position(OWNER, CONID)
    assert intent["kind"] == "PROTECTIVE"
    assert intent["payload"]["broker_intent_id"] == row["clientIntentId"]
    assert intent["payload"]["order_ids"] == [row["orderId"]]
    assert intent["payload"]["ownership_epoch"] == position["ownership_epoch"]
    assert intent["payload"]["ownership_started_at"] == position["ownership_started_at"]
    assert intent["payload"]["attribution_unresolved"] is False


def test_terminal_broker_status_can_locate_a_stop_with_unknown_fill_status(adoption):
    ctx = adoption
    ctx.rows.append(_row(ctx, status="Unknown", brokerStatus="Cancelled", fillQuantityKnown=False))

    ctx.executor._adopt_observed_protectives(OWNER, CONID)

    intent, = ctx.executor.intents.all()
    assert intent["payload"]["order_ids"] == [71]
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 10


def test_dataframe_snapshot_preserves_the_same_adoption_contract(adoption, monkeypatch):
    ctx = adoption
    frame = pd.DataFrame([_row(ctx)])
    monkeypatch.setattr(ctx.executor, "_execution_snapshot", lambda: {"complete": True, "orders": frame})

    ctx.executor._adopt_observed_protectives(OWNER, CONID)

    intent, = ctx.executor.intents.all()
    assert intent["payload"]["broker_intent_id"] == "native-stop-71"


def test_duplicate_physical_observation_is_adopted_once_per_snapshot(adoption, monkeypatch):
    ctx = adoption
    row = _row(ctx)
    ctx.rows.extend([row, dict(row)])
    calls = []
    actual_adopt = ctx.executor.intents.adopt_protective

    def observe_adopt(*args, **kwargs):
        calls.append((args, kwargs))
        return actual_adopt(*args, **kwargs)

    monkeypatch.setattr(ctx.executor.intents, "adopt_protective", observe_adopt)

    ctx.executor._adopt_observed_protectives(OWNER, CONID)

    assert len(calls) == 1
    assert len(ctx.executor.intents.all()) == 1


def test_existing_native_protective_intent_does_not_gain_a_second_local_identity(adoption):
    ctx = adoption
    native = ctx.executor.intents.create(
        OWNER, CONID, "PROTECTIVE", {"bar_ts": timestamp_text(ENTRY), "order_ids": [71]},
        status="WORKING",
    )
    ctx.rows.append(_row(ctx, clientIntentId=native["intent_id"]))

    ctx.executor._adopt_observed_protectives(OWNER, CONID)

    assert ctx.executor.intents.all() == [native]


@pytest.mark.parametrize("status", ["Submitted", "Cancelled"])
def test_old_creation_proof_cannot_acquire_a_new_holding(adoption, status):
    ctx = adoption
    origin = ctx.executor.state.open_position(OWNER, CONID)["ownership_started_at"]
    ctx.rows.append(_row(ctx, status=status, brokerIntentCreatedAt=origin - 1.0))

    ctx.executor._adopt_observed_protectives(OWNER, CONID)

    intents = ctx.executor.intents.all()
    if status == "Cancelled":
        assert intents == []
    else:
        intent, = intents
        assert intent["status"] == "UNKNOWN"
        assert intent["payload"]["attribution_unresolved"] is True
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 10


@pytest.mark.parametrize("created", [None, True, 0.0, -1.0, math.nan, math.inf])
def test_unusable_creation_proof_reserves_an_observed_working_stop(adoption, created):
    ctx = adoption
    ctx.rows.append(_row(ctx, brokerIntentCreatedAt=created))

    ctx.executor._adopt_observed_protectives(OWNER, CONID)

    intent, = ctx.executor.intents.all()
    assert intent["status"] == "UNKNOWN"
    assert intent["payload"]["attribution_unresolved"] is True


def test_exact_creation_boundary_can_resolve_the_same_adopted_holding(adoption):
    ctx = adoption
    origin = ctx.executor.state.open_position(OWNER, CONID)["ownership_started_at"]
    row = _row(ctx, brokerIntentCreatedAt=None)
    ctx.rows.append(row)
    ctx.executor._adopt_observed_protectives(OWNER, CONID)
    pending, = ctx.executor.intents.all()
    assert pending["status"] == "UNKNOWN"
    row["brokerIntentCreatedAt"] = origin

    ctx.executor._adopt_observed_protectives(OWNER, CONID)

    resolved, = IntentStore(ctx.path).all()
    assert resolved["intent_id"] == pending["intent_id"]
    assert resolved["status"] == "WORKING"
    assert resolved["payload"]["attribution_unresolved"] is False


def test_ambiguous_identity_remains_reserved_despite_a_valid_creation_time(adoption):
    ctx = adoption
    ctx.rows.append(_row(ctx, identityAmbiguous=True))

    ctx.executor._adopt_observed_protectives(OWNER, CONID)

    intent, = ctx.executor.intents.all()
    assert intent["status"] == "UNKNOWN"
    assert intent["payload"]["attribution_unresolved"] is True


def test_unavailable_snapshot_does_not_create_or_change_an_intent(adoption, monkeypatch):
    ctx = adoption

    def unavailable():
        raise ConnectionError("no broker snapshot")

    monkeypatch.setattr(ctx.executor, "_execution_snapshot", unavailable)
    ctx.executor._adopt_observed_protectives(OWNER, CONID)

    assert ctx.executor.intents.all() == []
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 10
