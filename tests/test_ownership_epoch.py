"""Holding identity and origin survive fill replay without inventing old proof."""
import datetime as dt
import json
from types import SimpleNamespace

import duckdb
import pytest

from trader.strategy.auto_executor import AutoExecState
from trader.strategy.execution_intents import IntentStore, timestamp_text


OWNER = "epoch_contract"
CONID = 100
BAR = dt.datetime(2026, 9, 9, 12, tzinfo=dt.timezone.utc)
ORIGIN = BAR.timestamp()


@pytest.fixture
def state_and_journal(tmp_path):
    path = str(tmp_path / "ownership.duckdb")
    stores = []

    def journal():
        store = IntentStore(path)
        stores.append(store)
        return store

    try:
        yield SimpleNamespace(path=path, state=AutoExecState(path), journal=journal)
    finally:
        for store in stores:
            store.journal.close()


def opening(store, *, bar=BAR, quantity=40):
    return store.create(OWNER, CONID, "OPEN",
                        dict(bar_ts=timestamp_text(bar), quantity=quantity, proposal_id=501))


def test_claim_time_is_immutable_across_retry_and_journal_restart(state_and_journal, monkeypatch):
    ctx = state_and_journal
    clock = [ORIGIN]
    monkeypatch.setattr("trader.strategy.execution_intents.time.time", lambda: clock[0])
    store = ctx.journal()
    first = opening(store)
    assert first["payload"]["intent_created_at"] == ORIGIN

    clock[0] += 100
    retried = opening(store, quantity=99)
    assert retried["intent_id"] == first["intent_id"]
    assert retried["payload"]["quantity"] == 40
    assert retried["payload"]["intent_created_at"] == ORIGIN
    restarted = ctx.journal()
    restored = opening(restarted)
    assert restored["intent_id"] == first["intent_id"]
    assert restored["payload"]["intent_created_at"] == ORIGIN
    with pytest.raises(ValueError, match="immutable"):
        restarted.update(restored, intent_created_at=clock[0])
    restarted.update(restored, status="WORKING", cumulative_filled=10)
    saved, = restarted.all(kind="OPEN")
    assert saved["payload"]["intent_created_at"] == ORIGIN
    assert saved["payload"]["cumulative_filled"] == 10


def test_legacy_claim_does_not_acquire_a_new_origin_on_retry(state_and_journal, monkeypatch):
    ctx = state_and_journal
    monkeypatch.setattr("trader.strategy.execution_intents.time.time", lambda: ORIGIN)
    store = ctx.journal()
    payload = dict(bar_ts=timestamp_text(BAR), quantity=40, proposal_id=501)
    with store.journal.transaction() as conn:
        conn.execute("INSERT INTO execution_intents VALUES (?, ?, ?, ?, ?, ?, ?)",
                     ["auto-legacy-claim", OWNER, CONID, "OPEN", "WORKING", json.dumps(payload), ORIGIN - 100])

    restarted = ctx.journal()
    restored = opening(restarted)
    assert restored["intent_id"] == "auto-legacy-claim"
    assert "intent_created_at" not in restored["payload"]
    with pytest.raises(ValueError, match="immutable"):
        restarted.update(restored, intent_created_at=ORIGIN)
    assert ctx.state.apply_fill(restored, 40) == 40
    position = ctx.state.open_position(OWNER, CONID)
    assert position["quantity"] == 40
    assert position["ownership_started_at"] is None
    assert "intent_created_at" not in restarted.all(kind="OPEN")[0]["payload"]


def test_first_open_fill_uses_claim_origin_even_when_applied_later(state_and_journal, monkeypatch):
    ctx = state_and_journal
    clock = [ORIGIN]
    monkeypatch.setattr("trader.strategy.execution_intents.time.time", lambda: clock[0])
    store = ctx.journal()
    intent = opening(store)
    clock[0] += 100

    assert ctx.state.apply_fill(intent, 40) == 40

    position = AutoExecState(ctx.path).open_position(OWNER, CONID)
    assert position["quantity"] == 40
    assert position["ownership_started_at"] == ORIGIN
    assert isinstance(position["ownership_epoch"], str) and position["ownership_epoch"]
    assert ctx.state.apply_fill(intent, 40) == 0
    assert ctx.state.open_position(OWNER, CONID) == position


def test_adds_and_partial_receipts_preserve_holding_epoch_and_origin(state_and_journal, monkeypatch):
    ctx = state_and_journal
    clock = [ORIGIN]
    monkeypatch.setattr("trader.strategy.execution_intents.time.time", lambda: clock[0])
    store = ctx.journal()
    initial = opening(store)
    assert ctx.state.apply_fill(initial, 10) == 10
    first = ctx.state.open_position(OWNER, CONID)
    clock[0] += 10
    assert ctx.state.apply_fill(initial, 40) == 30
    store.update(initial, status="FILLED", cumulative_filled=40)
    add_bar = BAR + dt.timedelta(minutes=1)
    clock[0] += 60
    add = opening(store, bar=add_bar, quantity=20)
    assert add["intent_id"] != initial["intent_id"]
    assert add["payload"]["intent_created_at"] > first["ownership_started_at"]
    assert ctx.state.apply_fill(add, 5) == 5
    clock[0] += 10
    assert ctx.state.apply_fill(add, 20) == 15

    position = AutoExecState(ctx.path).open_position(OWNER, CONID)
    assert position["quantity"] == 60
    assert position["lots"] == 2
    assert timestamp_text(position["entry_bar_ts"]) == timestamp_text(add_bar)
    assert position["ownership_epoch"] == first["ownership_epoch"]
    assert position["ownership_started_at"] == ORIGIN
    positions, checkpoints = ctx.state.ownership_snapshot(store.all())
    assert positions[0]["ownership_epoch"] == first["ownership_epoch"]
    assert positions[0]["ownership_started_at"] == ORIGIN
    assert checkpoints == {initial["intent_id"]: 40, add["intent_id"]: 20}


def test_fresh_open_after_flat_has_new_epoch_even_at_same_bar_and_clock(state_and_journal, monkeypatch):
    ctx = state_and_journal
    monkeypatch.setattr("trader.strategy.execution_intents.time.time", lambda: ORIGIN)
    store = ctx.journal()
    initial = opening(store)
    assert ctx.state.apply_fill(initial, 40) == 40
    store.update(initial, status="FILLED", cumulative_filled=40)
    first = ctx.state.open_position(OWNER, CONID)
    close = store.create(OWNER, CONID, "CLOSE", dict(
        bar_ts=timestamp_text(BAR), quantity=40,
        ownership_epoch=first["ownership_epoch"], ownership_started_at=first["ownership_started_at"]))
    assert ctx.state.apply_fill(close, 40) == 40
    assert ctx.state.open_position(OWNER, CONID) is None
    fresh = opening(store, quantity=60)
    assert fresh["intent_id"] != initial["intent_id"]
    assert ctx.state.apply_fill(fresh, 60) == 60

    current = AutoExecState(ctx.path).open_position(OWNER, CONID)
    assert current["quantity"] == 60
    assert current["entry_bar_ts"] == first["entry_bar_ts"]
    assert current["ownership_started_at"] == first["ownership_started_at"] == ORIGIN
    assert current["ownership_epoch"] != first["ownership_epoch"]


def test_pre_column_position_migration_preserves_unknown_ownership_origin(tmp_path):
    path = str(tmp_path / "legacy.duckdb")
    stored_bar = BAR.replace(tzinfo=None)  # historical TIMESTAMP stores naive UTC
    with duckdb.connect(path) as conn:
        conn.execute("""CREATE TABLE auto_exec_positions (
            strategy VARCHAR NOT NULL, conid BIGINT NOT NULL, quantity DOUBLE NOT NULL,
            entry_bar_ts TIMESTAMP, entry_time TIMESTAMP NOT NULL, proposal_id BIGINT,
            close_by_time VARCHAR, max_hold_bars BIGINT, status VARCHAR NOT NULL,
            closed_reason VARCHAR, close_proposal_id BIGINT, updated TIMESTAMP NOT NULL)""")
        conn.execute("INSERT INTO auto_exec_positions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                     [OWNER, CONID, 40, stored_bar, stored_bar, 501, None, None,
                      "OPEN", None, None, stored_bar])
        original_bar, = conn.execute("SELECT entry_bar_ts FROM auto_exec_positions").fetchone()
        assert original_bar == stored_bar

    for _ in range(2):
        state = AutoExecState(path)
        position = state.open_position(OWNER, CONID)
        assert position["quantity"] == 40
        assert position["entry_bar_ts"] == original_bar
        assert position["ownership_epoch"] is None
        assert position["ownership_started_at"] is None
