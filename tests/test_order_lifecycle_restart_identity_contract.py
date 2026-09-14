"""Historical permanent broker identities remain separate after restart."""

from test_order_lifecycle_contract_gaps import _events, _send, _trade
from trader.data.event_store import EventStore
from trader.trading.order_lifecycle import OrderLifecycleTracker


def test_reused_scoped_order_id_with_distinct_permanent_ids_is_not_ambiguous(tmp_path):
    store = EventStore(str(tmp_path / "historical-identities.duckdb"))
    tracker = OrderLifecycleTracker(store)
    restarted = None
    try:
        # Client order IDs can be reused across broker sessions. These two
        # terminal historical orders have no durable MMR intent reference;
        # their distinct permanent IDs prove they are different orders.
        _send(tracker, _trade(status="Cancelled", perm_id=1717, reference="manual"))
        _send(tracker, _trade(status="Filled", filled=4.0, average=10.0,
                              perm_id=2727, reference="manual"))
        original = {row["permId"]: row["identity"] for row in tracker.snapshot([17])}
        assert set(original) == {1717, 2727}
        assert len(set(original.values())) == 2
        tracker.close(timeout=0.2)

        restarted = OrderLifecycleTracker(store)
        rows = restarted.snapshot([17])
        assert {row["permId"]: row["identity"] for row in rows} == original
        assert {(row["permId"], row["status"], row["filled"]) for row in rows} == {
            (1717, "Cancelled", 0.0), (2727, "Filled", 4.0),
        }
        assert all(not row.get("identityAmbiguous", False) for row in rows)
        assert restarted.health["replay_required"] is False
        assert restarted.health["healthy"] is True
        assert [event.quantity for event in _events(store)] == [4.0]
    finally:
        if restarted is not None:
            restarted.close(timeout=0.2)
        tracker.close(timeout=0.2)
