"""Unproven legacy adoption enrichment cannot create a second fill checkpoint."""

import pytest

from test_protective_adoption_identity import (
    CONID,
    OWNER,
    native_adoption,
    observe_fill,
    owned_quantity,
    persist_legacy_adoption,
    place_external_protection,
)
from trader.strategy.auto_executor import AutoExecutor


@pytest.fixture
def legacy_adoption_context(native_adoption):
    try:
        yield native_adoption
    finally:
        native_adoption.executor.intents.journal.close()
        journal = getattr(native_adoption.trader, "_server_order_journal", None)
        if journal is not None:
            journal.journal.close()


def test_aliasless_provisional_adoption_cannot_recount_after_permanent_id_enrichment(
    legacy_adoption_context,
):
    ctx = legacy_adoption_context
    stop = place_external_protection(ctx)
    provisional = observe_fill(ctx, stop, 10, "Submitted")
    assert provisional["permId"] == 0
    # Represent already committed pre-upgrade bytes: the numeric-only adapter
    # had applied ten fills under its provisional adopt-* identity. The new
    # code must not infer a new physical order from an opaque hash mismatch.
    legacy_id = persist_legacy_adoption(ctx, provisional)

    stop.order.permId = 1701
    stop.orderStatus.permId = 1701
    promoted = observe_fill(ctx, stop, 20, "Submitted")
    assert promoted["permId"] == 1701
    assert promoted["brokerIntentCreatedAt"] > 0

    ctx.executor.intents.journal.close()
    for _ in range(2):
        restarted = AutoExecutor(ctx.path, paper_trading=True, sdk_factory=lambda: ctx.sdk)
        try:
            # The old row has no saved broker alias proving the enrichment.
            # Preserve the refusal until identity can be reconciled; do not
            # debit all twenty cumulative fills under a newly invented key.
            with pytest.raises(ValueError):
                restarted._reconcile_intents(OWNER, CONID)
            saved, = restarted.intents.all(kind="PROTECTIVE")
            assert saved["intent_id"] == legacy_id
            assert not saved["payload"].get("broker_intent_id")
            assert saved["payload"]["cumulative_filled"] == 10
            assert owned_quantity(restarted) == 30
            _positions, checkpoints = restarted.state.ownership_snapshot(restarted.intents.all())
            assert checkpoints == {legacy_id: 10}
        finally:
            restarted.intents.journal.close()

    assert ctx.trader.inventory == 120
    assert len(ctx.trader.placed) == 1
    ctx.trader.client.ib.cancelOrder.assert_not_called()
