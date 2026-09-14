"""Reconciliation cost is bounded: settled intents stop costing a broker read.

Before this fix every CANCELLED intent — one per closed lifecycle, kept forever
— was reconciled with its own intent-scoped ``execution_snapshot`` round-trip on
EVERY management cycle (the runtime's 1 Hz pulse plus each idle worker wake).
Cancelled intents are still read for a while (IB can report Cancelled and then a
late fill), but on a backoff, pulled forward at once when the global snapshot
already fetched that cycle shows their rows changed, and settled for good once
unchanged for TERMINAL_SETTLE_SECONDS. Live intents are deliberately still read
every cycle: their scoped completeness is not derivable from the global view.
"""
import json
import time

from review.test_review_strategy_contract import TS
from test_intent_support import CONID, OWNER, SnapshotSDK, build_executor, clock, open_owned  # noqa: F401
from trader.strategy import auto_executor as auto_executor_module
from trader.strategy.execution_intents import timestamp_text


def _cancelled_protective(executor, sdk, index, *, row=None):
    """What a closed lifecycle leaves behind: a CANCELLED stop with a checkpoint."""
    position = executor.state.open_position(OWNER, CONID)
    oid = 5000 + index
    intent_id = f'auto-cancelled-{index:04d}'
    payload = dict(bar_ts=timestamp_text(TS), quantity=140.0, stop_price=92.0, order_ids=[oid],
                   ownership_epoch=position['ownership_epoch'],
                   ownership_started_at=position['ownership_started_at'], cumulative_filled=0.0)
    with executor.intents.journal.transaction() as conn:
        conn.execute('INSERT INTO execution_intents VALUES (?, ?, ?, ?, ?, ?, ?)',
                     [intent_id, OWNER, CONID, 'PROTECTIVE', 'CANCELLED', json.dumps(payload), time.time()])
    if row is None:
        sdk.cancelled_stops[oid] = dict(order_ref=OWNER, con_id=CONID, quantity=140.0, client_intent_id=intent_id)
    else:
        row.update(orderId=oid, clientIntentId=intent_id)
        sdk.accepted.append(row)
    return intent_id


def test_cancelled_intents_cost_at_most_one_shared_read_per_cycle(tmp_path, monkeypatch, clock):
    sdk = SnapshotSDK()
    executor, _ = build_executor(tmp_path, sdk, monkeypatch)
    open_owned(executor)
    cancelled = {_cancelled_protective(executor, sdk, index) for index in range(12)}
    working, = (intent['intent_id'] for intent in executor.intents.all(kind='PROTECTIVE', active=True))
    sdk.snapshot_calls.clear()

    executor.manage_positions()  # the first pass reads every candidate once
    assert cancelled <= {call[0] for call in sdk.scoped_calls()}

    sdk.snapshot_calls.clear()
    cycles = 8
    for _ in range(cycles):
        executor.manage_positions()
    scoped = sdk.scoped_calls()
    # Only the live protective is read per cycle; the twelve cancelled intents
    # share the one global read that proves nothing about them changed.
    assert {call[0] for call in scoped} == {working}, scoped
    assert len(sdk.snapshot_calls) <= 2 * cycles

    clock.advance(auto_executor_module.TERMINAL_SETTLE_SECONDS + 1)
    sdk.snapshot_calls.clear()
    executor.manage_positions()  # due again: one confirming read each, then settled
    assert cancelled <= {call[0] for call in sdk.scoped_calls()}
    settled = {intent['intent_id'] for intent in executor.intents.all(kind='PROTECTIVE')
               if intent['payload'].get('reconciled_terminal') is True}
    assert settled == cancelled

    clock.advance(1000)
    sdk.snapshot_calls.clear()
    for _ in range(3):
        executor.manage_positions()
    assert not ({call[0] for call in sdk.scoped_calls()} & cancelled), 'settled intents are never read again'
    # They remain listed for the operator; settling is a reconciliation fact, not deletion.
    assert cancelled <= {row['intent_id'] for row in executor.list_execution_intents(active_only=False)}


def test_a_late_fill_on_a_cancelled_intent_is_read_at_once(tmp_path, monkeypatch, clock):
    """Backoff never hides broker truth: a changed row pulls the read forward."""
    sdk = SnapshotSDK()
    executor, _ = build_executor(tmp_path, sdk, monkeypatch)
    open_owned(executor)
    row = dict(orderRef=OWNER, conId=CONID, status='Cancelled', action='SELL', orderType='STP',
               totalQuantity=140.0, filled=0.0, avgFillPrice=0.0)
    late = _cancelled_protective(executor, sdk, 0, row=row)
    executor.manage_positions()

    sdk.snapshot_calls.clear()
    executor.manage_positions()
    assert late not in {call[0] for call in sdk.scoped_calls()}, 'unchanged and inside its backoff'

    row.update(filled=40.0, avgFillPrice=92.0)  # IB reports the fill after the cancel
    sdk.broker[CONID] = 100
    sdk.snapshot_calls.clear()
    executor.manage_positions()
    assert late in {call[0] for call in sdk.scoped_calls()}
    saved = executor.intents.get(late)
    assert saved['payload']['cumulative_filled'] == 40.0 and saved['status'] == 'CANCELLED'
    assert executor.state.open_position(OWNER, CONID)['quantity'] == 100


def test_a_live_protective_is_still_read_every_cycle(tmp_path, monkeypatch, clock):
    sdk = SnapshotSDK()
    executor, _ = build_executor(tmp_path, sdk, monkeypatch)
    open_owned(executor)
    working, = (intent['intent_id'] for intent in executor.intents.all(kind='PROTECTIVE', active=True))
    executor.manage_positions()
    for _ in range(3):
        sdk.snapshot_calls.clear()
        executor.manage_positions()
        assert working in {call[0] for call in sdk.scoped_calls()}
