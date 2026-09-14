"""A durable close request ends with its exposure, not an unrelated later entry.

The ordinary recovery fixture uses a fake broker with separate order acceptance,
fills and cancellation, plus a temporary database. No service is contacted.
"""
import pandas as pd
import pytest

from review.test_review_strategy_contract import FakeResult, TS, make_work
from test_execution_recovery import recovery
from trader.objects import Action
from trader.strategy.auto_executor import AutoExecutor, BarWork
from trader.strategy.execution_queue import ExecutionWorkQueue


def restart(path, sdk):
    return AutoExecutor(path, paper_trading=True, cooldown_seconds=0,
                        sdk_factory=lambda: sdk)


def physical_sells(sdk):
    return [row for row in sdk.accepted if row['action'] == 'SELL']


@pytest.mark.parametrize('completion', ['cancelled_full_fill', 'filled', 'external_flat'])
def test_terminal_explicit_close_cannot_close_a_later_unrelated_entry(
        recovery, monkeypatch, completion):
    executor, sdk, path = recovery
    executor.cooldown_seconds = 0
    manual = 0 if completion == 'external_flat' else 100
    sdk.broker[1111] = manual
    positions = sdk.positions

    def complete_positions():
        frame = positions()
        frame.attrs['complete'] = True
        return frame

    # This fake observes the complete book, including a conclusive empty book
    # when an independent external sale has removed the original position.
    monkeypatch.setattr(sdk, 'positions', complete_positions)
    executor._process_signal(make_work(quantity=140))
    sdk.fill_next = None if completion == 'filled' else 0
    executor._process_signal(make_work(action=Action.SELL,
                                       bar_ts=TS + pd.Timedelta(seconds=1)))
    close = executor.intents.all(kind='CLOSE')[0]
    assert close['payload']['policy_entry_bar_ts'] is None
    assert len(physical_sells(sdk)) == 1
    if completion != 'filled':
        physical_sells(sdk)[0].update(
            status='Cancelled', filled=140 if completion == 'cancelled_full_fill' else 0)
        sdk.broker[1111] = manual

    # Repeated flat management and a process restart must retire the satisfied
    # desire, while retaining the original broker observation as audit history.
    for _ in range(3):
        executor.manage_positions()
    assert executor.state.open_position('orb_test', 1111) is None
    assert sdk.broker[1111] == manual
    assert len(physical_sells(sdk)) == 1
    executor = restart(path, sdk)
    for _ in range(2):
        executor.manage_positions()
    assert len(physical_sells(sdk)) == 1

    sdk.fill_next = None
    executor._process_signal(make_work(quantity=80,
                                       bar_ts=TS + pd.Timedelta(minutes=1)))
    assert executor.state.open_position('orb_test', 1111)['quantity'] == 80
    for _ in range(3):
        executor.manage_positions()
    executor = restart(path, sdk)
    executor.manage_positions()
    position = executor.state.open_position('orb_test', 1111)
    assert position is not None, 'the completed close request consumed an unrelated later entry'
    assert position['quantity'] == 80
    assert sdk.broker[1111] == manual + 80
    assert [row['action'] for row in sdk.accepted] == ['BUY', 'SELL', 'BUY']


@pytest.mark.parametrize(('close_status', 'delivery'), [
    ('Cancelled', 'direct'), ('Filled', 'direct'),
    ('Filled', 'overflow'), ('Filled', 'broker_recovery'),
])
def test_terminal_close_desire_survives_flat_ownership_while_open_fill_is_unknown(
        recovery, monkeypatch, close_status, delivery):
    executor, sdk, path = recovery
    sdk.broker[1111] = 100  # unrelated manual shares
    sdk.fill_next = 40
    executor._process_signal(make_work(quantity=140))
    opening = sdk.accepted[0]
    # IB has ended the BUY, but bounded replay cannot yet prove its final
    # executed quantity. The known 40 shares remain safe to close.
    opening.update(status='Cancelled', fillQuantityKnown=False)
    sdk.fill_next = 0
    work = make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(seconds=1))
    if delivery == 'broker_recovery':
        # An emergency coordinator canceled the protection and sold 40 while
        # the local close journal was unavailable. Restart must discover its
        # broker reference and retain the same late-fill reduction request.
        for order_id in list(sdk.active_stops):
            sdk.cancel(order_id)
        created_ms = int(work.bar_ts.tz_localize('UTC').timestamp() * 1000)
        epoch = executor.state.open_position('orb_test', 1111)['ownership_epoch']
        emergency_id = f'emergency-{created_ms:x}-terminal-lifetime-epoch-{epoch}'
        sdk.accepted.append(dict(
            orderId=1999, orderRef='orb_test|mmr:' + emergency_id,
            clientIntentId=emergency_id, conId=1111, action='SELL',
            orderType='LMT', totalQuantity=40, filled=40,
            avgFillPrice=100, status='Filled'))
        assert executor.intents.all(kind='CLOSE') == []
        executor = restart(path, sdk)
    else:
        if delivery == 'overflow':
            executor._queue = ExecutionWorkQueue(opening_capacity=0, exit_capacity=0)
            monkeypatch.setattr(executor, 'start', lambda: None)
            assert executor.submit_signal(work)
            assert executor.status_metrics()['overflow_exit_intents'] == 1
            assert physical_sells(sdk) == []
            executor.manage_positions()
            assert executor.status_metrics()['overflow_exit_intents'] == 0
        else:
            executor._process_signal(work)
        physical_sells(sdk)[0].update(status=close_status, filled=40)
    sdk.broker[1111] = 100
    sdk.fill_next = None
    for _ in range(3):
        executor.manage_positions()
    assert executor.state.open_position('orb_test', 1111) is None
    assert executor.intents.all(kind='OPEN', active=True)[0]['status'] == 'UNKNOWN'
    assert sdk.broker[1111] == 100
    assert [row['filled'] for row in physical_sells(sdk)] == [40]

    executor = restart(path, sdk)
    for _ in range(2):
        executor.manage_positions()
    assert [row['filled'] for row in physical_sells(sdk)] == [40]

    # A delayed receipt for that SAME BUY proves 20 additional shares. They
    # belong to the original exit request, even though ownership was flat in
    # between, and must be closed once without selling the manual 100.
    opening.update(filled=60, fillQuantityKnown=True)
    sdk.broker[1111] = 120
    for _ in range(3):
        executor.manage_positions()
    assert executor.state.open_position('orb_test', 1111) is None
    assert sdk.broker[1111] == 100
    assert [row['filled'] for row in physical_sells(sdk)] == [40, 20]
    executor = restart(path, sdk)
    for _ in range(2):
        executor.manage_positions()
    assert [row['filled'] for row in physical_sells(sdk)] == [40, 20]
    assert sdk.broker[1111] == 100


def test_close_waits_for_a_working_open_even_when_no_owned_row_exists(recovery):
    executor, sdk, path = recovery
    sdk.broker[1111] = 100
    sdk.fill_next = 0
    executor._process_signal(make_work(quantity=140))
    sdk.cancel_fails = True
    executor._process_signal(make_work(action=Action.SELL,
                                       bar_ts=TS + pd.Timedelta(seconds=1)))
    for _ in range(3):
        executor.manage_positions()
    assert executor.state.open_position('orb_test', 1111) is None
    assert executor.intents.all(kind='OPEN', active=True)[0]['status'] == 'WORKING'
    assert len(executor.intents.all(kind='CLOSE', active=True)) == 1
    assert physical_sells(sdk) == []

    executor = restart(path, sdk)
    executor.manage_positions()
    assert len(executor.intents.all(kind='CLOSE', active=True)) == 1
    assert physical_sells(sdk) == []
    # Cancellation eventually confirms a partial execution. The existing
    # close request now reduces exactly that execution, without another signal.
    sdk.accepted[0].update(status='Cancelled', filled=40)
    sdk.broker[1111] = 140
    sdk.cancel_fails = False
    sdk.fill_next = None
    for _ in range(3):
        executor.manage_positions()
    assert executor.state.open_position('orb_test', 1111) is None
    assert sdk.broker[1111] == 100
    assert [row['filled'] for row in physical_sells(sdk)] == [40]
    executor = restart(path, sdk)
    executor.manage_positions()
    assert [row['filled'] for row in physical_sells(sdk)] == [40]


@pytest.mark.parametrize(('terminal', 'first_fill'), [('Filled', 40), ('Cancelled', 20)])
@pytest.mark.parametrize('delivery', ['direct', 'overflow', 'emergency_restore'])
@pytest.mark.parametrize('receipt_timing', ['after_flat', 'before_terminal'])
def test_explicit_sell_keeps_preexisting_different_epoch_open_after_timer_handoff(
        recovery, monkeypatch, terminal, first_fill, delivery, receipt_timing):
    executor, sdk, path = recovery
    executor.cooldown_seconds = 0
    sdk.broker[1111] = 100
    executor._process_signal(make_work(quantity=40, max_hold_bars=1))  # held entry B

    # C was already submitted before SELL A. It has a different entry bar
    # from B, and broker completion has not proved its final executed amount.
    sdk.fill_next = 0
    entry_c = TS + pd.Timedelta(seconds=10)
    executor._process_signal(make_work(quantity=60, pyramid_max_adds=1,
                                       bar_ts=entry_c))
    opening_c = sdk.accepted[-1]
    assert opening_c['action'] == 'BUY'
    opening_c.update(status='Cancelled', fillQuantityKnown=False)
    sell = make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(seconds=20))
    if delivery == 'emergency_restore':
        def emergency_submit(**kwargs):
            assert kwargs['quantity'] == 40
            for order_id in list(sdk.active_stops):
                sdk.cancel(order_id)
            sdk.accepted.append(dict(
                orderId=1999, orderRef='orb_test|mmr:' + kwargs['client_intent_id'],
                clientIntentId=kwargs['client_intent_id'], conId=1111, action='SELL',
                orderType='LMT', totalQuantity=40, filled=0,
                avgFillPrice=100, status='Submitted'))
            return FakeResult(obj=[1999])

        def unavailable(*args, **kwargs):
            raise OSError('close journal unavailable')

        monkeypatch.setattr(sdk, 'emergency_close_position', emergency_submit, raising=False)
        expected_faults = []
        with monkeypatch.context() as patch:
            patch.setattr(executor.intents, 'create', unavailable)
            # Capture the deliberate fault without formatting an instrumented
            # traceback during mutation. Other behavior assertions stay live.
            patch.setattr('trader.strategy.auto_executor.logging.exception',
                          lambda message, *args, **kwargs: expected_faults.append(message))
            executor._process_signal(sell)
        assert expected_faults == [
            'auto-executor: durable exit journal unavailable; retaining emergency reduction']
        assert executor.intents.all(kind='CLOSE') == []
        executor.manage_positions()  # restore the observed A using retained emergency authority
        assert len(executor.intents.all(kind='CLOSE', active=True)) == 1
    elif delivery == 'overflow':
        executor._queue = ExecutionWorkQueue(opening_capacity=0, exit_capacity=0)
        monkeypatch.setattr(executor, 'start', lambda: None)
        assert executor.submit_signal(sell)
        executor.manage_positions()
    else:
        executor._process_signal(sell)
    assert len(physical_sells(sdk)) == 1
    assert executor.intents.all(kind='OPEN', active=True)[0]['status'] == 'UNKNOWN'

    # The retained B timer must not narrow the earlier explicit SELL's
    # authority over C, or authorize another SELL while A remains executable.
    due_bar = TS + pd.Timedelta(minutes=1)
    executor._process_bar(BarWork('orb_test', 1111, due_bar, 1,
                                  entry_bar_ts=TS, observed_bar_timestamps=(due_bar,)))
    assert len(physical_sells(sdk)) == 1
    if receipt_timing == 'before_terminal':
        # C changes the owned epoch while A is still executable. Replaying
        # that receipt must retain A, without sending a competing SELL.
        opening_c.update(filled=20, fillQuantityKnown=True)
        sdk.broker[1111] += 20
        executor = restart(path, sdk)
        executor.manage_positions()
        assert executor.state.open_position('orb_test', 1111)['quantity'] == 60
        assert len(physical_sells(sdk)) == 1
    physical_sells(sdk)[0].update(status=terminal, filled=first_fill)
    sdk.broker[1111] -= first_fill
    sdk.fill_next = None
    executor = restart(path, sdk)
    for _ in range(3):
        executor.manage_positions()
    assert executor.state.open_position('orb_test', 1111) is None
    assert sdk.broker[1111] == 100
    if receipt_timing == 'after_flat':
        assert executor.intents.all(kind='OPEN', active=True)[0]['status'] == 'UNKNOWN'
        # A delayed receipt proves 20 shares of that captured C intent. These
        # remain within SELL A's request even after B flattened and a restart.
        opening_c.update(filled=20, fillQuantityKnown=True)
        sdk.broker[1111] += 20
        executor = restart(path, sdk)
        for _ in range(3):
            executor.manage_positions()
        assert executor.state.open_position('orb_test', 1111) is None
        assert sdk.broker[1111] == 100
    assert executor.intents.all(kind='OPEN', active=True) == []
    expected_fills = ([first_fill, 60 - first_fill] if receipt_timing == 'before_terminal'
                      else [40, 20] if first_fill == 40 else [20, 20, 20])
    assert [row['filled'] for row in physical_sells(sdk)] == expected_fills
    executor = restart(path, sdk)
    for _ in range(2):
        executor.manage_positions()
    assert [row['filled'] for row in physical_sells(sdk)] == expected_fills
    assert sdk.broker[1111] == 100

    # D was neither owned nor submitted when the original SELL was received.
    # Completed captured C authority must not leak into this future entry.
    future_bar = TS + pd.Timedelta(minutes=2)
    monkeypatch.setattr(AutoExecutor, '_now_utc', lambda self:
                        (future_bar.tz_localize('UTC') + pd.Timedelta(seconds=70)).to_pydatetime())
    executor._process_signal(make_work(quantity=80, bar_ts=future_bar))
    for _ in range(3):
        executor.manage_positions()
    executor = restart(path, sdk)
    executor.manage_positions()
    assert executor.state.open_position('orb_test', 1111)['quantity'] == 80
    assert sdk.broker[1111] == 180
    assert [row['filled'] for row in physical_sells(sdk)] == expected_fills


def test_timer_only_exit_does_not_capture_a_pending_different_epoch_open(recovery):
    executor, sdk, path = recovery
    executor.cooldown_seconds = 0
    sdk.broker[1111] = 100
    executor._process_signal(make_work(quantity=40, max_hold_bars=1))
    sdk.fill_next = 0
    entry_c = TS + pd.Timedelta(seconds=10)
    executor._process_signal(make_work(quantity=60, pyramid_max_adds=1, bar_ts=entry_c))
    opening_c = sdk.accepted[-1]
    opening_c.update(status='Cancelled', fillQuantityKnown=False)
    sdk.fill_next = None
    due_bar = TS + pd.Timedelta(minutes=1)
    timer_b = BarWork('orb_test', 1111, due_bar, 1,
                      entry_bar_ts=TS, observed_bar_timestamps=(due_bar,))
    executor._process_bar(timer_b)
    for _ in range(3):
        executor.manage_positions()
    assert executor.state.open_position('orb_test', 1111) is None
    assert sdk.broker[1111] == 100
    assert [row['filled'] for row in physical_sells(sdk)] == [40]

    # There was no explicit SELL, so a delayed C receipt remains managed
    # under C's own policy instead of acquiring B's earlier timer authority.
    opening_c.update(filled=20, fillQuantityKnown=True)
    sdk.broker[1111] = 120
    executor = restart(path, sdk)
    for _ in range(3):
        executor.manage_positions()
    executor._process_bar(timer_b)
    executor = restart(path, sdk)
    executor.manage_positions()
    position = executor.state.open_position('orb_test', 1111)
    assert position['quantity'] == 20
    assert position['entry_bar_ts'] == entry_c
    assert sdk.broker[1111] == 120
    assert [row['filled'] for row in physical_sells(sdk)] == [40]
