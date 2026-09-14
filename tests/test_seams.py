"""Seam tests — verified components, composed (Appendix A, Door 2).

Every bug in the 2026-07-27 live battery that wasn't an environment fact
lived BETWEEN components that each passed their own tests: resize mutated
broker state the auto-executor tracked; the executor trusted a registry the
subscription path never wrote. Per-component verification cannot see these —
the mutation score of A is silent about what B does to A's assumptions.

The harness here is one shared broker state that both sides mutate FOR REAL:
`execute_resize_plan` runs against it through a stubbed RPC, and the
auto-executor reads it through its SDK. No test injects the post-resize
state by hand — the resize produces it, which is the entire point.
"""
import datetime as dt
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest

from trader.objects import Action
from trader.common.reactivex import SuccessFail
from trader.sdk import MMR
from trader.strategy.auto_executor import AutoExecutor, BarWork
from trader.strategy.execution_intents import timestamp_text
from trader.trading.strategy import Signal

# A fixed bar timestamp, kept deterministic on purpose. These tests drive the
# OPEN path, which the stale-bar gate guards; the age they are judged against
# is frozen by the _freeze_bar_age fixture below rather than taken from the
# wall clock, so the same code always produces the same result.
from trader.strategy import auto_executor as auto_executor_module

TS = pd.Timestamp('2026-07-06 10:39:00')

@pytest.fixture(autouse=True)
def _freeze_clock(monkeypatch):
    """Pin "now" for this module, without stubbing anything real.

    The stale-bar gate measures a bar against the wall clock, so these tests
    are time-dependent. Making the fixture timestamp dynamic fixed that and
    made the mutation score move without the code moving (~30 mutants flipping
    between identical runs). Stubbing `bar_age_seconds` fixed THAT and cost ~26
    points of coverage, because the function's own mutants became unreachable.

    Overriding only the clock costs neither: the real `bar_age_seconds` runs
    against the real fixture timestamp, and the age it computes is a fixed 10
    seconds. Tests that are actually about staleness call `decide_signal`
    directly with an explicit age and are untouched by this.
    """
    frozen = TS.tz_localize('UTC') + pd.Timedelta(seconds=10)
    monkeypatch.setattr(auto_executor_module.AutoExecutor, '_now_utc',
                        lambda self: frozen.to_pydatetime())



class SeamBroker:
    """The single source of broker truth both components mutate."""

    def __init__(self):
        self.positions = {}        # conid -> qty
        self.orders = []           # dicts: orderId/orderRef/conId/status/...
        self.next_id = 700

    def live_orders(self):
        return [o for o in self.orders if o['status'] in ('PreSubmitted', 'Submitted')]

    def add_order(self, **kw):
        oid = self.next_id
        self.next_id += 1
        self.orders.append({'orderId': oid, 'status': 'PreSubmitted',
                            'action': 'SELL', 'orderType': 'STP', 'tif': 'GTC',
                            'orderRef': '', 'auxPrice': 0.0, 'quantity': 0.0,
                            'conId': 0, 'filled': 0.0, 'clientIntentId': '',
                            'totalQuantity': kw.get('quantity', 0.0), **kw})
        return oid

    def cancel(self, oid):
        for o in self.orders:
            if o['orderId'] == int(oid) and o['status'] in ('PreSubmitted', 'Submitted'):
                o['status'] = 'Cancelled'
                return True
        return False


class ExecutorSDK:
    """The auto-executor's view of the SeamBroker (FakeSDK-shaped)."""

    def __init__(self, broker: SeamBroker, conid=578031277, symbol='QBTS'):
        self.b = broker
        self.conid = conid
        self.secdef = SimpleNamespace(symbol=symbol, exchange='NASDAQ',
                                      primaryExchange='NASDAQ', currency='USD',
                                      secType='STK', conId=conid)
        self.avg_cost = 19.0
        self.proposals = {}
        self.next_pid = 100
        self.propose_calls = []

    def resolve(self, symbol, sec_type='STK', exchange='', universe='', currency=''):
        return [self.secdef]

    def positions(self):
        rows = [{'conId': c, 'position': q, 'avgCost': self.avg_cost}
                for c, q in self.b.positions.items() if q != 0]
        frame = pd.DataFrame(rows, columns=['conId', 'position', 'avgCost'])
        frame.attrs['complete'] = True  # The shared fake is an authoritative snapshot, even when flat.
        return frame

    def trades(self):
        return pd.DataFrame(self.b.orders)

    def place_protective_order(self, **kw):
        oid = self.b.add_order(orderRef=kw.get('order_ref', ''),
                               conId=kw.get('con_id', self.conid),
                               auxPrice=kw.get('aux_price', 0.0),
                               quantity=kw.get('quantity', 0.0),
                               clientIntentId=kw.get('client_intent_id', ''))
        return SimpleNamespace(is_success=lambda: True, error=None,
                               obj=SimpleNamespace(order=SimpleNamespace(orderId=oid)))

    def cancel(self, oid):
        ok = self.b.cancel(oid)
        return SimpleNamespace(is_success=lambda: ok,
                               error=None if ok else 'not found')

    def propose(self, **kw):
        pid = self.next_pid
        self.next_pid += 1
        self.propose_calls.append(kw)
        self.proposals[pid] = SimpleNamespace(quantity=kw.get('quantity'),
                                              metadata=kw.get('metadata') or {})
        return pid, None, None

    def approve(self, pid):
        p = self.proposals[pid]
        qty = float(p.quantity or 0)
        action = self.propose_calls[-1]['action']
        cur = self.b.positions.get(self.conid, 0.0)
        self.b.positions[self.conid] = cur + qty if action == 'BUY' else cur - qty
        # A broker position mutation is a fill, not a submitted quantity. Give
        # both components the same execution evidence and durable identity.
        oid = self.b.add_order(action=action, conId=self.conid, orderType='MKT',
                              quantity=qty, filled=qty, remaining=0.0,
                              status='Filled', avgFillPrice=self.avg_cost,
                              orderRef=p.metadata.get('strategy', 'proposal'),
                              clientIntentId=p.metadata.get('client_intent_id', ''))
        return SimpleNamespace(is_success=lambda: True, error=None, obj=[oid])

    def _proposal_store(self):
        store = MagicMock()
        store.get = lambda pid: self.proposals.get(pid)
        return store


def _resize_mmr(broker: SeamBroker, conid=578031277, symbol='QBTS'):
    """An MMR wired so execute_resize_plan's effects land in the SeamBroker."""
    mmr = MMR.__new__(MMR)
    captured = []

    class _Client:
        is_setup = True

        def rpc(self, return_type=None):
            svc = MagicMock()

            def resize_position(contract, target_quantity, client_intent_id):
                """Model the server's supported matching-tranche handoff.

                The fake broker fills the trim before returning its receipt.
                OCA type 2 consumes the paired stop; the residual tranche is
                untouched. Arbitrary single-stop splits remain unsupported.
                The client plan's protective metadata is never authoritative.
                """
                assert contract.conId == conid
                cur = broker.positions[conid]
                trim = cur - target_quantity
                if target_quantity < 0 or trim < 0:
                    return SuccessFail.fail(error='DEFERRED: unsupported grow/flip')
                protective = [o for o in broker.live_orders() if o['conId'] == conid]
                paired = next((o for o in protective if o['quantity'] == trim), None)
                if protective and (paired is None or sum(o['quantity'] for o in protective) != cur):
                    return SuccessFail.fail(error='DEFERRED: no supported protective tranche')
                if paired:
                    paired['ocaGroup'] = client_intent_id
                    paired['ocaType'] = 2
                    captured.append(dict(paired))
                    paired.update(status='Cancelled', quantity=0.0, remaining=0.0)
                broker.positions[conid] = target_quantity
                oid = broker.add_order(action='SELL', conId=conid, orderType='MKT',
                                       quantity=trim, filled=trim, remaining=0.0,
                                       status='Filled', avgFillPrice=19.0,
                                       clientIntentId=client_intent_id, orderRef='resize')
                return SuccessFail.success(obj={'status': 'SUBMITTED', 'order_ids': [oid]})

            svc.resize_position = resize_position
            return svc

    mmr._client = _Client()
    mmr._contract_map = {symbol: SimpleNamespace(conId=conid)}
    mmr.cancel = lambda oid: SimpleNamespace(is_success=lambda: broker.cancel(oid))

    def _place_order(sym, action, amount, quantity, limit_price, market, **kw):
        qty = float(quantity or 0)
        cur = broker.positions.get(conid, 0.0)
        broker.positions[conid] = cur + qty if action == 'BUY' else cur - qty
        return SimpleNamespace(is_success=lambda: True, error=None)

    mmr._place_order = _place_order
    mmr._captured = captured
    return mmr


def _plan_for(broker, conid, symbol, target_qty):
    cur = broker.positions[conid]
    associated = [{'orderId': o['orderId'], 'orderType': o['orderType'],
                   'action': o['action'], 'quantity': o['quantity'],
                   'auxPrice': o['auxPrice'], 'lmtPrice': 0.0,
                   'trailingPercent': 0.0, 'tif': o['tif'],
                   'orderRef': o['orderRef']}
                  for o in broker.live_orders() if o['conId'] == conid]
    return {'current_total': cur * 19.0, 'target_total': target_qty * 19.0,
            'scale_factor': target_qty / cur,
            'adjustments': [{'symbol': symbol, 'conId': conid,
                             'current_qty': float(cur), 'target_qty': float(target_qty),
                             'delta_qty': float(target_qty - cur), 'action': 'SELL',
                             'current_value': cur * 19.0,
                             'target_value': target_qty * 19.0,
                             'associated_orders': associated}]}


@pytest.fixture
def seam(tmp_path):
    broker = SeamBroker()
    sdk = ExecutorSDK(broker)
    ex = AutoExecutor(duckdb_path=str(tmp_path / 'seam.duckdb'),
                      paper_trading=True, sdk_factory=lambda: sdk)
    ex._reconciled = True
    return broker, sdk, ex


CONID = 578031277


class TestResizeThenExecutor:
    """The exact live sequence of 2026-07-27, end to end through BOTH
    components — not a hand-injected imitation of the post-resize state."""

    def _open_with_stop(self, broker, ex):
        broker.positions[CONID] = 20.0
        # Price the holding from its own matched fill before exercising the
        # resize handoff; account-level average cost is not strategy evidence.
        entry = ex.intents.create('probe', CONID, 'OPEN',
                                  dict(bar_ts=timestamp_text(TS), quantity=20.0),
                                  status='WORKING')
        entry_id = broker.add_order(action='BUY', conId=CONID, orderType='MKT',
                                    quantity=20.0, filled=20.0, remaining=0.0,
                                    status='Filled', avgFillPrice=19.0,
                                    orderRef='probe', clientIntentId=entry['intent_id'])
        ex.intents.update(entry, order_ids=[entry_id])
        ex._reconcile_intents('probe', CONID)
        # These are fresh server-claimed stops. Preserve their original
        # identity/time through resize and later reads, as native snapshots do.
        oid = broker.add_order(orderRef='probe', conId=CONID,
                               auxPrice=17.5, quantity=10.0,
                               clientIntentId=f'protective:seam-{broker.next_id}',
                               brokerIntentCreatedAt=time.time())
        broker.add_order(orderRef='probe', conId=CONID, auxPrice=17.5, quantity=10.0,
                         clientIntentId=f'protective:seam-{broker.next_id}',
                         brokerIntentCreatedAt=time.time())
        ex.state.set_protective('probe', CONID, oid)
        return oid

    def test_resize_recreate_is_adopted_and_close_leaves_nothing(self, seam):
        broker, sdk, ex = seam
        orig_stop = self._open_with_stop(broker, ex)

        # --- the other component acts ---
        mmr = _resize_mmr(broker)
        results = mmr.execute_resize_plan(_plan_for(broker, CONID, 'QBTS', 10))
        assert results['failures'] == []
        assert broker.positions[CONID] == 10.0
        assert not any(o['orderId'] == orig_stop for o in broker.live_orders())
        (new_stop,) = [o for o in broker.live_orders() if o['conId'] == CONID]
        assert new_stop['orderRef'] == 'probe', 'resize must preserve attribution'

        # --- the executor meets the aftermath ---
        ex._process_bar(BarWork('probe', CONID, TS + pd.Timedelta(minutes=1), 1))
        assert ex.state.open_position('probe', CONID)['protective_order_id'] == \
            new_stop['orderId'], 'dead tracked id was not adopted to the re-created stop'

        # --- and closes clean: nothing of ours survives the position ---
        from trader.strategy.auto_executor import SignalWork
        ex._process_signal(SignalWork(
            strategy_name='probe', conid=CONID, action=Action.SELL,
            bar_ts=TS + pd.Timedelta(minutes=2), bar_size_seconds=60.0, auto_execute=True,
            state_running=True))
        # Resize is external to strategy fill attribution. Independent
        # management reconciles the residual attribution against confirmed
        # flat broker state after the strategy's actual close fill.
        ex.manage_positions()
        assert ex.state.open_position('probe', CONID) is None
        assert [o for o in broker.live_orders() if o['conId'] == CONID] == [], (
            'an orphaned GTC stop survived the close — fires into a short later')

    def test_stale_plan_cannot_strip_broker_protective_attribution(self, seam):
        """The coordinator reads broker ownership instead of copying a stale
        client's protective metadata, preventing the original lost-ref bug."""
        broker, sdk, ex = seam
        self._open_with_stop(broker, ex)
        mmr = _resize_mmr(broker)
        plan = _plan_for(broker, CONID, 'QBTS', 10)
        plan['adjustments'][0]['associated_orders'][0]['orderRef'] = ''   # the old bug
        results = mmr.execute_resize_plan(plan)
        assert results['failures'] == []
        (new_stop,) = [o for o in broker.live_orders() if o['conId'] == CONID]
        assert new_stop['orderRef'] == 'probe'

        from trader.strategy.auto_executor import SignalWork
        ex._process_signal(SignalWork(
            strategy_name='probe', conid=CONID, action=Action.SELL,
            bar_ts=TS + pd.Timedelta(minutes=2), bar_size_seconds=60.0, auto_execute=True,
            state_running=True))
        ex.manage_positions()
        assert ex.state.open_position('probe', CONID) is None
        assert [o for o in broker.live_orders() if o['conId'] == CONID] == []


class TestResizeThenReconcile:
    def test_partial_trim_keeps_attribution(self, seam):
        broker, sdk, ex = seam
        broker.positions[CONID] = 20.0
        ex.state.record_open('probe', CONID, 10.0, TS, None, None, None)
        mmr = _resize_mmr(broker)
        mmr.execute_resize_plan(_plan_for(broker, CONID, 'QBTS', 12))
        ex._reconciled = False
        ex._reconcile_once()
        assert ex.state.open_position('probe', CONID) is not None, (
            'a partial trim (broker 12 >= attributed 10) must not strip attribution')

    def test_trim_to_zero_marks_closed_externally_and_cancels_tracked_stop(self, seam):
        broker, sdk, ex = seam
        broker.positions[CONID] = 10.0
        broker.positions[999] = 5.0   # a second holding: the post-close book is
                                      # readable, not ALL-empty — an all-empty
                                      # read is (correctly) inconclusive under
                                      # the empty-broker grace and would defer
                                      # reconciliation instead of testing it
        ex.state.record_open('probe', CONID, 10.0, TS, None, None, None)
        oid = broker.add_order(orderRef='probe', conId=CONID,
                               auxPrice=17.5, quantity=10.0,
                               clientIntentId=f'protective:seam-{broker.next_id}',
                               brokerIntentCreatedAt=time.time())
        ex.state.set_protective('probe', CONID, oid)
        mmr = _resize_mmr(broker)
        plan = _plan_for(broker, CONID, 'QBTS', 0)
        plan['adjustments'][0]['associated_orders'] = []   # resize closed it outright
        mmr.execute_resize_plan(plan)
        assert broker.positions[CONID] == 0.0
        ex._reconciled = False
        ex._reconcile_once()
        assert ex.state.open_position('probe', CONID) is None
        assert not any(o['orderId'] == oid for o in broker.live_orders()), (
            'reconcile must cancel the orphaned tracked stop of an externally-closed position')
