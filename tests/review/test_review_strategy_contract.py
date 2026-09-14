"""Pinned strategy execution regressions from the 2026-09-08 review.

The original counterexamples now run as ordinary assertions against the
repaired contract. Only fake SDKs and temporary databases are used; broader
restart/fault scenarios live in tests/test_execution_recovery.py.

The fake SDK intentionally separates order acceptance, fills, and cancellation.
The older executor fixture fills every successful approve synchronously, which
cannot exercise these broker lifecycle states.
"""
import datetime as dt
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest
import yaml

from trader.strategy.auto_executor import AutoExecutor, SignalWork
from trader.strategy.strategy_runtime import StrategyRuntime
from trader.trading.strategy import Strategy, StrategyContext, StrategyState, Signal
from trader.data.gauntlet_store import GauntletRecord, GauntletStore
from trader.data.backtest_store import compute_strategy_hash
from trader.objects import Action, BarSize
from ib_async import Ticker, Contract


TS = pd.Timestamp('2026-07-06 10:39:00')


def make_work(**kwargs):
    fields = dict(strategy_name='orb_test', conid=1111, action=Action.BUY,
                  bar_ts=TS, bar_size_seconds=60.0, probability=.6, risk=.4,
                  auto_execute=True, state_running=True)
    fields.update(kwargs)
    return SignalWork(**fields)


class FakeResult:
    def __init__(self, ok=True, obj=None, error=None):
        self._ok = ok
        self.obj = obj
        self.error = error

    def is_success(self):
        return self._ok


class LifecycleSDK:
    """Acceptance, partial fills and cancellation have independent outcomes."""
    def __init__(self):
        self.secdef = SimpleNamespace(symbol='WDS', exchange='ASX', primaryExchange='ASX',
                                      currency='AUD', secType='STK', conId=1111)
        self.proposals = {}
        self.next_id = 100
        self.broker = {}
        self.propose_calls = []
        self.approve_calls = []
        self.fill_qty = 140.0
        self.avg_cost = 100.0
        self.protective_calls = []
        self.cancel_calls = []
        self.next_protective_id = 900
        self.fill_next = None  # None = requested amount, 0 = accepted but unfilled
        self.timeout_after_submit = False
        self.cancel_fails = False
        self.accepted = []
        self.active_stops = {}
        self.cancelled_stops = {}
        self.report_orders = True

    def resolve(self, symbol, **kwargs):
        return [self.secdef] if symbol in (1111, 'WDS') else []

    def propose(self, **kwargs):
        pid = self.next_id
        self.next_id += 1
        self.propose_calls.append(kwargs)
        self.proposals[pid] = SimpleNamespace(quantity=kwargs.get('quantity'), metadata=kwargs.get('metadata', {}))
        return pid, None, None

    def positions(self):
        return pd.DataFrame([dict(conId=c, position=q, avgCost=self.avg_cost)
                             for c, q in self.broker.items() if q != 0])

    def _proposal_store(self):
        return SimpleNamespace(get=self.proposals.get)

    def approve(self, pid):
        self.approve_calls.append(pid)
        p = self.proposals[pid]
        qty = p.quantity or self.fill_qty
        fill = qty if self.fill_next is None else self.fill_next
        action = self.propose_calls[-1]['action']
        self.broker[1111] = self.broker.get(1111, 0) + (fill if action == 'BUY' else -fill)
        self.accepted.append(dict(orderId=pid * 10, orderRef='orb_test', conId=1111,
                                  status='Filled' if fill == qty else 'Submitted',
                                  action=action, totalQuantity=qty, filled=fill, avgFillPrice=self.avg_cost,
                                  clientIntentId=p.metadata['client_intent_id']))
        if self.timeout_after_submit:
            return FakeResult(ok=False, error='order submission timed out; UNKNOWN; may be live')
        return FakeResult(ok=True, obj=[pid * 10])

    def place_protective_order(self, **kwargs):
        self.protective_calls.append(kwargs)
        oid = self.next_protective_id
        self.next_protective_id += 1
        self.active_stops[oid] = dict(kwargs)
        return FakeResult(obj=SimpleNamespace(order=SimpleNamespace(orderId=oid)))

    def cancel(self, oid):
        self.cancel_calls.append(oid)
        if self.cancel_fails:
            return FakeResult(ok=False, error='connection lost; cancellation not confirmed')
        stop = self.active_stops.pop(oid, None)
        if stop is not None:
            # Cancellation confirms terminal state, including the zero fill
            # checkpoint needed before the executor releases its reservation.
            self.cancelled_stops[oid] = stop
        for row in self.accepted:
            if row['orderId'] == oid and row['status'] != 'Filled':
                row['status'] = 'Cancelled'
        return FakeResult(ok=True)

    def trades(self):
        if not self.report_orders:
            return pd.DataFrame()
        stops = [dict(orderId=oid, orderRef=k['order_ref'], conId=k['con_id'],
                      status='Submitted', action='SELL', orderType='STP',
                      totalQuantity=k['quantity'], filled=0.0,
                      clientIntentId=k.get('client_intent_id', ''))
                 for oid, k in self.active_stops.items()]
        cancelled = [dict(orderId=oid, orderRef=k['order_ref'], conId=k['con_id'],
                          status='Cancelled', action='SELL', orderType='STP',
                          totalQuantity=k['quantity'], filled=0.0,
                          clientIntentId=k.get('client_intent_id', ''))
                     for oid, k in self.cancelled_stops.items()]
        return pd.DataFrame(self.accepted + stops + cancelled)


@pytest.fixture
def harness(tmp_path, monkeypatch):
    monkeypatch.delenv('MMR_AUTO_EXECUTE_DISABLED', raising=False)
    monkeypatch.setenv('MMR_PROTECTIVE_STOP_PCT', '8')
    monkeypatch.setenv('MMR_STALE_BAR_MULTIPLE', '3')
    monkeypatch.setenv('MMR_EMPTY_BROKER_GRACE_S', '120')
    monkeypatch.setattr(AutoExecutor, '_now_utc', lambda self: (TS.tz_localize('UTC') + pd.Timedelta(seconds=70)).to_pydatetime())
    sdk = LifecycleSDK()
    ex = AutoExecutor(str(tmp_path / 'state.duckdb'), paper_trading=True,
                      cooldown_seconds=300, sdk_factory=lambda: sdk)
    return ex, sdk


def test_control_fully_filled_owned_position_closes(harness):
    """Exit liveness: a confirmed owned long closes without being stranded."""
    ex, sdk = harness
    ex._process_signal(make_work(quantity=140))
    ex._process_signal(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))

    assert [call['action'] for call in sdk.propose_calls] == ['BUY', 'SELL']
    assert sdk.propose_calls[-1]['quantity'] == 140
    assert sdk.broker[1111] == 0
    assert ex.state.open_position('orb_test', 1111) is None
    assert not sdk.active_stops


def test_partial_fill_never_closes_manual_shares(harness):
    ex, sdk = harness
    sdk.broker[1111] = 100.0  # manually held before strategy entry
    sdk.fill_next = 40.0  # 40 of the requested 140 fill; remainder later cancelled
    ex._process_signal(make_work(quantity=140))
    sdk.accepted[0]['status'] = 'Cancelled'
    sdk.fill_next = None
    ex._process_signal(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))
    assert sdk.propose_calls[-1]['quantity'] == 40.0, 'closing must be bounded by actual strategy fills'


def test_accepted_but_unfilled_close_retains_managed_position(harness):
    ex, sdk = harness
    ex._process_signal(make_work(quantity=140))
    sdk.fill_next = 0
    ex._process_signal(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))
    assert sdk.broker[1111] == 140
    assert ex.state.open_position('orb_test', 1111) is not None, 'acceptance is not a fill'


def test_ambiguous_open_prevents_duplicate_attempt_for_same_bar(harness):
    ex, sdk = harness
    sdk.timeout_after_submit = True
    ex._process_signal(make_work(quantity=140))
    ex._process_signal(make_work(quantity=140))
    assert len(sdk.approve_calls) == 1, f'{len(sdk.approve_calls)} submissions / {sdk.broker[1111]} broker shares'


def test_crash_after_broker_fill_does_not_allow_another_open(harness, monkeypatch):
    ex, sdk = harness
    path = ex.state.db.db_path
    monkeypatch.setattr(ex.state, 'apply_fill', lambda *a, **k: (_ for _ in ()).throw(RuntimeError('process dies before attribution commit')))
    with pytest.raises(RuntimeError):
        ex._process_signal(make_work(quantity=140))
    restarted = AutoExecutor(path, paper_trading=True, cooldown_seconds=300, sdk_factory=lambda: sdk)
    restarted._reconcile_once()
    restarted._process_signal(make_work(quantity=140, bar_ts=TS + pd.Timedelta(minutes=1)))
    assert len(sdk.approve_calls) == 1, 'restart must reconcile durable submission intent before re-entry'


def test_unknown_execution_quantity_blocks_reentry(harness):
    ex, sdk = harness
    sdk.report_orders = False
    ex._process_signal(make_work())
    restarted = AutoExecutor(ex.state.db.db_path, paper_trading=True, sdk_factory=lambda: sdk)
    restarted._reconcile_once()
    restarted._process_signal(make_work(bar_ts=TS + pd.Timedelta(minutes=1)))
    assert len(sdk.approve_calls) == 1, 'UNKNOWN attribution is unresolved exposure, not flat'


def test_failed_stop_cancel_cannot_leave_live_sell_after_close(harness):
    ex, sdk = harness
    ex._process_signal(make_work(quantity=140))
    sdk.cancel_fails = True
    ex._process_signal(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))
    assert sdk.broker[1111] > 0 or not sdk.active_stops, 'flat broker still has a live stop that can create a short'


def test_failed_order_read_does_not_duplicate_protective(harness, monkeypatch):
    ex, sdk = harness
    ex._process_signal(make_work(quantity=140))
    monkeypatch.setattr(sdk, 'trades', lambda: (_ for _ in ()).throw(TimeoutError('orders read failed')))
    ex._ensure_protective('orb_test', 1111)
    assert len(sdk.active_stops) == 1, 'read failure is not proof the existing stop died'


def test_protective_grows_when_more_of_entry_fills(harness):
    ex, sdk = harness
    sdk.fill_next = 40
    ex._process_signal(make_work(quantity=140))
    sdk.broker[1111] = 140.0  # rest of the same entry fills asynchronously
    sdk.accepted[0]['status'] = 'Filled'
    sdk.accepted[0]['filled'] = 140.0
    ex._ensure_protective('orb_test', 1111)
    assert sum(s['quantity'] for s in sdk.active_stops.values()) == 140.0


def test_first_empty_broker_read_preserves_attribution_through_worker(harness):
    ex, sdk = harness
    ex.state.record_open('orb_test', 1111, 140.0, TS, 1, None, None)
    ex.state.set_protective('orb_test', 1111, 900)
    ex._load_open_view()
    ex._queue.put(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))
    ex._queue.put(None)
    ex._run()  # run the actual reconcile + processing sequence synchronously
    assert ex.state.open_position('orb_test', 1111) is not None, 'empty startup position cache is inconclusive'


def _make_runtime(tmp_path, strategies_dir):
    rt = StrategyRuntime.__new__(StrategyRuntime)
    rt.strategies_directory = str(strategies_dir)
    rt.strategy_config_file = str(tmp_path / 'strategy_runtime.yaml')
    rt.strategy_implementations = []
    rt.strategies = {}
    rt.streams = {}
    rt.storage = None
    rt.universe_accessor = None
    rt._config_mtime = 0.0
    rt.trader_client = None
    rt.paper_trading = True
    rt._published_conids = set()
    rt._trader_boot_id = None
    return rt


def _write_strategy(strategies_dir, name, body):
    strategies_dir.mkdir(parents=True, exist_ok=True)
    path = strategies_dir / f'{name}.py'
    path.write_text(body)
    return path


_VALID_STRATEGY_BODY = """
from trader.trading.strategy import Strategy

class V1(Strategy):
    THRESHOLD = 20
    def on_prices(self, prices):
        return None

class UntestedSibling(Strategy):
    def on_prices(self, prices):
        raise RuntimeError('untested sibling')
"""


def runtime_and_strategy(tmp_path):
    strategies = tmp_path / 'strategies'
    module = _write_strategy(strategies, 'review', _VALID_STRATEGY_BODY)
    rt = _make_runtime(tmp_path, strategies)
    rt.duckdb_path = str(tmp_path / 'runtime.duckdb')
    rt.trader_client = MagicMock()
    rt.trader_client.rpc().get_status.return_value = {'boot_id': 'boot'}
    rt.load_strategy(name='review', bar_size_str='1 min', conids=[], universe=None,
                     historical_days_prior=0, module=str(module), class_name='V1', description='',
                     params={'THRESHOLD': 5})
    return rt, module


def test_live_runtime_applies_declared_uppercase_tunables(tmp_path):
    rt, _ = runtime_and_strategy(tmp_path)
    assert rt.get_strategy('review').THRESHOLD == 5, 'runtime must run the same rule tested with --param'


def test_runtime_gauntlet_refuses_untested_sibling_class(tmp_path, monkeypatch):
    rt, module = runtime_and_strategy(tmp_path)
    GauntletStore(rt.duckdb_path).record(GauntletRecord(strategy_name='tested', module_path=str(module),
                                                     class_name='V1', code_hash=compute_strategy_hash(str(module)), verdict='PASS'))
    monkeypatch.setenv('MMR_GAUNTLET_ENFORCE', '1')
    rt.load_strategy(name='untested', bar_size_str='1 min', conids=[], universe=None,
                     historical_days_prior=0, module=str(module), class_name='UntestedSibling',
                     description='', auto_execute=True)
    assert rt.get_strategy('untested')._context.auto_execute is False


def test_config_disarm_applies_to_existing_runtime_instance(tmp_path):
    rt, module = runtime_and_strategy(tmp_path)
    instance = rt.get_strategy('review')
    instance._context.auto_execute = True
    instance.enable()
    entry = dict(name='review', module=str(module), class_name='V1', bar_size='1 min',
                 conids=[], auto_execute=False)
    (tmp_path / 'strategy_runtime.yaml').write_text(yaml.safe_dump({'strategies': [entry]}))
    rt._reconcile_sync()
    assert rt.get_strategy('review')._context.auto_execute is False


def test_config_removal_revokes_opening_authority(tmp_path):
    rt, _ = runtime_and_strategy(tmp_path)
    instance = rt.get_strategy('review')
    instance._context.auto_execute = True
    instance.enable()
    (tmp_path / 'strategy_runtime.yaml').write_text(yaml.safe_dump({'strategies': []}))
    rt._reconcile_sync()
    still = rt.get_strategy('review')
    assert still is None or not still._context.auto_execute or still.state != StrategyState.RUNNING


def prepare_dispatch(tmp_path, strategy):
    rt = _make_runtime(tmp_path, tmp_path / 'strategies')
    rt._last_dispatched_bar = {}
    rt._oos_bars = {}
    rt._oos_logged = set()
    rt._oos_last = {}
    rt._tick_retention_days = 2
    rt.auto_executor = MagicMock()
    rt.auto_executor.open_entry_bar.return_value = TS - pd.Timedelta(minutes=1)
    rt.event_store = MagicMock()
    rt.zmq_messagebus_client = MagicMock()
    strategy.install(StrategyContext('dispatch', BarSize.Mins1, [1111], None, 1, False,
                                     None, None, None, auto_execute=True))
    strategy.enable()
    rt.strategies[1111] = [strategy]
    rt.strategy_implementations.append(strategy)
    rt._bar_in_session = lambda *a: True
    rt._strategy_frame = lambda *a: pd.DataFrame({'close': [100.]}, index=pd.DatetimeIndex([TS]))
    ticker = Ticker(contract=Contract(conId=1111), time=TS.tz_localize('UTC').to_pydatetime(),
                    bid=100, ask=101, last=100, volume=100)
    return rt, ticker


def test_strategy_error_does_not_stop_future_time_exit_checks(tmp_path):
    class Crashing(Strategy):
        def on_prices(self, prices):
            raise ValueError('bad indicator input')
    rt, ticker = prepare_dispatch(tmp_path, Crashing())
    rt.on_ticker_next(ticker)
    assert rt.auto_executor.submit_bar.call_count == 1
    rt._strategy_frame = lambda *a: pd.DataFrame({'close': [100.]}, index=pd.DatetimeIndex([TS + pd.Timedelta(minutes=1)]))
    ticker.time += dt.timedelta(minutes=1)
    rt.on_ticker_next(ticker)
    assert rt.auto_executor.submit_bar.call_count == 2, 'strategy failure must not revoke executor exit management'


def test_live_runtime_rejects_or_dispatches_on_bar_only_strategy(tmp_path, monkeypatch, caplog):
    """The documented backtest-only API may be refused instead of supported live.

    Use the real loader with a valid module and exact class PASS so unrelated
    gauntlet/config failures cannot masquerade as a capability refusal.
    """
    body = """
from trader.trading.strategy import Strategy, Signal
from trader.objects import Action

class BarOnly(Strategy):
    def __init__(self):
        super().__init__()
        self.calls = 0

    def on_bar(self, prices, state, index):
        self.calls += 1
        return Signal(self.name, Action.BUY, .6, .4)
"""
    strategies = tmp_path / 'strategies'
    module = _write_strategy(strategies, 'bar_only', body)
    runtime = _make_runtime(tmp_path, strategies)
    runtime.duckdb_path = str(tmp_path / 'runtime.duckdb')
    GauntletStore(runtime.duckdb_path).record(GauntletRecord(
        strategy_name='bar_only', module_path=str(module), class_name='BarOnly',
        code_hash=compute_strategy_hash(str(module)), verdict='PASS'))
    monkeypatch.setenv('MMR_GAUNTLET_ENFORCE', '1')
    runtime.load_strategy(
        name='bar_only', bar_size_str='1 min', conids=[1111], universe=None,
        historical_days_prior=0, module=str(module), class_name='BarOnly',
        description='', auto_execute=True)
    strategy = runtime.get_strategy('bar_only')
    if strategy is None or not strategy._context.auto_execute:
        # A safe capability refusal must explain itself rather than letting an
        # unrelated missing dependency or configuration typo satisfy this test.
        assert any(term in caplog.text.lower() for term in (
            'on_bar', 'unsupported', 'backtest-only', 'dispatch capability'))
        return

    dispatch, ticker = prepare_dispatch(tmp_path, strategy)
    dispatch.on_ticker_next(ticker)
    assert strategy.calls == 1, 'loader accepted the strategy but its live hook never ran'
