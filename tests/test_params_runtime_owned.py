"""``SESSION_TZ`` is a RUNTIME-owned parameter, not a class tunable (2026-09-14).

The runtime reads ``ctx.params['SESSION_TZ']`` generically for live
time-of-day exits (``_session_bar_ts``) and for recorded exit policies
(``_refresh_management_contexts``), and CLAUDE.md documents it as the params
knob for ASX deployments. Yet the shared validator refused ANY upper-case key
the class did not declare, so a class without ``SESSION_TZ = ...`` failed
construction inside the callback worker on ``params: {SESSION_TZ: ...}``. The
six deployed ASX strategies survived by coincidence — their class declares it.
"""
import hashlib
import logging
import textwrap
from types import SimpleNamespace

import pytest

from trader.objects import BarSize
from trader.strategy.parameters import RUNTIME_OWNED_PARAMS, apply_param_overrides
from trader.trading.strategy import Strategy, StrategyContext


class Undeclared(Strategy):
    """Does NOT declare SESSION_TZ."""
    PERIOD = 10

    def on_prices(self, prices):
        return None


class Declared(Strategy):
    SESSION_TZ = 'America/New_York'

    def on_prices(self, prices):
        return None


def _installed(cls):
    instance = cls()
    instance.install(StrategyContext(
        name='t', bar_size=BarSize.Mins1, conids=[1], universe=None, historical_days_prior=0,
        paper_only=True, storage=None, universe_accessor=None, logger=logging.getLogger('t'),
        params={}))
    return instance


def test_session_tz_is_in_the_runtime_owned_set():
    assert 'SESSION_TZ' in RUNTIME_OWNED_PARAMS


def test_undeclared_class_accepts_session_tz_into_context_params_only():
    inst = _installed(Undeclared)
    applied = apply_param_overrides(inst, {'SESSION_TZ': 'Australia/Sydney', 'PERIOD': '3'})
    assert applied == {'SESSION_TZ': 'Australia/Sydney', 'PERIOD': 3}
    assert inst.params['SESSION_TZ'] == 'Australia/Sydney'
    # Never set on an instance whose class does not declare it.
    assert 'SESSION_TZ' not in vars(inst)
    assert not hasattr(type(inst), 'SESSION_TZ')
    assert inst.PERIOD == 3


def test_declared_class_still_gets_the_attribute_and_the_param():
    inst = _installed(Declared)
    apply_param_overrides(inst, {'SESSION_TZ': 'Australia/Sydney'})
    assert inst.SESSION_TZ == 'Australia/Sydney'
    assert inst.params['SESSION_TZ'] == 'Australia/Sydney'
    assert Declared.SESSION_TZ == 'America/New_York', 'class attribute is shadowed, not mutated'


@pytest.mark.parametrize('cls', [Undeclared, Declared])
@pytest.mark.parametrize('bad', ['Nope/Nowhere', '', 7, None, 'Sydney'])
def test_invalid_session_tz_is_refused_on_both_paths(cls, bad):
    inst = _installed(cls)
    with pytest.raises(ValueError, match='SESSION_TZ'):
        apply_param_overrides(inst, {'SESSION_TZ': bad})


def test_other_uppercase_typos_are_still_refused():
    inst = _installed(Undeclared)
    with pytest.raises(ValueError, match="no parameter 'PERIODD'") as info:
        apply_param_overrides(inst, {'PERIODD': 4})
    assert 'SESSION_TZ' in str(info.value), 'the error names the runtime-owned set'


def test_pre_install_session_tz_lands_in_pending_params_then_context():
    inst = Undeclared()
    apply_param_overrides(inst, {'SESSION_TZ': 'Europe/London'})
    assert 'SESSION_TZ' not in vars(inst)
    ctx = StrategyContext(
        name='t', bar_size=BarSize.Mins1, conids=[1], universe=None, historical_days_prior=0,
        paper_only=True, storage=None, universe_accessor=None, logger=logging.getLogger('t'),
        params={})
    inst.install(ctx)
    assert ctx.params['SESSION_TZ'] == 'Europe/London'


def test_backtester_param_path_accepts_session_tz_for_undeclared_class():
    """``mmr backtest --param SESSION_TZ=Australia/Sydney`` shares this validator."""
    from trader.simulation.backtester import Backtester
    inst = _installed(Undeclared)
    applied = Backtester.apply_param_overrides(inst, {'SESSION_TZ': 'Australia/Sydney'})
    assert applied == {'SESSION_TZ': 'Australia/Sydney'}
    assert inst.params['SESSION_TZ'] == 'Australia/Sydney'


@pytest.mark.timeout(120)
def test_callback_worker_constructs_undeclared_class_with_session_tz(tmp_path, monkeypatch):
    """The live path: the child process applies the same overrides at init."""
    import trader.container as container
    from trader.strategy.callback_worker import StrategyCallbackWorker
    monkeypatch.setattr(container, 'MMR_CONFIG_DIR', tmp_path / 'config')
    path = tmp_path / 'asx_undeclared.py'
    path.write_text(textwrap.dedent('''
        from trader.trading.strategy import Strategy, Signal
        from trader.objects import Action
        class Isolated(Strategy):
            def on_prices(self, prices):
                assert not hasattr(type(self), 'SESSION_TZ')
                return Signal(self.name, Action.SELL, .5, .2,
                              metadata={'tz': self.params['SESSION_TZ']})
    '''))
    context = StrategyContext(
        name='Isolated', bar_size=BarSize.Mins1, conids=[123], universe=None,
        historical_days_prior=0, paper_only=True,
        storage=SimpleNamespace(duckdb_path=str(tmp_path / 'history.duckdb')),
        universe_accessor=None, logger=logging.getLogger('callback-test'),
        params={'SESSION_TZ': 'Australia/Sydney'}, deployment_generation='generation-1')
    worker = StrategyCallbackWorker(
        str(path), hashlib.sha256(path.read_bytes()).hexdigest(), 'Isolated', context)
    try:
        metadata = worker.wait_ready()
        assert metadata['effective_params'] == {'SESSION_TZ': 'Australia/Sydney'}
        import pandas as pd
        import queue
        frame = pd.DataFrame({'open': [1.0], 'high': [1.0], 'low': [1.0], 'close': [1.0],
                              'volume': [1.0]},
                             index=pd.DatetimeIndex(['2026-09-14T00:00:00Z'], name='date'))
        results, errors = queue.Queue(), queue.Queue()
        assert worker.submit(123, frame, frame.index[-1], 'generation-1', results.put, errors.put)
        result = results.get(timeout=10)
        assert errors.empty()
        assert result.signal.metadata == {'tz': 'Australia/Sydney'}
    finally:
        worker.stop()
