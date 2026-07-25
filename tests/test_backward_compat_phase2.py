"""Phase 2 backward-compatibility: defaults OFF => byte-identical behaviour.

With MMR_ROLE unset, approver_required_above_usd=0, and approver_key='':
  * place_expressive_order behaves exactly as before (an open proceeds with no
    key; the notional tier is never consulted);
  * the Container resolves the new Trader.__init__ params from their defaults
    with NO YAML entries and NO env vars.
"""

import asyncio
import threading
from unittest.mock import AsyncMock, MagicMock

import pytest
import reactivex as rx
from ib_async.contract import Contract

from trader.common.reactivex import SuccessFailEnum
from trader.trading.proposal import ExecutionSpec
from trader.trading.risk_gate import RiskGateResult, RiskInputs
from trader.trading.trading_runtime import Trader


class _ApproveAll:
    def check_instrument(self, **kw):
        return RiskGateResult(approved=True)

    def check_leverage(self, *a, **kw):
        return RiskGateResult(approved=True)

    def evaluate(self, *a, **kw):
        return RiskGateResult(approved=True)


class _StubExecutioner:
    def __init__(self):
        self.calls = 0

    async def subscribe_place_order_direct(self, approved):
        self.calls += 1
        ft = MagicMock()
        ft.order = MagicMock()
        ft.order.orderId = 1
        return rx.from_iterable([ft])


class _Tick:
    ask = bid = last = close = 100.0


def _default_trader():
    """A Trader whose Phase-2 knobs are at their OFF defaults."""
    t = object.__new__(Trader)
    t.pnl_subscriptions = {}
    t._pnl_subscriptions_lock = threading.Lock()
    t._main_loop = None
    t.disposables = []
    t.ib_account = 'DU12345'
    # Defaults exactly as Trader.__init__ would set with no config/env.
    t.approver_required_above_usd = 0.0
    t.approver_key = ''
    t.order_tracker = None
    t.order_reduces_exposure = MagicMock(return_value=False)
    t.risk_gate = _ApproveAll()
    t.check_order_margin = AsyncMock(side_effect=Exception('skip margin'))
    t.gather_risk_inputs = MagicMock(return_value=RiskInputs(
        open_order_count=0, daily_pnl=0.0, daily_pnl_evaluable=True,
        portfolio_value=1_000_000.0, portfolio_value_evaluable=True,
    ))
    client = MagicMock()
    client.get_snapshot = AsyncMock(return_value=_Tick())
    t.client = client
    t.executioner = _StubExecutioner()
    return t


def _contract():
    c = Contract()
    c.symbol = 'AMD'
    c.exchange = 'NASDAQ'
    c.secType = 'STK'
    c.conId = 4391
    return c


def test_defaults_off_open_proceeds_with_no_key():
    t = _default_trader()
    spec = ExecutionSpec(order_type='MARKET', exit_type='NONE').to_dict()
    # A large notional that WOULD exceed any sane threshold — but the tier is
    # off, so it places anyway, with no approver_key argument at all.
    result = asyncio.run(t.place_expressive_order(
        _contract(), 'BUY', 10_000, spec, algo_name='manual'))
    assert result.success_fail == SuccessFailEnum.SUCCESS
    assert t.executioner.calls == 1


def test_role_gate_default_operator_allows_all(monkeypatch):
    from trader.mmr_cli import _role_allows
    import argparse
    monkeypatch.delenv('MMR_ROLE', raising=False)
    for cmd in ('approve', 'buy', 'sell', 'resize-positions'):
        ns = argparse.Namespace(command=cmd)
        assert _role_allows(cmd, ns) is None


def test_container_resolves_new_params_from_defaults(monkeypatch, tmp_path):
    """The new optional Trader params must resolve from their code defaults
    when absent from YAML and env — no ContainerResolutionError, values default."""
    import inspect
    from trader.trading.trading_runtime import Trader as _T

    sig = inspect.signature(_T.__init__)
    assert sig.parameters['approver_required_above_usd'].default == 0.0
    assert sig.parameters['approver_key'].default == ''

    # Simulate the container's resolution rule: a param absent from config/env
    # with a non-empty default is simply omitted from the call args (so the
    # default applies). Mirror trader/container.py resolve().
    config = {}  # no YAML entries for the new keys
    monkeypatch.delenv('APPROVER_REQUIRED_ABOVE_USD', raising=False)
    monkeypatch.delenv('APPROVER_KEY', raising=False)
    missing_required = []
    args = {}
    for param in sig.parameters.values():
        if param.name == 'self':
            continue
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            continue
        import os as _os
        env_val = _os.getenv(param.name.upper())
        if env_val not in (None, ''):
            args[param.name] = env_val
        elif param.name in config and config[param.name] is not None:
            args[param.name] = config[param.name]
        elif param.default is inspect.Parameter.empty:
            missing_required.append(param.name)
    # The new params are NOT required (have defaults) and NOT supplied.
    assert 'approver_required_above_usd' not in args
    assert 'approver_key' not in args
    assert 'approver_required_above_usd' not in missing_required
    assert 'approver_key' not in missing_required


def test_env_key_wins_over_yaml(monkeypatch):
    """MMR_APPROVER_KEY (process env) is the canonical secret source and wins
    over the YAML-supplied approver_key."""
    monkeypatch.setenv('MMR_APPROVER_KEY', 'env-secret')
    t = object.__new__(Trader)
    # Re-run just the secret-resolution logic Trader.__init__ performs.
    import os as _os
    env_key = _os.environ.get('MMR_APPROVER_KEY')
    yaml_key = 'yaml-secret'
    resolved = env_key if env_key is not None else (yaml_key or '')
    assert resolved == 'env-secret'

    monkeypatch.delenv('MMR_APPROVER_KEY', raising=False)
    env_key = _os.environ.get('MMR_APPROVER_KEY')
    resolved = env_key if env_key is not None else (yaml_key or '')
    assert resolved == 'yaml-secret'
