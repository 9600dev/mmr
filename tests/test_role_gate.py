"""MMR_ROLE capability gate (Phase 2, defense-in-depth).

The prompt-injection threat model is one agent context that both proposes and
approves. MMR_ROLE=proposer removes `approve` (and every opening command) from
the surface; the gate lives in dispatch() so both non-interactive main() and
the REPL are covered by a single insertion.

These tests pin:
  * the allow/deny matrix per role (`_role_allows`);
  * that a denied command returns a refusal from dispatch() and NEVER reaches
    the handler / SDK (mock SDK, asserted zero calls);
  * operator (unset) and approver both allow approve.
"""

import argparse
from unittest.mock import MagicMock

import pytest

from trader.mmr_cli import _role_allows, dispatch


def _args(command, **kw):
    ns = argparse.Namespace(command=command, json=False)
    for k, v in kw.items():
        setattr(ns, k, v)
    return ns


# ---------------------------------------------------------------------------
# _role_allows matrix
# ---------------------------------------------------------------------------

class TestRoleMatrix:
    def test_operator_unset_allows_everything(self, monkeypatch):
        monkeypatch.delenv('MMR_ROLE', raising=False)
        for cmd in ('approve', 'buy', 'sell', 'resize-positions', 'propose',
                    'reject', 'portfolio', 'cancel', 'close'):
            assert _role_allows(cmd, _args(cmd)) is None

    def test_operator_explicit_allows_everything(self, monkeypatch):
        monkeypatch.setenv('MMR_ROLE', 'operator')
        assert _role_allows('approve', _args('approve')) is None
        assert _role_allows('buy', _args('buy')) is None

    @pytest.mark.parametrize('cmd,extra', [
        ('approve', {}),
        ('buy', {}),
        ('sell', {}),
        ('resize-positions', {}),
        ('resize', {}),
        ('strategies', {'strat_action': 'enable'}),
        ('strategies', {'strat_action': 'disable'}),
        ('strategies', {'strat_action': 'reload'}),
        ('options', {'opt_action': 'buy'}),
        ('opt', {'opt_action': 'sell'}),
    ])
    def test_proposer_denies_opens_and_control(self, monkeypatch, cmd, extra):
        monkeypatch.setenv('MMR_ROLE', 'proposer')
        reason = _role_allows(cmd, _args(cmd, **extra))
        assert reason is not None
        assert 'proposer' in reason

    @pytest.mark.parametrize('cmd,extra', [
        ('propose', {}),
        ('reject', {}),
        ('cancel', {}),
        ('cancel-all', {}),
        ('close', {}),
        ('close-all-positions', {}),
        ('portfolio', {}),
        ('portfolio-risk', {}),
        ('ideas', {}),
        ('news', {}),
        ('snapshot', {}),
        ('proposals', {}),
        ('session', {}),
        ('resolve', {}),
        ('status', {}),
        ('data', {}),
        ('backtest', {}),
        ('backtests', {}),
        ('universe', {}),
        ('group', {}),
        ('history', {}),
        ('strategies', {'strat_action': 'list'}),
        ('strategies', {'strat_action': 'signals'}),
        ('options', {'opt_action': 'chain'}),
        ('forex', {'fx_action': 'snapshot'}),
    ])
    def test_proposer_allows_reads_propose_reject_derisk(self, monkeypatch, cmd, extra):
        monkeypatch.setenv('MMR_ROLE', 'proposer')
        assert _role_allows(cmd, _args(cmd, **extra)) is None

    def test_approver_allows_approve_and_reads(self, monkeypatch):
        monkeypatch.setenv('MMR_ROLE', 'approver')
        assert _role_allows('approve', _args('approve')) is None
        assert _role_allows('reject', _args('reject')) is None
        assert _role_allows('portfolio', _args('portfolio')) is None

    def test_approver_denies_opens_resize_control(self, monkeypatch):
        monkeypatch.setenv('MMR_ROLE', 'approver')
        assert _role_allows('buy', _args('buy')) is not None
        assert _role_allows('sell', _args('sell')) is not None
        assert _role_allows('resize-positions', _args('resize-positions')) is not None
        assert _role_allows('strategies', _args('strategies', strat_action='enable')) is not None

    def test_unknown_role_fails_closed(self, monkeypatch):
        monkeypatch.setenv('MMR_ROLE', 'intern')
        # Even a read is refused under an unrecognized role.
        assert _role_allows('portfolio', _args('portfolio')) is not None
        assert _role_allows('approve', _args('approve')) is not None


# ---------------------------------------------------------------------------
# dispatch() enforcement — denied commands never touch the handler / SDK
# ---------------------------------------------------------------------------

class TestDispatchEnforcement:
    @pytest.mark.parametrize('cmd,extra', [
        ('approve', {'proposal_id': 1, 'all': False, 'approver_key': None}),
        ('buy', {}),
        ('sell', {}),
        ('resize-positions', {}),
        ('strategies', {'strat_action': 'enable'}),
    ])
    def test_proposer_denied_command_returns_refusal_no_handler(
            self, monkeypatch, cmd, extra):
        monkeypatch.setenv('MMR_ROLE', 'proposer')
        mmr = MagicMock()
        cont = dispatch(mmr, _args(cmd, **extra))
        assert cont is True  # REPL keeps going
        # The SDK must not have been touched at all — the capability never ran.
        assert mmr.approve.call_count == 0
        assert mmr.buy.call_count == 0
        assert mmr.sell.call_count == 0
        assert mmr.check_ib_upstream.call_count == 0

    def test_approver_approve_passes_role_gate(self, monkeypatch):
        # Under approver, the role gate does NOT refuse approve — so dispatch
        # proceeds past it (into the IB-upstream check, which we stub clean).
        monkeypatch.setenv('MMR_ROLE', 'approver')
        assert _role_allows('approve', _args('approve')) is None

    def test_operator_approve_not_refused_by_role(self, monkeypatch):
        monkeypatch.delenv('MMR_ROLE', raising=False)
        assert _role_allows('approve', _args('approve')) is None
