"""The resize CLI render + the sdk re-create round-trip — the two halves of
the resize surface that live testing found unpinned (2026-07-27).

Half 1: the PLAN RENDER crashed on float deltas ({:+d}) before showing
anything, --dry-run included, because no test ever executed it. The dry-run
is the operator's safety preview; a preview that crashes pushes people toward
running the real thing blind.

Half 2: execute_resize_plan re-created protective stops WITHOUT their
orderRef (event store recorded them as 'manual'), severing fill attribution
and the auto-executor's ownership test. The executor-side survival tests pin
the consequence; this pins the cause at its source.
"""
import argparse
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from trader.sdk import MMR


def _plan():
    return {
        'current_total': 830.0, 'target_total': 400.0, 'scale_factor': 0.48,
        'adjustments': [{
            'symbol': 'GOOGL', 'conId': 208813719,
            'current_qty': 3.0, 'target_qty': 1.0, 'delta_qty': -2.0,  # FLOATS
            'action': 'SELL', 'current_value': 981.0, 'target_value': 327.0,
            'associated_orders': [{
                'orderId': 227, 'orderType': 'STP', 'action': 'SELL',
                'quantity': 1.0, 'auxPrice': 296.56, 'lmtPrice': 0.0,
                'trailingPercent': 0.0, 'tif': 'GTC', 'orderRef': 'orb_googl',
            }],
        }],
    }


class TestPlanRenderNeverCrashes:
    def test_dry_run_renders_float_deltas(self, capsys):
        """Pinned counterexample: delta_qty is a float and the old {:+d}
        raised 'Unknown format code' before ANY output appeared."""
        from trader.mmr_cli import _handle_resize_positions
        mmr = MagicMock()
        mmr.compute_resize_plan.return_value = _plan()
        args = argparse.Namespace(max_bound=400.0, min_bound=None, dry_run=True)
        _handle_resize_positions(mmr, args)
        out = capsys.readouterr().out
        assert 'GOOGL' in out and 'Dry run' in out
        assert '296.56' in out          # the protective-order preview rendered
        mmr.execute_resize_plan.assert_not_called()


class TestJsonMode:
    """`--json resize-positions --dry-run` drew the plan as a Rich table on
    stderr and printed NOTHING on stdout (found live 2026-11-16); without
    --dry-run it went on to input() and blocked the caller on a prompt."""

    @pytest.fixture(autouse=True)
    def _json(self, monkeypatch):
        import trader.mmr_cli as cli
        monkeypatch.setattr(cli, '_json_mode', True)
        monkeypatch.setattr(cli, '_exit_code', 0, raising=False)
        monkeypatch.setattr('builtins.input', lambda *a, **k: pytest.fail('asked for confirmation in JSON mode'))

    @staticmethod
    def _run(capsys, plan=None, **flags):
        import json
        from trader.mmr_cli import _handle_resize_positions
        mmr = MagicMock()
        mmr.compute_resize_plan.return_value = plan if plan is not None else _plan()
        mmr.execute_resize_plan.return_value = {'successes': ['GOOGL: SELL 2 submitted'], 'failures': [], 'warnings': []}
        args = argparse.Namespace(**{'max_bound': 400.0, 'min_bound': None, 'dry_run': False, 'yes': False, **flags})
        _handle_resize_positions(mmr, args)
        out = capsys.readouterr().out
        return json.loads(out), mmr

    def test_a_dry_run_is_one_json_document_with_the_plan(self, capsys):
        doc, mmr = self._run(capsys, dry_run=True)
        assert doc['success'] is True and doc['dry_run'] is True and doc['executed'] is False
        assert (doc['current_total'], doc['target_total'], doc['scale_factor']) == (830.0, 400.0, 0.48)
        assert [(a['symbol'], a['action'], a['delta_qty']) for a in doc['adjustments']] == [('GOOGL', 'SELL', -2.0)]
        assert doc['adjustments'][0]['associated_orders'][0]['orderId'] == 227
        assert 'no orders placed' in doc['message']
        mmr.execute_resize_plan.assert_not_called()

    def test_without_yes_nothing_is_executed_and_it_says_why(self, capsys):
        import trader.mmr_cli as cli
        doc, mmr = self._run(capsys)
        assert doc['success'] is False and doc['executed'] is False
        assert 'pass --yes' in doc['message']
        assert doc['adjustments']                     # the plan is still shown
        mmr.execute_resize_plan.assert_not_called()
        assert cli._exit_code == 1

    def test_with_yes_it_executes_and_reports_each_outcome(self, capsys):
        doc, mmr = self._run(capsys, yes=True)
        mmr.execute_resize_plan.assert_called_once()
        assert doc['success'] is True and doc['executed'] is True
        assert doc['results'] == {'successes': ['GOOGL: SELL 2 submitted'], 'failures': [], 'warnings': []}
        assert doc['message'] == '1 succeeded, 0 failed'

    def test_a_failed_leg_makes_the_command_fail(self, capsys):
        import json
        import trader.mmr_cli as cli
        from trader.mmr_cli import _handle_resize_positions
        mmr = MagicMock()
        mmr.compute_resize_plan.return_value = _plan()
        mmr.execute_resize_plan.return_value = {'successes': [], 'failures': ['GOOGL: DEFERRED'], 'warnings': ['x']}
        _handle_resize_positions(mmr, argparse.Namespace(max_bound=400.0, min_bound=None, dry_run=False, yes=True))
        doc = json.loads(capsys.readouterr().out)
        assert doc['success'] is False and doc['results']['failures'] == ['GOOGL: DEFERRED']
        assert doc['message'] == '0 succeeded, 1 failed' and cli._exit_code == 1

    def test_nothing_to_do_is_a_success_with_an_empty_plan(self, capsys):
        doc, mmr = self._run(capsys, plan={'current_total': 300.0, 'target_total': 300.0, 'scale_factor': 1.0,
                                           'adjustments': []}, yes=True)
        assert doc['success'] is True and doc['adjustments'] == [] and doc['executed'] is False
        assert 'already within bounds' in doc['message']
        mmr.execute_resize_plan.assert_not_called()

    def test_missing_bounds_is_a_json_error(self, capsys):
        import json
        import trader.mmr_cli as cli
        from trader.mmr_cli import _handle_resize_positions
        _handle_resize_positions(MagicMock(), argparse.Namespace(max_bound=None, min_bound=None, dry_run=True, yes=False))
        doc = json.loads(capsys.readouterr().out)
        assert doc == {'success': False, 'message': 'Specify --max-bound and/or --min-bound'}
        assert cli._exit_code == 1


class TestConfirmation:
    def test_a_terminal_user_is_still_asked_and_can_decline(self, monkeypatch, capsys):
        import trader.mmr_cli as cli
        from trader.mmr_cli import _handle_resize_positions
        monkeypatch.setattr(cli, '_json_mode', False)
        monkeypatch.setattr(cli, '_confirm', lambda prompt, assume_yes: False)
        mmr = MagicMock()
        mmr.compute_resize_plan.return_value = _plan()
        _handle_resize_positions(mmr, argparse.Namespace(max_bound=400.0, min_bound=None, dry_run=False, yes=False))
        mmr.execute_resize_plan.assert_not_called()
        assert 'Cancelled' in capsys.readouterr().out

    def test_a_non_interactive_caller_without_yes_is_refused_not_blocked(self, monkeypatch, capsys):
        import trader.mmr_cli as cli
        from trader.mmr_cli import _handle_resize_positions
        monkeypatch.setattr(cli, '_json_mode', False)
        monkeypatch.setattr(cli, '_confirm', lambda prompt, assume_yes: None)
        mmr = MagicMock()
        mmr.compute_resize_plan.return_value = _plan()
        _handle_resize_positions(mmr, argparse.Namespace(max_bound=400.0, min_bound=None, dry_run=False, yes=False))
        mmr.execute_resize_plan.assert_not_called()
        assert 'pass --yes' in capsys.readouterr().out

    def test_yes_skips_the_question(self, monkeypatch, capsys):
        import trader.mmr_cli as cli
        from trader.mmr_cli import _handle_resize_positions
        monkeypatch.setattr(cli, '_json_mode', False)
        monkeypatch.setattr('builtins.input', lambda *a, **k: pytest.fail('asked although --yes was given'))
        mmr = MagicMock()
        mmr.compute_resize_plan.return_value = _plan()
        mmr.execute_resize_plan.return_value = {'successes': ['ok'], 'failures': [], 'warnings': []}
        _handle_resize_positions(mmr, argparse.Namespace(max_bound=400.0, min_bound=None, dry_run=False, yes=True))
        mmr.execute_resize_plan.assert_called_once()


class TestCoordinatedProtection:
    """The supported server handoff preserves the residual protective tranche.

    The original re-create regression lost orderRef. The coordinator now
    retains the existing residual order, so every protective field survives.
    """

    def test_remaining_stop_preserves_ref_price_type_tif(self):
        from test_seams import SeamBroker, _resize_mmr
        broker = SeamBroker()
        conid = 208813719
        broker.positions[conid] = 3.0
        broker.add_order(orderRef='orb_googl', conId=conid,
                         auxPrice=296.56, quantity=2.0)
        survivor = broker.add_order(orderRef='orb_googl', conId=conid,
                                    auxPrice=296.56, quantity=1.0)
        mmr = _resize_mmr(broker, conid=conid, symbol='GOOGL')
        results = mmr.execute_resize_plan(_plan())
        assert results['failures'] == []
        assert broker.positions[conid] == 1.0
        (remaining,) = broker.live_orders()
        assert remaining['orderId'] == survivor
        old = _plan()['adjustments'][0]['associated_orders'][0]
        assert remaining['orderRef'] == 'orb_googl', (
            'orderRef dropped — the exact live bug: fills become unattributed '
            'and the executor can no longer recognize its own order')
        assert remaining['orderType'] == old['orderType']
        assert remaining['auxPrice'] == old['auxPrice']
        assert remaining['tif'] == old['tif']
        assert remaining['quantity'] == 1.0


class TestJsonOutputIsValidJson:
    """`mmr --json` is the machine-readable interface the LLM trading loop
    consumes. Python's json.dumps writes bare NaN / Infinity by default, which
    is not RFC-8259 JSON: jq, Go, Rust and JavaScript's JSON.parse all reject
    it. Found 2026-07-27 by proposing an order with a NaN quantity and watching
    the output become unparseable by a strict reader.
    """

    def test_non_finite_floats_become_null(self):
        import json
        from trader.mmr_cli import _json_dumps
        out = _json_dumps({'data': {'quantity': float('nan'),
                                    'leverage': float('inf'),
                                    'other': float('-inf'),
                                    'good': 1.5}})
        assert 'NaN' not in out and 'Infinity' not in out
        parsed = json.loads(out)          # strict parse must succeed
        assert parsed['data']['quantity'] is None
        assert parsed['data']['leverage'] is None
        assert parsed['data']['other'] is None
        assert parsed['data']['good'] == 1.5

    def test_nested_structures_are_cleaned(self):
        import json
        from trader.mmr_cli import _json_dumps
        out = _json_dumps({'data': [{'a': [float('nan'), 2]}, {'b': {'c': float('inf')}}]})
        parsed = json.loads(out)
        assert parsed['data'][0]['a'] == [None, 2]
        assert parsed['data'][1]['b']['c'] is None

    def test_ordinary_payloads_are_unchanged(self):
        import json
        from trader.mmr_cli import _json_dumps
        payload = {'data': [{'symbol': 'QBTS', 'position': 3.0}], 'title': 'Portfolio'}
        assert json.loads(_json_dumps(payload)) == payload
