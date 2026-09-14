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
