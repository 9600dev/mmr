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


class TestReCreateRoundTrip:
    """A re-created protective equals the old order except quantity —
    the round-trip property the live test proved was violated for orderRef."""

    def _mmr_with_captured_rpc(self):
        mmr = MMR.__new__(MMR)
        captured = []

        class _Rpc:
            def rpc(self, return_type=None):
                svc = MagicMock()
                def place_standalone_order(**kw):
                    captured.append(kw)
                    ok = MagicMock(); ok.is_success.return_value = True
                    return iter([ok])
                svc.place_standalone_order = place_standalone_order
                return svc

        # _rpc is a read-only property returning self._client (after an
        # is_setup check) — give it a client-shaped stub.
        stub = _Rpc()
        stub.is_setup = True
        mmr._client = stub
        mmr._contract_map = {'GOOGL': SimpleNamespace(conId=208813719)}
        mmr.cancel = MagicMock(return_value=SimpleNamespace(is_success=lambda: True))
        # the delta leg: pretend the trim filled
        ok = SimpleNamespace(is_success=lambda: True, error=None)
        mmr._place_order = MagicMock(return_value=ok)
        mmr.sell = MagicMock(return_value=ok)
        mmr.buy = MagicMock(return_value=ok)
        return mmr, captured

    def test_re_created_stop_preserves_ref_price_type_tif(self):
        mmr, captured = self._mmr_with_captured_rpc()
        results = mmr.execute_resize_plan(_plan())
        assert results['failures'] == []
        assert len(captured) == 1, f'expected one re-create, got {captured}'
        re = captured[0]
        old = _plan()['adjustments'][0]['associated_orders'][0]
        assert re['order_ref'] == 'orb_googl', (
            'orderRef dropped — the exact live bug: fills become unattributed '
            'and the executor can no longer recognize its own order')
        assert re['order_type'] == old['orderType']
        assert re['aux_price'] == old['auxPrice']
        assert re['tif'] == old['tif']
        assert re['quantity'] == 1.0            # the one INTENDED change
