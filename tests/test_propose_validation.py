"""propose refuses malformed proposals at the door (2026-11-05 findings).

`propose F BUY --quantity -5` and `propose ZZZZQX BUY ...` were both stored
PENDING; only approve refused them. Now propose refuses, and pins the resolved
conId when trader_service is reachable.
"""
import math

import pytest

from test_propose_approve_integration import _make_mmr, _StubRPCClient
from trader.common.reactivex import SuccessFail


class _NoSuchSymbolRPC(_StubRPCClient):
    def rpc(self, return_type=None):
        chain = super().rpc(return_type)
        outer = self

        class _Wrap:
            def __init__(self, inner, names=()):
                self._inner, self._names = inner, names

            def __getattr__(self, name):
                return _Wrap(getattr(self._inner, name), self._names + (name,))

            def __call__(self, *a, **k):
                sym = getattr(a[0], 'symbol', a[0] if a else k.get('symbol')) if a or k else None
                if self._names and self._names[-1] in ('resolve_contract', 'resolve_symbol') \
                        and str(sym) == 'ZZZZQX':
                    return []
                return self._inner(*a, **k)
        return _Wrap(chain)


def _mmr(tmp_duckdb_path, rpc=None):
    return _make_mmr(tmp_duckdb_path, rpc or _StubRPCClient(SuccessFail.success(obj=[])))


@pytest.mark.parametrize('qty', [-5, 0, float('nan'), float('inf'), True])
def test_bad_quantity_refused(tmp_duckdb_path, qty):
    mmr = _mmr(tmp_duckdb_path)
    with pytest.raises(ValueError, match='quantity'):
        mmr.propose('F', 'BUY', quantity=qty)
    assert mmr._proposal_store().query(limit=10) == []


@pytest.mark.parametrize('amt', [-100, 0, float('nan')])
def test_bad_amount_refused(tmp_duckdb_path, amt):
    mmr = _mmr(tmp_duckdb_path)
    with pytest.raises(ValueError, match='amount'):
        mmr.propose('F', 'BUY', amount=amt)


def test_bad_action_refused(tmp_duckdb_path):
    with pytest.raises(ValueError, match='action'):
        _mmr(tmp_duckdb_path).propose('F', 'HOLD', quantity=1)


def test_unresolvable_symbol_refused_when_service_up(tmp_duckdb_path):
    mmr = _mmr(tmp_duckdb_path, _NoSuchSymbolRPC(SuccessFail.success(obj=[])))
    with pytest.raises(ValueError, match='ZZZZQX'):
        mmr.propose('ZZZZQX', 'BUY', quantity=1)
    assert mmr._proposal_store().query(limit=10) == []


def test_resolved_conid_is_pinned(tmp_duckdb_path):
    mmr = _mmr(tmp_duckdb_path)
    pid, _, _ = mmr.propose('F', 'BUY', quantity=1)
    assert mmr._proposal_store().get(pid).metadata.get('con_id') == 1


def test_service_down_still_proposes_but_flags_unresolved(tmp_duckdb_path):
    mmr = _mmr(tmp_duckdb_path)
    mmr._client = None                 # not connected
    pid, _, _ = mmr.propose('F', 'BUY', quantity=1)
    meta = mmr._proposal_store().get(pid).metadata
    assert meta.get('unresolved_at_propose') is True and 'con_id' not in meta
