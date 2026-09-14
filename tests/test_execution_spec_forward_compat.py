"""Stored proposals written by a newer build must still load (review 2026-09-11).

``ExecutionSpec.from_dict`` now refuses unknown keys, which is right at the RPC
boundary but made every proposal listing fail after a rollback: one row with a
field this build does not know broke ``proposals``, ``reject`` and expiry for
all of them. The store drops such fields (and logs it) instead.
"""
import json

import pytest

from trader.data.proposal_store import ProposalStore
from trader.trading.proposal import ExecutionSpec, TradeProposal


def test_the_rpc_boundary_stays_strict():
    with pytest.raises(ValueError, match='unknown execution fields'):
        ExecutionSpec.from_dict({'order_type': 'MARKET', 'iceberg_display': 5})
    with pytest.raises(ValueError, match='mapping'):
        ExecutionSpec.from_dict('MARKET')  # type: ignore[arg-type]


def test_stored_rows_from_a_newer_build_still_load(tmp_path, caplog):
    store = ProposalStore(str(tmp_path / 'proposals.duckdb'))
    pid = store.add(TradeProposal('AMD', 'BUY', quantity=1, metadata={'con_id': 4391}))
    newer = json.dumps({'order_type': 'LIMIT', 'limit_price': 150.0, 'iceberg_display': 5})
    store.db.execute('UPDATE trade_proposals SET execution=? WHERE id=?', [newer, pid])

    with caplog.at_level('WARNING'):
        loaded = store.get(pid)

    assert loaded.execution == ExecutionSpec(order_type='LIMIT', limit_price=150.0)
    assert 'iceberg_display' in caplog.text and f'#{pid}' in caplog.text
    # What this build can honour is exactly what it compares against the wire.
    assert loaded.execution.to_dict() == {'order_type': 'LIMIT', 'limit_price': 150.0,
                                          'exit_type': 'NONE', 'tif': 'DAY', 'outside_rth': True}
