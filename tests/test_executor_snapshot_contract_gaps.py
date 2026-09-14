"""Terminal-order certainty across the supported historical frame adapter."""
from types import SimpleNamespace

import pandas as pd
import pytest

from trader.strategy.auto_executor import AutoExecutor


def _historical_open_snapshot(fields):
    row = dict(orderId=91, clientIntentId='opening-receipt', conId=1111,
               action='BUY', **fields)
    frame = pd.DataFrame([row])
    frame.attrs['complete'] = True
    executor = AutoExecutor.__new__(AutoExecutor)
    executor._sdk = SimpleNamespace(trades=lambda: frame.copy())
    executor._snapshot_cache = None
    intent = dict(intent_id='opening-receipt', strategy='snapshot_contract', conid=1111,
                  kind='OPEN', status='UNKNOWN', payload={'order_ids': [91]})
    return executor, intent


@pytest.mark.parametrize('fields', [{}, {'brokerStatus': '', 'status': ''},
                                   {'brokerStatus': None, 'status': None}])
def test_matching_open_without_terminal_status_retains_cancellation_reservation(fields):
    executor, intent = _historical_open_snapshot(fields)
    # A complete legacy order frame proves presence, not terminal execution.
    # Missing status must defer the close instead of raising from next().
    assert executor._opening_is_broker_terminal(intent) is False


def test_ambiguous_legacy_order_identity_cannot_authorize_terminal_open_handoff():
    executor, intent = _historical_open_snapshot(
        {'brokerStatus': 'Filled', 'status': 'Filled', 'identityAmbiguous': True})
    # Unlike the native RPC, historical frame completeness does not itself
    # encode identity ambiguity. The executor must retain its own guard.
    assert executor._opening_is_broker_terminal(intent) is False


@pytest.mark.parametrize('broker_status', ['Filled', 'Cancelled', 'ApiCancelled', 'Inactive'])
def test_terminal_broker_status_is_distinct_from_unknown_final_fill_quantity(broker_status):
    executor, intent = _historical_open_snapshot(
        {'brokerStatus': broker_status, 'status': 'Unknown', 'fillQuantityKnown': False})
    # This proves only that the opening order can no longer execute. Receipt
    # quantity certainty and request retirement are separate obligations.
    assert executor._opening_is_broker_terminal(intent) is True
