"""The restore marker needs the deliberate human act the contract names.

``restore_snapshot`` writes ``BROKER_RECONCILIATION_REQUIRED.json`` and
``opening_restore_error`` refuses every open while it exists. Until now the
only way to clear it was to delete the file by hand, leaving no record of who
decided the restore gap was reconciled or why.
"""
import json
from pathlib import Path

import pytest
from ib_async import Position

from review.test_review_order_contract import _coordinated_trader, _stock
from trader.data.db_backup import (ACKNOWLEDGED_MARKER_PREFIX, RESTORE_MARKER, acknowledge_restore_marker,
                                   read_restore_marker)
from trader.messaging.trader_service_api import TraderServiceApi
from trader.trading.risk_gate import RiskGate, RiskLimits


def _write_marker(tmp_path):
    (tmp_path / RESTORE_MARKER).write_text(json.dumps({'status': 'BROKER_RECONCILIATION_REQUIRED',
                                                       'snapshot_id': 'snap-1'}))


def test_read_and_acknowledge_marker_files(tmp_path):
    assert read_restore_marker(tmp_path) == {'present': False, 'path': str(tmp_path / RESTORE_MARKER),
                                             'marker': None, 'error': None}
    with pytest.raises(FileNotFoundError):
        acknowledge_restore_marker(tmp_path, 'nothing to ack')
    _write_marker(tmp_path)
    status = read_restore_marker(tmp_path)
    assert status['present'] and status['marker']['snapshot_id'] == 'snap-1'
    with pytest.raises(ValueError):
        acknowledge_restore_marker(tmp_path, '   ')
    assert (tmp_path / RESTORE_MARKER).exists(), 'a refused acknowledgment changes nothing'

    record = acknowledge_restore_marker(tmp_path, 'reviewed positions and orders', actor='operator',
                                        evidence={'observed_at': 'now'})

    assert not (tmp_path / RESTORE_MARKER).exists()
    written, = tmp_path.glob(f'{ACKNOWLEDGED_MARKER_PREFIX}*.json')
    assert Path(record['path']) == written
    persisted = json.loads(written.read_text())
    assert persisted['reason'] == 'reviewed positions and orders'
    assert persisted['marker']['snapshot_id'] == 'snap-1'
    assert persisted['evidence'] == {'observed_at': 'now'}
    assert persisted['status'] == 'BROKER_RECONCILIATION_ACKNOWLEDGED'


@pytest.mark.asyncio
async def test_marker_status_rpc_reports_blocking_marker(tmp_path):
    trader = _coordinated_trader(tmp_path)
    api = TraderServiceApi(trader)
    try:
        assert api.restore_marker_status()['present'] is False
        assert api.restore_marker_status()['opening_blocked'] is False
        _write_marker(tmp_path)
        status = api.restore_marker_status()
        assert status['present'] and status['opening_blocked']
        assert status['marker']['snapshot_id'] == 'snap-1'
        assert 'restore-marker ack' in status['reason']
    finally:
        trader.order_tracker.close(timeout=1)


@pytest.mark.asyncio
async def test_acknowledgment_requires_reason_marker_and_complete_snapshot(tmp_path):
    trader = _coordinated_trader(tmp_path)
    api = TraderServiceApi(trader)
    try:
        none = await api.acknowledge_restore_marker('reviewed')
        assert none['acknowledged'] is False and 'no restore marker' in none['error']
        _write_marker(tmp_path)
        empty = await api.acknowledge_restore_marker(' ')
        assert empty['acknowledged'] is False and 'reason' in empty['error']
        trader.client.ib.managedAccounts = lambda: ['different-account']
        incomplete = await api.acknowledge_restore_marker('reviewed')
        assert incomplete['acknowledged'] is False and 'not complete' in incomplete['error']
        assert incomplete['snapshot']['account_confirmed'] is False
        assert (tmp_path / RESTORE_MARKER).exists()
        assert trader.opening_restore_error()
    finally:
        trader.order_tracker.close(timeout=1)


@pytest.mark.asyncio
async def test_acknowledgment_reenables_opens_and_leaves_a_record(tmp_path):
    trader = _coordinated_trader(tmp_path)
    trader.risk_gate = RiskGate(RiskLimits(), trader.event_store)
    api = TraderServiceApi(trader)
    try:
        _write_marker(tmp_path)
        blocked = await api.place_expressive_order(_stock(), 'BUY', 1, {'order_type': 'MARKET'})
        assert not blocked.is_success() and 'reconciliation' in blocked.error

        ack = await api.acknowledge_restore_marker('reviewed execution-snapshot: 100 held, no working orders')

        assert ack['acknowledged'] is True, ack
        assert ack['evidence']['complete'] is True and ack['evidence']['positions'] == 1
        assert trader.opening_restore_error() is None
        assert api.restore_marker_status()['present'] is False
        assert not (tmp_path / RESTORE_MARKER).exists()
        record = json.loads(Path(ack['path']).read_text())
        assert record['reason'].startswith('reviewed execution-snapshot')

        opened = await api.place_expressive_order(_stock(), 'BUY', 1, {'order_type': 'MARKET'})
        assert opened.is_success(), opened.error
        again = await api.acknowledge_restore_marker('twice')
        assert again['acknowledged'] is False and 'no restore marker' in again['error']
    finally:
        trader.order_tracker.close(timeout=1)
