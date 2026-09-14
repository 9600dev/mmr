"""Operator reconciliation tools: the human path out of a deferred state.

The execution contract replaced "refuse" with "defer" for exits whose capacity
is uncertain. A deferral with no operator surface is a naked position behind
an actionable-sounding log line. These tests pin the CLI/SDK surface for the
three release acts (settle a reservation, resolve an intent, acknowledge a
restore marker): they are operator-only, require --attest, forward the exact
arguments, and never run automatically. No service or broker is involved.
"""

import json
from unittest.mock import Mock

import pytest

from trader import mmr_cli
from trader.common.reactivex import SuccessFail


def _dispatch(mmr, argv, capsys, monkeypatch, role=None):
    if role is None:
        monkeypatch.delenv('MMR_ROLE', raising=False)
    else:
        monkeypatch.setenv('MMR_ROLE', role)
    args = mmr_cli.build_parser().parse_args(argv)
    keep_going = mmr_cli.dispatch(mmr, args)
    out = capsys.readouterr().out
    return keep_going, out


def _json_lines(out):
    rows = []
    for line in out.splitlines():
        line = line.strip()
        if line.startswith('{'):
            rows.append(json.loads(line))
    return rows


@pytest.fixture
def mmr():
    m = Mock()
    m.list_order_reservations.return_value = []
    m.list_execution_intents.return_value = []
    m.restore_marker_status.return_value = {'present': False}
    return m


# --- role gate ------------------------------------------------------------

@pytest.mark.parametrize('role', ['proposer', 'approver'])
@pytest.mark.parametrize('argv,sdk_call', [
    (['--json', 'reservations', 'settle', 'i1', '1', '42', '--reason', 'r', '--attest'], 'settle_order_reservation'),
    (['--json', 'restore-ack', '--reason', 'r', '--attest'], 'acknowledge_restore_marker'),
    (['--json', 'strategies', 'resolve-intent', 'i1', '--reason', 'r', '--attest'], 'resolve_execution_intent'),
])
def test_release_acts_are_operator_only(mmr, capsys, monkeypatch, role, argv, sdk_call):
    _, out = _dispatch(mmr, argv, capsys, monkeypatch, role=role)
    payload = _json_lines(out)[-1]
    assert payload['error'] is True
    assert f'MMR_ROLE={role}' in payload['message']
    getattr(mmr, sdk_call).assert_not_called()


@pytest.mark.parametrize('role', ['proposer', 'approver'])
@pytest.mark.parametrize('argv,sdk_call', [
    (['--json', 'reservations'], 'list_order_reservations'),
    (['--json', 'reservations', 'list', '--all'], 'list_order_reservations'),
    (['--json', 'restore-ack', '--status'], 'restore_marker_status'),
    (['--json', 'strategies', 'intents'], 'list_execution_intents'),
])
def test_reads_stay_available_to_every_role(mmr, capsys, monkeypatch, role, argv, sdk_call):
    _, out = _dispatch(mmr, argv, capsys, monkeypatch, role=role)
    assert not any(p.get('error') for p in _json_lines(out))
    getattr(mmr, sdk_call).assert_called_once()


# --- attestation is required before any release act --------------------------

@pytest.mark.parametrize('argv,sdk_call', [
    (['--json', 'reservations', 'settle', 'i1', '1', '42', '--reason', 'r'], 'settle_order_reservation'),
    (['--json', 'restore-ack', '--reason', 'r'], 'acknowledge_restore_marker'),
    (['--json', 'strategies', 'resolve-intent', 'i1', '--reason', 'r'], 'resolve_execution_intent'),
])
def test_release_acts_require_attest(mmr, capsys, monkeypatch, argv, sdk_call):
    _, out = _dispatch(mmr, argv, capsys, monkeypatch)
    statuses = [p for p in _json_lines(out) if 'success' in p]
    assert statuses and statuses[-1]['success'] is False
    assert '--attest' in statuses[-1]['message']
    getattr(mmr, sdk_call).assert_not_called()


# --- argument plumbing -------------------------------------------------------

def test_settle_forwards_exact_identity_and_reports_outcome(mmr, capsys, monkeypatch):
    mmr.settle_order_reservation.return_value = {'settled': True, 'reason': 'audit RESERVATION_SETTLED #7'}
    _, out = _dispatch(mmr, ['--json', 'reservations', 'settle', 'proposal:9', '3', '4711',
                             '--reason', 'IB has no order 4711 after restart', '--attest'], capsys, monkeypatch)
    mmr.settle_order_reservation.assert_called_once_with('proposal:9', 3, 4711, 'IB has no order 4711 after restart')
    payload = _json_lines(out)[-1]
    assert payload['success'] is True
    assert 'audit RESERVATION_SETTLED #7' in payload['message']

    mmr.settle_order_reservation.return_value = {'settled': False, 'reason': 'live observation Submitted matches'}
    _, out = _dispatch(mmr, ['--json', 'reservations', 'settle', 'proposal:9', '3', '4711',
                             '--reason', 'x', '--attest'], capsys, monkeypatch)
    payload = _json_lines(out)[-1]
    assert payload['success'] is False
    assert 'live observation' in payload['message']


def test_reservations_list_passes_filters_and_emits_rows(mmr, capsys, monkeypatch):
    rows = [{'intent_id': 'i1', 'client_id': 1, 'order_id': 42, 'conid': 7, 'action': 'SELL',
             'quantity': 40.0, 'blocking': True, 'observation': None}]
    mmr.list_order_reservations.return_value = rows
    _, out = _dispatch(mmr, ['--json', 'reservations', 'list', '--all', '--account', 'DU1'], capsys, monkeypatch)
    mmr.list_order_reservations.assert_called_once_with('DU1', True)
    payload = _json_lines(out)[-1]
    assert payload['data'] == {'reservations': rows}

    mmr.list_order_reservations.reset_mock()
    _dispatch(mmr, ['--json', 'reservations'], capsys, monkeypatch)
    mmr.list_order_reservations.assert_called_once_with(None, False)


def test_intents_list_passes_filters(mmr, capsys, monkeypatch):
    rows = [{'intent_id': 'i1', 'kind': 'OPEN', 'status': 'SUBMITTING', 'strategy': 's', 'conid': 7,
             'order_ids': [], 'blocking': 'blocks opens and exits'}]
    mmr.list_execution_intents.return_value = rows
    _, out = _dispatch(mmr, ['--json', 'strategies', 'intents', '--strategy', 's', '--conid', '7'], capsys, monkeypatch)
    mmr.list_execution_intents.assert_called_once_with('s', 7, active_only=True)
    assert _json_lines(out)[-1]['data'] == {'intents': rows}

    mmr.list_execution_intents.reset_mock()
    _dispatch(mmr, ['--json', 'strategies', 'intents', '--all'], capsys, monkeypatch)
    mmr.list_execution_intents.assert_called_once_with(None, None, active_only=False)


def test_resolve_intent_reports_server_verdict(mmr, capsys, monkeypatch):
    mmr.resolve_execution_intent.return_value = SuccessFail.success(
        {'status': 'RESOLVED', 'kind': 'OPEN', 'strategy': 's', 'conid': 7})
    _, out = _dispatch(mmr, ['--json', 'strategies', 'resolve-intent', 'i1', '--reason', 'never sent', '--attest'],
                       capsys, monkeypatch)
    mmr.resolve_execution_intent.assert_called_once_with('i1', 'never sent')
    assert _json_lines(out)[-1]['success'] is True

    mmr.resolve_execution_intent.return_value = SuccessFail.fail(error='order 42 is Submitted at IB')
    _, out = _dispatch(mmr, ['--json', 'strategies', 'resolve-intent', 'i1', '--reason', 'x', '--attest'],
                       capsys, monkeypatch)
    payload = _json_lines(out)[-1]
    assert payload['success'] is False
    assert 'Submitted at IB' in payload['message']


def test_restore_ack_status_is_read_only_and_ack_forwards_reason(mmr, capsys, monkeypatch):
    mmr.restore_marker_status.return_value = {'present': True, 'written': '2026-09-14T00:00:00Z'}
    _, out = _dispatch(mmr, ['--json', 'restore-ack', '--status'], capsys, monkeypatch)
    assert _json_lines(out)[-1]['data']['present'] is True
    mmr.acknowledge_restore_marker.assert_not_called()

    mmr.acknowledge_restore_marker.return_value = {'acknowledged': True}
    _, out = _dispatch(mmr, ['--json', 'restore-ack', '--reason', 'reconciled vs IB', '--attest'], capsys, monkeypatch)
    mmr.acknowledge_restore_marker.assert_called_once_with('reconciled vs IB')
    assert _json_lines(out)[-1]['success'] is True


def test_sdk_wrappers_forward_exact_arguments():
    from trader import sdk

    class _Chain:
        def __init__(self, log):
            self.log = log

        def __getattr__(self, name):
            def call(*args):
                self.log.append((name, args))
                return {'name': name} if name != 'list_execution_intents' and not name.startswith('list_') else [{'name': name}]
            return call

    class _RPC:
        is_setup = True

        def __init__(self):
            self.log = []

        def rpc(self, **kwargs):
            return _Chain(self.log)

    m = object.__new__(sdk.MMR)
    m._client = _RPC()  # the `_rpc` property hands back the connected client
    assert m.list_execution_intents('s', '7', active_only=False) == [{'name': 'list_execution_intents'}]
    assert m.list_order_reservations('DU1', True) == [{'name': 'list_order_reservations'}]
    assert m.settle_order_reservation('i1', '3', '42', 'why') == {'name': 'settle_order_reservation'}
    assert m.acknowledge_restore_marker('why') == {'name': 'acknowledge_restore_marker'}
    assert m.restore_marker_status() == {'name': 'restore_marker_status'}
    assert m._rpc.log == [
        ('list_execution_intents', ('s', 7, False)),
        ('list_order_reservations', ('DU1', True)),
        ('settle_order_reservation', ('i1', 3, 42, 'why')),
        ('acknowledge_restore_marker', ('why',)),
        ('restore_marker_status', ()),
    ]
