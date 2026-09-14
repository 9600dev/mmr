"""Connection diagnostics must not certify an unavailable IB session.

All connections and callbacks are local test objects; no service or broker
connection is opened. Existing account-PnL tests cover risk readiness.
"""

import asyncio
import logging
import socket
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from eventkit import Event
from ib_async import IB

from trader.trading import trading_runtime
from trader.trading.trading_runtime import Trader


def _ib(connected=True):
    ib = SimpleNamespace(
        connected=connected,
        connectedEvent=Event('connectedEvent'),
        disconnectedEvent=Event('disconnectedEvent'),
    )
    ib.isConnected = lambda: ib.connected
    return ib


def _trader():
    trader = object.__new__(Trader)
    trader.client = SimpleNamespace(ib=_ib())
    trader.data = object()
    trader._ib_upstream_connected = True
    trader._ib_upstream_error = ''
    trader._ib_upstream_ib = trader.client.ib
    # Exercise connection/PnL binding before the existing setup coalescer,
    # without starting unrelated market-data subscriptions.
    trader._in_connected_event = True
    return trader


def test_socket_loss_overrides_warm_cache_without_waiting_for_callback(monkeypatch):
    trader = _trader()
    monkeypatch.setattr(trading_runtime.time, 'monotonic', lambda: 100.0)
    connected = trader.status()
    assert connected['ib_connected'] is True
    assert connected['ib_upstream_connected'] is True

    trader.client.ib.connected = False
    disconnected = trader.status()
    assert disconnected['ib_connected'] is False
    assert disconnected['ib_upstream_connected'] is False
    assert 'socket disconnected' in disconnected['ib_upstream_error']
    assert disconnected is not connected
    assert connected['ib_upstream_connected'] is True


@pytest.mark.parametrize('restore_code', [1101, 1102])
def test_upstream_loss_and_restore_bypass_warm_cache(restore_code):
    trader = _trader()
    trader.status()
    trader._on_ib_error(SimpleNamespace(errorCode=1100, errorString='IB session lost'))
    lost = trader.status()
    assert lost['ib_connected'] is True
    assert lost['ib_upstream_connected'] is False
    assert lost['ib_upstream_error'] == 'IB session lost'

    trader._on_ib_error(SimpleNamespace(errorCode=restore_code, errorString='restored'))
    restored = trader.status()
    assert restored['ib_upstream_connected'] is True
    assert 'ib_upstream_error' not in restored


@pytest.mark.parametrize('code', [2103, 2105, 2157])
def test_farm_warning_does_not_change_connected_session_status(code):
    trader = _trader()
    trader.status()
    trader._on_ib_error(SimpleNamespace(errorCode=code, errorString='one farm down'))
    assert trader.status()['ib_upstream_connected'] is True
    assert trader._ib_farms_down[code] == 'one farm down'
    trader.client.ib.connected = False
    assert trader.status()['ib_upstream_connected'] is False


@pytest.mark.asyncio
@pytest.mark.parametrize('prior_error', ['', 'IB session lost (1100)'])
async def test_disconnect_coalescing_clears_status_and_preserves_explicit_cause(prior_error):
    trader = _trader()
    trader._ib_upstream_connected = not bool(prior_error)
    trader._ib_upstream_error = prior_error
    trader.status()
    trader.client.ib.connected = False
    trader._reconnecting = True

    await trader.disconnected_event()

    assert trader._ib_upstream_connected is False
    assert trader.status()['ib_upstream_connected'] is False
    assert trader._ib_upstream_error == (prior_error or 'IB Gateway socket disconnected')


@pytest.mark.asyncio
async def test_delayed_disconnect_does_not_clear_connected_replacement():
    trader = _trader()
    trader._reconnecting = True
    await trader.disconnected_event()
    assert trader._ib_upstream_connected is True
    assert trader.status()['ib_upstream_connected'] is True


@pytest.mark.asyncio
async def test_new_ib_session_recovers_but_same_instance_1100_stays_authoritative():
    trader = _trader()
    trader._on_ib_error(SimpleNamespace(errorCode=1100, errorString='old session lost'))
    await trader.connected_event()
    assert trader.status()['ib_upstream_connected'] is False

    trader.client.ib = _ib()
    await trader.connected_event()
    assert trader.status()['ib_upstream_connected'] is True
    assert 'ib_upstream_error' not in trader.status()


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', [TimeoutError(), ConnectionRefusedError('API port closed')])
async def test_reconnect_names_failure_and_continues_to_fresh_session(monkeypatch, caplog, failure):
    trader = _trader()
    trader.client.ib.connected = False
    replacement = _ib()
    calls = 0

    async def connect():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise failure
        trader.client.ib = replacement

    trader.client.connect_async = connect
    sleep = AsyncMock()
    monkeypatch.setattr(trading_runtime.asyncio, 'sleep', sleep)
    caplog.set_level(logging.INFO)

    await trader.disconnected_event()

    assert calls == 2
    assert [call.args[0] for call in sleep.await_args_list] == [2, 4]
    assert f'reconnection attempt 1 failed: {type(failure).__name__}:' in caplog.text
    assert 'reconnected to IB Gateway on attempt 2' in caplog.text
    assert trader._reconnecting is False
    assert trader.connected_event in replacement.connectedEvent
    assert trader.disconnected_event in replacement.disconnectedEvent
    assert trader.status()['ib_upstream_connected'] is True


def test_pulse_does_not_report_upstream_connected_without_socket(caplog):
    trader = _trader()
    trader.client.ib.connected = False
    caplog.set_level(logging.INFO)
    trader._log_pulse()
    assert 'pulse ib_connected=False ib_upstream=False' in caplog.text


@pytest.mark.asyncio
async def test_native_disconnected_ib_ping_reports_named_failure():
    trader = _trader()
    trader.client.ib = IB()
    result = await trader.ping_ib()
    assert result['ok'] is False
    assert result['error'].startswith('ConnectionError:')


def _native_clock_ib(monkeypatch):
    """Retain native request/future/callback handling; replace only wire send."""
    ib = IB()
    sent = asyncio.Event()
    requests = []

    def send():
        requests.append('currentTime')
        sent.set()

    monkeypatch.setattr(ib.client, 'reqCurrentTime', send)
    return ib, sent, requests


async def _finish_local_pings(tasks, *instances):
    # Release native futures on assertion failure as well as success. No task
    # or broker request is allowed to escape this test's event loop.
    await asyncio.sleep(0)
    for ib in instances:
        ib.wrapper.currentTime(1_800_000_000)
    for task in tasks:
        if not task.done():
            task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_simultaneous_pings_share_native_future_but_later_ping_is_fresh(monkeypatch):
    trader = _trader()
    ib, sent, requests = _native_clock_ib(monkeypatch)
    trader.client.ib = ib
    tasks = [asyncio.create_task(trader.ping_ib()) for _ in range(3)]
    try:
        await asyncio.wait_for(sent.wait(), timeout=2)
        assert requests == ['currentTime']
        ib.wrapper.currentTime(1_800_000_000)
        results = await asyncio.gather(*tasks)
        assert all(result['ok'] is True for result in results)
        assert results[0] == results[1] == results[2]

        sent.clear()
        later = asyncio.create_task(trader.ping_ib())
        tasks.append(later)
        await asyncio.wait_for(sent.wait(), timeout=2)
        assert requests == ['currentTime', 'currentTime']
        ib.wrapper.currentTime(1_800_000_001)
        fresh = await later
        assert fresh['ok'] is True
        assert fresh['ib_server_time'] != results[0]['ib_server_time']
    finally:
        await _finish_local_pings(tasks, ib)


@pytest.mark.asyncio
async def test_cancelling_one_ping_does_not_cancel_another_callers_native_future(monkeypatch):
    trader = _trader()
    ib, sent, requests = _native_clock_ib(monkeypatch)
    trader.client.ib = ib
    tasks = [asyncio.create_task(trader.ping_ib()) for _ in range(2)]
    try:
        await asyncio.wait_for(sent.wait(), timeout=2)
        tasks[0].cancel()
        with pytest.raises(asyncio.CancelledError):
            await tasks[0]
        assert not tasks[1].done()
        assert not ib.wrapper._futures['currentTime'].cancelled()
        assert requests == ['currentTime']
        ib.wrapper.currentTime(1_800_000_000)
        assert (await tasks[1])['ok'] is True
    finally:
        await _finish_local_pings(tasks, ib)


@pytest.mark.asyncio
async def test_ib_replacement_gets_fresh_ping_and_old_reply_cannot_certify_it(monkeypatch):
    trader = _trader()
    old, old_sent, old_requests = _native_clock_ib(monkeypatch)
    new, new_sent, new_requests = _native_clock_ib(monkeypatch)
    trader.client.ib = old
    tasks = [asyncio.create_task(trader.ping_ib())]
    try:
        await asyncio.wait_for(old_sent.wait(), timeout=2)
        trader.client.ib = new
        current = asyncio.create_task(trader.ping_ib())
        tasks.append(current)
        await asyncio.wait_for(new_sent.wait(), timeout=2)
        assert old_requests == new_requests == ['currentTime']
        old.wrapper.currentTime(1_800_000_000)
        obsolete = await tasks[0]
        assert obsolete['ok'] is False
        assert obsolete['error'] == 'ConnectionError: IB connection replaced during ping'
        assert not current.done()

        joined = asyncio.create_task(trader.ping_ib())
        tasks.append(joined)
        await asyncio.sleep(0)
        assert new_requests == ['currentTime']
        new.wrapper.currentTime(1_800_000_001)
        results = await asyncio.gather(current, joined)
        assert all(result['ok'] is True for result in results)
        assert results[0] == results[1]
    finally:
        await _finish_local_pings(tasks, old, new)


@pytest.mark.asyncio
async def test_shared_ping_timeout_reaches_all_callers_and_next_ping_retries(monkeypatch):
    trader = _trader()
    ib, sent, requests = _native_clock_ib(monkeypatch)
    trader.client.ib = ib
    tasks = [asyncio.create_task(trader.ping_ib()) for _ in range(3)]
    try:
        await asyncio.wait_for(sent.wait(), timeout=2)
        results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=8)
        assert requests == ['currentTime']
        assert all(result['ok'] is False for result in results)
        assert all(result['error'].startswith('TimeoutError:') for result in results)

        sent.clear()
        retry = asyncio.create_task(trader.ping_ib())
        tasks.append(retry)
        await asyncio.wait_for(sent.wait(), timeout=2)
        assert requests == ['currentTime', 'currentTime']
        ib.wrapper.currentTime(1_800_000_002)
        assert (await retry)['ok'] is True
    finally:
        await _finish_local_pings(tasks, ib)


def _rate_limited_clock_ib(monkeypatch, *, reply_limit=None):
    """Model the observed gateway: no reply to another subsecond request."""
    ib = IB()
    requests, responses, suppressed = [], [], []
    loop = asyncio.get_running_loop()

    def send():
        now = loop.time()
        requests.append(now)
        if responses and now - responses[-1] < 1.0:
            suppressed.append(now)
            return
        if reply_limit is not None and len(responses) >= reply_limit:
            return
        timestamp = 1_800_000_000 + len(requests)

        def respond():
            responses.append(loop.time())
            ib.wrapper.currentTime(timestamp)

        loop.call_soon(respond)

    monkeypatch.setattr(ib.client, 'reqCurrentTime', send)
    return ib, requests, responses, suppressed


@pytest.mark.asyncio
async def test_back_to_back_pings_space_fresh_wire_requests_for_native_gateway(monkeypatch):
    trader = _trader()
    ib, requests, responses, suppressed = _rate_limited_clock_ib(monkeypatch)
    trader.client.ib = ib
    first = await trader.ping_ib()
    second = await trader.ping_ib()
    assert first['ok'] is second['ok'] is True
    assert first['ib_server_time'] != second['ib_server_time']
    assert len(requests) == len(responses) == 2
    assert requests[1] - responses[0] >= 1.0
    assert suppressed == []


@pytest.mark.asyncio
async def test_spacing_is_inside_shared_five_second_deadline(monkeypatch):
    trader = _trader()
    ib, requests, responses, suppressed = _rate_limited_clock_ib(monkeypatch, reply_limit=1)
    trader.client.ib = ib
    assert (await trader.ping_ib())['ok'] is True
    started = asyncio.get_running_loop().time()
    tasks = [asyncio.create_task(trader.ping_ib()) for _ in range(3)]
    try:
        # A separate five-second response budget AFTER spacing would take six
        # seconds. Bound the complete shared attempt, not just the wire wait.
        results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.6)
        assert asyncio.get_running_loop().time() - started < 5.6
        assert len(requests) == 2
        assert requests[1] - responses[0] >= 1.0
        assert suppressed == []
        assert all(result['ok'] is False for result in results)
        assert all(result['error'].startswith('TimeoutError:') for result in results)
    finally:
        await _finish_local_pings(tasks, ib)


@pytest.mark.asyncio
async def test_late_old_ib_reply_does_not_replace_new_sessions_spacing_state(monkeypatch):
    trader = _trader()
    old, old_sent, _ = _native_clock_ib(monkeypatch)
    new, requests, responses, suppressed = _rate_limited_clock_ib(monkeypatch)
    trader.client.ib = old
    tasks = [asyncio.create_task(trader.ping_ib())]
    try:
        await asyncio.wait_for(old_sent.wait(), timeout=2)
        trader.client.ib = new
        first = await trader.ping_ib()
        assert first['ok'] is True
        old.wrapper.currentTime(1_800_000_000)
        obsolete = await tasks[0]
        assert obsolete['ok'] is False
        second = await trader.ping_ib()
        assert second['ok'] is True
        assert second['ib_server_time'] != first['ib_server_time']
        assert len(requests) == 2
        assert requests[1] - responses[0] >= 1.0
        assert suppressed == []
    finally:
        await _finish_local_pings(tasks, old, new)


@pytest.mark.parametrize('upstream,ping,expected', [
    (True, 'success', 'PASS'),
    (True, 'disconnected', 'FAIL'),
    (True, 'rpc_failure', 'FAIL'),
    (False, 'success', 'FAIL'),
    (False, 'disconnected', 'FAIL'),
])
def test_cli_upstream_pass_requires_live_ping_and_upstream_flag(
        monkeypatch, tmp_path, upstream, ping, expected):
    from trader import mmr_cli, sdk
    from trader.container import Container
    from trader.messaging import clientserver

    mmr = Mock()
    mmr.connect.return_value = mmr
    mmr.get_service_status.return_value = {
        'ib_connected': True,
        'ib_upstream_connected': upstream,
        'ib_upstream_error': 'IB session lost',
    }
    mmr.ping_ib.return_value = (
        {'ok': True, 'ib_server_time': 'test time'} if ping == 'success'
        else {'ok': False, 'error': 'ConnectionError: Not connected'})
    if ping == 'rpc_failure':
        mmr.ping_ib.side_effect = TimeoutError('test RPC timeout')
    mmr.market_hours.return_value = []
    monkeypatch.setattr(sdk, 'MMR', Mock(return_value=mmr))

    config = SimpleNamespace(config=lambda: {})
    monkeypatch.setattr(Container, 'instance', staticmethod(lambda: config))
    strategy_client = SimpleNamespace(
        connect=AsyncMock(),
        rpc=lambda **kwargs: SimpleNamespace(runtime_status=lambda: {}),
    )
    monkeypatch.setattr(clientserver, 'RPCClient', lambda **kwargs: strategy_client)
    monkeypatch.setattr(mmr_cli, '_strategy_config_path', lambda: tmp_path / 'absent.yaml')
    monkeypatch.setattr(socket, 'create_connection', Mock(side_effect=ConnectionRefusedError('test only')))

    checks = {row['check']: row for row in mmr_cli._collect_stack_checks()}

    assert checks['trader_service']['status'] == 'PASS'
    assert checks['strategy_service']['status'] == 'PASS'
    assert checks['ib_socket']['status'] == ('PASS' if ping == 'success' else 'FAIL')
    assert checks['ib_upstream']['status'] == expected
    if upstream and ping != 'success':
        assert 'live IB round-trip failed' in checks['ib_upstream']['detail']
    if not upstream:
        assert 'IB session lost' in checks['ib_upstream']['detail']
