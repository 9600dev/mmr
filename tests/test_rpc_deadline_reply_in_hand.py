"""A matching RPC reply already received is never discarded (2026-09-14).

``_SyncMethodCall.__call__`` polled, read the reply, matched its ``req_id`` —
and then threw it away if ``monotonic()`` had crossed the deadline while the
frame was being read, raising ``TimeoutError ... outcome UNKNOWN``. For an
order placement that turns a KNOWN result the server delivered into an
ambiguous one the caller must reconcile. The deadline bounds the wait, not
whether a reply in hand is believed.
"""
import time

import pytest
import zmq

from trader.messaging.clientserver import RPCClient, pack, unpack


class _SlowReadSocket:
    """poll says a reply is ready; reading it takes longer than the budget."""

    def __init__(self, read_seconds, reply):
        self.read_seconds = read_seconds
        self.reply = reply
        self.sent = []
        self.closed = False

    def poll(self, timeout_ms, flags):
        return True

    def send(self, payload, flags=0):
        self.sent.append(unpack(payload))

    def recv_multipart(self, flags=0):
        time.sleep(self.read_seconds)
        request = self.sent[-1]
        return [pack({**self.reply, 'req_id': request['req_id']})]

    def close(self, linger=0):
        self.closed = True


def _client_with(socket):
    client = RPCClient(zmq_server_port=1)
    client.socket = socket
    client.is_setup = True
    # Any reset would build a real DEALER; record it instead of connecting.
    resets = []
    client._reset_socket = lambda: resets.append(True)
    return client, resets


def test_reply_received_after_deadline_crossed_is_accepted():
    socket = _SlowReadSocket(read_seconds=0.15, reply={'result': {'orderId': 42}})
    client, resets = _client_with(socket)
    try:
        result = client.rpc(timeout=0.05).place_order()
    finally:
        client.ctx.term()
    assert result == {'orderId': 42}
    assert resets == [], 'a delivered reply is not a failure; the socket is kept'


def test_reply_received_after_deadline_still_reconstructs_server_error():
    socket = _SlowReadSocket(read_seconds=0.15, reply={
        'error': True, 'exc_type': 'ValueError', 'exc_args': ['refused: quantity 0']})
    client, _ = _client_with(socket)
    try:
        with pytest.raises(ValueError, match='refused: quantity 0'):
            client.rpc(timeout=0.05).place_order()
    finally:
        client.ctx.term()


def test_no_reply_within_budget_is_still_a_timeout():
    class _Silent(_SlowReadSocket):
        def poll(self, timeout_ms, flags):
            if flags == zmq.POLLOUT:
                return True
            time.sleep(min(timeout_ms, 200) / 1000.0)
            return False

    client, resets = _client_with(_Silent(read_seconds=0, reply={}))
    try:
        with pytest.raises(TimeoutError, match='outcome UNKNOWN'):
            client.rpc(timeout=0.05).place_order()
    finally:
        client.ctx.term()
    assert resets == [True], 'a genuine timeout still drops the socket identity'
