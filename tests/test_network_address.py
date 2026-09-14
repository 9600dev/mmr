"""Address reporting must not prevent a connected trading service starting."""
import logging

from trader.common.helpers import get_network_ip


def test_network_address_permission_failure_closes_probe_and_uses_loopback(monkeypatch, caplog):
    class Socket:
        closed = False

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            self.closed = True

        def connect(self, _address):
            raise PermissionError('UDP routing probe denied')

    probe = Socket()
    monkeypatch.setattr('trader.common.helpers.socket.socket', lambda *_args: probe)
    with caplog.at_level(logging.WARNING):
        assert get_network_ip() == '127.0.0.1'
    assert probe.closed
    assert 'loopback' in caplog.text and 'denied' in caplog.text


def test_network_address_probe_returns_interface_and_closes(monkeypatch):
    class Socket:
        closed = False

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            self.closed = True

        def connect(self, _address):
            pass

        def getsockname(self):
            return ('192.0.2.5', 12345)

    probe = Socket()
    monkeypatch.setattr('trader.common.helpers.socket.socket', lambda *_args: probe)
    assert get_network_ip() == '192.0.2.5'
    assert probe.closed
