"""Tests for IB upstream connectivity detection (§1).

Tests the _on_ib_error state machine in Trader and the enriched status() method.
These don't require a live IB connection — we test the pure state transition logic.
"""

import pytest
from trader.listeners.ibreactive import IBAIORxError


class TestOnIbError:
    """Test the _on_ib_error method's upstream state tracking.

    We construct a minimal Trader-like object with just the fields
    _on_ib_error reads/writes, avoiding the full __init__ chain.
    """

    def _make_trader(self):
        """Create a minimal object with the fields _on_ib_error needs."""
        from trader.trading.trading_runtime import Trader
        trader = object.__new__(Trader)
        trader._ib_upstream_connected = True
        trader._ib_upstream_error = ''
        return trader

    def _make_error(self, code, msg='test'):
        return IBAIORxError(reqId=-1, errorCode=code, errorString=msg, contract=None)

    # --- Connection loss codes ---

    def test_1100_sets_disconnected(self):
        trader = self._make_trader()
        trader._on_ib_error(self._make_error(1100, 'Connectivity between IB and TWS has been lost'))
        assert trader._ib_upstream_connected is False
        assert 'Connectivity' in trader._ib_upstream_error

    def test_2103_sets_disconnected(self):
        trader = self._make_trader()
        trader._on_ib_error(self._make_error(2103, 'Market data farm connection is broken:usfarm'))
        assert trader._ib_upstream_connected is False

    def test_2105_sets_disconnected(self):
        trader = self._make_trader()
        trader._on_ib_error(self._make_error(2105, 'HMDS data farm connection is broken:ushmds'))
        assert trader._ib_upstream_connected is False

    def test_2157_sets_disconnected(self):
        trader = self._make_trader()
        trader._on_ib_error(self._make_error(2157, 'Sec-def data farm connection is broken'))
        assert trader._ib_upstream_connected is False

    # --- Connection restored codes ---

    def test_1102_restores_connection(self):
        trader = self._make_trader()
        trader._ib_upstream_connected = False
        trader._ib_upstream_error = 'was disconnected'
        trader._on_ib_error(self._make_error(1102, 'Connectivity restored'))
        assert trader._ib_upstream_connected is True
        assert trader._ib_upstream_error == ''

    def test_2104_restores_connection(self):
        trader = self._make_trader()
        trader._ib_upstream_connected = False
        trader._on_ib_error(self._make_error(2104, 'Market data farm connection is OK:usfarm'))
        assert trader._ib_upstream_connected is True

    def test_2106_restores_connection(self):
        trader = self._make_trader()
        trader._ib_upstream_connected = False
        trader._on_ib_error(self._make_error(2106, 'HMDS data farm connection is OK:ushmds'))
        assert trader._ib_upstream_connected is True

    def test_2158_restores_connection(self):
        trader = self._make_trader()
        trader._ib_upstream_connected = False
        trader._on_ib_error(self._make_error(2158, 'Sec-def data farm connection is OK'))
        assert trader._ib_upstream_connected is True

    # --- Unrelated codes ---

    def test_unrelated_code_does_not_change_state(self):
        trader = self._make_trader()
        trader._on_ib_error(self._make_error(200, 'The contract description is ambiguous'))
        assert trader._ib_upstream_connected is True
        assert trader._ib_upstream_error == ''

    def test_unrelated_code_does_not_restore(self):
        trader = self._make_trader()
        trader._ib_upstream_connected = False
        trader._ib_upstream_error = 'lost'
        trader._on_ib_error(self._make_error(200, 'some other error'))
        assert trader._ib_upstream_connected is False
        assert trader._ib_upstream_error == 'lost'

    # --- Sequences ---

    def test_loss_then_restore_cycle(self):
        trader = self._make_trader()
        assert trader._ib_upstream_connected is True

        trader._on_ib_error(self._make_error(1100, 'lost'))
        assert trader._ib_upstream_connected is False

        trader._on_ib_error(self._make_error(1102, 'restored'))
        assert trader._ib_upstream_connected is True
        assert trader._ib_upstream_error == ''

    def test_multiple_loss_codes_all_set_disconnected(self):
        trader = self._make_trader()
        for code in (1100, 2103, 2105, 2157):
            trader._ib_upstream_connected = True
            trader._on_ib_error(self._make_error(code, f'error {code}'))
            assert trader._ib_upstream_connected is False, f'code {code} did not set disconnected'

    def test_multiple_restore_codes_all_set_connected(self):
        trader = self._make_trader()
        for code in (1102, 2104, 2106, 2158):
            trader._ib_upstream_connected = False
            trader._ib_upstream_error = 'test'
            trader._on_ib_error(self._make_error(code, f'ok {code}'))
            assert trader._ib_upstream_connected is True, f'code {code} did not restore'
            assert trader._ib_upstream_error == '', f'code {code} did not clear error'

    def test_restore_when_already_connected_is_noop(self):
        trader = self._make_trader()
        trader._on_ib_error(self._make_error(2104, 'farm ok'))
        assert trader._ib_upstream_connected is True
        assert trader._ib_upstream_error == ''
