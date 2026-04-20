"""Tests for IB upstream connectivity detection.

Tests the ``_on_ib_error`` state machine in Trader. No live IB required —
the logic is pure state transitions driven by error codes.

**Important correction from earlier versions of this file**: 2103 / 2105 /
2157 are per-data-farm informational warnings, NOT full-disconnect
signals. IB Gateway has multiple redundant data farms (``usfarm``,
``euhmds``, ``cashfarm``, ``usfuture``, …) and a hiccup on one of them
does not disable trading. Only 1100 (and its restore 1102) should flip
the hard ``_ib_upstream_connected`` flag. The previous test file asserted
the buggy behavior — those cases are now inverted to lock in the correct
semantics.
"""

import pytest
from trader.listeners.ibreactive import IBAIORxError


class TestOnIbError:
    """State machine in ``Trader._on_ib_error``."""

    def _make_trader(self):
        from trader.trading.trading_runtime import Trader
        trader = object.__new__(Trader)
        trader._ib_upstream_connected = True
        trader._ib_upstream_error = ''
        return trader

    def _make_error(self, code, msg='test'):
        return IBAIORxError(reqId=-1, errorCode=code, errorString=msg, contract=None)

    # --- 1100 / 1102: the real "you can't trade" signals ---

    def test_1100_sets_disconnected(self):
        """1100 is the authoritative full-disconnect — trading disabled."""
        trader = self._make_trader()
        trader._on_ib_error(self._make_error(1100, 'Connectivity between IB and TWS has been lost'))
        assert trader._ib_upstream_connected is False
        assert 'Connectivity' in trader._ib_upstream_error

    def test_1102_restores_connection(self):
        """1102 is the restore pair for 1100."""
        trader = self._make_trader()
        trader._ib_upstream_connected = False
        trader._ib_upstream_error = 'was disconnected'
        trader._on_ib_error(self._make_error(1102, 'Connectivity restored'))
        assert trader._ib_upstream_connected is True
        assert trader._ib_upstream_error == ''

    def test_loss_then_restore_cycle(self):
        trader = self._make_trader()
        assert trader._ib_upstream_connected is True
        trader._on_ib_error(self._make_error(1100, 'lost'))
        assert trader._ib_upstream_connected is False
        trader._on_ib_error(self._make_error(1102, 'restored'))
        assert trader._ib_upstream_connected is True
        assert trader._ib_upstream_error == ''

    # --- 2103 / 2105 / 2157: per-farm informational warnings ---

    def test_2103_is_informational_only(self):
        """Per-farm warnings must NOT trip the hard-disconnect flag. The
        earlier (buggy) behavior produced false-positive "Gateway broken"
        warnings in the CLI while trading was fine — because IB sends 2103
        any time ANY of many data farms briefly hiccups, even though the
        rest stay up and trading continues."""
        trader = self._make_trader()
        trader._on_ib_error(self._make_error(2103, 'Market data farm connection is broken:usfarm'))
        assert trader._ib_upstream_connected is True, (
            '2103 is per-farm informational; other farms OK, trading OK'
        )
        assert trader._ib_upstream_error == ''
        # But we do track it separately so callers can see farm-level state
        assert 2103 in trader._ib_farms_down

    def test_2105_is_informational_only(self):
        trader = self._make_trader()
        trader._on_ib_error(self._make_error(2105, 'HMDS data farm connection is broken:ushmds'))
        assert trader._ib_upstream_connected is True
        assert 2105 in trader._ib_farms_down

    def test_2157_is_informational_only(self):
        trader = self._make_trader()
        trader._on_ib_error(self._make_error(2157, 'Sec-def data farm connection is broken'))
        assert trader._ib_upstream_connected is True
        assert 2157 in trader._ib_farms_down

    # --- 2104 / 2106 / 2158: per-farm restore pairs ---

    def test_2104_clears_farm_from_tracking(self):
        """Pairs with 2103 — clears the farm from _ib_farms_down. The
        hard-flag is left alone (it was never tripped by 2103 in the first
        place)."""
        trader = self._make_trader()
        trader._on_ib_error(self._make_error(2103, 'usfarm broken'))
        assert 2103 in trader._ib_farms_down
        trader._on_ib_error(self._make_error(2104, 'usfarm restored'))
        assert 2103 not in trader._ib_farms_down
        assert trader._ib_upstream_connected is True  # unchanged throughout

    def test_2106_clears_farm_from_tracking(self):
        trader = self._make_trader()
        trader._on_ib_error(self._make_error(2105, 'ushmds broken'))
        trader._on_ib_error(self._make_error(2106, 'ushmds restored'))
        assert 2105 not in trader._ib_farms_down

    def test_2158_clears_farm_from_tracking(self):
        trader = self._make_trader()
        trader._on_ib_error(self._make_error(2157, 'sec-def broken'))
        trader._on_ib_error(self._make_error(2158, 'sec-def restored'))
        assert 2157 not in trader._ib_farms_down

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

    # --- Interaction: 1100 wins over per-farm state ---

    def test_1100_during_farm_hiccup_wins(self):
        """If 2103 fires then 1100 fires, 1100 is authoritative — trading
        really is disabled, irrespective of the farm-level chatter."""
        trader = self._make_trader()
        trader._on_ib_error(self._make_error(2103, 'usfarm'))
        assert trader._ib_upstream_connected is True  # 2103 alone doesn't trip
        trader._on_ib_error(self._make_error(1100, 'full disconnect'))
        assert trader._ib_upstream_connected is False
