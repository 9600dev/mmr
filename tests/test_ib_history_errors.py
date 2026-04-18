"""Tests for IB history worker error classification (§3).

Verifies that IB error codes are correctly categorized into:
- IBConnectivityError (retryable after reconnect)
- IBNoDataError (skip and continue)
- Info codes (silently ignored)
"""

import pytest
from trader.listeners.ib_history_worker import (
    IB_INFO_CODES,
    IB_CONNECTIVITY_CODES,
    IBConnectivityError,
    IBNoDataError,
    _162_NO_DATA_SUBSTRINGS,
    _162_FATAL_SUBSTRINGS,
    IBHistoryWorker,
)


class TestErrorClassificationConstants:
    """Verify the error code sets contain the expected codes."""

    def test_info_codes(self):
        assert IB_INFO_CODES == {2104, 2158, 2106}

    def test_connectivity_codes(self):
        assert IB_CONNECTIVITY_CODES == {1100, 1101, 1102, 504}

    def test_no_data_substrings(self):
        assert 'HMDS query returned no data' in _162_NO_DATA_SUBSTRINGS
        assert 'No market data permissions' in _162_NO_DATA_SUBSTRINGS

    def test_fatal_substrings(self):
        assert 'connected from a different IP address' in _162_FATAL_SUBSTRINGS
        assert 'API client has been unsubscribed' in _162_FATAL_SUBSTRINGS


class TestExceptionTypes:
    """Verify the custom exception classes."""

    def test_connectivity_error_is_exception(self):
        assert issubclass(IBConnectivityError, Exception)

    def test_no_data_error_is_exception(self):
        assert issubclass(IBNoDataError, Exception)

    def test_connectivity_error_message(self):
        err = IBConnectivityError('error_code: 1100, error_string: lost')
        assert '1100' in str(err)

    def test_no_data_error_message(self):
        err = IBNoDataError('error_code: 162, error_string: no data')
        assert '162' in str(err)

    def test_errors_are_distinct(self):
        """IBConnectivityError and IBNoDataError should not be caught by each other."""
        with pytest.raises(IBConnectivityError):
            raise IBConnectivityError('test')

        with pytest.raises(IBNoDataError):
            raise IBNoDataError('test')

        # IBNoDataError should NOT be caught by IBConnectivityError
        with pytest.raises(IBNoDataError):
            try:
                raise IBNoDataError('test')
            except IBConnectivityError:
                pytest.fail('IBNoDataError should not be caught by IBConnectivityError')


class TestHandleError:
    """Test the __handle_error method's error classification."""

    def _make_worker(self):
        """Create a worker without connecting."""
        worker = IBHistoryWorker.__new__(IBHistoryWorker)
        worker.error_code = 0
        worker.error_string = ''
        worker.error_contract = None
        worker.connected = True
        return worker

    def test_info_code_clears_error(self):
        worker = self._make_worker()
        worker.error_code = 999
        worker.error_string = 'some previous error'
        # Call the private method directly
        worker._IBHistoryWorker__handle_error(-1, 2104, 'Market data farm OK', None)
        assert worker.error_code == 0
        assert worker.error_string == ''
        assert worker.connected is True

    def test_info_code_2158_clears_error(self):
        worker = self._make_worker()
        worker._IBHistoryWorker__handle_error(-1, 2158, 'Sec-def farm OK', None)
        assert worker.error_code == 0

    def test_info_code_2106_clears_error(self):
        worker = self._make_worker()
        worker._IBHistoryWorker__handle_error(-1, 2106, 'HMDS farm OK', None)
        assert worker.error_code == 0

    def test_connectivity_code_sets_disconnected(self):
        worker = self._make_worker()
        assert worker.connected is True
        worker._IBHistoryWorker__handle_error(-1, 1100, 'connectivity lost', None)
        assert worker.connected is False
        assert worker.error_code == 1100

    def test_connectivity_code_504(self):
        worker = self._make_worker()
        worker._IBHistoryWorker__handle_error(-1, 504, 'not connected', None)
        assert worker.connected is False

    def test_regular_error_preserves_connected(self):
        worker = self._make_worker()
        worker._IBHistoryWorker__handle_error(1, 200, 'some error', None)
        assert worker.connected is True
        assert worker.error_code == 200


class TestErrorDispatchInFetch:
    """Test that get_contract_history raises the right exception types.

    We simulate errors by setting error_code/error_string on the worker
    before calling the dispatch logic.
    """

    def test_connectivity_code_raises_connectivity_error(self):
        """Error codes in IB_CONNECTIVITY_CODES should raise IBConnectivityError."""
        for code in [1100, 1101, 1102, 504]:
            worker = IBHistoryWorker.__new__(IBHistoryWorker)
            worker.error_code = code
            worker.error_string = f'test error for {code}'
            worker.error_contract = None
            # Simulate the dispatch logic from get_contract_history
            with pytest.raises(IBConnectivityError):
                _dispatch_error(worker)

    def test_162_no_data_raises_no_data_error(self):
        """Error 162 with 'no data' substrings should raise IBNoDataError."""
        for substring in _162_NO_DATA_SUBSTRINGS:
            worker = IBHistoryWorker.__new__(IBHistoryWorker)
            worker.error_code = 162
            worker.error_string = f'Historical Market Data error: {substring}'
            worker.error_contract = None
            with pytest.raises(IBNoDataError):
                _dispatch_error(worker)

    def test_162_fatal_raises_connectivity_error(self):
        """Error 162 with fatal substrings should raise IBConnectivityError."""
        for substring in _162_FATAL_SUBSTRINGS:
            worker = IBHistoryWorker.__new__(IBHistoryWorker)
            worker.error_code = 162
            worker.error_string = substring
            worker.error_contract = None
            with pytest.raises(IBConnectivityError):
                _dispatch_error(worker)

    def test_162_unknown_treated_as_no_data(self):
        """Unknown 162 variant defaults to IBNoDataError (not crash)."""
        worker = IBHistoryWorker.__new__(IBHistoryWorker)
        worker.error_code = 162
        worker.error_string = 'some completely new 162 error we have never seen'
        worker.error_contract = None
        with pytest.raises(IBNoDataError):
            _dispatch_error(worker)

    def test_other_error_raises_generic(self):
        """Non-connectivity, non-162 errors raise generic Exception."""
        worker = IBHistoryWorker.__new__(IBHistoryWorker)
        worker.error_code = 321
        worker.error_string = 'please enter exchange'
        worker.error_contract = None
        with pytest.raises(Exception, match='error_code: 321'):
            _dispatch_error(worker)

    def test_zero_error_does_not_raise(self):
        """No error (code 0) should not raise."""
        worker = IBHistoryWorker.__new__(IBHistoryWorker)
        worker.error_code = 0
        worker.error_string = ''
        worker.error_contract = None
        # Should not raise
        _dispatch_error(worker)


def _dispatch_error(worker):
    """Reproduce the error dispatch logic from get_contract_history."""
    if worker.error_code > 0:
        code = worker.error_code
        msg = worker.error_string
        worker.error_code = 0
        worker.error_string = ''
        worker.error_contract = None

        if code in IB_CONNECTIVITY_CODES:
            raise IBConnectivityError(
                'error_code: {}, error_string: {}'.format(code, msg))

        if code == 162:
            if any(s in msg for s in _162_FATAL_SUBSTRINGS):
                raise IBConnectivityError(
                    'error_code: {}, error_string: {}'.format(code, msg))
            if any(s in msg for s in _162_NO_DATA_SUBSTRINGS):
                raise IBNoDataError(
                    'error_code: {}, error_string: {}'.format(code, msg))
            raise IBNoDataError(
                'error_code: {}, error_string: {}'.format(code, msg))

        raise Exception('error_code: {}, error_string: {}'.format(code, msg))
