"""IB errorEvent messages: one MMR log line each, at a level that fits the code.

Regression for the 2026-11-05 smoke test:
* every IB error appeared four times — ib_async's raw "Error <code>, reqId"
  line plus MMR's "ibrx reqId" line, which logging.yaml then wrote TWICE into
  errors_<ts>.log (logger-level AND root-level error_file_handler; covered in
  test_log_retention_and_routing.py). Reconnects additionally restored the
  name-mangled ``__handle_error`` as an UNBOUND function, which raised (and was
  logged by eventkit) on every subsequent IB error.
* benign codes (farm status, 2109, 10349, scanner-cancel 162/365) at ERROR.
"""
import asyncio
import logging

import pytest
from ib_async import IB

from trader.listeners import ib_informational
from trader.listeners.ib_informational import ib_log_level
from trader.listeners.ibreactive import IBAIORx, _restore_event_handlers, _snapshot_event_handlers


# --------------------------------------------------------------------------
# The policy table
# --------------------------------------------------------------------------

@pytest.mark.parametrize('code', [2104, 2106, 2107, 2158, 202])
def test_debug_codes(code):
    assert ib_log_level(code, 'x') == logging.DEBUG


@pytest.mark.parametrize('code', [2103, 2105, 2108, 2119, 2157, 2109, 10349])
def test_info_codes(code):
    assert ib_log_level(code, 'x') == logging.INFO


@pytest.mark.parametrize('code', [10089, 310])
def test_actionable_warning_codes(code):
    assert ib_log_level(code, 'requires additional subscription') == logging.WARNING


def test_scanner_cancel_ack_is_info_but_other_162_stays_error():
    assert ib_log_level(162, 'Historical Market Data Service error message:'
                             'API scanner subscription cancelled: 1054') == logging.INFO
    assert ib_log_level(162, 'Historical Market Data Service error message:Error: '
                             'Market Scanner is not configured for one of the chosen '
                             'locations.') == logging.ERROR
    assert ib_log_level(162, 'HMDS query returned no data') == logging.ERROR
    assert ib_log_level(162, '') == logging.ERROR


def test_365_after_cancel_is_info():
    assert ib_log_level(365, 'No scanner subscription found for ticker id:1053') == logging.INFO
    assert ib_log_level(365, 'something else') == logging.ERROR


def test_rest_of_ib_warning_range_is_warning():
    assert ib_log_level(2150, 'Invalid position trade derived value') == logging.WARNING
    assert ib_log_level(2110, 'Connectivity between TWS and server is broken') == logging.WARNING


@pytest.mark.parametrize('code', [200, 201, 354, 1100, 10197, 504, 321])
def test_real_failures_stay_error(code):
    assert ib_log_level(code, 'anything') == logging.ERROR


def test_10349_policy_shares_the_informational_order_table():
    assert ib_informational.INFORMATIONAL_ORDER_CODES <= ib_informational.INFO_CODES


# --------------------------------------------------------------------------
# IBAIORx handler: one line, mapped level, forwarding unchanged
# --------------------------------------------------------------------------

@pytest.fixture
def rx():
    return IBAIORx('127.0.0.1', 1, 1)


def _handle(rx, code, msg, req_id=7):
    seen = []
    sub = rx.error_subject.subscribe(on_next=seen.append)
    asyncio.run(rx._IBAIORx__handle_error(req_id, code, msg, None))
    sub.dispose()
    return seen


def _ibrx_records(caplog):
    return [r for r in caplog.records if r.name == 'ibreactivex' and 'errorCode' in r.getMessage()]


@pytest.mark.parametrize('code,msg,level', [
    (10089, 'Requested market data requires additional subscription for API.', logging.WARNING),
    (2109, "Order Event Warning:Attribute 'Outside Regular Trading Hours' is ignored", logging.INFO),
    (10349, 'Order TIF was set to DAY based on order preset.', logging.INFO),
    (162, 'Historical Market Data Service error message:API scanner subscription cancelled: 9', logging.INFO),
    (2103, 'Market data farm connection is broken:usfarm', logging.INFO),
    (200, 'No security definition has been found for the request', logging.ERROR),
])
def test_handler_logs_once_at_mapped_level_and_forwards(rx, caplog, code, msg, level):
    caplog.set_level(logging.DEBUG, logger='ibreactivex')
    seen = _handle(rx, code, msg)
    records = _ibrx_records(caplog)
    assert len(records) == 1, [r.getMessage() for r in records]
    assert records[0].levelno == level
    assert len(seen) == 1 and seen[0].errorCode == code


def test_farm_ok_codes_logged_at_debug_and_not_forwarded(rx, caplog):
    caplog.set_level(logging.DEBUG, logger='ibreactivex')
    seen = _handle(rx, 2104, 'Market data farm connection is OK:usfarm', req_id=-1)
    records = _ibrx_records(caplog)
    assert [r.levelno for r in records] == [logging.DEBUG]
    assert 'farm status' in records[0].getMessage()
    assert seen == []


def test_farm_status_keeps_monitor_phrase(rx, caplog):
    caplog.set_level(logging.DEBUG, logger='ibreactivex')
    _handle(rx, 2119, 'Market data farm is connecting:usfarm', req_id=-1)
    (record,) = _ibrx_records(caplog)
    assert record.getMessage().startswith('ibrx farm status:')  # scripts/monitor_health.sh


# --------------------------------------------------------------------------
# Reconnect: the private handler must come back BOUND, exactly once
# --------------------------------------------------------------------------

def _errorevent_handler_slots(ib):
    return [s for s in ib.errorEvent._slots.slots if s.func.__name__ == '__handle_error']


def test_snapshot_restore_rebinds_name_mangled_handler(rx):
    handler = rx._IBAIORx__handle_error
    rx.ib.errorEvent += handler
    for _ in range(3):  # three reconnects
        snapshot = _snapshot_event_handlers(rx.ib)
        rx.ib = IB()
        _restore_event_handlers(rx.ib, snapshot)
        assert rx._IBAIORx__handle_error in rx.ib.errorEvent
        if rx._IBAIORx__handle_error not in rx.ib.errorEvent:  # what connect() does
            rx.ib.errorEvent += rx._IBAIORx__handle_error
        slots = _errorevent_handler_slots(rx.ib)
        assert len(slots) == 1
        assert slots[0].weakref is not None and slots[0].weakref() is rx  # bound, not unbound


def test_error_after_reconnect_is_handled_once_without_eventkit_exception(rx, caplog):
    rx.ib.errorEvent += rx._IBAIORx__handle_error
    snapshot = _snapshot_event_handlers(rx.ib)
    rx.ib = IB()
    _restore_event_handlers(rx.ib, snapshot)
    if rx._IBAIORx__handle_error not in rx.ib.errorEvent:
        rx.ib.errorEvent += rx._IBAIORx__handle_error

    seen = []
    rx.error_subject.subscribe(on_next=seen.append)

    async def scenario():
        rx.ib.errorEvent.emit(5, 200, 'No security definition has been found', None)
        await asyncio.sleep(0.01)

    caplog.set_level(logging.DEBUG)
    asyncio.run(scenario())
    assert len(seen) == 1
    assert len(_ibrx_records(caplog)) == 1
    assert not [r for r in caplog.records if r.name == 'eventkit.event']


# --------------------------------------------------------------------------
# ib_async's raw line is muted only on MMR-handled instances
# --------------------------------------------------------------------------

class _ListHandler(logging.Handler):
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.records = []

    def emit(self, record):
        self.records.append(record)


@pytest.fixture
def wrapper_log():
    """ib_async.wrapper does not propagate (logging.yaml), so caplog can't see
    it — attach a handler directly."""
    lg = logging.getLogger('ib_async.wrapper')
    handler = _ListHandler()
    old_level = lg.level
    lg.addHandler(handler)
    lg.setLevel(logging.DEBUG)
    yield handler
    lg.removeHandler(handler)
    lg.setLevel(old_level)


def _wrapper_records(handler):
    return handler.records


def test_raw_wrapper_error_line_muted_on_mmr_instance(rx, wrapper_log):
    rx.ib.wrapper.error(7, 200, 'No security definition has been found', '')
    assert not [r for r in _wrapper_records(wrapper_log) if 'reqId 7' in r.getMessage()]


def test_raw_wrapper_error_line_kept_on_plain_instance(wrapper_log):
    ib = IB()
    ib.wrapper.error(7, 200, 'No security definition has been found', '')
    assert [r for r in _wrapper_records(wrapper_log) if r.getMessage().startswith('Error 200, reqId 7')]


def test_quiet_logger_passes_other_wrapper_records(wrapper_log):
    ib = IB()
    ib_informational.quiet_handled_errors(ib)
    ib_informational.quiet_handled_errors(ib)  # idempotent
    ib.wrapper._logger.error('Error handling something unrelated')
    ib.wrapper._logger.warning('IBKR API validation warning: Trade(...)')
    ib.wrapper._logger.error('Error 200, reqId 3: dropped')
    messages = [r.getMessage() for r in _wrapper_records(wrapper_log)]
    assert messages == ['Error handling something unrelated', 'IBKR API validation warning: Trade(...)']


def test_quiet_logger_still_delegates_attributes():
    ib = IB()
    ib_informational.quiet_handled_errors(ib)
    assert ib.wrapper._logger.name == 'ib_async.wrapper'
    assert isinstance(ib.wrapper._logger.isEnabledFor(logging.ERROR), bool)


def test_informational_install_still_works_with_quiet_logger(rx):
    """10349 keeps a live order live (ib_informational.install) on an IBAIORx IB."""
    from ib_async import Stock
    from ib_async.order import MarketOrder, OrderStatus, Trade
    w = rx.ib.wrapper
    trade = Trade(Stock('F', 'SMART', 'USD'), MarketOrder('SELL', 1, orderId=42),
                  OrderStatus(orderId=42, status='PreSubmitted'))
    w.trades[(w.clientId, 42)] = trade
    w.error(42, 10349, 'Order TIF was set to DAY based on order preset.', '')
    assert trade.orderStatus.status == 'PreSubmitted'


# --------------------------------------------------------------------------
# IBHistoryWorker uses the same table
# --------------------------------------------------------------------------

def test_history_worker_uses_central_levels(caplog):
    from trader.listeners.ib_history_worker import IBHistoryWorker
    worker = IBHistoryWorker('127.0.0.1', 1, 2)
    caplog.set_level(logging.DEBUG, logger='ib_history')
    worker._IBHistoryWorker__handle_error(-1, 2103, 'Market data farm connection is broken:afarm', None)
    worker._IBHistoryWorker__handle_error(9, 200, 'No security definition', None)
    levels = [r.levelno for r in caplog.records if r.name == 'ib_history']
    assert levels == [logging.INFO, logging.ERROR]
    assert isinstance(worker.ib_client.wrapper._logger, ib_informational._HandledErrorLogger)
