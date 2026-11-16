"""What status an order response shows (found live 2026-11-16).

Every MARKET / STOP order carries the outside-regular-hours attribute; IB
answers with warning 2109 and ib_async sets the trade's local status to
'ValidationError' for the moment the CLI reads it. `buy`, `sell` and `close`
therefore answered `"status": "ValidationError"` beside `"success": true` for
orders IB had accepted and filled.
"""
from __future__ import annotations

import datetime as dt
import json
from types import SimpleNamespace

import pytest

import trader.mmr_cli as cli
from trader.common.reactivex import SuccessFail
from trader.listeners.ib_informational import ORDER_WARNING_STATUS, order_status_and_warnings

NOW = dt.datetime(2026, 11, 16, 20, 15, tzinfo=dt.timezone.utc)
OUTSIDE_RTH = ("Warning 2109, reqId 2467: Order Event Warning:Attribute 'Outside Regular Trading Hours' is "
               "ignored based on the order type and destination. PlaceOrder is now being processed.")
TIF_PRESET = 'Warning 10349, reqId 2467: Order TIF was set to DAY based on order preset.'
MISSING_EXCHANGE = 'Warning 321, reqId 1073: Error validating request.-\'bN\' : cause - Missing order exchange'


def _entry(status, message='', code=0):
    return SimpleNamespace(time=NOW, status=status, message=message, errorCode=code)


def _trade(status, *log, order_id=2467):
    return SimpleNamespace(order=SimpleNamespace(orderId=order_id), orderStatus=SimpleNamespace(status=status),
                           log=list(log))


def test_the_marker_is_ib_asyncs():
    from ib_async.order import OrderStatus
    assert ORDER_WARNING_STATUS == OrderStatus.ValidationError == 'ValidationError'


def test_an_order_accepted_with_an_informational_notice_shows_its_broker_status():
    trade = _trade('ValidationError', _entry('PendingSubmit'), _entry('PreSubmitted'),
                   _entry('ValidationError', OUTSIDE_RTH, 2109))
    assert order_status_and_warnings(trade) == ('PreSubmitted', [OUTSIDE_RTH])


def test_with_no_broker_status_yet_it_is_pending_submission():
    trade = _trade('ValidationError', _entry('ValidationError', OUTSIDE_RTH, 2109))
    assert order_status_and_warnings(trade) == ('PendingSubmit', [OUTSIDE_RTH])


def test_several_informational_notices_are_all_carried():
    trade = _trade('ValidationError', _entry('PendingSubmit'), _entry('ValidationError', TIF_PRESET, 10349),
                   _entry('ValidationError', OUTSIDE_RTH, 2109))
    assert order_status_and_warnings(trade) == ('PendingSubmit', [TIF_PRESET, OUTSIDE_RTH])


def test_a_real_validation_failure_keeps_the_marker():
    """2026-11-05: a TRAIL with a blank exchange came back 321 and the order
    was dead; ib_async files that as a "warning" too. It must not read as
    PreSubmitted."""
    trade = _trade('ValidationError', _entry('PendingSubmit'), _entry('ValidationError', MISSING_EXCHANGE, 321))
    assert order_status_and_warnings(trade) == ('ValidationError', [MISSING_EXCHANGE])


def test_one_real_failure_among_notices_keeps_the_marker():
    trade = _trade('ValidationError', _entry('PendingSubmit'), _entry('ValidationError', OUTSIDE_RTH, 2109),
                   _entry('ValidationError', MISSING_EXCHANGE, 321))
    assert order_status_and_warnings(trade)[0] == 'ValidationError'


def test_a_marker_with_nothing_behind_it_is_not_explained_away():
    assert order_status_and_warnings(_trade('ValidationError')) == ('ValidationError', [])
    assert order_status_and_warnings(_trade('ValidationError', _entry('PendingSubmit'))) == ('ValidationError', [])


@pytest.mark.parametrize('status', ['Submitted', 'PreSubmitted', 'Filled', 'Cancelled', 'Inactive', None])
def test_any_other_status_is_reported_as_it_is(status):
    assert order_status_and_warnings(_trade(status, _entry('PendingSubmit')))[0] == status


def test_an_earlier_notice_is_still_reported_once_the_broker_has_moved_on():
    trade = _trade('Filled', _entry('PendingSubmit'), _entry('ValidationError', OUTSIDE_RTH, 2109), _entry('Filled'))
    assert order_status_and_warnings(trade) == ('Filled', [OUTSIDE_RTH])


def test_an_object_that_is_not_a_trade_has_no_status():
    assert order_status_and_warnings(None) == (None, [])
    assert order_status_and_warnings(SimpleNamespace()) == (None, [])
    assert order_status_and_warnings(SimpleNamespace(orderStatus=SimpleNamespace(status='Submitted'), log=None)) == (
        'Submitted', [])


# ---------------------------------------------------------------- the CLI

def _accepted_market_sell():
    return _trade('ValidationError', _entry('PendingSubmit'), _entry('ValidationError', OUTSIDE_RTH, 2109))


def test_sell_json_shows_the_broker_status_and_the_notice(monkeypatch, capsys):
    monkeypatch.setattr(cli, '_json_mode', True)
    cli._print_trade_result(SuccessFail.success(obj=_accepted_market_sell()), 'SELL', 'PFE')
    out = json.loads(capsys.readouterr().out)
    assert out['success'] is True and out['orderId'] == 2467
    assert out['status'] == 'PendingSubmit'
    assert out['ib_messages'] == [OUTSIDE_RTH]
    assert out['replaced'] == []


def test_sell_json_says_which_working_exits_it_cancelled(monkeypatch, capsys):
    """`sell PFE --limit 35` beside a protective stop cancelled the stop and
    said nothing: the server had the note, the printer dropped it."""
    monkeypatch.setattr(cli, '_json_mode', True)
    result = SuccessFail.success(obj=_trade('Submitted', order_id=2464))
    result.warnings = ['cancelled working STP SELL 2 PFE stop 26 (order 2457) — reducing orders may not exceed the position']
    cli._print_trade_result(result, 'SELL', 'PFE')
    out = json.loads(capsys.readouterr().out)
    assert out['replaced'] == result.warnings and out['status'] == 'Submitted'


def test_a_failed_order_has_no_status_or_replaced_keys(monkeypatch, capsys):
    monkeypatch.setattr(cli, '_json_mode', True)
    cli._print_trade_result(SuccessFail.fail(error='Direct order rejected'), 'BUY', 'PFE')
    out = json.loads(capsys.readouterr().out)
    assert out['success'] is False and 'status' not in out and 'replaced' not in out


def test_the_text_view_lists_what_was_replaced(monkeypatch):
    printed = []
    monkeypatch.setattr(cli, '_json_mode', False)
    monkeypatch.setattr(cli.console, 'print', lambda *args, **kwargs: printed.append(' '.join(str(a) for a in args)))
    monkeypatch.setattr(cli, 'print_dict', lambda data, **kwargs: printed.append(str(data)))
    result = SuccessFail.success(obj=_accepted_market_sell())
    result.warnings = ['cancelled working STP SELL 2 PFE stop 26 (order 2457)']
    cli._print_trade_result(result, 'SELL', 'PFE')
    text = '\n'.join(printed)
    assert 'replaced: cancelled working STP SELL 2 PFE stop 26 (order 2457)' in text
    assert "'status': 'PendingSubmit'" in text and 'ValidationError' not in text


def test_close_summary_shows_the_broker_status(monkeypatch):
    summary = cli._trade_summary(SuccessFail.success(obj=_accepted_market_sell()))
    assert summary['broker_status'] == 'PendingSubmit'
    assert summary['ib_messages'] == [OUTSIDE_RTH]
    assert summary['order_id'] == 2467 and summary['replaced'] == []


def test_close_summary_keeps_a_real_failure_visible():
    dead = _trade('ValidationError', _entry('ValidationError', MISSING_EXCHANGE, 321), order_id=1073)
    summary = cli._trade_summary(SuccessFail.success(obj=dead))
    assert summary['broker_status'] == 'ValidationError' and summary['ib_messages'] == [MISSING_EXCHANGE]
