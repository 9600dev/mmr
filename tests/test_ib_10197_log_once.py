"""10197 (competing live session): one ERROR per outage, per-contract repeats at WARNING."""
import asyncio
import logging

import pytest

from trader.listeners.ibreactive import IBAIORx

MSG = 'No market data during competing live session'


@pytest.fixture
def rx(monkeypatch):
    r = IBAIORx('127.0.0.1', 1, 1)
    monkeypatch.setattr(r, '_ensure_md_retry_task', lambda: None)
    return r


def _levels(rx, caplog, n, start_req=100):
    caplog.clear()
    seen = []
    sub = rx.error_subject.subscribe(on_next=seen.append)
    for i in range(n):
        asyncio.run(rx._IBAIORx__handle_error(start_req + i, 10197, MSG, None))
    sub.dispose()
    recs = [r for r in caplog.records if r.name == 'ibreactivex' and 'errorCode: 10197' in r.getMessage()]
    return [r.levelno for r in recs], seen


def test_first_error_then_warnings(rx, caplog):
    caplog.set_level(logging.DEBUG, logger='ibreactivex')
    levels, seen = _levels(rx, caplog, 5)
    assert levels == [logging.ERROR] + [logging.WARNING] * 4
    assert len(seen) == 5            # every one still reaches error_subject


def test_new_outage_after_ticks_errors_again(rx, caplog):
    caplog.set_level(logging.DEBUG, logger='ibreactivex')
    _levels(rx, caplog, 3)
    rx._note_market_data([])          # ticks flowed: outage over
    levels, _ = _levels(rx, caplog, 2, start_req=200)
    assert levels == [logging.ERROR, logging.WARNING]


def test_new_outage_after_recovered_clear_errors_again(rx, caplog):
    caplog.set_level(logging.DEBUG, logger='ibreactivex')
    _levels(rx, caplog, 2)
    rx._md_lost_at = None             # retry loop logged RECOVERED
    levels, _ = _levels(rx, caplog, 1, start_req=300)
    assert levels == [logging.ERROR]
