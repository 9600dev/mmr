"""Is IB answering? (found live 2026-11-16)

With the network between MMR and the gateway black-holed for 140 seconds the
socket stayed "connected", no error code arrived - there was nobody to send
one - and the trader's pulse and `mmr status` said ib_connected=True
ib_upstream=True throughout. Only `mmr verify`'s live round trip knew. The
pulse now starts one such round trip each time and reports the last answer.
"""
from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace

import pytest

from trader.trading.trading_runtime import Trader, _named


def _trader(connected=True):
    trader = object.__new__(Trader)
    ib = SimpleNamespace(connected=connected)
    ib.isConnected = lambda: ib.connected
    trader.client = SimpleNamespace(ib=ib)
    trader.data = object()
    trader._ib_upstream_connected = True
    trader._ib_upstream_error = ''
    return trader


def test_before_any_live_request_the_answer_is_unknown():
    assert _trader().link_state() == {'answering': None, 'silent_s': 0.0, 'error': ''}


def test_an_answered_request_says_so():
    trader = _trader()
    trader._record_link_probe(True, now=100.0)
    assert trader.link_state()['answering'] is True


def test_silence_is_measured_from_the_first_unanswered_request(monkeypatch):
    trader = _trader()
    trader._record_link_probe(True, now=100.0)
    trader._record_link_probe(False, 'TimeoutError: ', now=130.0)
    trader._record_link_probe(False, 'TimeoutError: ', now=160.0)
    trader._record_link_probe(False, 'TimeoutError: ', now=190.0)
    monkeypatch.setattr('trader.trading.trading_runtime.time.monotonic', lambda: 200.0)
    state = trader.link_state()
    assert state['answering'] is False and state['error'] == 'TimeoutError: '
    assert state['silent_s'] == pytest.approx(70.0)                      # since 130, not since the last probe


def test_one_missed_request_is_not_an_alarm_and_the_second_is_one_error(caplog):
    trader = _trader()
    caplog.set_level(logging.INFO)
    trader._record_link_probe(True, now=100.0)
    trader._record_link_probe(False, 'TimeoutError: ', now=130.0)
    assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []
    trader._record_link_probe(False, 'TimeoutError: ', now=160.0)
    errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1
    assert errors[0].startswith('IB has not answered a live request for 30s (TimeoutError: ) '
                                'while the socket reports connected=True')
    assert 'No reconnect is attempted automatically' in errors[0]
    for at in (190.0, 220.0, 250.0):
        trader._record_link_probe(False, 'TimeoutError: ', now=at)
    assert len([r for r in caplog.records if r.levelno == logging.ERROR]) == 1        # said once


def test_the_error_names_a_missing_reason(caplog):
    trader = _trader()
    caplog.set_level(logging.INFO)
    trader._record_link_probe(False, '', now=10.0)
    trader._record_link_probe(False, '', now=40.0)
    assert 'for 30s (no reply)' in caplog.text


def test_recovery_is_announced_once_with_how_long_it_was_silent(caplog):
    trader = _trader()
    caplog.set_level(logging.INFO)
    trader._record_link_probe(True, now=100.0)
    for at in (130.0, 160.0, 190.0):
        trader._record_link_probe(False, 'TimeoutError: ', now=at)
    trader._record_link_probe(True, now=270.0)
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings == ['IB is answering again after 140s of silence']
    assert trader.link_state() == {'answering': True, 'silent_s': 0.0, 'error': ''}
    trader._record_link_probe(True, now=300.0)
    assert len([r for r in caplog.records if r.levelno == logging.WARNING]) == 1


def test_a_single_miss_that_recovers_is_not_announced(caplog):
    trader = _trader()
    caplog.set_level(logging.INFO)
    trader._record_link_probe(True, now=100.0)
    trader._record_link_probe(False, 'TimeoutError: ', now=130.0)
    trader._record_link_probe(True, now=160.0)
    assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []


def test_the_threshold_is_two():
    assert Trader.LINK_SILENT_PROBES_BEFORE_ERROR == 2


def test_status_carries_the_answer(monkeypatch):
    trader = _trader()
    assert trader.status()['ib_answering'] is None
    trader._status_cache_ts = 0.0
    trader._record_link_probe(True, now=100.0)
    status = trader.status()
    assert status['ib_answering'] is True and 'ib_silent_s' not in status
    trader._status_cache_ts = 0.0
    trader._record_link_probe(False, 'TimeoutError: ', now=130.0)
    monkeypatch.setattr('trader.trading.trading_runtime.time.monotonic', lambda: 172.34)
    status = trader.status()
    assert status['ib_connected'] is True and status['ib_upstream_connected'] is True     # the flags that cannot tell
    assert status['ib_answering'] is False
    assert status['ib_silent_s'] == 42.3 and status['ib_answering_error'] == 'TimeoutError: '


def test_the_pulse_reports_the_last_answer(caplog):
    trader = _trader()
    caplog.set_level(logging.INFO)
    trader._log_pulse()
    assert 'ib_answering=None' in caplog.text
    trader._record_link_probe(False, 'TimeoutError: ', now=1.0)
    trader._log_pulse()
    assert 'pulse ib_connected=True ib_upstream=True' in caplog.text and 'ib_answering=False' in caplog.text


@pytest.mark.asyncio
async def test_each_pulse_starts_one_live_request_and_keeps_its_answer(caplog):
    trader = _trader()
    calls = []

    async def ping_ib():
        calls.append(1)
        return {'ok': len(calls) != 2, 'error': 'TimeoutError: '}

    trader.ping_ib = ping_ib
    caplog.set_level(logging.INFO)
    trader._log_pulse()
    await trader._link_probe_task
    assert trader.link_state()['answering'] is True
    trader._log_pulse()
    await trader._link_probe_task
    assert trader.link_state()['answering'] is False and trader.link_state()['error'] == 'TimeoutError: '
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_a_request_still_out_is_not_doubled():
    trader = _trader()
    started, release = [], asyncio.Event()

    async def ping_ib():
        started.append(1)
        await release.wait()
        return {'ok': True}

    trader.ping_ib = ping_ib
    trader._probe_link()
    await asyncio.sleep(0)
    trader._probe_link()
    trader._probe_link()
    await asyncio.sleep(0)
    assert len(started) == 1
    release.set()
    await trader._link_probe_task
    assert trader.link_state()['answering'] is True


def test_a_pulse_without_a_running_loop_still_logs(caplog):
    """The pulse must never raise (its absence is the wedge signal)."""
    trader = _trader()
    caplog.set_level(logging.INFO)
    trader._log_pulse()
    assert 'pulse ib_connected=True' in caplog.text


# ---------------------------------------------------------------- what a failure is called

def test_a_timeout_has_no_text_so_it_is_named():
    assert _named(asyncio.TimeoutError()) == 'TimeoutError (no reply within the request deadline)'
    assert _named(TimeoutError()) == 'TimeoutError (no reply within the request deadline)'


def test_an_exception_with_text_keeps_it():
    assert _named(ConnectionError('Not connected')) == 'ConnectionError: Not connected'
    assert _named(ValueError('  spaced  ')) == 'ValueError: spaced'
