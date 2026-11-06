"""asyncio debug mode is opt-in; a cheap lag monitor replaces its watchdog role.

Regression for the 2026-11-05 smoke-test finding: trader_service and
strategy_service ran asyncio debug mode in production because they enabled it
whenever TRADER_NODEBUG was unset (and TRADER_NODEBUG also forced all logging
to CRITICAL, so there was no sane way to turn it off).
"""
import asyncio
import logging
import pathlib
import time

import pytest

from trader.common import loop_health
from trader.common.loop_health import (
    ASYNCIO_DEBUG_ENV, LAG_WARN_ENV, LoopLagMonitor, asyncio_debug_requested,
    configure_event_loop, lag_warn_threshold,
)

REPO = pathlib.Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for key in (ASYNCIO_DEBUG_ENV, LAG_WARN_ENV, 'TRADER_NODEBUG', 'PYTHONASYNCIODEBUG'):
        monkeypatch.delenv(key, raising=False)


@pytest.fixture
def loop():
    lp = asyncio.new_event_loop()
    yield lp
    lp.close()


def test_debug_off_by_default_even_without_trader_nodebug():
    assert asyncio_debug_requested() is False


@pytest.mark.parametrize('value', ['1', 'true', 'YES', 'on'])
def test_debug_env_truthy(monkeypatch, value):
    monkeypatch.setenv(ASYNCIO_DEBUG_ENV, value)
    assert asyncio_debug_requested() is True


@pytest.mark.parametrize('value', ['0', 'false', 'no', 'off', ''])
def test_debug_env_falsy(monkeypatch, value):
    monkeypatch.setenv(ASYNCIO_DEBUG_ENV, value)
    assert asyncio_debug_requested() is False


def test_debug_cli_flag_enables():
    assert asyncio_debug_requested(cli_flag=True) is True


def test_debug_env_garbage_fails_loudly(monkeypatch):
    monkeypatch.setenv(ASYNCIO_DEBUG_ENV, 'maybe')
    with pytest.raises(ValueError, match=ASYNCIO_DEBUG_ENV):
        asyncio_debug_requested()


def test_trader_nodebug_does_not_affect_asyncio_debug(monkeypatch):
    monkeypatch.setenv('TRADER_NODEBUG', '1')
    assert asyncio_debug_requested() is False
    monkeypatch.setenv(ASYNCIO_DEBUG_ENV, '1')
    assert asyncio_debug_requested() is True


def test_lag_threshold_default_and_override(monkeypatch):
    assert lag_warn_threshold() == loop_health.DEFAULT_LAG_WARN_S
    monkeypatch.setenv(LAG_WARN_ENV, '1.25')
    assert lag_warn_threshold() == 1.25


@pytest.mark.parametrize('bad', ['abc', '0', '-1', 'inf', 'nan'])
def test_lag_threshold_bad_value_fails_loudly(monkeypatch, bad):
    monkeypatch.setenv(LAG_WARN_ENV, bad)
    with pytest.raises(ValueError, match=LAG_WARN_ENV):
        lag_warn_threshold()


def test_configure_event_loop_default_is_not_debug(loop):
    monitor = configure_event_loop(loop, logging.getLogger('test_loop_health'))
    try:
        assert loop.get_debug() is False
    finally:
        monitor.stop()


def test_configure_event_loop_debug_opt_in(loop, monkeypatch):
    monkeypatch.setenv(ASYNCIO_DEBUG_ENV, '1')
    monkeypatch.setenv(LAG_WARN_ENV, '0.7')
    monitor = configure_event_loop(loop, logging.getLogger('test_loop_health'))
    try:
        assert loop.get_debug() is True
        assert loop.slow_callback_duration == 0.7
        assert monitor.threshold == 0.7
    finally:
        monitor.stop()


def test_lag_monitor_warns_on_blocked_loop(loop, caplog):
    logger = logging.getLogger('test_loop_health.lag')
    monitor = LoopLagMonitor(loop, logger, threshold=0.1, interval=0.02).start()

    async def scenario():
        await asyncio.sleep(0.05)
        time.sleep(0.3)  # block the loop: the monitor's timer fires ~0.3s late
        await asyncio.sleep(0.1)

    with caplog.at_level(logging.WARNING, logger='test_loop_health.lag'):
        loop.run_until_complete(scenario())
    monitor.stop()
    lag_lines = [r for r in caplog.records if 'event loop lag' in r.getMessage()]
    assert len(lag_lines) == 1, [r.getMessage() for r in caplog.records]
    assert lag_lines[0].levelno == logging.WARNING
    assert monitor.stalls == 1
    assert monitor.max_lag >= 0.2


def test_lag_monitor_quiet_on_idle_loop(loop, caplog):
    logger = logging.getLogger('test_loop_health.idle')
    monitor = LoopLagMonitor(loop, logger, threshold=0.5, interval=0.02).start()
    with caplog.at_level(logging.WARNING, logger='test_loop_health.idle'):
        loop.run_until_complete(asyncio.sleep(0.2))
    monitor.stop()
    assert not [r for r in caplog.records if 'event loop lag' in r.getMessage()]
    assert monitor.stalls == 0


def test_lag_monitor_stop_cancels_timer(loop):
    monitor = LoopLagMonitor(loop, logging.getLogger('x'), threshold=0.5, interval=0.01).start()
    monitor.stop()
    loop.run_until_complete(asyncio.sleep(0.05))
    assert monitor.max_lag == 0.0  # never ticked


@pytest.mark.parametrize('service', ['trader/trader_service.py', 'trader/strategy_service.py'])
def test_services_do_not_force_asyncio_debug(service):
    """The services must route through configure_event_loop, never set_debug(True)."""
    src = (REPO / service).read_text()
    assert 'set_debug' not in src
    assert 'configure_event_loop(loop' in src
