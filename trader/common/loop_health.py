"""Event-loop health: opt-in asyncio debug mode and a cheap lag watchdog.

Until 2026-11-05 both long-running services called ``loop.set_debug(True)``
unless ``TRADER_NODEBUG`` was set — so production ALWAYS ran asyncio debug
mode (extra bookkeeping on every callback: source tracebacks captured for
every handle/task, coroutine-origin tracking, slow-callback timing) and the
only way to turn it off also silenced all logging below CRITICAL. The two are
now decoupled:

* asyncio debug mode is OPT-IN: ``MMR_ASYNCIO_DEBUG=1`` or the service's
  ``--debug`` flag. An unrecognised value for the env var raises rather than
  being read as "off" (fail loudly).
* ``TRADER_NODEBUG`` keeps its (historical) log-level meaning only: set, it
  lowers all loggers to CRITICAL. It no longer has anything to do with asyncio.
* :class:`LoopLagMonitor` runs in every mode. It schedules one ``call_later``
  per interval and measures how late it fires — a blocked loop shows up as a
  single WARNING with the lag, which is what debug mode's slow-callback log was
  being kept on for, at the cost of one timer per second.
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional

ASYNCIO_DEBUG_ENV = 'MMR_ASYNCIO_DEBUG'
LAG_WARN_ENV = 'MMR_LOOP_LAG_WARN_S'
DEFAULT_LAG_WARN_S = 0.5
DEFAULT_LAG_INTERVAL_S = 1.0

_TRUE = frozenset({'1', 'true', 'yes', 'on'})
_FALSE = frozenset({'', '0', 'false', 'no', 'off'})


def asyncio_debug_requested(cli_flag: bool = False) -> bool:
    """Whether asyncio debug mode should be enabled.

    Args:
        cli_flag: the service's ``--debug`` flag.

    Returns:
        True if ``cli_flag`` is set or ``MMR_ASYNCIO_DEBUG`` is truthy.

    Raises:
        ValueError: ``MMR_ASYNCIO_DEBUG`` holds a value that is neither a
            recognised true nor false spelling.
    """
    raw = os.environ.get(ASYNCIO_DEBUG_ENV, '').strip().lower()
    if raw in _TRUE:
        env_on = True
    elif raw in _FALSE:
        env_on = False
    else:
        raise ValueError(
            f'{ASYNCIO_DEBUG_ENV}={os.environ.get(ASYNCIO_DEBUG_ENV)!r} is not a '
            f'recognised boolean (use one of {sorted(_TRUE | (_FALSE - {""}))})')
    return bool(cli_flag) or env_on


def lag_warn_threshold() -> float:
    """Loop-lag WARNING threshold in seconds (``MMR_LOOP_LAG_WARN_S``, default 0.5).

    Raises:
        ValueError: the env var is set but not a positive finite number.
    """
    raw = os.environ.get(LAG_WARN_ENV, '').strip()
    if not raw:
        return DEFAULT_LAG_WARN_S
    try:
        value = float(raw)
    except ValueError:
        raise ValueError(f'{LAG_WARN_ENV}={raw!r} is not a number') from None
    if not (value > 0.0 and value != float('inf')):
        raise ValueError(f'{LAG_WARN_ENV}={raw!r} must be a positive finite number of seconds')
    return value


class LoopLagMonitor:
    """Log a WARNING whenever the event loop runs a timer late by > threshold.

    One ``loop.call_later`` per ``interval``; the callback compares the loop
    clock with when it was due. Lag above ``threshold`` means some callback
    (or a run of them) held the loop that long — every timer, socket read and
    pulse on that loop was delayed by the same amount.
    """

    def __init__(self,
                 loop: asyncio.AbstractEventLoop,
                 logger: logging.Logger,
                 threshold: float = DEFAULT_LAG_WARN_S,
                 interval: float = DEFAULT_LAG_INTERVAL_S):
        if not threshold > 0.0:
            raise ValueError(f'threshold must be > 0, got {threshold!r}')
        if not interval > 0.0:
            raise ValueError(f'interval must be > 0, got {interval!r}')
        self.loop = loop
        self.logger = logger
        self.threshold = threshold
        self.interval = interval
        self.max_lag = 0.0
        self.stalls = 0
        self._due: Optional[float] = None
        self._handle: Optional[asyncio.TimerHandle] = None

    def start(self) -> 'LoopLagMonitor':
        if self._handle is None:
            self._schedule()
        return self

    def stop(self) -> None:
        if self._handle is not None:
            self._handle.cancel()
            self._handle = None

    def _schedule(self) -> None:
        self._due = self.loop.time() + self.interval
        self._handle = self.loop.call_later(self.interval, self._tick)

    def _tick(self) -> None:
        due = self._due if self._due is not None else self.loop.time()
        lag = self.loop.time() - due
        if lag > self.max_lag:
            self.max_lag = lag
        if lag > self.threshold:
            self.stalls += 1
            self.logger.warning(
                'event loop lag %.3fs (threshold %.3fs): a callback held the loop; '
                'timers, socket reads and pulses were delayed by this much '
                '(set %s=1 to find the slow callback)',
                lag, self.threshold, ASYNCIO_DEBUG_ENV)
        self._schedule()


def configure_event_loop(loop: asyncio.AbstractEventLoop,
                         logger: logging.Logger,
                         cli_debug: bool = False) -> LoopLagMonitor:
    """Apply the service event-loop policy and start the lag monitor.

    asyncio debug mode only if requested (see :func:`asyncio_debug_requested`);
    when it is on, ``slow_callback_duration`` uses the same threshold as the lag
    monitor so the two agree on what "slow" means. Returns the started monitor.
    """
    threshold = lag_warn_threshold()
    debug = asyncio_debug_requested(cli_debug)
    loop.set_debug(debug)
    if debug:
        loop.slow_callback_duration = threshold
        logger.info('asyncio debug mode ENABLED (slow_callback_duration=%.3fs) — '
                    'adds per-callback overhead; not for normal operation', threshold)
    monitor = LoopLagMonitor(loop, logger, threshold=threshold)
    monitor.start()
    return monitor
