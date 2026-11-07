"""Snapshot waits briefly for previous close + volume after bid/ask/last arrive (cold scan blanked change/volume)."""
import asyncio
import math
import time
from types import SimpleNamespace

import pytest
from reactivex.subject import Subject

from trader.listeners.ibreactive import IBAIORx

NAN = math.nan


def _tick(**kw):
    base = dict(last=NAN, bid=NAN, ask=NAN, close=NAN, volume=NAN)
    base.update(kw)
    return SimpleNamespace(**base)


def _run(schedule):
    rx = IBAIORx('127.0.0.1', 1, 1)
    subject = Subject()
    rx._IBAIORx__subscribe_contract = lambda **kw: subject  # type: ignore[attr-defined]

    async def go():
        loop = asyncio.get_running_loop()
        for delay, t in schedule:
            loop.call_later(delay, subject.on_next, t)
        t0 = time.monotonic()
        ticker = await rx._IBAIORx__get_single_mrkt_data(
            SimpleNamespace(symbol='X', conId=1), snapshot=True)
        return ticker, time.monotonic() - t0

    return asyncio.run(go())


def test_waits_for_close_and_volume_after_quote():
    ticker, _ = _run([
        (0.05, _tick(last=1.0, bid=-1, ask=-1)),
        (0.4, _tick(last=1.0, bid=-1, ask=-1, close=0.9, volume=1234.0)),
    ])
    assert ticker.close == 0.9 and ticker.volume == 1234.0


def test_no_extra_wait_when_everything_arrives_together():
    ticker, secs = _run([(0.05, _tick(last=1.0, bid=0.99, ask=1.01, close=0.9, volume=5.0))])
    assert ticker.volume == 5.0
    assert secs < 0.6


def test_grace_is_bounded_when_close_never_comes():
    ticker, secs = _run([(0.05, _tick(last=1.0, bid=0.99, ask=1.01))])
    assert math.isnan(ticker.close)
    assert secs < 3.0   # EXTRAS_GRACE (2s) + settle, not the 15s hard timeout


def test_partial_path_also_waits_for_close_and_volume():
    # Concurrent cold batch: only `last` before PARTIAL_WAIT (4s); bid/close/
    # volume land just after it (live 19-row scan, 2026-11-06).
    ticker, secs = _run([
        (0.05, _tick(last=1.0)),
        (4.4, _tick(last=1.0, bid=-1, ask=-1, close=0.9, volume=77.0)),
    ])
    assert ticker.close == 0.9 and ticker.volume == 77.0
    assert secs < 5.5
