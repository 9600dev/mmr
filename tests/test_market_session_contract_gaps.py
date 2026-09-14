"""Session fallback, interval and actionable-warning examples.

The calendar below supplies synthetic but valid schedules. No external data
source or broker is consulted. This draft remains outside the active oracle.
"""
import datetime as dt
import logging

import pandas as pd
import pytest

from trader.data import market_session as ms


DAY = dt.date(2026, 9, 9)
OPEN = pd.Timestamp("2026-09-09T09:00:00Z")
CLOSE = pd.Timestamp("2026-09-09T12:00:00Z")


@pytest.fixture(autouse=True)
def isolated_session_caches(monkeypatch):
    monkeypatch.setattr(ms, "_schedule_cache", {})
    monkeypatch.setattr(ms, "_calendar_cache", {})
    monkeypatch.setattr(ms, "_warned_unknown", set())


@pytest.fixture
def market_warnings(monkeypatch, caplog):
    logger = logging.getLogger("mmr.contract.market_session")
    monkeypatch.setattr(ms, "logging", logger)
    caplog.set_level(logging.WARNING, logger=logger.name)
    return caplog


def install_schedule(monkeypatch, **fields):
    frame = pd.DataFrame([fields], index=[pd.Timestamp(DAY)])

    class Calendar:
        def schedule(self, *, start_date, end_date, market_times):
            assert start_date == end_date
            assert market_times == "all"
            return frame.copy() if start_date == DAY.isoformat() else frame.iloc[:0].copy()

    monkeypatch.setattr(ms, "_calendar", lambda name: Calendar())


@pytest.mark.parametrize("missing", ["NaT", pd.NA])
def test_missing_extended_times_do_not_hide_regular_session_boundaries(monkeypatch, missing):
    install_schedule(monkeypatch, pre=missing, market_open=OPEN, post=missing, market_close=CLOSE)
    lookup = ms.session_intervals("ASX", DAY)
    assert lookup.evaluable is True
    assert lookup.window == (OPEN, CLOSE)
    assert lookup.intervals == ((OPEN, CLOSE),)
    assert ms.in_session(OPEN, "ASX") is True
    assert ms.in_session(CLOSE + pd.Timedelta(minutes=1), "ASX") is False


@pytest.mark.parametrize(
    "break_fields",
    [
        {"break_start": pd.Timestamp("2026-09-09T10:00:00Z")},
        {"break_end": pd.Timestamp("2026-09-09T11:00:00Z")},
    ],
)
def test_one_readable_break_endpoint_preserves_the_evaluable_outer_window(monkeypatch, break_fields):
    install_schedule(monkeypatch, market_open=OPEN, market_close=CLOSE, **break_fields)
    lookup = ms.session_intervals("ASX", DAY)
    assert lookup.evaluable is True
    assert lookup.window == (OPEN, CLOSE)
    assert lookup.intervals == ((OPEN, CLOSE),)


@pytest.mark.parametrize(
    "pause_start,pause_end,inside_pause,resumes",
    [
        (OPEN, pd.Timestamp("2026-09-09T10:00:00Z"),
         pd.Timestamp("2026-09-09T09:30:00Z"), pd.Timestamp("2026-09-09T10:00:00Z")),
        (pd.Timestamp("2026-09-09T11:00:00Z"), CLOSE,
         pd.Timestamp("2026-09-09T11:30:00Z"), CLOSE),
    ],
)
def test_a_break_touching_a_session_boundary_still_excludes_the_pause(
    monkeypatch, pause_start, pause_end, inside_pause, resumes
):
    install_schedule(monkeypatch, market_open=OPEN, market_close=CLOSE,
                     break_start=pause_start, break_end=pause_end)
    assert ms.in_session(inside_pause, "ASX") is False
    # A final close remains admissible for the exchange's closing-auction print.
    assert ms.in_session(resumes, "ASX") is True


def test_unknown_venue_warnings_are_emitted_once_for_each_distinct_listing(monkeypatch, market_warnings):
    listings = [("UNMAPPED_ROUTE", "UNMAPPED_PRIMARY_A"), ("UNMAPPED_ROUTE", "UNMAPPED_PRIMARY_B")]
    for exchange, primary in listings * 2:
        assert ms.in_session(OPEN, exchange, primary) is True
    records = [record for record in market_warnings.records if record.name == "mmr.contract.market_session"]
    assert len(records) == 2
    for record, (exchange, primary) in zip(records, listings):
        message = record.getMessage()
        assert record.levelno >= logging.WARNING
        assert exchange in message and primary in message


def test_unreadable_boundary_warning_identifies_the_calendar_day_and_available_columns(monkeypatch, market_warnings):
    install_schedule(monkeypatch, market_open=pd.NaT, market_close=CLOSE)
    lookup = ms.session_window("ASX", DAY)
    assert lookup.evaluable is False and lookup.window is None
    assert ("ASX", DAY) not in ms._schedule_cache
    messages = [record.getMessage() for record in market_warnings.records]
    assert len(messages) == 1
    assert all(fact in messages[0] for fact in ("ASX", DAY.isoformat(), "market_open", "market_close"))


def test_calendar_exception_warning_retains_the_cause_without_poisoning_the_cache(monkeypatch, market_warnings):
    def unavailable(name):
        raise LookupError("calendar feed unavailable")

    monkeypatch.setattr(ms, "_calendar", unavailable)
    lookup = ms.session_window("ASX", DAY)
    assert lookup.evaluable is False and lookup.window is None
    assert ("ASX", DAY) not in ms._schedule_cache
    messages = [record.getMessage() for record in market_warnings.records]
    assert len(messages) == 1
    assert all(fact in messages[0] for fact in ("ASX", DAY.isoformat(), "calendar feed unavailable"))
