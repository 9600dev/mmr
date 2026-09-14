"""A known closed day has an evaluable, empty interval collection."""

import datetime as dt

import pandas as pd

from trader.data import market_session as ms


def test_known_closed_day_keeps_an_empty_tuple_of_trading_intervals(monkeypatch):
    class ClosedCalendar:
        def schedule(self, *, start_date, end_date, market_times):
            return pd.DataFrame(columns=["market_open", "market_close"])

    monkeypatch.setattr(ms, "_schedule_cache", {})
    monkeypatch.setattr(ms, "_calendar", lambda _name: ClosedCalendar())

    lookup = ms.session_intervals("ASX", dt.date(2026, 9, 12))

    assert lookup.evaluable is True
    assert lookup.window is None
    assert lookup.intervals == ()
