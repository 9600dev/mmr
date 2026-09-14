"""The real freshness clock supplies an aware UTC instant."""
import datetime as dt

from trader.strategy.auto_executor import AutoExecutor


def test_default_freshness_clock_is_timezone_aware_utc():
    executor = AutoExecutor.__new__(AutoExecutor)
    observed = executor._now_utc()
    assert observed.tzinfo is not None
    assert observed.utcoffset() == dt.timedelta(0)
