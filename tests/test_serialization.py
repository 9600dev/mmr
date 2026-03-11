import datetime as dt
import pytest
import pandas as pd
import numpy as np

from trader.messaging.clientserver import pack, unpack


class TestSerialization:
    def test_primitives(self):
        for val in [42, 3.14, "hello", True, None, [1, 2, 3], {"a": 1}]:
            assert unpack(pack(val)) == val

    def test_datetime(self):
        now = dt.datetime.now(tz=dt.timezone.utc)
        result = unpack(pack(now))
        assert isinstance(result, dt.datetime)
        assert abs((result - now).total_seconds()) < 0.01

    def test_date(self):
        d = dt.date(2024, 6, 15)
        result = unpack(pack(d))
        assert result == d

    def test_time(self):
        t = dt.time(14, 30, 45, 123456)
        result = unpack(pack(t))
        assert result == t

    def test_timedelta(self):
        td = dt.timedelta(days=3, seconds=3600)
        result = unpack(pack(td))
        assert abs(result.total_seconds() - td.total_seconds()) < 0.01

    def test_dataframe(self):
        df = pd.DataFrame({
            "a": [1, 2, 3],
            "b": [4.0, 5.0, 6.0],
            "c": ["x", "y", "z"],
        })
        result = unpack(pack(df))
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ["a", "b", "c"]
        assert len(result) == 3
        assert result["a"].tolist() == [1, 2, 3]

    def test_nested_structures(self):
        data = {
            "date": dt.date(2024, 1, 1),
            "values": [1, 2, 3],
            "nested": {"key": "val"},
        }
        result = unpack(pack(data))
        assert result["date"] == dt.date(2024, 1, 1)

    def test_arbitrary_object_dill_fallback(self):
        class Foo:
            def __init__(self, x):
                self.x = x
        obj = Foo(42)
        result = unpack(pack(obj))
        assert result.x == 42
