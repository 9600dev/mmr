"""Unit + integration tests for TwelveDataHistoryWorker."""

import datetime as dt
import os
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import pytz

from trader.listeners.twelvedata_history import TwelveDataHistoryWorker
from trader.objects import BarSize, WhatToShow


# ---------------------------------------------------------------------------
# BarSize → TwelveData interval mapping
# ---------------------------------------------------------------------------

class TestBarSizeIntervalMapping:

    def test_common_intervals(self):
        assert BarSize.to_twelvedata_interval(BarSize.Mins1) == '1min'
        assert BarSize.to_twelvedata_interval(BarSize.Mins5) == '5min'
        assert BarSize.to_twelvedata_interval(BarSize.Mins15) == '15min'
        assert BarSize.to_twelvedata_interval(BarSize.Mins30) == '30min'
        assert BarSize.to_twelvedata_interval(BarSize.Hours1) == '1h'
        assert BarSize.to_twelvedata_interval(BarSize.Hours2) == '2h'
        assert BarSize.to_twelvedata_interval(BarSize.Hours4) == '4h'
        assert BarSize.to_twelvedata_interval(BarSize.Days1) == '1day'
        assert BarSize.to_twelvedata_interval(BarSize.Weeks1) == '1week'
        assert BarSize.to_twelvedata_interval(BarSize.Months1) == '1month'

    def test_unsupported_seconds_raises(self):
        # TwelveData doesn't support sub-minute intervals
        with pytest.raises(ValueError, match='unsupported BarSize'):
            BarSize.to_twelvedata_interval(BarSize.Secs1)

    def test_unsupported_3min_raises(self):
        # TwelveData supports 1/5/15/30/45min but not 2/3/10/20min
        with pytest.raises(ValueError, match='unsupported BarSize'):
            BarSize.to_twelvedata_interval(BarSize.Mins3)

    def test_unsupported_8hour_raises(self):
        with pytest.raises(ValueError, match='unsupported BarSize'):
            BarSize.to_twelvedata_interval(BarSize.Hours8)


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestConstruction:

    def test_empty_api_key_rejected(self):
        with pytest.raises(ValueError, match='twelvedata_api_key is required'):
            TwelveDataHistoryWorker(twelvedata_api_key='')

    def test_accepts_api_key(self):
        worker = TwelveDataHistoryWorker(twelvedata_api_key='abc123')
        assert worker.client is not None


# ---------------------------------------------------------------------------
# get_history — mocked
# ---------------------------------------------------------------------------

def _fake_td_dataframe(n=3, interval='1day', tz='US/Eastern'):
    """TwelveData as_pandas() returns a DataFrame with datetime index named
    'datetime' and string-valued OHLCV columns by default."""
    start = dt.datetime(2026, 4, 14, 9, 30)
    idx = pd.DatetimeIndex(
        [start + dt.timedelta(days=i) for i in range(n)],
        name='datetime',
    )
    # TwelveData's as_pandas() typically tz-localizes — mirror that
    if tz:
        idx = idx.tz_localize(pytz.timezone(tz))
    return pd.DataFrame(
        {
            'open': [150.0 + i for i in range(n)],
            'high': [155.0 + i for i in range(n)],
            'low': [149.0 + i for i in range(n)],
            'close': [154.0 + i for i in range(n)],
            'volume': [1_000_000 + i * 100 for i in range(n)],
        },
        index=idx,
    )


class TestGetHistoryMocked:

    def _build_worker(self, fake_df):
        worker = TwelveDataHistoryWorker(twelvedata_api_key='test')
        fake_ts = MagicMock()
        fake_ts.as_pandas.return_value = fake_df
        worker.client = MagicMock()
        worker.client.time_series.return_value = fake_ts
        return worker, fake_ts

    def test_returns_expected_columns(self):
        worker, _ = self._build_worker(_fake_td_dataframe(3))
        df = worker.get_history(
            ticker='AAPL',
            bar_size=BarSize.Days1,
            start_date=dt.datetime(2026, 4, 14),
            end_date=dt.datetime(2026, 4, 18),
        )
        assert not df.empty
        expected = {'open', 'high', 'low', 'close', 'volume', 'average', 'bar_count', 'bar_size', 'what_to_show'}
        assert set(df.columns) == expected

    def test_rows_are_typed_numeric(self):
        worker, _ = self._build_worker(_fake_td_dataframe(2))
        df = worker.get_history('AAPL', BarSize.Days1, dt.datetime(2026, 4, 14), dt.datetime(2026, 4, 16))
        assert pd.api.types.is_numeric_dtype(df['open'])
        assert pd.api.types.is_numeric_dtype(df['close'])
        assert pd.api.types.is_numeric_dtype(df['volume'])

    def test_index_is_timezone_aware(self):
        worker, _ = self._build_worker(_fake_td_dataframe(3))
        df = worker.get_history('AAPL', BarSize.Days1, dt.datetime(2026, 4, 14), dt.datetime(2026, 4, 18))
        assert df.index.tz is not None
        # Default timezone is US/Eastern
        assert 'Eastern' in str(df.index.tz) or 'EST' in str(df.index.tz) or 'EDT' in str(df.index.tz)

    def test_index_name_is_date(self):
        worker, _ = self._build_worker(_fake_td_dataframe(3))
        df = worker.get_history('AAPL', BarSize.Days1, dt.datetime(2026, 4, 14), dt.datetime(2026, 4, 18))
        assert df.index.name == 'date'

    def test_bar_size_column_matches_request(self):
        worker, _ = self._build_worker(_fake_td_dataframe(3))
        df = worker.get_history('AAPL', BarSize.Days1, dt.datetime(2026, 4, 14), dt.datetime(2026, 4, 18))
        assert (df['bar_size'] == '1 day').all()

    def test_what_to_show_is_trades(self):
        worker, _ = self._build_worker(_fake_td_dataframe(3))
        df = worker.get_history('AAPL', BarSize.Days1, dt.datetime(2026, 4, 14), dt.datetime(2026, 4, 18))
        assert (df['what_to_show'] == int(WhatToShow.TRADES)).all()

    def test_sorted_ascending(self):
        # Build a df in reverse order (TwelveData sometimes returns desc)
        df_desc = _fake_td_dataframe(5).iloc[::-1]
        worker, _ = self._build_worker(df_desc)
        df = worker.get_history('AAPL', BarSize.Days1, dt.datetime(2026, 4, 14), dt.datetime(2026, 4, 18))
        assert df.index.is_monotonic_increasing

    def test_empty_response_returns_empty_df(self):
        worker, _ = self._build_worker(pd.DataFrame())
        df = worker.get_history('AAPL', BarSize.Days1, dt.datetime(2026, 4, 14), dt.datetime(2026, 4, 18))
        assert df.empty

    def test_none_response_returns_empty_df(self):
        worker, fake_ts = self._build_worker(_fake_td_dataframe(1))
        fake_ts.as_pandas.return_value = None
        df = worker.get_history('AAPL', BarSize.Days1, dt.datetime(2026, 4, 14), dt.datetime(2026, 4, 18))
        assert df.empty

    def test_propagates_api_exception(self):
        worker = TwelveDataHistoryWorker(twelvedata_api_key='test')
        fake_ts = MagicMock()
        fake_ts.as_pandas.side_effect = RuntimeError('api down')
        worker.client = MagicMock()
        worker.client.time_series.return_value = fake_ts
        with pytest.raises(RuntimeError, match='api down'):
            worker.get_history('AAPL', BarSize.Days1, dt.datetime(2026, 4, 14), dt.datetime(2026, 4, 18))

    def test_intraday_bar_size_passes_datetime_format(self):
        worker, _ = self._build_worker(_fake_td_dataframe(3))
        worker.get_history('AAPL', BarSize.Mins5, dt.datetime(2026, 4, 14, 9, 30), dt.datetime(2026, 4, 14, 16))
        call_kwargs = worker.client.time_series.call_args.kwargs
        # Intraday requests should include time, not just a bare date
        assert ':' in call_kwargs['start_date']
        assert call_kwargs['interval'] == '5min'

    def test_daily_bar_size_passes_date_only(self):
        worker, _ = self._build_worker(_fake_td_dataframe(3))
        worker.get_history('AAPL', BarSize.Days1, dt.datetime(2026, 4, 14), dt.datetime(2026, 4, 18))
        call_kwargs = worker.client.time_series.call_args.kwargs
        # Daily requests send YYYY-MM-DD without time component
        assert ':' not in call_kwargs['start_date']
        assert call_kwargs['interval'] == '1day'


# ---------------------------------------------------------------------------
# Live smoke test — gated on TWELVEDATA_API_KEY being set.
# Skipped in CI without a real key. Run locally with:
#   TWELVEDATA_API_KEY=... pytest tests/test_twelvedata_history.py -k live
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    not os.getenv('TWELVEDATA_API_KEY'),
    reason='TWELVEDATA_API_KEY not set; skipping live API test',
)
class TestLiveSmoke:

    def test_live_daily_history_aapl(self):
        worker = TwelveDataHistoryWorker(twelvedata_api_key=os.getenv('TWELVEDATA_API_KEY'))
        end = dt.datetime.now()
        start = end - dt.timedelta(days=14)
        df = worker.get_history('AAPL', BarSize.Days1, start, end)
        assert not df.empty
        # Sanity: at least some trading days in 2 weeks
        assert len(df) >= 5
        assert 'close' in df.columns
        assert (df['close'] > 0).all()
        assert df.index.tz is not None
