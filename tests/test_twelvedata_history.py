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
# Extended-hours (prepost) — 1/5/15/30min only
# ---------------------------------------------------------------------------

class TestPrepost:
    """TwelveData's prepost=true flag is what gates pre-market + post-market
    bars (07:00-20:00 ET). Without it, 1-min US equity requests return only
    the regular session (~390 bars/day) and we lose half the coverage Massive
    gives us. The flag only applies at 1/5/15/30min on US equities per
    https://support.twelvedata.com/en/articles/5195429-pre-post-market-data."""

    def _worker_with_spy(self, include_extended_hours=True):
        worker = TwelveDataHistoryWorker(
            twelvedata_api_key='test',
            include_extended_hours=include_extended_hours,
        )
        fake_ts = MagicMock()
        fake_ts.as_pandas.return_value = _fake_td_dataframe(3)
        worker.client = MagicMock()
        worker.client.time_series.return_value = fake_ts
        return worker

    def test_default_is_extended_hours_on(self):
        """Regression guard: dropping the default back to regular-session-only
        silently loses pre/post coverage — what the user explicitly didn't
        want. If someone lowers this default, the test catches it."""
        worker = TwelveDataHistoryWorker(twelvedata_api_key='test')
        assert worker.include_extended_hours is True

    def test_prepost_true_for_1min(self):
        worker = self._worker_with_spy()
        worker.get_history('AAPL', BarSize.Mins1,
                           dt.datetime(2026, 4, 14), dt.datetime(2026, 4, 15))
        kwargs = worker.client.time_series.call_args.kwargs
        assert kwargs['prepost'] == 'true'

    def test_prepost_true_for_5min(self):
        worker = self._worker_with_spy()
        worker.get_history('AAPL', BarSize.Mins5,
                           dt.datetime(2026, 4, 14), dt.datetime(2026, 4, 15))
        assert worker.client.time_series.call_args.kwargs['prepost'] == 'true'

    def test_prepost_true_for_15min(self):
        worker = self._worker_with_spy()
        worker.get_history('AAPL', BarSize.Mins15,
                           dt.datetime(2026, 4, 14), dt.datetime(2026, 4, 15))
        assert worker.client.time_series.call_args.kwargs['prepost'] == 'true'

    def test_prepost_true_for_30min(self):
        worker = self._worker_with_spy()
        worker.get_history('AAPL', BarSize.Mins30,
                           dt.datetime(2026, 4, 14), dt.datetime(2026, 4, 15))
        assert worker.client.time_series.call_args.kwargs['prepost'] == 'true'

    def test_prepost_false_for_1hour(self):
        """Not supported upstream at 1h — pass false to keep requests clean."""
        worker = self._worker_with_spy()
        worker.get_history('AAPL', BarSize.Hours1,
                           dt.datetime(2026, 4, 14), dt.datetime(2026, 4, 16))
        assert worker.client.time_series.call_args.kwargs['prepost'] == 'false'

    def test_prepost_false_for_daily(self):
        worker = self._worker_with_spy()
        worker.get_history('AAPL', BarSize.Days1,
                           dt.datetime(2026, 4, 14), dt.datetime(2026, 4, 18))
        assert worker.client.time_series.call_args.kwargs['prepost'] == 'false'

    def test_opt_out_via_constructor(self):
        """include_extended_hours=False forces prepost=false even on intraday."""
        worker = self._worker_with_spy(include_extended_hours=False)
        worker.get_history('AAPL', BarSize.Mins1,
                           dt.datetime(2026, 4, 14), dt.datetime(2026, 4, 15))
        assert worker.client.time_series.call_args.kwargs['prepost'] == 'false'


# ---------------------------------------------------------------------------
# Pagination — 1-minute requests spanning multiple 7-day chunks
# ---------------------------------------------------------------------------

class TestPagination:

    def _build_paginating_worker(self, chunks_per_call=2):
        """Return (worker, call_tracker). Each successive chunk returns
        distinct bars so we can verify concat + dedupe."""
        worker = TwelveDataHistoryWorker(twelvedata_api_key='test')
        calls = []

        def fake_time_series(**kwargs):
            calls.append(kwargs)
            # Build a chunk of 5 bars spanning the requested window
            from_dt = pd.to_datetime(kwargs['start_date'])
            idx = pd.DatetimeIndex(
                [from_dt + dt.timedelta(minutes=i) for i in range(5)],
                name='datetime',
            ).tz_localize(pytz.timezone(kwargs.get('timezone', 'US/Eastern')))
            df = pd.DataFrame(
                {
                    'open': [100.0 + i for i in range(5)],
                    'high': [101.0 + i for i in range(5)],
                    'low': [99.0 + i for i in range(5)],
                    'close': [100.5 + i for i in range(5)],
                    'volume': [1000 + i * 10 for i in range(5)],
                },
                index=idx,
            )
            mock_ts = MagicMock()
            mock_ts.as_pandas.return_value = df
            return mock_ts

        worker.client = MagicMock()
        worker.client.time_series.side_effect = fake_time_series
        return worker, calls

    def test_1min_over_30_days_paginates(self):
        """A 30-day window at 1-min must issue multiple time_series calls
        (chunk size = 7 days → expect ≥4 chunks)."""
        worker, calls = self._build_paginating_worker()
        df = worker.get_history(
            ticker='AAPL',
            bar_size=BarSize.Mins1,
            start_date=dt.datetime(2026, 1, 1),
            end_date=dt.datetime(2026, 1, 31),
        )
        # 30 days / 7-day chunks = 5 chunks (ceil)
        assert len(calls) >= 4, f'expected ≥4 chunk calls, got {len(calls)}'
        # Every chunk produced 5 bars; across 5 distinct windows = 25 unique bars
        assert len(df) > 0
        assert df.index.is_monotonic_increasing

    def test_daily_over_1_year_does_not_paginate(self):
        """Daily bars fit in a single call even for a year — no chunking."""
        worker, calls = self._build_paginating_worker()
        worker.get_history(
            ticker='AAPL',
            bar_size=BarSize.Days1,
            start_date=dt.datetime(2025, 1, 1),
            end_date=dt.datetime(2026, 1, 1),
        )
        assert len(calls) == 1

    def test_single_bad_chunk_does_not_abort_symbol(self):
        """A TwelveData "No data is available" response on chunk N must not
        lose bars from chunks N-1 or N+1 — regression guard for the
        production bug where one bad chunk emptied the whole symbol."""
        worker = TwelveDataHistoryWorker(twelvedata_api_key='test')

        call_idx = [0]

        def fake_time_series(**kwargs):
            i = call_idx[0]
            call_idx[0] += 1
            if i == 2:
                # Simulate TwelveData's "no bars in this window" error mid-run
                raise RuntimeError('No data is available on the specified dates.')
            from_dt = pd.to_datetime(kwargs['start_date'])
            idx = pd.DatetimeIndex(
                [from_dt + dt.timedelta(minutes=m) for m in range(3)],
                name='datetime',
            ).tz_localize(pytz.timezone(kwargs.get('timezone', 'US/Eastern')))
            df = pd.DataFrame(
                {'open': [100.0] * 3, 'high': [101.0] * 3, 'low': [99.0] * 3,
                 'close': [100.5] * 3, 'volume': [1000] * 3},
                index=idx,
            )
            mock_ts = MagicMock()
            mock_ts.as_pandas.return_value = df
            return mock_ts

        worker.client = MagicMock()
        worker.client.time_series.side_effect = fake_time_series

        df = worker.get_history(
            'AAPL', BarSize.Mins1,
            dt.datetime(2026, 1, 1), dt.datetime(2026, 1, 31),
        )
        # 5 chunks scheduled, chunk index 2 errors out → 4 successful chunks × 3 bars = 12 bars
        assert call_idx[0] >= 4
        assert len(df) > 0, 'empty df means one failed chunk killed the whole symbol'
        assert len(df) == (call_idx[0] - 1) * 3

    def test_dedupes_overlapping_chunks(self):
        """If consecutive chunks happen to return overlapping timestamps
        (e.g. boundary bar), the final df must dedupe by index."""
        worker = TwelveDataHistoryWorker(twelvedata_api_key='test')

        call_count = [0]
        shared_ts = pd.Timestamp('2026-01-15 09:30:00', tz='US/Eastern')

        def fake_time_series(**kwargs):
            call_count[0] += 1
            # Every call returns the *same* single bar — dedupe must collapse
            idx = pd.DatetimeIndex([shared_ts], name='datetime')
            df = pd.DataFrame(
                {'open': [100.0], 'high': [101.0], 'low': [99.0], 'close': [100.5], 'volume': [1000]},
                index=idx,
            )
            mock_ts = MagicMock()
            mock_ts.as_pandas.return_value = df
            return mock_ts

        worker.client = MagicMock()
        worker.client.time_series.side_effect = fake_time_series

        df = worker.get_history(
            'AAPL', BarSize.Mins1,
            dt.datetime(2026, 1, 1), dt.datetime(2026, 1, 22),
        )
        assert call_count[0] >= 3
        assert len(df) == 1


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
